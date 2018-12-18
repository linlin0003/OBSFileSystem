/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.fs.obs;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InterruptedIOException;
import java.net.URI;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;

import com.google.common.util.concurrent.SettableFuture;
import com.obs.services.ObsClient;
import com.obs.services.exception.ObsException;
import com.obs.services.model.*;
import com.obs.services.model.fs.*;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CreateFlag;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileAlreadyExistsException;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.GlobalStorageStatistics;
import org.apache.hadoop.fs.InvalidRequestException;
import org.apache.hadoop.fs.LocalDirAllocator;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.fs.PathIOException;
import org.apache.hadoop.fs.PathIsNotEmptyDirectoryException;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.fs.StorageStatistics;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.Progressable;
import org.apache.hadoop.util.ReflectionUtils;

import static org.apache.hadoop.fs.obs.Constants.*;
import static org.apache.hadoop.fs.obs.Listing.ACCEPT_ALL;
import static org.apache.hadoop.fs.obs.OBSUtils.*;
import static org.apache.hadoop.fs.obs.Statistic.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The core OBS Filesystem implementation.
 *
 * This subclass is marked as private as code should not be creating it
 * directly; use {@link FileSystem#get(Configuration)} and variants to
 * create one.
 *
 * If cast to {@code OBSFileSystem}, extra methods and features may be accessed.
 * Consider those private and unstable.
 *
 * Because it prints some of the state of the instrumentation,
 * the output of {@link #toString()} must also be considered unstable.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class OBSFileSystem extends FileSystem {
  /**
   * Default blocksize as used in blocksize and FS status queries.
   */
  public static final int DEFAULT_BLOCKSIZE = 32 * 1024 * 1024;
  private URI uri;
  private Path workingDir;
  private String username;
  private ObsClient obs;
  private boolean enablePosix = false;
  private boolean enableMultiObjectsDeleteRecursion = true;
  private boolean renameSupportEmptyDestinationFolder = true;
  private String bucket;
  private int maxKeys;
  private Listing listing;
  private long partSize;
  private boolean enableMultiObjectsDelete;
  private ListeningExecutorService boundedThreadPool;
  private ListeningExecutorService boundedCopyThreadPool;
  private ListeningExecutorService boundedDeleteThreadPool;
  private ThreadPoolExecutor unboundedReadThreadPool;
  private ExecutorService unboundedThreadPool;
  private ListeningExecutorService boundedCopyPartThreadPool;
  private long multiPartThreshold;
  public static final Logger LOG = LoggerFactory.getLogger(OBSFileSystem.class);
  private static final Logger PROGRESS =
      LoggerFactory.getLogger("org.apache.hadoop.fs.obs.OBSFileSystem.Progress");
  private LocalDirAllocator directoryAllocator;
  private String serverSideEncryptionAlgorithm;
  private OBSInstrumentation instrumentation;
  private OBSStorageStatistics storageStatistics;
  private long readAhead;
  private OBSInputPolicy inputPolicy;
  private final AtomicBoolean closed = new AtomicBoolean(false);

  // The maximum number of entries that can be deleted in any call to obs
  private int MAX_ENTRIES_TO_DELETE;
  private boolean blockUploadEnabled;
  private String blockOutputBuffer;
  private OBSDataBlocks.BlockFactory blockFactory;
  private int blockOutputActiveBlocks;

  private int bufferPartSize;
  private long bufferMaxRange;

  private boolean readaheadInputStreamEnabled = false;
  private long copyPartSize;
  private int maxCopyPartThreads;
  private int maxCopyPartQueue;
  private long keepAliveTime;

  /** Called after a new FileSystem instance is constructed.
   * @param name a uri whose authority section names the host, port, etc.
   *   for this FileSystem
   * @param originalConf the configuration to use for the FS. The
   * bucket-specific options are patched over the base ones before any use is
   * made of the config.
   */
  public void initialize(URI name, Configuration originalConf)
      throws IOException {
    uri = OBSLoginHelper.buildFSURI(name);
    // get the host; this is guaranteed to be non-null, non-empty
    bucket = name.getHost();
    // clone the configuration into one with propagated bucket options
    Configuration conf = propagateBucketOptions(originalConf, bucket);
    patchSecurityCredentialProviders(conf);
    super.initialize(name, conf);
    setConf(conf);
    try {
      instrumentation = new OBSInstrumentation(name);

      // Username is the current user at the time the FS was instantiated.
      username = UserGroupInformation.getCurrentUser().getShortUserName();
      workingDir = new Path("/user", username)
          .makeQualified(this.uri, this.getWorkingDirectory());


        Class<? extends ObsClientFactory> obsClientFactoryClass = conf.getClass(
                OBS_CLIENT_FACTORY_IMPL, DEFAULT_OBS_CLIENT_FACTORY_IMPL,
                ObsClientFactory.class);
        obs = ReflectionUtils.newInstance(obsClientFactoryClass, conf)
                .createObsClient(name);

      maxKeys = intOption(conf, MAX_PAGING_KEYS, DEFAULT_MAX_PAGING_KEYS, 1);
      listing = new Listing(this);
      partSize = getMultipartSizeProperty(conf,
          MULTIPART_SIZE, DEFAULT_MULTIPART_SIZE);
      multiPartThreshold = getMultipartSizeProperty(conf,
          MIN_MULTIPART_THRESHOLD, DEFAULT_MIN_MULTIPART_THRESHOLD);

      //check but do not store the block size
      longBytesOption(conf, FS_OBS_BLOCK_SIZE, DEFAULT_BLOCKSIZE, 1);
      enableMultiObjectsDelete = conf.getBoolean(ENABLE_MULTI_DELETE, true);
      MAX_ENTRIES_TO_DELETE = conf.getInt(MULTI_DELETE_MAX_NUMBER, MULTI_DELETE_DEFAULT_NUMBER);

      enableMultiObjectsDeleteRecursion = conf.getBoolean(MULTI_DELETE_RECURSION, true);
      renameSupportEmptyDestinationFolder = conf.getBoolean(RENAME_TO_EMPTY_FOLDER, true);

      readAhead = longBytesOption(conf, READAHEAD_RANGE,
          DEFAULT_READAHEAD_RANGE, 0);
      storageStatistics = (OBSStorageStatistics)
          GlobalStorageStatistics.INSTANCE
              .put(OBSStorageStatistics.NAME,
                  new GlobalStorageStatistics.StorageStatisticsProvider() {
                    @Override
                    public StorageStatistics provide() {
                      return new OBSStorageStatistics();
                    }
                  });

      int maxThreads = conf.getInt(MAX_THREADS, DEFAULT_MAX_THREADS);
      int maxCopyThreads = conf.getInt(MAX_COPY_THREADS, DEFAULT_MAX_COPY_THREADS);
      int maxDeleteThreads = conf.getInt(MAX_DELETE_THREADS, DEFAULT_MAX_DELETE_THREADS);
      int maxCopyQueue = intOption(conf,
              MAX_COPY_QUEUE, DEFAULT_MAX_COPY_QUEUE, 1);
      int maxDeleteQueue = intOption(conf,
                MAX_DELETE_QUEUE, DEFAULT_MAX_DELETE_QUEUE, 1);
      if (maxThreads < 2) {
        LOG.warn(MAX_THREADS + " must be at least 2: forcing to 2.");
        maxThreads = 2;
      }

      int maxReadThreads = conf.getInt(MAX_READ_THREADS, DEFAULT_MAX_READ_THREADS);
      int coreReadThreads = conf.getInt(CORE_READ_THREADS, DEFAULT_CORE_READ_THREADS);
      if (maxReadThreads < 2) {
        LOG.warn(MAX_THREADS + " must be at least 2: forcing to 2.");
        maxReadThreads = 2;
      }

      bufferPartSize = intOption(conf,
              BUFFER_PART_SIZE, DEFAULT_BUFFER_PART_SIZE, 1 * 1024);
      bufferMaxRange = intOption(conf,
              BUFFER_MAX_RANGE, DEFAULT_BUFFER_MAX_RANGE, 1 * 1024);


      int totalTasks = intOption(conf,
          MAX_TOTAL_TASKS, DEFAULT_MAX_TOTAL_TASKS, 1);
      keepAliveTime = longOption(conf, KEEPALIVE_TIME,
          DEFAULT_KEEPALIVE_TIME, 0);
      
      copyPartSize = longOption(conf, COPY_PART_SIZE, DEFAULT_COPY_PART_SIZE, 0);
      maxCopyPartThreads = conf.getInt(MAX_COPY_PART_THREADS, DEFAULT_MAX_COPY_PART_THREADS);
      maxCopyPartQueue = intOption(conf, MAX_COPY_PART_QUEUE, DEFAULT_MAX_COPY_PART_QUEUE, 1);
      
      boundedThreadPool = BlockingThreadPoolExecutorService.newInstance(
          maxThreads,
          maxThreads + totalTasks,
          keepAliveTime, TimeUnit.SECONDS,
          "obs-transfer-shared");

      boundedCopyThreadPool = new SemaphoredDelegatingExecutor(BlockingThreadPoolExecutorService.newInstance(
              maxCopyThreads,
              maxCopyThreads + maxCopyQueue,
              keepAliveTime, TimeUnit.SECONDS,
              "obs-copy-transfer-shared"),
              maxCopyQueue, true);
      boundedDeleteThreadPool = new SemaphoredDelegatingExecutor(BlockingThreadPoolExecutorService.newInstance(
                maxDeleteThreads,
                maxDeleteThreads + maxDeleteQueue,
                keepAliveTime, TimeUnit.SECONDS,
                "obs-delete-transfer-shared"),
                maxDeleteQueue, true);
      unboundedThreadPool = new ThreadPoolExecutor(
          maxThreads, Integer.MAX_VALUE,
          keepAliveTime, TimeUnit.SECONDS,
          new LinkedBlockingQueue<Runnable>(),
          BlockingThreadPoolExecutorService.newDaemonThreadFactory(
              "obs-transfer-unbounded"));
      readaheadInputStreamEnabled = conf.getBoolean(READAHEAD_INPUTSTREAM_ENABLED, READAHEAD_INPUTSTREAM_ENABLED_DEFAULT);
      if (readaheadInputStreamEnabled){
        unboundedReadThreadPool = new ThreadPoolExecutor(
                coreReadThreads, maxReadThreads,
                keepAliveTime, TimeUnit.SECONDS,
                new LinkedBlockingQueue<Runnable>(),
                BlockingThreadPoolExecutorService.newDaemonThreadFactory(
                        "obs-transfer-read-unbounded"));
      }
      
      boundedCopyPartThreadPool = new SemaphoredDelegatingExecutor(BlockingThreadPoolExecutorService.newInstance(
              maxCopyPartThreads,
              maxCopyPartThreads + maxCopyPartQueue,
              keepAliveTime, TimeUnit.SECONDS,
              "obs-copy-part-transfer-shared"),
              maxCopyPartQueue, true);
      
      initTransferManager();

      initCannedAcls(conf);

      verifyBucketExists();

      getBucketFsStatus();

      initMultipartUploads(conf);

      serverSideEncryptionAlgorithm =
          conf.getTrimmed(SERVER_SIDE_ENCRYPTION_ALGORITHM);
      LOG.debug("Using encryption {}", serverSideEncryptionAlgorithm);
      inputPolicy = OBSInputPolicy.getPolicy(
          conf.getTrimmed(INPUT_FADVISE, INPUT_FADV_NORMAL));

      blockUploadEnabled = conf.getBoolean(FAST_UPLOAD, DEFAULT_FAST_UPLOAD);

      if (blockUploadEnabled) {
        blockOutputBuffer = conf.getTrimmed(FAST_UPLOAD_BUFFER,
            DEFAULT_FAST_UPLOAD_BUFFER);
        partSize = ensureOutputParameterInRange(MULTIPART_SIZE, partSize);
        blockFactory = OBSDataBlocks.createFactory(this, blockOutputBuffer);
        blockOutputActiveBlocks = intOption(conf,
            FAST_UPLOAD_ACTIVE_BLOCKS, DEFAULT_FAST_UPLOAD_ACTIVE_BLOCKS, 1);
        LOG.debug("Using OBSBlockOutputStream with buffer = {}; block={};" +
                " queue limit={}",
            blockOutputBuffer, partSize, blockOutputActiveBlocks);
      } else {
        LOG.debug("Using OBSOutputStream");
      }
    } catch (ObsException e) {
      throw translateException("initializing ", new Path(name), e);
    }

  }

  /**
   * Verify that the bucket exists. This does not check permissions,
   * not even read access.
   * @throws FileNotFoundException the bucket is absent
   * @throws IOException any other problem talking to OBS
   */
  protected void verifyBucketExists()
      throws FileNotFoundException, IOException {
    try {
      if (!obs.headBucket(bucket)) {
        throw new FileNotFoundException("Bucket " + bucket + " does not exist");
      }
    } catch (ObsException e) {
      throw translateException("doesBucketExist", bucket, e);
    }
  }

  /**
   * Get the fs status of the bucket.
   * @throws FileNotFoundException the bucket is absent
   * @throws IOException any other problem talking to OBS
   */
  public boolean getBucketFsStatus(String bucketName)
          throws FileNotFoundException, IOException {
    try {
        GetBucketFSStatusRequest getBucketFsStatusRequest = new GetBucketFSStatusRequest();
        getBucketFsStatusRequest.setBucketName(bucketName);
        GetBucketFSStatusResult getBucketFsStatusResult = obs.getBucketFSStatus(getBucketFsStatusRequest);
        FSStatusEnum fsStatus = getBucketFsStatusResult.getStatus();
        return ((fsStatus != null) && (fsStatus == FSStatusEnum.ENABLED));
    } catch (ObsException e) {
        LOG.error(e.toString());
      throw translateException("getBucketFsStatus", bucket, e);
    }
  }
  private void getBucketFsStatus()
          throws FileNotFoundException, IOException {
    enablePosix = getBucketFsStatus(bucket);
  }
  public boolean isFsBucket() {
    return enablePosix;
  }

  /**
   * Get OBS Instrumentation. For test purposes.
   * @return this instance's instrumentation.
   */
  public OBSInstrumentation getInstrumentation() {
    return instrumentation;
  }

  private void initTransferManager() {

    /*TransferManagerConfiguration transferConfiguration =
        new TransferManagerConfiguration();
    transferConfiguration.setMinimumUploadPartSize(partSize);
    transferConfiguration.setMultipartUploadThreshold(multiPartThreshold);
    transferConfiguration.setMultipartCopyPartSize(partSize);
    transferConfiguration.setMultipartCopyThreshold(multiPartThreshold);

    transfers = new TransferManager(obs, unboundedThreadPool);
    transfers.setConfiguration(transferConfiguration);*/

  }

  private void initCannedAcls(Configuration conf) {
    //No canned acl in obs

    /*String cannedACLName = conf.get(CANNED_ACL, DEFAULT_CANNED_ACL);
    if (!cannedACLName.isEmpty()) {
      cannedACL = CannedAccessControlList.valueOf(cannedACLName);
    } else {
      cannedACL = null;
    }*/
  }

  private void initMultipartUploads(Configuration conf) throws IOException {
    boolean purgeExistingMultipart = conf.getBoolean(PURGE_EXISTING_MULTIPART,
        DEFAULT_PURGE_EXISTING_MULTIPART);
    long purgeExistingMultipartAge = longOption(conf,
        PURGE_EXISTING_MULTIPART_AGE, DEFAULT_PURGE_EXISTING_MULTIPART_AGE, 0);

    if (purgeExistingMultipart) {
      final Date purgeBefore =
          new Date(new Date().getTime() - purgeExistingMultipartAge * 1000);


      try {
        //List + purge
        MultipartUploadListing uploadListing = obs.listMultipartUploads(
                new ListMultipartUploadsRequest(bucket));
        do {
          for (MultipartUpload upload : uploadListing.getMultipartTaskList()) {
            if (upload.getInitiatedDate().compareTo(purgeBefore) < 0) {
              obs.abortMultipartUpload(new AbortMultipartUploadRequest(
                      bucket, upload.getObjectKey(), upload.getUploadId()));
            }
          }

          ListMultipartUploadsRequest request = new ListMultipartUploadsRequest(bucket);
          request.setUploadIdMarker(uploadListing.getNextUploadIdMarker());
          request.setKeyMarker(uploadListing.getNextKeyMarker());
          uploadListing = obs.listMultipartUploads(request);
        } while (uploadListing.isTruncated());
      } catch (ObsException e) {
        if (e.getResponseCode() == 403) {
          instrumentation.errorIgnored();
          LOG.debug("Failed to purging multipart uploads against {}," +
              " FS may be read only", bucket, e);
        } else {
          throw translateException("purging multipart uploads", bucket, e);
        }
      }
    }
  }

  /**
   * Return the protocol scheme for the FileSystem.
   *
   * @return "obs"
   */
  @Override
  public String getScheme() {
    return "obs";
  }

  /**
   * Returns a URI whose scheme and authority identify this FileSystem.
   */
  @Override
  public URI getUri() {
    return uri;
  }

  @Override
  public int getDefaultPort() {
    return Constants.OBS_DEFAULT_PORT;
  }

  /**
   * Returns the OBS client used by this filesystem.
   * @return ObsClient
   */
  @VisibleForTesting
  public ObsClient getObsClient() {
    return obs;
  }

  /**
   * Returns the read ahead range value used by this filesystem
   * @return
   */

  @VisibleForTesting
  long getReadAheadRange() {
    return readAhead;
  }

  /**
   * Get the input policy for this FS instance.
   * @return the input policy
   */
  @InterfaceStability.Unstable
  public OBSInputPolicy getInputPolicy() {
    return inputPolicy;
  }

  /**
   * Demand create the directory allocator, then create a temporary file.
   * {@link LocalDirAllocator#createTmpFileForWrite(String, long, Configuration)}.
   *  @param pathStr prefix for the temporary file
   *  @param size the size of the file that is going to be written
   *  @param conf the Configuration object
   *  @return a unique temporary file
   *  @throws IOException IO problems
   */
  synchronized File createTmpFileForWrite(String pathStr, long size,
      Configuration conf) throws IOException {
    if (directoryAllocator == null) {
      String bufferDir = conf.get(BUFFER_DIR) != null
          ? BUFFER_DIR : "hadoop.tmp.dir";
      directoryAllocator = new LocalDirAllocator(bufferDir);
    }
    return directoryAllocator.createTmpFileForWrite(pathStr, size, conf);
  }

  /**
   * Get the bucket of this filesystem.
   * @return the bucket
   */
  public String getBucket() {
    return bucket;
  }

  /**
   * Change the input policy for this FS.
   * @param inputPolicy new policy
   */
  @InterfaceStability.Unstable
  public void setInputPolicy(OBSInputPolicy inputPolicy) {
    Objects.requireNonNull(inputPolicy, "Null inputStrategy");
    LOG.debug("Setting input strategy: {}", inputPolicy);
    this.inputPolicy = inputPolicy;
  }

  /**
   * Turns a path (relative or otherwise) into an OBS key.
   *
   * @param path input path, may be relative to the working dir
   * @return a key excluding the leading "/", or, if it is the root path, ""
   */
  private String pathToKey(Path path) {
    if (!path.isAbsolute()) {
      path = new Path(workingDir, path);
    }

    if (path.toUri().getScheme() != null && path.toUri().getPath().isEmpty()) {
      return "";
    }

    return path.toUri().getPath().substring(1);
  }

  /**
   * Turns a path (relative or otherwise) into an OBS key, adding a trailing
   * "/" if the path is not the root <i>and</i> does not already have a "/"
   * at the end.
   *
   * @param key obs key or ""
   * @return the with a trailing "/", or, if it is the root key, "",
   */
  private String maybeAddTrailingSlash(String key) {
    if (!key.isEmpty() && !key.endsWith("/")) {
      return key + '/';
    } else {
      return key;
    }
  }

  /**
   * Convert a path back to a key.
   * @param key input key
   * @return the path from this key
   */
  private Path keyToPath(String key) {
    return new Path("/" + key);
  }

  /**
   * Convert a key to a fully qualified path.
   * @param key input key
   * @return the fully qualified path including URI scheme and bucket name.
   */
  Path keyToQualifiedPath(String key) {
    return qualify(keyToPath(key));
  }

  /**
   * Qualify a path.
   * @param path path to qualify
   * @return a qualified path.
   */
  Path qualify(Path path) {
    return path.makeQualified(uri, workingDir);
  }

  /**
   * Check that a Path belongs to this FileSystem.
   * Unlike the superclass, this version does not look at authority,
   * only hostnames.
   * @param path to check
   * @throws IllegalArgumentException if there is an FS mismatch
   */
  @Override
  public void checkPath(Path path) {
    OBSLoginHelper.checkPath(getConf(), getUri(), path, getDefaultPort());
  }

  @Override
  protected URI canonicalizeUri(URI rawUri) {
    return OBSLoginHelper.canonicalizeUri(rawUri, getDefaultPort());
  }

  /**
   * Opens an FSDataInputStream at the indicated Path.
   * @param f the file name to open
   * @param bufferSize the size of the buffer to be used.
   */
  public FSDataInputStream open(Path f, int bufferSize)
      throws IOException {
    LOG.debug("Opening '{}' for reading.", f);
    final FileStatus fileStatus = getFileStatus(f);
    if (fileStatus.isDirectory()) {
      throw new FileNotFoundException("Can't open " + f
          + " because it is a directory");
    }

    if (readaheadInputStreamEnabled){
      return new FSDataInputStream(new OBSReadaheadInputStream(bucket, pathToKey(f),
              fileStatus.getLen(), obs, statistics, instrumentation, readAhead,
              inputPolicy, unboundedReadThreadPool, bufferPartSize, bufferMaxRange));
    }

    return new FSDataInputStream(new OBSInputStream(bucket, pathToKey(f),
      fileStatus.getLen(), obs, statistics, instrumentation, readAhead,
        inputPolicy));
  }

  /**
   * Create an FSDataOutputStream at the indicated Path with write-progress
   * reporting.
   * @param f the file name to open
   * @param permission the permission to set.
   * @param overwrite if a file with this name already exists, then if true,
   *   the file will be overwritten, and if false an error will be thrown.
   * @param bufferSize the size of the buffer to be used.
   * @param replication required block replication for the file.
   * @param blockSize the requested block size.
   * @param progress the progress reporter.
   * @throws IOException in the event of IO related errors.
   * @see #setPermission(Path, FsPermission)
   */
  @Override
  @SuppressWarnings("IOResourceOpenedButNotSafelyClosed")
  public FSDataOutputStream create(Path f, FsPermission permission,
      boolean overwrite, int bufferSize, short replication, long blockSize,
      Progressable progress) throws IOException {
    long startTime = System.nanoTime();

    String key = pathToKey(f);
    OBSFileStatus status = null;
    try {
      // get the status or throw an FNFE
      status = getFileStatus(f);

      // if the thread reaches here, there is something at the path
      if (status.isDirectory()) {
        // path references a directory: automatic error
        throw new FileAlreadyExistsException(f + " is a directory");
      }
      if (!overwrite) {
        // path references a file and overwrite is disabled
        throw new FileAlreadyExistsException(f + " already exists");
      }
      LOG.debug("create: Overwriting file {}", f);
    } catch (FileNotFoundException e) {
      // this means the file is not found
      LOG.debug("create: Creating new file {}", f);
    }
    instrumentation.filesCreatedTotal(1);
    FSDataOutputStream output;
    if (blockUploadEnabled) {
      output = new FSDataOutputStream(
          new OBSBlockOutputStream(this,
              key,
              new SemaphoredDelegatingExecutor(boundedThreadPool,
                  blockOutputActiveBlocks, true),
              progress,
              partSize,
              blockFactory,
              instrumentation.newOutputStreamStatistics(statistics),
              new OBSWriteOperationHelper(key)
          ),
          null);
    } else {

      // We pass null to FSDataOutputStream so it won't count writes that
      // are being buffered to a file
      output = new FSDataOutputStream(
              new OBSOutputStream(getConf(),
                      this,
                      key,
                      progress
              ),
              null);
    }

    long delay = System.nanoTime() - startTime;
    instrumentation.filesCreated(1, delay);
    return output;
  }

  /**
   * {@inheritDoc}
   * @throws FileNotFoundException if the parent directory is not present -or
   * is not a directory.
   */
  @Override
  public FSDataOutputStream createNonRecursive(Path path,
      FsPermission permission,
      EnumSet<CreateFlag> flags,
      int bufferSize,
      short replication,
      long blockSize,
      Progressable progress) throws IOException {
    Path parent = path.getParent();
    if (parent != null) {
      // expect this to raise an exception if there is no parent
      if (!getFileStatus(parent).isDirectory()) {
        throw new FileAlreadyExistsException("Not a directory: " + parent);
      }
    }
    return create(path, permission,
        flags.contains(CreateFlag.OVERWRITE), bufferSize,
        replication, blockSize, progress);
  }

  /**
   * Append to an existing file (optional operation).
   * @param f the existing file to be appended.
   * @param bufferSize the size of the buffer to be used.
   * @param progress for reporting progress if it is not null.
   * @throws IOException indicating that append is not supported.
   */
  public FSDataOutputStream append(Path f, int bufferSize,
      Progressable progress) throws IOException {
    throw new IOException("Not supported");
  }


  /**
   * Renames Path src to Path dst.  Can take place on local fs
   * or remote DFS.
   *
   * Warning: OBS does not support renames. This method does a copy which can
   * take OBS some time to execute with large files and directories. Since
   * there is no Progressable passed in, this can time out jobs.
   *
   * Note: This implementation differs with other OBS drivers. Specifically:
   * <pre>
   *       Fails if src is a file and dst is a directory.
   *       Fails if src is a directory and dst is a file.
   *       Fails if the parent of dst does not exist or is a file.
   *       Fails if dst is a directory that is not empty.
   * </pre>
   *
   * @param src path to be renamed
   * @param dst new path after rename
   * @throws IOException on IO failure
   * @return true if rename is successful
   */
  public boolean rename(Path src, Path dst) throws IOException {
    try {
      return innerRename(src, dst);
    } catch (ObsException e) {
      throw translateException("rename(" + src +", " + dst + ")", src, e);
    } catch (RenameFailedException e) {
      LOG.error(e.getMessage());
      return e.getExitCode();
    } catch (FileNotFoundException e) {
      LOG.error(e.toString());
      return false;
    }
  }

  /**
   * The inner rename operation. See {@link #rename(Path, Path)} for
   * the description of the operation.
   * This operation throws an exception on any failure which needs to be
   * reported and downgraded to a failure. That is: if a rename
   * @param src path to be renamed
   * @param dst new path after rename
   * @throws RenameFailedException if some criteria for a state changing
   * rename was not met. This means work didn't happen; it's not something
   * which is reported upstream to the FileSystem APIs, for which the semantics
   * of "false" are pretty vague.
   * @throws FileNotFoundException there's no source file.
   * @throws IOException on IO failure.
   * @throws ObsException on failures inside the OBS SDK
   */
  private boolean innerRename(Path src, Path dst)
      throws RenameFailedException, FileNotFoundException, IOException,
        ObsException {
    LOG.debug("Rename path {} to {}", src, dst);
    incrementStatistic(INVOCATION_RENAME);

    String srcKey = pathToKey(src);
    String dstKey = pathToKey(dst);

    if (srcKey.isEmpty()) {
      throw new RenameFailedException(src, dst, "source is root directory");
    }
    if (dstKey.isEmpty()) {
      throw new RenameFailedException(src, dst, "dest is root directory");
    }

    // get the source file status; this raises a FNFE if there is no source
    // file.
    OBSFileStatus srcStatus = getFileStatus(src);

    if (srcKey.equals(dstKey)) {
      LOG.error("rename: src and dest refer to the same file or directory: {}",
          dst);
      throw new RenameFailedException(src, dst,
          "source and dest refer to the same file or directory")
          .withExitCode(srcStatus.isFile());
    }

    OBSFileStatus dstStatus = null;
    boolean dstFolderExisted = false;
    try {
      dstStatus = getFileStatus(dst);
      // if there is no destination entry, an exception is raised.
      // hence this code sequence can assume that there is something
      // at the end of the path; the only detail being what it is and
      // whether or not it can be the destination of the rename.
      if (srcStatus.isDirectory()) {
        if (dstStatus.isFile()) {
          throw new RenameFailedException(src, dst,
              "source is a directory and dest is a file")
              .withExitCode(srcStatus.isFile());
        } else if (!renameSupportEmptyDestinationFolder) {
          throw new RenameFailedException(src, dst,
              "destination is an existed directory")
              .withExitCode(false);
        } else if (!dstStatus.isEmptyDirectory()) {
          throw new RenameFailedException(src, dst,
              "Destination is a non-empty directory")
              .withExitCode(false);
        }
        dstFolderExisted = true;
        // at this point the destination is an empty directory
      } else {
        // source is a file. The destination must be a directory,
        // empty or not
        if (dstStatus.isFile()) {
          throw new RenameFailedException(src, dst,
              "Cannot rename onto an existing file")
              .withExitCode(false);
        }
        dstFolderExisted = true;
      }

    } catch (FileNotFoundException e) {
      LOG.debug("rename: destination path {} not found", dst);

      if (enablePosix && (!srcStatus.isDirectory()) && dstKey.endsWith("/")) {
        throw new RenameFailedException(src, dst,
                "source is a file but destination directory is not existed");
      }

      // Parent must exist
      Path parent = dst.getParent();
      if (!pathToKey(parent).isEmpty()) {
        try {
          OBSFileStatus dstParentStatus = getFileStatus(dst.getParent());
          if (!dstParentStatus.isDirectory()) {
            throw new RenameFailedException(src, dst,
                "destination parent is not a directory");
          }
        } catch (FileNotFoundException e2) {
          throw new RenameFailedException(src, dst,
              "destination has no parent ");
        }
      }
    }

    // Ok! Time to start
    if (srcStatus.isFile()) {
      LOG.debug("rename: renaming file {} to {}", src, dst);
      if (dstStatus != null && dstStatus.isDirectory()) {
        String newDstKey = dstKey;
        if (!newDstKey.endsWith("/")) {
          newDstKey = newDstKey + "/";
        }
        String filename =
            srcKey.substring(pathToKey(src.getParent()).length()+1);
        newDstKey = newDstKey + filename;
        dstKey = newDstKey;
      }

      renameFile(srcKey, dstKey, srcStatus);
    } else {
      LOG.debug("rename: renaming directory {} to {}", src, dst);

      // This is a directory to directory copy
      if (!dstKey.endsWith("/")) {
        dstKey = dstKey + "/";
      }

      if (!srcKey.endsWith("/")) {
        srcKey = srcKey + "/";
      }

      //Verify dest is not a child of the source directory
      if (dstKey.startsWith(srcKey)) {
        throw new RenameFailedException(srcKey, dstKey,
            "cannot rename a directory to a subdirectory o fitself ");
      }

      renameFolder(srcKey, dstKey, dstFolderExisted, dstStatus);
    }

    if (src.getParent() != dst.getParent()) {
      deleteUnnecessaryFakeDirectories(dst.getParent());
      createFakeDirectoryIfNecessary(src.getParent());
    }
    return true;
  }

  private void renameFile(String srcKey, String dstKey, OBSFileStatus srcStatus)
          throws IOException {
    instrumentation.frontendFilesRenamedTotal(1);
    long startTime = System.nanoTime();

    if (enablePosix) {
      fsRenameFile(srcKey, dstKey);
    } else {
      copyFile(srcKey, dstKey, srcStatus.getLen());
      innerDelete(srcStatus, false);
    }

    long delay = System.nanoTime() - startTime;
    instrumentation.frontendFilesRenamed(1, delay);
    instrumentation.filesRenamed(1, delay);
    LOG.debug("OBSFileSystem rename: "
            + instrumentation.frontendFilesRenamedToString()
            + ", {src=" + srcKey  + ", dst=" + dstKey + ", delay=" + delay + "}");
  }

  private void renameFolder(String srcKey, String dstKey,
                            boolean dstFolderExisted, OBSFileStatus dstStatus)
          throws IOException {
    instrumentation.frontendDirectoriesRenamedTotal(dstFolderExisted, 1);
    long startTime = System.nanoTime();

    if (enablePosix) {
      fsRenameFolder(dstFolderExisted, srcKey, dstKey);
    } else {
      instrumentation.directoriesRenamedTotal(1);

      List<KeyAndVersion> keysToDelete = new ArrayList<>();
      if (dstStatus != null && dstStatus.isEmptyDirectory()) {
        // delete unnecessary fake directory.
        keysToDelete.add(new KeyAndVersion(dstKey));
      }

      long listStartTime = System.nanoTime();
      ListObjectsRequest request = new ListObjectsRequest();
      request.setBucketName(bucket);
      request.setPrefix(srcKey);
      request.setMaxKeys(maxKeys);

      ObjectListing objects = listObjects(request);
      instrumentation.listObjectsInRename(System.nanoTime() - listStartTime);

      List<ListenableFuture<DeleteObjectsResult>> deletefutures = new LinkedList<>();
      List<ListenableFuture<CopyObjectResult>> copyfutures= new LinkedList<>();
      while (true) {
        for (ObsObject summary : objects.getObjects()) {
          keysToDelete.add(
                  new KeyAndVersion(summary.getObjectKey()));
          String newDstKey =
                  dstKey + summary.getObjectKey().substring(srcKey.length());
          //copyFile(summary.getObjectKey(), newDstKey, summary.getMetadata().getContentLength());
          copyfutures.add(copyFileAsync(summary.getObjectKey(), newDstKey, summary.getMetadata().getContentLength()));

          if (keysToDelete.size() == MAX_ENTRIES_TO_DELETE) {
            waitAllCopyFinished(copyfutures);
            copyfutures.clear();
            deletefutures.add(removeKeysAsync(keysToDelete, true, false));
          }
        }

        if (objects.isTruncated()) {
          listStartTime = System.nanoTime();
          objects = continueListObjects(objects);
          instrumentation.listObjectsInRename(System.nanoTime() - listStartTime );
        } else {
          if (!keysToDelete.isEmpty()) {
            waitAllCopyFinished(copyfutures);
            copyfutures.clear();
            deletefutures.add(removeKeysAsync(keysToDelete, false, false));
          }
          break;
        }
      }
      try {
        Futures.allAsList(deletefutures).get();
      } catch (InterruptedException e) {
        LOG.warn("Interrupted while copying objects (delete)");
        throw new InterruptedIOException("Interrupted while copying objects (delete)");
      } catch (ExecutionException e) {
        if (e.getCause() instanceof ObsException){
          throw (ObsException)e.getCause();
        }
        if (e.getCause() instanceof IOException){
          throw (IOException)e.getCause();
        }
        throw new ObsException("unknown error while copying objects (delete)",e.getCause());
      }
    }

    long delay = System.nanoTime() - startTime;
    instrumentation.frontendDirectoriesRenamed(dstFolderExisted, delay);
    instrumentation.directoriesRenamed(1, delay);
    LOG.debug("OBSFileSystem rename: "
            + instrumentation.frontendDirectoriesRenamedToString()
            + ", {src=" + srcKey + ", dst=" + dstKey + ", delay=" + delay + "}");
  }

  private void waitAllCopyFinished(List<ListenableFuture<CopyObjectResult>> copyfutures) throws InterruptedIOException {
    try {
      Futures.allAsList(copyfutures).get();
    } catch (InterruptedException e) {
      LOG.warn("Interrupted while copying objects (copy)");
      //TODO Interrupted
      throw new InterruptedIOException("Interrupted while copying objects (copy)");
    } catch (ExecutionException e) {
      e.printStackTrace();
    }
  }

  // Used to rename a file.
  private void fsRenameFile(String src, String dst)
          throws IOException, ObsException {
    LOG.debug("RenameFile path {} to {}", src, dst);

    try {
      final RenameRequest renameObjectRequest = new RenameRequest();
      renameObjectRequest.setBucketName(bucket);
      renameObjectRequest.setObjectKey(src);
      renameObjectRequest.setNewObjectKey(dst);

      final Future<RenameResult> future = unboundedThreadPool.submit(new Callable<RenameResult>() {
        @Override
        public RenameResult call() throws ObsException {
          return obs.renameFile(renameObjectRequest);
        }
      });

      try {
        future.get();
        incrementWriteOperations();
      } catch (InterruptedException e) {
        throw new InterruptedIOException("Interrupted renaming " + src
                + " to " + dst + ", cancelling");
      } catch (ExecutionException e) {
        if (e.getCause() instanceof ObsException) {
          throw (ObsException) e.getCause();
        }
        throw new ObsException("obs exception: ", e.getCause());
      }
    } catch (ObsException e) {
      throw translateException("renameFile(" + src + ", " + dst + ")",
              src, e);
    }
  }

  // Used to rename a source folder to a destination folder that is not existed before rename.
  private void fsRenameToNewFolder(String src, String dst)
          throws IOException, ObsException {
    LOG.debug("RenameFolder path {} to {}", src, dst);

    try {
      final RenameRequest renameObjectRequest = new RenameRequest();
      renameObjectRequest.setBucketName(bucket);
      renameObjectRequest.setObjectKey(src);
      renameObjectRequest.setNewObjectKey(dst);

      final ListenableFuture<RenameResult> future = boundedThreadPool.submit(new Callable<RenameResult>() {
        @Override
        public RenameResult call() throws ObsException {
          return obs.renameFolder(renameObjectRequest);
        }
      });

      try {
        future.get();
        incrementWriteOperations();
      } catch (InterruptedException e) {
        throw new InterruptedIOException("Interrupted renaming " + src
                + " to " + dst + ", cancelling");
      } catch (ExecutionException e) {
        if (e.getCause() instanceof ObsException) {
          throw (ObsException) e.getCause();
        }
        throw new ObsException("obs exception: ", e.getCause());
      }
    } catch (ObsException e) {
      throw translateException("renameFile(" + src + ", " + dst + ")",
              src, e);
    }
  }

  // Used to rename all sub objects of a source folder to an existed destination folder.
  private void fsRenameAllSubObjectsToOldFolder(String srcKey, String dstKey)
          throws IOException {
    // Get the first batch of son objects.
    final int maxKeyNum = maxKeys;
    long listStartTime = System.nanoTime();
    ListObjectsRequest request = createListObjectsRequest(srcKey, "/", maxKeyNum);
    ObjectListing objects = listObjects(request);
    instrumentation.listObjectsInRename(System.nanoTime() - listStartTime);

    while(true) {
      // Rename sub files of current batch.
      for (ObsObject sonSrcObject : objects.getObjects()) {
        String sonSrcKey = sonSrcObject.getObjectKey();
        String sonDstKey = dstKey + sonSrcKey.substring(srcKey.length());
        if (sonSrcKey.equals(srcKey)) {
          continue;
        }
        fsRenameToNewObject(sonSrcKey, sonDstKey);
      }

      // Recursively delete sub folders of current batch.
      for (String sonSrcKey : objects.getCommonPrefixes()) {
        String sonDstKey = dstKey + sonSrcKey.substring(srcKey.length());
        if (sonSrcKey.equals(srcKey)) {
          continue;
        }
        fsRenameToNewObject(sonSrcKey, sonDstKey);
      }

      // Get the next batch of sub objects.
      if (!objects.isTruncated()) {
        // There is no sub object remaining.
        break;
      }
      listStartTime = System.nanoTime();
      objects = continueListObjects(objects);
      instrumentation.listObjectsInRename(System.nanoTime() - listStartTime);
    }
  }

  // Used to rename a source object to a destination object which is not existed before rename.
  private void fsRenameToNewObject(String srcKey, String dstKey)
          throws IOException {
    /*
    Path srcPath = new Path(getUri().toString() + "/" + srcKey);
    Path dstPath = new Path(getUri().toString() + "/" + dstKey);
    rename(srcPath, dstPath);
    */
    if (srcKey.endsWith("/")) {
      // Rename folder.
      fsRenameToNewFolder(srcKey, dstKey);
    } else {
      // Rename file.
      fsRenameFile(srcKey, dstKey);
    }
  }

  // Used to :
  // 1) rename a source folder to a destination folder which is not existed before rename,
  // 2) rename all sub objects of a source folder to a destination folder which is already existed before rename and
  //    delete the source folder at last.
  // Comment: Both the source folder and the destination folder should be end with '/'.
  private void fsRenameFolder(final boolean dstFolderIsExisted, String srcKey, String dstKey)
          throws IOException {
    LOG.debug("RenameFolder path {} to {}, dstFolderIsExisted={}", srcKey, dstKey, dstFolderIsExisted);

    if (!dstFolderIsExisted) {
      // Rename the source folder to the destination folder that is not existed.
      fsRenameToNewFolder(srcKey, dstKey);
    } else {
      // Rename all sub objects of the source folder to the destination folder.
      fsRenameAllSubObjectsToOldFolder(srcKey, dstKey);
      // Delete the source folder which has become empty.
      deleteObject(srcKey, true);
    }
  }

  /**
   * Low-level call to get at the object metadata.
   * @param path path to the object
   * @return metadata
   * @throws IOException IO and object access problems.
   */
  @VisibleForTesting
  public ObjectMetadata getObjectMetadata(Path path) throws IOException {
    return getObjectMetadata(pathToKey(path));
  }

  /**
   * Increment a statistic by 1.
   * @param statistic The operation to increment
   */
  protected void incrementStatistic(Statistic statistic) {
    incrementStatistic(statistic, 1);
  }

  /**
   * Increment a statistic by a specific value.
   * @param statistic The operation to increment
   * @param count the count to increment
   */
  protected void incrementStatistic(Statistic statistic, long count) {
    instrumentation.incrementCounter(statistic, count);
    storageStatistics.incrementCounter(statistic, count);
  }

  /**
   * Decrement a gauge by a specific value.
   * @param statistic The operation to decrement
   * @param count the count to decrement
   */
  protected void decrementGauge(Statistic statistic, long count) {
    instrumentation.decrementGauge(statistic, count);
  }

  /**
   * Increment a gauge by a specific value.
   * @param statistic The operation to increment
   * @param count the count to increment
   */
  protected void incrementGauge(Statistic statistic, long count) {
    instrumentation.incrementGauge(statistic, count);
  }

  /**
   * Get the storage statistics of this filesystem.
   * @return the storage statistics
   */
  @Override
  public OBSStorageStatistics getStorageStatistics() {
    return storageStatistics;
  }

  /**
   * Request object metadata; increments counters in the process.
   * @param key key
   * @return the metadata
   */
  protected ObjectMetadata getObjectMetadata(String key) {
    incrementStatistic(OBJECT_METADATA_REQUESTS);
    ObjectMetadata meta = obs.getObjectMetadata(bucket, key);
    incrementReadOperations();
    return meta;
  }

  /**
   * Initiate a {@code listObjects} operation, incrementing metrics
   * in the process.
   * @param request request to initiate
   * @return the results
   */
  protected ObjectListing listObjects(ListObjectsRequest request) {
    incrementStatistic(OBJECT_LIST_REQUESTS);
    incrementReadOperations();
    return obs.listObjects(request);

  }

  /**
   * List the next set of objects.
   * @param objects paged result
   * @return the next result object
   */
  protected ObjectListing continueListObjects(ObjectListing objects)
  {
    String delimiter = objects.getDelimiter();
    int maxKeyNum = objects.getMaxKeys();
    incrementStatistic(OBJECT_CONTINUE_LIST_REQUESTS);
//    LOG.debug("delimiters: "+objects.getDelimiter());
    incrementReadOperations();
    ListObjectsRequest request=new ListObjectsRequest();
    request.setMarker(objects.getNextMarker());
    request.setBucketName(bucket);
    request.setPrefix(objects.getPrefix());
    if ((maxKeyNum > 0) && (maxKeyNum < maxKeys)) {
      request.setMaxKeys(maxKeyNum);
    } else {
      request.setMaxKeys(maxKeys);
    }
    if (delimiter != null) {
      request.setDelimiter(delimiter);
    }
    return obs.listObjects(request);
  }

  /**
   * Increment read operations.
   */
  public void incrementReadOperations() {
    statistics.incrementReadOps(1);
  }

  /**
   * Increment the write operation counter.
   * This is somewhat inaccurate, as it appears to be invoked more
   * often than needed in progress callbacks.
   */
  public void incrementWriteOperations() {
    statistics.incrementWriteOps(1);
  }

  /**
   * Delete an object.
   * Increments the {@code OBJECT_DELETE_REQUESTS} and write
   * operation statistics.
   * @param key key to blob to delete.
   */
  private void deleteObject(String key, boolean isFolder) throws InvalidRequestException {
    blockRootDelete(key);
    incrementWriteOperations();
    incrementStatistic(OBJECT_DELETE_REQUESTS);
    long startTime = System.nanoTime();
    obs.deleteObject(bucket, key);
    long delay = System.nanoTime() - startTime;
    if (isFolder) {
      instrumentation.directoriesDeleted(1, delay);
    } else {
      instrumentation.filesDeleted(1, delay);
    }
  }

  /**
   * Reject any request to delete an object where the key is root.
   * @param key key to validate
   * @throws InvalidRequestException if the request was rejected due to
   * a mistaken attempt to delete the root directory.
   */
  private void blockRootDelete(String key) throws InvalidRequestException {
    if (key.isEmpty() || "/".equals(key)) {
      throw new InvalidRequestException("Bucket "+ bucket
          +" cannot be deleted");
    }
  }

  /**
   * Perform a bulk object delete operation.
   * Increments the {@code OBJECT_DELETE_REQUESTS} and write
   * operation statistics.
   * @param deleteRequest keys to delete on the obs-backend
   */
  private void deleteObjects(DeleteObjectsRequest deleteRequest) {
    long startTime = System.nanoTime();
    incrementWriteOperations();
    incrementStatistic(OBJECT_DELETE_REQUESTS, 1);
    obs.deleteObjects(deleteRequest);
    long delay = System.nanoTime() - startTime;
    instrumentation.batchDeleted(1, delay);
  }

  /**
   * Create a putObject request.
   * Adds the ACL and metadata
   * @param key key of object
   * @param metadata metadata header
   * @param srcfile source file
   * @return the request
   */
  public PutObjectRequest newPutObjectRequest(String key,
      ObjectMetadata metadata, File srcfile) {
    Preconditions.checkNotNull(srcfile);
    PutObjectRequest putObjectRequest = new PutObjectRequest(bucket, key,
        srcfile);
    //TODO putObjectRequest.setCannedAcl(cannedACL);
    putObjectRequest.setMetadata(metadata);
    return putObjectRequest;
  }

  /**
   * Create a {@link PutObjectRequest} request.
   * The metadata is assumed to have been configured with the size of the
   * operation.
   * @param key key of object
   * @param metadata metadata header
   * @param inputStream source data.
   * @return the request
   */
  private PutObjectRequest newPutObjectRequest(String key,
      ObjectMetadata metadata, InputStream inputStream) {
    Preconditions.checkNotNull(inputStream);
    PutObjectRequest putObjectRequest = new PutObjectRequest(bucket, key,
        inputStream);
    putObjectRequest.setMetadata(metadata);
    //TODO putObjectRequest.setCannedAcl(cannedACL);
    return putObjectRequest;
  }

  /**
   * Create a new object metadata instance.
   * Any standard metadata headers are added here, for example:
   * encryption.
   * @return a new metadata instance
   */
  public ObjectMetadata newObjectMetadata() {
    final ObjectMetadata om = new ObjectMetadata();
    //TODO  KMS
    /*if (StringUtils.isNotBlank(serverSideEncryptionAlgorithm)) {
      om.setSSEAlgorithm(serverSideEncryptionAlgorithm);
    }*/
    return om;
  }

  /**
   * Create a new object metadata instance.
   * Any standard metadata headers are added here, for example:
   * encryption.
   *
   * @param length length of data to set in header.
   * @return a new metadata instance
   */
  public ObjectMetadata newObjectMetadata(long length) {
    final ObjectMetadata om = newObjectMetadata();
    if (length >= 0) {
      om.setContentLength(length);
    }

    return om;
  }

  /**
   * Start a transfer-manager managed async PUT of an object,
   * incrementing the put requests and put bytes
   * counters.
   * It does not update the other counters,
   * as existing code does that as progress callbacks come in.
   * Byte length is calculated from the file length, or, if there is no
   * file, from the content length of the header.
   * Because the operation is async, any stream supplied in the request
   * must reference data (files, buffers) which stay valid until the upload
   * completes.
   * @param putObjectRequest the request
   * @return the upload initiated
   */
  public Future putObject(final PutObjectRequest putObjectRequest) {
    long len;
    if (putObjectRequest.getFile() != null) {
      len = putObjectRequest.getFile().length();
    } else {
      len = putObjectRequest.getMetadata().getContentLength();
    }
    incrementPutStartStatistics(len);
    Future future=null;
    try {
      future=unboundedThreadPool.submit(new Callable<PutObjectResult>() {
        @Override
        public PutObjectResult call() throws ObsException {
          return obs.putObject(putObjectRequest);
        }
      });
      return future;
    } catch (ObsException e) {
      throw e;
    }
  }

  /**
   * PUT an object directly (i.e. not via the transfer manager).
   * Byte length is calculated from the file length, or, if there is no
   * file, from the content length of the header.
   * <i>Important: this call will close any input stream in the request.</i>
   * @param putObjectRequest the request
   * @return the upload initiated
   * @throws ObsException on problems
   */
  public PutObjectResult putObjectDirect(PutObjectRequest putObjectRequest)
      throws ObsException {
    long len;
    if (putObjectRequest.getFile() != null) {
      len = putObjectRequest.getFile().length();
    } else {
      len = putObjectRequest.getMetadata().getContentLength();
    }
    incrementPutStartStatistics(len);
    try {
      PutObjectResult result = obs.putObject(putObjectRequest);
      incrementPutCompletedStatistics(true, len);
      return result;
    } catch (ObsException e) {
      incrementPutCompletedStatistics(false, len);
      throw e;
    }
  }

  /**
   * Upload part of a multi-partition file.
   * Increments the write and put counters.
   * <i>Important: this call does not close any input stream in the request.</i>
   * @param request request
   * @return the result of the operation.
   * @throws ObsException on problems
   */
  public UploadPartResult uploadPart(UploadPartRequest request)
      throws ObsException {
    long len = request.getPartSize();
    incrementPutStartStatistics(len);
    try {
      UploadPartResult uploadPartResult = obs.uploadPart(request);
      incrementPutCompletedStatistics(true, len);
      return uploadPartResult;
    } catch (ObsException e) {
      incrementPutCompletedStatistics(false, len);
      throw e;
    }
  }

  /**
   * At the start of a put/multipart upload operation, update the
   * relevant counters.
   *
   * @param bytes bytes in the request.
   */
  public void incrementPutStartStatistics(long bytes) {
    LOG.debug("PUT start {} bytes", bytes);
    incrementWriteOperations();
    incrementStatistic(OBJECT_PUT_REQUESTS);
    incrementGauge(OBJECT_PUT_REQUESTS_ACTIVE, 1);
    if (bytes > 0) {
      incrementGauge(OBJECT_PUT_BYTES_PENDING, bytes);
    }
  }

  /**
   * At the end of a put/multipart upload operation, update the
   * relevant counters and gauges.
   *
   * @param success did the operation succeed?
   * @param bytes bytes in the request.
   */
  public void incrementPutCompletedStatistics(boolean success, long bytes) {
    LOG.debug("PUT completed success={}; {} bytes", success, bytes);
    incrementWriteOperations();
    if (bytes > 0) {
      incrementStatistic(OBJECT_PUT_BYTES, bytes);
      decrementGauge(OBJECT_PUT_BYTES_PENDING, bytes);
    }
    incrementStatistic(OBJECT_PUT_REQUESTS_COMPLETED);
    decrementGauge(OBJECT_PUT_REQUESTS_ACTIVE, 1);
  }

  /**
   * Callback for use in progress callbacks from put/multipart upload events.
   * Increments those statistics which are expected to be updated during
   * the ongoing upload operation.
   * @param key key to file that is being written (for logging)
   * @param bytes bytes successfully uploaded.
   */
  public void incrementPutProgressStatistics(String key, long bytes) {
    PROGRESS.debug("PUT {}: {} bytes", key, bytes);
    incrementWriteOperations();
    if (bytes > 0) {
      statistics.incrementBytesWritten(bytes);
    }
  }

  /**
   * A helper method to delete a list of keys on a obs-backend.
   *
   * @param keysToDelete collection of keys to delete on the obs-backend.
   *        if empty, no request is made of the object store.
   * @param clearKeys clears the keysToDelete-list after processing the list
   *            when set to true
   * @param deleteFakeDir indicates whether this is for deleting fake dirs
   * @throws InvalidRequestException if the request was rejected due to
   * a mistaken attempt to delete the root directory.
   */
  private ListenableFuture<DeleteObjectsResult> removeKeysAsync(List<KeyAndVersion> keysToDelete,
      boolean clearKeys, boolean deleteFakeDir)
      throws ObsException, InvalidRequestException {
    if (keysToDelete.isEmpty()) {
      // exit fast if there are no keys to delete
      Future future=SettableFuture.create();
      ((SettableFuture) future).set(null);
      return (ListenableFuture<DeleteObjectsResult>) future;
    }
    for (KeyAndVersion keyVersion : keysToDelete) {
      blockRootDelete(keyVersion.getKey());
    }
    ListenableFuture<DeleteObjectsResult> future;

    if (enableMultiObjectsDelete) {
      instrumentation.batchDeletedTotal(1);
      final DeleteObjectsRequest deleteObjectsRequest=new DeleteObjectsRequest(bucket);
      deleteObjectsRequest.setKeyAndVersions(keysToDelete.toArray(new KeyAndVersion[keysToDelete.size()]));
      future = boundedDeleteThreadPool.submit(new Callable<DeleteObjectsResult>() {
        @Override
        public DeleteObjectsResult call() throws Exception {
          deleteObjects(deleteObjectsRequest);
          return null;
        }
      });
    } else {
      //Copy
      final List<KeyAndVersion> keys=new ArrayList<>(keysToDelete);
      future = boundedDeleteThreadPool.submit(new Callable<DeleteObjectsResult>() {
          @Override
          public DeleteObjectsResult call() throws Exception {
            for (KeyAndVersion keyVersion : keys) {
              deleteObject(keyVersion.getKey(), keyVersion.getKey().endsWith("/"));
            }
            keys.clear();
            return null;
          }
        });
    }
    if (!deleteFakeDir) {
      instrumentation.filesDeletedTotal(keysToDelete.size());
    } else {
      instrumentation.fakeDirsDeletedTotal(keysToDelete.size());
    }
    if (clearKeys) {
      keysToDelete.clear();
    }
    return future;
  }

  // Remove all objects indicated by a leaf key list.
  private void removeKeys(List<KeyAndVersion> keysToDelete, boolean clearKeys)
          throws InvalidRequestException {
    removeKeys(keysToDelete, clearKeys, false);
  }
  private void removeKeys(List<KeyAndVersion> keysToDelete, boolean clearKeys, boolean checkRootDelete)
          throws InvalidRequestException {
    if (keysToDelete.isEmpty()) {
      // exit fast if there are no keys to delete
      return;
    }

    if (checkRootDelete) {
      for (KeyAndVersion keyVersion : keysToDelete) {
        blockRootDelete(keyVersion.getKey());
      }
    }

    if (!enableMultiObjectsDelete) {
      // delete one by one.
      for (KeyAndVersion keyVersion : keysToDelete) {
        deleteObject(keyVersion.getKey(), keyVersion.getKey().endsWith("/"));
      }
    } else if (keysToDelete.size() <= MAX_ENTRIES_TO_DELETE) {
      // Only one batch.
      DeleteObjectsRequest deleteObjectsRequest = new DeleteObjectsRequest(bucket);
      deleteObjectsRequest.setKeyAndVersions(keysToDelete.toArray(new KeyAndVersion[keysToDelete.size()]));
      deleteObjects(deleteObjectsRequest);
    } else {
      // Multi batches.
      List<KeyAndVersion> keys = new ArrayList<KeyAndVersion>(MAX_ENTRIES_TO_DELETE);
      for (KeyAndVersion key : keysToDelete) {
        keys.add(key);
        if (keys.size() == MAX_ENTRIES_TO_DELETE) {
          // Delete one batch.
          removeKeys(keys, true, false);
        }
      }
      // Delete the last batch
      removeKeys(keys, true, false);
    }

    if (clearKeys) {
      keysToDelete.clear();
    }
  }

  /**
   * Delete a Path. This operation is at least {@code O(files)}, with
   * added overheads to enumerate the path. It is also not atomic.
   *
   * @param f the path to delete.
   * @param recursive if path is a directory and set to
   * true, the directory is deleted else throws an exception. In
   * case of a file the recursive can be set to either true or false.
   * @return  true if delete is successful else false.
   * @throws IOException due to inability to delete a directory or file.
   */
  public boolean delete(Path f, boolean recursive) throws IOException {
    try {
      return innerDelete(getFileStatus(f), recursive);
    } catch (FileNotFoundException e) {
      LOG.debug("Couldn't delete {} - does not exist", f);
      instrumentation.errorIgnored();
      return false;
    } catch (ObsException e) {
      throw translateException("delete", f, e);
    }
  }

  /**
   * Delete an object. See {@link #delete(Path, boolean)}.
   *
   * @param status fileStatus object
   * @param recursive if path is a directory and set to
   * true, the directory is deleted else throws an exception. In
   * case of a file the recursive can be set to either true or false.
   * @return  true if delete is successful else false.
   * @throws IOException due to inability to delete a directory or file.
   * @throws ObsException on failures inside the OBS SDK
   */
  private boolean innerDelete(OBSFileStatus status, boolean recursive)
      throws IOException, ObsException {
    if (enablePosix) {
      return fsDelete(status, recursive);
    }

    Path f = status.getPath();
    LOG.debug("delete: path {} - recursive {}", f, recursive);

    String key = pathToKey(f);

    if (status.isDirectory()) {
      LOG.debug("delete: Path is a directory: {} - recursive {}", f, recursive);
      instrumentation.frontendDirectoryDeletedTotal(1);
      long startTime = System.nanoTime();

      if (!key.endsWith("/")) {
        key = key + "/";
      }

      if (key.equals("/")) {
        return rejectRootDirectoryDelete(status, recursive);
      }

      if (!recursive && !status.isEmptyDirectory()) {
        throw new PathIsNotEmptyDirectoryException(f.toString());
      }

      if (status.isEmptyDirectory()) {
        LOG.debug("delete: Deleting fake empty directory {} - recursive {}", f, recursive);
        deleteObject(key, true);
      } else {
        LOG.debug("delete: Deleting objects for directory prefix {} - recursive {}", f, recursive);

        String delimiter = recursive ? null : "/";
        ListObjectsRequest request = createListObjectsRequest(key, delimiter);

        ObjectListing objects = listObjects(request);
        List<KeyAndVersion> keys =
            new ArrayList<>(objects.getObjects().size());
        while (true) {
          for (ObsObject summary : objects.getObjects()) {
            keys.add(new KeyAndVersion(summary.getObjectKey()));
            LOG.debug("Got object to delete {}", summary.getObjectKey());

            if (keys.size() == MAX_ENTRIES_TO_DELETE) {
              removeKeys(keys, true, true);
            }
          }

          if (objects.isTruncated()) {
            objects = continueListObjects(objects);
          } else {
            if (!keys.isEmpty()) {
              removeKeys(keys, false, true);
            }
            break;
          }
        }
      }

      long delay = System.nanoTime() - startTime;
      instrumentation.frontendDirectoryDeleted(1, delay);
    } else {
      LOG.debug("delete: Path is a file");
      instrumentation.frontendFileDeletedTotal(1);
      long startTime = System.nanoTime();
      deleteObject(key, false);
      long delay = System.nanoTime() - startTime;
      instrumentation.frontendFileDeleted(1, delay);
    }

    Path parent = f.getParent();
    if (parent != null) {
      createFakeDirectoryIfNecessary(parent);
    }
    return true;
  }

  // Recursively delete a folder that might be not empty.
  private boolean fsDelete(OBSFileStatus status, boolean recursive)
          throws IOException, ObsException {
    Path f = status.getPath();
    LOG.debug("Delete path {} - recursive {}", f, recursive);

    String key = pathToKey(f);

    do {
      if (!status.isDirectory()) {
        LOG.debug("delete: Path is a file");
        instrumentation.frontendFileDeletedTotal(1);
        long startTime = System.nanoTime();
        deleteObject(key, false);
        long delay = System.nanoTime() - startTime;
        instrumentation.frontendFileDeleted(1, delay);
        break;
      }

      LOG.debug("delete: Path is a directory: {} - recursive {}", f, recursive);
      instrumentation.frontendDirectoryDeletedTotal(1);

      if (!key.endsWith("/")) {
        key = key + "/";
      }

      if (key.equals("/")) {
        return rejectRootDirectoryDelete(status, recursive);
      }

      if (!recursive && !status.isEmptyDirectory()) {
        LOG.warn("delete: Path is not empty: {} - recursive {}", f, recursive);
        throw new PathIsNotEmptyDirectoryException(f.toString());
      }

      if (status.isEmptyDirectory()) {
        LOG.debug("delete: Deleting fake empty directory {} - recursive {}", f, recursive);
        deleteObject(key, true);
        break;
      }

      LOG.debug("delete: Deleting objects for directory prefix {} to delete - recursive {}", f, recursive);
      if (enableMultiObjectsDeleteRecursion) {
        fsRecursivelyDelete(key, true);
      } else {
        fsNonRecursivelyDelete(f);
      }
    } while(false);

    Path parent = f.getParent();
    if (parent != null) {
      createFakeDirectoryIfNecessary(parent);
    }
    return true;
  }

  // List all sub objects at first, delete sub objects in batch secondly.
  private void fsNonRecursivelyDelete(Path parent)
          throws IOException, ObsException {
    // List sub objects sorted by path depth.
    FileStatus[] arFileStatus = innerListStatus(parent, true);
    // Remove sub objects one depth by one depth to avoid that parents and children in a same batch.
    fsRemoveKeys(arFileStatus);
    // Delete parent folder that should has become empty.
    deleteObject(pathToKey(parent), true);
  }

  // Remove sub objects of each depth one by one to avoid that parents and children in a same batch.
  private void fsRemoveKeys(FileStatus[] arFileStatus)
          throws ObsException, InvalidRequestException {
    if (arFileStatus.length <= 0) {
      // exit fast if there are no keys to delete
      return;
    }

    String key = "";
    for (int i = 0; i < arFileStatus.length; i++) {
      key = pathToKey(arFileStatus[i].getPath());
      blockRootDelete(key);
    }

    fsRemoveKeysByDepth(arFileStatus);
  }

  // Batch delete sub objects one depth by one depth to avoid that parents and children in a same batch.
  // A batch deletion might be split into some concurrent deletions to promote the performance, but it
  // can't make sure that an object is deleted before it's children.
  private void fsRemoveKeysByDepth(FileStatus[] arFileStatus)
          throws ObsException, InvalidRequestException {
    if (arFileStatus.length <= 0) {
      // exit fast if there is no keys to delete
      return;
    }

    // Find all leaf keys in the list.
    int filesNum = 0;
    int foldersNum = 0;
    String key = "";
    int depth = Integer.MAX_VALUE;
    List<KeyAndVersion> leafKeys = new ArrayList<KeyAndVersion>(MAX_ENTRIES_TO_DELETE);
    for (int idx = (arFileStatus.length-1); idx >= 0; idx--) {
      if (leafKeys.size() >= MAX_ENTRIES_TO_DELETE) {
        removeKeys(leafKeys, true);
      }

      key = pathToKey(arFileStatus[idx].getPath());

      // Check file.
      if (!arFileStatus[idx].isDirectory()) {
        // A file must be a leaf.
        leafKeys.add(new KeyAndVersion(key, null));
        filesNum++;
        continue;
      }

      // Check leaf folder at current depth.
      int keyDepth = fsGetObjectKeyDepth(key);
      if (keyDepth == depth) {
        // Any key at current depth must be a leaf.
        leafKeys.add(new KeyAndVersion(key, null));
        foldersNum++;
        continue;
      }
      if (keyDepth < depth) {
        // The last batch delete at current depth.
        removeKeys(leafKeys, true);
        // Go on at the upper depth.
        depth = keyDepth;
        leafKeys.add(new KeyAndVersion(key, null));
        foldersNum++;
        continue;
      }
      LOG.warn("The objects list is invalid because it isn't sorted by path depth.");
      throw new ObsException("System failure");
    }

    // The last batch delete at the minimum depth of all keys.
    removeKeys(leafKeys, true);
  }

  private int fsRemoveKeysByDepth(List<KeyAndVersion> keys)
          throws ObsException, InvalidRequestException {
    if (keys.size() <= 0) {
      // exit fast if there is no keys to delete
      return 0;
    }

    // Find all leaf keys in the list.
    int filesNum = 0;
    int foldersNum = 0;
    String key = "";
    int depth = Integer.MAX_VALUE;
    List<KeyAndVersion> leafKeys = new ArrayList<KeyAndVersion>(MAX_ENTRIES_TO_DELETE);
    for (int idx = (keys.size()-1); idx >= 0; idx--) {
      if (leafKeys.size() >= MAX_ENTRIES_TO_DELETE) {
        removeKeys(leafKeys, true);
      }

      key = keys.get(idx).getKey();

      // Check file.
      if (!key.endsWith("/")) {
        // A file must be a leaf.
        leafKeys.add(new KeyAndVersion(key, null));
        filesNum++;
        continue;
      }

      // Check leaf folder at current depth.
      int keyDepth = fsGetObjectKeyDepth(key);
      if (keyDepth == depth) {
        // Any key at current depth must be a leaf.
        leafKeys.add(new KeyAndVersion(key, null));
        foldersNum++;
        continue;
      }
      if (keyDepth < depth) {
        // The last batch delete at current depth.
        removeKeys(leafKeys, true);
        // Go on at the upper depth.
        depth = keyDepth;
        leafKeys.add(new KeyAndVersion(key, null));
        foldersNum++;
        continue;
      }
      LOG.warn("The objects list is invalid because it isn't sorted by path depth.");
      throw new ObsException("System failure");
    }

    // The last batch delete at the minimum depth of all keys.
    removeKeys(leafKeys, true);

    return filesNum + foldersNum;
  }

  // Get the depth of an absolute path, that is the number of '/' in the path.
  private static int fsGetObjectKeyDepth(String key) {
    int depth = 0;
    for (int idx = key.indexOf('/',0); idx >=0; idx = key.indexOf('/',idx+1)) {
      depth++;
    }
    return (key.endsWith("/") ? depth - 1 : depth);
  }

  // Recursively delete a folder that might be not empty.
  public int fsRecursivelyDelete(String parentKey, boolean deleteParent, int recursionScale)
          throws IOException {
    List<KeyAndVersion> folders = new ArrayList<KeyAndVersion>(MAX_ENTRIES_TO_DELETE);
    List<KeyAndVersion> files = new ArrayList<KeyAndVersion>(MAX_ENTRIES_TO_DELETE);

    // Get the first batch of son objects.
    ListObjectsRequest request = createListObjectsRequest(parentKey, null, maxKeys);
    ObjectListing objects = listObjects(request);
    int delNum = 0;

    while(true) {
      // Delete sub files of current batch.
      for (ObsObject sonObject : objects.getObjects()) {
        String sonObjectKey = sonObject.getObjectKey();

        if (sonObjectKey.length() == parentKey.length()) {
          // Ignore parent folder.
          continue;
        }

        // save or remove the son object.
        delNum += fsRemoveSonObject(sonObjectKey, files, folders);
      }

      if (folders.size() >= recursionScale) {
        // There are too many folders, need to recursively delete current deepest folders in list.
        // 1)Delete remaining files which may be sub objects of deepest folders or not.
        delNum += files.size();
        removeKeys(files, true);
        // 2)Extract deepest folders and recursively delete them.
        delNum += fsRecursivelyDeleteDeepest(folders);
      }

      // Get the next batch of sub objects.
      if (!objects.isTruncated()) {
        // There is no sub object remaining.
        break;
      }
      objects = continueListObjects(objects);
    }

    // Delete remaining files.
    delNum += files.size();
    removeKeys(files, true);

    // Delete remaining folders.
    delNum += fsRemoveKeysByDepth(folders);

    // Delete parent folder.
    if (deleteParent) {
      deleteObject(parentKey, true);
      delNum++;
    }

    return delNum;
  }

  public int fsRecursivelyDelete(String parentKey, boolean deleteParent)
          throws IOException {
    return fsRecursivelyDelete(parentKey, deleteParent, MAX_ENTRIES_TO_DELETE*2);
  }

  private int fsRemoveSonObject(String sonObjectKey, List<KeyAndVersion> files, List<KeyAndVersion> folders)
          throws IOException {
    if (!sonObjectKey.endsWith("/")) {
      // file
      return fsRemoveFile(sonObjectKey, files);
    } else {
      // folder
      folders.add(new KeyAndVersion(sonObjectKey));
      return 0;
    }
  }

  // Delete a file.
  private int fsRemoveFile(String sonObjectKey, List<KeyAndVersion> files)
          throws IOException {
    files.add(new KeyAndVersion(sonObjectKey));
    if (files.size() == MAX_ENTRIES_TO_DELETE) {
      // batch delete files.
      removeKeys(files, true);
      return MAX_ENTRIES_TO_DELETE;
    }
    return 0;
  }

  // Extract deepest folders from a folders list sorted by path depth.
  private List<KeyAndVersion> fsExtractDeepestFolders(List<KeyAndVersion> folders)
          throws ObsException {
    if (folders.isEmpty()) {
      return null;
    }

    // Find the index of the first deepest folder.
    int start = folders.size() - 1;
    KeyAndVersion folder = folders.get(start);
    int deepestDepth = fsGetObjectKeyDepth(folder.getKey());
    int currDepth = 0;
    for (start--; start >=0; start--) {
      folder = folders.get(start);
      currDepth = fsGetObjectKeyDepth(folder.getKey());
      if (currDepth == deepestDepth) {
        continue;
      } else  if (currDepth < deepestDepth) {
        // Found the first deepest folder.
        start++;
        break;
      } else {
        LOG.warn("The folders list is invalid because it isn't sorted by path depth.");
        throw new ObsException("System failure");
      }
    }
    if (start < 0) {
      // All folders in list is at the same depth.
      start = 0;
    }

    // Extract deepest folders.
    int deepestFoldersNum = folders.size() - start;
    List<KeyAndVersion> deepestFolders = new ArrayList<>(Math.min(folders.size(), deepestFoldersNum));
    for (int i = folders.size() - 1; i >= start; i--) {
      folder = folders.get(i);
      deepestFolders.add(folder);
      folders.remove(i);
    }

    return deepestFolders;
  }

  // Extract deepest folders from a list sorted by path depth and recursively delete them.
  private int fsRecursivelyDeleteDeepest(List<KeyAndVersion> folders)
          throws IOException {
    int delNum = 0;

    // Extract deepest folders.
    List<KeyAndVersion> deepestFolders = fsExtractDeepestFolders(folders);

    // Recursively delete sub objects of each deepest folder one by one.
    for (KeyAndVersion folder : deepestFolders) {
      delNum += fsRecursivelyDelete(folder.getKey(), false);
    }

    // Batch delete deepest folders.
    delNum += deepestFolders.size();
    removeKeys(deepestFolders, false);

    return delNum;
  }

  /**
   * Implements the specific logic to reject root directory deletion.
   * The caller must return the result of this call, rather than
   * attempt to continue with the delete operation: deleting root
   * directories is never allowed. This method simply implements
   * the policy of when to return an exit code versus raise an exception.
   * @param status filesystem status
   * @param recursive recursive flag from command
   * @return a return code for the operation
   * @throws PathIOException if the operation was explicitly rejected.
   */
  private boolean rejectRootDirectoryDelete(OBSFileStatus status,
      boolean recursive) throws IOException {
    LOG.info("obs delete the {} root directory of {}", bucket, recursive);
    boolean emptyRoot = status.isEmptyDirectory();
    if (emptyRoot) {
      return true;
    }
    if (recursive) {
      return false;
    } else {
      // reject
      throw new PathIOException(bucket, "Cannot delete root path");
    }
  }

  private void createFakeDirectoryIfNecessary(Path f)
      throws IOException, ObsException {
    if (enablePosix) {
      return;
    }

    String key = pathToKey(f);
    if (!key.isEmpty() && !exists(f)) {
      LOG.debug("Creating new fake directory at {}", f);
      createFakeDirectory(key);
    }
  }

  /**
   * List the statuses of the files/directories in the given path if the path is
   * a directory.
   *
   * @param f given path
   * @return the statuses of the files/directories in the given patch
   * @throws FileNotFoundException when the path does not exist;
   *         IOException see specific implementation
   */
  public FileStatus[] listStatus(Path f) throws FileNotFoundException,
      IOException {
    try {
      return innerListStatus(f, false);
    } catch (ObsException e) {
      throw translateException("listStatus", f, e);
    }
  }

  /**
   * List the statuses of the files/directories in the given path if the path is
   * a directory.
   *
   * @param f given path
   * @return the statuses of the files/directories in the given patch
   * @throws FileNotFoundException when the path does not exist;
   * @throws IOException due to an IO problem.
   * @throws ObsException on failures inside the OBS SDK
   */
  public FileStatus[] innerListStatus(Path f, boolean recursive) throws FileNotFoundException,
      IOException, ObsException {
    Path path = qualify(f);
    String key = pathToKey(path);
    LOG.debug("List status for path: {}", path);
    incrementStatistic(INVOCATION_LIST_STATUS);

    List<FileStatus> result;
    final FileStatus fileStatus = getFileStatus(path);

    if (fileStatus.isDirectory()) {
      if (!key.isEmpty()) {
        key = key + '/';
      }

      String delimiter = recursive ? null : "/";
      ListObjectsRequest request = createListObjectsRequest(key, delimiter);
      LOG.debug("listStatus: doing listObjects for directory {} - recursive {}", f, recursive);

      Listing.FileStatusListingIterator files =
          listing.createFileStatusListingIterator(path,
              request,
              ACCEPT_ALL,
              new Listing.AcceptAllButSelfAndS3nDirs(path));
      result = new ArrayList<>(files.getBatchSize());
      while (files.hasNext()) {
        result.add(files.next());
      }
      return result.toArray(new FileStatus[result.size()]);
    } else {
      LOG.debug("Adding: rd (not a dir): {}", path);
      FileStatus[] stats = new FileStatus[1];
      stats[0]= fileStatus;
      return stats;
    }
  }

  // Used to rename all sub objects of a source folder to an existed destination folder.
  public List<String> fsGetSubObjects(Path f)
          throws IOException {
    Path path = qualify(f);
    String parentKey = pathToKey(path);
    parentKey =(parentKey.endsWith("/") ? parentKey : (parentKey + "/"));
    List<String> keys = new ArrayList<String>(100);

    // Get the first batch of son objects.
    String delimiter = null;
    ListObjectsRequest request = createListObjectsRequest(parentKey, delimiter, maxKeys);
    ObjectListing objects = listObjects(request);

    while(true) {
      // Rename sub files of current batch.
      for (ObsObject son : objects.getObjects()) {
        String sonKey = son.getObjectKey();
        if (sonKey.equals(parentKey)) {
          continue;
        }
        keys.add(sonKey);
      }

      // Recursively delete sub folders of current batch.
      for (String sonKey : objects.getCommonPrefixes()) {
        if (sonKey.equals(parentKey)) {
          continue;
        }
        keys.add(sonKey);
      }

      // Get the next batch of sub objects.
      if (!objects.isTruncated()) {
        // There is no sub object remaining.
        break;
      }
      objects = continueListObjects(objects);
    }

    return keys;
  }

  /**
   * Create a {@code ListObjectsRequest} request against this bucket,
   * with the maximum keys returned in a query set by {@link #maxKeys}.
   * @param key key for request
   * @param delimiter any delimiter
   * @return the request
   */
  private ListObjectsRequest createListObjectsRequest(String key,
      String delimiter) {
    return createListObjectsRequest(key, delimiter, -1);
  }
  private ListObjectsRequest createListObjectsRequest(String key,
      String delimiter, int maxKeyNum) {
    ListObjectsRequest request = new ListObjectsRequest();
    request.setBucketName(bucket);
    if ((maxKeyNum > 0) && (maxKeyNum < maxKeys)) {
      request.setMaxKeys(maxKeyNum);
    } else {
      request.setMaxKeys(maxKeys);
    }
    request.setPrefix(key);
    if (delimiter != null) {
      request.setDelimiter(delimiter);
    }
    return request;
  }

  /**
   * Set the current working directory for the given file system. All relative
   * paths will be resolved relative to it.
   *
   * @param newDir the current working directory.
   */
  public void setWorkingDirectory(Path newDir) {
    workingDir = newDir;
  }

  /**
   * Get the current working directory for the given file system.
   * @return the directory pathname
   */
  public Path getWorkingDirectory() {
    return workingDir;
  }

  /**
   * Get the username of the FS.
   * @return the short name of the user who instantiated the FS
   */
  public String getUsername() {
    return username;
  }

  /**
   *
   * Make the given path and all non-existent parents into
   * directories. Has the semantics of Unix {@code 'mkdir -p'}.
   * Existence of the directory hierarchy is not an error.
   * @param path path to create
   * @param permission to apply to f
   * @return true if a directory was created
   * @throws FileAlreadyExistsException there is a file at the path specified
   * @throws IOException other IO problems
   */
  // TODO: If we have created an empty file at /foo/bar and we then call
  // mkdirs for /foo/bar/baz/roo what happens to the empty file /foo/bar/?
  public boolean mkdirs(Path path, FsPermission permission) throws IOException,
      FileAlreadyExistsException {
    try {
      return innerMkdirs(path, permission);
    } catch (ObsException e) {
      throw translateException("innerMkdirs", path, e);
    }
  }
  /**
   *
   * Make the given path and all non-existent parents into
   * directories.
   * See {@link #mkdirs(Path, FsPermission)}
   * @param f path to create
   * @param permission to apply to f
   * @return true if a directory was created
   * @throws FileAlreadyExistsException there is a file at the path specified
   * @throws IOException other IO problems
   * @throws ObsException on failures inside the OBS SDK
   */
  // TODO: If we have created an empty file at /foo/bar and we then call
  // mkdirs for /foo/bar/baz/roo what happens to the empty file /foo/bar/?
  private boolean innerMkdirs(Path f, FsPermission permission)
      throws IOException, FileAlreadyExistsException, ObsException {
    LOG.debug("Making directory: {}", f);
    incrementStatistic(INVOCATION_MKDIRS);
    FileStatus fileStatus;
    try {
      fileStatus = getFileStatus(f);

      if (fileStatus.isDirectory()) {
        return true;
      } else {
        throw new FileAlreadyExistsException("Path is a file: " + f);
      }
    } catch (FileNotFoundException e) {
      Path fPart = f.getParent();
      do {
        try {
          fileStatus = getFileStatus(fPart);
          if (fileStatus.isDirectory()) {
            break;
          }
          if (fileStatus.isFile()) {
            throw new FileAlreadyExistsException(String.format(
                "Can't make directory for path '%s' since it is a file.",
                fPart));
          }
        } catch (FileNotFoundException fnfe) {
          instrumentation.errorIgnored();
        }
        fPart = fPart.getParent();
      } while (fPart != null);

      String key = pathToKey(f);
      createFakeDirectory(key);
      return true;
    }
  }

  /**
   * Return a file status object that represents the path.
   * @param f The path we want information from
   * @return a FileStatus object
   * @throws java.io.FileNotFoundException when the path does not exist;
   * @throws IOException on other problems.
   */
  public OBSFileStatus getFileStatus(final Path f) throws IOException {
    if (enablePosix) {
      return fsGetObjectStatus(f);
    }

    incrementStatistic(INVOCATION_GET_FILE_STATUS);
    final Path path = qualify(f);
    String key = pathToKey(path);
    LOG.debug("Getting path status for {}  ({})", path , key);
    if (!key.isEmpty()) {
      try {
        ObjectMetadata meta = getObjectMetadata(key);

        if (objectRepresentsDirectory(key, meta.getContentLength())) {
          LOG.debug("Found exact file: fake directory");
          return new OBSFileStatus(true, path, username);
        } else {
          LOG.debug("Found exact file: normal file");
          return new OBSFileStatus(meta.getContentLength(),
              dateToLong(meta.getLastModified()),
              path,
              getDefaultBlockSize(path),
              username);
        }
      } catch (ObsException e) {
        if (e.getResponseCode() != 404) {
          throw translateException("getFileStatus", path, e);
        }
      }

      // Necessary?  TODO ???
      if (!key.endsWith("/")) {
        String newKey = key + "/";
        try {
          ObjectMetadata meta = getObjectMetadata(newKey);

          if (objectRepresentsDirectory(newKey, meta.getContentLength())) {
            LOG.debug("Found file (with /): fake directory");
            return new OBSFileStatus(true, path, username);
          } else {
            LOG.debug("Found file (with /): real file? should not happen: {}",
                key);

            return new OBSFileStatus(meta.getContentLength(),
                dateToLong(meta.getLastModified()),
                path,
                getDefaultBlockSize(path),
                username);
          }
        } catch (ObsException e) {
          if (e.getResponseCode() != 404) {
            throw translateException("getFileStatus", newKey, e);
          }
        }
      }
    }

    try {
      boolean isEmpty = isFolderEmpty(key);
      return new OBSFileStatus(isEmpty, path, username);
    } catch (ObsException e) {
      if (e.getResponseCode() != 404) {
        throw translateException("getFileStatus", key, e);
      }
    }

    LOG.error("Not Found: {}", path);
    throw new FileNotFoundException("No such file or directory: " + path);
  }

  // Used to check if a folder is empty or not by counting the number of sub objects in list.
  public boolean isFolderEmpty(String key, ObjectListing objects) {
    int count = objects.getObjects().size();
    if ((count >= 2)
            || ((count == 1) && (!objects.getObjects().get(0).getObjectKey().equals(key)))) {
      // There is a sub file at least.
      return false;
    }

    count = objects.getCommonPrefixes().size();
    if ((count >= 2)
            || ((count == 1) && (!objects.getCommonPrefixes().get(0).equals(key)))) {
      // There is a sub directory at least.
      return false;
    }

    // There is no sub object.
    return true;
  }

  // Used to check if a folder is empty or not.
  public boolean isFolderEmpty(String key)
          throws FileNotFoundException, ObsException {
    key = maybeAddTrailingSlash(key);
    ListObjectsRequest request = new ListObjectsRequest();
    request.setBucketName(bucket);
    request.setPrefix(key);
    request.setDelimiter("/");
    request.setMaxKeys(3);
    ObjectListing objects = listObjects(request);

    if (!objects.getCommonPrefixes().isEmpty()
            || !objects.getObjectSummaries().isEmpty()) {
      if (enablePosix && isFolderEmpty(key, objects)) {
        LOG.debug("Found empty directory {}", key);
        return true;
      }
      if (LOG.isDebugEnabled()) {
        LOG.debug("Found path as directory (with /): {}/{}",
                objects.getCommonPrefixes().size() ,
                objects.getObjects().size());

        for (ObsObject summary : objects.getObjects()) {
          LOG.debug("Summary: {} {}", summary.getObjectKey(), summary.getMetadata().getContentLength());
        }
        for (String prefix : objects.getCommonPrefixes()) {
          LOG.debug("Prefix: {}", prefix);
        }
      }
      LOG.debug("Found non-empty directory {}", key);
      return false;
    } else if (key.isEmpty()) {
      LOG.debug("Found root directory");
      return true;
    } else if (enablePosix) {
      LOG.debug("Found empty directory {}", key);
      return true;
    }

    LOG.debug("Not Found: {}", key);
    throw new FileNotFoundException("No such file or directory: " + key);
  }

  // Used to get the status of a file or folder in a file-gateway bucket.
  public OBSFileStatus fsGetObjectStatus(final Path f)
          throws FileNotFoundException, IOException {
    incrementStatistic(INVOCATION_GET_FILE_STATUS);
    final Path path = qualify(f);
    String key = pathToKey(path);
    LOG.debug("Getting path status for {}  ({})", path, key);

    if (key.isEmpty()) {
      LOG.debug("Found root directory");
      boolean isEmpty = isFolderEmpty(key);
      return new OBSFileStatus(isEmpty, path, username);
    }

    try {
      final GetAttributeRequest getAttrRequest = new GetAttributeRequest(bucket, key);
      ObsFSAttribute meta = obs.getAttribute(getAttrRequest);
      if (fsIsFolder(meta)) {
        LOG.debug("Found file (with /): fake directory");
        boolean isEmpty = isFolderEmpty(key);
        return new OBSFileStatus(isEmpty, path, username);
      } else {
        LOG.debug("Found file (with /): real file? should not happen: {}",
                key);
        return new OBSFileStatus(meta.getContentLength(),
                dateToLong(meta.getLastModified()),
                path,
                getDefaultBlockSize(path),
                username);
      }
    } catch (ObsException e) {
      if (e.getResponseCode() != 404) {
        throw translateException("getFileStatus", path, e);
      }
    }

    LOG.debug("Not Found: {}", path);
    throw new FileNotFoundException("No such file or directory: " + path);
  }

  // Used to judge that an object is a file or folder.
  public static boolean fsIsFolder(ObsFSAttribute attr) {
    final int S_IFDIR = 0040000;
    int mode = attr.getMode();
    return ((mode & S_IFDIR) != 0);
  }

  /**
   * The src file is on the local disk.  Add it to FS at
   * the given dst name.
   *
   * This version doesn't need to create a temporary file to calculate the md5.
   * Sadly this doesn't seem to be used by the shell cp :(
   *
   * delSrc indicates if the source should be removed
   * @param delSrc whether to delete the src
   * @param overwrite whether to overwrite an existing file
   * @param src path
   * @param dst path
   * @throws IOException IO problem
   * @throws FileAlreadyExistsException the destination file exists and
   * overwrite==false
   * @throws ObsException failure in the OBS SDK
   */
  @Override
  public void copyFromLocalFile(boolean delSrc, boolean overwrite, Path src,
      Path dst) throws IOException {
    try {
      innerCopyFromLocalFile(delSrc, overwrite, src, dst);
    } catch (ObsException e) {
      throw translateException("copyFromLocalFile(" + src + ", " + dst + ")",
          src, e);
    }
  }

  /**
   * The src file is on the local disk.  Add it to FS at
   * the given dst name.
   *
   * This version doesn't need to create a temporary file to calculate the md5.
   * Sadly this doesn't seem to be used by the shell cp :(
   *
   * delSrc indicates if the source should be removed
   * @param delSrc whether to delete the src
   * @param overwrite whether to overwrite an existing file
   * @param src path
   * @param dst path
   * @throws IOException IO problem
   * @throws FileAlreadyExistsException the destination file exists and
   * overwrite==false
   * @throws ObsException failure in the OBS SDK
   */
  private void innerCopyFromLocalFile(boolean delSrc, boolean overwrite,
      Path src, Path dst)
      throws IOException, FileAlreadyExistsException, ObsException {
    incrementStatistic(INVOCATION_COPY_FROM_LOCAL_FILE);
    final String key = pathToKey(dst);

    if (!overwrite && exists(dst)) {
      throw new FileAlreadyExistsException(dst + " already exists");
    }
    LOG.debug("Copying local file from {} to {}", src, dst);

    // Since we have a local file, we don't need to stream into a temporary file
    LocalFileSystem local = getLocal(getConf());
    File srcfile = local.pathToFile(src);

    final ObjectMetadata om = newObjectMetadata(srcfile.length());
    PutObjectRequest putObjectRequest = newPutObjectRequest(key, om, srcfile);
    Future future = putObject(putObjectRequest);
   /*TODO listener ProgressableProgressListener listener = new ProgressableProgressListener(
        this, key, up, null);
    up.addProgressListener(listener);*/
    try {
      future.get();
      incrementPutCompletedStatistics(true, srcfile.length());
    } catch (InterruptedException e) {
      incrementPutCompletedStatistics(false, srcfile.length());
      throw new InterruptedIOException("Interrupted copying " + src
          + " to "  + dst + ", cancelling");
    } catch (ExecutionException e) {
      incrementPutCompletedStatistics(false, srcfile.length());
      e.printStackTrace();
    }
    //TODO listener.uploadCompleted();

    // This will delete unnecessary fake parent directories
    finishedWrite(key);

    if (delSrc) {
      local.delete(src, false);
    }
  }

  /**
   * Close the filesystem. This shuts down all transfers.
   * @throws IOException IO problem
   */
  @Override
  public void close() throws IOException {
    if (closed.getAndSet(true)) {
      // already closed
      return;
    }
    
    super.close();
  }

  /**
   * Override getCanonicalServiceName because we don't support token in obs.
   */
  @Override
  public String getCanonicalServiceName() {
    // Does not support Token
    return null;
  }

  private ListenableFuture<CopyObjectResult> copyFileAsync(final String srcKey, final String dstKey, final long size)
  {
    return boundedCopyThreadPool.submit(new Callable<CopyObjectResult>() {
      @Override
      public CopyObjectResult call() throws Exception {
        copyFile(srcKey, dstKey, size);
        return null;
      }
    });
  }

  /**
   * Copy a single object in the bucket via a COPY operation.
   * @param srcKey source object path
   * @param dstKey destination object path
   * @param size object size
   * @throws ObsException on failures inside the OBS SDK
   * @throws InterruptedIOException the operation was interrupted
   * @throws IOException Other IO problems
   */
    private void copyFile(final String srcKey, final String dstKey, final long size)
            throws IOException, InterruptedIOException, ObsException
    {
        long startTime = System.nanoTime();

        if (LOG.isDebugEnabled())
        {
            LOG.debug("copyFile {} -> {} ", srcKey, dstKey);
        }
        
        try
        {
            // 100MB per part
            long objectSize = size;
            if (objectSize > copyPartSize)
            {
                // initial copy part task
                InitiateMultipartUploadRequest request = new InitiateMultipartUploadRequest(bucket, dstKey);
                InitiateMultipartUploadResult result = obs.initiateMultipartUpload(request);
                
                final String uploadId = result.getUploadId();
                if (LOG.isDebugEnabled())
                {
                    LOG.debug("Multipart copy file, uploadId: {}", uploadId);
                }
                // count the parts
                long partCount = objectSize % copyPartSize == 0 ? objectSize / copyPartSize
                        : objectSize / copyPartSize + 1;

                final List<PartEtag> partEtags = Collections.synchronizedList(new ArrayList<PartEtag>());
                final List<ListenableFuture<?>> partCopyFutures = new ArrayList<ListenableFuture<?>>();
                for (int i = 0; i < partCount; i++)
                {
                    final long rangeStart = i * copyPartSize;
                    final long rangeEnd = (i + 1 == partCount) ? objectSize - 1 : rangeStart + copyPartSize - 1;
                    final int partNumber = i + 1;
                    partCopyFutures.add(boundedCopyPartThreadPool.submit(new Runnable()
                    {
                        
                        @Override
                        public void run()
                        {
                            CopyPartRequest request = new CopyPartRequest();
                            request.setUploadId(uploadId);
                            request.setSourceBucketName(bucket);
                            request.setSourceObjectKey(srcKey);
                            request.setDestinationBucketName(bucket);
                            request.setDestinationObjectKey(dstKey);
                            request.setByteRangeStart(rangeStart);
                            request.setByteRangeEnd(rangeEnd);
                            request.setPartNumber(partNumber);
                            
                            try
                            {
                                CopyPartResult result = obs.copyPart(request);
                                partEtags.add(new PartEtag(result.getEtag(), result.getPartNumber()));
                                if (LOG.isDebugEnabled())
                                {
                                    LOG.debug("Multipart copy file, uploadId: {}, Part#{} done.", uploadId, partNumber);
                                }
                            }
                            catch (ObsException e)
                            {
                                LOG.error("Multipart copy file exception.", e);
                            }
                        }
                    }));
                }
                
                // wait the tasks for completing
                try
                {
                    Futures.allAsList(partCopyFutures).get();
                }
                catch (InterruptedException e)
                {
                    LOG.warn("Interrupted while copying objects (copy)");
                    throw new InterruptedIOException("Interrupted while copying objects (copy)");
                }
                catch (ExecutionException e)
                {
                    LOG.error("Multipart copy file exception.", e);
                }
                
                if (partEtags.size() != partCount)
                {
                    LOG.error("partEtags({}) is not equals partCount({}).", partEtags.size(), partCount);
                    throw new IllegalStateException("Upload multiparts fail due to some parts are not finished yet");
                }
                
                // Make part numbers in ascending order
                Collections.sort(partEtags, new Comparator<PartEtag>()
                {
                    @Override
                    public int compare(PartEtag o1, PartEtag o2)
                    {
                        return o1.getPartNumber() - o2.getPartNumber();
                    }
                });
                
                // merge the copy parts
                CompleteMultipartUploadRequest completeMultipartUploadRequest = new CompleteMultipartUploadRequest(
                        bucket, dstKey, uploadId, partEtags);
                obs.completeMultipartUpload(completeMultipartUploadRequest);
            }
            else
            {
                ObjectMetadata srcom = getObjectMetadata(srcKey);
                ObjectMetadata dstom = cloneObjectMetadata(srcom);
                /*
                if (StringUtils.isNotBlank(serverSideEncryptionAlgorithm)) {
                  dstom.setSSEAlgorithm(serverSideEncryptionAlgorithm);
                }
                */
                final CopyObjectRequest copyObjectRequest = new CopyObjectRequest(bucket, srcKey, bucket, dstKey);
                // TODO copyObjectRequest.setCannedAccessControlList(cannedACL);
                copyObjectRequest.setNewObjectMetadata(dstom);
                obs.copyObject(copyObjectRequest);
            }
            
            incrementWriteOperations();
        }
        catch (ObsException e)
        {
            throw translateException("copyFile("+ srcKey+ ", " + dstKey + ")", srcKey, e);
        }

        long delay = System.nanoTime() - startTime;
        instrumentation.filesCopied(1, delay, size);
    }

  /**
   * Perform post-write actions.
   * @param key key written to
   */
  public void finishedWrite(String key) {
    LOG.debug("Finished write to {}", key);
    deleteUnnecessaryFakeDirectories(keyToPath(key).getParent());
  }

  /**
   * Delete mock parent directories which are no longer needed.
   * This code swallows IO exceptions encountered
   * @param path path
   */
  private void deleteUnnecessaryFakeDirectories(Path path) {
    if (enablePosix) {
      return;
    }

    List<KeyAndVersion> keysToRemove = new ArrayList<>();
    while (!path.isRoot()) {
      String key = pathToKey(path);
      key = (key.endsWith("/")) ? key : (key + "/");
      keysToRemove.add(new KeyAndVersion(key));
      path = path.getParent();
    }

    try {
      removeKeys(keysToRemove, false);
    } catch (  ObsException | InvalidRequestException e ) {
      instrumentation.errorIgnored();
      if (LOG.isDebugEnabled()) {
        StringBuilder sb = new StringBuilder();
        for(KeyAndVersion kv : keysToRemove) {
          sb.append(kv.getKey()).append(",");
        }
        LOG.debug("While deleting keys {} ", sb.toString(), e);
      }
    }
  }

  private void createFakeDirectory(final String objectName)
      throws ObsException,
      InterruptedIOException {
    if (enablePosix) {
      fsCreateFolder(objectName);
      return;
    }

    if (!objectName.endsWith("/")) {
      createEmptyObject(objectName + "/");
    } else {
      createEmptyObject(objectName);
    }
  }

  // Used to create an empty file that represents an empty directory
  private void createEmptyObject(final String objectName)
      throws ObsException,
      InterruptedIOException {
    final InputStream im = new InputStream() {
      @Override
      public int read() throws IOException {
        return -1;
      }
    };

    instrumentation.directoriesCreatedTotal(1);
    long startTime = System.nanoTime();

    PutObjectRequest putObjectRequest = newPutObjectRequest(objectName,
        newObjectMetadata(0L),
        im);
    Future upload = putObject(putObjectRequest);
    try {
      upload.get();
      incrementPutCompletedStatistics(true, 0);
    } catch (InterruptedException e) {
      incrementPutCompletedStatistics(false, 0);
      throw new InterruptedIOException("Interrupted creating " + objectName);
    } catch (ExecutionException e) {
      incrementPutCompletedStatistics(false, 0);
      if (e.getCause() instanceof ObsException){
        throw (ObsException)e.getCause();
      }
      throw new ObsException("obs exception: ",e.getCause());
    }
    incrementPutProgressStatistics(objectName, 0);

    long delay = System.nanoTime() - startTime;
    instrumentation.directoriesCreated(1, delay);
  }

  // Used to create a file
  private void fsCreateFile(final String objectName)
          throws ObsException, InterruptedIOException {
    instrumentation.filesCreatedTotal(1);
    long startTime = System.nanoTime();

    try {
      final NewFileRequest newFileRequest = new NewFileRequest(bucket, objectName);
      long len = newFileRequest.getObjectKey().length();
      incrementPutStartStatistics(len);
      ListenableFuture<ObsFSFile> future = null;
      try {
        future = boundedThreadPool.submit(new Callable<ObsFSFile>() {
          @Override
          public ObsFSFile call() throws ObsException {
            return obs.newFile(newFileRequest);
          }
        });
      } catch (ObsException e) {
        throw e;
      }
      future.get();
      incrementPutCompletedStatistics(true, 0);
    } catch (InterruptedException e) {
      incrementPutCompletedStatistics(false, 0);
      throw new InterruptedIOException("Interrupted creating " + objectName);
    } catch (ExecutionException e) {
      incrementPutCompletedStatistics(false, 0);
      if (e.getCause() instanceof ObsException) {
        throw (ObsException) e.getCause();
      }
      throw new ObsException("obs exception: ", e.getCause());
    }
    incrementPutProgressStatistics(objectName, 0);

    long delay = System.nanoTime() - startTime;
    instrumentation.filesCreated(1, delay);
  }

  // Used to create a folder
  private void fsCreateFolder(final String objectName)
          throws ObsException,
          InterruptedIOException {
    instrumentation.directoriesCreatedTotal(1);
    long startTime = System.nanoTime();

    try {
      final NewFolderRequest newFolderRequest = new NewFolderRequest(bucket, objectName);
      long len = newFolderRequest.getObjectKey().length();
      incrementPutStartStatistics(len);
      ListenableFuture<ObsFSFolder> future = null;
      try {
        future = boundedThreadPool.submit(new Callable<ObsFSFolder>() {
          @Override
          public ObsFSFolder call() throws ObsException {
            return obs.newFolder(newFolderRequest);
          }
        });
      } catch (ObsException e) {
        throw e;
      }
      future.get();
      incrementPutCompletedStatistics(true, 0);
    } catch (InterruptedException e) {
      incrementPutCompletedStatistics(false, 0);
      throw new InterruptedIOException("Interrupted creating " + objectName);
    } catch (ExecutionException e) {
      incrementPutCompletedStatistics(false, 0);
      if (e.getCause() instanceof ObsException) {
        throw (ObsException) e.getCause();
      }
      throw new ObsException("obs exception: ", e.getCause());
    }
    incrementPutProgressStatistics(objectName, 0);

    long delay = System.nanoTime() - startTime;
    instrumentation.directoriesCreated(1, delay);
  }

  /**
   * Creates a copy of the passed {@link ObjectMetadata}.
   * Does so without using the {@link ObjectMetadata#clone()} method,
   * to avoid copying unnecessary headers.
   * @param source the {@link ObjectMetadata} to copy
   * @return a copy of {@link ObjectMetadata} with only relevant attributes
   */
  private ObjectMetadata cloneObjectMetadata(ObjectMetadata source) {
    // This approach may be too brittle, especially if
    // in future there are new attributes added to ObjectMetadata
    // that we do not explicitly call to set here
    ObjectMetadata ret = newObjectMetadata(source.getContentLength());

    if (source.getContentEncoding() != null){
      ret.setContentEncoding(source.getContentEncoding());
    }

    //TODO
    // Possibly null attributes
    // Allowing nulls to pass breaks it during later use
    /*if (source.getCacheControl() != null) {
      ret.setCacheControl(source.getCacheControl());
    }
    if (source.getContentDisposition() != null) {
      ret.setContentDisposition(source.getContentDisposition());
    }
    if (source.getContentEncoding() != null) {
      ret.setContentEncoding(source.getContentEncoding());
    }
    if (source.getContentMD5() != null) {
      ret.setContentMD5(source.getContentMD5());
    }
    if (source.getContentType() != null) {
      ret.setContentType(source.getContentType());
    }
    if (source.getExpirationTime() != null) {
      ret.setExpirationTime(source.getExpirationTime());
    }
    if (source.getExpirationTimeRuleId() != null) {
      ret.setExpirationTimeRuleId(source.getExpirationTimeRuleId());
    }
    if (source.getHttpExpiresDate() != null) {
      ret.setHttpExpiresDate(source.getHttpExpiresDate());
    }
    if (source.getLastModified() != null) {
      ret.setLastModified(source.getLastModified());
    }
    if (source.getOngoingRestore() != null) {
      ret.setOngoingRestore(source.getOngoingRestore());
    }
    if (source.getRestoreExpirationTime() != null) {
      ret.setRestoreExpirationTime(source.getRestoreExpirationTime());
    }
    if (source.getSSEAlgorithm() != null) {
      ret.setSSEAlgorithm(source.getSSEAlgorithm());
    }
    if (source.getSSECustomerAlgorithm() != null) {
      ret.setSSECustomerAlgorithm(source.getSSECustomerAlgorithm());
    }
    if (source.getSSECustomerKeyMd5() != null) {
      ret.setSSECustomerKeyMd5(source.getSSECustomerKeyMd5());
    }*/

    //TODO: no such method
    /*for (Map.Entry<String, String> e : source.getUserMetadata().entrySet()) {
      ret.addUserMetadata(e.getKey(), e.getValue());
    }*/
    return ret;
  }

  /**
   * Return the number of bytes that large input files should be optimally
   * be split into to minimize I/O time.
   * @deprecated use {@link #getDefaultBlockSize(Path)} instead
   */
  @Deprecated
  public long getDefaultBlockSize() {
    return getConf().getLongBytes(FS_OBS_BLOCK_SIZE, DEFAULT_BLOCKSIZE);
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder(
        "OBSFileSystem{");
    sb.append("uri=").append(uri);
    sb.append(", workingDir=").append(workingDir);
    sb.append(", inputPolicy=").append(inputPolicy);
    sb.append(", partSize=").append(partSize);
    sb.append(", enableMultiObjectsDelete=").append(enableMultiObjectsDelete);
    sb.append(", maxKeys=").append(maxKeys);
    sb.append(", readAhead=").append(readAhead);
    sb.append(", blockSize=").append(getDefaultBlockSize());
    sb.append(", multiPartThreshold=").append(multiPartThreshold);
    if (serverSideEncryptionAlgorithm != null) {
      sb.append(", serverSideEncryptionAlgorithm='")
          .append(serverSideEncryptionAlgorithm)
          .append('\'');
    }
    if (blockFactory != null) {
      sb.append(", blockFactory=").append(blockFactory);
    }
    sb.append(", boundedExecutor=").append(boundedThreadPool);
    sb.append(", unboundedExecutor=").append(unboundedThreadPool);
    sb.append(", statistics {")
        .append(statistics)
        .append("}");
    sb.append(", metrics {")
        .append(instrumentation.dump("{", "=", "} ", true))
        .append("}");
    sb.append('}');
    return sb.toString();
  }

  /**
   * Get the partition size for multipart operations.
   * @return the value as set during initialization
   */
  public long getPartitionSize() {
    return partSize;
  }

  /**
   * Get the threshold for multipart files.
   * @return the value as set during initialization
   */
  public long getMultiPartThreshold() {
    return multiPartThreshold;
  }

  /**
   * Get the maximum key count.
   * @return a value, valid after initialization
   */
  int getMaxKeys() {
    return maxKeys;
  }

  /**
   * Increments the statistic {@link Statistic#INVOCATION_GLOB_STATUS}.
   * {@inheritDoc}
   */
  @Override
  public FileStatus[] globStatus(Path pathPattern) throws IOException {
    incrementStatistic(INVOCATION_GLOB_STATUS);
    return super.globStatus(pathPattern);
  }

  /**
   * Override superclass so as to add statistic collection.
   * {@inheritDoc}
   */
  @Override
  public FileStatus[] globStatus(Path pathPattern, PathFilter filter)
      throws IOException {
    incrementStatistic(INVOCATION_GLOB_STATUS);
    return super.globStatus(pathPattern, filter);
  }

  /**
   * Override superclass so as to add statistic collection.
   * {@inheritDoc}
   */
  @Override
  public boolean exists(Path f) throws IOException {
    incrementStatistic(INVOCATION_EXISTS);
    return super.exists(f);
  }

  /**
   * Override superclass so as to add statistic collection.
   * {@inheritDoc}
   */
  @Override
  public boolean isDirectory(Path f) throws IOException {
    incrementStatistic(INVOCATION_IS_DIRECTORY);
    return super.isDirectory(f);
  }

  /**
   * Override superclass so as to add statistic collection.
   * {@inheritDoc}
   */
  @Override
  public boolean isFile(Path f) throws IOException {
    incrementStatistic(INVOCATION_IS_FILE);
    return super.isFile(f);
  }

  /**
   * {@inheritDoc}.
   *
   * This implementation is optimized for OBS, which can do a bulk listing
   * off all entries under a path in one single operation. Thus there is
   * no need to recursively walk the directory tree.
   *
   * Instead a {@link ListObjectsRequest} is created requesting a (windowed)
   * listing of all entries under the given path. This is used to construct
   * an {@code ObjectListingIterator} instance, iteratively returning the
   * sequence of lists of elements under the path. This is then iterated
   * over in a {@code FileStatusListingIterator}, which generates
   * {@link OBSFileStatus} instances, one per listing entry.
   * These are then translated into {@link LocatedFileStatus} instances.
   *
   * This is essentially a nested and wrapped set of iterators, with some
   * generator classes; an architecture which may become less convoluted
   * using lambda-expressions.
   * @param f a path
   * @param recursive if the subdirectories need to be traversed recursively
   *
   * @return an iterator that traverses statuses of the files/directories
   *         in the given path
   * @throws FileNotFoundException if {@code path} does not exist
   * @throws IOException if any I/O error occurred
   */
  @Override
  public RemoteIterator<LocatedFileStatus> listFiles(Path f,
      boolean recursive) throws FileNotFoundException, IOException {
    incrementStatistic(INVOCATION_LIST_FILES);
    Path path = qualify(f);
    LOG.debug("listFiles({}, {})", path, recursive);
    try {
      // lookup dir triggers existence check
      final FileStatus fileStatus = getFileStatus(path);
      if (fileStatus.isFile()) {
        // simple case: File
        LOG.debug("Path is a file");
        return new Listing.SingleStatusRemoteIterator(
            toLocatedFileStatus(fileStatus));
      } else {
        LOG.debug("listFiles: doing listFiles of directory {} - recursive {}", path, recursive);
        // directory: do a bulk operation
        String key = maybeAddTrailingSlash(pathToKey(path));
        String delimiter = recursive ? null : "/";
        LOG.debug("Requesting all entries under {} with delimiter '{}'",
            key, delimiter);
        return listing.createLocatedFileStatusIterator(
            listing.createFileStatusListingIterator(path,
                createListObjectsRequest(key, delimiter),
                ACCEPT_ALL,
                new Listing.AcceptFilesOnly(path)));
      }
    } catch (ObsException e) {
      throw translateException("listFiles", path, e);
    }
  }

  /**
   * Override superclass so as to add statistic collection.
   * {@inheritDoc}
   */
  @Override
  public RemoteIterator<LocatedFileStatus> listLocatedStatus(Path f)
      throws FileNotFoundException, IOException {
    return listLocatedStatus(f, ACCEPT_ALL);
  }

  /**
   * {@inheritDoc}.
   *
   * OBS Optimized directory listing. The initial operation performs the
   * first bulk listing; extra listings will take place
   * when all the current set of results are used up.
   * @param f a path
   * @param filter a path filter
   * @return an iterator that traverses statuses of the files/directories
   *         in the given path
   * @throws FileNotFoundException if {@code path} does not exist
   * @throws IOException if any I/O error occurred
   */
  @Override
  public RemoteIterator<LocatedFileStatus> listLocatedStatus(final Path f,
      final PathFilter filter)
      throws FileNotFoundException, IOException {
    incrementStatistic(INVOCATION_LIST_LOCATED_STATUS);
    Path path = qualify(f);
    LOG.debug("listLocatedStatus({}, {}", path, filter);
    try {
      // lookup dir triggers existence check
      final FileStatus fileStatus = getFileStatus(path);
      if (fileStatus.isFile()) {
        // simple case: File
        LOG.debug("Path is a file");
        return new Listing.SingleStatusRemoteIterator(
            filter.accept(path) ? toLocatedFileStatus(fileStatus) : null);
      } else {
        // directory: trigger a lookup
        String key = maybeAddTrailingSlash(pathToKey(path));
        return listing.createLocatedFileStatusIterator(
            listing.createFileStatusListingIterator(path,
                createListObjectsRequest(key, "/"),
                filter,
                new Listing.AcceptAllButSelfAndS3nDirs(path)));
      }
    } catch (ObsException e) {
      throw translateException("listLocatedStatus", path, e);
    }
  }

  /**
   * Build a {@link LocatedFileStatus} from a {@link FileStatus} instance.
   * @param status file status
   * @return a located status with block locations set up from this FS.
   * @throws IOException IO Problems.
   */
  LocatedFileStatus toLocatedFileStatus(FileStatus status)
      throws IOException {
    return new LocatedFileStatus(status,
        status.isFile() ?
          getFileBlockLocations(status, 0, status.getLen())
          : null);
  }



  /**
   * Helper for an ongoing write operation.
   * <p>
   * It hides direct access to the OBS API from the output stream,
   * and is a location where the object upload process can be evolved/enhanced.
   * <p>
   * Features
   * <ul>
   *   <li>Methods to create and submit requests to OBS, so avoiding
   *   all direct interaction with the OBS APIs.</li>
   *   <li>Some extra preflight checks of arguments, so failing fast on
   *   errors.</li>
   *   <li>Callbacks to let the FS know of events in the output stream
   *   upload process.</li>
   * </ul>
   *
   * Each instance of this state is unique to a single output stream.
   */
  final class OBSWriteOperationHelper {
    private final String key;

    private OBSWriteOperationHelper(String key) {
      this.key = key;
    }

    /**
     * Create a {@link PutObjectRequest} request.
     * If {@code length} is set, the metadata is configured with the size of
     * the upload.
     * @param inputStream source data.
     * @param length size, if known. Use -1 for not known
     * @return the request
     */
    PutObjectRequest newPutRequest(InputStream inputStream, long length) {
      PutObjectRequest request = newPutObjectRequest(key,
              newObjectMetadata(length), inputStream);
      return request;
    }

    /**
     * Create a {@link PutObjectRequest} request to upload a file.
     * @param sourceFile source file
     * @return the request
     */
    PutObjectRequest newPutRequest(File sourceFile) {
      int length = (int) sourceFile.length();
      PutObjectRequest request = newPutObjectRequest(key,
              newObjectMetadata(length), sourceFile);
      return request;
    }

    /**
     * Callback on a successful write.
     */
    void writeSuccessful() {
      finishedWrite(key);
    }

    /**
     * Callback on a write failure.
     * @param e Any exception raised which triggered the failure.
     */
    void writeFailed(Exception e) {
      LOG.debug("Write to {} failed", this, e);
    }

    /**
     * Create a new object metadata instance.
     * Any standard metadata headers are added here, for example:
     * encryption.
     * @param length size, if known. Use -1 for not known
     * @return a new metadata instance
     */
    public ObjectMetadata newObjectMetadata(long length) {
      return OBSFileSystem.this.newObjectMetadata(length);
    }

    /**
     * Start the multipart upload process.
     * @return the upload result containing the ID
     * @throws IOException IO problem
     */
    String initiateMultiPartUpload() throws IOException {
      LOG.debug("Initiating Multipart upload");
      final InitiateMultipartUploadRequest initiateMPURequest =
              new InitiateMultipartUploadRequest(bucket,
                      key);
              //TODO object length
      initiateMPURequest.setMetadata(newObjectMetadata(-1));
      try {
        return obs.initiateMultipartUpload(initiateMPURequest)
                .getUploadId();
      } catch (ObsException ace) {
        throw translateException("initiate MultiPartUpload", key, ace);
      }
    }

    /**
     * Complete a multipart upload operation.
     * @param uploadId multipart operation Id
     * @param partETags list of partial uploads
     * @return the result
     * @throws ObsException on problems.
     */
    CompleteMultipartUploadResult completeMultipartUpload(String uploadId,
                                                          List<PartEtag> partETags) throws ObsException {
      Preconditions.checkNotNull(uploadId);
      Preconditions.checkNotNull(partETags);
      Preconditions.checkArgument(!partETags.isEmpty(),
              "No partitions have been uploaded");
      LOG.debug("Completing multipart upload {} with {} parts",
              uploadId, partETags.size());
      // a copy of the list is required, so that the OBS SDK doesn't
      // attempt to sort an unmodifiable list.
      return obs.completeMultipartUpload(
              new CompleteMultipartUploadRequest(bucket,
                      key,
                      uploadId,
                      new ArrayList<>(partETags)));
    }

    /**
     * Abort a multipart upload operation.
     * @param uploadId multipart operation Id
     * @throws ObsException on problems.
     * Immediately execute
     */
    void abortMultipartUpload(String uploadId) throws ObsException {
      LOG.debug("Aborting multipart upload {}", uploadId);
      obs.abortMultipartUpload(
              new AbortMultipartUploadRequest(bucket, key, uploadId));
    }
    /**
     * Create request for uploading one part of a multipart task
     *
     * @param uploadId
     * @param partNumber
     * @param size
     * @param uploadStream
     * @param sourceFile
     * @return
     */
    UploadPartRequest newUploadPartRequest(String uploadId, int partNumber,int size, InputStream uploadStream, File sourceFile) {
      Preconditions.checkNotNull(uploadId);

      Preconditions.checkArgument((uploadStream != null) ^ (sourceFile != null),"Data source");
      Preconditions.checkArgument(size>0,"Invalid partition size %s", size);
      Preconditions.checkArgument(partNumber>0 && partNumber<=10000);

      LOG.debug("Creating part upload request for {} #{} size {}",
              uploadId, partNumber, size);
      UploadPartRequest request = new UploadPartRequest();
      request.setUploadId(uploadId);
      request.setBucketName(bucket);
      request.setObjectKey(key);
      request.setPartSize((long)size);
      request.setPartNumber(partNumber);
      if (uploadStream != null) {
        // there's an upload stream. Bind to it.
        request.setInput(uploadStream);
      } else {
        // or file
        request.setFile(sourceFile);
      }
      return request;
    }
    /**
     * The toString method is intended to be used in logging/toString calls.
     * @return a string description.
     */
    @Override
    public String toString() {
      final StringBuilder sb = new StringBuilder(
              "{bucket=").append(bucket);
      sb.append(", key='").append(key).append('\'');
      sb.append('}');
      return sb.toString();
    }

    /**
     * PUT an object directly (i.e. not via the transfer manager).
     * @param putObjectRequest the request
     * @return the upload initiated
     * @throws IOException on problems
     */
    PutObjectResult putObject(PutObjectRequest putObjectRequest)
            throws IOException {
      try {
        return putObjectDirect(putObjectRequest);
      } catch (ObsException e) {
        throw translateException("put", putObjectRequest.getObjectKey(), e);
      }
    }
  }
}
