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

import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;


import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.obs.services.exception.ObsException;
import com.obs.services.model.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.io.retry.RetryPolicies;
import org.apache.hadoop.io.retry.RetryPolicy;
import org.apache.hadoop.util.Progressable;

import static org.apache.hadoop.fs.obs.OBSUtils.*;
import static org.apache.hadoop.fs.obs.Statistic.*;

/**
 * Upload files/parts directly via different buffering mechanisms:
 * including memory and disk.
 *
 * If the stream is closed and no update has started, then the upload
 * is instead done as a single PUT operation.
 *
 * Unstable: statistics and error handling might evolve.
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
class OBSBlockOutputStream extends OutputStream {

  private static final Logger LOG =
      LoggerFactory.getLogger(OBSBlockOutputStream.class);

  /** Owner FileSystem. */
  private final OBSFileSystem fs;

  /** Object being uploaded. */
  private final String key;

  /** Size of all blocks. */
  private final int blockSize;

  /** Callback for progress. */
  /*TODO private final ProgressListener progressListener;*/
  private final ListeningExecutorService executorService;

  /**
   * Retry policy for multipart commits; not all OBS SDK versions retry that.
   */
  private final RetryPolicy retryPolicy =
      RetryPolicies.retryUpToMaximumCountWithProportionalSleep(
          5,
          2000,
          TimeUnit.MILLISECONDS);
  /**
   * Factory for blocks.
   */
  private final OBSDataBlocks.BlockFactory blockFactory;

  /** Preallocated byte buffer for writing single characters. */
  private final byte[] singleCharWrite = new byte[1];

  /** Multipart upload details; null means none started. */
  private MultiPartUpload multiPartUpload;

  /** Closed flag. */
  private final AtomicBoolean closed = new AtomicBoolean(false);

  /** Current data block. Null means none currently active */
  private OBSDataBlocks.DataBlock activeBlock;

  /** Count of blocks uploaded. */
  private long blockCount = 0;

  /** Statistics to build up. */
  private final OBSInstrumentation.OutputStreamStatistics statistics;

  /**
   * Write operation helper; encapsulation of the filesystem operations.
   */
  private final OBSFileSystem.OBSWriteOperationHelper writeOperationHelper;

  /**
   * An OBS output stream which uploads partitions in a separate pool of
   * threads; different {@link OBSDataBlocks.BlockFactory}
   * instances can control where data is buffered.
   *
   * @param fs OBSFilesystem
   * @param key OBS object to work on.
   * @param executorService the executor service to use to schedule work
   * @param progress report progress in order to prevent timeouts. If
   * this object implements {@code ProgressListener} then it will be
   * directly wired up to the OBS client, so receive detailed progress
   * information.
   * @param blockSize size of a single block.
   * @param blockFactory factory for creating stream destinations
   * @param statistics stats for this stream
   * @param writeOperationHelper state of the write operation.
   * @throws IOException on any problem
   */
  OBSBlockOutputStream(OBSFileSystem fs,
                       String key,
                       ExecutorService executorService,
                       Progressable progress,
                       long blockSize,
                       OBSDataBlocks.BlockFactory blockFactory,
                       OBSInstrumentation.OutputStreamStatistics statistics,
                       OBSFileSystem.OBSWriteOperationHelper writeOperationHelper)
      throws IOException {
    this.fs = fs;
    this.key = key;
    this.blockFactory = blockFactory;
    this.blockSize = (int) blockSize;
    this.statistics = statistics;
    this.writeOperationHelper = writeOperationHelper;
    Preconditions.checkArgument(blockSize >= Constants.MULTIPART_MIN_SIZE,
        "Block size is too small: %d", blockSize);
    this.executorService = MoreExecutors.listeningDecorator(executorService);
    this.multiPartUpload = null;
    /*TODO this.progressListener = (progress instanceof ProgressListener) ?
        (ProgressListener) progress
        : new ProgressableListener(progress);*/
    // create that first block. This guarantees that an open + close sequence
    // writes a 0-byte entry.
    createBlockIfNeeded();
    LOG.debug("Initialized OBSBlockOutputStream for {}" +
        " output to {}", writeOperationHelper, activeBlock);
  }

  /**
   * Demand create a destination block.
   * @return the active block; null if there isn't one.
   * @throws IOException on any failure to create
   */
  private synchronized OBSDataBlocks.DataBlock createBlockIfNeeded()
      throws IOException {
    if (activeBlock == null) {
      blockCount++;
      if (blockCount>= Constants.MAX_MULTIPART_COUNT) {
        LOG.error("Number of partitions in stream exceeds limit for OBS: "
             + Constants.MAX_MULTIPART_COUNT +  " write may fail.");
      }
      activeBlock = blockFactory.create(blockCount, this.blockSize, statistics);
    }
    return activeBlock;
  }

  /**
   * Synchronized accessor to the active block.
   * @return the active block; null if there isn't one.
   */
  private synchronized OBSDataBlocks.DataBlock getActiveBlock() {
    return activeBlock;
  }

  /**
   * Predicate to query whether or not there is an active block.
   * @return true if there is an active block.
   */
  private synchronized boolean hasActiveBlock() {
    return activeBlock != null;
  }

  /**
   * Clear the active block.
   */
  private void clearActiveBlock() {
    if (activeBlock != null) {
      LOG.debug("Clearing active block");
    }
    synchronized (this) {
      activeBlock = null;
    }
  }

  /**
   * Check for the filesystem being open.
   * @throws IOException if the filesystem is closed.
   */
  void checkOpen() throws IOException {
    if (closed.get()) {
      throw new IOException("Filesystem " + writeOperationHelper + " closed");
    }
  }

  /**
   * The flush operation does not trigger an upload; that awaits
   * the next block being full. What it does do is call {@code flush() }
   * on the current block, leaving it to choose how to react.
   * @throws IOException Any IO problem.
   */
  @Override
  public synchronized void flush() throws IOException {
    checkOpen();
    OBSDataBlocks.DataBlock dataBlock = getActiveBlock();
    if (dataBlock != null) {
      dataBlock.flush();
    }
  }

  /**
   * Writes a byte to the destination. If this causes the buffer to reach
   * its limit, the actual upload is submitted to the threadpool.
   * @param b the int of which the lowest byte is written
   * @throws IOException on any problem
   */
  @Override
  public synchronized void write(int b) throws IOException {
    singleCharWrite[0] = (byte)b;
    write(singleCharWrite, 0, 1);
  }

  /**
   * Writes a range of bytes from to the memory buffer. If this causes the
   * buffer to reach its limit, the actual upload is submitted to the
   * threadpool and the remainder of the array is written to memory
   * (recursively).
   * @param source byte array containing
   * @param offset offset in array where to start
   * @param len number of bytes to be written
   * @throws IOException on any problem
   */
  @Override
  public synchronized void write(byte[] source, int offset, int len)
      throws IOException {

    OBSDataBlocks.validateWriteArgs(source, offset, len);
    checkOpen();
    if (len == 0) {
      return;
    }
    OBSDataBlocks.DataBlock block = createBlockIfNeeded();
    int written = block.write(source, offset, len);
    int remainingCapacity = block.remainingCapacity();
    if (written < len) {
      // not everything was written —the block has run out
      // of capacity
      // Trigger an upload then process the remainder.
      LOG.debug("writing more data than block has capacity -triggering upload");
      uploadCurrentBlock();
      // tail recursion is mildly expensive, but given buffer sizes must be MB.
      // it's unlikely to recurse very deeply.
      this.write(source, offset + written, len - written);
    } else {
      if (remainingCapacity == 0) {
        // the whole buffer is done, trigger an upload
        uploadCurrentBlock();
      }
    }
  }

  /**
   * Start an asynchronous upload of the current block.
   * @throws IOException Problems opening the destination for upload
   * or initializing the upload.
   */
  private synchronized void uploadCurrentBlock() throws IOException {
    Preconditions.checkState(hasActiveBlock(), "No active block");
    LOG.debug("Writing block # {}", blockCount);
    if (multiPartUpload == null) {
      LOG.debug("Initiating Multipart upload");
      multiPartUpload = new MultiPartUpload();
    }
    try {
      multiPartUpload.uploadBlockAsync(getActiveBlock());
    } finally {
      // set the block to null, so the next write will create a new block.
      clearActiveBlock();
    }
  }

  /**
   * Close the stream.
   *
   * This will not return until the upload is complete
   * or the attempt to perform the upload has failed.
   * Exceptions raised in this method are indicative that the write has
   * failed and data is at risk of being lost.
   * @throws IOException on any failure.
   */
  @Override
  public void close() throws IOException {
    if (closed.getAndSet(true)) {
      // already closed
      LOG.debug("Ignoring close() as stream is already closed");
      return;
    }
    OBSDataBlocks.DataBlock block = getActiveBlock();
    boolean hasBlock = hasActiveBlock();
    LOG.debug("{}: Closing block #{}: current block= {}",
        this,
        blockCount,
        hasBlock ? block : "(none)");
    try {
      if (multiPartUpload == null) {
        if (hasBlock) {
          // no uploads of data have taken place, put the single block up.
          // This must happen even if there is no data, so that 0 byte files
          // are created.
          putObject();
        }
      } else {
        // there has already been at least one block scheduled for upload;
        // put up the current then wait
        if (hasBlock && block.hasData()) {
          //send last part
          uploadCurrentBlock();
        }
        // wait for the partial uploads to finish
        final List<PartEtag> partETags =
            multiPartUpload.waitForAllPartUploads();
        // then complete the operation
        multiPartUpload.complete(partETags);
      }
      LOG.debug("Upload complete for {}", writeOperationHelper);
    } catch (IOException ioe) {
      writeOperationHelper.writeFailed(ioe);
      throw ioe;
    } finally {
      closeAll(LOG, block, blockFactory);
      LOG.debug("Statistics: {}", statistics);
      closeAll(LOG, statistics);
      clearActiveBlock();
    }
    // All end of write operations, including deleting fake parent directories
    writeOperationHelper.writeSuccessful();
  }

  /**
   * Upload the current block as a single PUT request; if the buffer
   * is empty a 0-byte PUT will be invoked, as it is needed to create an
   * entry at the far end.
   * @throws IOException any problem.
   */
  private void putObject() throws IOException {
    LOG.debug("Executing regular upload for {}", writeOperationHelper);

    final OBSDataBlocks.DataBlock block = getActiveBlock();
    final int size = block.dataSize();
    final OBSDataBlocks.BlockUploadData uploadData = block.startUpload();
    final PutObjectRequest putObjectRequest = uploadData.hasFile() ?
        writeOperationHelper.newPutRequest(uploadData.getFile())
        : writeOperationHelper.newPutRequest(uploadData.getUploadStream(), size);
    long transferQueueTime = now();
   /* BlockUploadProgress callback =
        new BlockUploadProgress(
            block, progressListener, transferQueueTime);*/
    //TODO
    //putObjectRequest.setGeneralProgressListener(callback);
    statistics.blockUploadQueued(size);
    incrementWriteOperations();
    final long uploadQueuedTime = now();
    ListenableFuture<PutObjectResult> putObjectResult =
        executorService.submit(new Callable<PutObjectResult>() {
          @Override
          public PutObjectResult call() throws Exception {
            long uploadStartTime=now();
            statistics.blockUploadStarted(uploadStartTime-uploadQueuedTime,size);
            PutObjectResult result;
            try {
              // the putObject call automatically closes the input
              // stream afterwards.
              result = writeOperationHelper.putObject(putObjectRequest);
              long uploadCompletedTime=now();
              statistics.blockUploadCompleted(uploadCompletedTime-uploadStartTime,size);
            } catch (Exception e){
              long uploadFailedTime=now();
              statistics.blockUploadFailed(uploadFailedTime-uploadStartTime,size);
              throw e;
            }finally {
              closeAll(LOG, uploadData, block);
            }
            return result;
          }
        });
    clearActiveBlock();
    //wait for completion
    try {
      putObjectResult.get();
    } catch (InterruptedException ie) {
      LOG.warn("Interrupted object upload", ie);
      Thread.currentThread().interrupt();
    } catch (ExecutionException ee) {
      throw extractException("regular upload", key, ee);
    }
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder(
        "OBSBlockOutputStream{");
    sb.append(writeOperationHelper.toString());
    sb.append(", blockSize=").append(blockSize);
    // unsynced access; risks consistency in exchange for no risk of deadlock.
    OBSDataBlocks.DataBlock block = activeBlock;
    if (block != null) {
      sb.append(", activeBlock=").append(block);
    }
    sb.append('}');
    return sb.toString();
  }

  private void incrementWriteOperations() {
    fs.incrementWriteOperations();
  }

  /**
   * Current time in milliseconds.
   * @return time
   */
  private long now() {
    return System.currentTimeMillis();
  }

  /**
   * Get the statistics for this stream.
   * @return stream statistics
   */
  OBSInstrumentation.OutputStreamStatistics getStatistics() {
    return statistics;
  }

  /**
   * Multiple partition upload.
   */
  private class MultiPartUpload {
    private final String uploadId;
    private final List<ListenableFuture<PartEtag>> partETagsFutures;

    MultiPartUpload() throws IOException {
      this.uploadId = writeOperationHelper.initiateMultiPartUpload();
      this.partETagsFutures = new ArrayList<>(2);
      LOG.debug("Initiated multi-part upload for {} with " +
          "id '{}'", writeOperationHelper, uploadId);
    }

    /**
     * Upload a block of data.
     * This will take the block
     * @param block block to upload
     * @throws IOException upload failure
     */
    private void uploadBlockAsync(final OBSDataBlocks.DataBlock block)
        throws IOException {
      LOG.debug("Queueing upload of {}", block);
      final int size = block.dataSize();
      final OBSDataBlocks.BlockUploadData uploadData = block.startUpload();
      final int currentPartNumber = partETagsFutures.size() + 1;
      final UploadPartRequest request =
          writeOperationHelper.newUploadPartRequest(
              uploadId,
              currentPartNumber,
              size,
              uploadData.getUploadStream(),
              uploadData.getFile());

      long transferQueueTime = now();
     /* TODO BlockUploadProgress callback =
          new BlockUploadProgress(
              block, progressListener, transferQueueTime);*/

      //TODO  request.setGeneralProgressListener(callback);
      statistics.blockUploadQueued(block.dataSize());
      ListenableFuture<PartEtag> partETagFuture =
          executorService.submit(new Callable<PartEtag>() {
            @Override
            public PartEtag call() throws Exception {
              // this is the queued upload operation
              LOG.debug("Uploading part {} for id '{}'", currentPartNumber,
                  uploadId);
              // do the upload
              PartEtag partETag;
              try {
                UploadPartResult uploadPartResult = fs.uploadPart(request);
                partETag=new PartEtag(uploadPartResult.getEtag(),uploadPartResult.getPartNumber());
                LOG.debug("Completed upload of {} to part {}", block,partETag);
                LOG.debug("Stream statistics of {}", statistics);
              } finally {
                // close the stream and block
                closeAll(LOG, uploadData, block);
              }
              return partETag;
            }
          });
      partETagsFutures.add(partETagFuture);
    }

    /**
     * Block awaiting all outstanding uploads to complete.
     * @return list of results
     * @throws IOException IO Problems
     */
    private List<PartEtag> waitForAllPartUploads() throws IOException {
      LOG.debug("Waiting for {} uploads to complete", partETagsFutures.size());
      try {
        return Futures.allAsList(partETagsFutures).get();
      } catch (InterruptedException ie) {
        LOG.warn("Interrupted partUpload", ie);
        Thread.currentThread().interrupt();
        return null;
      } catch (ExecutionException ee) {
        //there is no way of recovering so abort
        //cancel all partUploads
        LOG.debug("While waiting for upload completion", ee);
        LOG.debug("Cancelling futures");
        for (ListenableFuture<PartEtag> future : partETagsFutures) {
          future.cancel(true);
        }
        //abort multipartupload
        this.abort();
        throw extractException("Multi-part upload with id '" + uploadId
                + "' to " + key, key, ee);
      }
    }

    /**
     * This completes a multipart upload.
     * Sometimes it fails; here retries are handled to avoid losing all data
     * on a transient failure.
     * @param partETags list of partial uploads
     * @throws IOException on any problem
     */
    private CompleteMultipartUploadResult complete(List<PartEtag> partETags)
        throws IOException {
      int retryCount = 0;
      ObsException lastException;
      String operation =
          String.format("Completing multi-part upload for key '%s'," +
                  " id '%s' with %s partitions ",
              key, uploadId, partETags.size());
      do {
        try {
          LOG.debug(operation);
          return writeOperationHelper.completeMultipartUpload(
                  uploadId,
                  partETags);
        } catch (ObsException e) {
          lastException = e;
          statistics.exceptionInMultipartComplete();
        }
      } while (shouldRetry(operation, lastException, retryCount++));
      // this point is only reached if the operation failed more than
      // the allowed retry count
      throw translateException(operation, key, lastException);
    }

    /**
     * Abort a multi-part upload. Retries are attempted on failures.
     * IOExceptions are caught; this is expected to be run as a cleanup process.
     */
    public void abort() {
      int retryCount = 0;
      ObsException lastException;
      fs.incrementStatistic(OBJECT_MULTIPART_UPLOAD_ABORTED);
      String operation =
          String.format("Aborting multi-part upload for '%s', id '%s",
              writeOperationHelper, uploadId);
      do {
        try {
          LOG.debug(operation);
          writeOperationHelper.abortMultipartUpload(uploadId);
          return;
        } catch (ObsException e) {
          lastException = e;
          statistics.exceptionInMultipartAbort();
        }
      } while (shouldRetry(operation, lastException, retryCount++));
      // this point is only reached if the operation failed more than
      // the allowed retry count
      LOG.warn("Unable to abort multipart upload, you may need to purge  " +
          "uploaded parts", lastException);
    }

    /**
     * Predicate to determine whether a failed operation should
     * be attempted again.
     * If a retry is advised, the exception is automatically logged and
     * the filesystem statistic {@link Statistic#IGNORED_ERRORS} incremented.
     * The method then sleeps for the sleep time suggested by the sleep policy;
     * if the sleep is interrupted then {@code Thread.interrupted()} is set
     * to indicate the thread was interrupted; then false is returned.
     *
     * @param operation operation for log message
     * @param e exception raised.
     * @param retryCount  number of retries already attempted
     * @return true if another attempt should be made
     */
    private boolean shouldRetry(String operation,
        ObsException e,
        int retryCount) {
      try {
        RetryPolicy.RetryAction retryAction =
            retryPolicy.shouldRetry(e, retryCount, 0, true);
        boolean retry = retryAction == RetryPolicy.RetryAction.RETRY;
        if (retry) {
          fs.incrementStatistic(IGNORED_ERRORS);
          LOG.info("Retrying {} after exception ", operation, e);
          Thread.sleep(retryAction.delayMillis);
        }
        return retry;
      } catch (InterruptedException ex) {
        Thread.currentThread().interrupt();
        return false;
      } catch (Exception ignored) {
        return false;
      }
    }

  }

 //TODO transfer progress statistic

}
