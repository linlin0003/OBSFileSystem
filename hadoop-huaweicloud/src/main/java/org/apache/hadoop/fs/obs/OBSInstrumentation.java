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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.metrics2.MetricStringBuilder;
import org.apache.hadoop.metrics2.annotation.Metrics;
import org.apache.hadoop.metrics2.lib.Interns;
import org.apache.hadoop.metrics2.lib.MetricsRegistry;
import org.apache.hadoop.metrics2.lib.MutableCounterLong;
import org.apache.hadoop.metrics2.lib.MutableGaugeLong;
import org.apache.hadoop.metrics2.lib.MutableMetric;

import java.io.Closeable;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.hadoop.fs.FileSystem.Statistics;

import static org.apache.hadoop.fs.obs.Statistic.*;

/**
 * Instrumentation of OBS.
 * Derived from the {@code AzureFileSystemInstrumentation}.
 *
 * Counters and metrics are generally addressed in code by their name or
 * {@link Statistic} key. There <i>may</i> be some Statistics which do
 * not have an entry here. To avoid attempts to access such counters failing,
 * the operations to increment/query metric values are designed to handle
 * lookup failures.
 */
@Metrics(about = "Metrics for OBS", context = "OBSFileSystem")
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class OBSInstrumentation {
  private static final Logger LOG = LoggerFactory.getLogger(
      OBSInstrumentation.class);

  public static final String CONTEXT = "OBSFileSystem";
  private final MetricsRegistry registry =
      new MetricsRegistry("OBSFileSystem").setContext(CONTEXT);
  private final MutableCounterLong streamOpenOperations;
  private final MutableCounterLong streamCloseOperations;
  private final MutableCounterLong streamClosed;
  private final MutableCounterLong streamAborted;
  private final MutableCounterLong streamSeekOperations;
  private final MutableCounterLong streamReadExceptions;
  private final MutableCounterLong streamForwardSeekOperations;
  private final MutableCounterLong streamBackwardSeekOperations;
  private final MutableCounterLong streamBytesSkippedOnSeek;
  private final MutableCounterLong streamBytesBackwardsOnSeek;
  private final MutableCounterLong streamBytesRead;
  private final MutableCounterLong streamReadOperations;
  private final MutableCounterLong streamReadFullyOperations;
  private final MutableCounterLong streamReadsIncomplete;
  private final MutableCounterLong streamBytesReadInClose;
  private final MutableCounterLong streamBytesDiscardedInAbort;
  private final MutableCounterLong ignoredErrors;

  private final OBSCounter counterOfFilesCreated;
  private final OBSCounter counterOfFilesCopied;
  private final OBSCounter counterOfFilesDeleted;
  private final OBSCounter counterOfFakeDirectoryDeletes;
  private final OBSCounter counterOfBatchDeletes;
  private final OBSCounter counterOfDirectoriesCreated;
  private final OBSCounter counterOfDirectoriesDeleted;
  private final OBSCounter counterOfFrontEndFilesDeleted;
  private final OBSCounter counterOfFrontEndDirectoryDeletes;

  // for rename
  private final OBSCounter counterOfFilesRenamed;
  private final OBSCounter counterOfDirectoriesRenamed;
  private final OBSCounter counterOfFrontEndFilesRenamed;
  private final OBSCounter counterOfFrontEndDirectoriesRenamed;
  private final OBSCounter counterOfFrontEndExistedDirectoriesRenamed;
  private final OBSCounter counterOfListObjectsInRename;

  private final Map<String, MutableCounterLong> streamMetrics =
      new HashMap<>(30);

  private static final Statistic[] COUNTERS_TO_CREATE = {
      INVOCATION_COPY_FROM_LOCAL_FILE,
      INVOCATION_EXISTS,
      INVOCATION_GET_FILE_STATUS,
      INVOCATION_GLOB_STATUS,
      INVOCATION_IS_DIRECTORY,
      INVOCATION_IS_FILE,
      INVOCATION_LIST_FILES,
      INVOCATION_LIST_LOCATED_STATUS,
      INVOCATION_LIST_STATUS,
      INVOCATION_MKDIRS,
      INVOCATION_RENAME,
      OBJECT_COPY_REQUESTS,
      OBJECT_DELETE_REQUESTS,
      OBJECT_LIST_REQUESTS,
      OBJECT_CONTINUE_LIST_REQUESTS,
      OBJECT_METADATA_REQUESTS,
      OBJECT_MULTIPART_UPLOAD_ABORTED,
      OBJECT_PUT_BYTES,
      OBJECT_PUT_REQUESTS,
      OBJECT_PUT_REQUESTS_COMPLETED,
      STREAM_WRITE_FAILURES,
      STREAM_WRITE_BLOCK_UPLOADS,
      STREAM_WRITE_BLOCK_UPLOADS_COMMITTED,
      STREAM_WRITE_BLOCK_UPLOADS_ABORTED,
      STREAM_WRITE_TOTAL_TIME,
      STREAM_WRITE_TOTAL_DATA,
  };


  private static final Statistic[] GAUGES_TO_CREATE = {
      OBJECT_PUT_REQUESTS_ACTIVE,
      OBJECT_PUT_BYTES_PENDING,
      STREAM_WRITE_BLOCK_UPLOADS_ACTIVE,
      STREAM_WRITE_BLOCK_UPLOADS_PENDING,
      STREAM_WRITE_BLOCK_UPLOADS_DATA_PENDING,
  };

  public OBSInstrumentation(URI name) {
    UUID fileSystemInstanceId = UUID.randomUUID();
    registry.tag("FileSystemId",
        "A unique identifier for the FS ",
        fileSystemInstanceId.toString() + "-" + name.getHost());
    registry.tag("fsURI",
        "URI of this filesystem",
        name.toString());
    streamOpenOperations = streamCounter(STREAM_OPENED);
    streamCloseOperations = streamCounter(STREAM_CLOSE_OPERATIONS);
    streamClosed = streamCounter(STREAM_CLOSED);
    streamAborted = streamCounter(STREAM_ABORTED);
    streamSeekOperations = streamCounter(STREAM_SEEK_OPERATIONS);
    streamReadExceptions = streamCounter(STREAM_READ_EXCEPTIONS);
    streamForwardSeekOperations =
        streamCounter(STREAM_FORWARD_SEEK_OPERATIONS);
    streamBackwardSeekOperations =
        streamCounter(STREAM_BACKWARD_SEEK_OPERATIONS);
    streamBytesSkippedOnSeek = streamCounter(STREAM_SEEK_BYTES_SKIPPED);
    streamBytesBackwardsOnSeek =
        streamCounter(STREAM_SEEK_BYTES_BACKWARDS);
    streamBytesRead = streamCounter(STREAM_SEEK_BYTES_READ);
    streamReadOperations = streamCounter(STREAM_READ_OPERATIONS);
    streamReadFullyOperations =
        streamCounter(STREAM_READ_FULLY_OPERATIONS);
    streamReadsIncomplete =
        streamCounter(STREAM_READ_OPERATIONS_INCOMPLETE);
    streamBytesReadInClose = streamCounter(STREAM_CLOSE_BYTES_READ);
    streamBytesDiscardedInAbort = streamCounter(STREAM_ABORT_BYTES_DISCARDED);
    counterOfFilesCreated = newOBSCounter(FILES_CREATED);
    counterOfFilesCopied = newOBSCounter(FILES_COPIED);
    counterOfFilesDeleted = newOBSCounter(FILES_DELETED);
    counterOfFakeDirectoryDeletes = newOBSCounter(FAKE_DIRECTORIES_DELETED);
    counterOfFrontEndFilesDeleted = newOBSCounter(FRONT_END_FILES_DELETED);
    counterOfFrontEndDirectoryDeletes = newOBSCounter(FRONT_END_DIRECTORIES_DELETED);
    counterOfBatchDeletes = newOBSCounter(BATCH_DELETED);
    counterOfDirectoriesCreated = newOBSCounter(DIRECTORIES_CREATED);
    counterOfDirectoriesDeleted = newOBSCounter(DIRECTORIES_DELETED);
    counterOfFilesRenamed = newOBSCounter(FILES_RENAMED);
    counterOfDirectoriesRenamed = newOBSCounter(DIRECTORIES_RENAMED);
    counterOfFrontEndFilesRenamed = newOBSCounter(FRONT_END_FILES_RENAMED);
    counterOfFrontEndDirectoriesRenamed = newOBSCounter(FRONT_END_DIRECTORIES_RENAMED);
    counterOfFrontEndExistedDirectoriesRenamed = newOBSCounter(FRONT_END_EXISTED_DIRECTORIES_RENAMED);
    counterOfListObjectsInRename = newOBSCounter(LIST_OBJECTS_IN_RENAME);
    ignoredErrors = counter(IGNORED_ERRORS);
    for (Statistic statistic : COUNTERS_TO_CREATE) {
      counter(statistic);
    }
    for (Statistic statistic : GAUGES_TO_CREATE) {
      gauge(statistic.getSymbol(), statistic.getDescription());
    }
  }

  /**
   * Create a counter in the registry.
   * @param name counter name
   * @param desc counter description
   * @return a new counter
   */
  protected final MutableCounterLong counter(String name, String desc) {
    return registry.newCounter(name, desc, 0L);
  }

  /**
   * Create a counter in the stream map: these are unregistered in the public
   * metrics.
   * @param name counter name
   * @param desc counter description
   * @return a new counter
   */
  protected final MutableCounterLong streamCounter(String name, String desc) {
    MutableCounterLong counter = new MutableCounterLong(
        Interns.info(name, desc), 0L);
    streamMetrics.put(name, counter);
    return counter;
  }

  /**
   * Create a counter in the registry.
   * @param op statistic to count
   * @return a new counter
   */
  protected final MutableCounterLong counter(Statistic op) {
    return counter(op.getSymbol(), op.getDescription());
  }

  /**
   * Create a time counter in the registry.
   * @param op statistic to count
   * @return a new counter
   */
  public class OBSCounter {
    private String name;
    private MutableCounterLong total;
    private MutableCounterLong success;
    private MutableCounterLong delay;
    private MutableCounterLong bytes;

    public OBSCounter(Statistic op) {
      name = op.getSymbol();
      total = counter(name, op.getDescription());
      success = counter(
              name + "_success",
              op.getDescription().replaceFirst("Total number of", "Total success number of"));
      delay = counter(
              name + "_time",
              op.getDescription().replaceFirst("Total number of", "Total time of"));
      bytes = counter(
              name + "_bytes",
              op.getDescription().replaceFirst("Total number of", "Total bytes of"));
    }

    private void incrCounter(MutableCounterLong counter, long count) {
      if (count > 0) {
        counter.incr(count);
      }
    }

    public void incr(long count) {
      incrCounter(this.total, count);
    }

    public void incrSuccess(long count, long delay, long bytes) {
      incrCounter(this.success, count);
      incrCounter(this.delay, delay);
      incrCounter(this.bytes, bytes);
    }

    public void incrSuccess(long count, long delay) {
      incrSuccess(count, delay, 0);
    }

    public long getTotal() {
      return total.value();
    }
    public long getSuccess() {
      return success.value();
    }
    public long getDelay() {
      return delay.value();
    }
    public long getBytes() {
      return bytes.value();
    }

    public String toString() {
      final StringBuilder sb = new StringBuilder(name);
      sb.append("{");
      sb.append("total=").append(getTotal());
      sb.append(",succ=").append(getSuccess());
      sb.append(",delay=").append(getDelay());
      sb.append(",bytes=").append(getBytes());
      sb.append('}');
      return sb.toString();
    }
  }

  protected final OBSCounter newOBSCounter(Statistic op) {
    return new OBSCounter(op);
  }

  /**
   * Create a counter in the stream map: these are unregistered in the public
   * metrics.
   * @param op statistic to count
   * @return a new counter
   */
  protected final MutableCounterLong streamCounter(Statistic op) {
    return streamCounter(op.getSymbol(), op.getDescription());
  }

  /**
   * Create a gauge in the registry.
   * @param name name gauge name
   * @param desc description
   * @return the gauge
   */
  protected final MutableGaugeLong gauge(String name, String desc) {
    return registry.newGauge(name, desc, 0L);
  }

  /**
   * Get the metrics registry.
   * @return the registry
   */
  public MetricsRegistry getRegistry() {
    return registry;
  }

  /**
   * Dump all the metrics to a string.
   * @param prefix prefix before every entry
   * @param separator separator between name and value
   * @param suffix suffix
   * @param all get all the metrics even if the values are not changed.
   * @return a string dump of the metrics
   */
  public String dump(String prefix,
      String separator,
      String suffix,
      boolean all) {
    MetricStringBuilder metricBuilder = new MetricStringBuilder(null,
        prefix,
        separator, suffix);
    registry.snapshot(metricBuilder, all);
    for (Map.Entry<String, MutableCounterLong> entry:
        streamMetrics.entrySet()) {
      metricBuilder.tuple(entry.getKey(),
          Long.toString(entry.getValue().value()));
    }
    return metricBuilder.toString();
  }

  /**
   * Get the value of a counter.
   * @param statistic the operation
   * @return its value, or 0 if not found.
   */
  public long getCounterValue(Statistic statistic) {
    return getCounterValue(statistic.getSymbol());
  }

  /**
   * Get the value of a counter.
   * If the counter is null, return 0.
   * @param name the name of the counter
   * @return its value.
   */
  public long getCounterValue(String name) {
    MutableCounterLong counter = lookupCounter(name);
    return counter == null ? 0 : counter.value();
  }

  /**
   * Lookup a counter by name. Return null if it is not known.
   * @param name counter name
   * @return the counter
   * @throws IllegalStateException if the metric is not a counter
   */
  private MutableCounterLong lookupCounter(String name) {
    MutableMetric metric = lookupMetric(name);
    if (metric == null) {
      return null;
    }
    if (!(metric instanceof MutableCounterLong)) {
      throw new IllegalStateException("Metric " + name
          + " is not a MutableCounterLong: " + metric);
    }
    return (MutableCounterLong) metric;
  }

  /**
   * Look up a gauge.
   * @param name gauge name
   * @return the gauge or null
   * @throws ClassCastException if the metric is not a Gauge.
   */
  public MutableGaugeLong lookupGauge(String name) {
    MutableMetric metric = lookupMetric(name);
    if (metric == null) {
      LOG.debug("No gauge {}", name);
    }
    return (MutableGaugeLong) metric;
  }

  /**
   * Look up a metric from both the registered set and the lighter weight
   * stream entries.
   * @param name metric name
   * @return the metric or null
   */
  public MutableMetric lookupMetric(String name) {
    MutableMetric metric = getRegistry().get(name);
    if (metric == null) {
      metric = streamMetrics.get(name);
    }
    return metric;
  }

  /**
   * Indicate that OBS created a file.
   */
  public void filesCreated(long count, long delay) {
    counterOfFilesCreated.incrSuccess(count, delay);
  }
  public void filesCreated() {
    counterOfFilesCreated.incrSuccess(1, 0);
  }
  public void filesCreatedTotal(long count) {
    counterOfFilesCreated.incr(count);
  }

  /**
   * Indicate that OBS deleted one or more file.s
   * @param count number of files.
   */
  public void filesDeleted(long count, long delay) {
    counterOfFilesDeleted.incrSuccess(count, delay);
  }
  public void filesDeletedTotal(long count) {
    counterOfFilesDeleted.incr(count);
  }

  /**
   * Indicate that fake directory request was made.
   * @param count number of directory entries included in the delete request.
   */
  public void fakeDirsDeleted(long count, long delay) {
    counterOfFakeDirectoryDeletes.incrSuccess(count, delay);
  }
  public void fakeDirsDeletedTotal(long count) {
    counterOfFakeDirectoryDeletes.incr(count);
  }

  /**
   * Indicate that OBS created a directory.
   */
  public void directoriesCreated(long count, long delay) {
    counterOfDirectoriesCreated.incrSuccess(count, delay);
  }
  public void directoriesCreatedTotal(long count) {
    counterOfDirectoriesCreated.incr(count);
  }

  /**
   * Indicate that OBS just deleted a directory.
   */
  public void directoriesDeleted(long count, long delay) {
    counterOfDirectoriesDeleted.incrSuccess(count, delay);
  }
  public void directoriesDeletedTotal(long count) {
    counterOfDirectoriesDeleted.incr(count);
  }

  /**
   * Indicate that OBS copied some files within the store.
   *
   * @param count number of files
   * @param delay time of copying files
   * @param size total size in bytes
   */
  public void filesCopied(long count, long delay, long size) {
    counterOfFilesCopied.incrSuccess(count, delay, size);
  }
  public void filesCopiedTotal(long count) {
    counterOfFilesCopied.incr(count);
  }

  /**
   * Indicate that OBS renamed some files within the store.
   *
   * @param count number of renamed files
   * @param delay number of renamed files
   */
  public void filesRenamed(long count, long delay) {
    counterOfFilesRenamed.incrSuccess(count, delay);
  }
  public void filesRenamedTotal(long count) {
    counterOfFilesRenamed.incr(count);
  }

  /**
   * Indicate that OBS renamed some directories within the store.
   *
   * @param count number of renamed directories
   * @param delay time of renamed directories
   */
  public void directoriesRenamed(long count, long delay) {
    counterOfDirectoriesRenamed.incrSuccess(count, delay);
  }
  public void directoriesRenamedTotal(long count) {
    counterOfDirectoriesRenamed.incr(count);
  }

  /**
   * Indicate that OBS renamed some files within the store.
   *
   * @param count number of renamed files
   * @param delay total time of renaming files
   */
  public void frontendFilesRenamed(long count, long delay) {
    counterOfFrontEndFilesRenamed.incrSuccess(count, delay);
  }
  public void frontendFilesRenamedTotal(long count) {
    counterOfFrontEndFilesRenamed.incr(count);
  }
  public String frontendFilesRenamedToString() {
    return counterOfFrontEndFilesRenamed.toString();
  }

  /**
   * Indicate that OBS renamed some directories within the store.
   *
   * @param isExisted number of files
   * @param count number of renamed directories
   * @param delay total time of renaming directories
   */
  public void frontendDirectoriesRenamed(boolean isExisted, long count, long delay) {
    counterOfFrontEndDirectoriesRenamed.incrSuccess(count, delay);
    if (isExisted) {
      counterOfFrontEndExistedDirectoriesRenamed.incrSuccess(count, delay);
    }
  }
  public void frontendDirectoriesRenamed(boolean isExisted, long delay) {
    frontendDirectoriesRenamed(isExisted, 1, delay);
  }
  public void frontendDirectoriesRenamedTotal(boolean isExisted, long count) {
    counterOfFrontEndDirectoriesRenamed.incr(count);
    if (isExisted) {
      counterOfFrontEndExistedDirectoriesRenamed.incr(count);
    }
  }
  public String frontendDirectoriesRenamedToString() {
    return counterOfFrontEndDirectoriesRenamed.toString()
            + ", " + counterOfFrontEndExistedDirectoriesRenamed.toString();
  }

  /**
   * Indicate that OBS list objects during renaming some directories within the store.
   *
   * @param count number of listObjects during of renaming directories
   * @param delay total time of listObjects during of renaming directories
   */
  public void listObjectsInRename(long count, long delay) {
    counterOfListObjectsInRename.incrSuccess(count, delay);
  }
  public void listObjectsInRename(long delay) {
    counterOfListObjectsInRename.incrSuccess(1, delay);
  }
  public void listObjectsInRenameTotal(long count) {
    counterOfListObjectsInRename.incr(count);
  }
  public String listObjectsInRenameToString() {
    return counterOfListObjectsInRename.toString();
  }

  /**
   * Indicate that Front-End deleted one or more files.
   * @param count number of files.
   */
  public void frontendFileDeleted(long count, long delay) {
    counterOfFrontEndFilesDeleted.incrSuccess(count, delay);
  }
  public void frontendFileDeletedTotal(long count) {
    counterOfFrontEndFilesDeleted.incr(count);
  }

  /**
   * Indicate that front-end directory delete request was made.
   * @param count number of front-end directory entries included in the delete request.
   */
  public void frontendDirectoryDeleted(long count, long delay) {
    counterOfFrontEndDirectoryDeletes.incrSuccess(count, delay);
  }
  public void frontendDirectoryDeletedTotal(long count) {
    counterOfFrontEndDirectoryDeletes.incr(count);
  }

  /**
   * Indicate that batch request was made.
   * @param count number of batche delete request.
   */
  public void batchDeleted(long count, long delay) {
    counterOfBatchDeletes.incrSuccess(count, delay);
  }
  public void batchDeletedTotal(long count) {
    counterOfBatchDeletes.incr(count);
  }

  /**
   * Note that an error was ignored.
   */
  public void errorIgnored() {
    ignoredErrors.incr();
  }

  /**
   * Increment a specific counter.
   * No-op if not defined.
   * @param op operation
   * @param count increment value
   */
  public void incrementCounter(Statistic op, long count) {
    MutableCounterLong counter = lookupCounter(op.getSymbol());
    if (counter != null) {
      counter.incr(count);
    }
  }
  /**
   * Increment a specific counter.
   * No-op if not defined.
   * @param op operation
   * @param count atomic long containing value
   */
  public void incrementCounter(Statistic op, AtomicLong count) {
    incrementCounter(op, count.get());
  }

  /**
   * Increment a specific gauge.
   * No-op if not defined.
   * @param op operation
   * @param count increment value
   * @throws ClassCastException if the metric is of the wrong type
   */
  public void incrementGauge(Statistic op, long count) {
    MutableGaugeLong gauge = lookupGauge(op.getSymbol());
    if (gauge != null) {
      gauge.incr(count);
    } else {
      LOG.debug("No Gauge: "+ op);
    }
  }

  /**
   * Decrement a specific gauge.
   * No-op if not defined.
   * @param op operation
   * @param count increment value
   * @throws ClassCastException if the metric is of the wrong type
   */
  public void decrementGauge(Statistic op, long count) {
    MutableGaugeLong gauge = lookupGauge(op.getSymbol());
    if (gauge != null) {
      gauge.decr(count);
    } else {
      LOG.debug("No Gauge: {}", op);
    }
  }

  /**
   * Create a stream input statistics instance.
   * @return the new instance
   */
  InputStreamStatistics newInputStreamStatistics() {
    return new InputStreamStatistics();
  }

  /**
   * Merge in the statistics of a single input stream into
   * the filesystem-wide statistics.
   * @param statistics stream statistics
   */
  private void mergeInputStreamStatistics(InputStreamStatistics statistics) {
    streamOpenOperations.incr(statistics.openOperations);
    streamCloseOperations.incr(statistics.closeOperations);
    streamClosed.incr(statistics.closed);
    streamAborted.incr(statistics.aborted);
    streamSeekOperations.incr(statistics.seekOperations);
    streamReadExceptions.incr(statistics.readExceptions);
    streamForwardSeekOperations.incr(statistics.forwardSeekOperations);
    streamBytesSkippedOnSeek.incr(statistics.bytesSkippedOnSeek);
    streamBackwardSeekOperations.incr(statistics.backwardSeekOperations);
    streamBytesBackwardsOnSeek.incr(statistics.bytesBackwardsOnSeek);
    streamBytesRead.incr(statistics.bytesRead);
    streamReadOperations.incr(statistics.readOperations);
    streamReadFullyOperations.incr(statistics.readFullyOperations);
    streamReadsIncomplete.incr(statistics.readsIncomplete);
//    streamBytesReadInClose.incr(statistics.bytesReadInClose);
//    streamBytesDiscardedInAbort.incr(statistics.bytesDiscardedInAbort);
  }

  /**
   * Statistics updated by an input stream during its actual operation.
   * These counters not thread-safe and are for use in a single instance
   * of a stream.
   */
  @InterfaceAudience.Private
  @InterfaceStability.Unstable
  public final class InputStreamStatistics implements AutoCloseable {
    public long openOperations;
    public long closeOperations;
    public long closed;
    public long aborted;
    public long seekOperations;
    public long readExceptions;
    public long forwardSeekOperations;
    public long backwardSeekOperations;
    public long bytesRead;
    public long bytesSkippedOnSeek;
    public long bytesBackwardsOnSeek;
    public long readOperations;
    public long readFullyOperations;
    public long readsIncomplete;
//    public long bytesReadInClose;
//    public long bytesDiscardedInAbort;

    private InputStreamStatistics() {
    }

    /**
     * Seek backwards, incrementing the seek and backward seek counters.
     * @param negativeOffset how far was the seek?
     * This is expected to be negative.
     */
    public void seekBackwards(long negativeOffset) {
      seekOperations++;
      backwardSeekOperations++;
      bytesBackwardsOnSeek -= negativeOffset;
    }

    /**
     * Record a forward seek, adding a seek operation, a forward
     * seek operation, and any bytes skipped.
     * @param skipped number of bytes skipped by reading from the stream.
     * If the seek was implemented by a close + reopen, set this to zero.
     */
    public void seekForwards(long skipped) {
      seekOperations++;
      forwardSeekOperations++;
      if (skipped > 0) {
        bytesSkippedOnSeek += skipped;
      }
    }

    /**
     * The inner stream was opened.
     */
    public void streamOpened() {
      openOperations++;
    }

    /**
     * The inner stream was closed.
     */
    public void streamClose() {
      closeOperations++;
//      if (abortedConnection) {
//        this.aborted++;
//        bytesDiscardedInAbort += remainingInCurrentRequest;
//      } else {
//        closed++;
//        bytesReadInClose += remainingInCurrentRequest;
//      }
    }

    /**
     * An ignored stream read exception was received.
     */
    public void readException() {
      readExceptions++;
    }

    /**
     * Increment the bytes read counter by the number of bytes;
     * no-op if the argument is negative.
     * @param bytes number of bytes read
     */
    public void bytesRead(long bytes) {
      if (bytes > 0) {
        bytesRead += bytes;
      }
    }

    /**
     * A {@code read(byte[] buf, int off, int len)} operation has started.
     * @param pos starting position of the read
     * @param len length of bytes to read
     */
    public void readOperationStarted(long pos, long len) {
      readOperations++;
    }

    /**
     * A {@code PositionedRead.read(position, buffer, offset, length)}
     * operation has just started.
     * @param pos starting position of the read
     * @param len length of bytes to read
     */
    public void readFullyOperationStarted(long pos, long len) {
      readFullyOperations++;
    }

    /**
     * A read operation has completed.
     * @param requested number of requested bytes
     * @param actual the actual number of bytes
     */
    public void readOperationCompleted(int requested, int actual) {
      if (requested > actual) {
        readsIncomplete++;
      }
    }

    /**
     * Close triggers the merge of statistics into the filesystem's
     * instrumentation instance.
     */
    @Override
    public void close() {
      mergeInputStreamStatistics(this);
    }

    /**
     * String operator describes all the current statistics.
     * <b>Important: there are no guarantees as to the stability
     * of this value.</b>
     * @return the current values of the stream statistics.
     */
    @Override
    @InterfaceStability.Unstable
    public String toString() {
      final StringBuilder sb = new StringBuilder(
          "StreamStatistics{");
      sb.append("OpenOperations=").append(openOperations);
      sb.append(", CloseOperations=").append(closeOperations);
      sb.append(", Closed=").append(closed);
      sb.append(", Aborted=").append(aborted);
      sb.append(", SeekOperations=").append(seekOperations);
      sb.append(", ReadExceptions=").append(readExceptions);
      sb.append(", ForwardSeekOperations=")
          .append(forwardSeekOperations);
      sb.append(", BackwardSeekOperations=")
          .append(backwardSeekOperations);
      sb.append(", BytesSkippedOnSeek=").append(bytesSkippedOnSeek);
      sb.append(", BytesBackwardsOnSeek=").append(bytesBackwardsOnSeek);
      sb.append(", BytesRead=").append(bytesRead);
      sb.append(", BytesRead excluding skipped=")
          .append(bytesRead - bytesSkippedOnSeek);
      sb.append(", ReadOperations=").append(readOperations);
      sb.append(", ReadFullyOperations=").append(readFullyOperations);
      sb.append(", ReadsIncomplete=").append(readsIncomplete);
//      sb.append(", BytesReadInClose=").append(bytesReadInClose);
//      sb.append(", BytesDiscardedInAbort=").append(bytesDiscardedInAbort);
      sb.append('}');
      return sb.toString();
    }
  }

  /**
   * Create a stream output statistics instance.
   * @return the new instance
   */
  OutputStreamStatistics newOutputStreamStatistics(Statistics statistics) {
    return new OutputStreamStatistics(statistics);
  }

  /**
   * Merge in the statistics of a single output stream into
   * the filesystem-wide statistics.
   * @param statistics stream statistics
   */
  private void mergeOutputStreamStatistics(OutputStreamStatistics statistics) {
    incrementCounter(STREAM_WRITE_TOTAL_TIME, statistics.totalUploadDuration());
    incrementCounter(STREAM_WRITE_QUEUE_DURATION, statistics.queueDuration);
    incrementCounter(STREAM_WRITE_TOTAL_DATA, statistics.bytesUploaded);
    incrementCounter(STREAM_WRITE_BLOCK_UPLOADS,
        statistics.blockUploadsCompleted);
  }

  /**
   * Statistics updated by an output stream during its actual operation.
   * Some of these stats may be relayed. However, as block upload is
   * spans multiple
   */
  @InterfaceAudience.Private
  @InterfaceStability.Unstable
  public final class OutputStreamStatistics implements Closeable {
    private final AtomicLong blocksSubmitted = new AtomicLong(0);
    private final AtomicLong blocksInQueue = new AtomicLong(0);
    private final AtomicLong blocksActive = new AtomicLong(0);
    private final AtomicLong blockUploadsCompleted = new AtomicLong(0);
    private final AtomicLong blockUploadsFailed = new AtomicLong(0);
    private final AtomicLong bytesPendingUpload = new AtomicLong(0);

    private final AtomicLong bytesUploaded = new AtomicLong(0);
    private final AtomicLong transferDuration = new AtomicLong(0);
    private final AtomicLong queueDuration = new AtomicLong(0);
    private final AtomicLong exceptionsInMultipartFinalize = new AtomicLong(0);
    private final AtomicInteger blocksAllocated = new AtomicInteger(0);
    private final AtomicInteger blocksReleased = new AtomicInteger(0);

    private Statistics statistics;

    public OutputStreamStatistics(Statistics statistics){
      this.statistics = statistics;
    }

    /**
     * A block has been allocated.
     */
    void blockAllocated() {
      blocksAllocated.incrementAndGet();
    }

    /**
     * A block has been released.
     */
    void blockReleased() {
      blocksReleased.incrementAndGet();
    }

    /**
     * Block is queued for upload.
     */
    void blockUploadQueued(int blockSize) {
      blocksSubmitted.incrementAndGet();
      blocksInQueue.incrementAndGet();
      bytesPendingUpload.addAndGet(blockSize);
      incrementGauge(STREAM_WRITE_BLOCK_UPLOADS_PENDING, 1);
      incrementGauge(STREAM_WRITE_BLOCK_UPLOADS_DATA_PENDING, blockSize);
    }

    /** Queued block has been scheduled for upload. */
    void blockUploadStarted(long duration, int blockSize) {
      queueDuration.addAndGet(duration);
      blocksInQueue.decrementAndGet();
      blocksActive.incrementAndGet();
      incrementGauge(STREAM_WRITE_BLOCK_UPLOADS_PENDING, -1);
      incrementGauge(STREAM_WRITE_BLOCK_UPLOADS_ACTIVE, 1);
    }

    /** A block upload has completed. */
    void blockUploadCompleted(long duration, int blockSize) {
      this.transferDuration.addAndGet(duration);
      incrementGauge(STREAM_WRITE_BLOCK_UPLOADS_ACTIVE, -1);
      blocksActive.decrementAndGet();
      blockUploadsCompleted.incrementAndGet();
    }

    /**
     *  A block upload has failed.
     *  A final transfer completed event is still expected, so this
     *  does not decrement the active block counter.
     */
    void blockUploadFailed(long duration, int blockSize) {
      blockUploadsFailed.incrementAndGet();
    }

    /** Intermediate report of bytes uploaded. */
    void bytesTransferred(long byteCount) {
      bytesUploaded.addAndGet(byteCount);
      statistics.incrementBytesWritten(byteCount);
      bytesPendingUpload.addAndGet(-byteCount);
      incrementGauge(STREAM_WRITE_BLOCK_UPLOADS_DATA_PENDING, -byteCount);
    }

    /**
     * Note an exception in a multipart complete.
     */
    void exceptionInMultipartComplete() {
      exceptionsInMultipartFinalize.incrementAndGet();
    }

    /**
     * Note an exception in a multipart abort.
     */
    void exceptionInMultipartAbort() {
      exceptionsInMultipartFinalize.incrementAndGet();
    }

    /**
     * Get the number of bytes pending upload.
     * @return the number of bytes in the pending upload state.
     */
    public long getBytesPendingUpload() {
      return bytesPendingUpload.get();
    }

    /**
     * Output stream has closed.
     * Trigger merge in of all statistics not updated during operation.
     */
    @Override
    public void close() {
      if (bytesPendingUpload.get() > 0) {
        LOG.debug("Closing output stream statistics while data is still marked" +
            " as pending upload in {}", this);
      }
      mergeOutputStreamStatistics(this);
    }

    long averageQueueTime() {
      return blocksSubmitted.get() > 0 ?
          (queueDuration.get() / blocksSubmitted.get()) : 0;
    }

    double effectiveBandwidth() {
      double duration = totalUploadDuration() / 1000.0;
      return duration > 0 ?
          (bytesUploaded.get() / duration) : 0;
    }

    long totalUploadDuration() {
      return queueDuration.get() + transferDuration.get();
    }

    public int blocksAllocated() {
      return blocksAllocated.get();
    }

    public int blocksReleased() {
      return blocksReleased.get();
    }

    /**
     * Get counters of blocks actively allocated; my be inaccurate
     * if the numbers change during the (non-synchronized) calculation.
     * @return the number of actively allocated blocks.
     */
    public int blocksActivelyAllocated() {
      return blocksAllocated.get() - blocksReleased.get();
    }


    @Override
    public String toString() {
      final StringBuilder sb = new StringBuilder(
          "OutputStreamStatistics{");
      sb.append("blocksSubmitted=").append(blocksSubmitted);
      sb.append(", blocksInQueue=").append(blocksInQueue);
      sb.append(", blocksActive=").append(blocksActive);
      sb.append(", blockUploadsCompleted=").append(blockUploadsCompleted);
      sb.append(", blockUploadsFailed=").append(blockUploadsFailed);
      sb.append(", bytesPendingUpload=").append(bytesPendingUpload);
      sb.append(", bytesUploaded=").append(bytesUploaded);
      sb.append(", blocksAllocated=").append(blocksAllocated);
      sb.append(", blocksReleased=").append(blocksReleased);
      sb.append(", blocksActivelyAllocated=").append(blocksActivelyAllocated());
      sb.append(", exceptionsInMultipartFinalize=").append(
          exceptionsInMultipartFinalize);
      sb.append(", transferDuration=").append(transferDuration).append(" ms");
      sb.append(", queueDuration=").append(queueDuration).append(" ms");
      sb.append(", averageQueueTime=").append(averageQueueTime()).append(" ms");
      sb.append(", totalUploadDuration=").append(totalUploadDuration())
          .append(" ms");
      sb.append(", effectiveBandwidth=").append(effectiveBandwidth())
          .append(" bytes/s");
      sb.append('}');
      return sb.toString();
    }
  }

  public String renameToString() {
    final StringBuilder sb = new StringBuilder(
            "RenameStatistics{ ");
    sb.append(counterOfFilesRenamed.toString());
    sb.append(", ").append(counterOfDirectoriesRenamed.toString());
    sb.append(", ").append(counterOfFrontEndFilesRenamed.toString());
    sb.append(", ").append(counterOfFrontEndDirectoriesRenamed.toString());
    sb.append(", ").append(counterOfFrontEndExistedDirectoriesRenamed.toString());
    sb.append(", ").append(counterOfListObjectsInRename.toString());
    sb.append(", ").append(counterOfFilesCreated.toString());
    sb.append(", ").append(counterOfFilesCopied.toString());
    sb.append(", ").append(counterOfFilesDeleted.toString());
    sb.append(", ").append(counterOfFakeDirectoryDeletes.toString());
    sb.append(", ").append(counterOfBatchDeletes.toString());
    sb.append(", ").append(counterOfDirectoriesCreated.toString());
    sb.append(", ").append(counterOfDirectoriesDeleted.toString());
    sb.append(", ").append(counterOfFrontEndFilesDeleted.toString());
    sb.append(", ").append(counterOfFrontEndDirectoryDeletes.toString());
    sb.append("' }");
    return sb.toString();
  }
}
