package org.apache.hadoop.fs.obs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.contract.ContractTestUtils;
import org.apache.hadoop.io.IOUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.io.OutputStream;
import java.util.UUID;

import static org.apache.hadoop.fs.contract.ContractTestUtils.*;

public class ITestOBSBlockOutputStream{
    private OBSFileSystem fs;
    private static String testRootPath =
            OBSTestUtils.generateUniqueTestPath();
    @Before
    public void setUp() throws Exception {
        Configuration conf = new Configuration();
        conf.setBoolean(Constants.FAST_UPLOAD, true);
        fs = OBSTestUtils.createTestFileSystem(conf);


    }

    @After
    public void tearDown() throws Exception {
        if (fs != null) {
            fs.delete(new Path(testRootPath), true);
        }
    }
    private Path getTestPath() {
        return new Path(testRootPath + "/test-obs");
    }
    /**
     *
     * @throws IOException
     */
    @Test
    public void testBlockUpload() throws IOException {
//        ContractTestUtils.createAndVerifyFile(fs, testPath, 0);
//        verifyUpload(100*1024-1);
        verifyUpload(10*1024*1024+1);
        verifyUpload(10*1024*1024);
        verifyUpload(10*1024*1024-1);
    }
    /**
     *
     * @throws IOException
     */
    @Test
    public void testZeroUpload() throws IOException {
//        ContractTestUtils.createAndVerifyFile(fs, testPath, 0);
//        verifyUpload(100*1024-1);
        verifyUpload(0);
    }
    @Test(expected = IOException.class)
    public void testWriteAfterStreamClose() throws Throwable {
        Path dest = new Path(getTestPath(), "testWriteAfterStreamClose");
        FSDataOutputStream stream = fs.create(dest, true);
        byte[] data = ContractTestUtils.dataset(16, 'a', 26);
        try {
            stream.write(data);
            stream.close();
            stream.write(data);

        } finally {
            fs.delete(dest, false);
            IOUtils.closeStream(stream);
        }
    }
    @Test
    public void testBlocksClosed() throws Throwable {
        Path dest = new Path(getTestPath(), "testBlocksClosed");

        FSDataOutputStream stream = fs.create(dest, true);
        OBSInstrumentation.OutputStreamStatistics statistics
                = OBSTestUtils.getOutputStreamStatistics(stream);
        byte[] data = ContractTestUtils.dataset(16, 'a', 26);
        stream.write(data);
        stream.close();
        fs.delete(dest, false);
        assertEquals("total allocated blocks in " + statistics,
                1, statistics.blocksAllocated());
        assertEquals("actively allocated blocks in " + statistics,
                0, statistics.blocksActivelyAllocated());
    }
    private void verifyUpload(long fileSize) throws IOException {
        int testBufferSize = fs.getConf().getInt("io.chunk.buffer.size", 128);
        int modulus = fs.getConf().getInt("io.chunk.modulus.size", 128);
        String objectName = UUID.randomUUID().toString();
        Path objectPath = new Path(getTestPath(), objectName);
        ContractTestUtils.NanoTimer timer = new ContractTestUtils.NanoTimer();
        byte[] testBuffer = new byte[testBufferSize];

        for(int i = 0; i < testBuffer.length; ++i) {
            testBuffer[i] = (byte)(i % modulus);
        }

        long bytesWritten = 0L;
        OutputStream outputStream = fs.create(objectPath, false);
        Throwable var10 = null;

        long diff;
        try {
            while(bytesWritten < fileSize) {
                diff = fileSize - bytesWritten;
                if (diff < (long)testBuffer.length) {
                    outputStream.write(testBuffer, 0, (int)diff);
                    bytesWritten += diff;
                } else {
                    outputStream.write(testBuffer);
                    bytesWritten += (long)testBuffer.length;
                }
            }

            diff = bytesWritten;
        } catch (Throwable var21) {
            var10 = var21;
            throw var21;
        } finally {
            if (outputStream != null) {
                if (var10 != null) {
                    try {
                        outputStream.close();
                    } catch (Throwable var20) {
                        var10.addSuppressed(var20);
                    }
                } else {
                    outputStream.close();
                }
            }

        }

        assertEquals(fileSize, diff);
        assertPathExists(fs, "not created successful", objectPath);
        timer.end("Time to write %d bytes", fileSize);
        bandwidth(timer, fileSize);

        try {
            verifyReceivedData(fs, objectPath, fileSize, testBufferSize, modulus);
        } finally {
            fs.delete(objectPath, false);
        }
    }

}