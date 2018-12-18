package org.apache.hadoop.fs.obs;

import java.io.IOException;
import java.io.InputStream;
import java.io.InterruptedIOException;
import java.util.concurrent.Callable;

import com.obs.services.ObsClient;
import com.obs.services.internal.io.UnrecoverableIOException;
import com.obs.services.model.GetObjectRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import com.google.common.io.ByteStreams;

public class MultiReadTask implements Callable<Void> {

    private final int RETRY_TIME=3;
    private String bucket;
    private String key;
    private ObsClient client;
    private ReadBuffer readBuffer;
    private static final Logger LOG=LoggerFactory.getLogger(MultiReadTask.class);
    @Override
    public Void call() throws Exception {
        GetObjectRequest request = new GetObjectRequest(bucket, key);
        request.setRangeStart(readBuffer.getStart());
        request.setRangeEnd(readBuffer.getEnd());


        InputStream stream = null;
        readBuffer.setState(ReadBuffer.STATE.ERROR);

        boolean interrupted=false;

        for (int i = 0 ; i< RETRY_TIME ; i++) {
            try {
                if (Thread.interrupted()){
                   throw new InterruptedException("Interrupted read task");
                }
                stream=client.getObject(request).getObjectContent();
                ByteStreams.readFully(stream,readBuffer.getBuffer(),0,(int)(readBuffer.getEnd()-readBuffer.getStart()+1));
                readBuffer.setState(ReadBuffer.STATE.FINISH);

                return null;
            }catch (IOException e){
                if (e instanceof InterruptedIOException){
                    //LOG.info("Buffer closed, task abort");
                    interrupted=true;
                    throw e;
                }
                LOG.warn("IOException occurred in Read task",e);
                readBuffer.setState(ReadBuffer.STATE.ERROR);
                if (i==RETRY_TIME-1){
                    throw e;
                }
            }
            catch (Exception e){
                readBuffer.setState(ReadBuffer.STATE.ERROR);
                if (e instanceof UnrecoverableIOException){
                    //LOG.info("Buffer closed, task abort");
                    interrupted=true;
                    throw e;
                }else {
                    LOG.warn("Exception occurred in Read task",e);
                    if (i==RETRY_TIME-1){
                        throw e;
                    }
                }
            }finally {
                //Return to connection pool
                if (stream != null) {
                    stream.close();
                }

                //SLEEP
                if (!interrupted && readBuffer.getState()!= ReadBuffer.STATE.FINISH){
                    try {
                        Thread.sleep(3000);
                    } catch (InterruptedException e) {
                        //TODO
                        interrupted=true;
                        throw e;
                    }
                }
            }
        }
        return null;
    }

    public MultiReadTask(String bucket, String key, ObsClient client, ReadBuffer readBuffer) {
        this.bucket = bucket;
        this.key = key;
        this.client = client;
        this.readBuffer = readBuffer;
    }
}
