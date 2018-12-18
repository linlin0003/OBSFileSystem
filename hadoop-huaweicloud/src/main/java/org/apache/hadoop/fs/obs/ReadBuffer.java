package org.apache.hadoop.fs.obs;

import java.util.concurrent.Future;


/**
 * Buffer with state
 */
public class ReadBuffer{


    enum STATE {
        START, ERROR, FINISH
    }


    private Future task;

    private STATE state;
    private long start;
    private long end;
    private byte[] buffer;

    public ReadBuffer(long start, long end) {
        this.start = start;
        this.end = end;
        this.state = STATE.START;
        this.buffer = new byte[(int)(end-start+1)];
        this.task = null;
    }



    public STATE getState() {
        return state;
    }

    public void setState(STATE state) {
        this.state = state;
    }

    public long getStart() {
        return start;
    }

    public void setStart(long start) {
        this.start = start;
    }

    public long getEnd() {
        return end;
    }

    public void setEnd(long end) {
        this.end = end;
    }

    public byte[] getBuffer() {
        return buffer;
    }

    public void setBuffer(byte[] buffer) {
        this.buffer = buffer;
    }

    public Future getTask() {
        return task;
    }

    public void setTask(Future task) {
        this.task = task;
    }
}
