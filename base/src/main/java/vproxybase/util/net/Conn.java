package vproxybase.util.net;

import vproxybase.connection.Connection;
import vproxybase.util.ByteArray;
import vproxybase.util.LogType;
import vproxybase.util.Logger;
import vproxybase.util.nio.ByteArrayChannel;

import java.io.IOException;
import java.util.LinkedList;

public class Conn {
    private final Connection conn;

    // add into last, pop from first
    private final LinkedList<ByteArrayChannel> bytes = new LinkedList<>();

    DataCallback dataCallback = null;
    ErrorCallback errorCallback = null;
    ClosedCallback closedCallback = null;

    boolean closedCallbackCalled = false;

    Conn(Connection conn) {
        this.conn = conn;
    }

    public Conn onData(DataCallback dataCallback) {
        if (this.dataCallback != null) {
            throw new IllegalStateException("dataCallback is already set");
        }
        this.dataCallback = dataCallback;
        return this;
    }

    public Conn onError(ErrorCallback errorCallback) {
        if (this.errorCallback != null) {
            throw new IllegalStateException("errorCallback is already set");
        }
        this.errorCallback = errorCallback;
        return this;
    }

    public Conn onClosed(ClosedCallback closedCallback) {
        if (this.closedCallback != null) {
            throw new IllegalStateException("closedCallback is already set");
        }
        this.closedCallback = closedCallback;
        return this;
    }

    public void write(ByteArray data) {
        bytes.addLast(ByteArrayChannel.fromFull(data));
        writable();
    }

    void writable() {
        var outBuf = conn.getOutBuffer();
        while (true) {
            if (outBuf.free() <= 0) {
                // no free space yet, ignore
                return;
            }
            if (bytes.isEmpty()) {
                // no data to write
                return;
            }
            ByteArrayChannel data = bytes.peekFirst();
            if (data.used() <= 0) {
                bytes.pollFirst();
                continue;
            }
            outBuf.storeBytesFrom(data);
        }
    }

    void readable() {
        var inBuf = conn.getInBuffer();
        int used = inBuf.used();
        ByteArrayChannel chnl = ByteArrayChannel.fromEmpty(used);
        inBuf.writeTo(chnl);
        var arr = chnl.getArray();
        dataCallback.data(arr);
    }

    private void callClosedCallback() {
        if (this.closedCallbackCalled) {
            return;
        }
        if (this.closedCallback != null) {
            this.closedCallback.closed();
        }
        this.closedCallbackCalled = true;
    }

    void exception(IOException err) {
        Logger.error(LogType.CONN_ERROR, "connection " + conn + " got exception", err);
        if (this.errorCallback != null) {
            this.errorCallback.error(err);
        }
        callClosedCallback();
    }

    void remoteClosed() {
        callClosedCallback();
        this.close();
    }

    void closed() {
        callClosedCallback();
    }

    void removed() {
        if (this.closedCallbackCalled) {
            return;
        }
        callClosedCallback();
        this.close();
    }

    public void close() {
        conn.close();
    }

    @Override
    public String toString() {
        return "" + conn;
    }
}
