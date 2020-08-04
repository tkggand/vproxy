package vproxybase.util.net;

import vfd.IPPort;
import vproxybase.connection.*;
import vproxybase.util.LogType;
import vproxybase.util.Logger;
import vproxybase.util.RingBuffer;

import java.io.IOException;

public class NetClient {
    private static final int IN_BUFFER_SIZE = 16384;
    private static final int OUT_BUFFER_SIZE = IN_BUFFER_SIZE;

    private final NetEventLoop loop;

    public NetClient(NetEventLoop loop) {
        this.loop = loop;
    }

    public void connect(IPPort ipport, ConnectedCallback connectedCallback, ErrorCallback errorCallback) throws IOException {
        ConnectableConnection c;
        try {
            c = ConnectableConnection.create(ipport, ConnectionOpts.getDefault(),
                RingBuffer.allocateDirect(IN_BUFFER_SIZE), RingBuffer.allocateDirect(OUT_BUFFER_SIZE));
        } catch (IOException e) {
            Logger.error(LogType.SOCKET_ERROR, "creating connectable connection failed: " + ipport, e);
            throw e;
        }
        Conn conn = new Conn(c);
        var handler = new ConnectableConnectionHandler() {
            private boolean connected = false;

            @Override
            public void connected(ConnectableConnectionHandlerContext ctx) {
                connectedCallback.connected(conn);
                if (conn.dataCallback == null) {
                    Logger.error(LogType.IMPROPER_USE, "dataCallback is not set for " + conn);
                    conn.close();
                    conn.closed();
                    return;
                }
                connected = true;
            }

            @Override
            public void readable(ConnectionHandlerContext ctx) {
                conn.readable();
            }

            @Override
            public void writable(ConnectionHandlerContext ctx) {
                conn.writable();
            }

            @Override
            public void exception(ConnectionHandlerContext ctx, IOException err) {
                if (connected) {
                    conn.exception(err);
                } else {
                    Logger.error(LogType.CONN_ERROR, "connection " + conn + " got exception before connected", err);
                    errorCallback.error(err);
                }
            }

            @Override
            public void remoteClosed(ConnectionHandlerContext ctx) {
                conn.remoteClosed();
            }

            @Override
            public void closed(ConnectionHandlerContext ctx) {
                conn.closed();
            }

            @Override
            public void removed(ConnectionHandlerContext ctx) {
                conn.removed();
            }
        };
        try {
            loop.addConnectableConnection(c, null, handler);
        } catch (IOException e) {
            Logger.error(LogType.EVENT_LOOP_ADD_FAIL, "adding conn " + c + " into event loop failed", e);
            c.close();
            throw e;
        }
    }
}
