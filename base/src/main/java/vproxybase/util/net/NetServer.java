package vproxybase.util.net;

import vfd.IPPort;
import vfd.SocketFD;
import vproxybase.connection.*;
import vproxybase.util.*;

import java.io.IOException;
import java.util.function.Supplier;

public class NetServer {
    private static final int IN_BUFFER_SIZE = 16384;
    private static final int OUT_BUFFER_SIZE = IN_BUFFER_SIZE;

    private final NetEventLoop loop;
    private final Supplier<NetEventLoop> workerSupplier;

    private AcceptCallback acceptCallback;
    private ErrorCallback errorCallback = null;

    private ServerSock s;

    public NetServer(NetEventLoop loop, Supplier<NetEventLoop> workerSupplier) {
        this.loop = loop;
        this.workerSupplier = workerSupplier;
    }

    public NetServer onAccept(AcceptCallback acceptCallback) {
        if (this.acceptCallback != null) {
            throw new IllegalStateException("acceptCallback is already set");
        }
        this.acceptCallback = acceptCallback;
        return this;
    }

    public NetServer onError(ErrorCallback errorCallback) {
        if (this.errorCallback != null) {
            throw new IllegalStateException("errorCallback is already set");
        }
        this.errorCallback = errorCallback;
        return this;
    }

    public void listen(IPPort ipport) throws IOException {
        if (acceptCallback == null) {
            throw new IllegalStateException("acceptCallback is not set");
        }
        if (this.s != null) {
            throw new IllegalStateException("server is already started: " + s);
        }

        ServerSock s = ServerSock.create(ipport);
        var handler = new ServerHandler() {
            @Override
            public void acceptFail(ServerHandlerContext ctx, IOException err) {
                Logger.error(LogType.SOCKET_ERROR, "acceptFail, sock: " + s, err);
                if (errorCallback != null) {
                    errorCallback.error(err);
                }
            }

            @Override
            public void connection(ServerHandlerContext ctx, Connection connection) {
                NetEventLoop worker = workerSupplier.get();
                if (worker == null) {
                    Logger.error(LogType.NO_EVENT_LOOP, "no worker loop provided for " + s);
                    connection.close();
                    return;
                }
                Conn conn = new Conn(connection);
                acceptCallback.accept(conn); // run callback before adding into event loop

                if (conn.dataCallback == null) {
                    Logger.error(LogType.IMPROPER_USE, "user code not setting onData callback");
                    connection.close();
                    return;
                }

                var handler = new ConnectionHandler() {
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
                        conn.exception(err);
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
                    worker.addConnection(connection, null, handler);
                } catch (IOException e) {
                    Logger.error(LogType.EVENT_LOOP_ADD_FAIL, "adding accepted connection into event loop failed: " + connection, e);
                    if (errorCallback != null) {
                        errorCallback.error(e);
                    }
                    connection.close();
                }
            }

            @Override
            public Tuple<RingBuffer, RingBuffer> getIOBuffers(SocketFD channel) {
                return new Tuple<>(RingBuffer.allocateDirect(IN_BUFFER_SIZE), RingBuffer.allocateDirect(OUT_BUFFER_SIZE));
            }

            @Override
            public void removed(ServerHandlerContext ctx) {
                if (NetServer.this.s == null) {
                    return;
                }
                Logger.error(LogType.NO_EVENT_LOOP, "server sock is removed from event loop: " + s);
                s.close();
            }
        };
        try {
            loop.addServer(s, null, handler);
        } catch (IOException e) {
            Logger.error(LogType.EVENT_LOOP_ADD_FAIL, "adding server sock into event loop failed: " + s, e);
            s.close();
            throw e;
        }
        this.s = s;
    }

    public void close() {
        var s = this.s;
        if (s == null) {
            throw new IllegalStateException("server not started");
        } else {
            this.s = null;
            s.close();
        }
    }
}
