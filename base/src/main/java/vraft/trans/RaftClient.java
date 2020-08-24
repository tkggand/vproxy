package vraft.trans;

import vfd.IP;
import vfd.IPPort;
import vproxybase.connection.NetEventLoop;
import vproxybase.redis.RESPParser;
import vproxybase.redis.entity.RESP;
import vproxybase.util.ByteArray;
import vproxybase.util.LogType;
import vproxybase.util.Logger;
import vproxybase.util.RingBuffer;
import vproxybase.util.net.*;
import vraft.msg.RaftMessage;

import java.io.IOException;

public class RaftClient {
    private final NetClient client;
    private final IPPort remote;

    private AppendEntriesRPCReqHandler appendEntriesRPCReqHandler = null;
    private AppendEntriesRPCRespHandler appendEntriesRPCRespHandler = null;
    private RequestVoteRPCReqHandler requestVoteRPCReqHandler = null;
    private RequestVoteRPCRespHandler requestVoteRPCRespHandler = null;

    private ErrorCallback errorCallback = null;
    private ClosedCallback closedCallback = null;
    private Runnable connectedCallback = null;

    private boolean started = false;
    private boolean connected = false;
    private Conn conn = null;

    public RaftClient(NetEventLoop loop, IP ip, int port) {
        this.client = new NetClient(loop);
        this.remote = new IPPort(ip, port);
    }

    public void start() {
        if (errorCallback == null) {
            throw new IllegalStateException("errorCallback is not set");
        }
        if (closedCallback == null) {
            throw new IllegalStateException("closedCallback is not set");
        }
        if (connectedCallback == null) {
            throw new IllegalStateException("connectedCallback is not set");
        }

        started = true;
        if (conn != null) {
            return; // already connected
        }
        ConnectedCallback handler = conn -> {
            if (!started) {
                // the client is stopped
                closeConnection(true);
                return;
            }

            // record the connection object
            this.conn = conn;

            conn.onClosed(() -> closeConnection(true));
            conn.onError(err -> closeConnection(true));

            var o = new Object() {
                RESPParser parser = new RESPParser(65536);
            };
            conn.onData(bytes -> {
                RingBuffer rb = RingBuffer.wrap(bytes.toJavaArray());
                while (rb.used() > 0) {
                    int res = o.parser.feed(rb);
                    if (res == 0) {
                        // done
                        RESP resp = o.parser.getResult();
                        var err = handleRESP(resp);
                        if (err != null) {
                            // invalid
                            closeConnection(true);
                            Logger.error(LogType.INVALID_EXTERNAL_DATA, "connection " + conn + " received invalid data, connection is closed: " + err);
                            return;
                        }
                        o.parser = new RESPParser(65535);
                    } else {
                        var err = o.parser.getErrorMessage();
                        if (err != null) {
                            // invalid
                            closeConnection(true);
                            Logger.error(LogType.INVALID_EXTERNAL_DATA, "connection " + conn + " received invalid data, connection is closed: " + err);
                        } else {
                            // want more data
                            assert rb.used() == 0;
                        }
                        return;
                    }
                }
            });

            connected = true;
            connectedCallback.run();
        };
        try {
            client.connect(remote, handler, errorCallback);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private String handleRESP(RESP resp) {
        return RaftTransUtils.parse(remote.getAddress(), resp, appendEntriesRPCReqHandler, appendEntriesRPCRespHandler, requestVoteRPCReqHandler, requestVoteRPCRespHandler);
    }

    public void stop() {
        closeConnection(false);
    }

    private void closeConnection(boolean runCallback) {
        if (!this.started) {
            return;
        }
        this.started = false;
        this.connected = false;
        if (conn != null) {
            conn.close();
            conn = null;
        }
        if (runCallback && closedCallback != null) {
            closedCallback.closed();
        }
    }

    public boolean send(RaftMessage msg) {
        if (!connected) { // cannot send when not connected
            return false;
        }
        conn.write(ByteArray.from(msg.serialize()));
        return true;
    }

    public boolean isConnected() {
        return connected;
    }

    public RaftClient onAppendEntriesReq(AppendEntriesRPCReqHandler handler) {
        if (appendEntriesRPCReqHandler != null) {
            throw new IllegalStateException("appendEntriesRPCReqConsumer is already set");
        }
        appendEntriesRPCReqHandler = handler;
        return this;
    }

    public RaftClient onAppendEntriesResp(AppendEntriesRPCRespHandler handler) {
        if (appendEntriesRPCRespHandler != null) {
            throw new IllegalStateException("appendEntriesRPCRespConsumer is already set");
        }
        appendEntriesRPCRespHandler = handler;
        return this;
    }

    public RaftClient onRequestVoteReq(RequestVoteRPCReqHandler handler) {
        if (requestVoteRPCReqHandler != null) {
            throw new IllegalStateException("requestVoteRPCReqConsumer is already set");
        }
        requestVoteRPCReqHandler = handler;
        return this;
    }

    public RaftClient onRequestVoteResp(RequestVoteRPCRespHandler handler) {
        if (requestVoteRPCRespHandler != null) {
            throw new IllegalStateException("requestVoteRPCRespConsumer is already set");
        }
        requestVoteRPCRespHandler = handler;
        return this;
    }

    public RaftClient onError(ErrorCallback handler) {
        if (errorCallback != null) {
            throw new IllegalStateException("errorCallback is already set");
        }
        errorCallback = handler;
        return this;
    }

    public RaftClient onClosed(ClosedCallback handler) {
        if (closedCallback != null) {
            throw new IllegalStateException("closedCallback is already set");
        }
        closedCallback = handler;
        return this;
    }

    public RaftClient onConnected(Runnable handler) {
        if (connectedCallback != null) {
            throw new IllegalStateException("connectedCallback is already set");
        }
        connectedCallback = handler;
        return this;
    }
}
