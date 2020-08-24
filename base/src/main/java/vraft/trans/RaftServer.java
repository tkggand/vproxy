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
import vproxybase.util.net.Conn;
import vproxybase.util.net.NetServer;
import vraft.msg.RaftMessage;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class RaftServer {
    private final NetServer server;
    private final IPPort ipport;

    private final Map<IP, Conn> connections = new HashMap<>();

    private AppendEntriesRPCReqHandler appendEntriesRPCReqHandler = null;
    private AppendEntriesRPCRespHandler appendEntriesRPCRespHandler = null;
    private RequestVoteRPCReqHandler requestVoteRPCReqHandler = null;
    private RequestVoteRPCRespHandler requestVoteRPCRespHandler = null;

    public RaftServer(NetEventLoop loop, IP ip, int port) {
        this.server = new NetServer(loop, () -> loop);
        this.ipport = new IPPort(ip, port);

        this.server.onError(err -> {
            // do nothing for now
        });
        this.server.onAccept(conn -> {
            var remote = conn.remote();
            Logger.alert("accepted new connection from remote server: " + remote);
            var remoteIp = remote.getAddress();
            var oldConn = connections.get(remoteIp);
            if (oldConn != null) {
                oldConn.close();
                Logger.warn(LogType.ALERT, "old connection of server " + oldConn.remote().formatToIPPortString() + " is dropped");
            }
            connections.put(remoteIp, conn);
            conn.onError(err -> {
                // do nothing for now
            });
            conn.onClosed(() -> {
                removeAndCloseConnection(conn);
                Logger.warn(LogType.ALERT, "connection of server " + conn.remote().formatToIPPortString() + " is closed");
            });
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
                        var err = handleRESP(remoteIp, resp);
                        if (err != null) {
                            // invalid
                            removeAndCloseConnection(conn);
                            Logger.error(LogType.INVALID_EXTERNAL_DATA, "connection " + conn + " received invalid data, connection is closed: " + err);
                            return;
                        }
                        o.parser = new RESPParser(65535);
                    } else {
                        var err = o.parser.getErrorMessage();
                        if (err != null) {
                            // invalid
                            removeAndCloseConnection(conn);
                            Logger.error(LogType.INVALID_EXTERNAL_DATA, "connection " + conn + " received invalid data, connection is closed: " + err);
                        } else {
                            // want more data
                            assert rb.used() == 0;
                        }
                        return;
                    }
                }
            });
        });
    }

    private String handleRESP(IP remoteIp, RESP resp) {
        return RaftTransUtils.parse(remoteIp, resp, appendEntriesRPCReqHandler, appendEntriesRPCRespHandler, requestVoteRPCReqHandler, requestVoteRPCRespHandler);
    }

    private void removeAndCloseConnection(Conn conn) {
        var remoteIp = conn.remote().getAddress();
        var tmp = connections.remove(remoteIp);
        if (tmp != null && tmp != conn) {
            // to prevent some possible race condition that the recorded connection is not the closed connection
            Logger.shouldNotHappen("closed connection " + conn + " is not recorded connection " + tmp);
            connections.put(remoteIp, tmp);
        }
        conn.close();
    }

    public boolean hasConnectionFrom(IP ip) {
        return connections.containsKey(ip);
    }

    public void start() throws IOException {
        server.listen(ipport);
    }

    public void stop() {
        server.close();
    }

    public boolean sendTo(IP ip, RaftMessage msg) {
        Conn conn = connections.get(ip);
        if (conn == null) {
            return false;
        }
        conn.write(ByteArray.from(msg.serialize()));
        return true;
    }

    public void onAppendEntriesReq(AppendEntriesRPCReqHandler handler) {
        if (appendEntriesRPCReqHandler != null) {
            throw new IllegalStateException("appendEntriesRPCReqConsumer is already set");
        }
        appendEntriesRPCReqHandler = handler;
    }

    public void onAppendEntriesResp(AppendEntriesRPCRespHandler handler) {
        if (appendEntriesRPCRespHandler != null) {
            throw new IllegalStateException("appendEntriesRPCRespConsumer is already set");
        }
        appendEntriesRPCRespHandler = handler;
    }

    public void onRequestVoteReq(RequestVoteRPCReqHandler handler) {
        if (requestVoteRPCReqHandler != null) {
            throw new IllegalStateException("requestVoteRPCReqConsumer is already set");
        }
        requestVoteRPCReqHandler = handler;
    }

    public void onRequestVoteResp(RequestVoteRPCRespHandler handler) {
        if (requestVoteRPCRespHandler != null) {
            throw new IllegalStateException("requestVoteRPCRespConsumer is already set");
        }
        requestVoteRPCRespHandler = handler;
    }
}
