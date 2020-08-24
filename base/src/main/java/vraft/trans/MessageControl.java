package vraft.trans;

import vfd.IP;
import vproxybase.connection.NetEventLoop;
import vproxybase.util.LogType;
import vproxybase.util.Logger;
import vproxybase.util.net.ErrorCallback;
import vraft.msg.RaftMessage;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class MessageControl {
    private final NetEventLoop loop;
    private final RaftServer server;
    private final Map<IP, RaftClient> workingClients = new HashMap<>();
    private final Map<IP, RaftClient> pendingClients = new HashMap<>();

    private AppendEntriesRPCReqHandler appendEntriesRPCReqHandler = null;
    private AppendEntriesRPCRespHandler appendEntriesRPCRespHandler = null;
    private RequestVoteRPCReqHandler requestVoteRPCReqHandler = null;
    private RequestVoteRPCRespHandler requestVoteRPCRespHandler = null;

    public MessageControl(NetEventLoop loop, IP ip, int port) throws IOException {
        this.loop = loop;
        this.server = new RaftServer(loop, ip, port);
        initServer();
        this.server.start();
    }

    private void initServer() {
        this.server.onAppendEntriesReq((ip, msg) -> {
            if (appendEntriesRPCReqHandler != null) appendEntriesRPCReqHandler.handle(ip, msg);
        });
        this.server.onAppendEntriesResp((ip, msg) -> {
            if (appendEntriesRPCRespHandler != null) appendEntriesRPCRespHandler.handle(ip, msg);
        });
        this.server.onRequestVoteReq((ip, msg) -> {
            if (requestVoteRPCReqHandler != null) requestVoteRPCReqHandler.handle(ip, msg);
        });
        this.server.onRequestVoteResp((ip, msg) -> {
            if (requestVoteRPCRespHandler != null) requestVoteRPCRespHandler.handle(ip, msg);
        });
    }

    public void ensureConnection(IP ip, int port) {
        loop.getSelectorEventLoop().runOnLoop(() -> {
            if (server.hasConnectionFrom(ip)) {
                return;
            }
            if (workingClients.containsKey(ip)) {
                return;
            }
            if (pendingClients.containsKey(ip)) {
                return;
            }
            // need to establish connection
            newRaftClient(ip, port);
        });
    }

    public void sendTo(IP ip, int port, RaftMessage msg) {
        loop.getSelectorEventLoop().runOnLoop(() -> {
            boolean success = server.sendTo(ip, msg);
            if (success) {
                assert Logger.lowLevelDebug("sending the message via accepted connection");
                return;
            }
            var cli = workingClients.get(ip);
            if (cli != null) {
                success = cli.send(msg);
                if (!success) {
                    workingClients.remove(ip);
                    cli.stop();
                }
            }
            if (success) {
                assert Logger.lowLevelDebug("sending the message via connected connection");
                return;
            }
            cli = pendingClients.get(ip);
            if (cli != null) {
                assert Logger.lowLevelDebug("client to " + ip + " not ready yet");
                return;
            }
            assert Logger.lowLevelDebug("trying to create a new connection to " + ip + ":" + port + " for the message");
            newRaftClient(ip, port);
        });
    }

    private void newRaftClient(IP ip, int port) {
        RaftClient cli = new RaftClient(loop, ip, port);
        initClient(cli, ip);
        cli.start();
    }

    private void initClient(RaftClient cli, IP ip) {
        pendingClients.put(ip, cli);
        cli.onConnected(() -> {
            var x = pendingClients.remove(ip);
            if (x != cli) {
                Logger.shouldNotHappen("connection to " + ip + " succeeded, " +
                    "but pending client is not the same as it on the current stack");
                cli.stop();
                pendingClients.put(ip, x);
                return;
            }
            x = workingClients.put(ip, cli);
            if (x != null) {
                Logger.shouldNotHappen("existing client for " + ip + " removed");
                x.stop();
            }
            Logger.alert("connection to " + ip + " established");
        });
        ErrorCallback errorCallback = err -> {
            if (err != null) {
                Logger.error(LogType.CONN_ERROR, "connection to " + ip + " got exception", err);
            }
            var x = pendingClients.remove(ip);
            if (x != null && x != cli) {
                Logger.shouldNotHappen("connection to " + ip + " closed, " +
                    "but pending client is not the same as it on the current stack");
                pendingClients.put(ip, x);
            }
            x = workingClients.remove(ip);
            if (x != null && x != cli) {
                Logger.shouldNotHappen("connection to " + ip + " closed, " +
                    "but working client is not the same as it on the current stack");
                workingClients.put(ip, x);
            }
            if (x != null) {
                Logger.warn(LogType.ALERT, "connection to " + ip + " closed");
            }
        };
        cli.onError(errorCallback);
        cli.onClosed(() -> errorCallback.error(null));
        cli.onAppendEntriesReq((xip, msg) -> {
            if (appendEntriesRPCReqHandler != null) appendEntriesRPCReqHandler.handle(xip, msg);
        });
        cli.onAppendEntriesResp((xip, msg) -> {
            if (appendEntriesRPCRespHandler != null) appendEntriesRPCRespHandler.handle(xip, msg);
        });
        cli.onRequestVoteReq((xip, msg) -> {
            if (requestVoteRPCReqHandler != null) requestVoteRPCReqHandler.handle(xip, msg);
        });
        cli.onRequestVoteResp((xip, msg) -> {
            if (requestVoteRPCRespHandler != null) requestVoteRPCRespHandler.handle(xip, msg);
        });
    }

    public void onAppendEntriesReq(AppendEntriesRPCReqHandler handler) {
        if (appendEntriesRPCReqHandler != null) {
            throw new IllegalStateException("appendEntriesRPCReqConsumer is already set");
        }
        appendEntriesRPCReqHandler = (ip, msg) -> {
            assert Logger.lowLevelDebug("received AppendEntriesRPCReq message from " + ip + ": " + msg);
            handler.handle(ip, msg);
        };
    }

    public void onAppendEntriesResp(AppendEntriesRPCRespHandler handler) {
        if (appendEntriesRPCRespHandler != null) {
            throw new IllegalStateException("appendEntriesRPCRespConsumer is already set");
        }
        appendEntriesRPCRespHandler = (ip, msg) -> {
            assert Logger.lowLevelDebug("received AppendEntriesRPCResp message from " + ip + ": " + msg);
            handler.handle(ip, msg);
        };
    }

    public void onRequestVoteReq(RequestVoteRPCReqHandler handler) {
        if (requestVoteRPCReqHandler != null) {
            throw new IllegalStateException("requestVoteRPCReqConsumer is already set");
        }
        requestVoteRPCReqHandler = (ip, msg) -> {
            assert Logger.lowLevelDebug("received RequestVoteRPCReq message from " + ip + ": " + msg);
            handler.handle(ip, msg);
        };
    }

    public void onRequestVoteResp(RequestVoteRPCRespHandler handler) {
        if (requestVoteRPCRespHandler != null) {
            throw new IllegalStateException("requestVoteRPCRespConsumer is already set");
        }
        requestVoteRPCRespHandler = (ip, msg) -> {
            assert Logger.lowLevelDebug("received RequestVoteRPCResp message from " + ip + ": " + msg);
            handler.handle(ip, msg);
        };
    }
}
