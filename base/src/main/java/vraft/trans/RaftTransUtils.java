package vraft.trans;

import vfd.IP;
import vproxybase.redis.entity.RESP;
import vproxybase.redis.entity.RESPArray;
import vproxybase.redis.entity.RESPBulkString;
import vraft.msg.*;

public class RaftTransUtils {
    private RaftTransUtils() {
    }

    public static String parse(IP remoteIp, RESP resp,
                               AppendEntriesRPCReqHandler appendEntriesRPCReqHandler,
                               AppendEntriesRPCRespHandler appendEntriesRPCRespHandler,
                               RequestVoteRPCReqHandler requestVoteRPCReqHandler,
                               RequestVoteRPCRespHandler requestVoteRPCRespHandler) {
        if (!(resp instanceof RESPArray)) {
            return "message should be an array";
        }
        RESPArray meta = (RESPArray) resp;
        int index = 0;
        String lastKey = null;

        String type = null;
        RESP msg = null;

        for (RESP e : meta.array) {
            if (index % 2 == 0) {
                // is key
                if (!(e instanceof RESPBulkString)) {
                    return "element at index " + index + " should be a string";
                }
                lastKey = ((RESPBulkString) e).string.toString();
            } else {
                // is value
                switch (lastKey) {
                    case "type":
                        if (type != null) {
                            return "duplicated entry for type";
                        }
                        if (!(e instanceof RESPBulkString)) {
                            return "value of type should be a string";
                        }
                        type = ((RESPBulkString) e).string.toString();
                        break;
                    case "msg":
                        if (msg != null) {
                            return "duplicated entry for msg";
                        }
                        msg = e;
                        break;
                    default:
                        return "unknown key " + lastKey;
                }
            }
            ++index;
        }

        if (type == null) {
            return "missing type";
        }
        if (msg == null) {
            return "missing msg";
        }

        try {
            switch (type) {
                case "AppendEntriesReq":
                    var appendEntriesReq = new AppendEntriesRPCReq(msg);
                    if (appendEntriesRPCReqHandler != null) {
                        appendEntriesRPCReqHandler.handle(remoteIp, appendEntriesReq);
                    }
                    break;
                case "AppendEntriesResp":
                    var appendEntriesResp = new AppendEntriesRPCResp(msg);
                    if (appendEntriesRPCRespHandler != null) {
                        appendEntriesRPCRespHandler.handle(remoteIp, appendEntriesResp);
                    }
                    break;
                case "RequestVoteReq":
                    var requestVoteReq = new RequestVoteRPCReq(msg);
                    if (requestVoteRPCReqHandler != null) {
                        requestVoteRPCReqHandler.handle(remoteIp, requestVoteReq);
                    }
                    break;
                case "RequestVoteResp":
                    var requestVoteResp = new RequestVoteRPCResp(msg);
                    if (requestVoteRPCRespHandler != null) {
                        requestVoteRPCRespHandler.handle(remoteIp, requestVoteResp);
                    }
                    break;
                default:
                    return "unknown type " + type;
            }
        } catch (InvalidMessageException e) {
            return e.getMessage();
        }
        return null;
    }
}
