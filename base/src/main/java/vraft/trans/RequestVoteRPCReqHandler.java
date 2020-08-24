package vraft.trans;

import vfd.IP;
import vraft.msg.RequestVoteRPCReq;

public interface RequestVoteRPCReqHandler {
    void handle(IP ip, RequestVoteRPCReq req);
}
