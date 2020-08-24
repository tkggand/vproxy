package vraft.trans;

import vfd.IP;
import vraft.msg.RequestVoteRPCResp;

public interface RequestVoteRPCRespHandler {
    void handle(IP ip, RequestVoteRPCResp resp);
}
