package vraft.trans;

import vfd.IP;
import vraft.msg.AppendEntriesRPCReq;

public interface AppendEntriesRPCReqHandler {
    void handle(IP ip, AppendEntriesRPCReq req);
}
