package vraft.trans;

import vfd.IP;
import vraft.msg.AppendEntriesRPCResp;

public interface AppendEntriesRPCRespHandler {
    void handle(IP ip, AppendEntriesRPCResp resp);
}
