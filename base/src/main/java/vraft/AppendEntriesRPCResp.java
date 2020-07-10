package vraft;

import vproxybase.redis.Serializer;

import java.util.Arrays;

public class AppendEntriesRPCResp {
    public final int term; // The currentTerm for the leader to update.
    public final boolean success; // True if the follower contains log entries matching prevLogIndex and prevLogTerm.

    public AppendEntriesRPCResp(int term, boolean success) {
        this.term = term;
        this.success = success;
    }

    public byte[] serialize() {
        return Serializer.fromArray(Arrays.asList(
            "type",
            "AppendEntriesResp",
            "msg",
            Arrays.asList(
                "term",
                term,
                "success",
                success ? 1 : 0
            )
        ));
    }

    @Override
    public String toString() {
        return "AppendEntriesRPCResp{" +
            "term=" + term +
            ", success=" + success +
            '}';
    }
}
