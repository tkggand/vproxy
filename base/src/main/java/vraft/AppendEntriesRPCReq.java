package vraft;

import vproxybase.redis.Serializer;
import vproxybase.redis.entity.RESP;

import java.util.*;

// https://www.sofastack.tech/en/projects/sofa-jraft/raft-introduction/
/*
 * Reject the log entry and return false if term < currentTerm.
 * Reject the log entry and return false if the log does not contain an entry at prevLogIndex whose term matches prevLogTerm.
 * If an existing entry conflicts with a new one (same index but different terms), delete the existing entry and all that follow it.
 * Append any new entries that do not exist in the log.
 * If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry).
 */

public class AppendEntriesRPCReq {
    public final String leaderId; // The leader’s ID that can be used to redirect clients to the leader.
    public final int prevLogIndex; // The index of the preceding log entry.
    public final int prevLogTerm; // The term of the prevLogIndex entry.
    public final int leaderCommit; // The leader’s commitIndex (for committed log entries).
    public final List<Log> entries; // The log entries to be stored (empty for heartbeat, and the leader may send more than one for efficiency).

    public AppendEntriesRPCReq(String leaderId, int prevLogIndex, int prevLogTerm, int leaderCommit, List<Log> entries) {
        this.leaderId = leaderId;
        this.prevLogIndex = prevLogIndex;
        this.prevLogTerm = prevLogTerm;
        this.leaderCommit = leaderCommit;
        this.entries = Collections.unmodifiableList(entries);
    }

    public byte[] serialize() {
        List<List<Object>> entries = new ArrayList<>(this.entries.size());
        for (Log log : this.entries) {
            entries.add(Arrays.asList(
                "term",
                log.term,
                "index",
                log.index,
                "binary",
                log.binary ? 1 : 0,
                "data",
                log.binary ? Base64.getEncoder().encodeToString(log.data.toJavaArray()) : new String(log.data.toJavaArray())
            ));
        }

        return Serializer.fromArray(Arrays.asList(
            "type",
            "AppendEntriesReq",
            "msg",
            Arrays.asList(
                "leaderId",
                leaderId,
                "prevLogIndex",
                prevLogIndex,
                "prevLogTerm",
                prevLogTerm,
                "leaderCommit",
                leaderCommit,
                "entries",
                entries
            )
        ));
    }

    @Override
    public String toString() {
        return "AppendEntriesRPCReq{" +
            "leaderId='" + leaderId + '\'' +
            ", prevLogIndex=" + prevLogIndex +
            ", prevLogTerm=" + prevLogTerm +
            ", leaderCommit=" + leaderCommit +
            ", entries=" + entries +
            '}';
    }
}
