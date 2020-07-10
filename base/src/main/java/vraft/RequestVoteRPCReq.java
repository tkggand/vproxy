package vraft;

// https://www.sofastack.tech/en/projects/sofa-jraft/raft-introduction/
/*
 * Reject the vote and return false if term < currentTerm.
 * If votedFor is null or candidateId, and the candidate’s log is at least as up-to-date as the receiver’s log, the receiver grants a vote to the candidate, and returns true.
 */

import vproxybase.redis.Serializer;

import java.util.Arrays;

public class RequestVoteRPCReq {
    public final int term; // The candidate’s term.
    public final String candidateId; // The candidate initiating a vote request.
    public final int lastLogIndex; // The index of the candidate’s last log entry.
    public final int lastLogTerm; // The term of the candidate’s last log entry.

    public RequestVoteRPCReq(int term, String candidateId, int lastLogIndex, int lastLogTerm) {
        this.term = term;
        this.candidateId = candidateId;
        this.lastLogIndex = lastLogIndex;
        this.lastLogTerm = lastLogTerm;
    }

    public byte[] serialize() {
        return Serializer.fromArray(Arrays.asList(
            "type",
            "RequestVoteReq",
            "msg",
            Arrays.asList(
                "term",
                term,
                "candidateId",
                candidateId,
                "lastLogIndex",
                lastLogIndex,
                "lastLogTerm",
                lastLogTerm
            )
        ));
    }

    @Override
    public String toString() {
        return "RequestVoteRPCReq{" +
            "term=" + term +
            ", candidateId='" + candidateId + '\'' +
            ", lastLogIndex=" + lastLogIndex +
            ", lastLogTerm=" + lastLogTerm +
            '}';
    }
}
