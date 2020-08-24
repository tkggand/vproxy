package vraft.msg;

// https://www.sofastack.tech/en/projects/sofa-jraft/raft-introduction/
/*
 * Reject the vote and return false if term < currentTerm.
 * If votedFor is null or candidateId, and the candidate’s log is at least as up-to-date as the receiver’s log, the receiver grants a vote to the candidate, and returns true.
 */

import vproxybase.redis.Serializer;
import vproxybase.redis.entity.RESP;
import vproxybase.redis.entity.RESPArray;
import vproxybase.redis.entity.RESPBulkString;
import vproxybase.redis.entity.RESPInteger;

import java.util.Arrays;
import java.util.Objects;

public class RequestVoteRPCReq implements RaftMessage {
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

    public RequestVoteRPCReq(RESP data) throws InvalidMessageException {
        if (!(data instanceof RESPArray)) {
            throw new InvalidMessageException("message is not an array");
        }
        RESPArray msg = (RESPArray) data;
        int index = 0;
        String lastKey = null;

        int term = Integer.MIN_VALUE;
        String candidateId = null;
        int lastLogIndex = Integer.MIN_VALUE;
        int lastLogTerm = Integer.MIN_VALUE;

        for (RESP e : msg.array) {
            if (index % 2 == 0) {
                // is key
                if (!(e instanceof RESPBulkString)) {
                    throw new InvalidMessageException("element at index " + index + " should be a string");
                }
                lastKey = ((RESPBulkString) e).string.toString();
            } else {
                // is value
                switch (lastKey) {
                    case "term":
                        if (term != Integer.MIN_VALUE) {
                            throw new InvalidMessageException("duplicated entry for term");
                        }
                        if (!(e instanceof RESPInteger)) {
                            throw new InvalidMessageException("value of term should be an integer");
                        }
                        term = ((RESPInteger) e).integer;
                        break;
                    case "candidateId":
                        if (candidateId != null) {
                            throw new InvalidMessageException("duplicated entry for candidateId");
                        }
                        if (!(e instanceof RESPBulkString)) {
                            throw new InvalidMessageException("value of candidateId should be a string");
                        }
                        candidateId = ((RESPBulkString) e).string.toString();
                        break;
                    case "lastLogIndex":
                        if (lastLogIndex != Integer.MIN_VALUE) {
                            throw new InvalidMessageException("duplicated entry for lastLogIndex");
                        }
                        if (!(e instanceof RESPInteger)) {
                            throw new InvalidMessageException("value of lastLogIndex should be an integer");
                        }
                        lastLogIndex = ((RESPInteger) e).integer;
                        break;
                    case "lastLogTerm":
                        if (lastLogTerm != Integer.MIN_VALUE) {
                            throw new InvalidMessageException("duplicated entry for lastLgoTerm");
                        }
                        if (!(e instanceof RESPInteger)) {
                            throw new InvalidMessageException("value of lastLogTerm should be an integer");
                        }
                        lastLogTerm = ((RESPInteger) e).integer;
                        break;
                    default:
                        throw new InvalidMessageException("unknown key " + lastKey);
                }
            }
            ++index;
        }
        if (term == Integer.MIN_VALUE) {
            throw new InvalidMessageException("missing term");
        }
        if (candidateId == null) {
            throw new InvalidMessageException("missing candidateId");
        }
        if (lastLogIndex == Integer.MIN_VALUE) {
            throw new InvalidMessageException("missing lastLogIndex");
        }
        if (lastLogTerm == Integer.MIN_VALUE) {
            throw new InvalidMessageException("missing lastLogTerm");
        }

        this.term = term;
        this.candidateId = candidateId;
        this.lastLogIndex = lastLogIndex;
        this.lastLogTerm = lastLogTerm;
    }

    @Override
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

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        RequestVoteRPCReq that = (RequestVoteRPCReq) o;
        return term == that.term &&
            lastLogIndex == that.lastLogIndex &&
            lastLogTerm == that.lastLogTerm &&
            Objects.equals(candidateId, that.candidateId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(term, candidateId, lastLogIndex, lastLogTerm);
    }
}
