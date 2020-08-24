package vraft.msg;

import vproxybase.redis.Serializer;
import vproxybase.redis.entity.RESP;
import vproxybase.redis.entity.RESPArray;
import vproxybase.redis.entity.RESPBulkString;
import vproxybase.redis.entity.RESPInteger;

import java.util.Arrays;
import java.util.Objects;

public class RequestVoteRPCResp implements RaftMessage {
    public final int term; // The currentTerm for the candidate to update.
    public final boolean voteGranted; // True means the candidate has received votes.

    public RequestVoteRPCResp(int term, boolean voteGranted) {
        this.term = term;
        this.voteGranted = voteGranted;
    }

    public RequestVoteRPCResp(RESP data) throws InvalidMessageException {
        if (!(data instanceof RESPArray)) {
            throw new InvalidMessageException("message is not an array");
        }
        RESPArray msg = (RESPArray) data;
        int index = 0;
        String lastKey = null;

        int term = Integer.MIN_VALUE;
        int voteGranted = Integer.MIN_VALUE;

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
                    case "voteGranted":
                        if (voteGranted != Integer.MIN_VALUE) {
                            throw new InvalidMessageException("duplicated entry for voteGranted");
                        }
                        if (!(e instanceof RESPInteger)) {
                            throw new InvalidMessageException("value of voteGranted should be an integer");
                        }
                        voteGranted = ((RESPInteger) e).integer;
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
        if (voteGranted == Integer.MIN_VALUE) {
            throw new InvalidMessageException("missing voteGranted");
        }

        if (voteGranted != 1 && voteGranted != 0) {
            throw new InvalidMessageException("invalid value for voteGranted");
        }

        this.term = term;
        this.voteGranted = voteGranted == 1;
    }

    @Override
    public byte[] serialize() {
        return Serializer.fromArray(Arrays.asList(
            "type",
            "RequestVoteResp",
            "msg",
            Arrays.asList(
                "term",
                term,
                "voteGranted",
                voteGranted ? 1 : 0
            )
        ));
    }

    @Override
    public String toString() {
        return "RequestVoteRPCResp{" +
            "term=" + term +
            ", voteGranted=" + voteGranted +
            '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        RequestVoteRPCResp that = (RequestVoteRPCResp) o;
        return term == that.term &&
            voteGranted == that.voteGranted;
    }

    @Override
    public int hashCode() {
        return Objects.hash(term, voteGranted);
    }
}
