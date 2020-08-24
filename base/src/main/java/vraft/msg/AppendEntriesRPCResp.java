package vraft.msg;

import vproxybase.redis.Serializer;
import vproxybase.redis.entity.RESP;
import vproxybase.redis.entity.RESPArray;
import vproxybase.redis.entity.RESPBulkString;
import vproxybase.redis.entity.RESPInteger;

import java.util.Arrays;
import java.util.Objects;

public class AppendEntriesRPCResp implements RaftMessage {
    public final int term; // The currentTerm for the leader to update.
    public final boolean success; // True if the follower contains log entries matching prevLogIndex and prevLogTerm.

    public AppendEntriesRPCResp(int term, boolean success) {
        this.term = term;
        this.success = success;
    }

    public AppendEntriesRPCResp(RESP data) throws InvalidMessageException {
        if (!(data instanceof RESPArray)) {
            throw new InvalidMessageException("message is not an array");
        }
        RESPArray msg = (RESPArray) data;
        int index = 0;
        String lastKey = null;

        int term = Integer.MIN_VALUE;
        int success = Integer.MIN_VALUE;

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
                    case "success":
                        if (success != Integer.MIN_VALUE) {
                            throw new InvalidMessageException("duplicated entry for success");
                        }
                        if (!(e instanceof RESPInteger)) {
                            throw new InvalidMessageException("value of success should be an integer");
                        }
                        success = ((RESPInteger) e).integer;
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
        if (success == Integer.MIN_VALUE) {
            throw new InvalidMessageException("missing success");
        }

        if (success != 1 && success != 0) {
            throw new InvalidMessageException("invalid value for success: " + success);
        }

        this.term = term;
        this.success = success == 1;
    }

    @Override
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

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        AppendEntriesRPCResp that = (AppendEntriesRPCResp) o;
        return term == that.term &&
            success == that.success;
    }

    @Override
    public int hashCode() {
        return Objects.hash(term, success);
    }
}
