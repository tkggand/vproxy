package vraft.msg;

import vproxybase.redis.Serializer;
import vproxybase.redis.entity.RESP;
import vproxybase.redis.entity.RESPArray;
import vproxybase.redis.entity.RESPBulkString;
import vproxybase.redis.entity.RESPInteger;
import vproxybase.util.ByteArray;

import java.util.*;

// https://www.sofastack.tech/en/projects/sofa-jraft/raft-introduction/
/*
 * Reject the log entry and return false if term < currentTerm.
 * Reject the log entry and return false if the log does not contain an entry at prevLogIndex whose term matches prevLogTerm.
 * If an existing entry conflicts with a new one (same index but different terms), delete the existing entry and all that follow it.
 * Append any new entries that do not exist in the log.
 * If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry).
 */

public class AppendEntriesRPCReq implements RaftMessage {
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

    public AppendEntriesRPCReq(RESP data) throws InvalidMessageException {
        if (!(data instanceof RESPArray)) {
            throw new InvalidMessageException("message is not an array");
        }
        RESPArray msg = (RESPArray) data;
        int index = 0;
        String lastKey = null;

        String leaderId = null;
        int prevLogIndex = Integer.MIN_VALUE;
        int prevLogTerm = Integer.MIN_VALUE;
        int leaderCommit = Integer.MIN_VALUE;
        List<Log> entries = null;

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
                    case "leaderId":
                        if (leaderId != null) {
                            throw new InvalidMessageException("duplicated entry for leaderId");
                        }
                        if (!(e instanceof RESPBulkString)) {
                            throw new InvalidMessageException("value of leaderId should be a string");
                        }
                        leaderId = ((RESPBulkString) e).string.toString();
                        break;
                    case "prevLogIndex":
                        if (prevLogIndex != Integer.MIN_VALUE) {
                            throw new InvalidMessageException("duplicated entry for prevLogIndex");
                        }
                        if (!(e instanceof RESPInteger)) {
                            throw new InvalidMessageException("value of prevLogIndex should be an integer");
                        }
                        prevLogIndex = ((RESPInteger) e).integer;
                        break;
                    case "prevLogTerm":
                        if (prevLogTerm != Integer.MIN_VALUE) {
                            throw new InvalidMessageException("duplicated entry for prevLogTerm");
                        }
                        if (!(e instanceof RESPInteger)) {
                            throw new InvalidMessageException("value of prevLogTerm should be an integer");
                        }
                        prevLogTerm = ((RESPInteger) e).integer;
                        break;
                    case "leaderCommit":
                        if (leaderCommit != Integer.MIN_VALUE) {
                            throw new InvalidMessageException("duplicated entry for leaderCommit");
                        }
                        if (!(e instanceof RESPInteger)) {
                            throw new InvalidMessageException("value of leaderCommit should be an integer");
                        }
                        leaderCommit = ((RESPInteger) e).integer;
                        break;
                    case "entries":
                        if (entries != null) {
                            throw new InvalidMessageException("duplicated entry for entries");
                        }
                        if (!(e instanceof RESPArray)) {
                            throw new InvalidMessageException("value of entries should be an array");
                        }
                        entries = parseEntries((RESPArray) e);
                        break;
                    default:
                        throw new InvalidMessageException("unknown key " + lastKey);
                }
            }
            ++index;
        }
        if (leaderId == null) {
            throw new InvalidMessageException("missing leaderId");
        }
        if (prevLogIndex == Integer.MIN_VALUE) {
            throw new InvalidMessageException("missing prevLogIndex");
        }
        if (prevLogTerm == Integer.MIN_VALUE) {
            throw new InvalidMessageException("missing prevLogTerm");
        }
        if (leaderCommit == Integer.MIN_VALUE) {
            throw new InvalidMessageException("missing leaderCommit");
        }
        if (entries == null) {
            throw new InvalidMessageException("missing entries");
        }

        this.leaderId = leaderId;
        this.prevLogIndex = prevLogIndex;
        this.prevLogTerm = prevLogTerm;
        this.leaderCommit = leaderCommit;
        this.entries = entries;
    }

    private List<Log> parseEntries(RESPArray array) throws InvalidMessageException {
        List<Log> ls = new LinkedList<>();
        int index = 0;
        for (RESP e : array.array) {
            if (!(e instanceof RESPArray)) {
                throw new InvalidMessageException("element at " + index + " should be an array");
            }
            RESPArray o = (RESPArray) e;
            ls.add(parseEntry(o));
            ++index;
        }
        return ls;
    }

    private Log parseEntry(RESPArray entry) throws InvalidMessageException {
        int index = 0;
        String lastKey = null;

        int term = Integer.MIN_VALUE;
        int vIndex = Integer.MIN_VALUE;
        int binary = Integer.MIN_VALUE;
        String data = null;

        for (RESP e : entry.array) {
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
                    case "index":
                        if (vIndex != Integer.MIN_VALUE) {
                            throw new InvalidMessageException("duplicated entry for index");
                        }
                        if (!(e instanceof RESPInteger)) {
                            throw new InvalidMessageException("value of index should be an integer");
                        }
                        vIndex = ((RESPInteger) e).integer;
                        break;
                    case "binary":
                        if (binary != Integer.MIN_VALUE) {
                            throw new InvalidMessageException("duplicated entry for binary");
                        }
                        if (!(e instanceof RESPInteger)) {
                            throw new InvalidMessageException("value of binary should be an integer");
                        }
                        binary = ((RESPInteger) e).integer;
                        break;
                    case "data":
                        if (data != null) {
                            throw new InvalidMessageException("duplicated entry for data");
                        }
                        if (!(e instanceof RESPBulkString)) {
                            throw new InvalidMessageException("value of data should be a string");
                        }
                        data = ((RESPBulkString) e).string.toString();
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
        if (vIndex == Integer.MIN_VALUE) {
            throw new InvalidMessageException("missing index");
        }
        if (binary == Integer.MIN_VALUE) {
            throw new InvalidMessageException("missing binary");
        }
        if (data == null) {
            throw new InvalidMessageException("missing data");
        }

        if (binary != 1 && binary != 0) {
            throw new InvalidMessageException("invalid value for binary: " + binary);
        }

        if (binary == 0) {
            return new Log(term, vIndex, data);
        } else {
            byte[] bData;
            try {
                bData = Base64.getDecoder().decode(data);
            } catch (IllegalArgumentException e) {
                throw new InvalidMessageException("invalid data: is binary but not base64");
            }
            return new Log(term, vIndex, ByteArray.from(bData));
        }
    }

    @Override
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

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        AppendEntriesRPCReq that = (AppendEntriesRPCReq) o;
        return prevLogIndex == that.prevLogIndex &&
            prevLogTerm == that.prevLogTerm &&
            leaderCommit == that.leaderCommit &&
            Objects.equals(leaderId, that.leaderId) &&
            Objects.equals(entries, that.entries);
    }

    @Override
    public int hashCode() {
        return Objects.hash(leaderId, prevLogIndex, prevLogTerm, leaderCommit, entries);
    }
}
