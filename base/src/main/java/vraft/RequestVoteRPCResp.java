package vraft;

import vproxybase.redis.Serializer;
import vproxybase.redis.entity.RESP;
import vproxybase.redis.entity.RESPArray;
import vproxybase.redis.entity.RESPInteger;

import java.util.Arrays;

public class RequestVoteRPCResp {
    public final int term; // The currentTerm for the candidate to update.
    public final boolean voteGranted; // True means the candidate has received votes.

    public RequestVoteRPCResp(int term, boolean voteGranted) {
        this.term = term;
        this.voteGranted = voteGranted;
    }

    public RequestVoteRPCResp(RESP msg) throws Exception {
        if (!(msg instanceof RESPArray))
            throw new Exception("invalid msg: not array");
        RESPArray arr = (RESPArray) msg;
        if (arr.len != 4)
            throw new Exception("invalid msg: wrong len");
        if (!"type".equals(arr.array.get(0).getJavaObject()))
            throw new Exception("invalid msg: element at [0] is not `type`");
        if (!(arr.array.get(1) instanceof RESPInteger))
            throw new Exception("invalid msg: element at [1] is not of type integer");
        if (!"voteGranted".equals(arr.array.get(2).getJavaObject()))
            throw new Exception("invalid msg: element at [2] is not `voteGranted`");
        if (!(arr.array.get(3) instanceof RESPInteger))
            throw new Exception("invalid msg: element at [3] is not of type integer");
        int voteGranted = ((RESPInteger) arr.array.get(3)).integer;
        if (voteGranted != 0 && voteGranted != 1)
            throw new Exception("invalid msg: voteGranted is not 0 nor 1");

        this.term = ((RESPInteger) arr.array.get(1)).integer;
        this.voteGranted = voteGranted == 1;
    }

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
}
