package vraft;

import vproxybase.redis.Serializer;
import vproxybase.redis.entity.RESP;
import vproxybase.redis.entity.RESPArray;
import vproxybase.redis.entity.RESPBulkString;

import java.util.Arrays;

public class IdentityRPCReq {
    public final String id;

    public IdentityRPCReq(String id) {
        this.id = id;
    }

    public IdentityRPCReq(RESP msg) throws Exception {
        if (!(msg instanceof RESPArray))
            throw new Exception("invalid msg: not array");
        RESPArray arr = (RESPArray) msg;
        if (arr.len != 2)
            throw new Exception("invalid msg: wrong len");
        if (!"id".equals(arr.array.get(0).getJavaObject()))
            throw new Exception("invalid msg: element at [0] is not `id`");
        if (!(arr.array.get(1) instanceof RESPBulkString))
            throw new Exception("invalid msg: element at [1] is not of type bulk string");

        this.id = ((RESPBulkString) arr.array.get(1)).string.toString();
    }

    public byte[] serialize() {
        return Serializer.fromArray(Arrays.asList(
            "type",
            "IdentityReq",
            "msg",
            Arrays.asList(
                "id",
                id
            )
        ));
    }

    @Override
    public String toString() {
        return "IdentityRPCReq{" +
            "id='" + id + '\'' +
            '}';
    }
}
