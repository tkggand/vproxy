package vproxy.test.cases;

import org.junit.Test;
import vfd.IP;
import vproxybase.connection.NetEventLoop;
import vproxybase.redis.RESPParser;
import vproxybase.redis.entity.RESP;
import vproxybase.redis.entity.RESPArray;
import vproxybase.redis.entity.RESPBulkString;
import vproxybase.selector.SelectorEventLoop;
import vproxybase.util.ByteArray;
import vproxybase.util.RingBuffer;
import vproxybase.util.nio.ByteArrayChannel;
import vraft.msg.*;
import vraft.trans.MessageControl;

import java.util.Arrays;

import static org.junit.Assert.assertEquals;

public class TestRaftMessages {
    private RESP parse(byte[] bin) {
        var parser = new RESPParser(32768);
        var rb = RingBuffer.allocate(bin.length);
        rb.storeBytesFrom(ByteArrayChannel.fromFull(bin));
        int r = parser.feed(rb);
        assertEquals(0, r);
        return parser.getResult();
    }

    @Test
    public void appendEntriesRPCReq() throws Exception {
        var req = new AppendEntriesRPCReq(
            "leaderId", 1, 2, 3,
            Arrays.asList(
                new Log(4, 5, ByteArray.from("aabb".getBytes())),
                new Log(6, 7, "ccdd")
            )
        );
        var bin = req.serialize();
        var resp = parse(bin);
        var arr = (RESPArray) resp;
        assertEquals("type", ((RESPBulkString) arr.array.get(0)).string.toString());
        assertEquals("AppendEntriesReq", ((RESPBulkString) arr.array.get(1)).string.toString());
        assertEquals("msg", ((RESPBulkString) arr.array.get(2)).string.toString());
        var deser = new AppendEntriesRPCReq(arr.array.get(3));
        assertEquals(req, deser);
    }

    @Test
    public void appendEntriesRPCResp() throws Exception {
        var req = new AppendEntriesRPCResp(1, true);
        var bin = req.serialize();
        var resp = parse(bin);
        var arr = (RESPArray) resp;
        assertEquals("type", ((RESPBulkString) arr.array.get(0)).string.toString());
        assertEquals("AppendEntriesResp", ((RESPBulkString) arr.array.get(1)).string.toString());
        assertEquals("msg", ((RESPBulkString) arr.array.get(2)).string.toString());
        var deser = new AppendEntriesRPCResp(arr.array.get(3));
        assertEquals(req, deser);
    }

    @Test
    public void appendEntriesRPCRespSuccessFalse() throws Exception {
        var req = new AppendEntriesRPCResp(123, false);
        var bin = req.serialize();
        var resp = parse(bin);
        var arr = (RESPArray) resp;
        var deser = new AppendEntriesRPCResp(arr.array.get(3));
        assertEquals(req, deser);
    }

    @Test
    public void requestVoteRPCReq() throws Exception {
        var req = new RequestVoteRPCReq(1, "abc", 2, 3);
        var bin = req.serialize();
        var resp = parse(bin);
        var arr = (RESPArray) resp;
        assertEquals("type", ((RESPBulkString) arr.array.get(0)).string.toString());
        assertEquals("RequestVoteReq", ((RESPBulkString) arr.array.get(1)).string.toString());
        assertEquals("msg", ((RESPBulkString) arr.array.get(2)).string.toString());
        var deser = new RequestVoteRPCReq(arr.array.get(3));
        assertEquals(req, deser);
    }

    @Test
    public void requestVoteRPCResp() throws Exception {
        var req = new RequestVoteRPCResp(1, true);
        var bin = req.serialize();
        var resp = parse(bin);
        var arr = (RESPArray) resp;
        assertEquals("type", ((RESPBulkString) arr.array.get(0)).string.toString());
        assertEquals("RequestVoteResp", ((RESPBulkString) arr.array.get(1)).string.toString());
        assertEquals("msg", ((RESPBulkString) arr.array.get(2)).string.toString());
        var deser = new RequestVoteRPCResp(arr.array.get(3));
        assertEquals(req, deser);
    }

    @Test
    public void requestVoteRPCRespVoteGrantedFalse() throws Exception {
        var req = new RequestVoteRPCResp(123, false);
        var bin = req.serialize();
        var resp = parse(bin);
        var arr = (RESPArray) resp;
        var deser = new RequestVoteRPCResp(arr.array.get(3));
        assertEquals(req, deser);
    }

    @SuppressWarnings("BusyWait")
    @Test
    public void sendingAndReceiving() throws Exception {
        SelectorEventLoop sLoop = SelectorEventLoop.open();
        NetEventLoop loop = new NetEventLoop(sLoop);
        sLoop.loop(Thread::new);
        MessageControl end1 = new MessageControl(loop, IP.from("127.0.0.1"), 22234);
        MessageControl end2 = new MessageControl(loop, IP.from("127.0.0.2"), 22234);

        RaftMessage[] msgHolder = new RaftMessage[1];
        end2.onAppendEntriesReq((ip, msg) -> msgHolder[0] = msg);
        end2.onAppendEntriesResp((ip, msg) -> msgHolder[0] = msg);
        end2.onRequestVoteReq((ip, msg) -> msgHolder[0] = msg);
        end2.onRequestVoteResp((ip, msg) -> msgHolder[0] = msg);
        end1.ensureConnection(IP.from("127.0.0.2"), 22234);
        Thread.sleep(100);

        var appendEntriesRPCReq = new AppendEntriesRPCReq("leaderId", 1, 2, 3,
            Arrays.asList(
                new Log(4, 5, ByteArray.from("aabb".getBytes())),
                new Log(6, 7, "ccdd")
            ));
        end1.sendTo(IP.from("127.0.0.2"), 22234, appendEntriesRPCReq);
        while (msgHolder[0] == null) {
            Thread.sleep(1);
        }
        assertEquals(appendEntriesRPCReq, msgHolder[0]);

        msgHolder[0] = null;
        var appendEntriesRPCResp = new AppendEntriesRPCResp(1, true);
        end1.sendTo(IP.from("127.0.0.2"), 22234, appendEntriesRPCResp);
        while (msgHolder[0] == null) {
            Thread.sleep(1);
        }
        assertEquals(appendEntriesRPCResp, msgHolder[0]);

        msgHolder[0] = null;
        var requestVoteRPCReq = new RequestVoteRPCReq(1, "abc", 2, 3);
        end1.sendTo(IP.from("127.0.0.2"), 22234, requestVoteRPCReq);
        while (msgHolder[0] == null) {
            Thread.sleep(1);
        }
        assertEquals(requestVoteRPCReq, msgHolder[0]);

        msgHolder[0] = null;
        var requestVoteRPCResp = new RequestVoteRPCResp(1, true);
        end1.sendTo(IP.from("127.0.0.2"), 22234, requestVoteRPCResp);
        while (msgHolder[0] == null) {
            Thread.sleep(1);
        }
        assertEquals(requestVoteRPCResp, msgHolder[0]);

        sLoop.close();
    }
}
