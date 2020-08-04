package vproxy.test.cases;

import org.junit.Test;
import vfd.IPPort;
import vproxy.test.tool.Client;
import vproxybase.connection.NetEventLoop;
import vproxybase.selector.SelectorEventLoop;
import vproxybase.util.ByteArray;
import vproxybase.util.net.NetClient;
import vproxybase.util.net.NetServer;

import static org.junit.Assert.assertEquals;

public class TestNetServerClient {
    @Test
    public void simpleNetServer() throws Exception {
        SelectorEventLoop sLoop = SelectorEventLoop.open();
        NetEventLoop loop = new NetEventLoop(sLoop);
        NetServer server = new NetServer(loop, () -> loop);
        server
            .onAccept(conn -> conn.onData(conn::write))
            .listen(new IPPort("0.0.0.0", 22245));
        sLoop.loop(Thread::new);

        var cli = new Client(22245);
        cli.connect();
        String rcv = cli.sendAndRecv("hello", 5);
        assertEquals("hello", rcv);

        cli.close();
        server.close();
        sLoop.close();
    }

    @Test
    public void simpleNetClient() throws Exception {
        String[] rcv = new String[1];

        SelectorEventLoop sLoop = SelectorEventLoop.open();
        NetEventLoop loop = new NetEventLoop(sLoop);
        NetServer server = new NetServer(loop, () -> loop);
        server
            .onAccept(conn -> conn.onData(conn::write))
            .listen(new IPPort("0.0.0.0", 22246));
        NetClient cli = new NetClient(loop);
        cli.connect(new IPPort("127.0.0.1", 22246),
            conn -> {
                conn.onData(data -> {
                    rcv[0] = new String(data.toJavaArray());
                    conn.close();
                });
                conn.write(ByteArray.from("hello-world".getBytes()));
            },
            Throwable::printStackTrace);
        sLoop.loop(Thread::new);

        while (rcv[0] == null) {
            //noinspection BusyWait
            Thread.sleep(1);
        }
        assertEquals("hello-world", rcv[0]);
        server.close();
        sLoop.close();
    }
}
