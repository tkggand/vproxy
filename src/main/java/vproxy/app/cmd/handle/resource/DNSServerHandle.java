package vproxy.app.cmd.handle.resource;

import vproxy.app.Application;
import vproxy.app.cmd.Command;
import vproxy.app.cmd.Param;
import vproxy.app.cmd.Resource;
import vproxy.app.cmd.handle.param.AddrHandle;
import vproxy.app.cmd.handle.param.TTLHandle;
import vproxy.component.elgroup.EventLoopGroup;
import vproxy.component.exception.NotFoundException;
import vproxy.component.svrgroup.Upstream;
import vproxy.dns.DNSServer;
import vproxy.util.Utils;

import java.net.InetSocketAddress;
import java.util.LinkedList;
import java.util.List;

public class DNSServerHandle {
    private DNSServerHandle() {
    }

    @SuppressWarnings("Duplicates")
    public static void checkCreateDNSServer(Command cmd) throws Exception {
        if (!cmd.args.containsKey(Param.addr))
            throw new Exception("missing argument " + Param.addr.fullname);
        if (!cmd.args.containsKey(Param.ups))
            throw new Exception("missing argument " + Param.ups.fullname);

        AddrHandle.check(cmd);
        if (cmd.args.containsKey(Param.ttl)) {
            TTLHandle.check(cmd);
        }
    }

    public static DNSServer get(Resource dnsServer) throws NotFoundException {
        return Application.get().dnsServerHolder.get(dnsServer.alias);
    }

    public static List<String> names() {
        return Application.get().dnsServerHolder.names();
    }

    public static List<DNSServerRef> details() throws Exception {
        List<DNSServerRef> result = new LinkedList<>();
        for (String name : names()) {
            result.add(new DNSServerRef(
                Application.get().dnsServerHolder.get(name)
            ));
        }
        return result;
    }

    @SuppressWarnings("Duplicates")
    public static void add(Command cmd) throws Exception {
        if (!cmd.args.containsKey(Param.elg)) {
            cmd.args.put(Param.elg, Application.DEFAULT_WORKER_EVENT_LOOP_GROUP_NAME);
        }

        String alias = cmd.resource.alias;
        EventLoopGroup eventLoopGroup = Application.get().eventLoopGroupHolder.get(cmd.args.get(Param.elg));
        InetSocketAddress addr = AddrHandle.get(cmd);
        Upstream backend = Application.get().upstreamHolder.get(cmd.args.get(Param.ups));
        int ttl;
        if (cmd.args.containsKey(Param.ttl)) {
            ttl = TTLHandle.get(cmd);
        } else {
            ttl = 0;
        }
        Application.get().dnsServerHolder.add(alias, addr, eventLoopGroup, backend, ttl);
    }

    public static void checkUpdateDNSServer(Command cmd) throws Exception {
        if (cmd.args.containsKey(Param.ttl))
            TTLHandle.check(cmd);
    }

    public static void update(Command cmd) throws Exception {
        DNSServer dnsServer = get(cmd.resource);

        if (cmd.args.containsKey(Param.ttl)) {
            dnsServer.ttl = TTLHandle.get(cmd);
        }
    }

    public static void forceRemove(Command cmd) throws Exception {
        Application.get().dnsServerHolder.removeAndStop(cmd.resource.alias);
    }

    public static class DNSServerRef {
        public final DNSServer dnsServer;

        public DNSServerRef(DNSServer dnsServer) {
            this.dnsServer = dnsServer;
        }

        @Override
        public String toString() {
            return dnsServer.alias + " -> event-loop-group " + dnsServer.eventLoopGroup.alias
                + " bind " + Utils.ipStr(dnsServer.bindAddress.getAddress().getAddress()) + ":" + dnsServer.bindAddress.getPort()
                + " rrsets " + dnsServer.rrsets.alias
                + " ttl " + dnsServer.ttl;
        }
    }
}