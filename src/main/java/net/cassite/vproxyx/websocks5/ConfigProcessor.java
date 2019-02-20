package net.cassite.vproxyx.websocks5;

import net.cassite.vproxy.component.svrgroup.ServerGroup;
import net.cassite.vproxy.dns.Resolver;
import net.cassite.vproxy.util.BlockCallback;
import net.cassite.vproxy.util.Utils;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.LinkedList;
import java.util.List;
import java.util.regex.Pattern;

public class ConfigProcessor {
    public final String fileName;
    public final ServerGroup group;
    private int listenPort = 1080;
    private List<Pattern> domains = new LinkedList<>();

    public ConfigProcessor(String fileName, ServerGroup group) {
        this.fileName = fileName;
        this.group = group;
    }

    public int getListenPort() {
        return listenPort;
    }

    public List<Pattern> getDomains() {
        return domains;
    }

    public void parse() throws Exception {
        FileInputStream inputStream = new FileInputStream(fileName);
        BufferedReader br = new BufferedReader(new InputStreamReader(inputStream));
        int step = 0;
        // 0 -> normal
        // 1 -> proxy.server.list
        // 2 -> proxy.domain.list
        String line;
        while ((line = br.readLine()) != null) {
            line = line.trim();
            if (line.isEmpty() || line.startsWith("#"))
                continue; // ignore whitespace lines and comment lines

            if (step == 0) {
                if (line.startsWith("agent.listen ")) {
                    String port = line.substring("agent.listen ".length());
                    try {
                        listenPort = Integer.parseInt(port);
                    } catch (NumberFormatException e) {
                        throw new Exception("invalid agent.listen, expecting an integer");
                    }
                    if (listenPort < 1 || listenPort > 65535) {
                        throw new Exception("invalid agent.listen, port number out of range");
                    }
                } else if (line.equals("proxy.server.list.start")) {
                    step = 1; // retrieving server list
                } else if (line.equals("proxy.domain.list.start")) {
                    step = 2;
                } else {
                    throw new Exception("unknown line: " + line);
                }
            } else if (step == 1) {
                if (line.equals("proxy.server.list.end")) {
                    step = 0; // return to normal state
                    continue;
                }
                int colonIdx = line.lastIndexOf(':');
                if (colonIdx == -1)
                    throw new Exception("invalid address:port for proxy.server.list: " + line);
                String hostPart = line.substring(0, colonIdx);
                String portPart = line.substring(colonIdx + 1);
                if (hostPart.isEmpty())
                    throw new Exception("invalid host: " + line);
                int port;
                try {
                    port = Integer.parseInt(portPart);
                } catch (NumberFormatException e) {
                    throw new Exception("invalid port: " + line);
                }
                if (port < 1 || port > 65535) {
                    throw new Exception("invalid port: " + line);
                }
                if (Utils.parseIpv4StringConsiderV6Compatible(hostPart) != null) {
                    InetAddress inet = InetAddress.getByName(hostPart);
                    group.add(hostPart, new InetSocketAddress(inet, port), InetAddress.getByName("0.0.0.0"), 10);
                } else if (Utils.isIpv6(hostPart)) {
                    InetAddress inet = InetAddress.getByName(hostPart);
                    group.add(hostPart, new InetSocketAddress(inet, port), InetAddress.getByName("::"), 10);
                } else {
                    BlockCallback<InetAddress, IOException> cb = new BlockCallback<>();
                    Resolver.getDefault().resolveV4(hostPart, cb);
                    InetAddress inet = cb.block();
                    group.add(hostPart, hostPart, new InetSocketAddress(inet, port), InetAddress.getByName("0.0.0.0"), 10);
                }
            } else {
                //noinspection ConstantConditions
                assert step == 2;
                if (line.equals("proxy.domain.list.end")) {
                    step = 0;
                    continue;
                }
                String regexp;
                if (line.startsWith("/") && line.endsWith("/")) {
                    regexp = line.substring(1, line.length() - 1);
                } else {
                    regexp = ".*" + line.replaceAll("\\.", "\\\\.") + "$";
                }
                Pattern pattern = Pattern.compile(regexp);
                domains.add(pattern);
            }
        }
    }
}
