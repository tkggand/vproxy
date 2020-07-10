package vraft;

public class ServerRef {
    public final boolean isThisServer;
    public final String id;

    public ServerRef(boolean isThisServer, String id) {
        this.isThisServer = isThisServer;
        this.id = id;
    }
}
