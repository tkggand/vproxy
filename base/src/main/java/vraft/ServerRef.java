package vraft;

import java.util.Objects;

public class ServerRef {
    public final boolean isThisServer;
    public final String id;

    public ServerRef(boolean isThisServer, String id) {
        this.isThisServer = isThisServer;
        this.id = id;
    }

    @Override
    public String toString() {
        return "ServerRef{" +
            "isThisServer=" + isThisServer +
            ", id='" + id + '\'' +
            '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ServerRef serverRef = (ServerRef) o;
        return isThisServer == serverRef.isThisServer &&
            Objects.equals(id, serverRef.id);
    }

    @Override
    public int hashCode() {
        return Objects.hash(isThisServer, id);
    }
}
