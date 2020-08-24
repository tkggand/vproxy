package vraft.msg;

public class InvalidMessageException extends Exception {
    public InvalidMessageException(String msg) {
        super(msg);
    }
}
