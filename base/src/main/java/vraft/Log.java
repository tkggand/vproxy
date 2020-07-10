package vraft;

import vproxybase.util.ByteArray;

public class Log {
    public final int term;
    public final int index;

    public final boolean binary;
    public final ByteArray data;

    public Log(int term, int index, ByteArray data) {
        this.term = term;
        this.index = index;
        this.binary = true;
        this.data = data;
    }

    public Log(int term, int index, String data) {
        this.term = term;
        this.index = index;
        this.binary = false;
        this.data = ByteArray.from(data.getBytes());
    }

    public Log(int term, int index, boolean binary, ByteArray data) {
        this.term = term;
        this.index = index;
        this.binary = binary;
        this.data = data;
    }
}
