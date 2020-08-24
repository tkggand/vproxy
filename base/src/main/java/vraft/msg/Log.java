package vraft.msg;

import vproxybase.util.ByteArray;

import java.util.Objects;

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

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Log log = (Log) o;
        return term == log.term &&
            index == log.index &&
            binary == log.binary &&
            Objects.equals(data, log.data);
    }

    @Override
    public int hashCode() {
        return Objects.hash(term, index, binary, data);
    }

    @Override
    public String toString() {
        return "Log{" +
            "term=" + term +
            ", index=" + index +
            ", binary=" + binary +
            ", data=" + data +
            '}';
    }
}
