package vproxybase.util.net;

import java.io.IOException;

public interface ErrorCallback {
    void error(IOException err);
}
