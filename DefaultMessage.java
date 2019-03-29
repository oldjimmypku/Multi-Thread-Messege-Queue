

import java.io.Serializable;

/**
 * Created by wangzx on 2019/3/14.
 *
 */
public class DefaultMessage implements ByteMessage{

    private KeyValue headers = new DefaultKeyValue();
    private byte[] body;

    public void setHeaders(KeyValue headers) {
        this.headers = headers;
    }

    public DefaultMessage(byte[] body) {
        this.body = body;
    }

    public byte[] getBody() {
        return body;
    }

    public void setBody(byte[] body) {
        this.body = body;
    }

    public KeyValue headers() {
        return headers;
    }


    public DefaultMessage putHeaders(String key, int value) {
        headers.put(key, value);
        return this;
    }

    public DefaultMessage putHeaders(String key, long value) {
        headers.put(key, value);
        return this;
    }

    public DefaultMessage putHeaders(String key, double value) {
        headers.put(key, value);
        return this;
    }

    public DefaultMessage putHeaders(String key, String value) {
        headers.put(key, value);
        return this;
    }

}
