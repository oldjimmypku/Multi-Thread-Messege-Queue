

/**
 * Created by wangzx on 2019/3/14.
 * 字节消息接口
 *
 */
public interface ByteMessage {

    void setHeaders(KeyValue headers);
    byte[] getBody();
    void setBody(byte[] body);

    public KeyValue headers();

    public ByteMessage putHeaders(String key, int value);

    public ByteMessage putHeaders(String key, long value);

    public ByteMessage putHeaders(String key, double value);

    public ByteMessage putHeaders(String key, String value) ;

}
