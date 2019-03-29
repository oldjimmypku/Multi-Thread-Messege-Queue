
import java.io.*;
import java.nio.ByteBuffer;
import java.nio.*;
import java.nio.channels.FileChannel;
import java.util.*;
import java.lang.String;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Created by wangzx on 2019/3/14.
 */
public class Producer implements Serializable {
    private HashMap<String, ArrayList<byte[]>> outbuf = new HashMap<>();
    private HashMap<String, File> topicpath = new HashMap<>();
    private AtomicInteger count = new AtomicInteger(0);
    private static String PATH = "D:\\data\\";


    public ByteMessage createBytesMessageToTopic(String topic, byte[] body) throws IOException {
        ByteMessage msg = new DefaultMessage(body);
        msg.putHeaders(MessageHeader.TOPIC, topic);
        //msg.putHeaders(MessageHeader.SHARDING_KEY,"asd");
        if (!topicpath.containsKey(topic)) {
            try {
                File file = new File(PATH + topic);
                file.createNewFile();
                topicpath.put(topic, file);//在keyvalue接口中新定义一个put函数
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return msg;
    }

    public void flush() {


    }

    enum headerkey {
        MessageId("a"), Topic("b"), BornTimestamp("c"), BornHost("d"), StoreTimestamp("e"), StoreHost("f"), StartTime("g"), StopTime("h"),
        Timeout("i"), Priority("j"), Reliability("k"), SearchKey("l"), ScheduleExpression("m"), ShardingKey("n"), ShardingPartition("o"), TraceId("p");
        private String abbreviation;

        private headerkey(String abbreviation) {
            this.abbreviation = abbreviation;
        }

        public String getabb() {
            return abbreviation;
        }

    }

    public void send(ByteMessage defaultMessage) throws IOException {
        String topic = defaultMessage.headers().getString(MessageHeader.TOPIC);

        StringBuilder s = new StringBuilder();
        for (String key : defaultMessage.headers().keySet())//每个header存一下，用空格分开,先存类型，再存值
        {
            Object object = defaultMessage.headers().getObj(key);
            headerkey hk = Enum.valueOf(headerkey.class, key);
            if (object instanceof Integer)
                s.append("a").append(hk.getabb()).append(Integer.toString((Integer) object)).append(" ");
            if (object instanceof Double)
                s.append("b").append(hk.getabb()).append(Double.toString((Double) object)).append(" ");
            if (object instanceof Long)
                s.append("c").append(hk.getabb()).append(Long.toString((Long) object)).append(" ");
            if (object instanceof String)
                s.append("d").append(hk.getabb()).append((String) object + "").append(" ");
        }
        int lheader = s.substring(0, s.length() - 1).getBytes().length;
        byte[] headerinfo = s.substring(0, s.length() - 1).getBytes();
        int lbody = defaultMessage.getBody().length;
        byte[] bodyinfo = defaultMessage.getBody();

        byte[] bodylength = intToByteArray(lbody);
        byte[] headerlength = intToByteArray(lheader);
        byte[] intlength = unitByteArray(headerlength, bodylength);
        byte[] info = unitByteArray(headerinfo, bodyinfo);
        byte[] output = unitByteArray(intlength, info);
        ArrayList<byte[]> t=outbuf.get(topic);
        if(t==null) {
            outbuf.put(topic, new ArrayList<byte[]>());
            t = outbuf.get(topic);
        }
        t.add(output);
        count.incrementAndGet();
        if (count.get() >= 8000) {
            for (String key : outbuf.keySet()) {
                File file = topicpath.get(topic);
                DataOutputStream out = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(file, true)));
                try {
                    synchronized (file) {
                        for(int i=0;i<outbuf.get(key).size();i++)
                            out.write(outbuf.get(key).get(i));
                        out.flush();
                        out.close();
                    }

                } catch (IOException e) {
                    e.printStackTrace();
                }

            }
            String []top=new String[outbuf.keySet().size()];
            int j=0;
            for (String key : outbuf.keySet()) {
                top[j++]=key;
            }
            for(int i=0;i<top.length;i++)
                outbuf.remove(top[i],outbuf.get(top[i]));
            count.set(0);
        }
    }

    public static byte[] unitByteArray ( byte[] byte1, byte[] byte2){//两个byte[]合并
        byte[] unitByte = new byte[byte1.length + byte2.length];
        System.arraycopy(byte1, 0, unitByte, 0, byte1.length);
        System.arraycopy(byte2, 0, unitByte, byte1.length, byte2.length);
        return unitByte;
    }

    public static byte[] intToByteArray ( int i){
        byte[] result = new byte[4];
        result[0] = (byte) ((i >> 24) & 0xFF);   // 第一个是 最高 位
        result[1] = (byte) ((i >> 16) & 0xFF);
        result[2] = (byte) ((i >> 8) & 0xFF);
        result[3] = (byte) (i & 0xFF);           //最后一个是 最低位
        return result;
    }


}
