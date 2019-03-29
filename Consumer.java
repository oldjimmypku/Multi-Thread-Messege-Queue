


import java.io.*;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Created by wangzx on 2019/3/14.
 */

public class Consumer  {
    private static String PATH = "D:\\data\\";
    ArrayList<String> topics = new ArrayList<>();//demo里面这里用的linkedlist，我觉得不如arraylist快。因为要随机访问
    String queue;
    private MappedByteBuffer[] mapbufarray;//内存映射数组
    private AtomicInteger arrayindex = new AtomicInteger(0);
    ArrayList<String> realtopics = new ArrayList<>();//有对应文件存在的topic数组
    private int totalnum = 0;//mapbufarray[]的长度
    private boolean empty = false;

    //在这里要文件映射到内存映射数组中,先用Fileinputstream试试
    public void attachQueue(String queuename, Collection<String> t) throws Exception {
        //线程初始化;
        if (queue != null) {
            throw new Exception("Subscribe one time only！");
        }
        queue = queuename;
        topics.addAll(t);
        for (String to : topics) {
            File file = new File(PATH + to);
            if (file.exists())
                realtopics.add(to);
        }
        totalnum = realtopics.size();
        //System.out.println(realtopics.toString()+totalnum);
        if (totalnum == 0) empty = true;
        mapbufarray = new MappedByteBuffer[totalnum];
        //read file
        int i = 0;//index
        for (String to : realtopics) {
            FileInputStream fi = new FileInputStream(new File(PATH + to));
            FileChannel fileChannel = fi.getChannel();
            long filelength = fileChannel.size();//
            mapbufarray[i++] = fileChannel.map(FileChannel.MapMode.READ_ONLY, 0, filelength);//read in memory
        }


    }


    public static int ByteArrayToint(byte[] b) {
        int i = (b[3] & 0xFF) |
                (b[2] & 0xFF) << 8 |
                (b[1] & 0xFF) << 16 |
                (b[0] & 0xFF) << 24;
        return i;
    }

    public static byte[] unitByteArray(byte[] byte1, byte[] byte2) {//两个byte[]合并
        byte[] unitByte = new byte[byte1.length + byte2.length];
        System.arraycopy(byte1, 0, unitByte, 0, byte1.length);
        System.arraycopy(byte2, 0, unitByte, byte1.length, byte2.length);
        return unitByte;
    }

    public static byte[] subByteArray(byte[] src, int begin, int count) {//截取字节数组
        byte[] bs = new byte[count];
        System.arraycopy(src, begin, bs, 0, count);
        return bs;
    }

    //取字节数组，需要判断是否跨越映射文件
   /* public byte[] qu(int length) {
        byte[] msg = new byte[length];
        int limit = mapbufarray[arrayindex.get()].limit();
        int position = mapbufarray[arrayindex.get()].position();
        if ((limit - position) >= length) {
            mapbufarray[arrayindex.get()].get(msg);
        } else//恰好处于mapbuffer连接处
        {
            byte[] msg1 = new byte[limit - position];
            mapbufarray[arrayindex.incrementAndGet()].get(msg1);
            if (arrayindex.get() >= totalnum) return null;
            byte[] msg2 = new byte[length + position - limit];
            mapbufarray[arrayindex.get()].get(msg2);
            msg = unitByteArray(msg1, msg2);
        }
        return msg;
    }*/
    enum headerkey {
        a("MessageId"), b("Topic"), c("BornTimestamp"), d("BornHost"), e("StoreTimestamp"), f("StoreHost"), g("StartTime"), h("StopTime"),
        i("Timeout"), j("Priority"), k("Reliability"), l("SearchKey"), m("ScheduleExpression"), n("ShardingKey"), o("ShardingPartition"), p("TraceId");
        private String abbreviation;

        private headerkey(String abbreviation) {
            this.abbreviation = abbreviation;
        }

        public String getabb() {
            return abbreviation;
        }

    }

    public synchronized ByteMessage poll() throws IOException {
        if (empty == true) return null;
        if (mapbufarray[arrayindex.get()].limit() == mapbufarray[arrayindex.get()].position())
            arrayindex.incrementAndGet();
        if (arrayindex.get() >= totalnum) {//所有topic都读完了
            return null;
        } else {
            //header长度
            byte[] headerlengthinfo = new byte[4];
            mapbufarray[arrayindex.get()].get(headerlengthinfo);
            int headerlength = ByteArrayToint(headerlengthinfo);
            //body长度
            byte[] bodylengthinfo = new byte[4];
            mapbufarray[arrayindex.get()].get(bodylengthinfo);
            int bodylength = ByteArrayToint(bodylengthinfo);
            //header信息
            byte[] headerbyte = new byte[headerlength];
            mapbufarray[arrayindex.get()].get(headerbyte);
            //body信息
            byte[] body = new byte[bodylength];
            mapbufarray[arrayindex.get()].get(body);
            DefaultMessage message = new DefaultMessage(body);
            //处理header信息
            String[] header = new String(headerbyte).split(" ");
            int headernum = header.length;
            for (int i = 0; i < headernum; i++) {
                byte[]headerinfo=header[i].getBytes();
                int l=headerinfo.length;
                byte[] keytp = subByteArray(headerinfo, 0, 1);
                String keytype = new String(keytp);//key类型的压缩值
                byte[] keybyte = subByteArray(headerinfo, 1, 1);
                String key = new String(keybyte);//key的压缩值。
                byte [] valuebyte=subByteArray(headerinfo,2,l-2);
                String value=new String(valuebyte);
                if (keytype.equals("a")) {
                    headerkey kv1 = Enum.valueOf(headerkey.class,key );
                    message.putHeaders(kv1.getabb(), Integer.parseInt(value));
                }
                if (keytype.equals("c")) {
                    headerkey kv2 = Enum.valueOf(headerkey.class, key);
                    message.putHeaders(kv2.getabb(), Long.parseLong(value));
                }
                if (keytype.equals("b")) {
                    headerkey kv3 = Enum.valueOf(headerkey.class, key);
                    message.putHeaders(kv3.getabb(), Double.parseDouble(value));
                }
                if (keytype.equals("d")) {
                    headerkey kv4 = Enum.valueOf(headerkey.class, key);
                    message.putHeaders(kv4.getabb(), value);
                }
            }
            return message;
        }
    }
}
