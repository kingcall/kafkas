package util;
/**
 * 版本是个坑
 */

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.common.TopicPartition;

import java.io.*;
import java.text.SimpleDateFormat;
import java.util.*;
public class KafkaUtil {
    static final HashMap<Integer,Long> OFFSETSMAP=new HashMap<Integer, Long>(5);
    static final HashMap<Integer,Long> TMPSMAP=new HashMap<Integer, Long>(5);
    static int stop=0;
    static String ips="10.52.7.40,10.52.7.41,10.52.7.42,10.52.7.43,10.52.7.44,10.52.7.45,10.52.7.46";
    static String tmps="master,slave1,slave2";
    static SimpleDateFormat simpleDateFormat=new SimpleDateFormat("yyyy-MM-dd");

    /**
     * 获取kafka生产者实例
     * @param ip 服务器ip
     * @param port 推送端口
     * @return 已打开的生产者实例
     */

    public static KafkaProducer<String,String> getProducer(String ip,int port){
        Properties prop = new Properties();
        String[] ips=ip.split(",");
        String tmpip="";
        for(int i=0;i<ips.length;i++){
            tmpip=tmpip+ips[i]+":"+port+",";
        }
        tmpip=tmpip.substring(0,tmpip.length()-1);
        prop.put("bootstrap.servers",tmpip);
        prop.put("acks", "0");
        prop.put("retries", 0);
        prop.put("batch.size", 16384);
        prop.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        prop.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        //打开一个生产者
        KafkaProducer<String,String> kafkaProducer = new KafkaProducer(prop);
        return kafkaProducer;

    }

    /**
     * 这里还有一个问题就是 没有初始化offset 的时候应该置为最旧或者最新的offset 而不是0
     * @param ip
     * @param port
     * @param autoCommit 当自动提交设置为true 时
     * @return
     */
    public static KafkaConsumer<String,String> getConsumer(String ip,int port,String autoCommit){
        //new一个配置文件
        String[] ips=ip.split(",");
        String tmpip="";
        for(int i=0;i<ips.length;i++){
            tmpip=tmpip+ips[i]+":"+port+",";
        }
        tmpip=tmpip.substring(0,tmpip.length()-1);
        Properties prop = new Properties();
        prop.put("bootstrap.servers",tmpip);
        prop.put("group.id", "java_api");
        prop.put("auto.commit.interval.ms", "1000");
        prop.put("enable.auto.commit", "true");
        prop.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        prop.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        KafkaConsumer<String,String>kafkaConsumer=new KafkaConsumer<String, String>(prop);
        return kafkaConsumer;
    }

    public static void main(String[] args) throws Exception {
        if (args.length!=8){
            System.out.println("输入参数有误，请重新数据,根据下述顺序依次输入参数");
            System.out.println("offset文件保存,数据保存目录,topic,分区数目,环境:(dev,product),hdfs保存路径,是否分区,hdfs连接地址(非命名空间)");
            System.exit(1);
        }else {
            getmessage(args[0],args[1],args[2],Integer.parseInt(args[3]),args[4],args[5],args[6],args[7]);
        }
    }

    /**
     *
     * @param offsetpath
     * @param datadir
     * @param topic
     * @param partitions
     * @param option 是选择测试环境还是生产环境  就是ip的不同
     * @throws IOException
     */
    public static void  getmessage(String offsetpath,String datadir,String topic,int partitions,String option,String hdfspath,String partition,String hdfsconn) throws IOException {
        String day=simpleDateFormat.format(new Date(System.currentTimeMillis()-86400000L));
        String filename=day+".txt";
        KafkaConsumer<String, String> consumer=null;
        if (option.equals("product")){
            consumer = KafkaUtil.getConsumer(ips,9092,"false");
        }else if(option.equals("test")) {
            consumer = KafkaUtil.getConsumer(tmps,9092,"false");
        }else {
            System.out.println("所选的环境不对");
            System.exit(1);
        }
        File file=new File(offsetpath);
        if (!file.exists()){
            file.createNewFile();
        }
        BufferedReader reader=new BufferedReader(new FileReader(file));
        BufferedWriter writer=initsavefile(datadir);
        initConsumer(topic,consumer,reader,partitions);
        // 这个 死循环该什么时候推出呢  根据zookeeper 上的值进行判断（但是这不是个好方法） 所以比较好的方法是等待多长一段时间，如果还没有数据就退出
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(100);
            if (records.count()!=0){
                System.out.println(String.format("此次获取的数据条数：%d", records.count()));
            }
            for (ConsumerRecord<String, String> record : records){
                writer.write(record.value()+"\r\n");
                OFFSETSMAP.put(record.partition(),record.offset());
            }
            if (!OFFSETSMAP.equals(TMPSMAP)){
                saveoffset(OFFSETSMAP,offsetpath);
                TMPSMAP.putAll(OFFSETSMAP);
            }else {
                stop++;
                if (stop>10){
                    break;
                }
            }
        }
        writer.flush();
        writer.close();
        if ("true".equals(partition)){
            String [] ss=day.split("-");
            String partitiondir="/year="+ss[0]+"/month="+ss[0]+"-"+ss[1]+"/day="+ss[0]+"-"+ss[1]+"-"+ss[2];
            HDFSUtil.uploadFile(datadir+"/"+filename,hdfspath+"/"+partitiondir+"/"+filename,hdfsconn);

        }else {
            HDFSUtil.uploadFile(datadir+"/"+filename,hdfspath+"/"+filename,hdfsconn);
        }
        File deletefile=new File(datadir+"/"+filename);
        deletefile.delete();

    }
    /**
     * @throws IOException
     * 初始化 消费者  这里是获取上次的offset 要是没有则指定offset 为0
     * @param topicname
     * @param consumer
     * @param reader
     * @throws IOException
     * @param partitions 分区的个数
     * @questions  一是所有的partition 都没有offset(首次订阅)  二是部分的partition 没有offset
     */
    public static void initConsumer(String topicname,KafkaConsumer<String,String> consumer,BufferedReader reader,int partitions) throws IOException {
        Set<Integer> sets=new HashSet<Integer>(5);
        for (int i = 0; i < partitions; i++) {
            sets.add(i);
        }
        // 初始化已有的partition
        List<TopicPartition> topicPartitions=new ArrayList<TopicPartition>(5);
        List<Long> topicPartitions_offset=new ArrayList<Long>(5);
        Set<Integer> hasinited=new HashSet<Integer>(5);
        String s=reader.readLine();
        while (s!=null) {
            String[] tmpArr = s.split(",");
            System.out.println(String.format("partition:%s \t offset:%s", tmpArr[0],tmpArr[1]));
            topicPartitions.add(new TopicPartition(topicname, new Integer(tmpArr[0]).intValue()));
            topicPartitions_offset.add(new Long(tmpArr[1]));
            hasinited.add(new Integer(tmpArr[0]).intValue());
            s=reader.readLine();
        }
        // 初始化没有初始信息的partitions 首先要判断哪些没有被初始化
        sets.removeAll(hasinited);
        for (int i:sets) {
            topicPartitions.add(new TopicPartition(topicname, i));
            topicPartitions_offset.add(0L);
        }
        consumer.assign(topicPartitions);
        // 将文件中的offset 读到内存中来
        for (int i = 0; i < topicPartitions.size(); i++) {
            consumer.seek(topicPartitions.get(i),topicPartitions_offset.get(i));
            OFFSETSMAP.put(topicPartitions.get(i).partition(),topicPartitions_offset.get(i));
            TMPSMAP.putAll(OFFSETSMAP);
        }
        reader.close();
    }

    /**
     * 保存offset  这个数据其实可以保存到 Redis
     * 在这里有个逻辑问题 就是offset 要加一 这样数据消费才不会重复
     */
    public static void saveoffset(HashMap<Integer,Long> OFFSETSMAP,String offsetpath) throws IOException {
        File file=new File(offsetpath);
        FileWriter writer= new FileWriter(file,false);
        for (Map.Entry<Integer, Long> entry : OFFSETSMAP.entrySet()){
            writer.write(entry.getKey()+","+(entry.getValue()+1)+"\r\n");
        }
        writer.flush();
        writer.close();
    }
    /**
     *  初始化数据保存
     * @param path 要保存数据的目录
     * @return  FileWriter文件对象
     */
    public static BufferedWriter initsavefile(String path) throws IOException {
        File saverecord=new File(path);
        if (!saverecord.exists()||saverecord.isFile()){
            System.out.println("初始化数据保存失败");
            return null;
        }else{
            String day=simpleDateFormat.format(new Date(System.currentTimeMillis()-86400000L));
            return new BufferedWriter(new FileWriter(path+"/"+day+".txt"));
        }
    }
}
