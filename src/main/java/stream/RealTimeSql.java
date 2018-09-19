package stream;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.protocol.types.Field;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;

import java.util.HashSet;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.CountDownLatch;

class WordCountDemo {
    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "streams-wordcount");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "master:9092,slave1:9092,slave2:9092");
        props.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG, MyEventTimeExtractor.class);

        // setting offset reset to earliest so that we can re-run the demo code with the same pre-loaded data
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, String> source = builder.stream("maxwell");
        source.foreach((key,value)-> {
            System.out.println(value);
            System.out.println(orginalSql(value));
        });
        final KafkaStreams streams = new KafkaStreams(builder.build(), props);
        final CountDownLatch latch = new CountDownLatch(1);

        try {
            streams.start();
            latch.await();
        } catch (Throwable e) {
            System.exit(1);
        }
        System.exit(0);
    }

    /**
     * 根据maxwel 中上报到 topic 中的数据，还原mysql数据库中执行的sql语句
     * 难点：
     * @param value maxwell的上报数据
     * @return
     */
    public static String orginalSql(String value){
        JSONObject sqlobj=JSON.parseObject(value);
        String sqltype=sqlobj.getString("type");
        if ("insert".equals(sqltype)){
            return insertSql(sqlobj);
        }else if ("delete".equals(sqltype)){
            return deleteSql(sqlobj);
        }else if ("update".equals(sqltype)){
            return updateSql(sqlobj);
        }
        return "暂时不解析";
    }

    /**
     * 插入语句
     * @param sqlobj
     * @return
     */
    public static String insertSql(JSONObject sqlobj){
        JSONObject data=sqlobj.getJSONObject("data");
        String database=sqlobj.getString("database");
        String table=sqlobj.getString("table");
        String basesql = String.format("insert into %s.%s(%s) values(%s)",database,table,"%s","%s" );
        Set<String> keys=data.keySet();
        String columns = "";
        String values = "";
        for (String key:keys){
            String value=data.getString(key);
            if (value!=null){
                columns=columns+key+",";
                values=values+"\""+value+"\""+",";
            }
        }
        if (columns==""){
            return String.format(basesql,"","");
        }else {
            return String.format(basesql, columns.substring(0,columns.length()-1),values.substring(0,values.length()-1));
        }
    }

    /**
     * 解析删除语句
     *     对于mysql一条删除一句,可能在maxwell中上报多条记录,从而导致也会解析出多条语句，虽然不会出错，但是可能会影响性能
     * @param sqlobj
     * @return
     */
    public static String deleteSql(JSONObject sqlobj){
        JSONObject data=sqlobj.getJSONObject("data");
        String database=sqlobj.getString("database");
        String table=sqlobj.getString("table");
        String basesql = String.format("delete from %s.%s where ",database,table );
        Set<String> keys=data.keySet();
        String where="";
        for (String key:keys){
            String value=data.getString(key);
            if (value!=null){
               where=where+key+"=\""+value+"\" and " ;
            }else {
                where = where +" "+key+" is null and ";
            }
        }
        return basesql + where.substring(0, where.length() - 5);
    }

    /**
     * 解析更新语句
     *     下面的解析流程要根据maxwell的数据上报格式进行
     * @param sqlobj
     * @return
     */
    public static String updateSql(JSONObject sqlobj){
        JSONObject newdata=sqlobj.getJSONObject("data");
        JSONObject olddata=sqlobj.getJSONObject("old");
        String database=sqlobj.getString("database");
        String table=sqlobj.getString("table");
        String basesql = String.format("update %s.%s set %s where %s",database,table,"%s","%s");
        Set<String> allkeys=newdata.keySet();
        Set<String> updatekeys=olddata.keySet();
        Set<String> intersection = new HashSet<>(2);
        intersection.addAll(allkeys);
        // 获取交集——也就是更新的 key
        intersection.retainAll(updatekeys);
        String set="";
        String where = "";
        for (String key:allkeys){
            if (updatekeys.contains(key)){
                set = set + key + "=" +"\""+ newdata.getString(key)+"\"" + ",";
                where=where+key+"="+"\""+olddata.getString(key)+"\""+" and ";
            }else {
                where=where+key+"="+"\""+newdata.getString(key)+"\""+" and ";
            }
        }
        return String.format(basesql, set.substring(0,set.length()-1),where.substring(0,where.length()-5));
    }
}