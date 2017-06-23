package wikiedits;



import org.apache.flink.api.common.functions.MapFunction;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;

import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer08;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer09;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer08;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer09;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;
import org.apache.log4j.Logger;
import org.apache.sling.commons.json.JSONObject;


import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;
import java.util.Properties;

/**
 * Created by ZalaCheung on 6/20/17.
 */
public class ApkAnalysis {
    private static final Logger logger = Logger.getLogger(ApkAnalysis.class);

    public static void main(String[] args) throws Exception {
        //get Stream execute environment
        StreamExecutionEnvironment see = StreamExecutionEnvironment.getExecutionEnvironment();
        see.setParallelism(1);
        see.disableOperatorChaining();
        see.getConfig().setAutoWatermarkInterval(10000);

        see.enableCheckpointing(20000);
        see.getCheckpointConfig().setMinPauseBetweenCheckpoints(10000);
        see.getCheckpointConfig().setCheckpointTimeout(40000);
        see.setStateBackend(new FsStateBackend("file:///home/gzzhangdesheng/checkpoint"));

        see.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        //set property of Kafka and add kafka as source
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "10.82.45.18:9092");
//        properties.setProperty("bootstrap.servers", "sigma-kafka01-test.i.nease.net:9092");
//        properties.setProperty("zookeeper.connect", "sigma-kafka01-test.i.nease.net:2181/kafka");
        properties.setProperty("group.id", "test");
        properties.put("auto.offset.reset", "latest");
        properties.put("enable.auto.commit", "false");
        DataStream<String> stream = see.addSource(new FlinkKafkaConsumer09<String>("flink", new SimpleStringSchema(), properties));

//        DataStream<String> stream=see.fromElements("222.26.160.148 - - [17/Feb/2017:00:08:16 +0800] \"GET http://xxx.yyy.com/package-1.1.3.apk HTTP/1.1\" 200 342 \"-\" \"Mozilla/5.0 Gecko/20100115 Firefox/3.6\" 166 0 \"-\" \"-\"",
//                "222.26.160.148 - - [17/Feb/2017:00:08:16 +0800] \"GET http://xxx.yyy.com/package-1.1.3.apk HTTP/1.1\" 200 342 \"-\" \"Mozilla/5.0 Gecko/20100115 Firefox/3.6\" 166 0 \"-\" \"-\"",
//                "222.26.160.148 - - [21/Jun/2017:17:40:17 +0800] \"GET http://xxx.yyy.com/package-1.1.3.apk HTTP/1.1\" 200 342 \"-\" \"Mozilla/5.0 Gecko/20100115 Firefox/3.6\" 166 0 \"-\" \"-\"");
        //map string to tuple
        DataStream<Tuple4<Long,String,Integer,Integer>> filtered = stream
                .map(new mapper())
                .assignTimestampsAndWatermarks(new MyTimestampsAndWatermarks())
                .keyBy(1)
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .reduce(new ReduceFunction<Tuple4<Long, String, Integer, Integer>>() {
                    @Override
                    public Tuple4<Long, String, Integer, Integer> reduce(Tuple4<Long, String, Integer, Integer> t2, Tuple4<Long, String, Integer, Integer> t1) throws Exception {
                        logger.debug("This is a reduce message" + t2.f1);
                        return new Tuple4<>(t2.f0,t2.f1,t2.f2+t1.f2,t2.f3+t1.f3);

                    }
                });


        //map tuple to string and sent to kafka sink
        DataStream<String> result = filtered
                .map(new MapFunction<Tuple4<Long,String,Integer,Integer>, String>() {
            @Override
            public String map(Tuple4<Long, String,Integer,Integer> tuple) {
//                return tuple.toString();
                try {
                    String output = new JSONObject()
                            .put("time", tuple.f0)
                            .put("package",tuple.f1)
                            .put("success",tuple.f2)
                            .put("failure",tuple.f3)
                            .toString();
                    return output;
                }catch (Exception e){
                    logger.info("JsonError");
                    return "error";
                }

            }
        });

        result.writeAsText("/home/gzzhangdesheng/outputFile");
//        result.print();
                result.addSink(new FlinkKafkaProducer08<>("10.82.45.18:9092", "flink_test", new SimpleStringSchema()));
//        stream.addSink(new FlinkKafkaProducer08<>("sigma-kafka01-test.i.nease.net:9092", "flink_test", new SimpleStringSchema()));
        see.execute();
    }

    public static  class  MyTimestampsAndWatermarks implements AssignerWithPeriodicWatermarks<Tuple4<Long, String, Integer, Integer>> {
//        private static final Logger logger = Logger.getLogger(MyTimestampsAndWatermarks.class);

        private final long maxOutOfOrderness = 3500;
        private long currentMaxTimestamp;

        @Override
        public long extractTimestamp(Tuple4<Long,String,Integer,Integer> tuple,long previousElementTimestamp){
            long timestamp =tuple.f0;
//            logger.info("---in time stamp ----" + currentMaxTimestamp);
//            System.out.println(timestamp);
            currentMaxTimestamp = Math.max(timestamp, currentMaxTimestamp);
            return timestamp;
        }

        @Override
        public Watermark getCurrentWatermark(){
            return new Watermark(currentMaxTimestamp-maxOutOfOrderness);
        }
    }

    //map string to tuple with field<TimeStamp, Apk_version, success, failure>
    //For one message, if the status code is 200, success field marks 1, or failure field marks 1.
    public  static class mapper implements MapFunction<String,Tuple4<Long,String,Integer,Integer>>{
        @Override
        public Tuple4<Long,String,Integer,Integer> map(String log) throws Exception{
            String[] splited = log.split("\"");
            String TimeStamp = splited[0].trim().split("\\[")[1];
            int timeStringLength = TimeStamp.length();
            TimeStamp = TimeStamp.substring(0,timeStringLength - 1);
//            Long unixTimeStamp = timeTransfer(TimeStamp);
            Long unixTimeStamp = System.currentTimeMillis();
            String Apk = splited[1].trim().split(" ")[1];
            int len = Apk.split("/").length;
            String Apk_version = Apk.split("/")[len - 1];
            Integer Status = Integer.parseInt(splited[2].trim().split(" ")[0]);
            if(Status == 200) {
                return new Tuple4<>(unixTimeStamp, Apk_version, 1,0);
            }
            else {
                return new Tuple4 <>(unixTimeStamp, Apk_version, 0, 1);
            }
        }
    }

    public static Long timeTransfer(String dateString) throws Exception{
        DateFormat dateFormat = new SimpleDateFormat("dd/MMM/yyyy:hh:mm:ss z", Locale.ENGLISH);
        Date date = dateFormat.parse(dateString);
        Long unixTime = date.getTime();
        return unixTime;
    }
}
