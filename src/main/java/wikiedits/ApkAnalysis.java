package wikiedits;



import org.apache.flink.api.common.functions.MapFunction;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
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
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.WindowedTable;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.api.java.Tumble;

import org.apache.flink.table.sinks.CsvTableSink;
import org.apache.flink.table.sinks.TableSink;
import org.apache.hadoop.yarn.webapp.hamlet.HamletSpec;
import org.apache.log4j.Logger;
import org.apache.sling.commons.json.JSONObject;


import javax.annotation.Nullable;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.*;
//import java.util.regex.Pattern;

/**
 * Created by ZalaCheung on 6/20/17.
 */
public class ApkAnalysis {
    private static final Logger logger = Logger.getLogger(ApkAnalysis.class);

    public static void main(String[] args) throws Exception {
        //get Stream execute environment
        StreamExecutionEnvironment see = StreamExecutionEnvironment.getExecutionEnvironment();


        ParameterTool parameterTool = ParameterTool.fromArgs(args);
//        see.disableOperatorChaining();

        //Get table execution environment
        StreamTableEnvironment tableEnv = TableEnvironment.getTableEnvironment(see);

        //set property of Kafka and add kafka as source
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "10.82.45.18:9092");

        //Properties needed for kafka 0.8
//        properties.setProperty("bootstrap.servers", "sigma-kafka01-test.i.nease.net:9092");
//        properties.setProperty("zookeeper.connect", "sigma-kafka01-test.i.nease.net:2181/kafka");

        properties.setProperty("group.id", "test");
        properties.put("auto.offset.reset", "latest");
        properties.put("enable.auto.commit", "false");
        //Add Kafka as source
        DataStream<String> stream = see.addSource(new FlinkKafkaConsumer09<String>("flink", new SimpleStringSchema(), properties));

        //Set time characteristic to event time
        see.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        see.getConfig().setAutoWatermarkInterval(5000);
        see.setParallelism(1);

        //Set checkpoint properties
        see.enableCheckpointing(20000);
        see.getCheckpointConfig().setMinPauseBetweenCheckpoints(10000);
        see.getCheckpointConfig().setCheckpointTimeout(40000);
        see.setStateBackend(new FsStateBackend("file:///home/gzzhangdesheng/checkpoint"));


        //map string to tuple and assign Timestamp
        DataStream<Tuple4<Long,String,Integer,Integer>> withTimestamp = stream
                .map(new mapper())
                .assignTimestampsAndWatermarks(new MyTimestampsAndWatermarks());


        /**
         *
         * TABLE API Method
         * First register a table from stream, then use a tumbling window on table
         * Then use the method in table api to get the result
         * Finally write the result to specific sink
         */

        Table table = tableEnv.fromDataStream(withTimestamp,"UserActionTime.rowTime, Package,success,failure");
        WindowedTable windowedTable =table.window(Tumble.over("10.seconds").on("UserActionTime").as("UserActionWindow"));
        Table tableResult = windowedTable.groupBy("Package,UserActionWindow")
                .select("success.sum,failure.sum");
        TableSink sink = new CsvTableSink("/home/gzzhangdesheng/CvsSink", "|");

//        tableResult.writeToSink(sink);

//        tableEnv.registerDataStream("SQLTable",withTimestamp,"rowtime, Package,success,failure");
//        Table sqlResult  = tableEnv.sql(
//          "select AVG(RowTime), Package, SUM(success), SUM(failure) FROM SQLTable "
//
//                + " GROUP BY TUMBLE(rowtime, INTERVAL '1' MINUTE), Package"
//        );
//        TableSink sqlSink = new CsvTableSink("/home/gzzhangdesheng/SQLSink", "|");
//        sqlResult.writeToSink(sqlSink);

        /**
         * STREAM API METHOD
         * First key by package version
         * Use tumbling window with period 60 seconds
         * Then do reduce on each windowed stream
         * Then do mapping to map the result into json-liked string
         * Finally write output stream to sink
         */

        DataStream<Tuple4<Long,String,Integer,Integer>> filtered = withTimestamp
                .keyBy(1)
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .reduce(new ReduceFunction<Tuple4<Long, String, Integer, Integer>>() {
                    @Override
                    public Tuple4<Long, String, Integer, Integer> reduce(Tuple4<Long, String, Integer, Integer> t2, Tuple4<Long, String, Integer, Integer> t1) throws Exception {
                        logger.debug("This is a reduce message" + t2.f1);
                        return new Tuple4<>(t2.f0,t2.f1,t2.f2+t1.f2,t2.f3+t1.f3);

                    }
                });


        //map tuple to json object string
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


        /**
         * This is another way to map the String.
         * Instead of mapping String to Tuple,we map String to POJO type
         */
        DataStream<PackageInfo> pojoWithTimestamp = stream
                .map(new mapToPojo())
                .assignTimestampsAndWatermarks(new TimeStampsAndWatermarksForPojoElement());


        DataStream<PackageInfo> reducedPojo = pojoWithTimestamp
                .keyBy(new KeySelector<PackageInfo, Object>() {
                    @Override
                    public Object getKey(PackageInfo packageInfo) throws Exception {
                        return packageInfo.getPackageVersion();
                    }
                })
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .reduce(new ReduceFunction<PackageInfo>() {
                    @Override
                    public PackageInfo reduce(PackageInfo t2, PackageInfo t1) throws Exception {
                        long timestamp = (t2.getTimeStamp() + t1.getTimeStamp()) /2;
                        int success = t1.getSuccess() + t2.getSuccess();
                        int fail = t1.getFailure() + t2.getFailure();
                        return new PackageInfo(timestamp,t1.getPackageVersion(),success,fail);
                    }
                });

        reducedPojo.map(new MapFunction<PackageInfo, Object>() {
            @Override
            public String map(PackageInfo packageInfo) throws Exception {
                try {
                    String output = new JSONObject()
                            .put("time", packageInfo.getTimeStamp())
                            .put("package",packageInfo.getPackageVersion())
                            .put("success",packageInfo.getSuccess())
                            .put("failure",packageInfo.getFailure())
                            .toString();
                    return output;
                }catch (Exception e){
                    logger.info("JsonError");
                    return "error";
                }
            }
        }).writeAsText("/home/gzzhangdesheng/PojoOutput");


        /**
         * Flink CEP DEMO
         * Detect the failure rate
         * If there are two consecutive Aggregations that failure rates over 0.5
         * Raise an alarm
         */
        final Pattern<PackageInfo,?> FailureAlarm = Pattern.<PackageInfo>begin("first")
                .where(new SimpleCondition<PackageInfo>() {
                    @Override
                    public boolean filter(PackageInfo packageInfo) throws Exception {
                        return packageInfo.getSuccess() <= packageInfo.getSuccess();
                    }
                })
                .next("second")
                .where(new SimpleCondition<PackageInfo>() {
                    @Override
                    public boolean filter(PackageInfo packageInfo) throws Exception {
                        return packageInfo.getSuccess() <= packageInfo.getSuccess();
                    }
                });

        PatternStream<PackageInfo> alarmPatternStream = CEP.pattern(reducedPojo.keyBy(new KeySelector<PackageInfo, Object>() {
            @Override
            public Object getKey(PackageInfo packageInfo) throws Exception {
                return packageInfo.getPackageVersion();
            }
        }),FailureAlarm);

        DataStream<FailureRateAlarm> alarmDataStream = alarmPatternStream.select(new PatternSelectFunction<PackageInfo, FailureRateAlarm>() {
            @Override
            public FailureRateAlarm select(Map<String, List<PackageInfo>> map) throws Exception {
                PackageInfo first = map.get("first").get(0);
                PackageInfo second = map.get("second").get(0);
                int totalSucess = first.getSuccess() + second.getSuccess();
                int totalDownload = totalSucess + first.getFailure() + second.getFailure();
                float averageFailureRate = (first.getFailure() + second.getFailure()) / totalDownload;
                logger.info("--Failure Rate--  "+ averageFailureRate);
                return new FailureRateAlarm(first.getTimeStamp(),first.getPackageVersion(),averageFailureRate);
            }
        });

        alarmDataStream.map(new MapFunction<FailureRateAlarm, Object>() {
            @Override
            public String map(FailureRateAlarm failureRateAlarm) throws Exception {
                try {
                    String output = new JSONObject()
                            .put("time", failureRateAlarm.getTimestamp())
                            .put("package",failureRateAlarm.getPackageVersion())
                            .put("failureRate",failureRateAlarm.getFailureRate())
                            .toString();
                    return output;
                }catch (Exception e){
                    logger.info("JsonError");
                    return "error_Pattern_Info";
                }
            }
        }).writeAsText("/home/gzzhangdesheng/PatternOut");



        see.execute();
        tableEnv.execEnv();
    }




    public static  class  MyTimestampsAndWatermarks implements AssignerWithPeriodicWatermarks<Tuple4<Long, String, Integer, Integer>> {
//        private static final Logger logger = Logger.getLogger(MyTimestampsAndWatermarks.class);

        private final long maxOutOfOrderness = 3500;
        private long currentMaxTimestamp;

        @Override
        public long extractTimestamp(Tuple4<Long,String,Integer,Integer> tuple,long previousElementTimestamp){
            long timestamp =tuple.f0;
            logger.info("---in time stamp ----" + currentMaxTimestamp);
//            System.out.println(timestamp);
            currentMaxTimestamp = Math.max(timestamp, currentMaxTimestamp);
            return timestamp;
        }

        @Override
        public Watermark getCurrentWatermark(){
            Watermark newWatermark = new Watermark(currentMaxTimestamp-maxOutOfOrderness);
            logger.info("---  emmit watermark with timestamp" +newWatermark.getTimestamp());
            return newWatermark;
        }
    }

    public static class TimeStampsAndWatermarksForPojoElement implements AssignerWithPeriodicWatermarks<PackageInfo>{

        private final long maxOutOfOrderness = 3500;
        private long currentMaxTimestamp;
        @Nullable
        @Override
        public Watermark getCurrentWatermark() {
            Watermark newWatermark = new Watermark(currentMaxTimestamp-maxOutOfOrderness);
            logger.info("---  emmit watermark with POJO timestamp" +newWatermark.getTimestamp());
            return newWatermark;
        }

        @Override
        public long extractTimestamp(PackageInfo element, long previousElementTimestamp) {
            long timestamp =element.getTimeStamp();
            currentMaxTimestamp = Math.max(timestamp, currentMaxTimestamp);
            return timestamp;

        }
    }

    //map string to tuple with field<TimeStamp, Apk_version, success, failure>
    //For one message, if the status code is 200, success field marks 1, or failure field marks 1.
    public  static class mapper implements MapFunction<String,Tuple4<Long,String,Integer,Integer>> {
        @Override
        public Tuple4<Long,String,Integer,Integer> map(String log) throws Exception{
            String[] splited = log.split("\"");
            if(splited.length < 4){
                return new Tuple4<>(System.currentTimeMillis(),"Illegal_MSG",0,0);
            }
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


    public static class mapToPojo implements MapFunction<String,PackageInfo>{

        @Override
        public PackageInfo map(String s) throws Exception {
            String[] splited = s.split("\"");
            if(splited.length < 4){
                return new PackageInfo(System.currentTimeMillis(),"Illegal_MSG",0,0);
            }
            String TimeStamp = splited[0].trim().split("\\[")[1];
            int timeStringLength = TimeStamp.length();
            TimeStamp = TimeStamp.substring(0,timeStringLength - 1);
//            Long unixTimeStamp = timeTransfer(TimeStamp);
            Long unixTimeStamp = System.currentTimeMillis();
            String Apk = splited[1].trim().split(" ")[1];
            int len = Apk.split("/").length;
            String Apk_version = Apk.split("/")[len - 1];
            Integer Status = Integer.parseInt(splited[2].trim().split(" ")[0]);

            if(Status == 200){
                return new PackageInfo(unixTimeStamp,Apk_version,1,0);
            }else {
                return new PackageInfo(unixTimeStamp, Apk_version,0,1);
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
