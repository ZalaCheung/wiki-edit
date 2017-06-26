package wikiedits;

import org.apache.flink.api.common.functions.FoldFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.Kafka08JsonTableSource;
import org.apache.flink.streaming.connectors.kafka.KafkaJsonTableSource;
import org.apache.flink.streaming.connectors.wikiedits.WikipediaEditEvent;
import org.apache.flink.streaming.connectors.wikiedits.WikipediaEditsSource;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer08;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.Types;
import org.apache.flink.table.api.java.BatchTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.log4j.Logger;

/**
 * Created by ZalaCheung on 6/11/17.
 */
public class WikipediaAnalysis {
    private static final Logger logger = Logger.getLogger(WikipediaAnalysis.class);
    public static void main(String[] args) throws Exception {



        //创建流处理环境
        StreamExecutionEnvironment see = StreamExecutionEnvironment.getExecutionEnvironment();
        //添加读取Wikipedia IRC 日志的源(source)
        DataStream<WikipediaEditEvent> edits = see.addSource(new WikipediaEditsSource());
        see.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        see.getConfig().setAutoWatermarkInterval(5000);
        see.setParallelism(1);



        //由于关心的是每个用户在特定的时间窗口增加或者删除内容的字节数。
        //所以在此我们需要制定用户名作为数据流的key字段
        KeyedStream<WikipediaEditEvent, String> keyedEdits = edits
                .assignTimestampsAndWatermarks(new MyTimestampsAndWatermarks())
                .keyBy(new KeySelector<WikipediaEditEvent, String>() {
                    @Override
                    public String getKey(WikipediaEditEvent event) {
                        return event.getUser();
                    }
                });


        //.timeWindow()方法指定了我们需要一个大小为5秒钟的滚动窗口(非重叠)
        //.fold方法指定了对每个窗口分片中每个唯一key做 Fold transformation转换
        DataStream<String> result = keyedEdits
//                .timeWindow(Time.seconds(10))
                .window(TumblingEventTimeWindows.of(Time.seconds(3)))
                .fold(new Tuple2<>("", 0L), new FoldFunction<WikipediaEditEvent, Tuple2<String, Long>>() {
                    @Override
                    public Tuple2<String, Long> fold(Tuple2<String, Long> acc, WikipediaEditEvent event) {

                        acc.f0 = event.getUser();
                        acc.f1 += event.getByteDiff();
                        logger.info("---in fold function ----" + event.getUser());
                        return acc;
                    }
                })
                .map(new MapFunction<Tuple2<String,Long>, String>() {
                    @Override
                    public String map(Tuple2<String, Long> tuple) {
                        logger.info("result--string"+tuple.toString());
                        return tuple.toString();
                    }
                });
        logger.info("--  pre result --");
        result.print();
        result.writeAsText("/Users/ZalaCheung/Documents/wiki_ouput");

//        DataStream<String> printed_result = result
//                .map(new MapFunction<Tuple2<String,Long>, String>() {
//                    @Override
//                    public String map(Tuple2<String, Long> tuple) {
//                        return tuple.toString();
//                    }
//                });
        logger.info("--  result --");
//        printed_result.print();
//                .addSink(new FlinkKafkaProducer08<>("sigma-kafka01-test.i.nease.net:9092", "flink_test", new SimpleStringSchema()));
        see.execute();
    }

//    private static class MyTimestampsAndWatermarks extends AscendingTimestampExtractor<WikipediaEditEvent>{
//
//        @Override
//        public long extractAscendingTimestamp(WikipediaEditEvent element) {
//            return System.currentTimeMillis()/1000L;
//        }
//    }

    public static  class  MyTimestampsAndWatermarks implements AssignerWithPeriodicWatermarks<WikipediaEditEvent> {
//        private static final Logger logger = Logger.getLogger(MyTimestampsAndWatermarks.class);

        private final long maxOutOfOrderness = 2000;
        private long currentMaxTimestamp;

        @Override
        public long extractTimestamp(WikipediaEditEvent tuple,long previousElementTimestamp){
            long timestamp = System.currentTimeMillis();

//            System.out.println(timestamp);
            currentMaxTimestamp = Math.max(timestamp, currentMaxTimestamp);
            logger.info("---in time stamp ----" + currentMaxTimestamp);
            return timestamp;
        }

        @Override
        public Watermark getCurrentWatermark(){


//            System.out.println(System.currentTimeMillis());
            Watermark current = new Watermark(System.currentTimeMillis() - maxOutOfOrderness);
            logger.info("---in watermark ----" + current.getTimestamp());
            return current;
//            return new Watermark(System.currentTimeMillis()/1000L - maxOutOfOrderness);
//            return null;
        }
    }
}
