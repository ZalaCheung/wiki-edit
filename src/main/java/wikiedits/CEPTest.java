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
import org.apache.log4j.Logger;
import org.apache.sling.commons.json.JSONObject;
import scala.tools.cmd.gen.AnyVals;


import javax.annotation.Nullable;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * Created by ZalaCheung on 7/4/17.
 */
public class CEPTest {

    private static final Logger logger = Logger.getLogger(CEPTest.class);

    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment see = StreamExecutionEnvironment.getExecutionEnvironment();
        see.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        see.getConfig().setAutoWatermarkInterval(5000);
        see.setParallelism(1);

        ParameterTool parameterTool = ParameterTool.fromArgs(args);
        see.disableOperatorChaining();


        //set property of Kafka and add kafka as source
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "10.82.45.18:9092");


        properties.setProperty("group.id", "test");
        properties.put("auto.offset.reset", "latest");
        properties.put("enable.auto.commit", "false");
        //Add Kafka as source
        DataStream<String> stream = see.addSource(new FlinkKafkaConsumer09<String>("CEP", new SimpleStringSchema(), properties));

        DataStream<TestEvent> InputStream = stream.map(new MapFunction<String, TestEvent>() {
            @Override
            public TestEvent map(String s) throws Exception {
                logger.info("------- input String ----------"+ s);
                String[] SplittedString = s.split(" ");
                if(SplittedString.length != 2){

                    return new TestEvent( "error",0);
                }
                logger.info("------- input String[0] ----------"+ SplittedString[0]);
                logger.info("------- input String[1] ----------"+ SplittedString[1]);
                return new TestEvent(SplittedString[0],Integer.valueOf(SplittedString[1]));
            }
        }).assignTimestampsAndWatermarks(new MyTimestampsAndWatermarks());


        final Pattern<TestEvent,?> testEventPattern = Pattern.<TestEvent>begin("first")
                .where(new SimpleCondition<TestEvent>() {
                    @Override
                    public boolean filter(TestEvent testEvent) throws Exception {
                        logger.info("---  eventType1:" +testEvent.getEventType());
                        if(testEvent.getEventType() == "a")
                            logger.info("------- Matched event ------");
                        else
                            logger.info(("------ unmatched ------"));
                        return testEvent.getEventType().equals("a");
                    }
                })
                .next("second")
                .where(new SimpleCondition<TestEvent>() {
                    @Override
                    public boolean filter(TestEvent testEvent) throws Exception {
                        logger.info("---  eventType2:" +testEvent.getEventType());
                        return testEvent.getEventType().equals("b");
                    }
                });

        PatternStream<TestEvent> testEventPatternStream = CEP.pattern(InputStream,testEventPattern);

        DataStream<PatternResult> patternResultDataStream = testEventPatternStream.select(new PatternSelectFunction<TestEvent, PatternResult>() {
            @Override
            public PatternResult select(Map<String, List<TestEvent>> map) throws Exception {
                StringBuilder resultString = new StringBuilder();
                resultString.append("a");
                for(TestEvent testEvent:map.get("first")){
                    resultString.append(String.valueOf(testEvent.getValue()));
                }
                resultString.append("b");
                resultString.append(String.valueOf(String.valueOf(map.get("second").get(0).getValue())));
                logger.info("---  output String:" + resultString.toString());
                return new PatternResult(resultString.toString());
            }
        });

        patternResultDataStream.writeAsText("/Users/ZalaCheung/Documents/testResult");

        see.execute();


    }

    public static class MyTimestampsAndWatermarks implements AssignerWithPeriodicWatermarks<TestEvent>{

        private final long maxOutOfOrderness = 1000;
        private long currentMaxTimestamp;
        @Nullable
        @Override
        public Watermark getCurrentWatermark() {
            Watermark newWatermark = new Watermark(currentMaxTimestamp-maxOutOfOrderness);
            logger.info("---  emmit watermark with timestamp" +newWatermark.getTimestamp());
            return newWatermark;
        }

        @Override
        public long extractTimestamp(TestEvent testEvent, long l) {
            long timeStamp = testEvent.getTimeStamp();
            logger.info("------- TimeStamp ----------"+ String.valueOf(timeStamp));
            currentMaxTimestamp = Math.max(timeStamp, currentMaxTimestamp);
            return timeStamp;
        }
    }

}
