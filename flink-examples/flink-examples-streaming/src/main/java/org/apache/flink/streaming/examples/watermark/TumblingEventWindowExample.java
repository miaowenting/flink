package org.apache.flink.streaming.examples.watermark;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

/**
 * Description:
 *
 * @author mwt
 * @version 1.0
 * @date 2019-12-10
 */
public class TumblingEventWindowExample {

	public static void main(String[] args) throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

		DataStream<String> socketStream = env.socketTextStream("localhost", 9999);
		DataStream<Tuple2<String, Long>> resultStream = socketStream
			// Time.seconds(3)有序的情况修改为0
			.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<String>(Time.seconds(3)) {
				@Override
				public long extractTimestamp(String element) {
					long eventTime = Long.parseLong(element.split(" ")[0]);
					System.out.println(eventTime);
					return eventTime;
				}
			})
			.map(new MapFunction<String, Tuple2<String, Long>>() {
				@Override
				public Tuple2<String, Long> map(String value) throws Exception {
					return Tuple2.of(value.split(" ")[1], 1L);
				}
			})
			.keyBy(0)
			.window(TumblingEventTimeWindows.of(Time.seconds(10)))
			.reduce(new ReduceFunction<Tuple2<String, Long>>() {
				@Override
				public Tuple2<String, Long> reduce(Tuple2<String, Long> value1, Tuple2<String, Long> value2) throws Exception {
					return new Tuple2<>(value1.f0, value1.f1 + value2.f1);
				}
			});
		resultStream.print();
		env.execute();
	}

}
