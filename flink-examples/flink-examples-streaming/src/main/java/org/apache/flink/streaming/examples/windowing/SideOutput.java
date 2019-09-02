package org.apache.flink.streaming.examples.windowing;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.scala.OutputTag;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.util.ArrayList;
import java.util.List;

/**
 * Description:
 *
 * @author mwt
 * @version 1.0
 * @date 2019-09-02
 */
public class SideOutput {
	@SuppressWarnings("serial")
	public static void main(String[] args) throws Exception {

		final ParameterTool params = ParameterTool.fromArgs(args);
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		env.getConfig().setGlobalJobParameters(params);
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
		env.setParallelism(1);

		final boolean fileOutput = params.has("output");

		final List<Tuple3<String, Long, Integer>> input = new ArrayList<>();

		// key、时间戳、次数
		input.add(new Tuple3<>("a", 1L, 1));
		input.add(new Tuple3<>("b", 1L, 1));
		input.add(new Tuple3<>("b", 3L, 1));
		input.add(new Tuple3<>("b", 5L, 1));
		input.add(new Tuple3<>("c", 6L, 1));
		input.add(new Tuple3<>("a", 1L, 2));
		// We expect to detect the session "a" earlier than this point (the old
		// functionality can only detect here when the next starts)
		input.add(new Tuple3<>("a", 10L, 1));
		// We expect to detect session "b" and "c" at this point as well
		input.add(new Tuple3<>("c", 11L, 1));

		DataStream<Tuple3<String, Long, Integer>> source = env
			.addSource(new SourceFunction<Tuple3<String, Long, Integer>>() {
				private static final long serialVersionUID = 1L;

				@Override
				public void run(SourceContext<Tuple3<String, Long, Integer>> ctx) throws Exception {
					for (Tuple3<String, Long, Integer> value : input) {
						ctx.collectWithTimestamp(value, value.f1);
						ctx.emitWatermark(new Watermark(value.f1 - 1));
					}
					ctx.emitWatermark(new Watermark(Long.MAX_VALUE));
				}

				@Override
				public void cancel() {
				}
			});

		final OutputTag<Tuple3<String, Long, Integer>> lateOutputTag = new OutputTag<Tuple3<String, Long, Integer>>(
			"lateOutputTag", source.getType()) {
		};
		// We create sessions for each id with max timeout of 3 time units
		DataStream<Tuple3<String, Long, Integer>> aggregated = source
			.keyBy(0)
			.window(SlidingEventTimeWindows.of(Time.milliseconds(3L), Time.milliseconds(3L)))
			.allowedLateness(Time.milliseconds(2L))
			.sideOutputLateData(lateOutputTag)
			.sum(2);

		if (fileOutput) {
			aggregated.writeAsText(params.get("output"));
		} else {
			System.out.println("Printing result to stdout. Use --output to specify output path.");
//			aggregated.print();
		}

		// 旁路输出
		DataStream<Tuple3<String, Long, Integer>> lateStream =
			((SingleOutputStreamOperator<Tuple3<String, Long, Integer>>) aggregated).getSideOutput(lateOutputTag);
		lateStream.keyBy(0).sum(2).print();

		env.execute();
	}
}
