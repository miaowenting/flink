package org.apache.flink.streaming.examples.windowing;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * Description:
 * 翻滚的count window
 *
 * 输出结果：
 * (a,123)
 * (a,456)
 * (b,789)
 *
 * 最后一条 Tuple2.of("b", "0") 被丢弃，因为最后一条数据已经无法触发计算了
 *
 * @author mwt
 * @version 1.0
 * @date 2019-09-05
 */
public class TumbleCountWindowExample {

	public static void main(String[] args) throws Exception {
		final ParameterTool params = ParameterTool.fromArgs(args);
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.getConfig().setGlobalJobParameters(params);
		env.setParallelism(1);
		final int windowSize = params.getInt("window", 3);

		Tuple2[] elements = new Tuple2[]{
			Tuple2.of("a", "1"),
			Tuple2.of("a", "2"),
			Tuple2.of("a", "3"),
			Tuple2.of("a", "4"),
			Tuple2.of("a", "5"),
			Tuple2.of("a", "6"),
			Tuple2.of("b", "7"),
			Tuple2.of("b", "8"),
			Tuple2.of("b", "9"),
			Tuple2.of("b", "0")
		};

		// read source data
		DataStreamSource<Tuple2<String, String>> inStream = env.fromElements(elements);

		// calculate
		DataStream<Tuple2<String, String>> outStream = inStream
			.keyBy(0)
			// tumbling count window of 3 elements size
			.countWindow(windowSize)
			.reduce(
				new ReduceFunction<Tuple2<String, String>>() {
					@Override
					public Tuple2<String, String> reduce(Tuple2<String, String> value1, Tuple2<String, String> value2) throws Exception {
						return Tuple2.of(value1.f0, value1.f1 + "" + value2.f1);
					}
				}
			);
		outStream.print();
		env.execute("WindowWordCount");
	}

//	public static class StreamDataSource extends RichParallelSourceFunction<Tuple2<String, String>> {
//		private volatile boolean running = true;
//
//		@Override
//		public void run(SourceContext<Tuple2<String, String>> ctx) throws InterruptedException {
//
//			Tuple2[] elements = new Tuple2[]{
//				Tuple2.of("a", "1"),
//				Tuple2.of("a", "2"),
//				Tuple2.of("a", "3"),
//				Tuple2.of("a", "4"),
//				Tuple2.of("a", "5"),
//				Tuple2.of("a", "6"),
//				Tuple2.of("b", "7"),
//				Tuple2.of("b", "8"),
//				Tuple2.of("b", "9"),
//				Tuple2.of("b", "0")
//			};
//
//			int count = 0;
//			while (running && count < elements.length) {
//				ctx.collect(new Tuple2<>((String) elements[count].f0, (String) elements[count].f1));
//				count++;
//				Thread.sleep(1000);
//			}
//		}
//
//		@Override
//		public void cancel() {
//			running = false;
//		}
//	}
}
