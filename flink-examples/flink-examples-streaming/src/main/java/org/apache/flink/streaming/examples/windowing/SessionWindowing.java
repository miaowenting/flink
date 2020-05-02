/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.examples.windowing;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.util.ArrayList;
import java.util.List;

/**
 * An example of session windowing that keys events by ID and groups and counts them in
 * session with gaps of 3 milliseconds.
 *
 * 输出结果：
 * (a,1,1)
 *
 * (b,1,3)
 * (c,6,1)
 *
 * (a,10,1)
 * (c,11,1)
 *
 * watermark生成与触发计算详情如下：
 * Advanced watermark 0
 * Advanced watermark 2
 * Advanced watermark 4
 * # watermark 4 = a 窗口的结束时间4，所以触发a计算输出
 * Timer{timestamp=3, key=(a), namespace=TimeWindow{start=1, end=4}}
 * (a,1,1)
 * Advanced watermark 5
 * Advanced watermark 9
 * # (b,1,1)、(b,3,1)、b(b,5,1)，这3条数据会进行窗口合并，所以这里的结束时间是8
 * # watermark 9 > b 窗口的结束时间8，所以触发b计算输出
 * Timer{timestamp=7, key=(b), namespace=TimeWindow{start=1, end=8}}
 * (b,1,3)
 * # watermark 9 = c 窗口的结束时间9，所以触发c计算输出
 * Timer{timestamp=8, key=(c), namespace=TimeWindow{start=6, end=9}}
 * (c,6,1)
 * Advanced watermark 10
 * Advanced watermark 9223372036854775807
 * # watermark 9223372036854775807 > a 窗口的结束时间13，所以触发a计算输出
 * Timer{timestamp=12, key=(a), namespace=TimeWindow{start=10, end=13}}
 * (a,10,1)
 * # watermark 9223372036854775807 > c 窗口的结束时间14，所以触发c计算输出
 * Timer{timestamp=13, key=(c), namespace=TimeWindow{start=11, end=14}}
 * (c,11,1)
 */
public class SessionWindowing {

	@SuppressWarnings("serial")
	public static void main(String[] args) throws Exception {

		final ParameterTool params = ParameterTool.fromArgs(args);
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		env.getConfig().setGlobalJobParameters(params);
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
		env.setParallelism(1);

		final boolean fileOutput = params.has("output");

		final List<Tuple3<String, Long, Integer>> input = new ArrayList<>();

		// key、event time时间戳、key出现的次数
		input.add(new Tuple3<>("a", 1L, 1));

		input.add(new Tuple3<>("b", 1L, 1));
		input.add(new Tuple3<>("b", 3L, 1));
		input.add(new Tuple3<>("b", 5L, 1));

		input.add(new Tuple3<>("c", 6L, 1));

		// 即将被窗口丢弃的数据
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
							// 发射watermark
							ctx.emitWatermark(new Watermark(value.f1 - 1));
						}
						// input输入流中的数据读取完毕之后，发射一个大的watermark，确保触发最后的窗口计算
						// 无限流，表示终止的watermark，需要一个超过window的end time的watermark来触发window计算
						ctx.emitWatermark(new Watermark(Long.MAX_VALUE));
					}

					@Override
					public void cancel() {
					}
				});

		// We create sessions for each id with max timeout of 3 time units
		DataStream<Tuple3<String, Long, Integer>> aggregated = source
				.keyBy(0)
				.window(EventTimeSessionWindows.withGap(Time.milliseconds(3L)))
				.sum(2);

		if (fileOutput) {
			aggregated.writeAsText(params.get("output"));
		} else {
			System.out.println("Printing result to stdout. Use --output to specify output path.");
			aggregated.print();
		}

		env.execute();
	}
}
