/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.cep;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.cep.nfa.NFA;
import org.apache.flink.cep.nfa.aftermatch.AfterMatchSkipStrategy;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.RichIterativeCondition;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.DataStreamUtils;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.test.util.AbstractTestBase;
import org.apache.flink.types.Either;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;

/**
 * End to end tests of both CEP operators and {@link NFA}.
 */
@SuppressWarnings("serial")
public class CEPITCase extends AbstractTestBase {

	/**
	 * Checks that a certain event sequence is recognized.
	 * 全局匹配一组先后发生的事件序列
	 */
	@Test
	public void testSimplePatternCEP() throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		DataStream<Event> input = env.fromElements(
			new Event(1, "barfoo", 1.0),
			new Event(2, "start", 2.0),
			new Event(3, "foobar", 3.0),
			new SubEvent(4, "foo", 4.0, 1.0),
			new Event(5, "middle", 5.0),
			new SubEvent(6, "middle", 6.0, 2.0),
			new SubEvent(7, "bar", 3.0, 3.0),
			new Event(42, "42", 42.0),
			new Event(8, "end", 1.0)
		);

		// Pattern 组
		Pattern<Event, ?> pattern = Pattern.<Event>begin("start").where(new SimpleCondition<Event>() {

			@Override
			public boolean filter(Event value) throws Exception {
				// start Pattern
				System.out.println("testSimplePatternCEP start -> " + value + ", " + value.getName().equals("start"));
				return value.getName().equals("start");
			}
		})
			// 1. 前一个模式
			// 2. 创建一个新的模式
			// 3. 模式名
			// 4. where 指定模式内容
			.followedByAny("middle").subtype(SubEvent.class).where(
				new SimpleCondition<SubEvent>() {

					@Override
					public boolean filter(SubEvent value) throws Exception {
						// middle Pattern
						// 5. 核心处理逻辑
						System.out.println("testSimplePatternCEP middle -> " + value + ", " + value.getName().equals(
							"middle"));
						return value.getName().equals("middle");
					}
				}
			)
			.followedByAny("end").where(new SimpleCondition<Event>() {

				@Override
				public boolean filter(Event value) throws Exception {
					// end Pattern
					System.out.println("testSimplePatternCEP end -> " + value + ", " + value.getName().equals("end"));
					return value.getName().equals("end");
				}
			});

		// CEP 匹配
		PatternStream<Event> patternStream = CEP.pattern(input, pattern);

		// 经过 CEP 匹配之后的输出流
		DataStream<String> result = patternStream.flatSelect((p, o) -> {
			StringBuilder builder = new StringBuilder();

			// Map<String,List<Event>>
			builder.append(p.get("start").get(0).getId()).append(",")
				.append(p.get("middle").get(0).getId()).append(",")
				.append(p.get("end").get(0).getId());

			o.collect(builder.toString());
			System.out.println("testSimplePatternCEP -> " + builder.toString());
		}, Types.STRING);

		List<String> resultList = new ArrayList<>();

		DataStreamUtils.collect(result).forEachRemaining(resultList::add);

		// 根据设置的 Pattern，返回 id 列表 2,6,8
		assertEquals(Arrays.asList("2,6,8"), resultList);
	}

	/**
	 * 先按照 id 分组，再去匹配各自的 Pattern
	 */
	@Test
	public void testSimpleKeyedPatternCEP() throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(2);

		DataStream<Event> input = env.fromElements(
			new Event(1, "barfoo", 1.0),
			new Event(2, "start", 2.0),
			new Event(3, "start", 2.1),
			new Event(3, "foobar", 3.0),
			new SubEvent(4, "foo", 4.0, 1.0),
			new SubEvent(3, "middle", 3.2, 1.0),
			new Event(42, "start", 3.1),
			new SubEvent(42, "middle", 3.3, 1.2),
			new Event(5, "middle", 5.0),
			new SubEvent(2, "middle", 6.0, 2.0),
			new SubEvent(7, "bar", 3.0, 3.0),
			new Event(42, "42", 42.0),
			new Event(3, "end", 2.0),
			new Event(2, "end", 1.0),
			new Event(42, "end", 42.0)
		).keyBy(new KeySelector<Event, Integer>() {

			@Override
			public Integer getKey(Event value) throws Exception {
				return value.getId();
			}
		});

		Pattern<Event, ?> pattern = Pattern.<Event>begin("start").where(new SimpleCondition<Event>() {

			@Override
			public boolean filter(Event value) throws Exception {
				System.out.println("testSimpleKeyedPatternCEP start -> " + value + ", " + value.getName().equals(
					"start"));
				return value.getName().equals("start");
			}
		})
			.followedByAny("middle").subtype(SubEvent.class).where(
				new SimpleCondition<SubEvent>() {

					@Override
					public boolean filter(SubEvent value) throws Exception {
						System.out.println("testSimpleKeyedPatternCEP middle -> " + value + ", " + value.getName().equals("middle"));
						return value.getName().equals("middle");
					}
				}
			)
			.followedByAny("end").where(new SimpleCondition<Event>() {

				@Override
				public boolean filter(Event value) throws Exception {
					System.out.println("testSimpleKeyedPatternCEP end -> " + value + ", " + value.getName().equals(
						"end"));
					return value.getName().equals("end");
				}
			});

		// 处理同一个 input 中的不同 event
		DataStream<String> result = CEP.pattern(input, pattern).select(p -> {
			StringBuilder builder = new StringBuilder();

			builder.append(p.get("start").get(0).getId()).append(",")
				.append(p.get("middle").get(0).getId()).append(",")
				.append(p.get("end").get(0).getId());

			System.out.println("testSimpleKeyedPatternCEP -> " + builder.toString());
			return builder.toString();
		});

		List<String> resultList = new ArrayList<>();

		DataStreamUtils.collect(result).forEachRemaining(resultList::add);

		resultList.sort(String::compareTo);

		// 先按照 key 分组形成 KeyedStream，再去匹配各自的 Pattern
		assertEquals(Arrays.asList("2,2,2", "3,3,3", "42,42,42"), resultList);
	}

	/**
	 * 全局匹配一组先后发生的事件序列，基于 EventTime 排序
	 */
	@Test
	public void testSimplePatternEventTime() throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

		// Tuple2 模拟事件以及事件发生的时间
		// (Event, timestamp)
		// 事件匹配顺序，2 -> 3 -> 1 -> 5 -> 4 -> 5
		DataStream<Event> input = env.fromElements(
			Tuple2.of(new Event(1, "start", 1.0), 5L),
			Tuple2.of(new Event(2, "middle", 2.0), 1L),
			Tuple2.of(new Event(3, "end", 3.0), 3L),
			Tuple2.of(new Event(4, "end", 4.0), 10L),
			Tuple2.of(new Event(5, "middle", 5.0), 7L),
			// last element for high final watermark
			Tuple2.of(new Event(5, "middle", 5.0), 100L)
		).assignTimestampsAndWatermarks(new AssignerWithPunctuatedWatermarks<Tuple2<Event, Long>>() {

			@Override
			public long extractTimestamp(Tuple2<Event, Long> element, long previousTimestamp) {
				// 抽取出时间戳
				System.out.print("Extract timestamp -> " + element.f1 + ", previousTimestamp -> " + previousTimestamp + ", ");
				return element.f1;
			}

			@Override
			public Watermark checkAndGetNextWatermark(Tuple2<Event, Long> lastElement, long extractedTimestamp) {
				// 发射出新的 watermark
				System.out.println("Emit watermark -> " + (lastElement.f1 - 5));
				return new Watermark(lastElement.f1 - 5);
			}

		}).map(new MapFunction<Tuple2<Event, Long>, Event>() {

			@Override
			public Event map(Tuple2<Event, Long> value) throws Exception {
				return value.f0;
			}
		});

		Pattern<Event, ?> pattern = Pattern.<Event>begin("start").where(new SimpleCondition<Event>() {

			@Override
			public boolean filter(Event value) throws Exception {
				System.out.println("testSimplePatternEventTime start -> " + value + ", " + value.getName().equals(
					"start"));
				return value.getName().equals("start");
			}
		}).followedByAny("middle").where(new SimpleCondition<Event>() {

			@Override
			public boolean filter(Event value) throws Exception {
				System.out.println("testSimplePatternEventTime middle -> " + value + ", " + value.getName().equals("middle"));
				return value.getName().equals("middle");
			}
		}).followedByAny("end").where(new SimpleCondition<Event>() {

			@Override
			public boolean filter(Event value) throws Exception {
				System.out.println("testSimplePatternEventTime end -> " + value + ", " + value.getName().equals("end"));
				return value.getName().equals("end");
			}
		});

		DataStream<String> result = CEP.pattern(input, pattern).select(
			new PatternSelectFunction<Event, String>() {

				@Override
				public String select(Map<String, List<Event>> pattern) {
					StringBuilder builder = new StringBuilder();

					builder.append(pattern.get("start").get(0).getId()).append(",")
						.append(pattern.get("middle").get(0).getId()).append(",")
						.append(pattern.get("end").get(0).getId());

					System.out.println("testSimplePatternEventTime -> " + builder.toString());
					return builder.toString();
				}
			}
		);

		List<String> resultList = new ArrayList<>();

		DataStreamUtils.collect(result).forEachRemaining(resultList::add);

		resultList.sort(String::compareTo);

		assertEquals(Arrays.asList("1,5,4"), resultList);
	}


	/**
	 * 先按照 id 分组，再去匹配各自的 Pattern，基于 EventTime
	 */
	@Test
	public void testSimpleKeyedPatternEventTime() throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
		env.setParallelism(2);

		// (Event, timestamp)
		DataStream<Event> input = env.fromElements(
			Tuple2.of(new Event(1, "start", 1.0), 5L),
			Tuple2.of(new Event(1, "middle", 2.0), 1L),
			Tuple2.of(new Event(2, "middle", 2.0), 4L),
			Tuple2.of(new Event(2, "start", 2.0), 3L),
			Tuple2.of(new Event(1, "end", 3.0), 3L),
			Tuple2.of(new Event(3, "start", 4.1), 5L),
			Tuple2.of(new Event(1, "end", 4.0), 10L),
			Tuple2.of(new Event(2, "end", 2.0), 8L),
			Tuple2.of(new Event(1, "middle", 5.0), 7L),
			Tuple2.of(new Event(3, "middle", 6.0), 9L),
			Tuple2.of(new Event(3, "end", 7.0), 7L)
		).assignTimestampsAndWatermarks(new AssignerWithPunctuatedWatermarks<Tuple2<Event, Long>>() {

			@Override
			public long extractTimestamp(Tuple2<Event, Long> element, long currentTimestamp) {
				return element.f1;
			}

			@Override
			public Watermark checkAndGetNextWatermark(Tuple2<Event, Long> lastElement, long extractedTimestamp) {
				return new Watermark(lastElement.f1 - 5);
			}

		}).map(new MapFunction<Tuple2<Event, Long>, Event>() {

			@Override
			public Event map(Tuple2<Event, Long> value) throws Exception {
				return value.f0;
			}
		}).keyBy(new KeySelector<Event, Integer>() {

			@Override
			public Integer getKey(Event value) throws Exception {
				return value.getId();
			}
		});

		Pattern<Event, ?> pattern = Pattern.<Event>begin("start").where(new SimpleCondition<Event>() {

			@Override
			public boolean filter(Event value) throws Exception {
				System.out.println("testSimpleKeyedPatternEventTime start -> " + value + ", " + value.getName().equals(
					"start"));
				return value.getName().equals("start");
			}
		}).followedByAny("middle").where(new SimpleCondition<Event>() {

			@Override
			public boolean filter(Event value) throws Exception {
				System.out.println("testSimpleKeyedPatternEventTime middle -> " + value + ", " + value.getName().equals(
					"middle"));
				return value.getName().equals("middle");
			}
		}).followedByAny("end").where(new SimpleCondition<Event>() {

			@Override
			public boolean filter(Event value) throws Exception {
				System.out.println("testSimpleKeyedPatternEventTime end -> " + value + ", " + value.getName().equals("end"));
				return value.getName().equals("end");
			}
		});

		DataStream<String> result = CEP.pattern(input, pattern).select(
			new PatternSelectFunction<Event, String>() {

				@Override
				public String select(Map<String, List<Event>> pattern) {
					StringBuilder builder = new StringBuilder();

					builder.append(pattern.get("start").get(0).getId()).append(",")
						.append(pattern.get("middle").get(0).getId()).append(",")
						.append(pattern.get("end").get(0).getId());

					System.out.println("testSimpleKeyedPatternEventTime -> " + builder.toString());
					return builder.toString();
				}
			}
		);

		List<String> resultList = new ArrayList<>();

		DataStreamUtils.collect(result).forEachRemaining(resultList::add);

		resultList.sort(String::compareTo);

		assertEquals(Arrays.asList("1,1,1", "2,2,2"), resultList);
	}


	/**
	 * 根据事件过滤规则匹配出一个事件作为开始事件，"start" 作为该事件的一个 state
	 */
	@Test
	public void testSimplePatternWithSingleState() throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		DataStream<Tuple2<Integer, Integer>> input = env.fromElements(
			new Tuple2<>(0, 1),
			new Tuple2<>(0, 2));

		Pattern<Tuple2<Integer, Integer>, ?> pattern =
			Pattern.<Tuple2<Integer, Integer>>begin("start")
				.where(new SimpleCondition<Tuple2<Integer, Integer>>() {
					@Override
					public boolean filter(Tuple2<Integer, Integer> rec) throws Exception {
						return rec.f1 == 1;
					}
				});

		PatternStream<Tuple2<Integer, Integer>> pStream = CEP.pattern(input, pattern);

		DataStream<Tuple2<Integer, Integer>> result = pStream.select(new PatternSelectFunction<Tuple2<Integer, Integer>, Tuple2<Integer, Integer>>() {
			@Override
			public Tuple2<Integer, Integer> select(Map<String, List<Tuple2<Integer, Integer>>> pattern) throws Exception {
				return pattern.get("start").get(0);
			}
		});

		List<Tuple2<Integer, Integer>> resultList = new ArrayList<>();

		DataStreamUtils.collect(result).forEachRemaining(resultList::add);

		assertEquals(Arrays.asList(new Tuple2<>(0, 1)), resultList);
	}

	/**
	 * 使用 ProcessingTime
	 * 获取时间窗口中的开始事件和结束事件，1、2、3 3个元素依次累加
	 */
	@Test
	public void testProcessingTimeWithWindow() throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(1);

		DataStream<Integer> input = env.fromElements(1, 2, 3);

		Pattern<Integer, ?> pattern = Pattern.<Integer>begin("start")
			.followedByAny("end")
			// 1天的窗口
			.within(Time.days(1));

		DataStream<Integer> result = CEP.pattern(input, pattern).select(new PatternSelectFunction<Integer, Integer>() {
			@Override
			public Integer select(Map<String, List<Integer>> pattern) throws Exception {
				// 返回开始元素和结束元素的累加和
				int start = pattern.get("start").get(0);
				int end = pattern.get("end").get(0);
				int sum = start + end;
				System.out.println("testProcessingTimeWithWindow start -> " + start + ", end -> " + end + ", sum -> " + sum);
				return sum;
			}
		});

		List<Integer> resultList = new ArrayList<>();

		DataStreamUtils.collect(result).forEachRemaining(resultList::add);

		assertEquals(Arrays.asList(3, 4, 5), resultList);
	}

	/**
	 * 使用 EventTime
	 * 处理规则匹配超时的事件，旁路输出
	 * 由于 timeout 而未完成的部分匹配的序列
	 */
	@Test
	public void testTimeoutHandling() throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(1);
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

		// (Event, timestamp)
		DataStream<Event> input = env.fromElements(
			Tuple2.of(new Event(1, "start", 1.0), 1L),
			Tuple2.of(new Event(1, "middle", 2.0), 5L),
			Tuple2.of(new Event(1, "start", 3.0), 4L),
			Tuple2.of(new Event(1, "end", 4.0), 6L)
		).assignTimestampsAndWatermarks(new AssignerWithPunctuatedWatermarks<Tuple2<Event, Long>>() {

			@Override
			public long extractTimestamp(Tuple2<Event, Long> element, long currentTimestamp) {
				return element.f1;
			}

			@Override
			public Watermark checkAndGetNextWatermark(Tuple2<Event, Long> lastElement, long extractedTimestamp) {
				return new Watermark(lastElement.f1 - 5);
			}

		}).map(new MapFunction<Tuple2<Event, Long>, Event>() {

			@Override
			public Event map(Tuple2<Event, Long> value) throws Exception {
				return value.f0;
			}
		});

		Pattern<Event, ?> pattern = Pattern.<Event>begin("start").where(new SimpleCondition<Event>() {

			@Override
			public boolean filter(Event value) throws Exception {
				return value.getName().equals("start");
			}
		}).followedByAny("middle").where(new SimpleCondition<Event>() {

			@Override
			public boolean filter(Event value) throws Exception {
				return value.getName().equals("middle");
			}
		}).followedByAny("end").where(new SimpleCondition<Event>() {

			@Override
			public boolean filter(Event value) throws Exception {
				return value.getName().equals("end");
			}
		}).within(Time.milliseconds(3));

		DataStream<Either<String, String>> result = CEP.pattern(input, pattern).select(
			new PatternTimeoutFunction<Event, String>() {
				// 超时事件旁路输出
				@Override
				public String timeout(Map<String, List<Event>> pattern, long timeoutTimestamp) throws Exception {
					System.out.println("testTimeoutHandling start timeout -> " + pattern.get("start").get(0) + ", " + timeoutTimestamp);
					if (pattern.get("middle") != null) {
						System.out.println("testTimeoutHandling middle timeout -> " + pattern.get("middle").get(0) +
							", " + timeoutTimestamp);
					}
					return pattern.get("start").get(0).getPrice() + "";
				}
			},
			new PatternSelectFunction<Event, String>() {

				// 主流程正常匹配事件输出
				@Override
				public String select(Map<String, List<Event>> pattern) {
					StringBuilder builder = new StringBuilder();

					builder.append(pattern.get("start").get(0).getPrice()).append(",")
						.append(pattern.get("middle").get(0).getPrice()).append(",")
						.append(pattern.get("end").get(0).getPrice());

					System.out.println("testTimeoutHandling -> " + builder.toString());
					return builder.toString();
				}
			}
		);

		List<Either<String, String>> resultList = new ArrayList<>();

		DataStreamUtils.collect(result).forEachRemaining(resultList::add);

		resultList.sort(Comparator.comparing(either -> either.toString()));

		List<Either<String, String>> expected = Arrays.asList(
			Either.Left.of("1.0"),
			Either.Left.of("3.0"),
			Either.Left.of("3.0"),
			Either.Right.of("3.0,2.0,4.0")
		);

		assertEquals(expected, resultList);
	}

	/**
	 * Checks that a certain event sequence is recognized with an OR filter.
	 * Pattern 中事件的过滤条件是可以用 or 的
	 */
	@Test
	public void testSimpleOrFilterPatternCEP() throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		DataStream<Event> input = env.fromElements(
			new Event(1, "start", 1.0),
			new Event(2, "middle", 2.0),
			new Event(3, "end", 3.0),
			new Event(4, "start", 4.0),
			new Event(5, "middle", 5.0),
			new Event(6, "end", 6.0)
		);

		Pattern<Event, ?> pattern = Pattern.<Event>begin("start")
			.where(new SimpleCondition<Event>() {
				@Override
				public boolean filter(Event value) throws Exception {
					return value.getName().equals("start");
				}
			})
			.followedByAny("middle")
			.where(new SimpleCondition<Event>() {
				@Override
				public boolean filter(Event value) throws Exception {
					return value.getPrice() == 2.0;
				}
			})
			.or(new SimpleCondition<Event>() {
				@Override
				public boolean filter(Event value) throws Exception {
					return value.getPrice() == 5.0;
				}
			})
			.followedByAny("end").where(new SimpleCondition<Event>() {

				@Override
				public boolean filter(Event value) throws Exception {
					return value.getName().equals("end");
				}
			});

		DataStream<String> result = CEP.pattern(input, pattern).select(new PatternSelectFunction<Event, String>() {

			@Override
			public String select(Map<String, List<Event>> pattern) {
				StringBuilder builder = new StringBuilder();

				builder.append(pattern.get("start").get(0).getId()).append(",")
					.append(pattern.get("middle").get(0).getId()).append(",")
					.append(pattern.get("end").get(0).getId());

				System.out.println("testSimpleOrFilterPatternCEP -> " + builder.toString());
				return builder.toString();
			}
		});

		List<String> resultList = new ArrayList<>();

		DataStreamUtils.collect(result).forEachRemaining(resultList::add);

		List<String> expected = Arrays.asList(
			"1,5,6",
			"1,2,3",
			"4,5,6",
			"1,2,6"
		);

		expected.sort(String::compareTo);

		resultList.sort(String::compareTo);

		assertEquals(expected, resultList);
	}

	/**
	 * Checks that a certain event sequence is recognized.
	 * 先把事件按 EventTime 排序，再按自定义的 EventComparator 排序
	 */
	@Test
	public void testSimplePatternEventTimeWithComparator() throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

		// (Event, timestamp)
		// 事件匹配顺序，2 -> 3 -> 1 -> 6 -> 5 -> 4 -> 7
		DataStream<Event> input = env.fromElements(
			Tuple2.of(new Event(1, "start", 1.0), 5L),
			Tuple2.of(new Event(2, "middle", 2.0), 1L),
			Tuple2.of(new Event(3, "end", 3.0), 3L),
			Tuple2.of(new Event(4, "end", 4.0), 10L),
			Tuple2.of(new Event(5, "middle", 6.0), 7L),
			Tuple2.of(new Event(6, "middle", 5.0), 7L),
			// last element for high final watermark
			Tuple2.of(new Event(7, "middle", 5.0), 100L)
		).assignTimestampsAndWatermarks(new AssignerWithPunctuatedWatermarks<Tuple2<Event, Long>>() {

			@Override
			public long extractTimestamp(Tuple2<Event, Long> element, long previousTimestamp) {
				return element.f1;
			}

			@Override
			public Watermark checkAndGetNextWatermark(Tuple2<Event, Long> lastElement, long extractedTimestamp) {
				return new Watermark(lastElement.f1 - 5);
			}

		}).map(new MapFunction<Tuple2<Event, Long>, Event>() {

			@Override
			public Event map(Tuple2<Event, Long> value) throws Exception {
				return value.f0;
			}
		});

		EventComparator<Event> comparator = new CustomEventComparator();

		Pattern<Event, ? extends Event> pattern = Pattern.<Event>begin("start").where(new SimpleCondition<Event>() {

			@Override
			public boolean filter(Event value) throws Exception {
				System.out.println("testSimplePatternEventTimeWithComparator start -> " + value);
				return value.getName().equals("start");
			}
		}).followedByAny("middle").where(new SimpleCondition<Event>() {

			@Override
			public boolean filter(Event value) throws Exception {
				System.out.println("testSimplePatternEventTimeWithComparator middle -> " + value);
				return value.getName().equals("middle");
			}
		}).followedByAny("end").where(new SimpleCondition<Event>() {

			@Override
			public boolean filter(Event value) throws Exception {
				System.out.println("testSimplePatternEventTimeWithComparator end -> " + value);
				return value.getName().equals("end");
			}
		});

		// 这里使用了一个 CustomEventComparator ，事件比较器
		// 先按照事件发生的 EventTime 排序，再按价格排序来依次处理
		DataStream<String> result = CEP.pattern(input, pattern, comparator).select(
			new PatternSelectFunction<Event, String>() {

				@Override
				public String select(Map<String, List<Event>> pattern) {
					StringBuilder builder = new StringBuilder();

					builder.append(pattern.get("start").get(0).getId()).append(",")
						.append(pattern.get("middle").get(0).getId()).append(",")
						.append(pattern.get("end").get(0).getId());

					System.out.println("testSimplePatternEventTimeWithComparator -> " + builder.toString());
					return builder.toString();
				}
			}
		);

		List<String> resultList = new ArrayList<>();

		DataStreamUtils.collect(result).forEachRemaining(resultList::add);

		List<String> expected = Arrays.asList(
			"1,6,4",
			"1,5,4"
		);

		expected.sort(String::compareTo);

		resultList.sort(String::compareTo);

		assertEquals(expected, resultList);
	}

	/**
	 * Event 比较器，按事件中的 Price 值大小从低到高排序
	 */
	private static class CustomEventComparator implements EventComparator<Event> {
		@Override
		public int compare(Event o1, Event o2) {
			return Double.compare(o1.getPrice(), o2.getPrice());
		}
	}

	/**
	 * Pattern 中设置匹配次数，依次匹配两次 "a" ，重新开始一次新的匹配
	 */
	@Test
	public void testSimpleAfterMatchSkip() throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		DataStream<Tuple2<Integer, String>> input = env.fromElements(
			new Tuple2<>(1, "a"),
			new Tuple2<>(2, "a"),
			new Tuple2<>(3, "a"),
			new Tuple2<>(4, "a"));

		Pattern<Tuple2<Integer, String>, ?> pattern =
			Pattern.<Tuple2<Integer, String>>begin("start", AfterMatchSkipStrategy.skipPastLastEvent())
				.where(new SimpleCondition<Tuple2<Integer, String>>() {
					@Override
					public boolean filter(Tuple2<Integer, String> rec) throws Exception {
						return rec.f1.equals("a");
					}
				}).times(2);

		PatternStream<Tuple2<Integer, String>> pStream = CEP.pattern(input, pattern);

		DataStream<Tuple2<Integer, String>> result = pStream.select(new PatternSelectFunction<Tuple2<Integer, String>, Tuple2<Integer, String>>() {
			@Override
			public Tuple2<Integer, String> select(Map<String, List<Tuple2<Integer, String>>> pattern) throws Exception {
				return pattern.get("start").get(0);
			}
		});

		List<Tuple2<Integer, String>> resultList = new ArrayList<>();

		DataStreamUtils.collect(result).forEachRemaining(resultList::add);

		resultList.sort(Comparator.comparing(tuple2 -> tuple2.toString()));

		List<Tuple2<Integer, String>> expected = Arrays.asList(Tuple2.of(1, "a"), Tuple2.of(3, "a"));

		assertEquals(expected, resultList);
	}

	/**
	 * 测试 RichPatternFlatSelectFunction
	 */
	@Test
	public void testRichPatternFlatSelectFunction() throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		DataStream<Event> input = env.fromElements(
			new Event(1, "barfoo", 1.0),
			new Event(2, "start", 2.0),
			new Event(3, "foobar", 3.0),
			new SubEvent(4, "foo", 4.0, 1.0),
			new Event(5, "middle", 5.0),
			new SubEvent(6, "middle", 6.0, 2.0),
			new SubEvent(7, "bar", 3.0, 3.0),
			new Event(42, "42", 42.0),
			new Event(8, "end", 1.0)
		);

		Pattern<Event, ?> pattern = Pattern.<Event>begin("start").where(new RichIterativeCondition<Event>() {

			@Override
			public boolean filter(Event value, Context<Event> ctx) throws Exception {
				return value.getName().equals("start");
			}
		}).followedByAny("middle").subtype(SubEvent.class).where(
			new SimpleCondition<SubEvent>() {

				@Override
				public boolean filter(SubEvent value) throws Exception {
					return value.getName().equals("middle");
				}
			}
		).followedByAny("end").where(new SimpleCondition<Event>() {

			@Override
			public boolean filter(Event value) throws Exception {
				return value.getName().equals("end");
			}
		});

		DataStream<String> result =
			CEP.pattern(input, pattern).flatSelect(new RichPatternFlatSelectFunction<Event, String>() {

				@Override
				public void open(Configuration config) {
					try {
						getRuntimeContext().getMapState(new MapStateDescriptor<>(
							"test",
							LongSerializer.INSTANCE,
							LongSerializer.INSTANCE));
						throw new RuntimeException("Expected getMapState to fail with unsupported operation exception.");
					} catch (UnsupportedOperationException e) {
						// ignore, expected
					}

					getRuntimeContext().getUserCodeClassLoader();
				}

				@Override
				public void flatSelect(Map<String, List<Event>> p, Collector<String> o) throws Exception {
					StringBuilder builder = new StringBuilder();

					builder.append(p.get("start").get(0).getId()).append(",")
						.append(p.get("middle").get(0).getId()).append(",")
						.append(p.get("end").get(0).getId());

					System.out.println("testRichPatternFlatSelectFunction -> " + builder.toString());

					// 将匹配到的事件流发射出去
					o.collect(builder.toString());
				}
			}, Types.STRING);

		List<String> resultList = new ArrayList<>();

		DataStreamUtils.collect(result).forEachRemaining(resultList::add);

		assertEquals(Arrays.asList("2,6,8"), resultList);
	}

	/**
	 * 测试 RichPatternSelectFunction
	 */
	@Test
	public void testRichPatternSelectFunction() throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(2);

		DataStream<Event> input = env.fromElements(
			new Event(1, "barfoo", 1.0),
			new Event(2, "start", 2.0),
			new Event(3, "start", 2.1),
			new Event(3, "foobar", 3.0),
			new SubEvent(4, "foo", 4.0, 1.0),
			new SubEvent(3, "middle", 3.2, 1.0),
			new Event(42, "start", 3.1),
			new SubEvent(42, "middle", 3.3, 1.2),
			new Event(5, "middle", 5.0),
			new SubEvent(2, "middle", 6.0, 2.0),
			new SubEvent(7, "bar", 3.0, 3.0),
			new Event(42, "42", 42.0),
			new Event(3, "end", 2.0),
			new Event(2, "end", 1.0),
			new Event(42, "end", 42.0)
		).keyBy(new KeySelector<Event, Integer>() {

			@Override
			public Integer getKey(Event value) throws Exception {
				return value.getId();
			}
		});

		Pattern<Event, ?> pattern = Pattern.<Event>begin("start").where(new RichIterativeCondition<Event>() {

			@Override
			public boolean filter(Event value, Context<Event> ctx) throws Exception {
				return value.getName().equals("start");
			}
		}).followedByAny("middle").subtype(SubEvent.class).where(
			new SimpleCondition<SubEvent>() {

				@Override
				public boolean filter(SubEvent value) throws Exception {
					return value.getName().equals("middle");
				}
			}
		).followedByAny("end").where(new SimpleCondition<Event>() {

			@Override
			public boolean filter(Event value) throws Exception {
				return value.getName().equals("end");
			}
		});

		DataStream<String> result = CEP.pattern(input, pattern).select(new RichPatternSelectFunction<Event, String>() {
			@Override
			public void open(Configuration config) {
				try {
					getRuntimeContext().getMapState(new MapStateDescriptor<>(
						"test",
						LongSerializer.INSTANCE,
						LongSerializer.INSTANCE));
					throw new RuntimeException("Expected getMapState to fail with unsupported operation exception.");
				} catch (UnsupportedOperationException e) {
					// ignore, expected
				}

				getRuntimeContext().getUserCodeClassLoader();
			}

			@Override
			public String select(Map<String, List<Event>> p) throws Exception {
				StringBuilder builder = new StringBuilder();

				builder.append(p.get("start").get(0).getId()).append(",")
					.append(p.get("middle").get(0).getId()).append(",")
					.append(p.get("end").get(0).getId());

				return builder.toString();
			}
		});

		List<String> resultList = new ArrayList<>();

		DataStreamUtils.collect(result).forEachRemaining(resultList::add);

		resultList.sort(String::compareTo);

		assertEquals(Arrays.asList("2,2,2", "3,3,3", "42,42,42"), resultList);
	}

	@Test
	public void testFlatSelectSerializationWithAnonymousClass() throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		DataStreamSource<Integer> elements = env.fromElements(1, 2, 3);
		OutputTag<Integer> outputTag = new OutputTag<Integer>("AAA") {
		};
		CEP.pattern(elements, Pattern.begin("A")).flatSelect(
			outputTag,
			new PatternFlatTimeoutFunction<Integer, Integer>() {
				@Override
				public void timeout(
					Map<String, List<Integer>> pattern,
					long timeoutTimestamp,
					Collector<Integer> out) throws Exception {

				}
			},
			new PatternFlatSelectFunction<Integer, Object>() {
				@Override
				public void flatSelect(Map<String, List<Integer>> pattern, Collector<Object> out) throws Exception {

				}
			}

		);

		env.execute();
	}
}
