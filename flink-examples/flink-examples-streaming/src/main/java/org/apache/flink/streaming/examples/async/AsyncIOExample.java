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

package org.apache.flink.streaming.examples.async;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.checkpoint.ListCheckpointed;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.async.AsyncFunction;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.ExecutorUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

/**
 * Example to illustrates how to use {@link AsyncFunction}.
 */
public class AsyncIOExample {

	private static final Logger LOG = LoggerFactory.getLogger(AsyncIOExample.class);

	private static final String EXACTLY_ONCE_MODE = "exactly_once";
	private static final String EVENT_TIME = "EventTime";
	private static final String INGESTION_TIME = "IngestionTime";
	private static final String ORDERED = "ordered";

	/**
	 * A checkpointed source.
	 * 具体功能：一个数据流 -> 不断发送一个从0递增的整数
	 */
	private static class SimpleSource implements SourceFunction<Integer>, ListCheckpointed<Integer> {
		private static final long serialVersionUID = 1L;

		private volatile boolean isRunning = true;
		/**
		 * 计数器
		 */
		private int counter = 0;
		/**
		 * 起始值
		 */
		private int start = 0;

		/**
		 * 储存快照状态：每次作业执行成功之后，会保存成功的上一条数据的状态，也就是start的值
		 */
		@Override
		public List<Integer> snapshotState(long checkpointId, long timestamp) throws Exception {
			return Collections.singletonList(start);
		}

		/**
		 * 当执行到某个作业流发生异常时，Flink会调用次方法，将状态还原到上一次的成功checkpoint的那个状态点
		 */
		@Override
		public void restoreState(List<Integer> state) throws Exception {
			// 找到最新的一次checkpoint成功时start的值
			for (Integer i : state) {
				this.start = i;
			}
		}

		public SimpleSource(int maxNum) {
			this.counter = maxNum;
		}

		@Override
		public void run(SourceContext<Integer> ctx) throws Exception {
			while ((start < counter || counter == -1) && isRunning) {
				synchronized (ctx.getCheckpointLock()) {
					ctx.collect(start);
					++start;

					// loop back to 0
					if (start == Integer.MAX_VALUE) {
						start = 0;
					}
				}
				Thread.sleep(10L);
			}
		}

		@Override
		public void cancel() {
			isRunning = false;
		}
	}


	/**
	 * An sample of {@link AsyncFunction} using a thread pool and executing working threads
	 * to simulate multiple async operations.
	 *
	 * <p>For the real use case in production environment, the thread pool may stay in the
	 * async client.
	 *
	 * 一个异步函数示例：用线程池模拟多个异步操作
	 * 具体功能：处理流任务的异步函数
	 */
	private static class SampleAsyncFunction extends RichAsyncFunction<Integer, String> {
		private static final long serialVersionUID = 2098635244857937717L;

		private transient ExecutorService executorService;

		/**
		 * The result of multiplying sleepFactor with a random float is used to pause
		 * the working thread in the thread pool, simulating a time consuming async operation.
		 * 模拟耗时的异步操作用的：就是假装这个异步操作很耗时，耗时时长为sleepFactor
		 */
		private final long sleepFactor;

		/**
		 * The ratio to generate an exception to simulate an async error. For example, the error
		 * may be a TimeoutException while visiting HBase.
		 * 模拟异步操作出现了异常：就是假装我的流任务的异步操作出现异常啦~ 会报错：Exception : wahahaha...
		 */
		private final float failRatio;

		private final long shutdownWaitTS;

		SampleAsyncFunction(long sleepFactor, float failRatio, long shutdownWaitTS) {
			this.sleepFactor = sleepFactor;
			this.failRatio = failRatio;
			this.shutdownWaitTS = shutdownWaitTS;
		}

		@Override
		public void open(Configuration parameters) throws Exception {
			super.open(parameters);

			executorService = Executors.newFixedThreadPool(30);
		}

		@Override
		public void close() throws Exception {
			super.close();
			ExecutorUtils.gracefulShutdown(shutdownWaitTS, TimeUnit.MILLISECONDS, executorService);
		}

		/**
		 * 真正执行异步IO的方法
		 * 这里用线程池模拟 source支持异步发送数据流
		 */
		@Override
		public void asyncInvoke(final Integer input, final ResultFuture<String> resultFuture) {
			executorService.submit(() -> {
				// wait for while to simulate async operation here
				// 模拟元素的操作时长：就是这个元素与外部系统交互的时长，然后sleep这么长的时间
				long sleep = (long) (ThreadLocalRandom.current().nextFloat() * sleepFactor);
				try {
					Thread.sleep(sleep);

					if (ThreadLocalRandom.current().nextFloat() < failRatio) {
						// 模拟触发异常：就是与外部系统交互时，假装出错发出了一个异常，此处可以查看日志，观察flink如何checkpoint恢复
						// restart-strategy.fixed-delay.attempts 默认为3，重启3次
						resultFuture.completeExceptionally(new Exception("wahahahaha..."));
					} else {
						// 根据输入的input/10的余数生成key
						resultFuture.complete(
							Collections.singletonList("key-" + (input % 10)));
					}
				} catch (InterruptedException e) {
					resultFuture.complete(new ArrayList<>(0));
				}
			});
		}
	}

	private static void printUsage() {
		System.out.println("To customize example, use: AsyncIOExample [--fsStatePath <path to fs state>] " +
				"[--checkpointMode <exactly_once or at_least_once>] " +
				"[--maxCount <max number of input from source, -1 for infinite input>] " +
				"[--sleepFactor <interval to sleep for each stream element>] [--failRatio <possibility to throw exception>] " +
				"[--waitMode <ordered or unordered>] [--waitOperatorParallelism <parallelism for async wait operator>] " +
				"[--eventType <EventTime or IngestionTime>] [--shutdownWaitTS <milli sec to wait for thread pool>]" +
				"[--timeout <Timeout for the asynchronous operations>]");
	}

	public static void main(String[] args) throws Exception {

		// obtain execution environment
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		// parse parameters
		final ParameterTool params = ParameterTool.fromArgs(args);

		// 状态存放路径
		final String statePath;
		// checkpoint模式
		final String cpMode;
		// source生成的最大值
		final int maxCount;
		// RichAsyncFunction 中 线程休眠的因子
		final long sleepFactor;
		// 模拟RichAsyncFunction出错的概率因子
		final float failRatio;
		// 标志RichAsyncFunction 中的消息是有序还是无序的
		final String mode;
		// 设置任务的并行度
		final int taskNum;
		// 使用的Flink时间类型
		final String timeType;
		// 优雅停止RichAsyncFunction中线程池的等待毫秒数
		final long shutdownWaitTS;
		// RichAsyncFunction中执行异步操作的超时时间
		final long timeout;

		try {
			// check the configuration for the job
			statePath = params.get("fsStatePath", null);
			cpMode = params.get("checkpointMode", "exactly_once");
			maxCount = params.getInt("maxCount", 100000);
			sleepFactor = params.getLong("sleepFactor", 100);
			failRatio = params.getFloat("failRatio", 0.001f);
//			failRatio = params.getFloat("failRatio", 0.5f);
			mode = params.get("waitMode", "ordered");
			taskNum = params.getInt("waitOperatorParallelism", 1);
			timeType = params.get("eventType", "EventTime");
			shutdownWaitTS = params.getLong("shutdownWaitTS", 20000);
			timeout = params.getLong("timeout", 10000L);
		} catch (Exception e) {
			printUsage();

			throw e;
		}

		StringBuilder configStringBuilder = new StringBuilder();

		final String lineSeparator = System.getProperty("line.separator");

		configStringBuilder
			.append("Job configuration").append(lineSeparator)
			.append("FS state path=").append(statePath).append(lineSeparator)
			.append("Checkpoint mode=").append(cpMode).append(lineSeparator)
			.append("Max count of input from source=").append(maxCount).append(lineSeparator)
			.append("Sleep factor=").append(sleepFactor).append(lineSeparator)
			.append("Fail ratio=").append(failRatio).append(lineSeparator)
			.append("Waiting mode=").append(mode).append(lineSeparator)
			.append("Parallelism for async wait operator=").append(taskNum).append(lineSeparator)
			.append("Event type=").append(timeType).append(lineSeparator)
			.append("Shutdown wait timestamp=").append(shutdownWaitTS);

		LOG.info(configStringBuilder.toString());

		if (statePath != null) {
			// setup state and checkpoint mode
			env.setStateBackend(new FsStateBackend(statePath));
		}

		if (EXACTLY_ONCE_MODE.equals(cpMode)) {
			// 生成checkpoint的默认间隔是1s
			env.enableCheckpointing(1000L, CheckpointingMode.EXACTLY_ONCE);
		} else {
			env.enableCheckpointing(1000L, CheckpointingMode.AT_LEAST_ONCE);
		}

		// enable watermark or not
		if (EVENT_TIME.equals(timeType)) {
			env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
		} else if (INGESTION_TIME.equals(timeType)) {
			env.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime);
		}

		// 创建一个数据源
		// create input stream of an single integer
		DataStream<Integer> inputStream = env.addSource(new SimpleSource(maxCount));

		// 创建async函数，通过等待来模拟异步i/o的过程
		// create async function, which will *wait* for a while to simulate the process of async i/o
		AsyncFunction<Integer, String> function =
			new SampleAsyncFunction(sleepFactor, failRatio, shutdownWaitTS);

		// add async operator to streaming job
		DataStream<String> result;
		if (ORDERED.equals(mode)) {
			result = AsyncDataStream.orderedWait(
				inputStream,
				function,
				timeout,
				TimeUnit.MILLISECONDS,
				20).setParallelism(taskNum);
		} else {
			result = AsyncDataStream.unorderedWait(
				inputStream,
				function,
				timeout,
				TimeUnit.MILLISECONDS,
				20).setParallelism(taskNum);
		}

		// add a reduce to get the sum of each keys.
		// 统计 key-> 源头数据除以10的余数，分别对应的个数
		result.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
			private static final long serialVersionUID = -938116068682344455L;

			@Override
			public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
				out.collect(new Tuple2<>(value, 1));
			}
		}).keyBy(0).sum(1).print();

		// execute the program
		env.execute("Async IO Example");
	}
}



