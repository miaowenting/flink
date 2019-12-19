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

package org.apache.flink.streaming.api.functions.sink;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.api.common.typeutils.base.VoidSerializer;
import org.apache.flink.api.java.typeutils.runtime.kryo.KryoSerializer;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.streaming.api.operators.StreamSink;
import org.apache.flink.streaming.util.ContentDump;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;

import org.apache.log4j.AppenderSkeleton;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.spi.LoggingEvent;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.hasItem;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

/**
 * Tests for {@link TwoPhaseCommitSinkFunction}.
 */
public class TwoPhaseCommitSinkFunctionTest {

	/**
	 * 自定义实现的两阶段提交的Function
	 */
	private ContentDumpSinkFunction sinkFunction;

	/**
	 * 输入流data source的封装类
	 */
	private OneInputStreamOperatorTestHarness<String, Object> harness;

	private AtomicBoolean throwException = new AtomicBoolean();

	private ContentDump targetDirectory;

	private ContentDump tmpDirectory;

	/**
	 * 自定义时钟，不用系统时间，便于调试控制时间的流逝
	 */
	private SettableClock clock;

	private Logger logger;

	private AppenderSkeleton testAppender;

	private List<LoggingEvent> loggingEvents;

	@Before
	public void setUp() throws Exception {
		loggingEvents = new ArrayList<>();
		setupLogger();

		// 创建提交阶段的最终目的存储目录
		targetDirectory = new ContentDump();
		// 创建预提交阶段的临时文件目录
		tmpDirectory = new ContentDump();
		clock = new SettableClock();

		setUpTestHarness();
	}

	@After
	public void tearDown() throws Exception {
		closeTestHarness();
		if (logger != null) {
			logger.removeAppender(testAppender);
		}
		loggingEvents = null;
	}

	/**
	 * Setup {@link org.apache.log4j.Logger}, the default logger implementation for tests,
	 * to append {@link LoggingEvent}s to {@link #loggingEvents} so that we can assert if
	 * the right messages were logged.
	 *
	 * @see #testLogTimeoutAlmostReachedWarningDuringCommit
	 * @see #testLogTimeoutAlmostReachedWarningDuringRecovery
	 */
	private void setupLogger() {
		Logger.getRootLogger().removeAllAppenders();
		// 得到TwoPhaseCommitSinkFunction类的日志打印对象logger
		logger = Logger.getLogger(TwoPhaseCommitSinkFunction.class);
		testAppender = new AppenderSkeleton() {
			@Override
			protected void append(LoggingEvent event) {
				// 执行打印的语句会收集到loggingEvents这个List中
				loggingEvents.add(event);
			}

			@Override
			public void close() {

			}

			@Override
			public boolean requiresLayout() {
				return false;
			}
		};
		logger.addAppender(testAppender);
		logger.setLevel(Level.WARN);
	}

	private void setUpTestHarness() throws Exception {
		sinkFunction = new ContentDumpSinkFunction();
		// StreamSink包裹着SinkFunction去执行
		harness = new OneInputStreamOperatorTestHarness<>(new StreamSink<>(sinkFunction), StringSerializer.INSTANCE);
		harness.setup();
	}

	private void closeTestHarness() throws Exception {
		harness.close();
	}

	/**
	 * 发送element  "42" - 0
	 * 发送snapshot  0   - 1
	 * 发送element  "43" - 2
	 * 发送snapshot  1   - 3
	 * 发送element  "44" - 4
	 * 发送snapshot  2   - 5
	 * <p>
	 * 测试场景：
	 * 测试完整的两阶段提交流程，通知checkpointId-1完成，那写入到target目录的数据只有"42","43"
	 * 此时，在tmp目录下状态数据应该有一个checkpointId-2和数据"44"
	 */
	@Test
	public void testNotifyOfCompletedCheckpoint() throws Exception {
		// 准备工作：initializeEmptyState、initializeState()方法中会进行开启事务操作、打开userFunction、初始化SimpleContext
		harness.open();
		harness.processElement("42", 0);
		harness.snapshot(0, 1);
		tmpDirectory.listFiles();
		harness.processElement("43", 2);
		harness.snapshot(1, 3);
		tmpDirectory.listFiles();
		harness.processElement("44", 4);
		harness.snapshot(2, 5);
		tmpDirectory.listFiles();
		harness.notifyOfCompletedCheckpoint(1);

		for (String name : targetDirectory.listFiles()) {
			System.out.println("File " + name + " in target directory. content " + targetDirectory.read(name));
		}

		for (String name : tmpDirectory.listFiles()) {
			System.out.println("File " + name + " in tmp directory. content " + tmpDirectory.read(name));
		}

		assertExactlyOnce(Arrays.asList("42", "43"));
		// one for checkpointId 2 and second for the currentTransaction
		assertEquals(2, tmpDirectory.listFiles().size());
	}

	/**
	 * 测试场景：
	 * 没有执行notifyOfCompletedCheckpoint之前程序挂了，恢复时从checkpointId-1恢复
	 * 应该保证target目录中的数据只有"42","43"
	 * 临时文件在close时被清除，无状态数据
	 */
	@Test
	public void testFailBeforeNotify() throws Exception {
		harness.open();
		harness.processElement("42", 0);
		harness.snapshot(0, 1);
		harness.processElement("43", 2);
		OperatorSubtaskState snapshot = harness.snapshot(1, 3);

		// 模拟在执行notifyOfCompletedCheckpoint之前出故障的场景
		// 设置临时目录不可写入，为了制造不可写入的异常，以方便捕获它
		tmpDirectory.setWritable(false);
		try {
			// 执行到这一步因不可写入直接走到catch
			harness.processElement("44", 4);
			harness.snapshot(2, 5);
			fail("something should fail");
		} catch (Exception ex) {
			if (!(ex.getCause() instanceof ContentDump.NotWritableException)) {
				throw ex;
			}
			// ignore
		}
		// 模拟发生异常之后退出流处理流程
		closeTestHarness();

		// 设置临时目录可写
		tmpDirectory.setWritable(true);

		// 从快照checkpointId-1恢复state，恢复的同时会进行提交操作，会把checkpointId-1之前的临时文件移动到目的目录中
		setUpTestHarness();
		harness.initializeState(snapshot);

		// close操作最终会调用TwoPhaseCommitSinkFunction中的close方法，会调用事务的abort()方法，abort()方法中会清除临时文件
		closeTestHarness();

		for (String name : targetDirectory.listFiles()) {
			System.out.println("File " + name + " in target directory. content " + targetDirectory.read(name));
		}

		if (tmpDirectory.listFiles().isEmpty()) {
			System.out.println("There is no file in tmp directory.");
		}

		for (String name : tmpDirectory.listFiles()) {
			System.out.println("File " + name + " in tmp directory. content " + tmpDirectory.read(name));
		}

		assertExactlyOnce(Arrays.asList("42", "43"));
		assertEquals(0, tmpDirectory.listFiles().size());
	}

	/**
	 * 测试场景：
	 * 当执行commit操作时，如果设置了忽略提交超时异常，并且也确定是超时了，则不会向外抛出异常了
	 */
	@Test
	public void testIgnoreCommitExceptionDuringRecovery() throws Exception {
		clock.setEpochMilli(0);

		harness.open();
		harness.processElement("42", 0);

		final OperatorSubtaskState snapshot = harness.snapshot(0, 1);
		// 此时，42数据已经被写入目标目录，checkpointId-1 大于当前的0，可以执行到提交操作
		harness.notifyOfCompletedCheckpoint(1);

		for (String name : targetDirectory.listFiles()) {
			System.out.println("File " + name + " in target directory. content " + targetDirectory.read(name));
		}

		// 人为设置提交时抛出异常
		throwException.set(true);

		closeTestHarness();
		setUpTestHarness();

		final long transactionTimeout = 1000;
		// 设置事务超时时长
		sinkFunction.setTransactionTimeout(transactionTimeout);
		// 忽略事务超时异常
		sinkFunction.ignoreFailuresAfterTransactionTimeout();

		try {
			// 从snapshot恢复state，从checkpointId-0处恢复，会执行commit操作，此时commit操作中就会抛出异常
			harness.initializeState(snapshot);
			fail("Expected exception not thrown");
		} catch (RuntimeException e) {
			assertEquals("Expected exception", e.getMessage());
		}

		clock.setEpochMilli(transactionTimeout + 1);
		// 从snapshot恢复state，从checkpointId-0处恢复，会执行commit操作，此时commit操作中就会抛出异常
		// 如果设置了忽略事务提交超时异常，并且确实发生了超时异常，则忽略此异常，外层就不用捕获了
		harness.initializeState(snapshot);

		assertExactlyOnce(Collections.singletonList("42"));
	}

	/**
	 * 测试场景：
	 * 在commit过程中，让时间流逝到超时时间的一半多点，看看会不会产生警告信息
	 */
	@Test
	public void testLogTimeoutAlmostReachedWarningDuringCommit() throws Exception {
		clock.setEpochMilli(0);

		final long transactionTimeout = 1000;
		final double warningRatio = 0.5;
		sinkFunction.setTransactionTimeout(transactionTimeout);
		sinkFunction.enableTransactionTimeoutWarnings(warningRatio);

		harness.open();
		harness.snapshot(0, 1);
		// 设置流逝时间大小为502ms
		final long elapsedTime = (long) ((double) transactionTimeout * warningRatio + 2);
		clock.setEpochMilli(elapsedTime);
		// 提交过程中耗时有点久
		harness.notifyOfCompletedCheckpoint(1);

		final List<String> logMessages =
			loggingEvents.stream().map(LoggingEvent::getRenderedMessage).collect(Collectors.toList());

		assertThat(
			logMessages,
			hasItem(containsString("has been open for 502 ms. " +
				"This is close to or even exceeding the transaction timeout of 1000 ms.")));
	}

	/**
	 * 测试场景：
	 * 在恢复重新初始化状态的过程中，让时间流逝到超时时间的一半多点，看看会不会产生警告信息
	 */
	@Test
	public void testLogTimeoutAlmostReachedWarningDuringRecovery() throws Exception {
		clock.setEpochMilli(0);

		final long transactionTimeout = 1000;
		final double warningRatio = 0.5;
		sinkFunction.setTransactionTimeout(transactionTimeout);
		sinkFunction.enableTransactionTimeoutWarnings(warningRatio);

		harness.open();

		final OperatorSubtaskState snapshot = harness.snapshot(0, 1);
		// 设置流逝时间大小为502ms
		final long elapsedTime = (long) ((double) transactionTimeout * warningRatio + 2);
		clock.setEpochMilli(elapsedTime);

		// recovery过程
		closeTestHarness();
		setUpTestHarness();
		sinkFunction.setTransactionTimeout(transactionTimeout);
		sinkFunction.enableTransactionTimeoutWarnings(warningRatio);

		harness.initializeState(snapshot);
		harness.open();

		final List<String> logMessages =
			loggingEvents.stream().map(LoggingEvent::getRenderedMessage).collect(Collectors.toList());

		closeTestHarness();

		assertThat(
			logMessages,
			hasItem(containsString("has been open for 502 ms. " +
				"This is close to or even exceeding the transaction timeout of 1000 ms.")));
	}

	private void assertExactlyOnce(List<String> expectedValues) throws IOException {
		ArrayList<String> actualValues = new ArrayList<>();
		for (String name : targetDirectory.listFiles()) {
			actualValues.addAll(targetDirectory.read(name));
		}
		Collections.sort(actualValues);
		Collections.sort(expectedValues);
		assertEquals(expectedValues, actualValues);
	}

	/**
	 * 实现两阶段提交的function
	 */
	private class ContentDumpSinkFunction extends TwoPhaseCommitSinkFunction<String, ContentTransaction, Void> {

		public ContentDumpSinkFunction() {
			super(
				// 设置时钟对象
				new KryoSerializer<>(ContentTransaction.class, new ExecutionConfig()),
				VoidSerializer.INSTANCE, clock);
		}

		/**
		 * 接收到事件，invoke处理事件
		 */
		@Override
		protected void invoke(ContentTransaction transaction, String value, Context context) throws Exception {
			transaction.tmpContentWriter.write(value);
		}

		/**
		 * 开启事务
		 */
		@Override
		protected ContentTransaction beginTransaction() throws Exception {
			// 开启事务，做一些准备工作：在临时目录中创建临时文件，文件名为随机生成的UUID；创建写文件的writer
			return new ContentTransaction(tmpDirectory.createWriter(UUID.randomUUID().toString()));
		}

		/**
		 * 预提交阶段
		 */
		@Override
		protected void preCommit(ContentTransaction transaction) throws Exception {
			// 刷写到临时文件并关闭文件，因为再也不会向这个临时文件中写入数据
			transaction.tmpContentWriter.flush();
			transaction.tmpContentWriter.close();
		}

		/**
		 * 提交阶段，提交操作是幂等的，要么在原来的临时目录，要么被移动到目标目录了
		 */
		@Override
		protected void commit(ContentTransaction transaction) {
			// 模拟提交阶段的异常
			if (throwException.get()) {
				throw new RuntimeException("Expected exception");
			}

			// 转移tmp目录下的文件到target目录下
			ContentDump.move(
				transaction.tmpContentWriter.getName(),
				tmpDirectory,
				targetDirectory);

		}

		/**
		 * abort，进行回滚操作，把预提交阶段刷写的临时文件删除
		 */
		@Override
		protected void abort(ContentTransaction transaction) {
			transaction.tmpContentWriter.close();
			tmpDirectory.delete(transaction.tmpContentWriter.getName());
		}
	}

	/**
	 * 事务操作类
	 */
	private static class ContentTransaction {
		private ContentDump.ContentWriter tmpContentWriter;

		public ContentTransaction(ContentDump.ContentWriter tmpContentWriter) {
			this.tmpContentWriter = tmpContentWriter;
		}

		@Override
		public String toString() {
			return String.format("ContentTransaction[%s]", tmpContentWriter.getName());
		}
	}

	private static class SettableClock extends Clock {

		private final ZoneId zoneId;

		private long epochMilli;

		private SettableClock() {
			this.zoneId = ZoneOffset.UTC;
		}

		public SettableClock(ZoneId zoneId, long epochMilli) {
			this.zoneId = zoneId;
			this.epochMilli = epochMilli;
		}

		public void setEpochMilli(long epochMilli) {
			this.epochMilli = epochMilli;
		}

		@Override
		public ZoneId getZone() {
			return zoneId;
		}

		@Override
		public Clock withZone(ZoneId zone) {
			if (zone.equals(this.zoneId)) {
				return this;
			}
			return new SettableClock(zone, epochMilli);
		}

		@Override
		public Instant instant() {
			return Instant.ofEpochMilli(epochMilli);
		}
	}
}
