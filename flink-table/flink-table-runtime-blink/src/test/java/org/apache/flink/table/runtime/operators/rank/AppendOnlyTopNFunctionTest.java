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

package org.apache.flink.table.runtime.operators.rank;

import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.table.data.RowData;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;

import static org.apache.flink.table.runtime.util.StreamRecordUtils.deleteRecord;
import static org.apache.flink.table.runtime.util.StreamRecordUtils.insertRecord;

/**
 * Tests for {@link AppendOnlyTopNFunction}.
 */
public class AppendOnlyTopNFunctionTest extends TopNFunctionTestBase {

	@Override
	protected AbstractTopNFunction createFunction(RankType rankType, RankRange rankRange,
												  boolean generateUpdateBefore, boolean outputRankNumber) {
		return new AppendOnlyTopNFunction(
			minTime.toMilliseconds(),
			maxTime.toMilliseconds(),
			// 输入的每行数据的 RowType
			inputRowType,
			// key 比较器类
			sortKeyComparator,
			// key 选择器类
			sortKeySelector,
			// 排位枚举，目前只支持 RankType.ROW_NUMBER
			rankType,
			// TopN集合的范围大小
			rankRange,
			generateUpdateBefore,
			outputRankNumber,
			// TopN 总的缓存大小
			cacheSize);
	}

	@Test
	public void testVariableRankRange() throws Exception {
		AbstractTopNFunction func = createFunction(RankType.ROW_NUMBER,
			// 指定数据的第2个字段值为 rankEnd
			new VariableRankRange(1),
			true,
			// 不用输出 topN 的排序序号
			false);
		// 将 TopNFunction 包装进 KeyedProcessOperator
		OneInputStreamOperatorTestHarness<RowData, RowData> testHarness = createTestHarness(func);
		// 测试类准备工作
		testHarness.open();

		// KeyedProcessOperator 作为 input operator 模拟处理数据
		// 开始处理(book,2,12)，key 为 book，rankEnd 为 2，加入 TopN 集合
		testHarness.processElement(insertRecord("book", 2L, 12));
		// 开始处理(book,2,19)，key 为 book，rankEnd 为 2，加入 TopN 集合
		testHarness.processElement(insertRecord("book", 2L, 19));
		// 开始处理(book,2,11)，key 为 book，rankEnd 为 2，超出 TopN 集合容量，因此需要先删除 (book,2,19)，留下 2 个较小的
		testHarness.processElement(insertRecord("book", 2L, 11));
		// 开始处理(fruit,1,33)，key 为 fruit，rankEnd 为 1，加入 TopN 集合
		testHarness.processElement(insertRecord("fruit", 1L, 33));
		// 开始处理(fruit,1,44)，key 为 fruit，rankEnd 为 1，44 > 33，直接过滤掉
		testHarness.processElement(insertRecord("fruit", 1L, 44));
		// 开始处理(fruit,1,22)，key 为 fruit，rankEnd 为 1，超出 TopN 集合容量，因此需要先删除 (fruit,1,33)，留下 1 个较小的
		testHarness.processElement(insertRecord("fruit", 1L, 22));
		testHarness.close();

		ConcurrentLinkedQueue<Object> output = testHarness.getOutput();
		for (Object o : output) {
			StreamRecord streamRecord = (StreamRecord) o;
			System.out.println("Output element -> " + streamRecord.getValue());
		}

		List<Object> expectedOutput = new ArrayList<>();
		// ("book", 2L, 12)
		expectedOutput.add(insertRecord("book", 2L, 12));
		// ("book", 2L, 19)
		expectedOutput.add(insertRecord("book", 2L, 19));
		// ("book", 2L, 11)
		expectedOutput.add(deleteRecord("book", 2L, 19));
		expectedOutput.add(insertRecord("book", 2L, 11));
		// ("fruit", 1L, 33)
		expectedOutput.add(insertRecord("fruit", 1L, 33));
		// ("fruit", 1L, 44)
		// ("fruit", 1L, 22)
		expectedOutput.add(deleteRecord("fruit", 1L, 33));
		expectedOutput.add(insertRecord("fruit", 1L, 22));
		assertorWithoutRowNumber
			.assertOutputEquals("output wrong.", expectedOutput, testHarness.getOutput());
	}

}
