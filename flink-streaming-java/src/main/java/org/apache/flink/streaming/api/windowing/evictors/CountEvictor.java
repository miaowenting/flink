/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.api.windowing.evictors;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.streaming.api.windowing.windows.Window;
import org.apache.flink.streaming.runtime.operators.windowing.TimestampedValue;

import java.util.Iterator;

/**
 * An {@link Evictor} that keeps up to a certain amount of elements.
 *
 * 保持窗口中用户指定数量的元素，并从窗口缓冲区的开头丢弃剩余的元素
 * @param <W> The type of {@link Window Windows} on which this {@code Evictor} can operate.
 */
@PublicEvolving
public class CountEvictor<W extends Window> implements Evictor<Object, W> {
	private static final long serialVersionUID = 1L;

	private final long maxCount;
	private final boolean doEvictAfter;

	private CountEvictor(long count, boolean doEvictAfter) {
		this.maxCount = count;
		this.doEvictAfter = doEvictAfter;
	}

	private CountEvictor(long count) {
		this.maxCount = count;
		this.doEvictAfter = false;
	}

	/**
	 * 移除窗口元素，在Window Function之前调用
	 *
	 * @param elements The elements currently in the pane. 当前窗口中的元素
	 * @param size     The current number of elements in the pane. 当前窗口中的元素数量
	 * @param window   The {@link Window} 当前窗口
	 * @param ctx      evict的上下文
	 */
	@Override
	public void evictBefore(Iterable<TimestampedValue<Object>> elements, int size, W window, EvictorContext ctx) {
		if (!doEvictAfter) {
			evict(elements, size, ctx);
		}
	}

	/**
	 * 移除窗口元素，在Window Function之后调用
	 */
	@Override
	public void evictAfter(Iterable<TimestampedValue<Object>> elements, int size, W window, EvictorContext ctx) {
		if (doEvictAfter) {
			evict(elements, size, ctx);
		}
	}

	private void evict(Iterable<TimestampedValue<Object>> elements, int size, EvictorContext ctx) {
		if (size <= maxCount) {
			// 没达到窗口大小，直接返回
			return;
		} else {
			int evictedCount = 0;
			for (Iterator<TimestampedValue<Object>> iterator = elements.iterator(); iterator.hasNext(); ) {
				iterator.next();
				evictedCount++;
				if (evictedCount > size - maxCount) {
					break;
				} else {
					// 移除前size - maxCount个元素，只剩下最后maxCount个元素
					iterator.remove();
				}
			}
		}
	}

	/**
	 * Creates a {@code CountEvictor} that keeps the given number of elements.
	 * Eviction is done before the window function.
	 *
	 * @param maxCount The number of elements to keep in the pane.
	 */
	public static <W extends Window> CountEvictor<W> of(long maxCount) {
		return new CountEvictor<>(maxCount);
	}

	/**
	 * Creates a {@code CountEvictor} that keeps the given number of elements in the pane
	 * Eviction is done before/after the window function based on the value of doEvictAfter.
	 *
	 * @param maxCount     The number of elements to keep in the pane.
	 * @param doEvictAfter Whether to do eviction after the window function.
	 */
	public static <W extends Window> CountEvictor<W> of(long maxCount, boolean doEvictAfter) {
		return new CountEvictor<>(maxCount, doEvictAfter);
	}
}
