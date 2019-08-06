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

package org.apache.flink.streaming.api.windowing.assigners;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.streaming.api.windowing.windows.Window;

import java.util.Collection;

/**
 * A {@code WindowAssigner} that can merge windows.
 *
 * 扩展Flink中的窗口机制，使得能够支持窗口合并，首先window assigner要能合并现有的窗口，
 * Flink增加了一个新的抽象类MergingWindowAssigner继承自WindowAssigner
 *
 * @param <T> The type of elements that this WindowAssigner can assign windows to.
 * @param <W> The type of {@code Window} that this assigner assigns.
 */
@PublicEvolving
public abstract class MergingWindowAssigner<T, W extends Window> extends WindowAssigner<T, W> {
	private static final long serialVersionUID = 1L;

	/**
	 * Determines which windows (if any) should be merged.
	 * 决定哪些窗口需要被合并。对于每组需要合并的窗口，都会调用callback.merge
	 *
	 * @param windows The window candidates. 现存的窗口集合
	 * @param callback A callback that can be invoked to signal which windows should be merged. 需要被合并的窗口会回调callback
	 *                    .merge方法
	 */
	public abstract void mergeWindows(Collection<W> windows, MergeCallback<W> callback);

	/**
	 * Callback to be used in {@link #mergeWindows(Collection, MergeCallback)} for specifying which
	 * windows should be merged.
	 */
	public interface MergeCallback<W> {

		/**
		 * Specifies that the given windows should be merged into the result window.
		 * 用来声明合并窗口的具体动作（合并窗口底层状态、合并窗口trigger等）
		 *
		 * @param toBeMerged The list of windows that should be merged into one window. 需要被合并的窗口列表
		 * @param mergeResult The resulting merged window. 合并后的窗口
		 */
		void merge(Collection<W> toBeMerged, W mergeResult);
	}
}
