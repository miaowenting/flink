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

package org.apache.flink.cep.nfa;

/**
 * Set of actions when doing a state transition from a {@link State} to another.
 */
public enum StateTransitionAction {
	/**
	 * take the current event and assign it to the current state
	 * 以一种条件作为判断，当进来的一条事件满足 take 边的条件时，就把相应的事件放入结果集，并且将相应的状态向下转移。
	 */
	TAKE,
	/**
	 * ignore the current event
	 * 允许忽略不匹配的事件
	 */
	IGNORE,
	/**
	 * do the state transition and keep the current event for further processing (epsilon transition)
	 * 状态的空转移，当前的状态可以不依赖于任何事件而转移到下一个状态。透传
	 */
	PROCEED
}
