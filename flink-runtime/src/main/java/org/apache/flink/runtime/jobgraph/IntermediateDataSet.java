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

package org.apache.flink.runtime.jobgraph;

import org.apache.flink.runtime.io.network.partition.ResultPartitionType;

import java.util.ArrayList;
import java.util.List;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * An intermediate data set is the data set produced by an operator - either a
 * source or any intermediate operation.
 * 
 * Intermediate data sets may be read by other operators, materialized, or
 * discarded.
 *
 * JobVertex 产生的数据被抽象为 IntermediateDataSet ，字面意思为中间数据集。
 * JobEdge 是和节点的输出结果相关联的，其实就是指可以把 JobEdge 看作是 IntermediateDataSet 的消费者，那么 JobVertex 自然就是生产者了。
 * 它是由一个 Operator(可能是 Source，也可能是某个中间算子) 产生的一个中间数据集。
 */
public class IntermediateDataSet implements java.io.Serializable {
	
	private static final long serialVersionUID = 1L;

	/**
	 * the identifier
	 * IntermediateDataSet ID
	 */
	private final IntermediateDataSetID id;

	/**
	 * the operation that produced this data set
	 * JobVertex 是 IntermediateDataSet 的生产者
	 */
	private final JobVertex producer;

	/**
	 * JobEdge 是和节点的输出结果相关联的，其实就是指可以把 JobEdge 看作是 IntermediateDataSet 的消费者
	 */
	private final List<JobEdge> consumers = new ArrayList<JobEdge>();

	/**
	 * The type of partition to use at runtime
	 * 运行时要使用的分区类型，表示中间结果类型
 	 */
	private final ResultPartitionType resultType;
	
	// --------------------------------------------------------------------------------------------

	public IntermediateDataSet(IntermediateDataSetID id, ResultPartitionType resultType, JobVertex producer) {
		this.id = checkNotNull(id);
		this.producer = checkNotNull(producer);
		this.resultType = checkNotNull(resultType);
	}

	// --------------------------------------------------------------------------------------------
	
	public IntermediateDataSetID getId() {
		return id;
	}

	public JobVertex getProducer() {
		return producer;
	}
	
	public List<JobEdge> getConsumers() {
		return this.consumers;
	}

	public ResultPartitionType getResultType() {
		return resultType;
	}
	
	// --------------------------------------------------------------------------------------------
	
	public void addConsumer(JobEdge edge) {
		this.consumers.add(edge);
	}
	
	// --------------------------------------------------------------------------------------------
	
	@Override
	public String toString() {
		return "Intermediate Data Set (" + id + ")";
	}
}
