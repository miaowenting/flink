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

package org.apache.flink.runtime.jobmaster;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.checkpoint.CheckpointCoordinatorGateway;
import org.apache.flink.runtime.clusterframework.types.AllocationID;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.executiongraph.ArchivedExecutionGraph;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.executiongraph.ExecutionJobVertex;
import org.apache.flink.runtime.executiongraph.ExecutionVertex;
import org.apache.flink.runtime.io.network.partition.ResultPartition;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.jobgraph.IntermediateDataSetID;
import org.apache.flink.runtime.jobgraph.JobStatus;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.messages.webmonitor.JobDetails;
import org.apache.flink.runtime.registration.RegistrationResponse;
import org.apache.flink.runtime.resourcemanager.ResourceManagerId;
import org.apache.flink.runtime.rest.handler.legacy.backpressure.OperatorBackPressureStatsResponse;
import org.apache.flink.runtime.rpc.FencedRpcGateway;
import org.apache.flink.runtime.rpc.RpcTimeout;
import org.apache.flink.runtime.taskexecutor.AccumulatorReport;
import org.apache.flink.runtime.taskexecutor.slot.SlotOffer;
import org.apache.flink.runtime.taskmanager.TaskExecutionState;
import org.apache.flink.runtime.taskmanager.TaskManagerLocation;

import javax.annotation.Nullable;

import java.util.Collection;
import java.util.concurrent.CompletableFuture;

/**
 * {@link JobMaster} rpc gateway interface.
 * jobManager 的rpc网关接口
 */
public interface JobMasterGateway extends
	CheckpointCoordinatorGateway,
	FencedRpcGateway<JobMasterId>,
	KvStateLocationOracle,
	KvStateRegistryGateway {

	/**
	 * Cancels the currently executed job.
	 * 取消当前执行的job
	 *
	 * @param timeout of this operation
	 * @return Future acknowledge of the operation
	 */
	CompletableFuture<Acknowledge> cancel(@RpcTimeout Time timeout);

	/**
	 * Updates the task execution state for a given task.
	 * 修改指定task的执行状态
	 *
	 * @param taskExecutionState New task execution state for a given task
	 * @return Future flag of the task execution state update result
	 */
	CompletableFuture<Acknowledge> updateTaskExecutionState(
			final TaskExecutionState taskExecutionState);

	/**
	 * Requests the next input split for the {@link ExecutionJobVertex}.
	 * The next input split is sent back to the sender as a
	 * {@link SerializedInputSplit} message.
	 *
	 * 请求{@link ExecutionJobVertex}的下一个输入拆分。下一个输入拆分作为{@link SerializedInputSplit}消息发送回发送方。
	 *
	 *
	 * @param vertexID         The job vertex id
	 * @param executionAttempt The execution attempt id
	 * @return The future of the input split. If there is no further input split, will return an empty object.
	 */
	CompletableFuture<SerializedInputSplit> requestNextInputSplit(
			final JobVertexID vertexID,
			final ExecutionAttemptID executionAttempt);

	/**
	 * Requests the current state of the partition. The state of a
	 * partition is currently bound to the state of the producing execution.
	 *
	 * 请求分区的当前状态。分区的状态当前绑定到产生执行的状态
	 *
	 * @param intermediateResultId The execution attempt ID of the task requesting the partition state.
	 * @param partitionId          The partition ID of the partition to request the state of.
	 * @return The future of the partition state
	 */
	CompletableFuture<ExecutionState> requestPartitionState(
			final IntermediateDataSetID intermediateResultId,
			final ResultPartitionID partitionId);

	/**
	 * Notifies the JobManager about available data for a produced partition.
	 * 通知JobManager生成分区的可用数据
	 *
	 * <p>There is a call to this method for each {@link ExecutionVertex} instance once per produced
	 * {@link ResultPartition} instance, either when first producing data (for pipelined executions)
	 * or when all data has been produced (for staged executions).
	 *
	 * <p>The JobManager then can decide when to schedule the partition consumers of the given session.
	 *
	 * @param partitionID     The partition which has already produced data
	 * @param timeout         before the rpc call fails
	 * @return Future acknowledge of the schedule or update operation
	 */
	CompletableFuture<Acknowledge> scheduleOrUpdateConsumers(
			final ResultPartitionID partitionID,
			@RpcTimeout final Time timeout);

	/**
	 * Disconnects the given {@link org.apache.flink.runtime.taskexecutor.TaskExecutor} from the
	 * {@link JobMaster}.
	 * taskExecutor和jobMaster断开连接
	 *
	 * @param resourceID identifying the TaskManager to disconnect
	 * @param cause for the disconnection of the TaskManager
	 * @return Future acknowledge once the JobMaster has been disconnected from the TaskManager
	 */
	CompletableFuture<Acknowledge> disconnectTaskManager(ResourceID resourceID, Exception cause);

	/**
	 * Disconnects the resource manager from the job manager because of the given cause.
	 * resource manager从job manager因为给定的原因断开连接
	 * @param resourceManagerId identifying the resource manager leader id
	 * @param cause of the disconnect
	 */
	void disconnectResourceManager(
		final ResourceManagerId resourceManagerId,
		final Exception cause);

	/**
	 * Offers the given slots to the job manager. The response contains the set of accepted slots.
	 *
	 * 为jobmanager提供给定的slot，返回所有接受的slots
	 *
	 * @param taskManagerId identifying the task manager
	 * @param slots         to offer to the job manager
	 * @param timeout       for the rpc call
	 * @return Future set of accepted slots.
	 */
	CompletableFuture<Collection<SlotOffer>> offerSlots(
			final ResourceID taskManagerId,
			final Collection<SlotOffer> slots,
			@RpcTimeout final Time timeout);

	/**
	 * Fails the slot with the given allocation id and cause.
	 * 根据给定的allocation id和原因，返回失败的slots
	 *
	 * @param taskManagerId identifying the task manager
	 * @param allocationId  identifying the slot to fail
	 * @param cause         of the failing
	 */
	void failSlot(final ResourceID taskManagerId,
			final AllocationID allocationId,
			final Exception cause);

	/**
	 * Registers the task manager at the job manager.
	 *
	 * 在job manager上注册task manager
	 *
	 * @param taskManagerRpcAddress the rpc address of the task manager
	 * @param taskManagerLocation   location of the task manager
	 * @param timeout               for the rpc call
	 * @return Future registration response indicating whether the registration was successful or not
	 * 表明是否注册成功的未来答复
	 */
	CompletableFuture<RegistrationResponse> registerTaskManager(
			final String taskManagerRpcAddress,
			final TaskManagerLocation taskManagerLocation,
			@RpcTimeout final Time timeout);

	/**
	 * Sends the heartbeat to job manager from task manager.
	 *
	 * 从task manager发送心跳到job manager
	 *
	 * @param resourceID unique id of the task manager
	 * @param accumulatorReport report containing accumulator updates
	 */
	void heartbeatFromTaskManager(
		final ResourceID resourceID,
		final AccumulatorReport accumulatorReport);

	/**
	 * Sends heartbeat request from the resource manager.
	 *
	 * 发送心跳到resourcemanager
	 *
	 * @param resourceID unique id of the resource manager
	 */
	void heartbeatFromResourceManager(final ResourceID resourceID);

	/**
	 * Request the details of the executed job.
	 *
	 * 请求执行完的job的详细
	 *
	 * @param timeout for the rpc call
	 * @return Future details of the executed job
	 */
	CompletableFuture<JobDetails> requestJobDetails(@RpcTimeout Time timeout);

	/**
	 * Requests the current job status.
	 * 请求当前job的状态
	 *
	 * @param timeout for the rpc call
	 * @return Future containing the current job status
	 */
	CompletableFuture<JobStatus> requestJobStatus(@RpcTimeout Time timeout);

	/**
	 * Requests the {@link ArchivedExecutionGraph} of the executed job.
	 * 请求job的档案文件
	 *
	 * @param timeout for the rpc call
	 * @return Future which is completed with the {@link ArchivedExecutionGraph} of the executed job
	 */
	CompletableFuture<ArchivedExecutionGraph> requestJob(@RpcTimeout Time timeout);

	/**
	 * Triggers taking a savepoint of the executed job.
	 * 触发savepoint，targetDeirectory 会写入savepoint的数据，如果默认的savepoint 目录没有被使用
	 * @param targetDirectory to which to write the savepoint data or null if the
	 *                           default savepoint directory should be used
	 * @param timeout for the rpc call
	 * @return Future which is completed with the savepoint path once completed
	 */
	CompletableFuture<String> triggerSavepoint(
		@Nullable final String targetDirectory,
		final boolean cancelJob,
		@RpcTimeout final Time timeout);

	/**
	 * Stops the job with a savepoint.
	 * 用savepoint 停止job
	 * @param targetDirectory to which to write the savepoint data or null if the
	 *                           default savepoint directory should be used
	 * @param advanceToEndOfEventTime Flag indicating if the source should inject a {@code MAX_WATERMARK} in the pipeline
	 *                              to fire any registered event-time timers
	 * @param timeout for the rpc call
	 * @return Future which is completed with the savepoint path once completed
	 */
	CompletableFuture<String> stopWithSavepoint(
		@Nullable final String targetDirectory,
		final boolean advanceToEndOfEventTime,
		@RpcTimeout final Time timeout);

	/**
	 * Requests the statistics on operator back pressure.
	 * 要求提供操作人员压力的统计数据。
	 *
	 * @param jobVertexId JobVertex for which the stats are requested.
	 * @return A Future to the {@link OperatorBackPressureStatsResponse}.
	 */
	CompletableFuture<OperatorBackPressureStatsResponse> requestOperatorBackPressureStats(JobVertexID jobVertexId);

	/**
	 * Notifies that the allocation has failed.
	 * 通知分配失败
	 *
	 * @param allocationID the failed allocation id.
	 * @param cause the reason that the allocation failed
	 */
	void notifyAllocationFailure(AllocationID allocationID, Exception cause);

	/**
	 * Update the aggregate and return the new value.
	 * 更新聚合值，并返回新值
	 *
	 * @param aggregateName The name of the aggregate to update
	 * @param aggregand The value to add to the aggregate
	 * @param serializedAggregationFunction The function to apply to the current aggregate and aggregand to
	 * obtain the new aggregate value, this should be of type {@link AggregateFunction}
	 * @return The updated aggregate
	 */
	CompletableFuture<Object> updateGlobalAggregate(String aggregateName, Object aggregand, byte[] serializedAggregationFunction);
}
