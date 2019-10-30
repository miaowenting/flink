package org.apache.flink.table.examples.java.kafka;

import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks;
import org.apache.flink.streaming.api.operators.StreamingRuntimeContext;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.streaming.connectors.kafka.config.OffsetCommitMode;
import org.apache.flink.streaming.connectors.kafka.internals.AbstractFetcher;
import org.apache.flink.streaming.connectors.kafka.internals.KafkaTopicPartition;
import org.apache.flink.types.Row;
import org.apache.flink.util.SerializedValue;

import java.util.Arrays;
import java.util.Map;
import java.util.Properties;
import java.util.regex.Pattern;

/**
 * Description:
 *
 * @author mwt
 * @version 1.0
 * @date 2019-10-11
 */
public class CustomerJsonConsumer extends FlinkKafkaConsumer011<Row> {

	private static final long serialVersionUID = -2265366268827807739L;

	private CustomerJsonDeserialization customerJsonDeserialization;

	public CustomerJsonConsumer(String topic, CustomerJsonDeserialization valueDeserializer, Properties props) {
		super(Arrays.asList(topic.split(",")), valueDeserializer, props);
		this.customerJsonDeserialization = valueDeserializer;
	}

	public CustomerJsonConsumer(Pattern subscriptionPattern,
								CustomerJsonDeserialization valueDeserializer, Properties props) {
		super(subscriptionPattern, valueDeserializer, props);
		this.customerJsonDeserialization = valueDeserializer;
	}


	@Override
	public void run(SourceContext<Row> sourceContext) throws Exception {
		super.run(sourceContext);
	}

	@Override
	protected AbstractFetcher<Row, ?> createFetcher(SourceContext<Row> sourceContext,
													Map<KafkaTopicPartition, Long> assignedPartitionsWithInitialOffsets,
													SerializedValue<AssignerWithPeriodicWatermarks<Row>> watermarksPeriodic,
													SerializedValue<AssignerWithPunctuatedWatermarks<Row>> watermarksPunctuated,
													StreamingRuntimeContext runtimeContext,
													OffsetCommitMode offsetCommitMode,
													MetricGroup consumerMetricGroup,
													boolean useMetrics) throws Exception {
		AbstractFetcher<Row, ?> fetcher = super.createFetcher(
			sourceContext,
			assignedPartitionsWithInitialOffsets,
			watermarksPeriodic,
			watermarksPunctuated,
			runtimeContext,
			offsetCommitMode,
			consumerMetricGroup,
			useMetrics);
		customerJsonDeserialization.setFetcher(fetcher);
		return fetcher;
	}
}

