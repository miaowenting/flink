package org.apache.flink.table.examples.java.kafka;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import scala.tools.jline_embedded.internal.TestAccessible;

import java.util.Properties;

/**
 * Description:
 *
 * @author mwt
 * @version 1.0
 * @date 2019-10-11
 */
public class KafkaSourceTable {

	public static void main(String[] args) {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

		String[] fields = {"a", "b"};
		TypeInformation[] types = {TypeInformation.of(String.class), TypeInformation.of(Long.class)};
		TypeInformation<Row> rowTypeInfo = new RowTypeInfo(types, fields);

		Properties props = new Properties();
		DataStream<Row> stream = env
			.addSource(new FlinkKafkaConsumer011<>("topic", new CustomerJsonDeserialization(rowTypeInfo, null),
				props)).returns(rowTypeInfo);
		Table table1 = tEnv.fromDataStream(stream, "a,b");


		FlinkKafkaConsumer011<Row> flinkKafkaConsumer011 = new CustomerJsonConsumer("topic",
			new CustomerJsonDeserialization(rowTypeInfo, null), props);

		DataStreamSource<Row> kafkaSource =
			(DataStreamSource<Row>) env.addSource(flinkKafkaConsumer011, "sourceName", rowTypeInfo).returns(rowTypeInfo);
		Table kafkaSourceTable = tEnv.fromDataStream(kafkaSource, "a,b");
		tEnv.registerTable("kafkaSourceTable", kafkaSourceTable);
	}
}
