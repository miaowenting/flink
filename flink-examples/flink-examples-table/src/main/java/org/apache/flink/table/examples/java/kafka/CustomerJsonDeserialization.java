package org.apache.flink.table.examples.java.kafka;

import com.google.common.collect.Maps;
import org.apache.flink.api.common.serialization.AbstractDeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.JsonNodeType;
import org.apache.flink.streaming.connectors.kafka.internal.KafkaConsumerThread;
import org.apache.flink.streaming.connectors.kafka.internals.AbstractFetcher;
import org.apache.flink.types.Row;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.reflect.Field;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Iterator;
import java.util.Map;

/**
 * Description:
 *
 * @author mwt
 * @version 1.0
 * @date 2019-10-11
 */
public class CustomerJsonDeserialization extends AbstractDeserializationSchema<Row> {
	private static final Logger log = LoggerFactory.getLogger(CustomerJsonDeserialization.class);
	private static final long serialVersionUID = 2385115520960444192L;

	private final ObjectMapper objectMapper = new ObjectMapper();
	/**
	 * Type information describing the result type.
	 */
	private final TypeInformation<Row> typeInfo;
	/**
	 * Field names to parse. Indices match fieldTypes indices.
	 */
	private final String[] fieldNames;
	/**
	 * Types to parse fields as. Indices match fieldNames indices.
	 */
	private final TypeInformation<?>[] fieldTypes;
	/**
	 * Flag indicating whether to fail on a missing field.
	 */
	private boolean failOnMissingField;
	private AbstractFetcher<Row, ?> fetcher;
	private boolean firstMsg = true;
	private Map<String, String> rowAndFieldMapping;
	private Map<String, JsonNode> nodeAndJsonnodeMapping = Maps.newHashMap();

	public CustomerJsonDeserialization(TypeInformation<Row> typeInfo, Map<String, String> rowAndFieldMapping) {
		this.typeInfo = typeInfo;
		this.fieldNames = ((RowTypeInfo) typeInfo).getFieldNames();
		this.fieldTypes = ((RowTypeInfo) typeInfo).getFieldTypes();
		this.rowAndFieldMapping = rowAndFieldMapping;
	}

	@Override
	public Row deserialize(byte[] message) {

		if (firstMsg) {
			try {
				registerPtMetric(fetcher);
			} catch (Exception e) {
				log.error("register topic partition metric error.", e);
			}

			firstMsg = false;
		}

		try {

			JsonNode root = objectMapper.readTree(message);
			parseTree(root, "");

			Row row = new Row(fieldNames.length);

			for (int i = 0; i < fieldNames.length; i++) {
				JsonNode node = getIgnoreCase(fieldNames[i]);

				if (node == null) {
					if (failOnMissingField) {
						throw new IllegalStateException("Failed to find field with name '"
							+ fieldNames[i] + "'.");
					} else {
						row.setField(i, null);
					}
				} else {
					row.setField(i, convert(node, fieldTypes[i]));
				}
			}

			return row;
		} catch (Exception e) {
			// add metric of dirty data
			log.error(e.getMessage(), e);
			return null;
		}
	}

	public void parseTree(JsonNode jsonNode, String prefix) {
		nodeAndJsonnodeMapping.clear();

		Iterator<String> iterator = jsonNode.fieldNames();
		while (iterator.hasNext()) {
			String next = iterator.next();
			JsonNode child = jsonNode.get(next);
			if (child.isObject()) {
				parseTree(child, next + "." + prefix);
			} else {
				nodeAndJsonnodeMapping.put(prefix + next, child);
			}
		}
	}

	public void setFailOnMissingField(boolean failOnMissingField) {
		this.failOnMissingField = failOnMissingField;
	}


	public JsonNode getIgnoreCase(String key) {
		String nodeMappingKey = rowAndFieldMapping.get(key);
		JsonNode node = nodeAndJsonnodeMapping.get(nodeMappingKey);
		JsonNodeType nodeType = node.getNodeType();

		if (nodeType == JsonNodeType.ARRAY) {
			throw new IllegalStateException("Unsupported  type information  array .");
		}

		return node;
	}

	public void setFetcher(AbstractFetcher<Row, ?> fetcher) {
		this.fetcher = fetcher;
	}


	protected void registerPtMetric(AbstractFetcher<Row, ?> fetcher) throws Exception {

		Field consumerThreadField = fetcher.getClass().getSuperclass().getDeclaredField("consumerThread");
		consumerThreadField.setAccessible(true);
		KafkaConsumerThread consumerThread = (KafkaConsumerThread) consumerThreadField.get(fetcher);

		Field hasAssignedPartitionsField = consumerThread.getClass().getDeclaredField("hasAssignedPartitions");
		hasAssignedPartitionsField.setAccessible(true);

		//wait until assignedPartitions

		boolean hasAssignedPartitions = (boolean) hasAssignedPartitionsField.get(consumerThread);

		if (!hasAssignedPartitions) {
			throw new RuntimeException("wait 50 secs, but not assignedPartitions");
		}

		Field consumerField = consumerThread.getClass().getDeclaredField("consumer");
		consumerField.setAccessible(true);

		KafkaConsumer kafkaConsumer = (KafkaConsumer) consumerField.get(consumerThread);
		Field subscriptionStateField = kafkaConsumer.getClass().getDeclaredField("subscriptions");
		subscriptionStateField.setAccessible(true);

		//topic partitions lag

	}

	// --------------------------------------------------------------------------------------------

	private Object convert(JsonNode node, TypeInformation<?> info) {
		if (info.getTypeClass().equals(Types.BOOLEAN.getTypeClass())) {
			return node.asBoolean();
		} else if (info.getTypeClass().equals(Types.STRING.getTypeClass())) {
			return node.asText();
		} else if (info.getTypeClass().equals(Types.SQL_DATE.getTypeClass())) {
			return Date.valueOf(node.asText());
		} else if (info.getTypeClass().equals(Types.SQL_TIME.getTypeClass())) {
			// local zone
			return Time.valueOf(node.asText());
		} else if (info.getTypeClass().equals(Types.SQL_TIMESTAMP.getTypeClass())) {
			// local zone
			return Timestamp.valueOf(node.asText());
		} else if (info.getTypeClass().equals(Types.BYTE.getTypeClass())) {
			return convertByteArray(node);
		} else {
			// for types that were specified without JSON schema
			// e.g. POJOs
			try {
				return objectMapper.treeToValue(node, info.getTypeClass());
			} catch (JsonProcessingException e) {
				throw new IllegalStateException("Unsupported type information '" + info + "' for node: " + node);
			}
		}
	}

	private Object convertByteArray(JsonNode node) {
		try {
			return node.binaryValue();
		} catch (IOException e) {
			throw new RuntimeException("Unable to deserialize byte array.", e);
		}
	}
}


