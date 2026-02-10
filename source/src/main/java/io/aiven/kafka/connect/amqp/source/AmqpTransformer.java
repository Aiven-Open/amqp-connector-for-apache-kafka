/*
         Copyright 2026 Aiven Oy and project contributors

        Licensed under the Apache License, Version 2.0 (the "License");
        you may not use this file except in compliance with the License.
        You may obtain a copy of the License at

        http://www.apache.org/licenses/LICENSE-2.0

        Unless required by applicable law or agreed to in writing,
        software distributed under the License is distributed on an
        "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
        KIND, either express or implied.  See the License for the
        specific language governing permissions and limitations
        under the License.

        SPDX-License-Identifier: Apache-2
 */

package io.aiven.kafka.connect.amqp.source;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
//import io.aiven.commons.kafka.source.config.SourceCommonConfig;
//import io.aiven.commons.kafka.source.task.Context;
//import io.aiven.commons.kafka.source.transformer.Transformer;
import com.github.victools.jsonschema.generator.OptionPreset;
import com.github.victools.jsonschema.generator.SchemaGenerator;
import com.github.victools.jsonschema.generator.SchemaGeneratorConfig;
import com.github.victools.jsonschema.generator.SchemaGeneratorConfigBuilder;
import com.github.victools.jsonschema.generator.SchemaVersion;
import io.aiven.commons.kafka.connector.source.config.SourceCommonConfig;
import io.aiven.commons.kafka.connector.source.task.Context;
import io.aiven.commons.kafka.connector.source.transformer.Transformer;
import io.aiven.kafka.connect.amqp.common.config.AmqpHeaderProperties;
import io.aiven.kafka.connect.amqp.source.config.AmqpSourceConfig;
import io.aiven.kafka.connect.amqp.source.config.AmqpSourceFragment;
import org.apache.commons.io.function.IOSupplier;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.qpid.protonj2.client.Message;
import org.apache.qpid.protonj2.client.exceptions.ClientException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.List;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.stream.Stream;

/**
 * AmqpTransformer processes a single AMQP Message extracting data and key
 * values.
 */
public class AmqpTransformer extends Transformer {
	private static final Logger LOGGER = LoggerFactory.getLogger(AmqpTransformer.class);
	private final AmqpSourceConfig amqpSourceConfig;
	private final ObjectMapper mapper = new ObjectMapper();
	private SchemaGeneratorConfigBuilder configBuilder = new SchemaGeneratorConfigBuilder(SchemaVersion.DRAFT_2020_12,
			OptionPreset.PLAIN_JSON);
	private SchemaGeneratorConfig config = configBuilder.build();
	private SchemaGenerator generator = new SchemaGenerator(config);

	public AmqpTransformer(final AmqpSourceConfig amqpSourceConfig) {
		this.amqpSourceConfig = amqpSourceConfig;
	}

	@Override
	public StreamSpliterator createSpliterator(final IOSupplier<InputStream> inputStreamIOSupplier,
			final long streamLength, final Context<?> context, final SourceCommonConfig sourceConfig) {
		throw new IllegalStateException();
	}

	private byte[] asBytes(Object object) {
		if (mapper.canSerialize(object.getClass())) {
			return mapper.convertValue(object, byte[].class);
		}
		return null;
	}

	public SchemaAndValue createRecords(Function<AmqpSourceFragment.Section, Stream<String>> dataProvider,
			Message<?> message) {
		ObjectNode root = mapper.createObjectNode();
		SchemaBuilder valueSchema = SchemaBuilder.struct();
		for (AmqpHeaderProperties property : dataProvider.apply(AmqpSourceFragment.Section.HEADER)
				.map(AmqpHeaderProperties::valueOf).toList()) {
			if (property.getSchema() == null) {
				valueSchema.field(property.getSchemaName(), SchemaBuilder.OPTIONAL_BYTES_SCHEMA);
			} else {
				valueSchema.field(property.getSchemaName(), property.getSchema());
			}
			try {
				switch (property) {
					case MESSAGE_ID -> root.put(property.getSchemaName(), asBytes(message.messageId()));
					case USER_ID -> root.put(property.getSchemaName(), message.userId());
					case TO -> root.put(property.getSchemaName(), message.to());
					case SUBJECT -> root.put(property.getSchemaName(), message.subject());
					case REPLY_TO -> root.put(property.getSchemaName(), message.replyTo());
					case CORRELATION_ID -> root.put(property.getSchemaName(), asBytes(message.correlationId()));
					case CONTENT_TYPE -> root.put(property.getSchemaName(), message.contentType());
					case CONTENT_ENCODING -> root.put(property.getSchemaName(), message.contentEncoding());
					case ABSOLUTE_EXPIRY -> root.put(property.getSchemaName(), message.absoluteExpiryTime());
					case CREATION_TIME -> root.put(property.getSchemaName(), message.creationTime());
					case GROUP_ID -> root.put(property.getSchemaName(), message.groupId());
					case GROUP_SEQUENCE -> root.put(property.getSchemaName(), message.groupSequence());
					case REPLY_TO_GROUP_ID -> root.put(property.getSchemaName(), message.replyToGroupId());
					case DURABLE -> root.put(property.getSchemaName(), message.durable());
					case FIRST_ACQUIRER -> root.put(property.getSchemaName(), message.firstAcquirer());
					case DELIVERY_COUNT -> root.put(property.getSchemaName(), message.deliveryCount());
				}
			} catch (ClientException e) {
				LOGGER.error("Unable to create value entry for {}: {}", property, e.getMessage(), e);
			}
		}

		try {
			List<String> acceptedKeys = dataProvider.apply(AmqpSourceFragment.Section.ANNOTATION).toList();
			ObjectConsumer processor = new ObjectConsumer();
			message.forEachAnnotation((k, v) -> {
				if (acceptedKeys.contains(k)) {
					processor.accept(k, v);
				}
			});
			root.set("annotations", processor.getValue());
			valueSchema.field("annotations", processor.schemaBuilder);
		} catch (ClientException e) {
			LOGGER.error("Unable to process annotations: {}", e.getMessage(), e);
		}

		try {
			List<String> acceptedKeys = dataProvider.apply(AmqpSourceFragment.Section.PROPERTY).toList();
			ObjectConsumer propertiesProcessor = new ObjectConsumer();
			message.forEachProperty((k, v) -> {
				if (acceptedKeys.contains(k)) {
					propertiesProcessor.accept(k, v);
				}
			});
			root.set("properties", propertiesProcessor.getValue());
			valueSchema.field("properties", propertiesProcessor.schemaBuilder);
		} catch (ClientException e) {
			LOGGER.error("Unable to process properties: {}", e.getMessage(), e);
		}

		try {
			List<String> acceptedKeys = dataProvider.apply(AmqpSourceFragment.Section.FOOTER).toList();
			ObjectConsumer footersProcessor = new ObjectConsumer();
			message.forEachFooter((k, v) -> {
				if (acceptedKeys.contains(k)) {
					footersProcessor.accept(k, v);
				}
			});
			root.set("footers", footersProcessor.getValue());
			valueSchema.field("footers", footersProcessor.schemaBuilder);
		} catch (ClientException e) {
			LOGGER.error("Unable to process footers: {}", e.getMessage(), e);
		}

		try {
			ObjectConsumer bodyProcessor = new ObjectConsumer();
			bodyProcessor.accept("body", message.body());
			root.set("body", bodyProcessor.getValue().get("body"));
			valueSchema.field("body", bodyProcessor.getSchema().field("body").schema());
		} catch (ClientException e) {
			LOGGER.error("Unable to process body: {}", e.getMessage(), e);
		}

		return new SchemaAndValue(valueSchema, root);
	}

	public SchemaAndValue getValueData(AmqpSourceRecord amqpSourceRecord) {
		try {
			return createRecords(k -> amqpSourceConfig.getValues(k),
					amqpSourceRecord.getNativeItem().getDelivery().message());
		} catch (ClientException e) {
			throw new ConnectException(String.format("Unable to read message: %s", e.getMessage()), e);
		}
	}

	@Override
	public SchemaAndValue getKeyData(Object nativeKey, String topic, SourceCommonConfig sourceConfig) {
		try {
			return createRecords(k -> amqpSourceConfig.getKeys(k),
					amqpSourceRecord.getNativeItem().getDelivery().message());
		} catch (ClientException e) {
			throw new ConnectException(String.format("Unable to read message: %s", e.getMessage()), e);
		}
	}

	private class ObjectConsumer implements BiConsumer<String, Object> {
		private final ObjectNode root = mapper.createObjectNode();
		private final SchemaBuilder schemaBuilder = SchemaBuilder.struct();

		public ObjectNode getValue() {
			return root;
		}

		public SchemaBuilder getSchema() {
			return schemaBuilder;
		}

		@Override
		public void accept(String s, Object o) {
			if (o == null) {
				schemaBuilder.field(s, Schema.OPTIONAL_BYTES_SCHEMA);
				root.putNull(s);
			} else if (o instanceof Number n) {
				if (o instanceof Byte) {
					schemaBuilder.field(s, Schema.INT8_SCHEMA);
					root.put(s, n.byteValue());
				} else if (o instanceof Short) {
					schemaBuilder.field(s, Schema.INT16_SCHEMA);
					root.put(s, n.shortValue());
				} else if (o instanceof Integer) {
					schemaBuilder.field(s, Schema.INT32_SCHEMA);
					root.put(s, n.intValue());
				} else if (o instanceof Long) {
					schemaBuilder.field(s, Schema.INT64_SCHEMA);
					root.put(s, n.longValue());
				} else if (o instanceof Float) {
					schemaBuilder.field(s, Schema.FLOAT32_SCHEMA);
					root.put(s, n.floatValue());
				} else if (o instanceof Double) {
					schemaBuilder.field(s, Schema.FLOAT64_SCHEMA);
					root.put(s, n.floatValue());
				} else if (o instanceof BigDecimal bd) {
					schemaBuilder.field(s,
							SchemaBuilder.struct().name("BigDecimal").field("value", Schema.STRING_SCHEMA));
					root.put(s, root.objectNode().put("value", bd.toString()));
				} else if (o instanceof BigInteger bi) {
					schemaBuilder.field(s,
							SchemaBuilder.struct().name("BigInteger").field("value", Schema.STRING_SCHEMA));
					root.put(s, root.objectNode().put("value", bi.toString()));
				}
			} else if (o instanceof String) {
				schemaBuilder.field(s, Schema.STRING_SCHEMA);
				root.put(s, (String) o);
			} else if (o instanceof Boolean) {
				schemaBuilder.field(s, Schema.BOOLEAN_SCHEMA);
				root.put(s, (Boolean) o);
			} else if (o instanceof byte[]) {
				schemaBuilder.field(s, Schema.BYTES_SCHEMA);
				root.put(s, (byte[]) o);
			} else {
				schemaBuilder.field(s, SchemaBuilder.struct().name("Object").field("class", Schema.STRING_SCHEMA)
						.field("json", Schema.STRING_SCHEMA));
				root.set(s, root.objectNode().put("class", o.getClass().getCanonicalName()).put("json",
						generator.generateSchema(o.getClass()).toPrettyString()));
			}
		}
	}
}
