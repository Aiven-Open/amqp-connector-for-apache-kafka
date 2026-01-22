/*
 * Copyright 2024 Aiven Oy
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.aiven.kafka.connect.amqp.source;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.aiven.commons.kafka.source.config.SourceCommonConfig;
import io.aiven.commons.kafka.source.task.Context;
import io.aiven.commons.kafka.source.transformer.Transformer;
import io.aiven.kafka.connect.amqp.source.exception.ConversionConnectException;
import org.apache.commons.io.function.IOSupplier;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.qpid.protonj2.client.Delivery;
import org.apache.qpid.protonj2.client.Message;
import org.apache.qpid.protonj2.client.exceptions.ClientException;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;

///**
// * AmqpTransformer chunks an entire object into a maximum size specified by the
// * {@link io.aiven.kafka.connect.common.config.TransformerFragment#TRANSFORMER_MAX_BUFFER_SIZE} configuration option.
// * <p>
// * If the configuration option specifies a buffer that is smaller than the length of the input stream, the record will
// * be split into multiple parts. When this happens the transformer makes no guarantees for only once delivery or
// * delivery order as those are dependant upon the Kafka producer and remote consumer configurations. This class will
// * produce the blocks in order and on restart will send any blocks that were not acknowledged by Kafka.
// * </p>
// */
public class AmqpTransformer extends Transformer {
	// private static final Logger LOGGER =
	// LoggerFactory.getLogger(AmqpTransformer.class);
	// private static final Map<Class<?>, Schema> PrimitiveMap = new HashMap<>();

	private final ObjectMapper mapper = new ObjectMapper();

	AmqpTransformer() {
	}

	// static {
	// PrimitiveMap.put(byte.class, Schema.INT8_SCHEMA);
	// PrimitiveMap.put(short.class, Schema.INT16_SCHEMA);
	// PrimitiveMap.put(int.class, Schema.INT32_SCHEMA);
	// PrimitiveMap.put(long.class, Schema.INT64_SCHEMA);
	// PrimitiveMap.put(float.class, Schema.FLOAT32_SCHEMA);
	// PrimitiveMap.put(double.class, Schema.FLOAT64_SCHEMA);
	// PrimitiveMap.put(boolean.class, Schema.BOOLEAN_SCHEMA);
	// PrimitiveMap.put(String.class, Schema.STRING_SCHEMA);
	// }

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

	@Override
	public Stream<SchemaAndValue> getRecords(AmqpSourceData nativeSourceData, AmqpSourceRecord sourceRecord,
											 SourceCommonConfig sourceConfig) {
		Delivery delivery = nativeSourceData.getNativeItemStream().findAny().get();
		List<SchemaAndValue> entries = new ArrayList<>();

		try {
			Message<?> message = delivery.message();

			ObjectNode root = mapper.createObjectNode().put("creationTime", message.creationTime())
					.put("subject", message.subject()).put("groupId", message.groupId())
					.put("groupSequence", message.groupSequence()).put("messageId", asBytes(message.messageId()))
					.put("body", asBytes(message.body())).put("priority", message.priority())
					.put("userId", message.userId()).put("durable", message.durable())
					.put("timeToLive", message.timeToLive()).put("firstAcquirer", message.firstAcquirer())
					.put("replyTo", message.replyTo()).put("absoluteExpiryTime", message.absoluteExpiryTime())
					.put("contentEncoding", message.contentEncoding()).put("replyToGroupId", message.replyToGroupId())
					.put("correlationId", asBytes(message.correlationId())).put("to", message.to())
					.put("contentType", message.contentType()).put("deliveryCount", message.deliveryCount());
			ObjectNode annotations = mapper.createObjectNode();
			message.forEachAnnotation((k, v) -> annotations.put(k, asBytes(v)));
			root.set("annotations", annotations);
			ObjectNode properties = mapper.createObjectNode();
			message.forEachProperty((k, v) -> properties.put(k, asBytes(v)));
			root.set("properties", properties);
			ObjectNode footers = mapper.createObjectNode();
			message.forEachProperty((k, v) -> footers.put(k, asBytes(v)));
			root.set("footers", footers);

			return Arrays.asList(new SchemaAndValue(MessageSchema, root)).stream();
			// new SchemaAndValue()
			//
			// entries.add(convertObject("body", message.body(), false));
			// entries.add(convertObject("absoluteExpiryTime", message.absoluteExpiryTime(),
			// false));
			// entries.add(convertObject("contentEncoding", message.contentEncoding(),
			// true));
			// entries.add(convertObject("contentType", message.contentType(), true));
			// entries.add(convertObject("correlationId", message.correlationId(), true));
			// entries.add(convertObject("creationTime", message.creationTime(), false));
			// entries.add(convertObject("deliveryCount", message.deliveryCount(), false));
			// entries.add(convertObject("durable", message.durable(), false));
			// entries.add(convertObject("firstAcquirer", message.firstAcquirer(), false));
			// entries.add(convertObject("groupId", message.groupId(), true));
			// entries.add(convertObject("groupSequence", message.groupSequence(), true));
			// entries.add(convertObject("priority", message.priority(), false));
			// entries.add(convertObject("messageId", message.messageId(), true));
			// entries.add(convertObject("replyTo", message.replyTo(), true));
			// entries.add(convertObject("replyToGroupId", message.replyToGroupId(), true));
			// entries.add(convertObject("subject", message.subject(), true));
			// entries.add(convertObject("timeToLive", message.timeToLive(), true));
			// entries.add(convertObject("to", message.to(), true));
			// entries.add(convertObject("userId", message.userId(), true));
		} catch (ClientException e) {
			throw new ConversionConnectException(e);
		}

	}

	// public static void main(String[] args) throws ClassNotFoundException {
	// Schema schema = createSchema(Message.class);
	// System.out.println(schema);
	// }

	static Schema MessageSchema = SchemaBuilder.struct().name(Message.class.getCanonicalName())
			.field("creationTime", Schema.INT16_SCHEMA).field("subject", Schema.STRING_SCHEMA)
			.field("groupId", Schema.STRING_SCHEMA).field("groupSequence", Schema.INT32_SCHEMA)
			.field("messageId", Schema.BYTES_SCHEMA).field("body", Schema.BYTES_SCHEMA)
			.field("priority", Schema.INT32_SCHEMA).field("userId", Schema.BYTES_SCHEMA)
			.field("durable", Schema.BOOLEAN_SCHEMA).field("timeToLive", Schema.INT64_SCHEMA)
			.field("firstAcquirer", Schema.BOOLEAN_SCHEMA).field("replyTo", Schema.STRING_SCHEMA)
			.field("absoluteExpiryTime", Schema.INT64_SCHEMA).field("contentEncoding", Schema.STRING_SCHEMA)
			.field("replyToGroupId", Schema.STRING_SCHEMA).field("correlationId", Schema.BYTES_SCHEMA)
			.field("to", Schema.STRING_SCHEMA).field("contentType", Schema.STRING_SCHEMA)
			.field("deliveryCount", Schema.INT64_SCHEMA)
			.field("annotations", SchemaBuilder.map(Schema.STRING_SCHEMA, Schema.BYTES_SCHEMA))
			.field("properties", SchemaBuilder.map(Schema.STRING_SCHEMA, Schema.BYTES_SCHEMA))
			.field("footers", SchemaBuilder.map(Schema.STRING_SCHEMA, Schema.BYTES_SCHEMA));

	// static Schema createSchema(Class<?> clazz) throws ClassNotFoundException {
	// if (clazz.equals(Object.class)) {
	// return Schema.BYTES_SCHEMA;
	// }
	// if (clazz.isPrimitive()) {
	// return PrimitiveMap.get(clazz);
	// }
	// SchemaBuilder builder;
	// Class.forName(ConnectSchema.class.getName());
	// Schema.Type schemaType = ConnectSchema.schemaType(clazz);
	// if (schemaType != null) {
	// builder = SchemaBuilder.type(schemaType);
	// } else {
	// builder = SchemaBuilder.struct().name(clazz.getSimpleName());
	// Map<String, Schema> methods = new TreeMap<>();
	// for (Method method : clazz.getDeclaredMethods()) {
	// if (Modifier.isPublic(method.getModifiers()) && method.getParameterCount() ==
	// 0) {
	// Class<?> returnType = method.getReturnType();
	// if (!(returnType.equals(Void.TYPE) || returnType.equals(clazz) ||
	// returnType.equals(AdvancedMessage.class))) {
	// methods.put(method.getName(), createSchema(returnType));
	// }
	// }
	// }
	// methods.forEach(builder::field);
	// }
	// return builder.schema();
	// }

	@Override
	public SchemaAndValue getKeyData(final Object nativeKey, final String topic,
			final SourceCommonConfig sourceConfig) {
		return new SchemaAndValue(null, ((String) nativeKey).getBytes(StandardCharsets.UTF_8));
	}
	//
	//
	// static final Schema SCHEMA_KEY = SchemaBuilder.struct()
	// .field(
	// "messageId",
	//
	// SchemaBuilder.string().optional().doc("The value in the messageId field. " +
	// "`BasicProperties.getMessageId()
	// <https://www.rabbitmq.com/releases/rabbitmq-java-client/current-javadoc/com/rabbitmq/client/BasicProperties.html#getMessageId-->`_").build()
	// )
	// .build();
	//
	//
	// private static void configureConversionMap() {
	// Map<Class<?>, Function<Object, SchemaAndValue>> conversionMap = new
	// HashMap<>();
	// conversionMap.put(String.class, obj -> new
	// SchemaAndValue(Schema.STRING_SCHEMA, obj));
	// conversionMap.put(Binary.class, obj -> new
	// SchemaAndValue(Schema.BYTES_SCHEMA, ((Binary)obj).asByteArray()));
	// conversionMap.put(Decimal32.class, obj -> new
	// SchemaAndValue(Schema.INT32_SCHEMA, ((Decimal32)obj).intValue()));
	// conversionMap.put(Decimal64.class, obj -> new
	// SchemaAndValue(Schema.INT64_SCHEMA, ((Decimal64)obj).longValue()));
	// conversionMap.put(UnsignedByte.class, obj -> new
	// SchemaAndValue(Schema.INT16_SCHEMA, ((UnsignedByte)obj).intValue()));
	// conversionMap.put(UnsignedShort.class, obj -> new
	// SchemaAndValue(Schema.INT32_SCHEMA, ((UnsignedShort)obj).intValue()));
	// conversionMap.put(UnsignedInteger.class, obj -> new
	// SchemaAndValue(Schema.INT64_SCHEMA, ((UnsignedInteger)obj).longValue()));
	// // Decimal128
	// // unsigned long
	//
	// }
	//
	// private Optional<SchemaAndValue> convertObject(String name, Object
	// ampqObject, boolean isOptional) {
	// if (ampqObject == null) {
	// if (isOptional) {
	// // short circuit converting the object
	// return Optional.empty();
	// } else {
	// throw new ConversionConnectException(
	// name + " is not optional, but converting object had null value");
	// }
	// }
	//
	// SchemaAndValue result = null;
	// Schema schema = null;
	// Schema.Type schemaType = ConnectSchema.schemaType(ampqObject.getClass());
	// if (schemaType != null) {
	// schema = SchemaBuilder.type(schemaType).schema();
	// } else {
	//
	//
	// }
	// if (ampqObject instanceof String) {
	// result = new SchemaAndValue(Schema.STRING_SCHEMA, (String) ampqObject);
	// } else {
	//
	// }
	//
	// switch (ampqObject.getClass()) {}
	//
	//
	// switch (kafkaConnectSchemaType) {
	// case ARRAY:
	// return convertArray(kafkaConnectObject, kafkaConnectSchema);
	// case MAP:
	// return convertMap(kafkaConnectObject, kafkaConnectSchema);
	// case STRUCT:
	// return convertStruct(kafkaConnectObject, kafkaConnectSchema);
	// case BYTES:
	// return convertBytes(kafkaConnectObject);
	// case FLOAT64:
	// return convertDouble((Double) kafkaConnectObject);
	//
	// case FLOAT32:
	// return useStorageWriteApi
	// ? ((Float) kafkaConnectObject).doubleValue()
	// : convertFloat(kafkaConnectObject);
	// case INT8:
	// return useStorageWriteApi
	// ? ((Byte) kafkaConnectObject).intValue()
	// : kafkaConnectObject;
	// case INT16:
	// return useStorageWriteApi
	// ? ((Short) kafkaConnectObject).intValue()
	// : kafkaConnectObject;
	//
	// case BOOLEAN:
	// case INT32:
	// case INT64:
	// case STRING:
	// return kafkaConnectObject;
	// default:
	// throw new ConversionConnectException("Unrecognized schema type: " +
	// kafkaConnectSchemaType);
	// }
	// }
	//
	// private KafkaType
}
