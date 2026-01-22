// package io.aiven.kafka.connect.amqp.source.converter;
//
// import com.fasterxml.jackson.databind.ObjectMapper;
// import com.fasterxml.jackson.databind.node.ObjectNode;
// import org.apache.kafka.connect.data.Schema;
// import org.apache.kafka.connect.data.SchemaAndValue;
// import org.apache.kafka.connect.data.SchemaBuilder;
// import org.apache.qpid.protonj2.client.Message;
//
// import java.util.HashMap;
// import java.util.Map;
//
// public class MessageConverter implements RecordConverter<Message<?>> {
//
// private final ObjectMapper mapper = new ObjectMapper();
//
// private final static Map<String, Schema> builders = new HashMap<>();
//
// static {
// Message<?> message;
// builders.put("creationTime", Schema.INT64_SCHEMA);
// builders.put("subject", Schema.OPTIONAL_STRING_SCHEMA);
// builders.put("groupId", Schema.OPTIONAL_STRING_SCHEMA);
// builders.put("groupSequence", Schema.FLOAT32_SCHEMA);
// builders.put("messageId", null);
// builders.put("body", null);
// builders.put("priority", Schema.INT8_SCHEMA);
// builders.put("userId", Schema.OPTIONAL_BYTES_SCHEMA);
// builders.put("durable", Schema.message.durable());
// builders.put("timeToLive", message.timeToLive());
// builders.put("firstAcquirer", message.firstAcquirer());
// builders.put("replyTo", message.replyTo());
// builders.put("absoluteExpiryTime", message.absoluteExpiryTime());
// builders.put("contentEncoding", message.contentEncoding());
// builders.put("replyToGroupId", message.replyToGroupId());
// builders.put("correlationId", asBytes(message.correlationId()));
// builders.put("to", message.to());
// builders.put("contentType", message.contentType());
// builders.put("deliveryCount", message.deliveryCount());
// }
// /*
// * Schema dateSchema = SchemaBuilder.struct()
// * .name("com.example.CalendarDate").version(2).
// * doc("A calendar date including month, day, and year.") .field("month",
// * Schema.STRING_SCHEMA) .field("day", Schema.INT8_SCHEMA) .field("year",
// * Schema.INT16_SCHEMA) .build();
// */
// private final Schema schema =
// SchemaBuilder.struct().name(Message.class.getName()).version(1).doc("A AMQP
// Message")
// .field("c", Schema.STRING_SCHEMA);
// private byte[] asBytes(Object object) {
// if (mapper.canSerialize(object.getClass())) {
// return mapper.convertValue(object, byte[].class);
// }
// return null;
// }
//
// private void mapping(Message<?> message) {
//
// ObjectNode root = mapper.createObjectNode().put("creationTime",
// message.creationTime())
// .put("subject", message.subject()).put("groupId", message.groupId())
// .put("groupSequence", message.groupSequence()).put("messageId",
// asBytes(message.messageId()))
// .put("body", asBytes(message.body())).put("priority", message.priority())
// .put("userId", message.userId()).put("durable", message.durable())
// .put("timeToLive", message.timeToLive()).put("firstAcquirer",
// message.firstAcquirer())
// .put("replyTo", message.replyTo()).put("absoluteExpiryTime",
// message.absoluteExpiryTime())
// .put("contentEncoding", message.contentEncoding()).put("replyToGroupId",
// message.replyToGroupId())
// .put("correlationId", asBytes(message.correlationId())).put("to",
// message.to())
// .put("contentType", message.contentType()).put("deliveryCount",
// message.deliveryCount());
//
// }
//
// @Override
// public SchemaAndValue convertRecord(Message<?> record) {
// return null;
// }
// }
