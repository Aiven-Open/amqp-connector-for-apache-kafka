package io.aiven.kafka.connect.amqp.source.transformer;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;
import io.aiven.kafka.connect.amqp.common.config.AmqpHeaderProperties;
import java.io.IOException;
import java.util.Collection;
import java.util.Map;
import java.util.TreeMap;
import org.apache.qpid.protonj2.client.Message;
import org.apache.qpid.protonj2.client.exceptions.ClientException;
import org.apache.qpid.protonj2.types.messaging.Section;

public final class MessageSerializer extends StdSerializer<Message> {

  public MessageSerializer() {
    this(null);
  }

  public MessageSerializer(Class<Message> t) {
    super(t);
  }

  private void writeMap(final JsonGenerator jgen, final String name, final Map<String, Object> map)
      throws IOException {
    if (!map.isEmpty()) {
      jgen.writeObjectField(name, map);
    }
  }

  private void writeObject(final JsonGenerator jgen, final String name, final Object object)
      throws IOException {
    if (object != null) {
      jgen.writeObjectField(name, object);
    }
  }

  private void writeString(final JsonGenerator jgen, final String name, final String object)
      throws IOException {
    if (object != null) {
      jgen.writeStringField(name, object);
    }
  }

  @Override
  public void serialize(Message msg, JsonGenerator jgen, SerializerProvider provider)
      throws IOException, JsonProcessingException {

    Message<Object> value = (Message<Object>) msg;
    jgen.writeStartObject();
    try {
      for (AmqpHeaderProperties property : AmqpHeaderProperties.values()) {
        switch (property) {
          case MESSAGE_ID -> writeObject(jgen, property.getSchemaName(), value.messageId());
          case USER_ID -> writeString(jgen, property.getSchemaName(), value.to());
          case SUBJECT -> writeString(jgen, property.getSchemaName(), value.subject());
          case REPLY_TO -> writeString(jgen, property.getSchemaName(), value.replyTo());
          case CORRELATION_ID -> writeObject(jgen, property.getSchemaName(), value.correlationId());
          case CONTENT_TYPE -> writeString(jgen, property.getSchemaName(), value.contentType());
          case CONTENT_ENCODING ->
              writeString(jgen, property.getSchemaName(), value.contentEncoding());
          case ABSOLUTE_EXPIRY ->
              jgen.writeNumberField(property.getSchemaName(), value.absoluteExpiryTime());
          case CREATION_TIME ->
              jgen.writeNumberField(property.getSchemaName(), value.creationTime());
          case GROUP_ID -> writeString(jgen, property.getSchemaName(), value.groupId());
          case GROUP_SEQUENCE ->
              jgen.writeNumberField(property.getSchemaName(), value.groupSequence());
          case REPLY_TO_GROUP_ID ->
              writeString(jgen, property.getSchemaName(), value.replyToGroupId());
          case DURABLE -> jgen.writeBooleanField(property.getSchemaName(), value.durable());
          case FIRST_ACQUIRER ->
              jgen.writeBooleanField(property.getSchemaName(), value.firstAcquirer());
          case DELIVERY_COUNT ->
              jgen.writeNumberField(property.getSchemaName(), value.deliveryCount());
        }
      }

      TreeMap<String, Object> map = new TreeMap<>();
      value.forEachAnnotation(map::put);
      writeMap(jgen, "annotations", map);

      map.clear();
      value.forEachProperty(map::put);
      writeMap(jgen, "properties", map);

      map.clear();
      value.forEachFooter(map::put);
      writeMap(jgen, "footers", map);

      Object body = extractBody(value);
      if (body != null) {
        jgen.writePOJOField("body", body);
      }

      jgen.writeEndObject();
    } catch (ClientException e) {
      throw new IOException(e);
    }
  }

  private Object extractBody(Message<?> value) throws IOException {
    try {
      Collection<Section<?>> sections = value.toAdvancedMessage().bodySections();
      if (sections.isEmpty()) {
        return null;
      }
      return sections.size() == 1 ? sections.iterator().next() : sections;
    } catch (ClientException e) {
      throw new IOException(e);
    }
  }
}
