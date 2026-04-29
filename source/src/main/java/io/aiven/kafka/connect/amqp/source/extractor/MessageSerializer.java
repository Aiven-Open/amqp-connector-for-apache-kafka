/*
        Copyright 2026 Aiven Oy and project contributors

       Licensed under the Apache License, Version 2.0 (the "License");
       you may not use this file except in compliance with the License.
       You may obtain a copy of the License at

       https://www.apache.org/licenses/LICENSE-2.0

       Unless required by applicable law or agreed to in writing,
       software distributed under the License is distributed on an
       "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
       KIND, either express or implied.  See the License for the
       specific language governing permissions and limitations
       under the License.

       SPDX-License-Identifier: Apache-2.0
*/
package io.aiven.kafka.connect.amqp.source.extractor;

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

/** Serializes an AMQP Message into a JSON format. */
public final class MessageSerializer extends StdSerializer<Message> {

  /** Default constructor. */
  public MessageSerializer() {
    this(null);
  }

  /**
   * Constructor for an arbitrary Message.
   *
   * @param t the class of the Message to process.
   */
  public MessageSerializer(Class<Message> t) {
    super(t);
  }

  /**
   * Write the map object if the map is not empty.
   *
   * @param jgen the JSON generator.
   * @param name the name of the map.
   * @param map the map to write.
   * @throws IOException on IO error.
   */
  private void writeMap(final JsonGenerator jgen, final String name, final Map<String, Object> map)
      throws IOException {
    if (!map.isEmpty()) {
      jgen.writeObjectField(name, map);
    }
  }

  /**
   * Write the object if the object is not null.
   *
   * @param jgen the JSON generator.
   * @param name the name of the map.
   * @param object the object to write.
   * @throws IOException on IO error.
   */
  private void writeObject(final JsonGenerator jgen, final String name, final Object object)
      throws IOException {
    if (object != null) {
      jgen.writeObjectField(name, object);
    }
  }

  /**
   * Write the string if the string is not null.
   *
   * @param jgen the JSON generator.
   * @param name the name of the map.
   * @param string the string to write.
   * @throws IOException on IO error.
   */
  private void writeString(final JsonGenerator jgen, final String name, final String string)
      throws IOException {
    if (string != null) {
      jgen.writeStringField(name, string);
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
        if (body instanceof Section<?> section) {
          jgen.writeObjectField("body", section);
        } else {
          jgen.writeArrayFieldStart("body");
          for (Section<?> section : (Collection<Section<?>>) body) {
            jgen.writeObject(section);
          }
          jgen.writeEndArray();
        }
      }

      jgen.writeEndObject();
    } catch (ClientException e) {
      throw new IOException(e);
    }
  }

  /**
   * Extract the body sections of the message. An AMQP message may have 0 or more sections. If the
   * body has:
   *
   * <ul>
   *   <li>zero element, null is returned
   *   <li>one element, the single section is returned.
   *   <li>two or more elements, the collection is returned.
   * </ul>
   *
   * @param value the message to extract the sections from.
   * @return the Object representing the body or {@code null}.
   * @throws IOException on IO Error
   */
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
