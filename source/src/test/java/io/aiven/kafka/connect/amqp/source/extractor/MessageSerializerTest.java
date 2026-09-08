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

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.UUID;
import org.apache.qpid.protonj2.client.Message;
import org.apache.qpid.protonj2.client.exceptions.ClientException;
import org.apache.qpid.protonj2.client.impl.ClientMessage;
import org.apache.qpid.protonj2.types.Binary;
import org.apache.qpid.protonj2.types.UnsignedLong;
import org.apache.qpid.protonj2.types.messaging.Section;
import org.junit.jupiter.api.Test;

public class MessageSerializerTest {
  private final ObjectMapper objectMapper;

  public MessageSerializerTest() {
    objectMapper = new ObjectMapper();
    SimpleModule module = new SimpleModule();
    module.addSerializer(Message.class, new MessageSerializer());
    module.addSerializer(Section.class, new AmqpSectionSerializer());
    objectMapper.registerModule(module);
  }

  @Test
  void byteArrayBody() throws ClientException, JsonProcessingException {
    Message<byte[]> message = ClientMessage.create();
    message.body("Hello world".getBytes(StandardCharsets.UTF_8));
    String expected =
        ",\"body\":"
            + objectMapper.writeValueAsString("Hello world".getBytes(StandardCharsets.UTF_8));
    String actual = objectMapper.writeValueAsString(message);
    assertThat(actual).contains(expected);
  }

  @Test
  void stringBody() throws ClientException, JsonProcessingException {
    Message<String> message = ClientMessage.create();
    message.body("Hello world");
    String expected = ",\"body\":" + objectMapper.writeValueAsString("Hello world");
    String actual = objectMapper.writeValueAsString(message);
    assertThat(actual).contains(expected);
  }

  @Test
  void listBody() throws ClientException, JsonProcessingException {
    List<String> lst = List.of("Hello", "World");
    Message<List<String>> message = ClientMessage.create();
    message.body(lst);
    String expected = ",\"body\":" + objectMapper.writeValueAsString(lst);
    String actual = objectMapper.writeValueAsString(message);
    assertThat(actual).contains(expected);
  }

  @Test
  void jsonBody() throws ClientException, JsonProcessingException {
    ObjectNode node = objectMapper.createObjectNode();
    node.put("Hello", "hola")
        .put("World", "la monde")
        .set("inner", objectMapper.createObjectNode().put("one", "uno").put("two", "dos"));
    Message<ObjectNode> message = ClientMessage.create();
    message.body(node);
    String expected = ",\"body\":" + objectMapper.writeValueAsString(node);
    String actual = objectMapper.writeValueAsString(message);
    assertThat(actual).contains(expected);
  }

  @Test
  void MessageId() throws ClientException, JsonProcessingException {
    final String uuidValue = "917f8d9-9c2e-4533-bf22-cd5be296ab1c";
    final UnsignedLong unsignedLong = new UnsignedLong(5L);
    final Binary binary = new Binary("Hello World".getBytes(StandardCharsets.UTF_8));
    /*
    if (messageId == null ||
           messageId instanceof String ||
           messageId instanceof UUID ||
           messageId instanceof UnsignedLong ||
           messageId instanceof Binary) {

           // Allowed types of message.
           return;
       }
    */
    Message<ObjectNode> message = ClientMessage.create();
    message.messageId("A String");
    String actual = objectMapper.writeValueAsString(message);
    assertThat(actual).contains("A String");

    message.messageId(UUID.fromString(uuidValue));
    actual = objectMapper.writeValueAsString(message);
    assertThat(actual).contains(uuidValue);

    message.messageId(unsignedLong);
    actual = objectMapper.writeValueAsString(message);
    assertThat(actual).contains("5");

    message.messageId(binary);
    actual = objectMapper.writeValueAsString(message);
    assertThat(actual).contains("5");
  }

  //  @Test
  //  void schemaGenerator() {
  //
  //    Schema messageIdSchema = new SchemaBuilder(Schema.Type.STRUCT)
  //            .name("encodedData").optional()
  //            .field("encoding", Schema.OPTIONAL_STRING_SCHEMA)
  //            .field("data", Schema.STRING_SCHEMA);
  //
  //
  //
  //    SchemaBuilder builder = new SchemaBuilder(Schema.Type.STRUCT);
  //    builder.field("messageId", messageIdSchema)
  //            .field("userId", Schema.OPTIONAL_STRING_SCHEMA)
  //            .field("subject", Schema.OPTIONAL_STRING_SCHEMA)
  //            .field("replyTo", Schema.OPTIONAL_STRING_SCHEMA)
  //            .field("correlationId", messageIdSchema)
  //            .field("contentType", Schema.OPTIONAL_STRING_SCHEMA)
  //            .field("contentEncoding", Schema.OPTIONAL_STRING_SCHEMA)
  //            .field("absoluteExpiry", Schema.INT64_SCHEMA)
  //            .field("creationTime", Schema.INT64_SCHEMA)
  //            .field("groupId", Schema.OPTIONAL_STRING_SCHEMA)
  //            .field("groupSequence", Schema.INT32_SCHEMA)
  //            .field("replyToGroup", Schema.OPTIONAL_STRING_SCHEMA)
  //            .field("durable", Schema.BOOLEAN_SCHEMA)
  //            .field("firstAcquirer", Schema.BOOLEAN_SCHEMA)
  //            .field("deliveryCount", Schema.INT64_SCHEMA)
  //            .field("annotations", SchemaBuilder.map(Schema.STRING_SCHEMA, ))
  //
  //            .field("properties", SchemaBuilder.map(Schema.STRING_SCHEMA, ))
  //            .field("footers", SchemaBuilder.map(Schema.STRING_SCHEMA, ))
  //            .field("body", SchemaBuilder.array() )
  //
  //
  //    TreeMap<String, Object> map = new TreeMap<>();
  //    value.forEachAnnotation(map::put);
  //    writeMap(jgen, "annotations", map);
  //
  //    map.clear();
  //    value.forEachProperty(map::put);
  //    writeMap(jgen, "properties", map);
  //
  //    map.clear();
  //    value.forEachFooter(map::put);
  //    writeMap(jgen, "footers", map);
  //
  //    Object body = extractBody(value);
  //    if (body != null) {
  //      if (body instanceof Section<?> section) {
  //        jgen.writeObjectField("body", section);
  //      } else {
  //        jgen.writeArrayFieldStart("body");
  //        for (Section<?> section : (Collection<Section<?>>) body) {
  //          jgen.writeObject(section);
  //        }
  //        jgen.writeEndArray();
  //      }
  //    }
  //  }
}
