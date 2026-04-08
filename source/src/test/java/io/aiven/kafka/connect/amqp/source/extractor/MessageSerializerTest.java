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
import org.apache.qpid.protonj2.client.Message;
import org.apache.qpid.protonj2.client.exceptions.ClientException;
import org.apache.qpid.protonj2.client.impl.ClientMessage;
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
}
