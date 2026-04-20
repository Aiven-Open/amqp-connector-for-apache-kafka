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
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.aiven.commons.kafka.connector.common.config.ConnectorCommonConfigFragment;
import io.aiven.commons.kafka.connector.source.EvolvingSourceRecord;
import io.aiven.commons.kafka.connector.source.config.SourceCommonConfig;
import io.aiven.commons.util.io.compression.CompressionType;
import io.aiven.kafka.connect.amqp.common.config.AmqpFragment;
import io.aiven.kafka.connect.amqp.source.AmqpSourceNativeInfo;
import io.aiven.kafka.connect.amqp.source.config.AmqpSourceConfig;
import java.io.IOException;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.qpid.protonj2.client.AdvancedMessage;
import org.apache.qpid.protonj2.client.Message;
import org.apache.qpid.protonj2.client.exceptions.ClientException;
import org.apache.qpid.protonj2.client.impl.ClientMessage;
import org.apache.qpid.protonj2.client.impl.ClientMessageSupport;
import org.apache.qpid.protonj2.types.Binary;
import org.apache.qpid.protonj2.types.Symbol;
import org.apache.qpid.protonj2.types.UnsignedLong;
import org.apache.qpid.protonj2.types.messaging.Section;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class AmqpExtractorTest {
  private final ObjectMapper objectMapper;
  private AmqpExtractor underTest;
  private AmqpSourceNativeInfo sourceNativeInfo;
  private Map<String, String> encodings =
      Map.of("Hello", "SGVsbG8=", "World", "V29ybGQ=", "Man bites dog", "TWFuIGJpdGVzIGRvZw==");

  private static final Map<String, String> CONFIG =
      AmqpFragment.setter(new HashMap<String, String>())
          .setHost("localhost")
          .setAddress("address")
          .setUser("user")
          .setPassword("password")
          .data();

  public AmqpExtractorTest() {
    objectMapper = new ObjectMapper();
    SimpleModule module = new SimpleModule();
    module.addSerializer(Message.class, new MessageSerializer());
    module.addSerializer(Section.class, new AmqpSectionSerializer());
    objectMapper.registerModule(module);

    SourceCommonConfig sourceCommonConfig = new AmqpSourceConfig(CONFIG);

    underTest = new AmqpExtractor(sourceCommonConfig);
  }

  @BeforeEach
  public void setup() {
    sourceNativeInfo = mock(AmqpSourceNativeInfo.class);
  }

  private List<SchemaAndValue> generateRecords(Message<?> message) throws ClientException {
    when(sourceNativeInfo.getMessage()).thenReturn((Message<Object>) message);
    EvolvingSourceRecord sourceRecord = mock(EvolvingSourceRecord.class);
    when(sourceRecord.getSourceNativeInfo()).thenReturn(sourceNativeInfo);
    return underTest.generateRecords(sourceRecord).toList();
  }

  @Test
  void byteArrayBody() throws ClientException, JsonProcessingException {
    Message<Object> message = ClientMessage.create();
    message.body("Hello world".getBytes(StandardCharsets.UTF_8));
    String expected =
        ", body="
            + objectMapper
                .writeValueAsString("Hello world".getBytes(StandardCharsets.UTF_8))
                .substring(1);
    expected = expected.substring(0, expected.length() - 1); // cut off the trailing quote.
    List<SchemaAndValue> actual = generateRecords(message);
    assertThat(actual).hasSize(1);
    SchemaAndValue schemaAndValue = actual.get(0);
    assertThat(schemaAndValue.value().toString()).contains(expected);
  }

  @Test
  void stringBody() throws ClientException, JsonProcessingException {
    Message<String> message = ClientMessage.create();
    message.body("Hello world");
    String expected = ", body=Hello world";
    List<SchemaAndValue> actual = generateRecords(message);
    assertThat(actual).hasSize(1);
    SchemaAndValue schemaAndValue = actual.get(0);
    assertThat(schemaAndValue.value().toString()).contains(expected);
  }

  @Test
  void listBody() throws ClientException, JsonProcessingException {
    List<String> lst = List.of("Hello", "World");
    Message<List<String>> message = ClientMessage.create();
    message.body(lst);
    String expected = ", body=[Hello, World]";
    List<SchemaAndValue> actual = generateRecords(message);
    assertThat(actual).hasSize(1);
    SchemaAndValue schemaAndValue = actual.get(0);
    assertThat(schemaAndValue.value().toString()).contains(expected);
  }

  @Test
  void jsonBody() throws ClientException, JsonProcessingException {
    ObjectNode node = objectMapper.createObjectNode();
    node.put("Hello", "hola")
        .put("World", "la monde")
        .set("inner", objectMapper.createObjectNode().put("one", "uno").put("two", "dos"));
    Message<ObjectNode> message = ClientMessage.create();
    message.body(node);
    String expected = ", body={Hello=hola, World=la monde, inner={one=uno, two=dos}}";
    List<SchemaAndValue> actual = generateRecords(message);
    assertThat(actual).hasSize(1);
    SchemaAndValue schemaAndValue = actual.get(0);
    assertThat(schemaAndValue.value().toString()).contains(expected);
  }

  @Test
  void compressedBody() throws ClientException, IOException {
    when(sourceNativeInfo.getInputStream()).thenCallRealMethod();
    Map<String, String> compressedConfig = new HashMap<>(CONFIG);
    ConnectorCommonConfigFragment.setter(compressedConfig).compressionType(CompressionType.GZIP);
    SourceCommonConfig sourceCommonConfig = new AmqpSourceConfig(compressedConfig);
    underTest = new AmqpExtractor(sourceCommonConfig);
    byte[] compressedValue =
        CompressionType.GZIP.compress("Hello world".getBytes(StandardCharsets.UTF_8));
    Message<byte[]> message = ClientMessage.create();
    message.body(compressedValue);

    String expected =
        ", body="
            + objectMapper
                .writeValueAsString("Hello world".getBytes(StandardCharsets.UTF_8))
                .substring(1);
    expected = expected.substring(0, expected.length() - 1);
    List<SchemaAndValue> actual = generateRecords(message);
    assertThat(actual).hasSize(1);
    SchemaAndValue schemaAndValue = actual.get(0);
    assertThat(schemaAndValue.value().toString()).contains(expected);
  }

  @Test
  void multiSectionDataBodyTest() throws ClientException {
    AdvancedMessage<byte[]> message =
        ClientMessage.create(
            ClientMessageSupport.createSectionFromValue("Hello".getBytes(StandardCharsets.UTF_8)));
    message.addBodySection(
        ClientMessageSupport.createSectionFromValue("World".getBytes(StandardCharsets.UTF_8)));
    List<SchemaAndValue> actual = generateRecords(message);
    assertThat(actual).hasSize(1);
    SchemaAndValue schemaAndValue = actual.get(0);
    assertThat(schemaAndValue.value().toString()).contains(encodings.get("Hello"));
    assertThat(schemaAndValue.value().toString()).contains(encodings.get("World"));
  }

  @Test
  void singleSectionDataBodyTest() throws ClientException {
    AdvancedMessage<byte[]> message =
        ClientMessage.create(
            ClientMessageSupport.createSectionFromValue("Hello".getBytes(StandardCharsets.UTF_8)));
    List<SchemaAndValue> actual = generateRecords(message);
    assertThat(actual).hasSize(1);
    SchemaAndValue schemaAndValue = actual.get(0);
    assertThat(schemaAndValue.value().toString()).contains(encodings.get("Hello"));
    assertThat(schemaAndValue.value().toString()).doesNotContain(encodings.get("World"));
  }

  @Test
  void multiSectionSequenceBodyTest() throws ClientException {
    List<String> lst = List.of("Hello", "World");
    List<String> lst2 = List.of("Hola", "Mundo");
    AdvancedMessage<List<String>> message =
        ClientMessage.create(ClientMessageSupport.createSectionFromValue(lst));
    message.addBodySection(ClientMessageSupport.createSectionFromValue(lst2));
    List<SchemaAndValue> actual = generateRecords(message);
    assertThat(actual).hasSize(1);
    SchemaAndValue schemaAndValue = actual.get(0);
    assertThat(schemaAndValue.value().toString()).contains("body=[[Hello, World], [Hola, Mundo]]");
  }

  @Test
  void singleSectionSequenceBodyTest() throws ClientException {
    List<String> lst = List.of("Hello", "World");
    AdvancedMessage<List<String>> message =
        ClientMessage.create(ClientMessageSupport.createSectionFromValue(lst));
    List<SchemaAndValue> actual = generateRecords(message);
    assertThat(actual).hasSize(1);
    SchemaAndValue schemaAndValue = actual.get(0);
    assertThat(schemaAndValue.value().toString()).contains("body=[Hello, World]");
  }

  @Test
  void messageIdTest() throws ClientException {
    AdvancedMessage<List<String>> message = ClientMessage.create();

    message.messageId("Man bites dog");
    List<SchemaAndValue> actual = generateRecords(message);
    assertThat(actual.get(0).value().toString().contains("messageId=Man bites dog,"));

    message.messageId(UUID.nameUUIDFromBytes("Man bites dog".getBytes(StandardCharsets.UTF_8)));
    actual = generateRecords(message);
    assertThat(
        actual
            .get(0)
            .value()
            .toString()
            .contains("messageId=612dfaeb-9570-3aee-bf60-ff1b3437e0eb,"));

    message.messageId(new UnsignedLong(5L));
    actual = generateRecords(message);
    assertThat(actual.get(0).value().toString().contains("messageId=5,"));

    message.messageId(new Binary("Hello".getBytes(StandardCharsets.UTF_8)));
    actual = generateRecords(message);

    assertThat(actual.get(0).value().toString()).contains("messageId=" + encodings.get("Hello"));
  }

  @Test
  void subjectTest() throws ClientException {
    AdvancedMessage<List<String>> message = ClientMessage.create();

    message.subject("The subject");
    List<SchemaAndValue> actual = generateRecords(message);
    assertThat(actual.get(0).value().toString().contains("subject=The subject,"));
  }

  @Test
  void replyToTest() throws ClientException {
    AdvancedMessage<List<String>> message = ClientMessage.create();

    message.replyTo("reply to me");
    List<SchemaAndValue> actual = generateRecords(message);
    assertThat(actual.get(0).value().toString().contains("replyTo=reply to me,"));
  }

  @Test
  void correlationId() throws ClientException {
    AdvancedMessage<List<String>> message = ClientMessage.create();

    message.correlationId("Man bites dog");
    List<SchemaAndValue> actual = generateRecords(message);
    assertThat(actual.get(0).value().toString().contains("correlationId=Man bites dog,"));

    message.correlationId(UUID.nameUUIDFromBytes("Man bites dog".getBytes(StandardCharsets.UTF_8)));
    actual = generateRecords(message);
    assertThat(
        actual
            .get(0)
            .value()
            .toString()
            .contains("corrolationId=612dfaeb-9570-3aee-bf60-ff1b3437e0eb,"));

    message.correlationId(new UnsignedLong(5L));
    actual = generateRecords(message);
    assertThat(actual.get(0).value().toString().contains("correlationId=5,"));

    message.correlationId(new Binary("Hello".getBytes(StandardCharsets.UTF_8)));
    actual = generateRecords(message);
    assertThat(actual.get(0).value().toString())
        .contains("correlationId=" + encodings.get("Hello"));
  }

  @Test
  void contentTypeTest() throws ClientException {
    AdvancedMessage<List<String>> message = ClientMessage.create();

    message.contentType("myType");
    List<SchemaAndValue> actual = generateRecords(message);
    assertThat(actual.get(0).value().toString().contains("contentType=myType,"));
  }

  @Test
  void contentEncodingTest() throws ClientException {
    AdvancedMessage<List<String>> message = ClientMessage.create();

    message.contentEncoding("my encoding");
    List<SchemaAndValue> actual = generateRecords(message);
    assertThat(actual.get(0).value().toString().contains("contentEncoding=my encoding,"));
  }

  @Test
  void absoluteExpiryTest() throws ClientException {
    AdvancedMessage<List<String>> message = ClientMessage.create();

    message.absoluteExpiryTime(10L);
    List<SchemaAndValue> actual = generateRecords(message);
    assertThat(actual.get(0).value().toString().contains("absoluteExpiry=10,"));
  }

  @Test
  void creationTimeTest() throws ClientException {
    AdvancedMessage<List<String>> message = ClientMessage.create();

    message.creationTime(10L);
    List<SchemaAndValue> actual = generateRecords(message);
    assertThat(actual.get(0).value().toString().contains("creationTime=10,"));
  }

  @Test
  void groupIdTest() throws ClientException {
    AdvancedMessage<List<String>> message = ClientMessage.create();

    message.groupId("MyGroupId");
    List<SchemaAndValue> actual = generateRecords(message);
    assertThat(actual.get(0).value().toString().contains("groupId=MyGroupId,"));
  }

  @Test
  void groupSequenceTest() throws ClientException {
    AdvancedMessage<List<String>> message = ClientMessage.create();

    message.groupSequence(5);
    List<SchemaAndValue> actual = generateRecords(message);
    assertThat(actual.get(0).value().toString().contains("groupSequence=5,"));
  }

  @Test
  void replyToGroupIdTest() throws ClientException {
    AdvancedMessage<List<String>> message = ClientMessage.create();

    message.replyToGroupId("the group to reply to");
    List<SchemaAndValue> actual = generateRecords(message);
    assertThat(actual.get(0).value().toString().contains("replyToGroupId=the group to reply to,"));
  }

  @Test
  void durableTest() throws ClientException {
    AdvancedMessage<List<String>> message = ClientMessage.create();

    message.durable(true);
    List<SchemaAndValue> actual = generateRecords(message);
    assertThat(actual.get(0).value().toString().contains("durable=true"));
  }

  @Test
  void firstAcquirerTest() throws ClientException {
    AdvancedMessage<List<String>> message = ClientMessage.create();

    message.firstAcquirer(true);
    List<SchemaAndValue> actual = generateRecords(message);
    assertThat(actual.get(0).value().toString().contains("firstAcquirer=true"));
  }

  @Test
  void deliveryCountTest() throws ClientException {
    AdvancedMessage<List<String>> message = ClientMessage.create();

    message.deliveryCount(500);
    List<SchemaAndValue> actual = generateRecords(message);
    assertThat(actual.get(0).value().toString().contains("deliveryCount=500"));
  }

  @Test
  void annotationTest() throws ClientException {
    AdvancedMessage<List<String>> message = ClientMessage.create();
    message.annotation("Hello", "Hola");
    message.annotation("World", List.of("Munto", "Domhan"));
    message.annotation("Bytes", "Man bites dog".getBytes(StandardCharsets.UTF_8));
    List<SchemaAndValue> actual = generateRecords(message);
    assertThat(actual).hasSize(1);
    SchemaAndValue schemaAndValue = actual.get(0);
    assertThat(schemaAndValue.value().toString())
        .contains("annotations={Bytes=TWFuIGJpdGVzIGRvZw==, Hello=Hola, World=[Munto, Domhan]}");
  }

  @Test
  void propertiesTest() throws ClientException {
    AdvancedMessage<List<String>> message = ClientMessage.create();
    message.property("Hello", "Hola");
    message.property("World", List.of("Munto", "Domhan"));
    message.property("Bytes", "Man bites dog".getBytes(StandardCharsets.UTF_8));
    message.property("Integer", 5);
    message.property("BigDecimal", BigDecimal.TEN);
    message.property("Symbol", Symbol.valueOf("My new symbol"));
    List<SchemaAndValue> actual = generateRecords(message);
    assertThat(actual).hasSize(1);
    SchemaAndValue schemaAndValue = actual.get(0);
    assertThat(schemaAndValue.value().toString())
        .contains(
            "properties={Integer=5, Bytes=TWFuIGJpdGVzIGRvZw==, Hello=Hola, Symbol=My new symbol, World=[Munto, Domhan], BigDecimal=10}}");
  }

  @Test
  void footerTest() throws ClientException {
    AdvancedMessage<List<String>> message = ClientMessage.create();
    message.footer("Hello", "Hola");
    message.footer("World", List.of("Munto", "Domhan"));
    message.footer("Bytes", "Man bites dog".getBytes(StandardCharsets.UTF_8));
    message.footer("Integer", 5);
    message.footer("BigDecimal", BigDecimal.TEN);
    message.footer("Symbol", Symbol.valueOf("My new symbol"));
    List<SchemaAndValue> actual = generateRecords(message);
    assertThat(actual).hasSize(1);
    SchemaAndValue schemaAndValue = actual.get(0);
    assertThat(schemaAndValue.value().toString())
        .contains(
            "footers={Integer=5, Bytes=TWFuIGJpdGVzIGRvZw==, Hello=Hola, Symbol=My new symbol, World=[Munto, Domhan], BigDecimal=10}}");
  }
}
