package io.aiven.kafka.connect.amqp.source.transformer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.aiven.commons.io.compression.CompressionType;
import io.aiven.commons.kafka.connector.common.config.ConnectorCommonConfigFragment;
import io.aiven.commons.kafka.connector.source.EvolvingSourceRecord;
import io.aiven.commons.kafka.connector.source.config.SourceCommonConfig;
import io.aiven.kafka.connect.amqp.common.config.AmqpFragment;
import io.aiven.kafka.connect.amqp.source.AmqpSourceNativeInfo;
import io.aiven.kafka.connect.amqp.source.config.AmqpSourceConfig;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.qpid.protonj2.client.Message;
import org.apache.qpid.protonj2.client.exceptions.ClientException;
import org.apache.qpid.protonj2.client.impl.ClientMessage;
import org.apache.qpid.protonj2.types.messaging.Section;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class AmqpExtractorTest {
  private final ObjectMapper objectMapper;
  private AmqpExtractor underTest;
  private AmqpSourceNativeInfo sourceNativeInfo;

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
}
