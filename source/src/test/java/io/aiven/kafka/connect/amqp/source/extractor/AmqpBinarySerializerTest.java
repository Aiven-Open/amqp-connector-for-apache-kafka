package io.aiven.kafka.connect.amqp.source.extractor;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import java.nio.charset.StandardCharsets;
import org.apache.qpid.protonj2.types.Binary;
import org.junit.jupiter.api.Test;

public class AmqpBinarySerializerTest {

  private final ObjectMapper objectMapper;

  public AmqpBinarySerializerTest() {
    objectMapper = new ObjectMapper();
    SimpleModule module = new SimpleModule();
    module.addSerializer(Binary.class, new AmqpBinarySerializer());
    objectMapper.registerModule(module);
  }

  @Test
  void nullTest() throws JsonProcessingException {
    Binary binary = new Binary();
    String actual = objectMapper.writeValueAsString(binary);
    assertThat(actual).isEqualTo("null");
  }

  @Test
  void bytesTest() throws JsonProcessingException {
    Binary binary = new Binary("Hello".getBytes(StandardCharsets.UTF_8));
    String actual = objectMapper.writeValueAsString(binary);
    assertThat(actual).isEqualTo("SGVsbG8=");
  }
}
