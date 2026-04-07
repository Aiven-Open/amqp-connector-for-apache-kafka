package io.aiven.kafka.connect.amqp.source.transformer;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import java.nio.charset.StandardCharsets;
import java.util.List;
import org.apache.qpid.protonj2.types.messaging.AmqpSequence;
import org.apache.qpid.protonj2.types.messaging.AmqpValue;
import org.apache.qpid.protonj2.types.messaging.Data;
import org.apache.qpid.protonj2.types.messaging.Section;
import org.junit.jupiter.api.Test;

public class AmqpSectionSerializerTest {
  private final ObjectMapper objectMapper;

  public AmqpSectionSerializerTest() {
    objectMapper = new ObjectMapper();
    SimpleModule module = new SimpleModule();
    module.addSerializer(Section.class, new AmqpSectionSerializer());
    objectMapper.registerModule(module);
  }

  @Test
  void testData() throws JsonProcessingException {
    Data data = new Data("Hello World".getBytes(StandardCharsets.UTF_8));
    String expected =
        objectMapper.writeValueAsString("Hello World".getBytes(StandardCharsets.UTF_8));
    String actual = objectMapper.writeValueAsString(data);
    assertThat(actual).isEqualTo(expected);
  }

  @Test
  void testAmpqSequence() throws JsonProcessingException {
    List<String> lst = List.of("Hello", "World");
    AmqpSequence<String> seq = new AmqpSequence<>(lst);
    String expected = objectMapper.writeValueAsString(lst);
    String actual = objectMapper.writeValueAsString(seq);
    assertThat(actual).isEqualTo(expected);
  }

  @Test
  void testAmqpValue() throws JsonProcessingException {
    AmqpValue<String> value = new AmqpValue<>("Hello World");
    String expected = objectMapper.writeValueAsString("Hello World");
    String actual = objectMapper.writeValueAsString(value);
    assertThat(actual).isEqualTo(expected);
  }
}
