package io.aiven.kafka.connect.amqp.source.extractor;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import org.apache.qpid.protonj2.types.Symbol;
import org.junit.jupiter.api.Test;

public class AmqpSymbolSerializerTest {

  private final ObjectMapper objectMapper;

  public AmqpSymbolSerializerTest() {
    objectMapper = new ObjectMapper();
    SimpleModule module = new SimpleModule();
    module.addSerializer(Symbol.class, new AmqpSymbolSerializer());
    objectMapper.registerModule(module);
  }

  @Test
  void stringTest() throws JsonProcessingException {
    Symbol symbol = Symbol.getSymbol("Hello World");
    String actual = objectMapper.writeValueAsString(symbol);
    assertThat(actual).isEqualTo("\"Hello World\"");
  }
}
