package io.aiven.kafka.connect.amqp.source.extractor;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import java.math.BigDecimal;
import org.apache.qpid.protonj2.types.Decimal128;
import org.apache.qpid.protonj2.types.Decimal32;
import org.apache.qpid.protonj2.types.Decimal64;
import org.apache.qpid.protonj2.types.UnsignedByte;
import org.apache.qpid.protonj2.types.UnsignedInteger;
import org.junit.jupiter.api.Test;

public class AmqpNumberlSerializerTest {

  private final ObjectMapper objectMapper;

  /*
  Decimal32
  Decimal64
  Decimal128
  UnsignedByte
  unsignedInteger
  UnsignedLong
  UnsignedShort
   */
  public AmqpNumberlSerializerTest() {
    objectMapper = new ObjectMapper();
    SimpleModule module = new SimpleModule();
    objectMapper.registerModule(module);
  }

  @Test
  void decimal32Test() throws JsonProcessingException {
    Decimal32 decimal = new Decimal32(BigDecimal.TEN);
    String actual = objectMapper.writeValueAsString(decimal);
    assertThat(actual).isEqualTo("10");
  }

  @Test
  void decimal64Test() throws JsonProcessingException {
    Decimal64 decimal = new Decimal64(BigDecimal.TEN);
    String actual = objectMapper.writeValueAsString(decimal);
    assertThat(actual).isEqualTo("10");
  }

  @Test
  void decimal128Test() throws JsonProcessingException {
    Decimal128 decimal = new Decimal128(BigDecimal.TEN);
    String actual = objectMapper.writeValueAsString(decimal);
    assertThat(actual).isEqualTo("10");
  }

  @Test
  void unsignedByteTest() throws JsonProcessingException {
    UnsignedByte unsigned = new UnsignedByte((byte) 10);
    String actual = objectMapper.writeValueAsString(unsigned);
    assertThat(actual).isEqualTo("10");
  }

  @Test
  void unsignedIntegerTest() throws JsonProcessingException {
    UnsignedInteger unsinged = new UnsignedInteger(10);
    String actual = objectMapper.writeValueAsString(unsinged);
    assertThat(actual).isEqualTo("10");
  }
}
