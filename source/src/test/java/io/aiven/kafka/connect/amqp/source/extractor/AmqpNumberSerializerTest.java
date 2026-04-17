package io.aiven.kafka.connect.amqp.source.extractor;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.math.BigDecimal;
import org.apache.qpid.protonj2.types.Decimal128;
import org.apache.qpid.protonj2.types.Decimal32;
import org.apache.qpid.protonj2.types.Decimal64;
import org.apache.qpid.protonj2.types.UnsignedByte;
import org.apache.qpid.protonj2.types.UnsignedInteger;
import org.apache.qpid.protonj2.types.UnsignedLong;
import org.apache.qpid.protonj2.types.UnsignedShort;
import org.junit.jupiter.api.Test;

public class AmqpNumberSerializerTest {

  private final ObjectMapper objectMapper;
  private static final BigDecimal MAX_FLOAT = BigDecimal.valueOf(Float.MAX_VALUE);
  private static final BigDecimal BIGGER_FLOAT = MAX_FLOAT.add(BigDecimal.ONE);
  private static final BigDecimal MAX_DOUBLE = BigDecimal.valueOf(Double.MAX_VALUE);
  private static final BigDecimal BIGGER_DOUBLE = MAX_DOUBLE.add(BigDecimal.ONE);

  public AmqpNumberSerializerTest() {
    objectMapper = AmqpExtractor.registerSerializers(new ObjectMapper());
  }

  @Test
  void decimal32Test() throws JsonProcessingException {
    Decimal32 decimal = new Decimal32(BigDecimal.TEN);
    String actual = objectMapper.writeValueAsString(decimal);
    assertThat(actual).isEqualTo("10.0");

    decimal = new Decimal32(MAX_FLOAT);
    actual = objectMapper.writeValueAsString(decimal);
    assertThat(actual).isEqualTo("3.4028235E38");

    decimal = new Decimal32(BIGGER_FLOAT);
    actual = objectMapper.writeValueAsString(decimal);
    System.out.println(BIGGER_FLOAT.toString());
    assertThat(actual).isEqualTo("3.4028235E38");

    decimal = new Decimal32(MAX_DOUBLE);
    actual = objectMapper.writeValueAsString(decimal);
    assertThat(actual).isEqualTo("\"Infinity\"");

    decimal = new Decimal32(BIGGER_DOUBLE);
    actual = objectMapper.writeValueAsString(decimal);
    assertThat(actual).isEqualTo("\"Infinity\"");


  }

  @Test
  void decimal64Test() throws JsonProcessingException {
    Decimal64 decimal = new Decimal64(BigDecimal.TEN);
    String actual = objectMapper.writeValueAsString(decimal);
    assertThat(actual).isEqualTo("10.0");

    decimal = new Decimal64(MAX_FLOAT);
    actual = objectMapper.writeValueAsString(decimal);
    assertThat(actual).isEqualTo("3.4028234663852886E38");

    decimal = new Decimal64(BIGGER_FLOAT);
    actual = objectMapper.writeValueAsString(decimal);
    System.out.println(BIGGER_FLOAT.toString());
    assertThat(actual).isEqualTo("3.4028234663852886E38");

    decimal = new Decimal64(MAX_DOUBLE);
    actual = objectMapper.writeValueAsString(decimal);
    assertThat(actual).isEqualTo("1.7976931348623157E308");

    decimal = new Decimal64(BIGGER_DOUBLE);
    actual = objectMapper.writeValueAsString(decimal);
    assertThat(actual).isEqualTo("1.7976931348623157E308");
  }

  @Test
  void decimal128Test() throws JsonProcessingException {
    Decimal128 decimal = new Decimal128(BigDecimal.TEN);
    String actual = objectMapper.writeValueAsString(decimal);
    assertThat(actual).isEqualTo("10.0");

    decimal = new Decimal128(MAX_FLOAT);
    actual = objectMapper.writeValueAsString(decimal);
    assertThat(actual).isEqualTo("3.4028234663852886E38");

    decimal = new Decimal128(BIGGER_FLOAT);
    actual = objectMapper.writeValueAsString(decimal);
    System.out.println(BIGGER_FLOAT.toString());
    assertThat(actual).isEqualTo("3.4028234663852886E38");

    decimal = new Decimal128(MAX_DOUBLE);
    actual = objectMapper.writeValueAsString(decimal);
    assertThat(actual).isEqualTo("1.7976931348623157E308");

    decimal = new Decimal128(BIGGER_DOUBLE);
    actual = objectMapper.writeValueAsString(decimal);
    assertThat(actual).isEqualTo("1.7976931348623157E308");
  }

  @Test
  void unsignedByteTest() throws JsonProcessingException {
    UnsignedByte unsigned = new UnsignedByte((byte) 10);
    String actual = objectMapper.writeValueAsString(unsigned);
    assertThat(actual).isEqualTo("10");

    unsigned = new UnsignedByte((byte) 0xFF);
    actual = objectMapper.writeValueAsString(unsigned);
    assertThat(actual).isEqualTo("255");

    unsigned = new UnsignedByte((byte) -1);
    actual = objectMapper.writeValueAsString(unsigned);
    assertThat(actual).isEqualTo("255");
  }

  @Test
  void unsignedIntegerTest() throws JsonProcessingException {
    UnsignedInteger unsinged = new UnsignedInteger(10);
    String actual = objectMapper.writeValueAsString(unsinged);
    assertThat(actual).isEqualTo("10");

    unsinged = new UnsignedInteger(Integer.MAX_VALUE);
    actual = objectMapper.writeValueAsString(unsinged);
    assertThat(actual).isEqualTo("2147483647");

     unsinged = new UnsignedInteger(-1);
     actual = objectMapper.writeValueAsString(unsinged);
    assertThat(actual).isEqualTo("4294967295");
  }

  @Test
  void unsignedLongTest() throws JsonProcessingException {
    UnsignedLong unsinged = new UnsignedLong(10);
    String actual = objectMapper.writeValueAsString(unsinged);
    assertThat(actual).isEqualTo("10");

    unsinged = new UnsignedLong(Long.MAX_VALUE);
    actual = objectMapper.writeValueAsString(unsinged);
    assertThat(actual).isEqualTo("9223372036854775807");

     unsinged = new UnsignedLong(-1L);
     actual = objectMapper.writeValueAsString(unsinged);
    assertThat(actual).isEqualTo("18446744073709551615");
  }

  @Test
  void unsignedShortTest() throws JsonProcessingException {
    UnsignedShort unsinged = new UnsignedShort((short)10);
    String actual = objectMapper.writeValueAsString(unsinged);
    assertThat(actual).isEqualTo("10");

    unsinged = new UnsignedShort(Short.MAX_VALUE);
    actual = objectMapper.writeValueAsString(unsinged);
    assertThat(actual).isEqualTo("32767");

    unsinged = new UnsignedShort((short) -1);
    actual = objectMapper.writeValueAsString(unsinged);
    assertThat(actual).isEqualTo("65535");
  }
}
