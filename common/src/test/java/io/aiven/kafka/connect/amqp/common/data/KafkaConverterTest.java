package io.aiven.kafka.connect.amqp.common.data;

import static io.aiven.kafka.connect.amqp.common.data.KafkaConverter.BIG_DECIMAL_NAME;
import static io.aiven.kafka.connect.amqp.common.data.KafkaConverter.BIG_INTEGER_NAME;
import static org.assertj.core.api.Assertions.assertThat;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.util.Optional;
import java.util.UUID;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.junit.jupiter.api.Test;

public class KafkaConverterTest {

  private final KafkaConverter underTest = new KafkaConverter();

  @Test
  void nullValueTest() {
    Optional<SchemaAndValue> encoded = underTest.encode(null);
    SchemaAndValue schemaAndValue = assertThat(encoded).isPresent().get().actual();
    assertThat(schemaAndValue.schema()).isEqualTo(Schema.OPTIONAL_BYTES_SCHEMA);
    assertThat(schemaAndValue.value()).isEqualTo(null);
  }

  @Test
  void byteTest() {
    byte expectedValue = (byte) 128;
    Optional<SchemaAndValue> encoded = underTest.encode(expectedValue);
    SchemaAndValue schemaAndValue = assertThat(encoded).isPresent().get().actual();
    assertThat(schemaAndValue.schema()).isEqualTo(Schema.INT8_SCHEMA);
    assertThat(schemaAndValue.value()).isEqualTo(expectedValue);
    assertThat(underTest.decode(schemaAndValue))
        .isPresent()
        .get()
        .isInstanceOf(Byte.class)
        .isEqualTo(expectedValue);
  }

  @Test
  void shortTest() {
    short expectedValue = 128;
    Optional<SchemaAndValue> encoded = underTest.encode(expectedValue);
    SchemaAndValue schemaAndValue = assertThat(encoded).isPresent().get().actual();
    assertThat(schemaAndValue.schema()).isEqualTo(Schema.INT16_SCHEMA);
    assertThat(schemaAndValue.value()).isEqualTo(expectedValue);
    assertThat(underTest.decode(schemaAndValue))
        .isPresent()
        .get()
        .isInstanceOf(Short.class)
        .isEqualTo(expectedValue);
  }

  @Test
  void intTest() {
    Optional<SchemaAndValue> encoded = underTest.encode(128);
    SchemaAndValue schemaAndValue = assertThat(encoded).isPresent().get().actual();
    assertThat(schemaAndValue.schema()).isEqualTo(Schema.INT32_SCHEMA);
    assertThat(schemaAndValue.value()).isEqualTo(128);
    assertThat(underTest.decode(schemaAndValue))
        .isPresent()
        .get()
        .isInstanceOf(Integer.class)
        .isEqualTo(128);
  }

  @Test
  void longTest() {
    long expectedValue = 128;
    Optional<SchemaAndValue> encoded = underTest.encode(expectedValue);
    SchemaAndValue schemaAndValue = assertThat(encoded).isPresent().get().actual();
    assertThat(schemaAndValue.schema()).isEqualTo(Schema.INT64_SCHEMA);
    assertThat(schemaAndValue.value()).isEqualTo(expectedValue);
    assertThat(underTest.decode(schemaAndValue))
        .isPresent()
        .get()
        .isInstanceOf(Long.class)
        .isEqualTo(expectedValue);
  }

  @Test
  void floatTest() {
    float expectedValue = 12.8f;
    Optional<SchemaAndValue> encoded = underTest.encode(expectedValue);
    SchemaAndValue schemaAndValue = assertThat(encoded).isPresent().get().actual();
    assertThat(schemaAndValue.schema()).isEqualTo(Schema.FLOAT32_SCHEMA);
    assertThat(schemaAndValue.value()).isEqualTo(expectedValue);
    assertThat(underTest.decode(schemaAndValue))
        .isPresent()
        .get()
        .isInstanceOf(Float.class)
        .isEqualTo(expectedValue);
  }

  @Test
  void doubleTest() {
    Optional<SchemaAndValue> encoded = underTest.encode(12.8);
    SchemaAndValue schemaAndValue = assertThat(encoded).isPresent().get().actual();
    assertThat(schemaAndValue.schema()).isEqualTo(Schema.FLOAT64_SCHEMA);
    assertThat(schemaAndValue.value()).isEqualTo(12.8);
    assertThat(underTest.decode(schemaAndValue))
        .isPresent()
        .get()
        .isInstanceOf(Double.class)
        .isEqualTo(12.8);
  }

  @Test
  void bigDecimalTest() {
    BigDecimal expectedValue = BigDecimal.TEN;
    Optional<SchemaAndValue> encoded = underTest.encode(expectedValue);
    SchemaAndValue schemaAndValue = assertThat(encoded).isPresent().get().actual();
    assertThat(schemaAndValue.schema().name()).isEqualTo(BIG_DECIMAL_NAME);
    assertThat(schemaAndValue.schema().type()).isEqualTo(Schema.Type.STRING);
    assertThat(schemaAndValue.value()).isEqualTo(expectedValue.toString());
    assertThat(underTest.decode(schemaAndValue))
        .isPresent()
        .get()
        .isInstanceOf(BigDecimal.class)
        .isEqualTo(expectedValue);
  }

  @Test
  void bigIntegerTest() {
    BigInteger expectedValue = BigInteger.TEN;
    Optional<SchemaAndValue> encoded = underTest.encode(expectedValue);
    SchemaAndValue schemaAndValue = assertThat(encoded).isPresent().get().actual();
    assertThat(schemaAndValue.schema().name()).isEqualTo(BIG_INTEGER_NAME);
    assertThat(schemaAndValue.schema().type()).isEqualTo(Schema.Type.STRING);
    assertThat(schemaAndValue.value()).isEqualTo(expectedValue.toString());
    assertThat(underTest.decode(schemaAndValue))
        .isPresent()
        .get()
        .isInstanceOf(BigInteger.class)
        .isEqualTo(expectedValue);
  }

  @Test
  void stringTest() {
    String expectedValue = "Hello world";
    Optional<SchemaAndValue> encoded = underTest.encode(expectedValue);
    SchemaAndValue schemaAndValue = assertThat(encoded).isPresent().get().actual();
    assertThat(schemaAndValue.schema()).isEqualTo(Schema.STRING_SCHEMA);
    assertThat(schemaAndValue.value()).isEqualTo(expectedValue);
    assertThat(underTest.decode(schemaAndValue))
        .isPresent()
        .get()
        .isInstanceOf(String.class)
        .isEqualTo(expectedValue);
  }

  @Test
  void booleanTest() {
    boolean expectedValue = true;
    Optional<SchemaAndValue> encoded = underTest.encode(expectedValue);
    SchemaAndValue schemaAndValue = assertThat(encoded).isPresent().get().actual();
    assertThat(schemaAndValue.schema()).isEqualTo(Schema.BOOLEAN_SCHEMA);
    assertThat(schemaAndValue.value()).isEqualTo(expectedValue);
    assertThat(underTest.decode(schemaAndValue))
        .isPresent()
        .get()
        .isInstanceOf(Boolean.class)
        .isEqualTo(expectedValue);
  }

  @Test
  void bytesTest() {
    byte[] expectedValue = "This is the way".getBytes(StandardCharsets.UTF_8);
    Optional<SchemaAndValue> encoded = underTest.encode(expectedValue);
    SchemaAndValue schemaAndValue = assertThat(encoded).isPresent().get().actual();
    assertThat(schemaAndValue.schema()).isEqualTo(Schema.BYTES_SCHEMA);
    assertThat(schemaAndValue.value()).isEqualTo(expectedValue);
    assertThat(underTest.decode(schemaAndValue))
        .isPresent()
        .get()
        .isInstanceOf(byte[].class)
        .isEqualTo(expectedValue);
  }

  @Test
  void notKafkaTypeTest() {
    UUID expectedValue = UUID.randomUUID();
    Schema expectedSchema =
        new SchemaBuilder(Schema.Type.STRING).name(UUID.class.getCanonicalName()).build();
    assertThat(underTest.encode(expectedValue)).isNotPresent();
    assertThat(underTest.decode(new SchemaAndValue(expectedSchema, expectedValue.toString())))
        .isNotPresent();
  }
}
