package io.aiven.kafka.connect.amqp.common.data;

import static io.aiven.kafka.connect.amqp.common.data.KafkaConverter.BIG_DECIMAL_NAME;
import static io.aiven.kafka.connect.amqp.common.data.KafkaConverter.BIG_INTEGER_NAME;
import static org.assertj.core.api.Assertions.assertThat;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

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

  @ParameterizedTest
  @MethodSource("listTestData")
  void listTest(List<Object> expected) {
    Optional<SchemaAndValue> schemaAndValue = underTest.encode(expected);
    SchemaAndValue sv = assertThat(schemaAndValue).isPresent().get().actual();
    Collection<Object> actual = (Collection<Object>) sv.value();
    assertThat(actual).containsExactlyElementsOf(expected);
  }

  static List<List<Object>> listTestData() {
    List<List<Object>> result = new ArrayList<>();
    result.add(List.of(1, 2));
    result.add(List.of("hello", "world"));
    result.add(List.of(1L, 2L));
    result.add(List.of((byte) 1, (byte) 2));
    result.add(List.of((short) 1, (short) 2));
    return result;
  }

  @ParameterizedTest
  @MethodSource("mapTestData")
  void mapTest(Map<Object, Object> expected) {
    Optional<SchemaAndValue> schemaAndValue = underTest.encode(expected);
    SchemaAndValue sv = assertThat(schemaAndValue).isPresent().get().actual();
    Map<Object, Object> actual = (Map<Object, Object>) sv.value();
    assertThat(actual).containsExactlyEntriesOf(expected);
    // ..containsExactlyElementsOf(expected);
  }

  static List<Map<Object, Object>> mapTestData() {
    List<Map<Object, Object>> result = new ArrayList<>();
    result.add(Map.of("a", 1, "b", 2));
    result.add(Map.of("hello", "A", "world", "B"));
    result.add(Map.of("a", 1L, "b", 2L));
    result.add(Map.of("a", (short) 1, "b", (short) 2));
    result.add(Map.of("a", (byte) 1, "b", (byte) 2));
    result.add(Map.of(1, "hello", 2, "world"));
    return result;
  }

  @Test
  void arrayTest() {
    Integer[] expectedInt = new Integer[] {1, 2};
    Optional<SchemaAndValue> schemaAndValue = underTest.encode(expectedInt);
    SchemaAndValue sv = assertThat(schemaAndValue).isPresent().get().actual();
    Collection<Object> actual = (Collection<Object>) sv.value();
    assertThat(actual).containsExactly(expectedInt);
  }

  static List<Object[]> arrayTestData() {
    List<Object[]> result = new ArrayList<>();
    result.add((Object[]) new Integer[] {1, 2});
    result.add(new String[] {"hello", "world"});
    result.add(new Long[] {1L, 2L});
    result.add(new Byte[] {(byte) 1, (byte) 2});
    result.add(new Short[] {(short) 1, (short) 2});
    return result;
  }
}
