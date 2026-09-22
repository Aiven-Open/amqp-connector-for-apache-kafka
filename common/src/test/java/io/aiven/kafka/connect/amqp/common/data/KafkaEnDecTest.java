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
package io.aiven.kafka.connect.amqp.common.data;

import static io.aiven.kafka.connect.amqp.common.data.KafkaEnDec.BIG_DECIMAL_NAME;
import static io.aiven.kafka.connect.amqp.common.data.KafkaEnDec.BIG_INTEGER_NAME;
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

public class KafkaEnDecTest {

  private final KafkaEnDec underTest = new KafkaEnDec();

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
    // should not encode becasue it is not a Kafka recognized type.
    assertThat(underTest.encode(expectedValue)).isNotPresent();
    // should decode because the schema is a primitive Kafka type.
    assertThat(underTest.decode(new SchemaAndValue(expectedSchema, expectedValue.toString())))
        .isPresent()
        .get()
        .isEqualTo(expectedValue.toString());
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
}
