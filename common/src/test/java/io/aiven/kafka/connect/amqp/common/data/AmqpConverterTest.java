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

import static org.assertj.core.api.Assertions.assertThat;

import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.qpid.protonj2.types.Binary;
import org.apache.qpid.protonj2.types.Symbol;
import org.apache.qpid.protonj2.types.UnsignedByte;
import org.apache.qpid.protonj2.types.UnsignedInteger;
import org.apache.qpid.protonj2.types.UnsignedLong;
import org.apache.qpid.protonj2.types.UnsignedShort;
import org.apache.qpid.protonj2.types.messaging.Footer;
import org.apache.qpid.protonj2.types.messaging.MessageAnnotations;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

public class AmqpConverterTest {

  private static final BigInteger TWO_TO_THE_SIXTY_FOUR =
      new BigInteger(
          new byte[] {
            (byte) 1, (byte) 0, (byte) 0, (byte) 0, (byte) 0, (byte) 0, (byte) 0, (byte) 0, (byte) 0
          });

  private final AmqpEnDec underTest = new AmqpEnDec();

  @ParameterizedTest
  @ValueSource(shorts = {0xff, 0x7f, 0})
  void unsignedByteTest(short expectedValue) {

    Schema expectedSchema =
        new SchemaBuilder(Schema.Type.INT16).name(UnsignedByte.class.getCanonicalName()).build();

    Optional<SchemaAndValue> result = underTest.encode(new UnsignedByte((byte) expectedValue));
    SchemaAndValue sv = assertThat(result).isPresent().get().actual();
    assertThat(sv.schema()).isEqualTo(expectedSchema);
    assertThat(sv.value()).isInstanceOf(Short.class).isEqualTo(expectedValue);

    Optional<Object> object = underTest.decode(result.get());
    UnsignedByte decoded =
        (UnsignedByte)
            assertThat(object).isPresent().get().isInstanceOf(UnsignedByte.class).actual();
    assertThat(decoded.shortValue()).isEqualTo(expectedValue);
  }

  @ParameterizedTest
  @ValueSource(ints = {0xffff, 0x7fff, 0})
  void unsignedShortTest(int expectedValue) {

    Schema expectedSchema =
        new SchemaBuilder(Schema.Type.INT32).name(UnsignedShort.class.getCanonicalName()).build();

    Optional<SchemaAndValue> result = underTest.encode(new UnsignedShort((short) expectedValue));
    SchemaAndValue sv = assertThat(result).isPresent().get().actual();
    assertThat(sv.schema()).isEqualTo(expectedSchema);
    assertThat(sv.value()).isInstanceOf(Integer.class).isEqualTo(expectedValue);

    Optional<Object> object = underTest.decode(result.get());
    UnsignedShort decoded =
        (UnsignedShort)
            assertThat(object).isPresent().get().isInstanceOf(UnsignedShort.class).actual();
    assertThat(decoded.intValue()).isEqualTo(expectedValue);
  }

  @ParameterizedTest
  @ValueSource(longs = {0xffffffffL, 0x7fffffff, 0})
  void unsignedIntegerTest(long expectedValue) {

    Schema expectedSchema =
        new SchemaBuilder(Schema.Type.INT64).name(UnsignedInteger.class.getCanonicalName()).build();

    Optional<SchemaAndValue> result = underTest.encode(new UnsignedInteger((int) expectedValue));
    SchemaAndValue sv = assertThat(result).isPresent().get().actual();
    assertThat(sv.schema()).isEqualTo(expectedSchema);
    assertThat(sv.value()).isInstanceOf(Long.class).isEqualTo(expectedValue);

    Optional<Object> object = underTest.decode(result.get());
    UnsignedInteger decoded =
        (UnsignedInteger)
            assertThat(object).isPresent().get().isInstanceOf(UnsignedInteger.class).actual();
    assertThat(decoded.longValue()).isEqualTo(expectedValue);
  }

  @ParameterizedTest
  @ValueSource(longs = {0xffffffffffffffffL, 0x7fffffffffffffffL, 0})
  void unsignedLongTest(long underlying) {
    BigInteger expectedValue =
        underlying >= 0L
            ? BigInteger.valueOf(underlying)
            : TWO_TO_THE_SIXTY_FOUR.add(BigInteger.valueOf(underlying));
    Schema expectedSchema =
        new SchemaBuilder(Schema.Type.STRING).name(UnsignedLong.class.getCanonicalName()).build();

    Optional<SchemaAndValue> result = underTest.encode(new UnsignedLong(underlying));
    SchemaAndValue sv = assertThat(result).isPresent().get().actual();
    assertThat(sv.schema()).isEqualTo(expectedSchema);
    assertThat(sv.value()).isInstanceOf(String.class).isEqualTo(expectedValue.toString());

    Optional<Object> object = underTest.decode(result.get());
    UnsignedLong decoded =
        (UnsignedLong)
            assertThat(object).isPresent().get().isInstanceOf(UnsignedLong.class).actual();
    assertThat(decoded.bigIntegerValue()).isEqualTo(expectedValue);
  }

  @Test
  void binaryTest() {
    byte[] expectedValue = "Hello World".getBytes(StandardCharsets.UTF_8);
    Schema expectedSchema =
        new SchemaBuilder(Schema.Type.BYTES).name(Binary.class.getCanonicalName()).build();

    Optional<SchemaAndValue> result = underTest.encode(new Binary(expectedValue));
    SchemaAndValue sv = assertThat(result).isPresent().get().actual();
    assertThat(sv.schema()).isEqualTo(expectedSchema);
    assertThat(sv.value()).isInstanceOf(byte[].class).isEqualTo(expectedValue);

    Optional<Object> object = underTest.decode(result.get());
    Binary decoded =
        (Binary) assertThat(object).isPresent().get().isInstanceOf(Binary.class).actual();
    assertThat(decoded.asByteArray()).isEqualTo(expectedValue);
  }

  @ParameterizedTest
  @ValueSource(strings = {"Hello World", ""})
  void symbolTest(String expectedValue) {
    Schema expectedSchema =
        new SchemaBuilder(Schema.Type.STRING)
            .name(Symbol.class.getCanonicalName())
            .optional()
            .build();

    Optional<SchemaAndValue> result = underTest.encode(Symbol.valueOf(expectedValue));
    SchemaAndValue sv = assertThat(result).isPresent().get().actual();
    assertThat(sv.schema()).isEqualTo(expectedSchema);
    assertThat(sv.value()).isInstanceOf(String.class).isEqualTo(expectedValue);

    Optional<Object> object = underTest.decode(result.get());
    Symbol decoded =
        (Symbol) assertThat(object).isPresent().get().isInstanceOf(Symbol.class).actual();
    assertThat(decoded.toString()).isEqualTo(expectedValue);
  }

  @Test
  void nullSymbolTest() {
    Schema expectedSchema =
        new SchemaBuilder(Schema.Type.STRING)
            .name(Symbol.class.getCanonicalName())
            .optional()
            .build();
    SchemaAndValue schemaAndValue = new SchemaAndValue(expectedSchema, null);
    Optional<Object> object = underTest.decode(schemaAndValue);
    assertThat(object).isNotPresent();
  }

  @Test
  void notAmqpTypeTest() {
    assertThat(underTest.encode(6L)).isNotPresent();
    assertThat(underTest.decode(new SchemaAndValue(Schema.STRING_SCHEMA, "foo"))).isNotPresent();
    assertThat(underTest.encode(null)).isNotPresent();
  }

  @Test
  void annotationsTest() {
    // this test requires a composite converter
    Map<Symbol, Object> annotations = new LinkedHashMap<>();
    annotations.put(Symbol.valueOf("uuid"), UUID.randomUUID());
    annotations.put(Symbol.valueOf("ulong"), UnsignedLong.valueOf(-1L));
    annotations.put(Symbol.valueOf("uint"), UnsignedInteger.valueOf(-1));
    annotations.put(Symbol.valueOf("ushort"), UnsignedShort.valueOf((short) -1));
    annotations.put(Symbol.valueOf("ubyte"), UnsignedByte.valueOf((byte) 0xff));
    annotations.put(Symbol.valueOf("symbol"), Symbol.valueOf("hello world"));
    annotations.put(
        Symbol.valueOf("binary"),
        new Binary("Goodbye cruel world".getBytes(StandardCharsets.UTF_8)));
    annotations.put(Symbol.valueOf("int"), 5);
    MessageAnnotations messageAnnotations = new MessageAnnotations(annotations);

    EncoderDecoder compositConverter =
        new EncoderDecoder.ChainedEnDec(
            new AmqpEnDec(), new UniqueTypeEnDec(), new KafkaEnDec());
    SchemaAndValue encoded =
        assertThat(compositConverter.encode(messageAnnotations)).isPresent().actual().get();
    Schema encodedSchema = encoded.schema();
    assertThat(encodedSchema.name()).isEqualTo(MessageAnnotations.class.getName());

    assertThat(encodedSchema.fields().stream().map(Field::name).toList())
        .describedAs("Fields should be in the original order")
        .containsExactlyElementsOf(annotations.keySet().stream().map(Symbol::toString).toList());

    assertThat(encoded.value()).isInstanceOf(Struct.class);
    Struct values = (Struct) encoded.value();

    Optional<Object> decoded = compositConverter.decode(encoded);
    assertThat(decoded)
        .isPresent()
        .get()
        .isInstanceOf(MessageAnnotations.class)
        .isEqualTo(messageAnnotations);
  }

  @Test
  void footersTest() {
    // this test requires a composite converter
    Map<Symbol, Object> footers = new LinkedHashMap<>();
    footers.put(Symbol.valueOf("uuid"), UUID.randomUUID());
    footers.put(Symbol.valueOf("ulong"), UnsignedLong.valueOf(-1L));
    footers.put(Symbol.valueOf("uint"), UnsignedInteger.valueOf(-1));
    footers.put(Symbol.valueOf("ushort"), UnsignedShort.valueOf((short) -1));
    footers.put(Symbol.valueOf("ubyte"), UnsignedByte.valueOf((byte) 0xff));
    footers.put(Symbol.valueOf("symbol"), Symbol.valueOf("hello world"));
    footers.put(
        Symbol.valueOf("binary"),
        new Binary("Goodbye cruel world".getBytes(StandardCharsets.UTF_8)));
    footers.put(Symbol.valueOf("int"), 5);
    Footer messageFooters = new Footer(footers);

    EncoderDecoder compositConverter =
        new EncoderDecoder.ChainedEnDec(
            new AmqpEnDec(), new UniqueTypeEnDec(), new KafkaEnDec());
    SchemaAndValue encoded =
        assertThat(compositConverter.encode(messageFooters)).isPresent().actual().get();
    Schema encodedSchema = encoded.schema();
    assertThat(encodedSchema.name()).isEqualTo(Footer.class.getName());

    assertThat(encodedSchema.fields().stream().map(Field::name).toList())
        .describedAs("Fields should be in the original order")
        .containsExactlyElementsOf(footers.keySet().stream().map(Symbol::toString).toList());

    assertThat(encoded.value()).isInstanceOf(Struct.class);
    Struct values = (Struct) encoded.value();

    Optional<Object> decoded = compositConverter.decode(encoded);
    assertThat(decoded).isPresent().get().isInstanceOf(Footer.class).isEqualTo(messageFooters);
  }
}
