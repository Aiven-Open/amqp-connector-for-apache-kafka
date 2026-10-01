/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.aiven.kafka.connect.amqp.common.data;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Optional;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.junit.jupiter.api.Test;

public class EncoderDecoderTest {

  @Test
  void chainedConverterTest() {
    Converter1 c1 = new Converter1();
    Converter2 c2 = new Converter2();

    EncoderDecoder underTest = c1.andThen(c2);

    assertThat(underTest.encode(6)).isNotPresent();
    SchemaAndValue schemaAndValue = assertThat(underTest.encode(6L)).isPresent().get().actual();
    assertThat(schemaAndValue.schema().name()).isEqualTo("converter1");
    assertThat(schemaAndValue.value()).isEqualTo("6");

    assertThat(underTest.decode(new SchemaAndValue(Converter1.schema, "7")))
        .isPresent()
        .get()
        .isInstanceOf(Long.class)
        .isEqualTo(7L);

    assertThat(underTest.decode(new SchemaAndValue(Converter2.schema, "8")))
        .isPresent()
        .get()
        .isInstanceOf(Long.class)
        .isEqualTo(8L);

    assertThat(underTest.decode(new SchemaAndValue(Schema.INT64_SCHEMA, 6L))).isNotPresent();

    underTest = c2.andThen(c1);

    assertThat(underTest.encode(7)).isNotPresent();
    SchemaAndValue schemaAndValue2 = assertThat(underTest.encode(6L)).isPresent().get().actual();
    assertThat(schemaAndValue2.schema().name()).isEqualTo("converter2");
    assertThat(schemaAndValue2.value()).isEqualTo("6");

    assertThat(underTest.decode(new SchemaAndValue(Converter1.schema, "7")))
        .isPresent()
        .get()
        .isInstanceOf(Long.class)
        .isEqualTo(7L);

    assertThat(underTest.decode(new SchemaAndValue(Converter2.schema, "8")))
        .isPresent()
        .get()
        .isInstanceOf(Long.class)
        .isEqualTo(8L);

    assertThat(underTest.decode(new SchemaAndValue(Schema.INT64_SCHEMA, 6L))).isNotPresent();
  }

  static class Converter1 extends EncoderDecoder {

    static Schema schema = new SchemaBuilder(Schema.Type.STRING).name("converter1").build();

    @Override
    public Optional<SchemaAndValue> encode(Object value) {
      if (value instanceof Long) {
        return Optional.of(new SchemaAndValue(schema, value.toString()));
      }
      return Optional.empty();
    }

    @Override
    public Optional<Object> decode(SchemaAndValue schemaAndValue) {
      if ("converter1".equals(schemaAndValue.schema().name())) {
        return Optional.of(Long.valueOf((String) schemaAndValue.value()));
      }
      return Optional.empty();
    }
  }

  static class Converter2 extends EncoderDecoder {

    static Schema schema = new SchemaBuilder(Schema.Type.STRING).name("converter2").build();

    @Override
    public Optional<SchemaAndValue> encode(Object value) {
      if (value instanceof Long) {
        return Optional.of(new SchemaAndValue(schema, value.toString()));
      }
      return Optional.empty();
    }

    @Override
    public Optional<Object> decode(SchemaAndValue schemaAndValue) {
      if ("converter2".equals(schemaAndValue.schema().name())) {
        return Optional.of(Long.valueOf((String) schemaAndValue.value()));
      }
      return Optional.empty();
    }
  }
}
