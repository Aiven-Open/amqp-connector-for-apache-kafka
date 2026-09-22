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

import de.huxhorn.sulky.ulid.ULID;
import java.util.Optional;
import java.util.UUID;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.junit.jupiter.api.Test;

public class UniqueTypeEnDecTest {
  private final UniqueTypeEnDec underTest = new UniqueTypeEnDec();

  @Test
  void uuidTest() {
    UUID expectedValue = UUID.randomUUID();
    Schema expectedSchema =
        new SchemaBuilder(Schema.Type.STRING).name(UUID.class.getCanonicalName()).build();

    Optional<SchemaAndValue> result = underTest.encode(expectedValue);
    SchemaAndValue sv = assertThat(result).isPresent().get().actual();
    assertThat(sv.schema()).isEqualTo(expectedSchema);
    assertThat(sv.value()).isInstanceOf(String.class).isEqualTo(expectedValue.toString());

    Optional<Object> object = underTest.decode(result.get());
    UUID decoded = (UUID) assertThat(object).isPresent().get().isInstanceOf(UUID.class).actual();
    assertThat(decoded).isEqualTo(expectedValue);
  }

  @Test
  void ulidTest() {
    ULID ulid = new ULID();
    ULID.Value expectedValue = ulid.nextValue();

    Schema expectedSchema =
        new SchemaBuilder(Schema.Type.STRING).name(ULID.Value.class.getName()).build();

    Optional<SchemaAndValue> result = underTest.encode(expectedValue);
    SchemaAndValue sv = assertThat(result).isPresent().get().actual();
    assertThat(sv.schema()).isEqualTo(expectedSchema);
    assertThat(sv.value()).isInstanceOf(String.class).isEqualTo(expectedValue.toString());

    Optional<Object> object = underTest.decode(result.get());
    ULID.Value decoded =
        (ULID.Value) assertThat(object).isPresent().get().isInstanceOf(ULID.Value.class).actual();
    assertThat(decoded).isEqualTo(expectedValue);
  }

  @Test
  void notUniqueTypeTest() {
    assertThat(underTest.encode(6L)).isNotPresent();
    assertThat(underTest.decode(new SchemaAndValue(Schema.STRING_SCHEMA, "foo"))).isNotPresent();
    assertThat(underTest.encode(null)).isNotPresent();
  }
}
