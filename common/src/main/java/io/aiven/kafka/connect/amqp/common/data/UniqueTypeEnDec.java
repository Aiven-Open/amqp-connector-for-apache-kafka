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

import de.huxhorn.sulky.ulid.ULID;
import java.util.Optional;
import java.util.UUID;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.SchemaBuilder;

/**
 * Converts Unique type objects. Specifically:
 *
 * <ul>
 *   <li>UUID
 *   <li>ULID.Value
 * </ul>
 */
public final class UniqueTypeEnDec extends EncoderDecoder {

  /** Constructor. */
  public UniqueTypeEnDec() {}

  @Override
  public Optional<SchemaAndValue> encode(Object value) {

    if (value instanceof UUID || value instanceof ULID.Value) {
      String name = asName(value.getClass());
      return Optional.of(
          new SchemaAndValue(
              new SchemaBuilder(Schema.Type.STRING).name(name).build(), value.toString()));
    }
    return Optional.empty();
  }

  @Override
  public Optional<Object> decode(SchemaAndValue schemaAndValue) {
    String name = schemaAndValue.schema().name();
    if (isName(UUID.class, name)) {
      return Optional.of(UUID.fromString((String) schemaAndValue.value()));
    }
    if (isName(ULID.Value.class, name)) {
      return Optional.of(ULID.parseULID((String) schemaAndValue.value()));
    }

    return Optional.empty();
  }
}
