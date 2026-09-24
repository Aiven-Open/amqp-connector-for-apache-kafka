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
 *   <li>UUID - Converts the value to a String and produces a string schema with the name {@code
 *       java.util.UUID}.
 *   <li>ULID.Value - Converts the value into a String and produces a string schema with the name
 *       {@code de.huxhorn.sulky.ulid.ULID.Value}.
 * </ul>
 *
 * If {@code usePatternMatching} is enabled then the decoding of string values will be attempted. If
 * the string pattern matches either a UUID or ad ULID.Value then it will be decoded and an instance
 * of the object returned.
 */
public final class UniqueTypeEnDec extends EncoderDecoder {

  private final boolean usePatternMatching;

  /** Constructor that <em>does not</em> use pattern matching. */
  public UniqueTypeEnDec() {
    this.usePatternMatching = false;
  }

  /**
   * Constructor.
   *
   * @param usePatternMatching if {@code true} pattern matching will be attempted during decoding.
   *     If {@code false} pattern matching will not be attempted.
   */
  public UniqueTypeEnDec(boolean usePatternMatching) {
    this.usePatternMatching = usePatternMatching;
  }

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

  /**
   * Attempts to parse a UUID.
   *
   * @param str the potential UUID string.
   * @return a UUID if parsing was successful, or {@code null} if not.
   */
  private UUID parseUUID(String str) {
    try {
      return UUID.fromString(str);
    } catch (IllegalArgumentException expected) {
      return null;
    }
  }

  /**
   * Attempts to parse a ULID.Value.
   *
   * @param str the potential ULID.Value string.
   * @return a ULID.Value if parsing was successful, or {@code null} if not.
   */
  private ULID.Value parseULID(String str) {
    try {
      return ULID.parseULID(str);
    } catch (IllegalArgumentException expected) {
      return null;
    }
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

    if (usePatternMatching && schemaAndValue.value() instanceof String str) {
      Object result = parseUUID(str);
      if (result == null) {
        result = parseULID(str);
      }
      if (result != null) {
        return Optional.of(result);
      }
    }

    return Optional.empty();
  }
}
