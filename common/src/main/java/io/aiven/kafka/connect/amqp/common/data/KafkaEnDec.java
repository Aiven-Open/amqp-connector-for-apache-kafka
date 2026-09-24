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

import static org.apache.kafka.connect.data.Schema.Type.ARRAY;

import com.google.common.annotations.VisibleForTesting;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.List;
import java.util.Optional;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Values;

/**
 * Performs Kafka conversions.
 *
 * <p>This converter will handle;
 *
 * <ul>
 *   <li>{@code null} values into an optional byte schema with a null value. This only occurs if the
 *       includeNull flag is set in the constructor.
 *   <li>Any value that has an inferred schema form {@link Values#inferSchema(Object)}
 *   <li>{@code BigInteger} into a string value. The schema name for the encoded value is
 *       "BigInteger"
 *   <li>{@code BigDecimal} into a string value. The schema name for the encoded value is
 *       "BigDecimal"
 * </ul>
 */
public final class KafkaEnDec extends EncoderDecoder {
  @VisibleForTesting static final String BIG_DECIMAL_NAME = "BigDecimal";
  @VisibleForTesting static final String BIG_INTEGER_NAME = "BigInteger";

  /** null conversion flag. */
  private final boolean includeNull;

  /** Constructs an encoder that <em>does</em> convert {@code null} values. */
  public KafkaEnDec() {
    this(true);
  }

  /**
   * Constructor.
   *
   * @param includeNull if {@code true} nulls are converted, if {@code false} nulls are not
   *     converted.
   */
  private KafkaEnDec(boolean includeNull) {
    this.includeNull = includeNull;
  }

  @Override
  public Optional<SchemaAndValue> encode(Object value) {
    if (value == null && includeNull) {
      return Optional.of(new SchemaAndValue(Schema.OPTIONAL_BYTES_SCHEMA, null));
    }

    Schema schema = Values.inferSchema(value);
    if (schema != null) {
      return Optional.of(new SchemaAndValue(schema, value));
    }

    if (value instanceof BigDecimal) {
      return Optional.of(
          new SchemaAndValue(
              new SchemaBuilder(Schema.Type.STRING).name(BIG_DECIMAL_NAME).build(),
              value.toString()));
    }
    if (value instanceof BigInteger) {
      return Optional.of(
          new SchemaAndValue(
              new SchemaBuilder(Schema.Type.STRING).name(BIG_INTEGER_NAME).build(),
              value.toString()));
    }
    return Optional.empty();
  }

  @Override
  public Optional<Object> decode(SchemaAndValue schemaAndValue) {
    Schema schema = schemaAndValue.schema();
    if (schema != null) {
      if (schemaAndValue.value() instanceof String s) {
        if (BIG_DECIMAL_NAME.equals(schema.name())) {
          return Optional.of(new BigDecimal(s));
        }
        if (BIG_INTEGER_NAME.equals(schema.name())) {
          return Optional.of(new BigInteger(s));
        }
        if (Schema.STRING_SCHEMA.equals(schema)) {
          return Optional.of(s);
        }
      }

      if (schemaAndValue.schema().type().isPrimitive()) {
        return Optional.of(schemaAndValue.value());
      }

      if (schemaAndValue.schema().type() == ARRAY) {
        List<?> collection = (List<?>) schemaAndValue.value();
        return Optional.of(collection.toArray());
      }

      //      if (schemaAndValue.schema().type() == MAP
      //          && schemaAndValue.value() instanceof Map<?, ?> map) {
      //        return Optional.of(map);
      //      }
    }
    return Optional.empty();
  }
}
