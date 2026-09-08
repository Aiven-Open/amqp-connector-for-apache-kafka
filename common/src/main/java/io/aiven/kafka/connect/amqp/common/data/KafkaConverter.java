package io.aiven.kafka.connect.amqp.common.data;

import com.google.common.annotations.VisibleForTesting;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Optional;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Values;

/**
 * Performs Kafka conversions. This converter will convert
 *
 * <ul>
 *   <li>{@code null} values into an optional byte schema with a null value.
 *   <li>{@code BigInteger} into a string value
 *   <li>{@code BigDecimal} into a string value
 * </ul>
 */
public class KafkaConverter extends Converter {
  @VisibleForTesting static final String BIG_DECIMAL_NAME = "BigDecimal";
  @VisibleForTesting static final String BIG_INTEGER_NAME = "BigInteger";

  @Override
  public Optional<SchemaAndValue> encode(Object value) {
    if (value == null) {
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

    //    if (value instanceof String) {
    //      return Optional.of(new SchemaAndValue(Schema.STRING_SCHEMA, value));
    //    }
    //    if (value instanceof Boolean) {
    //      return Optional.of(new SchemaAndValue(Schema.BOOLEAN_SCHEMA, value));
    //    }
    //    if (value instanceof byte[]) {
    //      return Optional.of(new SchemaAndValue(Schema.BYTES_SCHEMA, value));
    //    }
    //
    //    if (value instanceof List) {
    //      Schema schema =  Values.inferSchema((List<?>) value);
    //      if (schema == null) {
    //
    //      }
    //    }
    return Optional.empty();
  }

  @Override
  public Optional<Object> decode(SchemaAndValue schemaAndValue) {
    Schema schema = schemaAndValue.schema();
    if (schema != null) {
      if (schemaAndValue.value() instanceof Number n) {
        if (schema.equals(Schema.INT8_SCHEMA)) {
          return Optional.of(n.byteValue());
        }
        if (schema.equals(Schema.INT16_SCHEMA)) {
          return Optional.of(n.shortValue());
        }

        if (schema.equals(Schema.INT32_SCHEMA)) {
          return Optional.of(n.intValue());
        }

        if (schema.equals(Schema.INT64_SCHEMA)) {
          return Optional.of(n.longValue());
        }

        if (schema.equals(Schema.FLOAT32_SCHEMA)) {
          return Optional.of(n.floatValue());
        }

        if (schema.equals(Schema.FLOAT64_SCHEMA)) {
          return Optional.of(n.doubleValue());
        }
      }
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

      if (schemaAndValue.value() instanceof Boolean b && Schema.BOOLEAN_SCHEMA.equals(schema)) {
        return Optional.of(b);
      }
      if (schemaAndValue.value() instanceof byte[] b && Schema.BYTES_SCHEMA.equals(schema)) {
        return Optional.of(b);
      }
    }
    return Optional.empty();
  }
}
