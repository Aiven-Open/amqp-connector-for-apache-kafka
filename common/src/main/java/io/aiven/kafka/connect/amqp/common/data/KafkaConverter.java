package io.aiven.kafka.connect.amqp.common.data;

import com.google.common.annotations.VisibleForTesting;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.List;
import java.util.Optional;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Values;

import static org.apache.kafka.connect.data.Schema.Type.ARRAY;

/**
 * Performs Kafka conversions.
 *
 * <p>This converter will handle;
 *
 * <ul>
 *   <li>{@code null} values into an optional byte schema with a null value.
 *   <li>Any value that has an infered schema form {@link Values#inferSchema(Object)}
 *   <li>{@code BigInteger} into a string value
 *   <li>{@code BigDecimal} into a string value
 * </ul>
 */
public final class KafkaConverter extends Converter {
  @VisibleForTesting static final String BIG_DECIMAL_NAME = "BigDecimal";
  @VisibleForTesting static final String BIG_INTEGER_NAME = "BigInteger";

  /** Constructor. */
  public KafkaConverter() {}

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

//      if (schemaAndValue.value() instanceof Number n) {
//        if (schema.equals(Schema.INT8_SCHEMA)) {
//          return Optional.of(n.byteValue());
//        }
//        if (schema.equals(Schema.INT16_SCHEMA)) {
//          return Optional.of(n.shortValue());
//        }
//
//        if (schema.equals(Schema.INT32_SCHEMA)) {
//          return Optional.of(n.intValue());
//        }
//
//        if (schema.equals(Schema.INT64_SCHEMA)) {
//          return Optional.of(n.longValue());
//        }
//
//        if (schema.equals(Schema.FLOAT32_SCHEMA)) {
//          return Optional.of(n.floatValue());
//        }
//
//        if (schema.equals(Schema.FLOAT64_SCHEMA)) {
//          return Optional.of(n.doubleValue());
//        }
//      }
//      if (schemaAndValue.value() instanceof String s) {
//        if (BIG_DECIMAL_NAME.equals(schema.name())) {
//          return Optional.of(new BigDecimal(s));
//        }
//        if (BIG_INTEGER_NAME.equals(schema.name())) {
//          return Optional.of(new BigInteger(s));
//        }
//        if (Schema.STRING_SCHEMA.equals(schema)) {
//          return Optional.of(s);
//        }
//      }


      if (schemaAndValue.schema().type() == ARRAY) {
        List<?> collection = (List<?>) schemaAndValue.value();
        return Optional.of(collection.toArray());
      }
    }
    return Optional.empty();
  }
}
