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
public final class UniqueTypeConverter extends Converter {

  /** Constructor. */
  public UniqueTypeConverter() {}

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
