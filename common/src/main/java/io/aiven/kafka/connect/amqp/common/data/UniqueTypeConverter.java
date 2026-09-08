package io.aiven.kafka.connect.amqp.common.data;

import de.huxhorn.sulky.ulid.ULID;
import java.util.Optional;
import java.util.UUID;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.SchemaBuilder;

public class UniqueTypeConverter extends Converter {
  @Override
  public Optional<SchemaAndValue> encode(Object value) {

    if (value instanceof UUID || value instanceof ULID.Value) {
      String name = value.getClass().getCanonicalName();
      return Optional.of(
          new SchemaAndValue(
              new SchemaBuilder(Schema.Type.STRING).name(name).build(), value.toString()));
    }
    return Optional.empty();
  }

  @Override
  public Optional<Object> decode(SchemaAndValue schemaAndValue) {
    String name = schemaAndValue.schema().name();
    if (Converter.isName(UUID.class, name)) {
      return Optional.of(UUID.fromString((String) schemaAndValue.value()));
    }
    if (Converter.isName(ULID.Value.class, name)) {
      return Optional.of(ULID.parseULID((String) schemaAndValue.value()));
    }

    return Optional.empty();
  }
}
