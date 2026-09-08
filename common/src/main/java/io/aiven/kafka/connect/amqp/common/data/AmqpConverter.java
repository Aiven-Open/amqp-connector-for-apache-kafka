package io.aiven.kafka.connect.amqp.common.data;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AmqpConverter extends Converter {

  private static final Logger LOGGER = LoggerFactory.getLogger(AmqpConverter.class);

  @Override
  public Optional<SchemaAndValue> encode(Object value) {
    if (value == null) {
      return Optional.empty();
    }
    String name = Converter.asName(value.getClass());
    if (name.startsWith("org.apache.qpid.protonj2.types.")) {
      if (value instanceof Number n) {
        //                if (value instanceof Decimal32) {
        //                    return Optional.of(new SchemaAndValue(builder.field("value",
        // Schema.FLOAT32_SCHEMA), n.floatValue()));
        //                }
        //                if (value instanceof Decimal64) {
        //                    return Optional.of(new SchemaAndValue(builder.field("value",
        // Schema.FLOAT64_SCHEMA), n.doubleValue()));
        //                }
        //                if (value instanceof Decimal128 d) {
        //                    return Optional.of(new SchemaAndValue(builder.field("msb",
        // Schema.FLOAT64_SCHEMA)
        //                            .field("lsb", Schema.FLOAT64_SCHEMA), new
        // long[]{d.getMostSignificantBits(), d.getLeastSignificantBits()}));
        //                }
        if (value instanceof UnsignedByte) {
          return Optional.of(
              new SchemaAndValue(
                  new SchemaBuilder(Schema.Type.INT16).name(name).build(), n.shortValue()));
        }
        if (value instanceof UnsignedShort) {
          return Optional.of(
              new SchemaAndValue(
                  new SchemaBuilder(Schema.Type.INT32).name(name).build(),
                  // return Optional.of(new SchemaAndValue(builder.field("value",
                  // Schema.INT32_SCHEMA).build(),
                  n.intValue()));
        }
        if (value instanceof UnsignedInteger) {
          return Optional.of(
              new SchemaAndValue(
                  new SchemaBuilder(Schema.Type.INT64).name(name).build(),
                  // return Optional.of(new SchemaAndValue(builder.field("value",
                  // Schema.INT64_SCHEMA).build(),
                  n.longValue()));
        }
        if (value instanceof UnsignedLong) {
          return Optional.of(
              new SchemaAndValue(
                  new SchemaBuilder(Schema.Type.STRING).name(name).build(), value.toString()));
          // return Optional.of(new SchemaAndValue(builder.field("value",
          // Schema.STRING_SCHEMA).build(), value));
        }
      }

      if (value instanceof Binary) {
        return Optional.of(
            new SchemaAndValue(
                new SchemaBuilder(Schema.Type.BYTES).name(name).build(),
                ((Binary) value).asByteArray()));
      }

      if (value instanceof Symbol) {
        return Optional.of(
            new SchemaAndValue(
                new SchemaBuilder(Schema.Type.STRING).name(name).optional().build(),
                value.toString()));
      }

      if (value instanceof MessageAnnotations annotations) {
        return symbolObjectMap(name, annotations.getValue());
      }

      if (value instanceof Footer footers) {
        return symbolObjectMap(name, footers.getValue());
      }
    }
    return Optional.empty();
  }

  private Optional<SchemaAndValue> symbolObjectMap(String name, Map<Symbol, Object> data) {
    SchemaBuilder schemaBuilder = SchemaBuilder.struct().name(name);
    Map<String, Object> values = new HashMap<>();
    data.forEach(
        (key1, value) -> {
          String k = key1.toString();
          self()
              .encode(value)
              .ifPresentOrElse(
                  schemaAndValue -> {
                    String key = key1.toString();
                    schemaBuilder.field(key, schemaAndValue.schema());
                    values.put(key, schemaAndValue.value());
                  },
                  () ->
                      LOGGER.warn(
                          "unable to encode {} with {}", value.getClass(), self().toString()));
        });
    Schema schema = schemaBuilder.build();
    Struct struct = new Struct(schema);
    for (Field field : schema.fields()) {
      struct.put(field, values.get(field.name()));
    }
    return Optional.of(new SchemaAndValue(schema, struct));
  }

  @Override
  public Optional<Object> decode(SchemaAndValue schemaAndValue) {
    String name = schemaAndValue.schema().name();
    if (name != null && name.startsWith("org.apache.qpid.protonj2.types.")) {
      if (Converter.isName(UnsignedByte.class, name)) {
        Number n = (Number) schemaAndValue.value();
        return Optional.of(new UnsignedByte(n.byteValue()));
      }

      if (Converter.isName(UnsignedShort.class, name)) {
        Number n = (Number) schemaAndValue.value();
        return Optional.of(new UnsignedShort(n.shortValue()));
      }

      if (Converter.isName(UnsignedInteger.class, name)) {
        Number n = (Number) schemaAndValue.value();
        return Optional.of(new UnsignedInteger(n.intValue()));
      }

      if (Converter.isName(UnsignedLong.class, name)) {
        return Optional.of(UnsignedLong.valueOf((String) schemaAndValue.value()));
      }

      if (Converter.isName(Binary.class, name)) {
        return Optional.of(new Binary((byte[]) schemaAndValue.value()));
      }

      if (Converter.isName(Symbol.class, name)) {
        return Optional.ofNullable(Symbol.getSymbol((String) schemaAndValue.value()));
      }

      if (Converter.isName(MessageAnnotations.class, name)) {

        Map<Symbol, Object> annotations = new LinkedHashMap<>();
        List<Object> values = (List<Object>) schemaAndValue.value();

        for (Field field : schemaAndValue.schema().fields()) {
          Symbol fieldSymbol = Symbol.valueOf(field.name());
          SchemaAndValue fieldValue = new SchemaAndValue(field.schema(), values.get(field.index()));
          self()
              .decode(fieldValue)
              .ifPresentOrElse(
                  object -> annotations.put(fieldSymbol, object),
                  () -> annotations.put(fieldSymbol, fieldValue.value()));
        }

        return Optional.of(new MessageAnnotations(annotations));
      }

      if (Converter.isName(Footer.class, name)) {

        Map<Symbol, Object> footers = new LinkedHashMap<>();
        List<Object> values = (List<Object>) schemaAndValue.value();

        for (Field field : schemaAndValue.schema().fields()) {
          Symbol fieldSymbol = Symbol.valueOf(field.name());
          SchemaAndValue fieldValue = new SchemaAndValue(field.schema(), values.get(field.index()));
          self()
              .decode(fieldValue)
              .ifPresentOrElse(
                  object -> footers.put(fieldSymbol, object),
                  () -> footers.put(fieldSymbol, fieldValue.value()));
        }

        return Optional.of(new Footer(footers));
      }
    }
    return Optional.empty();
  }
}
