package io.aiven.kafka.connect.amqp.common.data;

import java.util.HashMap;
import java.util.LinkedHashMap;
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
import org.apache.qpid.protonj2.types.messaging.AmqpValue;
import org.apache.qpid.protonj2.types.messaging.Footer;
import org.apache.qpid.protonj2.types.messaging.MessageAnnotations;
import org.apache.qpid.protonj2.types.messaging.Section;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Converts AMQP values. */
public final class AmqpConverter extends Converter {

  private static final Logger LOGGER = LoggerFactory.getLogger(AmqpConverter.class);

  /** Constructor. */
  public AmqpConverter() {}

  @Override
  public Optional<SchemaAndValue> encode(Object value) {
    if (value == null) {
      return Optional.empty();
    }
    String name = asName(value.getClass());
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
                          new SchemaBuilder(Schema.Type.INT32).name(name).build(), n.intValue()));
        }
        if (value instanceof UnsignedInteger) {
          return Optional.of(
                  new SchemaAndValue(
                          new SchemaBuilder(Schema.Type.INT64).name(name).build(), n.longValue()));
        }
        if (value instanceof UnsignedLong) {
          return Optional.of(
                  new SchemaAndValue(
                          new SchemaBuilder(Schema.Type.STRING).name(name).build(), value.toString()));
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

      if (value instanceof Section section) {
        self().encode(section.getValue());
      }

    }
    return Optional.empty();
  }

  /**
   * Converts a Synmbol/Object map into a struct based SchemaAndValue. Symbol order in the map is
   * retained as field order in the struct.
   *
   * @param name the name of the resulting struct.
   * @param data the map to place into the struct.
   * @return the struct based SchemaAndValue.
   */
  private Optional<SchemaAndValue> symbolObjectMap(String name, Map<Symbol, Object> data) {
    SchemaBuilder schemaBuilder = SchemaBuilder.struct().name(name);
    Map<String, Object> values = new HashMap<>();
    data.forEach(
        (key1, value) -> {
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
      if (isName(UnsignedByte.class, name)) {
        Number n = (Number) schemaAndValue.value();
        return Optional.of(new UnsignedByte(n.byteValue()));
      }

      if (isName(UnsignedShort.class, name)) {
        Number n = (Number) schemaAndValue.value();
        return Optional.of(new UnsignedShort(n.shortValue()));
      }

      if (isName(UnsignedInteger.class, name)) {
        Number n = (Number) schemaAndValue.value();
        return Optional.of(new UnsignedInteger(n.intValue()));
      }

      if (isName(UnsignedLong.class, name)) {
        return Optional.of(UnsignedLong.valueOf((String) schemaAndValue.value()));
      }

      if (isName(Binary.class, name)) {
        return Optional.of(new Binary((byte[]) schemaAndValue.value()));
      }

      if (isName(Symbol.class, name)) {
        return Optional.ofNullable(Symbol.getSymbol((String) schemaAndValue.value()));
      }

      if (isName(MessageAnnotations.class, name)) {
        return Optional.of(new MessageAnnotations(extractSymbolMap(schemaAndValue)));
      }

      if (isName(Footer.class, name)) {
        return Optional.of(new Footer(extractSymbolMap(schemaAndValue)));
      }
    }
    return Optional.empty();
  }

  private Map<Symbol, Object> extractSymbolMap(final SchemaAndValue schemaAndValue) {
    Map<Symbol, Object> symbolMap = new LinkedHashMap<>();
    Struct values = (Struct) schemaAndValue.value();

    for (Field field : schemaAndValue.schema().fields()) {
      Symbol fieldSymbol = Symbol.valueOf(field.name());
      SchemaAndValue fieldValue = new SchemaAndValue(field.schema(), values.get(field));
      self()
          .decode(fieldValue)
          .ifPresentOrElse(
              object -> symbolMap.put(fieldSymbol, object),
              () -> symbolMap.put(fieldSymbol, fieldValue.value()));
    }
    return symbolMap;
  }
}
