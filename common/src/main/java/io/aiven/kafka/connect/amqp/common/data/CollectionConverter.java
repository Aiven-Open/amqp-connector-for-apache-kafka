package io.aiven.kafka.connect.amqp.common.data;

import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Converts collections and maps with arbitrary values into Kafka {@link Struct} types. */
public final class CollectionConverter extends Converter {
  private static final Logger LOGGER = LoggerFactory.getLogger(CollectionConverter.class);

  /** Constructor. */
  public CollectionConverter() {}

  /**
   * Create a struct that contains the items from the collection.
   * @param schemaBuilder the schema builder to add the schema to.
   * @param collection the collection to process.
   * @return the completed schema.
   */
  public Optional<SchemaAndValue> encodeCollection(
      SchemaBuilder schemaBuilder, Collection<?> collection) {
    final List<Object> values = new ArrayList<>();
    int idx = 0;
    for (Object o : collection) {
      String key = Integer.toString(idx);
      self()
          .encode(o)
          .ifPresentOrElse(
              sv -> {
                schemaBuilder.field(key, sv.schema());
                values.add(sv.value());
              },
              () -> LOGGER.warn("unable to encode {} with {}", o.getClass(), self().toString()));
      idx++;
    }
    Schema schema = schemaBuilder.build();
    Struct struct = new Struct(schema);
    for (int i = 0; i < values.size(); i++) {
      struct.put(Integer.toString(i), values.get(i));
    }
    return Optional.of(new SchemaAndValue(schema, struct));
  }

  @Override
  public Optional<SchemaAndValue> encode(Object value) {
    if (value instanceof Collection) {
      return encodeCollection(
          SchemaBuilder.struct().name(asName(value.getClass())), (Collection<?>) value);
    }
    if (value instanceof Map) {
      SchemaBuilder schemaBuilder = SchemaBuilder.struct().name(asName(value.getClass()));
      final List<Object> values = new ArrayList<>();

      for (Map.Entry<Object, Object> o : ((Map<Object, Object>) value).entrySet()) {
        self()
            .encode(o.getValue())
            .ifPresentOrElse(
                sv -> {
                  schemaBuilder.field(o.getKey().toString(), sv.schema());
                  values.add(sv.value());
                },
                () -> LOGGER.warn("unable to encode {} with {}", o.getClass(), self().toString()));
      }
      Schema schema = schemaBuilder.build();
      List<Field> fields = schema.fields();
      Struct struct = new Struct(schema);
      for (int i = 0; i < values.size(); i++) {
        struct.put(fields.get(i), values.get(i));
      }
      return Optional.of(new SchemaAndValue(schema, struct));
    }

    if (value.getClass().isArray()) {
      List<Object> lst = new ArrayList<>();
      int length = Array.getLength(value);
      for (int i = 0; i < length; i++) {
        lst.add(Array.get(value, i));
      }
      return encodeCollection(SchemaBuilder.struct().name(asName(value.getClass())), lst);
    }

    return Optional.empty();
  }

  private List<Object> decodeCollection(SchemaAndValue schemaAndValue) {
    List<Object> result = new ArrayList<>();
    List<Field> fields = schemaAndValue.schema().fields();
    Struct struct = (Struct) schemaAndValue.value();
    for (int i = 0; i < fields.size(); i++) {
      Field field = fields.get(i);
      final SchemaAndValue sv = new SchemaAndValue(field.schema(), struct.get(field));
      self()
          .decode(sv)
          .ifPresentOrElse(
              result::add, () -> LOGGER.warn("unable to decode {} with {}", sv, self().toString()));
    }
    return result;
  }

  @Override
  public Optional<Object> decode(SchemaAndValue schemaAndValue) {

    String className = schemaAndValue.schema().name();
    if (className != null) {
      try {
        Class<?> c = Class.forName(className);

        if (c.isArray()) {
          List<Object> result = decodeCollection(schemaAndValue);
          return Optional.of(result.toArray());
        }

        if (Collection.class.isAssignableFrom(c)) {
          return Optional.of(decodeCollection(schemaAndValue));
        }

        if (Map.class.isAssignableFrom(c)) {
          Map<String, Object> map = new LinkedHashMap<>();
          List<Field> fields = schemaAndValue.schema().fields();
          Struct struct = (Struct) schemaAndValue.value();
          for (int i = 0; i < fields.size(); i++) {
            Field field = fields.get(i);
            final SchemaAndValue sv = new SchemaAndValue(field.schema(), struct.get(field));
            self()
                    .decode(sv)
                    .ifPresentOrElse(
                            value -> map.put(field.name(), value),
                            () -> LOGGER.warn("unable to decode {} with {}", sv, self().toString()));
          }
          return Optional.of(map);
        }

      } catch (ClassNotFoundException e) {
        LOGGER.warn("Class {} not found in {}", schemaAndValue.schema().name(), self().toString());
        return Optional.empty();
      }
    }

    return Optional.empty();
  }
}
