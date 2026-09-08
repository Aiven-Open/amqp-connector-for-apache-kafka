package io.aiven.kafka.connect.amqp.common.data;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import org.apache.kafka.connect.data.SchemaAndValue;

public abstract class Converter {

  private Converter self;

  static boolean isName(Class<?> clazz, String name) {
    return asName(clazz).equals(name);
  }

  static String asName(Class<?> clazz) {
    return clazz.getCanonicalName();
  }

  static Number asNumber(SchemaAndValue schemaAndValue) {
    return (Number) schemaAndValue.value();
  }

  public abstract Optional<SchemaAndValue> encode(Object value);

  public abstract Optional<Object> decode(SchemaAndValue schemaAndValue);

  protected final Converter self() {
    return self == null ? this : self;
  }

  protected void setSelf(Converter converter) {
    self = converter;
  }

  @Override
  public String toString() {
    return self().getClass().toString();
  }

  public final Converter andThen(Converter nextConverter) {
    return new ChainedConverter(this, nextConverter);
  }

  public static class ChainedConverter extends Converter {
    private final List<Converter> converters;

    public ChainedConverter(Converter... converters) {
      this.converters = Arrays.asList(converters);
      setSelf(this);
    }

    @Override
    public Optional<SchemaAndValue> encode(Object value) {
      for (Converter converter : converters) {
        Optional<SchemaAndValue> result = converter.encode(value);
        if (result.isPresent()) {
          return result;
        }
      }
      return Optional.empty();
    }

    @Override
    public Optional<Object> decode(SchemaAndValue schemaAndValue) {
      for (Converter converter : converters) {
        Optional<Object> result = converter.decode(schemaAndValue);
        if (result.isPresent()) {
          return result;
        }
      }
      return Optional.empty();
    }

    @Override
    protected void setSelf(Converter newConverter) {
      super.setSelf(newConverter);
      this.converters.forEach(converter -> converter.setSelf(newConverter));
    }

    @Override
    public String toString() {
      return String.format(
          "%s(%s)",
          this.getClass().getName(),
          String.join(", ", converters.stream().map(Converter::toString).toList()));
    }
  }
}
