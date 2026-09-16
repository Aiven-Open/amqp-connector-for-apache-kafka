package io.aiven.kafka.connect.amqp.common.data;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import org.apache.kafka.connect.data.SchemaAndValue;

/** Abstract class to convert objects to SchemaAnValue objects. */
public abstract class Converter {

  /** The converter that is the self for this converter */
  private Converter self;

  /** Default constructor. */
  protected Converter() {}

  /**
   * Determines if the class name is the name provided.
   *
   * @param clazz the class to check.
   * @param name the expected name.
   * @return {@code true} if the class name is the name provided, {@code false} otherwise.
   */
  static boolean isName(Class<?> clazz, String name) {
    return asName(clazz).equals(name);
  }

  /**
   * Converts the class into its name.
   *
   * @param clazz the class.
   * @return the canonical name for the class.
   */
  static String asName(Class<?> clazz) {
    return clazz.getName();
  }

  /**
   * Extracts the value from the schemaAndValue and casts it as a {@code Number}.
   *
   * @param schemaAndValue the SchemaAndValue to convert.
   * @return the value from the schemaAndValue cast as a {@code Number}.
   */
  static Number asNumber(SchemaAndValue schemaAndValue) {
    return (Number) schemaAndValue.value();
  }

  /**
   * Encodes the {@code value} as a SchemaAndValue. If this converter can not perform the encoding
   * it returns an empty {@code Optional}.
   *
   * @param value the value to convert.
   * @return the Optional SchemaAndValue or an empty Optional.
   */
  public abstract Optional<SchemaAndValue> encode(Object value);

  /**
   * Decodes a SchemaAndValue into an object. If this converter can not perform the decoding it
   * returns an empty {@code Optional}.
   *
   * @param schemaAndValue the schema and value to decode.
   * @return the Optional Object or an empty Optional if not decoded.
   */
  public abstract Optional<Object> decode(SchemaAndValue schemaAndValue);

  /**
   * A reference to this converter. The {@code self()} method is used so that when Converters are
   * chained together it is possible for a lower level Converter to recursively call {@link
   * #encode(Object)} or {@link #decode(SchemaAndValue)}.
   *
   * @return the Converter against which recursive calls should be made.
   */
  protected final Converter self() {
    return self == null ? this : self;
  }

  /**
   * A method to set the Converter against which recursive calls should be made. In most cases this
   * will not be called except when the converter is used in a {@link ChainedConverter}.
   *
   * @param converter the converter against which recursive calls should be made.
   */
  protected void setSelf(Converter converter) {
    self = converter;
  }

  @Override
  public String toString() {
    return self().getClass().toString();
  }

  /**
   * Creates a chained converter where the {@code nextConverter} is called if this converter can not
   * encode/decode a value.
   *
   * @param nextConverter the converter to call if this converter can not encode/decode a value.
   * @return a new {@link ChainedConverter} comprised of this converter and the @{code
   *     nextConverter}.
   */
  public final Converter andThen(Converter nextConverter) {
    return new ChainedConverter(this, nextConverter);
  }

  /**
   * A Converter that chains multiple converters together. When {@link #encode(Object)} or {@link
   * #decode(SchemaAndValue)} is called the converters are called in chain order. The first
   * non-empty {@code Optional} (if any) is returned.
   */
  public static final class ChainedConverter extends Converter {
    /** The list of converters to attempt */
    private final List<Converter> converters;

    /**
     * Constructs a chain of converters.
     *
     * @param converters the chain of converters to use. Order is preserved.
     */
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
