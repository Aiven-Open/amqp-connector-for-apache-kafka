package io.aiven.kafka.connect.amqp.source.transformer;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;
import java.io.IOException;
import org.apache.qpid.protonj2.types.messaging.Section;

/**
 * Serializes sections of an AMQP message into a JSON format.
 */
public final class AmqpSectionSerializer extends StdSerializer<Section<?>> {

  /**
   * Default constructor.
   */
  public AmqpSectionSerializer() {
    this(null);
  }

  /**
   * Constructor for a specific section type.
   * @param t the section type class.
   */
  public AmqpSectionSerializer(Class<Section<?>> t) {
    super(t);
  }

  @Override
  public void serialize(Section section, JsonGenerator gen, SerializerProvider provider)
      throws IOException {
    gen.writeObject(section.getValue());
  }
}
