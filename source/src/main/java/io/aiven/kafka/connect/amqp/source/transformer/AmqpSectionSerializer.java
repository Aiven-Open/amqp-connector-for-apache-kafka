package io.aiven.kafka.connect.amqp.source.transformer;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;
import org.apache.qpid.protonj2.types.messaging.Section;

import java.io.IOException;

public final class AmqpSectionSerializer extends StdSerializer<Section> {

	public AmqpSectionSerializer() {
		this(null);
	}

	public AmqpSectionSerializer(Class<Section> t) {
		super(t);
	}

	@Override
	public void serialize(Section section, JsonGenerator gen, SerializerProvider provider) throws IOException {
		gen.writeObject(section.getValue());
	}
}
