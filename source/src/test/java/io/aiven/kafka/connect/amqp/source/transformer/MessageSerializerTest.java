package io.aiven.kafka.connect.amqp.source.transformer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.qpid.protonj2.client.Message;
import org.apache.qpid.protonj2.client.exceptions.ClientException;
import org.apache.qpid.protonj2.client.impl.ClientMessage;
import org.apache.qpid.protonj2.types.messaging.Section;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

public class MessageSerializerTest {
	private final ObjectMapper objectMapper;

	public MessageSerializerTest() {
		objectMapper = new ObjectMapper();
		SimpleModule module = new SimpleModule();
		module.addSerializer(Message.class, new MessageSerializer());
		module.addSerializer(Section.class, new AmqpSectionSerializer());
		objectMapper.registerModule(module);
	}

	@Test
	void byteArrayBody() throws ClientException, JsonProcessingException {
		Message<byte[]> message = ClientMessage.create();
		message.body("Hello world".getBytes(StandardCharsets.UTF_8));
		String expected = ",\"body\":"
				+ objectMapper.writeValueAsString("Hello world".getBytes(StandardCharsets.UTF_8));
		String actual = objectMapper.writeValueAsString(message);
		assertThat(actual).contains(expected);

	}

	@Test
	void stringBody() throws ClientException, JsonProcessingException {
		Message<String> message = ClientMessage.create();
		message.body("Hello world");
		String expected = ",\"body\":" + objectMapper.writeValueAsString("Hello world");
		String actual = objectMapper.writeValueAsString(message);
		assertThat(actual).contains(expected);

	}

	@Test
	void listBody() throws ClientException, JsonProcessingException {
		List<String> lst = List.of("Hello", "World");
		Message<List<String>> message = ClientMessage.create();
		message.body(lst);
		String expected = ",\"body\":" + objectMapper.writeValueAsString(lst);
		String actual = objectMapper.writeValueAsString(message);
		assertThat(actual).contains(expected);
	}

	@Test
	void jsonBody() throws ClientException, JsonProcessingException {
		ObjectNode node = objectMapper.createObjectNode();
		node.put("Hello", "hola").put("World", "la monde").set("inner",
				objectMapper.createObjectNode().put("one", "uno").put("two", "dos"));
		Message<ObjectNode> message = ClientMessage.create();
		message.body(node);
		String expected = ",\"body\":" + objectMapper.writeValueAsString(node);
		String actual = objectMapper.writeValueAsString(message);
		assertThat(actual).contains(expected);
	}
}
