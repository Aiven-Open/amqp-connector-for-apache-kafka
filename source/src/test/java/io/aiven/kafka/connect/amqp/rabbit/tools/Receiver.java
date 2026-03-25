package io.aiven.kafka.connect.amqp.rabbit.tools;

import io.aiven.kafka.connect.amqp.common.config.AmqpFragment;
import io.aiven.kafka.connect.amqp.source.AmqpSourceTask;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTaskContext;
import org.apache.kafka.connect.storage.OffsetStorageReader;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.awaitility.Awaitility.await;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.assertj.core.api.Assertions.assertThat;

@Disabled("Not a real test")
public class Receiver {
	Map<String, String> props;
	AmqpSourceTask task;
	Receiver() {
		SourceTaskContext context = mock(SourceTaskContext.class);
		when(context.offsetStorageReader()).thenReturn(mock(OffsetStorageReader.class));
		when(context.offsetStorageReader().offset(anyMap())).thenReturn(null);

		props = new HashMap<>();
		AmqpFragment.setter(props).setHost("localhost").setUser("guest").setPassword("guest").setPort(5672)
				.setAddress("hello");
		task = new AmqpSourceTask();
		task.initialize(context);
	}

	@Test
	void start() {
		task.start(props);
		final List<SourceRecord> result = new ArrayList<>();
		await().atMost(Duration.ofMinutes(5)).untilAsserted(() -> {
			List<SourceRecord> pollResult = task.poll();
			if (pollResult != null) {
				result.addAll(pollResult);
			}
			assertThat(result).isNotEmpty();
		});
		System.out.println(result.get(0));

	}
}
