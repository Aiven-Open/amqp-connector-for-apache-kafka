package io.aiven.kafka.connect.amqp.source;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

import de.huxhorn.sulky.ulid.ULID;
import io.aiven.commons.kafka.config.fragment.CommonConfigFragment;
import io.aiven.commons.kafka.connector.source.AbstractSourceIntegrationBase;
import io.aiven.commons.kafka.connector.source.OffsetManager;
import io.aiven.commons.kafka.connector.source.SourceStorage;
import io.aiven.commons.kafka.connector.source.config.SourceConfigFragment;
import io.aiven.kafka.connect.amqp.common.integration.IntegrationTestSetup;
import io.aiven.kafka.connect.amqp.source.extractor.AmqpExtractor;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.qpid.protonj2.client.Delivery;
import org.apache.qpid.protonj2.client.exceptions.ClientException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.rabbitmq.RabbitMQContainer;

@Testcontainers
public class AmqpTaskTestIT extends AbstractSourceIntegrationBase<ULID.Value, Delivery> {
  private static final Logger LOGGER = LoggerFactory.getLogger(AmqpTaskTestIT.class);
  private final AmqpSourceStorage sourceStorage;
  private AmqpSourceTask underTest;

  @Container RabbitMQContainer rabbit = IntegrationTestSetup.rabbitMQContainer();

  AmqpTaskTestIT() throws ClientException {
    rabbit.start();
    sourceStorage = new AmqpSourceStorage(rabbit);
  }

  @BeforeEach
  void beforeEach() {
    underTest = new AmqpSourceTask();
  }

  @AfterEach
  void afterEach() {
    underTest.stop();
    underTest = null;
  }

  @Override
  protected SourceStorage<ULID.Value, Delivery> getSourceStorage() {
    return sourceStorage;
  }

  @Test
  @Disabled("Not complete")
  void testMessageRead() throws IOException {
    String topic = getTopic();
    sourceStorage.setAmqpAddress("AMQP_" + topic);
    OffsetManager offsetManager = mock(OffsetManager.class);
    Map<String, String> props = new HashMap<>();

    Map<String, String> config = sourceStorage.createConnectorConfig();
    CommonConfigFragment.setter(config).maxTasks(1);
    SourceConfigFragment.setter(config)
        .extractorClass(AmqpExtractor.class)
        .targetTopic(topic)
        .ringBufferSize(0);

    LOGGER.info("{}", config);

    String body = "hello world";

    SourceStorage.WriteResult<ULID.Value> writeResult =
        write(topic, body.getBytes(StandardCharsets.UTF_8), 1);

    // Poll messages from the Kafka topic and verify the consumed data
    final List<String> records =
        messageConsumer().consumeStringMessages(topic, 4, Duration.ofSeconds(10));

    // Verify that the correct data is read from the S3 bucket and pushed to Kafka
    assertThat(records).containsOnly("hello world");
  }
}
