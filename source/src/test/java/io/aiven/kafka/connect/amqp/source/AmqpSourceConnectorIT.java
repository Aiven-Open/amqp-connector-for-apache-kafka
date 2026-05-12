/*
        Copyright 2026 Aiven Oy and project contributors

       Licensed under the Apache License, Version 2.0 (the "License");
       you may not use this file except in compliance with the License.
       You may obtain a copy of the License at

       https://www.apache.org/licenses/LICENSE-2.0

       Unless required by applicable law or agreed to in writing,
       software distributed under the License is distributed on an
       "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
       KIND, either express or implied.  See the License for the
       specific language governing permissions and limitations
       under the License.

       SPDX-License-Identifier: Apache-2.0
*/
package io.aiven.kafka.connect.amqp.source;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import de.huxhorn.sulky.ulid.ULID;
import io.aiven.commons.kafka.config.fragment.CommonConfigFragment;
import io.aiven.commons.kafka.connector.source.AbstractSourceIntegrationBase;
import io.aiven.commons.kafka.connector.source.SourceStorage;
import io.aiven.commons.kafka.connector.source.config.SourceConfigFragment;
import io.aiven.commons.kafka.testkit.KafkaManager;
import io.aiven.kafka.connect.amqp.common.integration.IntegrationTestSetup;
import io.aiven.kafka.connect.amqp.source.extractor.AmqpExtractor;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import org.apache.kafka.connect.runtime.ConnectorConfig;
import org.apache.kafka.connect.storage.StringConverter;
import org.apache.qpid.protonj2.client.Delivery;
import org.apache.qpid.protonj2.client.exceptions.ClientException;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.rabbitmq.RabbitMQContainer;

@Testcontainers
public class AmqpSourceConnectorIT extends AbstractSourceIntegrationBase<ULID.Value, Delivery> {
  private static final Logger LOGGER = LoggerFactory.getLogger(AmqpSourceConnectorIT.class);
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
  private final AmqpSourceStorage sourceStorage;

  @Container RabbitMQContainer rabbit = IntegrationTestSetup.rabbitMQContainer();

  AmqpSourceConnectorIT() throws ClientException {
    rabbit.start();
    sourceStorage = new AmqpSourceStorage(rabbit);
  }

  @Override
  protected SourceStorage<ULID.Value, Delivery> getSourceStorage() {
    return sourceStorage;
  }

  @Test
  void testMessageRead() throws IOException {
    String topic = getTopic();
    sourceStorage.setAmqpAddress("AMQP_" + topic);

    final Map<String, String> workerConfigOverrides =
        Map.of(
            ConnectorConfig.KEY_CONVERTER_CLASS_CONFIG, StringConverter.class.getName(),
            ConnectorConfig.VALUE_CONVERTER_CLASS_CONFIG, StringConverter.class.getName());
    KafkaManager kafkaManager = setupKafka(workerConfigOverrides);
    kafkaManager.createTopic(topic);

    Map<String, String> config = sourceStorage.createConnectorConfig();
    CommonConfigFragment.setter(config).maxTasks(1);
    SourceConfigFragment.setter(config)
        .extractorClass(AmqpExtractor.class)
        .targetTopic(topic)
        .ringBufferSize(1);

    LOGGER.info("{}", config);

    kafkaManager.configureConnector(getTopic(), config);

    String body = "hello world";

    SourceStorage.WriteResult<ULID.Value> writeResult =
        write(topic, body.getBytes(StandardCharsets.UTF_8), 1);

    // Poll messages from the Kafka topic and verify the consumed data
    final List<String> records =
        messageConsumer().consumeStringMessages(topic, 1, Duration.ofSeconds(10));

    // Verify that the AMQP payload reaches Kafka in the serialized envelope format
    // and the embedded base64 body decodes to the original message bytes.
    assertThat(records).hasSize(1);
    final JsonNode payload = OBJECT_MAPPER.readTree(records.get(0));
    final String bodyBase64 = payload.path("body").asText();
    final String decodedBody =
        new String(Base64.getDecoder().decode(bodyBase64), StandardCharsets.UTF_8);
    assertThat(decodedBody).isEqualTo(body);
    assertThat(writeResult).isNotNull();
  }
}
