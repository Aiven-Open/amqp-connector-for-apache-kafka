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

import de.huxhorn.sulky.ulid.ULID;
import io.aiven.commons.kafka.connector.source.AbstractSourceConnectorIntegrationTest;
import io.aiven.commons.kafka.connector.source.SourceStorage;
import io.aiven.commons.kafka.connector.source.TestConfig;
import io.aiven.kafka.connect.amqp.common.integration.IntegrationTestSetup;
import org.apache.qpid.protonj2.client.Delivery;
import org.apache.qpid.protonj2.client.exceptions.ClientException;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.rabbitmq.RabbitMQContainer;

@Testcontainers
public class AmqpSourceConnectorIT
    extends AbstractSourceConnectorIntegrationTest<ULID.Value, Delivery> {

  private final AmqpSourceStorage sourceStorage;

  @Container static RabbitMQContainer rabbit = IntegrationTestSetup.rabbitMQContainer();

  AmqpSourceConnectorIT() throws ClientException {
    super();
    rabbit.start();
    sourceStorage = new AmqpSourceStorage(rabbit);
  }

  @Override
  protected TestConfig getTestConfig() {
    return new AmqpTestConfig(sourceStorage);
  }

  @Override
  protected SourceStorage<ULID.Value, Delivery> getSourceStorage() {
    return sourceStorage;
  }

  //  @Test
  //  void testMessageRead() throws IOException, ExecutionException, InterruptedException {
  //    String topic = getTopic();
  //    sourceStorage.setAmqpAddress("AMQP_" + topic);
  //
  //    final Map<String, String> workerConfigOverrides =
  //        Map.of(
  //            ConnectorConfig.KEY_CONVERTER_CLASS_CONFIG, StringConverter.class.getName(),
  //            ConnectorConfig.VALUE_CONVERTER_CLASS_CONFIG, StringConverter.class.getName());
  //    KafkaManager kafkaManager = setupKafka(workerConfigOverrides);
  //    kafkaManager.createTopic(topic);
  //
  //    Map<String, String> config = sourceStorage.createConnectorConfig();
  //    CommonConfigFragment.setter(config).maxTasks(1);
  //    SourceConfigFragment.setter(config)
  //        .extractorClass(AmqpExtractor.class)
  //        .targetTopic(topic)
  //        .ringBufferSize(1);
  //
  //    LOGGER.info("{}", config);
  //
  //    kafkaManager.configureConnector(getTopic(), config);
  //
  //    String body = "hello world";
  //
  //    SourceStorage.WriteResult writeResult = write(topic, body.getBytes(StandardCharsets.UTF_8),
  // 1);
  //
  //    // Poll messages from the Kafka topic and verify the consumed data
  //    final List<String> records =
  //        messageConsumer().consumeStringMessages(topic, 1, Duration.ofSeconds(10));
  //
  //    // Verify that the AMQP payload reaches Kafka in the serialized envelope format
  //    // and the embedded base64 body decodes to the original message bytes.
  //    assertThat(records).hasSize(1);
  //    final JsonNode payload = OBJECT_MAPPER.readTree(records.get(0));
  //    final String bodyBase64 = payload.path("body").asText();
  //    final String decodedBody =
  //        new String(Base64.getDecoder().decode(bodyBase64), StandardCharsets.UTF_8);
  //    assertThat(decodedBody).isEqualTo(body);
  //    assertThat(writeResult).isNotNull();
  //  }
}
