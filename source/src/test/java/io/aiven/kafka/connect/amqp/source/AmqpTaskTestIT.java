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

import de.huxhorn.sulky.ulid.ULID;
import io.aiven.commons.kafka.config.fragment.CommonConfigFragment;
import io.aiven.commons.kafka.connector.source.SourceStorage;
import io.aiven.commons.kafka.connector.source.config.SourceConfigFragment;
import io.aiven.kafka.connect.amqp.common.integration.IntegrationTestSetup;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.qpid.protonj2.client.exceptions.ClientException;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.rabbitmq.RabbitMQContainer;

@Testcontainers
public class AmqpTaskTestIT {
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

  @Test
  void testMessageRead() throws IOException {
    String topic = "testMessageRead";
    sourceStorage.setAmqpAddress("AMQP_" + topic);
    sourceStorage.createStorage();

    Map<String, String> config = sourceStorage.createConnectorConfig();
    CommonConfigFragment.setter(config).maxTasks(1);
    SourceConfigFragment.setter(config).targetTopic(topic);

    LOGGER.info("{}", config);

    String body = "hello world";

    ULID ulid = new ULID();
    SourceStorage.WriteResult<ULID.Value> writeResult =
        sourceStorage.writeWithKey(ulid.nextValue(), body.getBytes(StandardCharsets.UTF_8));

    assertThat(writeResult).isNotNull();
    // Poll messages from the Kafka topic and verify the consumed data
    underTest.start(config);
    List<SourceRecord> result = new ArrayList<>();

    Awaitility.await()
        .atMost(Duration.ofMinutes(2))
        .until(
            () -> {
              List<SourceRecord> newLst = underTest.poll();
              if (newLst != null) {
                result.addAll(newLst);
              }
              return newLst == null && result.size() == 1;
            });

    System.out.println(result);
  }
}
