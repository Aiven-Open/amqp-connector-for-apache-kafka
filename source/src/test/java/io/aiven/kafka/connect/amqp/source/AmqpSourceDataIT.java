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
import static org.mockito.Mockito.mock;

import de.huxhorn.sulky.ulid.ULID;
import io.aiven.commons.kafka.connector.source.AbstractSourceIntegrationBase;
import io.aiven.commons.kafka.connector.source.OffsetManager;
import io.aiven.commons.kafka.connector.source.SourceStorage;
import io.aiven.kafka.connect.amqp.common.integration.IntegrationTestSetup;
import io.aiven.kafka.connect.amqp.source.config.AmqpSourceConfig;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Iterator;
import java.util.Map;
import org.apache.qpid.protonj2.client.Delivery;
import org.apache.qpid.protonj2.client.exceptions.ClientException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.rabbitmq.RabbitMQContainer;
import org.testcontainers.shaded.org.awaitility.Awaitility;

@Testcontainers
public class AmqpSourceDataIT extends AbstractSourceIntegrationBase<ULID.Value, Delivery> {
  private static final Logger LOGGER = LoggerFactory.getLogger(AmqpSourceDataIT.class);
  private final AmqpSourceStorage sourceStorage;
  private AmqpSourceData underTest;

  @Container RabbitMQContainer rabbit = IntegrationTestSetup.rabbitMQContainer();

  AmqpSourceDataIT() throws ClientException {
    rabbit.start();
    sourceStorage = new AmqpSourceStorage(rabbit);
  }

  @AfterEach
  void afterEach() {
    try {
      underTest.close();
    } catch (Exception e) {
      LOGGER.error("Error closing AmqpSourceData: {}", e.getMessage(), e);
      if (e instanceof RuntimeException rE) {
        throw rE;
      }
      throw new RuntimeException(e);
    }
  }

  @Override
  protected SourceStorage<ULID.Value, Delivery> getSourceStorage() {
    return sourceStorage;
  }

  @Test
  void getNativeItemIteratorTest() throws ClientException {

    String topic = getTopic();
    sourceStorage.setAmqpAddress("AMQP_" + topic);
    sourceStorage.createStorage();

    String body = "hello world";

    OffsetManager offsetManager = mock(OffsetManager.class);
    Map<String, String> props = sourceStorage.createConnectorConfig();
    AmqpSourceConfig amqpConfig = new AmqpSourceConfig(props);
    try {
      underTest = new AmqpSourceData(amqpConfig, offsetManager);
    } catch (ClientException e) {
      LOGGER.error("Unable to create AmqpSourceData: {}", e.getMessage(), e);
      throw e;
    }

    Iterator<AmqpSourceNativeInfo> iter = underTest.getNativeItemIterator(null);
    SourceStorage.WriteResult<ULID.Value> writeResult =
        write(topic, body.getBytes(StandardCharsets.UTF_8), 1);

    Awaitility.await()
        .atMost(Duration.ofSeconds(5))
        .untilAsserted(() -> assertThat(iter).hasNext());
    AmqpSourceNativeInfo nativeInfo = iter.next();
    assertThat(iter).isExhausted();
  }
}
