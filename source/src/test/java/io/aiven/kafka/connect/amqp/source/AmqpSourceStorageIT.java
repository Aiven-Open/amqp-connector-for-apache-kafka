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
import static org.testcontainers.shaded.org.awaitility.Awaitility.await;

import de.huxhorn.sulky.ulid.ULID;
import io.aiven.commons.kafka.connector.common.NativeInfo;
import io.aiven.kafka.connect.amqp.common.integration.IntegrationTestSetup;
import io.aiven.kafka.connect.amqp.source.config.AmqpSourceConfig;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import org.apache.qpid.protonj2.client.Delivery;
import org.apache.qpid.protonj2.client.Message;
import org.apache.qpid.protonj2.client.Receiver;
import org.apache.qpid.protonj2.client.exceptions.ClientException;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.rabbitmq.RabbitMQContainer;

@Testcontainers
public class AmqpSourceStorageIT {
  private static final Logger LOGGER = LoggerFactory.getLogger(AmqpSourceStorageIT.class);
  private AmqpSourceStorage underTest;

  @Container RabbitMQContainer rabbit = IntegrationTestSetup.rabbitMQContainer();

  AmqpSourceStorageIT() throws ClientException {
    rabbit.start();
    underTest = new AmqpSourceStorage(rabbit);
  }

  @Test
  void sourceStorageTest() throws ClientException {
    ULID ulid = new ULID();
    underTest.setAmqpAddress("AMQP_" + "storageTest");
    underTest.createStorage();
    underTest.writeWithKey(ulid.nextValue(), "Hello world".getBytes(StandardCharsets.UTF_8));
    final List<NativeInfo<ULID.Value, Delivery>> lst = new ArrayList<>();
    await()
        .atMost(Duration.ofSeconds(5))
        .until(
            () -> {
              lst.addAll(underTest.getNativeInfo());
              return !lst.isEmpty();
            });
    assertThat(lst).hasSize(1);
    Delivery delivery = lst.get(0).nativeItem();
    Message<?> message = delivery.message();
    assertThat(message.body()).isEqualTo("Hello world".getBytes(StandardCharsets.UTF_8));
  }

  @Test
  void sourceStorageTestWithConfigGenClientTest()
      throws ClientException, ExecutionException, InterruptedException {
    ULID ulid = new ULID();
    underTest.setAmqpAddress("AMQP_" + "storageTestWithClient");
    underTest.createStorage();

    Map<String, String> props = underTest.createConnectorConfig();
    AmqpSourceConfig config = new AmqpSourceConfig(props);

    Receiver receiver = config.getReceiver();

    underTest.writeWithKey(ulid.nextValue(), "Hello world".getBytes(StandardCharsets.UTF_8));

    await().atMost(Duration.ofSeconds(5)).until(() -> receiver.queuedDeliveries() > 0);
    Delivery delivery = receiver.receive();
    Message<?> message = delivery.message();
    assertThat(message.body()).isEqualTo("Hello world".getBytes(StandardCharsets.UTF_8));
  }
}
