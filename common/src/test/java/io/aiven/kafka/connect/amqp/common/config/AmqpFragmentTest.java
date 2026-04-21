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
package io.aiven.kafka.connect.amqp.common.config;

import static org.assertj.core.api.Assertions.assertThat;

import io.aiven.commons.kafka.config.fragment.FragmentDataAccess;
import io.aiven.kafka.connect.amqp.common.integration.IntegrationTestSetup;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.qpid.protonj2.client.Client;
import org.apache.qpid.protonj2.client.Connection;
import org.apache.qpid.protonj2.client.Delivery;
import org.apache.qpid.protonj2.client.Message;
import org.apache.qpid.protonj2.client.Receiver;
import org.apache.qpid.protonj2.client.Sender;
import org.apache.qpid.protonj2.client.exceptions.ClientException;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.rabbitmq.RabbitMQContainer;
import org.testcontainers.shaded.org.awaitility.Awaitility;

@Testcontainers
public class AmqpFragmentTest {

  private static final String AMQP_ADDRESS = "Test";
  @Container RabbitMQContainer rabbit = IntegrationTestSetup.rabbitMQContainer();

  private final AmqpFragment underTest;

  AmqpFragmentTest() {
    rabbit.start();
    Map<String, String> data = new HashMap<>();
    AmqpFragment.setter(data)
        .setHost(rabbit.getHost())
        .setPort(rabbit.getAmqpPort())
        .setAddress(AMQP_ADDRESS)
        .setUser("guest")
        .setPassword("guest");

    ConfigDef configDef = new ConfigDef();
    AmqpFragment.update(configDef);
    AbstractConfig config = new AbstractConfig(configDef, data) {};
    underTest = new AmqpFragment(FragmentDataAccess.from(config));
    assertThat(rabbit.isRunning());
  }

  @Test
  void verifyConfigRetrival() throws ClientException, ExecutionException, InterruptedException {
    try (Client client = underTest.getClient()) {
      assertThat(client).isNotNull();
      Connection connection = underTest.getConnection(client);
      assertThat(connection).isNotNull();
      Receiver receiver = underTest.getReceiver(connection);
      assertThat(receiver).isNotNull();
      assertThat(receiver.address()).isEqualTo(AMQP_ADDRESS);
    }
  }

  @Test
  void verifySendReceive() throws ClientException, ExecutionException, InterruptedException {
    try (Client client = underTest.getClient();
        Connection connection = underTest.getConnection(client);
        Receiver receiver = underTest.getReceiver(connection)) {
      Sender sender = connection.openSender(AMQP_ADDRESS);

      assertThat(receiver).isNotNull();
      assertThat(receiver.address()).isEqualTo(AMQP_ADDRESS);

      assertThat(sender).isNotNull();
      Message<String> message = Message.create("hello world");
      sender.send(message);

      Awaitility.await()
          .atMost(Duration.ofSeconds(2))
          .untilAsserted(() -> assertThat(receiver.queuedDeliveries()).isEqualTo(1));
      Delivery delivery = receiver.receive();
      Message<String> receivedMsg = delivery.message();
      assertThat(receivedMsg.body()).isEqualTo("hello world");
    }
  }
}
