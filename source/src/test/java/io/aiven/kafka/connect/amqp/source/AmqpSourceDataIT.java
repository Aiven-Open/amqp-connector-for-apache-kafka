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
import static org.testcontainers.shaded.org.awaitility.Awaitility.await;

import io.aiven.commons.kafka.connector.source.OffsetManager;
import io.aiven.kafka.connect.amqp.common.integration.IntegrationTestSetup;
import io.aiven.kafka.connect.amqp.source.config.AmqpSourceConfig;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import org.apache.qpid.protonj2.client.Message;
import org.apache.qpid.protonj2.client.Tracker;
import org.apache.qpid.protonj2.client.exceptions.ClientException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.rabbitmq.RabbitMQContainer;

@Testcontainers
public class AmqpSourceDataIT {
  private static final Logger LOGGER = LoggerFactory.getLogger(AmqpSourceDataIT.class);
  private final AmqpSourceStorage sourceStorage;
  private AmqpSourceData underTest;

  @Container static RabbitMQContainer rabbit = IntegrationTestSetup.rabbitMQContainer();

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

  @Test
  void getNativeItemIteratorTest() throws ClientException {

    String topic = "getNativeItemIteratorTest";
    sourceStorage.setAmqpAddress("AMQP_" + topic);
    sourceStorage.createStorage();

    String body = "hello world";

    OffsetManager offsetManager = mock(OffsetManager.class);
    Map<String, String> props = sourceStorage.createConnectorConfig();
    AmqpSourceConfig amqpConfig = new AmqpSourceConfig(props);
    try {
      underTest = new AmqpSourceData(amqpConfig, offsetManager);
    } catch (ClientException | ExecutionException | InterruptedException e) {
      LOGGER.error("Unable to create AmqpSourceData: {}", e.getMessage(), e);
      throw new RuntimeException(e);
    }

    final Tracker tracker = sourceStorage.write(body.getBytes(StandardCharsets.UTF_8));
    await().atMost(Duration.ofSeconds(5)).until(tracker::remoteSettled);

    final Iterator[] iter = new Iterator[1];
    await()
        .atMost(Duration.ofSeconds(60))
        .until(
            () -> {
              iter[0] = underTest.getNativeItemIterator(null);
              return iter[0].hasNext();
            });
    AmqpSourceNativeInfo nativeInfo = (AmqpSourceNativeInfo) iter[0].next();
    assertThat(iter[0]).isExhausted();
    assertThat(nativeInfo).isNotNull();
    Message<Object> message = nativeInfo.getMessage();
    assertThat(message.body()).isEqualTo(body.getBytes(StandardCharsets.UTF_8));
  }

  @Test
  void multiRecordNativeItemIteratorTest() throws ClientException {

    String topic = "multiRecordNativeItemIteratorTest";
    sourceStorage.setAmqpAddress("AMQP_" + topic);
    sourceStorage.createStorage();

    OffsetManager offsetManager = mock(OffsetManager.class);
    Map<String, String> props = sourceStorage.createConnectorConfig();
    AmqpSourceConfig amqpConfig = new AmqpSourceConfig(props);
    try {
      underTest = new AmqpSourceData(amqpConfig, offsetManager);
    } catch (ClientException | ExecutionException | InterruptedException e) {
      LOGGER.error("Unable to create AmqpSourceData: {}", e.getMessage(), e);
      throw new RuntimeException(e);
    }

    List<byte[]> bodies = new ArrayList<>();
    List<Tracker> trackers = new ArrayList<Tracker>();

    for (int i = 0; i < 100; i++) {
      String body = "Hello world" + i;
      bodies.add(body.getBytes(StandardCharsets.UTF_8));
      trackers.add(sourceStorage.write(body.getBytes(StandardCharsets.UTF_8)));
    }

    trackers.forEach(
        tracker -> await().atMost(Duration.ofSeconds(5)).until(tracker::remoteSettled));
    List<byte[]> read = new ArrayList<>();

    final Iterator[] iter = new Iterator[1];
    await()
        .atMost(Duration.ofSeconds(60))
        .untilAsserted(
            () -> {
              iter[0] = underTest.getNativeItemIterator(null);
              while (iter[0].hasNext()) {
                AmqpSourceNativeInfo nativeInfo = (AmqpSourceNativeInfo) iter[0].next();
                Message<Object> message = nativeInfo.getMessage();
                read.add((byte[]) message.body());
              }
              assertThat(read).containsExactlyInAnyOrderElementsOf(bodies);
            });
  }
}
