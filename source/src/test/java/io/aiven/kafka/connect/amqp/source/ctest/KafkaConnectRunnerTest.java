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
package io.aiven.kafka.connect.amqp.source.ctest;

import static org.assertj.core.api.Assertions.assertThat;

import io.aiven.commons.kafka.testkit.KafkaConnectRunner;
import java.io.IOException;
import java.time.Duration;
import java.util.Collections;
import java.util.Map;
import org.apache.kafka.connect.runtime.SinkConnectorConfig;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class KafkaConnectRunnerTest {

  private static KafkaConnectRunner underTest = new KafkaConnectRunner(Duration.ofSeconds(2));

  @BeforeAll
  static void beforeAll() throws IOException {
    underTest.startConnectCluster("KafkaConnectRunnerTest", Collections.emptyMap());
  }

  @AfterAll
  static void afterAll() throws IOException {
    underTest.stopConnectCluster();
  }

  @Test
  void simpleRetrievalTests() {
    assertThat(underTest.listWorkers()).as("list workers").isNotEmpty();
    assertThat(underTest.getClusterName()).as("cluster name").isEqualTo("KafkaConnectRunnerTest");
    assertThat(underTest.getOffsetTopic())
        .as("offset topic")
        .isEqualTo("connect-offset-topic-KafkaConnectRunnerTest");
    assertThat(underTest.getConfigTopic())
        .as("config topic")
        .isEqualTo("connect-config-topic-KafkaConnectRunnerTest");
    assertThat(underTest.getStorageTopic())
        .as("storage topic")
        .isEqualTo("connect-storage-topic-KafkaConnectRunnerTest");
    assertThat(underTest.getStorageTopic())
        .as("group id")
        .isEqualTo("connect-storage-topic-KafkaConnectRunnerTest");
    assertThat(underTest.getContainerPort()).as("container port").isGreaterThan(0);
    assertThat(underTest.getBootstrapServers()).as("bootstrap servers").isNotBlank();
  }

  void assertConnectorCounters(int config, int start, int stop) {
    assertThat(DummyConnector.configCount.get()).as("config count").isEqualTo(config);
    assertThat(DummyConnector.startCount.get()).as("start count").isEqualTo(start);
    assertThat(DummyConnector.stopCount.get()).as("stop count").isEqualTo(stop);
  }

  @Test
  void connectorControlTests() {
    Map<String, String> connectorConfig =
        underTest.getWorkerProperties(DummyConnector.class, Collections.emptyMap());
    connectorConfig.put(SinkConnectorConfig.TOPICS_CONFIG, "dummyTopic");

    assertConnectorCounters(0, 0, 0);
    assertThat(underTest.hasConnector("testConnector")).isFalse();

    String result = underTest.configureConnector("testConnector", connectorConfig);
    assertThat(underTest.hasConnector("testConnector")).isTrue();
    Awaitility.await()
        .atMost(Duration.ofSeconds(2))
        .untilAsserted(() -> assertConnectorCounters(1, 1, 0));
    assertThat(underTest.connectorStatus("testConnector").connector().state()).isEqualTo("RUNNING");

    underTest.restartConnector("testConnector");
    assertThat(underTest.hasConnector("testConnector")).isTrue();
    Awaitility.await()
        .atMost(Duration.ofSeconds(2))
        .untilAsserted(() -> assertConnectorCounters(2, 2, 1));
    assertThat(underTest.connectorStatus("testConnector").connector().state()).isEqualTo("RUNNING");

    underTest.pauseConnector("testConnector");
    assertThat(underTest.hasConnector("testConnector")).isTrue();
    Awaitility.await()
        .atMost(Duration.ofSeconds(2))
        .untilAsserted(() -> assertConnectorCounters(2, 2, 2));
    assertThat(underTest.connectorStatus("testConnector").connector().state()).isEqualTo("PAUSED");

    underTest.resumeConnector("testConnector");
    assertThat(underTest.hasConnector("testConnector")).isTrue();
    Awaitility.await()
        .atMost(Duration.ofSeconds(2))
        .untilAsserted(() -> assertConnectorCounters(3, 3, 2));
    assertThat(underTest.connectorStatus("testConnector").connector().state()).isEqualTo("RUNNING");

    underTest.deleteConnector("testConnector");
    assertThat(underTest.hasConnector("testConnector")).isFalse();
    Awaitility.await()
        .atMost(Duration.ofSeconds(2))
        .untilAsserted(() -> assertConnectorCounters(3, 3, 3));
  }
}
