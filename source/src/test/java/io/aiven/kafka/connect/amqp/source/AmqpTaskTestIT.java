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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testcontainers.shaded.org.awaitility.Awaitility.await;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import de.huxhorn.sulky.ulid.ULID;
import io.aiven.commons.kafka.config.fragment.CommonConfigFragment;
import io.aiven.commons.kafka.connector.source.SourceStorage;
import io.aiven.commons.kafka.connector.source.config.SourceConfigFragment;
import io.aiven.commons.kafka.testkit.KafkaIntegrationTestBase;
import io.aiven.kafka.connect.amqp.common.integration.IntegrationTestSetup;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTaskContext;
import org.apache.kafka.connect.storage.OffsetStorageReader;
import org.apache.qpid.protonj2.client.exceptions.ClientException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.rabbitmq.RabbitMQContainer;

@Testcontainers
public class AmqpTaskTestIT extends KafkaIntegrationTestBase {
  private static final Logger LOGGER = LoggerFactory.getLogger(AmqpTaskTestIT.class);
  private final AmqpSourceStorage sourceStorage;
  private AmqpSourceTask underTest;
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  @Container static RabbitMQContainer rabbit = IntegrationTestSetup.rabbitMQContainer();

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
  void testMessageRead() throws IOException, ExecutionException, InterruptedException {
    String topic = getTopic();
    sourceStorage.setAmqpAddress(topic + "-AMQP");
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

    SourceTaskContext context = mock(SourceTaskContext.class);
    when(context.offsetStorageReader()).thenReturn(mock(OffsetStorageReader.class));
    when(context.offsetStorageReader().offset(any(Map.class))).thenReturn(null);
    underTest.initialize(context);

    // Poll messages from the Kafka topic and verify the consumed data
    underTest.start(config);
    final List<SourceRecord> result = new ArrayList<>();
    await().atMost(Duration.ofSeconds(5)).until(() -> underTest.isRunning());

    await()
        .atMost(Duration.ofMinutes(2))
        .until(
            () -> {
              List<SourceRecord> newLst = underTest.poll();
              if (newLst != null) {
                result.addAll(newLst);
              }
              return newLst == null && result.size() == 1;
            });

    SourceRecord sourceRecord = result.get(0);
    Map<String, ?> partition = sourceRecord.sourcePartition();
    assertThat(partition.keySet()).containsExactly("ulid");
    Map<String, ?> offset = sourceRecord.sourceOffset();
    assertThat(offset.keySet()).containsExactly("ulid", "recordCount");
    assertThat(offset.get("ulid")).isEqualTo(partition.get("ulid"));
    assertThat(offset.get("recordCount")).isEqualTo(0);
    assertThat(sourceRecord.kafkaPartition()).isNull();
    assertThat(sourceRecord.keySchema()).isEqualTo(Schema.STRING_SCHEMA);
    assertThat(sourceRecord.key()).isEqualTo(partition.get("ulid"));
    assertThat(sourceRecord.timestamp()).isNull();
    assertThat(sourceRecord.valueSchema()).isEqualTo(Schema.STRING_SCHEMA);
    JsonNode node = OBJECT_MAPPER.readTree((String) sourceRecord.value());
    assertThat(node.get("messageId").asText()).isEqualTo(writeResult.getNativeKey().toString());
    assertThat(node.get("body").asText()).isEqualTo("aGVsbG8gd29ybGQ=");
  }
}
