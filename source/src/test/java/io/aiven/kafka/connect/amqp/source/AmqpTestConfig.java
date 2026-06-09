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
import de.huxhorn.sulky.ulid.ULID;
import io.aiven.commons.kafka.config.fragment.CommonConfigFragment;
import io.aiven.commons.kafka.connector.source.AbstractSourceIntegrationBase;
import io.aiven.commons.kafka.connector.source.SourceStorage;
import io.aiven.commons.kafka.connector.source.TestConfig;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import org.apache.kafka.connect.storage.StringConverter;

public class AmqpTestConfig extends TestConfig {
  private final AmqpSourceStorage sourceStorage;
  private final ULID ulid = new ULID();

  /** Constructor. */
  protected AmqpTestConfig(AmqpSourceStorage sourceStorage) {
    super("AMQP standard test");
    this.sourceStorage = sourceStorage;
  }

  @Override
  public Map<String, String> consumerConfiguration() {
    return initialConfig();
  }

  @Override
  public Map<String, String> initialConfig() {
    return CommonConfigFragment.setter(sourceStorage.getAMQPInitialConfig())
        .keyConverter(StringConverter.class.getName())
        .valueConverter(StringConverter.class.getName())
        .data();
  }

  @Override
  public List<SourceStorage.TestData> getTestData(int count) {

    String body = "hello world #";

    List<SourceStorage.TestData> result = new ArrayList<>();
    for (int i = 0; i < count; i++) {
      byte[] data = (body + i).getBytes(StandardCharsets.UTF_8);
      result.add(new SourceStorage.TestData(data, data));
    }
    return result;
  }

  @Override
  public List<SourceStorage.WriteResult> writeTestData(
      String topic, List<SourceStorage.TestData> data) {
    sourceStorage.setAmqpAddress(topic);
    List<SourceStorage.WriteResult> result = new ArrayList<>();
    for (SourceStorage.TestData td : data) {
      result.add(sourceStorage.writeWithKey(ulid.nextValue(), (byte[]) td.data()));
    }
    return result;
  }

  @Override
  public void consumeMessages(
      AbstractSourceIntegrationBase.MessageConsumer messageConsumer,
      String topic,
      List<SourceStorage.TestData> testData,
      List<SourceStorage.WriteResult> writeResult,
      Duration timeout) {

    List<JsonNode> result = messageConsumer.consumeJsonMessages(topic, testData.size(), timeout);

    List<String> expected =
        testData.stream()
            .map(SourceStorage.TestData::expected)
            .map(o -> o == null ? null : new String((byte[]) o, StandardCharsets.UTF_8))
            .toList();

    String actual;
    // order is not guaranteed
    for (int i = 0; i < result.size(); i++) {
      JsonNode node = result.get(i);
      if (node.path("body").isNull()) {
        actual = null;
      } else {
        final String bodyBase64 = node.path("body").asText();
        final byte[] decodedBody = Base64.getDecoder().decode(bodyBase64);
        actual = new String(decodedBody, StandardCharsets.UTF_8);
      }
      assertThat(actual).isIn(expected).as(node.get("messageId").asText());
    }
  }
}
