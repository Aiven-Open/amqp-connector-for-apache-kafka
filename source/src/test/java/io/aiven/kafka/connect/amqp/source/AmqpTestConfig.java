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
import io.aiven.commons.kafka.connector.source.AbstractSourceIntegrationBase;
import io.aiven.commons.kafka.connector.source.ConsumerPropertiesBuilder;
import io.aiven.commons.kafka.connector.source.SourceStorage;
import io.aiven.commons.kafka.connector.source.TestConfig;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.BytesDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.connect.converters.ByteArrayConverter;
import org.apache.kafka.connect.storage.StringConverter;

public class AmqpTestConfig extends TestConfig {
  private final AmqpSourceStorage sourceStorage;
  private final Supplier<String> bootstrapServers;
  private final ULID ulid = new ULID();

  /** Constructor. */
  protected AmqpTestConfig(AmqpSourceStorage sourceStorage, Supplier<String> bootstrapServers) {
    super("AMQP standard test");
    this.sourceStorage = sourceStorage;
    this.bootstrapServers = bootstrapServers;
  }

  @Override
  public Map<String, String> consumerConfiguration() {
    return initialConfig();
  }

  @Override
  public Map<String, String> initialConfig() {
    return CommonConfigFragment.setter(sourceStorage.getAMQPInitialConfig())
        .keyConverter(StringConverter.class.getName())
        .valueConverter(ByteArrayConverter.class.getName())
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
      result.add(sourceStorage.writeWithKey(ulid.nextULID(), (byte[]) td.data()));
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

    List<ConsumerRecord<String, Bytes>> result =
        messageConsumer
            .consumeMessages(
                topic,
                new ConsumerPropertiesBuilder(bootstrapServers.get()),
                testData.size(),
                timeout,
                StringDeserializer.class,
                BytesDeserializer.class)
            .toList();

    List<String> expected =
        testData.stream()
            .map(SourceStorage.TestData::expected)
            .map(o -> o == null ? null : new String((byte[]) o, StandardCharsets.UTF_8))
            .toList();

    Object[] actualValue = result.stream().map(ConsumerRecord::value).toArray();
    Object[] actualKey = result.stream().map(ConsumerRecord::key).toArray();
    Object[] actualMsgId =
        result.stream()
            .map(cr -> new String(cr.headers().lastHeader("amqp.messageId").value()))
            .toArray();

    Object[] expectedValue =
        testData.stream()
            .map(td -> td.expected() == null ? null : new Bytes((byte[]) td.expected()))
            .toArray();
    Object[] expectedKey = writeResult.stream().map(SourceStorage.WriteResult::nativeKey).toArray();

    assertThat(actualValue).containsExactlyInAnyOrder(expectedValue);
    assertThat(actualKey).containsExactlyInAnyOrder(expectedKey);
    assertThat(actualMsgId).containsExactlyInAnyOrder(expectedKey);
  }
}
