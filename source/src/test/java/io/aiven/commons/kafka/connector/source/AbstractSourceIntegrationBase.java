
/*
 * Copyright 2026 Aiven Oy
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.aiven.commons.kafka.connector.source;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.aiven.commons.kafka.config.fragment.CommonConfigFragment;
import io.aiven.commons.kafka.connector.common.NativeInfo;
import io.aiven.commons.kafka.connector.source.config.SourceConfigFragment;
import io.aiven.commons.kafka.connector.source.extractor.Extractor;
import io.aiven.commons.kafka.testkit.KafkaIntegrationTestBase;
import io.aiven.commons.kafka.testkit.KafkaManager;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import java.io.IOException;
import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Stream;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.connect.connector.Connector;
import org.apache.kafka.connect.json.JsonDeserializer;
import org.assertj.core.api.Assertions;
import org.awaitility.Awaitility;

/**
 * The base abstract case for Kafka based integration tests.
 *
 * <p>This class handles the creation and destruction of a thread safe {@link KafkaManager}.
 *
 * @param <K> the native key type.
 * @param <N> the native object type
 */
public abstract class AbstractSourceIntegrationBase<K extends Comparable<K>, N>
    extends KafkaIntegrationTestBase {

  protected AbstractSourceIntegrationBase() {}

  protected String getLogPrefix() {
    return String.format("%s %s: ", this.getClass().getSimpleName(), this.getConnectorName());
  }

  /**
   * Create the SourceStorage. Should create the source storage once and then return it.
   *
   * @return the current SourceStorage object.
   */
  protected abstract SourceStorage<K, N> getSourceStorage();

  /**
   * Creates the native key.
   *
   * @param topic the topic for the key,
   * @param partition the partition for the key.
   * @return the native Key.
   */
  protected final K createKey(final String topic, final int partition) {
    return getSourceStorage().createKey(topic, partition);
  }

  /**
   * Write file to native storage with the specified key and data.
   *
   * @param nativeKey the key.
   * @param testDataBytes the data.
   */
  protected final SourceStorage.WriteResult<K> writeWithKey(
      final K nativeKey, final byte[] testDataBytes) {
    return getSourceStorage().writeWithKey(nativeKey, testDataBytes);
  }

  /**
   * Retrieves a list of {@link NativeInfo} implementations, one for each item in native storage.
   *
   * @return the list of {@link NativeInfo} implementations, one for each item in native storage.
   */
  protected final List<? extends NativeInfo<K, N>> getNativeInfo() {
    return getSourceStorage().getNativeInfo();
  }

  /**
   * The Connector class under test.
   *
   * @return the connector class under test.
   */
  protected final Class<? extends Connector> getConnectorClass() {
    return getSourceStorage().getConnectorClass();
  }

  /**
   * Gets the name of the current connector.
   *
   * @return the name of the connector.
   */
  protected final String getConnectorName() {
    return getConnectorName(getConnectorClass());
  }

  /**
   * Creates the configuration data for the connector.
   *
   * @return the configuration data for the Connector class under test.
   */
  protected final Map<String, String> createConfig() {
    Map<String, String> props = getSourceStorage().createConnectorConfig();
    CommonConfigFragment.setter(props).maxTasks(1);
    return props;
  }

  protected final Map<String, String> createConfig(
      String topic, Class<? extends Extractor> extractorClass) {
    Map<String, String> props = createConfig();
    SourceConfigFragment.setter(props).targetTopic(topic).extractorClass(extractorClass);
    return props;
  }

  /**
   * Gets the default offset flush interval.
   *
   * @return the default offset flush interval.
   */
  @Override
  protected Duration getOffsetFlushInterval() {
    return Duration.ofSeconds(5);
  }

  /** Delete the current connector from the running kafka. */
  protected final void deleteConnector() {
    deleteConnector(getConnectorClass());
  }

  /**
   * Returns a BiFunction that converts OffsetManager key and data into an OffsetManagerEntry for
   * this system.
   *
   * <ul>
   *   <li>The first argument to the method is the {@link
   *       OffsetManager.OffsetManagerEntry#getManagerKey()} value.
   *   <li>The second argument is the {@link OffsetManager.OffsetManagerEntry#getProperties()}
   *       value.
   *   <li>Method should return a proper {@link OffsetManager.OffsetManagerEntry}
   * </ul>
   *
   * @return A BiFunction that crates an OffsetManagerEntry.
   */
  protected final BiFunction<
          Map<String, Object>, Map<String, Object>, OffsetManager.OffsetManagerEntry>
      offsetManagerEntryFactory() {
    return getSourceStorage().offsetManagerEntryFactory();
  }

  /**
   * Creates a MessageConsumer for this test environment.
   *
   * @return a MessageConsumer instance.
   */
  protected final MessageConsumer messageConsumer() {
    return new MessageConsumer();
  }

  /**
   * Creates a ConsumerPropertiesBuilder configured with the proper bootstrap server.
   *
   * @return a ConsumerPropertiesBuilder configured with the proper bootstrap server.
   */
  protected final ConsumerPropertiesBuilder consumerPropertiesBuilder() {
    return new ConsumerPropertiesBuilder(getKafkaManager().bootstrapServers());
  }

  /**
   * Writes to the underlying storage. Does not use a prefix. Equivalent to calling {@code
   * write(topic, testDataBytes, partition, null)}
   *
   * @param topic the topic for the file.
   * @param testDataBytes the data.
   * @param partition the partition id fo the file.
   * @return the WriteResult for the operation.
   */
  protected final SourceStorage.WriteResult<K> write(
      final String topic, final byte[] testDataBytes, final int partition) {
    final K objectKey = createKey(topic, partition);
    return writeWithKey(objectKey, testDataBytes);
  }

  /**
   * Get the topic from the TestInfo.
   *
   * @return The topic extracted from the testInfo for the current test.
   */
  public String getTopic() {
    return testInfo.getTestMethod().map(Method::getName).orElse("noMethod");
  }

  /** Handles reading messages from the local kafka. */
  public class MessageConsumer {
    /** constructor. use {@link AbstractSourceIntegrationBase#messageConsumer()} */
    private MessageConsumer() {}

    public static final Function<byte[], String> byteToString =
        b -> new String(b, StandardCharsets.UTF_8);

    /**
     * Read the data from the topic as byte array key and byte value. Each value is converted into a
     * string and returned in the result. If the expected number of messages is not read in the
     * allotted time the test fails.
     *
     * @param topic the topic to red.
     * @param expectedMessageCount the expected number of messages.
     * @param timeout the maximum time to wait for the messages to arrive.
     * @return A list of values returned.
     */
    public List<byte[]> consumeByteMessages(
        final String topic, final int expectedMessageCount, final Duration timeout) {
      return consumeMessages(
              topic,
              consumerPropertiesBuilder(),
              expectedMessageCount,
              timeout,
              ByteArrayDeserializer.class,
              ByteArrayDeserializer.class)
          .map(ConsumerRecord::value)
          .toList();
    }

    /**
     * Read the data from the topic as byte array key and byte value. Each value is converted into a
     * string and returned in the result. If the expected number of messages is not read in the
     * allotted time the test fails.
     *
     * @param topic the topic to red.
     * @param expectedMessageCount the expected number of messages.
     * @param timeout the maximum time to wait for the messages to arrive.
     * @return A list of values returned.
     */
    public List<String> consumeStringMessages(
        final String topic, final int expectedMessageCount, final Duration timeout) {
      return consumeMessages(
              topic,
              consumerPropertiesBuilder(),
              expectedMessageCount,
              timeout,
              ByteArrayDeserializer.class,
              StringDeserializer.class)
          .map(ConsumerRecord::value)
          .toList();
    }

    /**
     * Read the data from the topic as Avro data the key is read as a string. This consumer uses the
     * {@link KafkaAvroDeserializer} to deserialize the values. The schema registry URL is the url
     * provided by the local KafkaManager. If the expected number of messages is not read in the
     * allotted time the test fails.
     *
     * @param topic the topic to red.
     * @param expectedMessageCount the expected number of messages.
     * @param timeout the maximum time to wait for the messages to arrive.
     * @return A list of values returned.
     */
    public List<GenericRecord> consumeAvroMessages(
        final String topic, final int expectedMessageCount, final Duration timeout) {
      return consumeMessages(
              topic,
              consumerPropertiesBuilder().schemaRegistry(getKafkaManager().getSchemaRegistryUrl()),
              expectedMessageCount,
              timeout,
              StringDeserializer.class,
              KafkaAvroDeserializer.class)
          .map(cr -> (GenericRecord) (cr.value()))
          .toList();
    }

    /**
     * Read the data from the topic as JSONL data the key is read as a string. This consumer uses
     * the {@link JsonDeserializer} to deserialize the values. If the expected number of messages is
     * not read in the allotted time the test fails.
     *
     * @param topic the topic to red.
     * @param expectedMessageCount the expected number of messages.
     * @param timeout the maximum time to wait for the messages to arrive.
     * @return A list of values returned.
     */
    public List<JsonNode> consumeJsonMessages(
        final String topic, final int expectedMessageCount, final Duration timeout) {
      return consumeMessages(
              topic,
              consumerPropertiesBuilder(),
              expectedMessageCount,
              timeout,
              StringDeserializer.class,
              JsonDeserializer.class)
          .map(ConsumerRecord::value)
          .toList();
    }

    /**
     * Read the data and key values from the topic. Teh consumer properties should be created with a
     * call to {@link #consumerPropertiesBuilder}. If the expected number of messages is not read in
     * the allotted time the test fails.
     *
     * @param topic the topic to red.
     * @param consumerPropertiesBuilder The consumer properties builder.
     * @param expectedMessageCount the expected number of messages.
     * @param timeout the maximum time to wait for the messages to arrive.
     * @param keyClass the key deserialization class.
     * @param valueClass the value deserialization class.
     * @param <X> The object returned from the key serializer.
     * @param <V> The object returned from the value serializer.
     * @return A list of values returned.
     */
    public <X, V> Stream<ConsumerRecord<X, V>> consumeMessages(
        final String topic,
        final ConsumerPropertiesBuilder consumerPropertiesBuilder,
        final int expectedMessageCount,
        final Duration timeout,
        final Class<? extends Deserializer<X>> keyClass,
        final Class<? extends Deserializer<V>> valueClass) {
      System.out.println("Consuming messages from topic: " + topic);
      try (KafkaConsumer<X, V> consumer =
          new KafkaConsumer<>(
              consumerPropertiesBuilder
                  .keyDeserializer(keyClass)
                  .valueDeserializer(valueClass)
                  .build())) {
        consumer.subscribe(Collections.singletonList(topic));
        System.out.println(consumer.subscription());
        final List<ConsumerRecord<X, V>> recordValues = new ArrayList<>();
        Awaitility.await()
            .atMost(timeout)
            .pollInterval(Duration.ofSeconds(1))
            .untilAsserted(
                () -> {
                  Assertions.assertThat(consumeRecordsInProgress(consumer, recordValues))
                      .hasSize(expectedMessageCount);
                });
        return recordValues.stream();
      }
    }

    /**
     * Consumes records in blocks and appends them to the {@code recordValues} parameter. This
     * method polls the consumer for 1/2 second and adds the result to the record values. As long as
     * there are at more than 10 records returned it continues to poll. Once there are fewer than 10
     * records returned this method returns.
     *
     * @param consumer The consumer to read from.
     * @param recordValues the record values to append to.
     * @return {@code recordValues}
     */
    private <X, V> List<ConsumerRecord<X, V>> consumeRecordsInProgress(
        final KafkaConsumer<X, V> consumer, final List<ConsumerRecord<X, V>> recordValues) {
      int recordsRetrieved;
      do {
        final ConsumerRecords<X, V> records = consumer.poll(Duration.ofMillis(500L));
        recordsRetrieved = records.count();
        System.out.format("Retrieved %s records%n", recordsRetrieved);
        records.forEach(recordValues::add);
        // Choosing 10 records as it allows for integration tests with a smaller max
        // poll to be added
        // while maintaining efficiency, a slightly larger number could be added but
        // this is slightly more
        // efficient
        // than larger numbers.
      } while (recordsRetrieved > 10);
      return recordValues;
    }

    /**
     * Gets the list of consumer offset messages.
     *
     * @param consumer A consumer configured to return byte array keys and values.
     * @return the list of {@link OffsetManager.OffsetManagerEntry} records created by the system
     *     under test.
     * @throws IOException on IO error.
     */
    public List<OffsetManager.OffsetManagerEntry> consumeOffsetMessages(
        final KafkaConsumer<byte[], byte[]> consumer) throws IOException {
      // Poll messages from the topic
      final BiFunction<Map<String, Object>, Map<String, Object>, OffsetManager.OffsetManagerEntry>
          converter = offsetManagerEntryFactory();
      final ObjectMapper objectMapper = new ObjectMapper();
      final List<OffsetManager.OffsetManagerEntry> messages = new ArrayList<>();
      final ConsumerRecords<byte[], byte[]> records = consumer.poll(Duration.ofSeconds(1));
      // TODO there is probably a way to clean this up by using the internal data
      // types from Kafka.
      for (final ConsumerRecord<byte[], byte[]> record : records) {
        final Map<String, Object> data =
            objectMapper.readValue(
                record.value(),
                new TypeReference<>() { // NOPMD
                });
        // the key has the format
        // key[0] = connector name
        // key[1] = Map<String, Object> partition map.
        final List<Object> key =
            objectMapper.readValue(
                record.key(),
                new TypeReference<>() { // NOPMD
                });
        final Map<String, Object> managerEntryKey = (Map<String, Object>) key.get(1);
        messages.add(converter.apply(managerEntryKey, data));
      }
      return messages;
    }
  }
}
