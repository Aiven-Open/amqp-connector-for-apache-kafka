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

import io.aiven.commons.kafka.connector.common.config.ConnectorCommonConfigFragment;
import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.StringDeserializer;

/** Creates consumer properties to read Kafka topics. */
public final class ConsumerPropertiesBuilder {
  /** The properties for this builder */
  private final Map<String, String> props = new HashMap<>();

  /**
   * Creates a ConsumerPropertiesBuilder for a bootstrap server. By default, the key and value are
   * serialized as strings.
   *
   * @param bootstrapServers the bootstrap server to talk to.
   */
  public ConsumerPropertiesBuilder(final String bootstrapServers) {
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
    props.put(ConsumerConfig.GROUP_ID_CONFIG, "test-consumer-group");
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    valueDeserializer(StringDeserializer.class);
    keyDeserializer(StringDeserializer.class);
  }

  /**
   * Sets the key deserializer.
   *
   * @param keyDeserializer the class for the key deserializer.
   * @return this.
   */
  public ConsumerPropertiesBuilder keyDeserializer(
      final Class<? extends Deserializer<?>> keyDeserializer) {
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, keyDeserializer.getCanonicalName());
    return this;
  }

  /**
   * Sets the value deserializer.
   *
   * @param valueDeserializer the class for the value deserializer.
   * @return this.
   */
  public ConsumerPropertiesBuilder valueDeserializer(
      final Class<? extends Deserializer<?>> valueDeserializer) {
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, valueDeserializer.getCanonicalName());
    return this;
  }

  /**
   * Sets the schema registry URL.
   *
   * @param schemaRegistryUrl the URL as a string.
   * @return this.
   */
  public ConsumerPropertiesBuilder schemaRegistry(final String schemaRegistryUrl) {
    ConnectorCommonConfigFragment.setter(props).schemaRegistry(schemaRegistryUrl);
    return this;
  }

  /**
   * Builds the properties.
   *
   * @return the properties from this builder.
   */
  public Map<String, Object> build() {
    return new HashMap<String, Object>(props);
  }
}
