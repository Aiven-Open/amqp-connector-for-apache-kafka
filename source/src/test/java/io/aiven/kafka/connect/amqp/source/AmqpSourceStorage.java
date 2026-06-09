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

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import de.huxhorn.sulky.ulid.ULID;
import io.aiven.commons.kafka.connector.common.NativeInfo;
import io.aiven.commons.kafka.connector.source.OffsetManager;
import io.aiven.commons.kafka.connector.source.SourceStorage;
import io.aiven.commons.kafka.connector.source.config.SourceConfigFragment;
import io.aiven.commons.kafka.connector.source.extractor.ExtractorRegistry;
import io.aiven.kafka.connect.amqp.common.config.AmqpFragment;
import io.aiven.kafka.connect.amqp.source.extractor.AmqpExtractor;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;
import org.apache.commons.io.function.IOSupplier;
import org.apache.kafka.connect.connector.Connector;
import org.apache.qpid.protonj2.client.Client;
import org.apache.qpid.protonj2.client.Connection;
import org.apache.qpid.protonj2.client.ConnectionOptions;
import org.apache.qpid.protonj2.client.Delivery;
import org.apache.qpid.protonj2.client.DeliveryState;
import org.apache.qpid.protonj2.client.Message;
import org.apache.qpid.protonj2.client.Receiver;
import org.apache.qpid.protonj2.client.Sender;
import org.apache.qpid.protonj2.client.Tracker;
import org.apache.qpid.protonj2.client.exceptions.ClientException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.rabbitmq.RabbitMQContainer;

public final class AmqpSourceStorage implements SourceStorage<ULID.Value, Delivery> {
  private static final Logger LOGGER = LoggerFactory.getLogger(AmqpSourceStorage.class);
  private static final ExtractorRegistry extractorRegistry =
      ExtractorRegistry.builder().add(AmqpExtractor.info()).build();

  private final RabbitMQContainer rabbit;
  private final Client client;
  private final Connection connection;
  private Sender sender;
  private Receiver receiver;
  private String amqpAddress;
  private String topic;
  private Map<Tracker, Message> trackers = new HashMap<>();

  @Override
  public boolean nullDataIsNullRecord() {
    return false;
  }

  private void ensureSender() throws ClientException {
    if (sender == null) {
      sender = connection.openSender(amqpAddress);
      trackers.clear();
    }
  }

  private void ensureReceiver() {
    if (receiver == null) {
      try {
        receiver = connection.openReceiver(amqpAddress);
      } catch (ClientException e) {
        LOGGER.error("createStorage error: {}", e.getMessage(), e);
        throw new RuntimeException(e);
      }
    }
  }

  public AmqpSourceStorage(RabbitMQContainer rabbit) throws ClientException {
    this.rabbit = rabbit;
    client = Client.create();
    try {
      connection =
          client.connect(
              rabbit.getHost(),
              rabbit.getAmqpPort(),
              new ConnectionOptions()
                  .user(rabbit.getAdminUsername())
                  .password(rabbit.getAdminPassword()));
    } catch (ClientException e) {
      LOGGER.error("constructor create connection error: {}", e.getMessage(), e);
      throw e;
    }
  }

  @Override
  public ExtractorRegistry supportedExtractors() {
    return extractorRegistry;
  }

  @Override
  public ULID.Value createKey(String topic, int partition) {
    return AmqpSourceNativeInfo.nextValue();
  }

  @Override
  public WriteResult writeWithKey(ULID.Value nativeKey, byte[] testDataBytes) {
    try {
      ensureSender();
      Message<byte[]> message = Message.create(testDataBytes).messageId(nativeKey.toString());
      trackers.put(sender.send(message), message);
      return new WriteResult(null, nativeKey);
    } catch (ClientException e) {
      LOGGER.error("writingWithKey error: {}", e.getMessage(), e);
      throw new RuntimeException(e);
    }
  }

  public Tracker write(byte[] testDataBytes) {
    try {
      ensureSender();
      Message<byte[]> message = Message.create(testDataBytes);
      return sender.send(message);
    } catch (ClientException e) {
      LOGGER.error("write error: {}", e.getMessage(), e);
      throw new RuntimeException(e);
    }
  }

  public void setAmqpAddress(String address) {
    this.amqpAddress = address;
  }

  @Override
  public Map<String, String> createConnectorConfig() {
    Map<String, String> data = new HashMap<>();
    AmqpFragment.setter(data)
        .setHost(rabbit.getHost())
        .setPort(rabbit.getAmqpPort())
        .setAddress(amqpAddress)
        .setUser(rabbit.getAdminUsername())
        .setPassword(rabbit.getAdminPassword());
    return data;
  }

  @Override
  public BiFunction<Map<String, Object>, Map<String, Object>, OffsetManager.OffsetManagerEntry>
      offsetManagerEntryFactory() {
    return null;
  }

  @Override
  public Class<? extends Connector> getConnectorClass() {
    return AmqpSourceConnector.class;
  }

  /**
   * Gets the configuration with the AMQP properties and topic set.
   *
   * @return the map of properties.
   */
  public Map<String, String> getAMQPInitialConfig() {
    Map<String, String> data = new HashMap<>();
    AmqpFragment.setter(data)
        .setAddress(amqpAddress)
        .setHost(rabbit.getHost())
        .setPort(rabbit.getAmqpPort())
        .setUser(rabbit.getAdminUsername())
        .setPassword(rabbit.getAdminPassword());
    SourceConfigFragment.setter(data).targetTopic(topic);
    return data;
  }

  @Override
  public void createStorage(String topic) {
    amqpAddress = topic;
    this.topic = topic;
    if (sender != null) {
      removeStorage();
    }
    try {
      sender = connection.openSender(amqpAddress);
    } catch (ClientException e) {
      LOGGER.error("createStorage error: {}", e.getMessage(), e);
      throw new RuntimeException(e);
    }
  }

  @Override
  public void removeStorage() {
    if (sender != null) {
      sender.close();
      sender = null;
      trackers.clear();
    }
    if (receiver != null) {
      receiver.close();
      receiver = null;
    }
  }

  @Override
  public List<? extends NativeInfo<ULID.Value, Delivery>> getNativeInfo() {
    // can not read from connector or the data won't be there for real read.  So return what we
    // think was written.
    List<NativeInfo<ULID.Value, Delivery>> result = new ArrayList<>();
    for (Tracker tracker : trackers.keySet()) {
      if (tracker.remoteState() == DeliveryState.accepted()) {
        try {
          Message<?> message = trackers.get(tracker);
          Delivery delivery = mock(Delivery.class);
          ULID.Value value = ULID.parseULID(message.messageId().toString());
          when(delivery.message()).thenReturn(trackers.get(tracker));
          result.add(new NativeInfo<>(value, delivery));
        } catch (ClientException e) {
          throw new RuntimeException(e);
        }
      }
    }
    return result;
  }

  @Override
  public IOSupplier<InputStream> getInputStream(ULID.Value nativeKey) {
    return null;
  }

  @Override
  public String defaultPrefix() {
    return "";
  }
}
