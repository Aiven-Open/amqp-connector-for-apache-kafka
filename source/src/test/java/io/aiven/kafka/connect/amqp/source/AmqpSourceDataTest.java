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
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import de.huxhorn.sulky.ulid.ULID;
import io.aiven.commons.kafka.connector.source.NativeSourceData;
import io.aiven.commons.kafka.connector.source.OffsetManager;
import io.aiven.commons.kafka.connector.source.task.Context;
import io.aiven.kafka.connect.amqp.common.config.AmqpFragment;
import io.aiven.kafka.connect.amqp.source.config.AmqpSourceConfig;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.qpid.protonj2.client.Client;
import org.apache.qpid.protonj2.client.Connection;
import org.apache.qpid.protonj2.client.Delivery;
import org.apache.qpid.protonj2.client.Message;
import org.apache.qpid.protonj2.client.Receiver;
import org.apache.qpid.protonj2.client.exceptions.ClientException;
import org.apache.qpid.protonj2.client.impl.ClientMessage;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class AmqpSourceDataTest {

  private static final Map<String, String> CONFIG =
      AmqpFragment.setter(new HashMap<String, String>())
          .setHost("localhost")
          .setAddress("address")
          .setUser("user")
          .setPassword("password")
          .data();

  private AmqpSourceConfig sourceConfig;
  private OffsetManager offsetManager;
  private Context context;

  @BeforeEach
  void setup() {
    sourceConfig = new AmqpSourceConfig(CONFIG);
    offsetManager = mock(OffsetManager.class);
    context = new Context(new ULID().nextValue());
  }

  @Test
  void getSourceName() throws ClientException {
    AmqpSourceData underTest = new AmqpSourceData(sourceConfig, offsetManager);
    assertThat(underTest.getSourceName()).isEqualTo("AMQP Source");
  }

  @Test
  void nativeSerde() throws ClientException {
    AmqpSourceData underTest = new AmqpSourceData(sourceConfig, offsetManager);
    Optional<NativeSourceData.KeySerde<ULID.Value>> optSerde = underTest.getNativeKeySerde();
    assertThat(optSerde.isPresent()).isTrue();
    NativeSourceData.KeySerde<ULID.Value> serde = optSerde.get();

    String keyString = serde.toString((ULID.Value) context.getNativeKey());
    ULID.Value value = serde.fromString(keyString);
    assertThat(value).isEqualTo(context.getNativeKey());
  }

  @Test
  void createOffsetManagerEntry() throws ClientException {
    AmqpSourceData underTest = new AmqpSourceData(sourceConfig, offsetManager);
    OffsetManager.OffsetManagerEntry offsetManagerEntry =
        underTest.createOffsetManagerEntry(context);
    assertThat(offsetManagerEntry.getProperties()).containsEntry("ulid", context.getNativeKey());
  }

  @Test
  void createOffsetManagerEntryWithMap() throws ClientException {
    AmqpSourceData underTest = new AmqpSourceData(sourceConfig, offsetManager);
    OffsetManager.OffsetManagerEntry offsetManagerEntry =
        underTest.createOffsetManagerEntry(
            Map.of("ulid", context.getNativeKey(), "recordCount", 5));
    assertThat(offsetManagerEntry.getProperties()).containsEntry("ulid", context.getNativeKey());

    offsetManagerEntry =
        underTest.createOffsetManagerEntry(
            Map.of("ulid", context.getNativeKey().toString(), "recordCount", 5));
    assertThat(offsetManagerEntry.getProperties()).containsEntry("ulid", context.getNativeKey());

    offsetManagerEntry =
        underTest.createOffsetManagerEntry(
            Map.of("ulid", "01KKVQF32P85BW8EYKBP1BTQR0", "recordCount", 5));
    assertThat(offsetManagerEntry.getProperties())
        .containsEntry("ulid", ULID.parseULID("01KKVQF32P85BW8EYKBP1BTQR0"));
  }

  @Test
  void getOffsetManagerKey() throws Exception {
    try (AmqpSourceData underTest = new AmqpSourceData(sourceConfig, offsetManager)) {
      OffsetManager.OffsetManagerKey key =
          underTest.getOffsetManagerKey((ULID.Value) context.getNativeKey());
      assertThat(key.getPartitionMap()).containsEntry("ulid", context.getNativeKey().toString());
    }
  }

  private ClientMessage<Object> createMessage(String content) {
    ClientMessage<Object> msg = ClientMessage.create();
    msg.body(content);
    return msg;
  }

  @Test
  void getNativeItemIterator() throws ClientException {
    Client client = mock(Client.class);
    Connection connection = mock(Connection.class);
    Receiver receiver = mock(Receiver.class);

    AmqpSourceConfig config2 =
        new AmqpSourceConfig(CONFIG) {
          @Override
          public Receiver getReceiver(Connection connection) throws ClientException {
            return receiver;
          }

          @Override
          public Client getClient() {
            return client;
          }

          @Override
          public Connection getConnection(Client client) throws ClientException {
            return connection;
          }
        };
    Delivery delivery1 = mock(Delivery.class);
    Message<Object> message1 = createMessage("hello");
    when(delivery1.message()).thenReturn(message1);
    Delivery delivery2 = mock(Delivery.class);
    Message<Object> message2 = createMessage("world");
    when(delivery2.message()).thenReturn(message2);
    when(receiver.queuedDeliveries()).thenReturn(3L);
    // don't return 3 this verifies that the result will be correct if we see 3 but
    // only 2 are returned.
    when(receiver.tryReceive()).thenReturn(delivery1, delivery2, null);

    try (AmqpSourceData underTest = new AmqpSourceData(config2, offsetManager)) {
      Iterator<AmqpSourceNativeInfo> nativeItemIterator = underTest.getNativeItemIterator(null);
      assertThat(nativeItemIterator).isNotNull();
      assertThat(nativeItemIterator).hasNext();
      List<AmqpSourceNativeInfo> lst = new ArrayList<>();
      nativeItemIterator.forEachRemaining(lst::add);
      assertThat(lst).hasSize(2);
      assertThat(lst.get(0).getMessage()).isEqualTo(message1);
      assertThat(lst.get(1).getMessage()).isEqualTo(message2);
    } catch (Exception e) {
        fail(e);
    }
  }
}
