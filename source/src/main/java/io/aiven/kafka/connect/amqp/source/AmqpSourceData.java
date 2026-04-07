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

import de.huxhorn.sulky.ulid.ULID;
import io.aiven.commons.kafka.connector.source.NativeSourceData;
import io.aiven.commons.kafka.connector.source.OffsetManager;
import io.aiven.commons.kafka.connector.source.task.Context;
import io.aiven.kafka.connect.amqp.source.config.AmqpSourceConfig;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.qpid.protonj2.client.Delivery;
import org.apache.qpid.protonj2.client.Receiver;
import org.apache.qpid.protonj2.client.exceptions.ClientException;

/** The AMQP NativeSourceData implementation. */
public final class AmqpSourceData extends NativeSourceData<ULID.Value> {
  private static final ULIDSerde serde = new ULIDSerde();
  private final Receiver receiver;

  /** The maximum number of Deliveries to pull from the Receiver. */
  private final int receiveLimit;

  /**
   * Constructor.
   *
   * @param sourceConfig The AMQP Source configuration.
   * @param offsetManager the OffsetManager to use.
   * @throws ClientException on error.
   */
  AmqpSourceData(final AmqpSourceConfig sourceConfig, final OffsetManager offsetManager)
      throws ClientException {
    super(sourceConfig, offsetManager);
    this.receiver = sourceConfig.getReceiver(sourceConfig.getConnection(sourceConfig.getClient()));
    receiveLimit = 500; // sourceConfig.getreceiveLimit();
  }

  @Override
  public String getSourceName() {
    return "AMQP Source";
  }

  @Override
  public Iterator<AmqpSourceNativeInfo> getNativeItemIterator(ULID.Value ignore) {
    try {
      long waiting = receiver.queuedDeliveries();
      int limit = (int) Math.min(waiting, receiveLimit);
      List<AmqpSourceNativeInfo> lst = new ArrayList<>(limit);
      try {
        for (int i = 0; i < limit; i++) {
          Delivery delivery = receiver.tryReceive();
          if (delivery != null) {
            lst.add(new AmqpSourceNativeInfo(delivery));
          }
        }
      } catch (ClientException e) {
        // do nothing.
      }

      return lst.iterator();
    } catch (ClientException e) {
      throw new ConnectException(e);
    }
  }

  @Override
  public OffsetManager.OffsetManagerEntry createOffsetManagerEntry(Map<String, Object> data) {
    return new AmqpOffsetManagerEntry(data);
  }

  @Override
  protected OffsetManager.OffsetManagerEntry createOffsetManagerEntry(Context context) {
    return new AmqpOffsetManagerEntry((ULID.Value) context.getNativeKey());
  }

  @Override
  protected Optional<KeySerde<ULID.Value>> getNativeKeySerde() {
    return Optional.of(serde);
  }

  @Override
  public OffsetManager.OffsetManagerKey getOffsetManagerKey(ULID.Value nativeKey) {
    return new AmqpOffsetManagerEntry(nativeKey).getManagerKey();
  }

  @Override
  public void close() throws Exception {
    super.close();
    receiver.connection().client().close();
    receiver.connection().close();
    receiver.close();
  }

  /** The AMQP native source data implementation of NativeSourceData.KeySerde. */
  public static class ULIDSerde implements NativeSourceData.KeySerde<ULID.Value> {

    @Override
    public String toString(ULID.Value nativeKey) {
      return nativeKey.toString();
    }

    @Override
    public ULID.Value fromString(String nativeKeyString) {
      return ULID.parseULID(nativeKeyString);
    }
  }
}
