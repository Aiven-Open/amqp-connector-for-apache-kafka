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
import io.aiven.commons.kafka.connector.common.NativeInfo;
import io.aiven.commons.kafka.connector.source.AbstractSourceNativeInfo;
import io.aiven.commons.kafka.connector.source.task.Context;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import org.apache.qpid.protonj2.client.Delivery;
import org.apache.qpid.protonj2.client.Message;
import org.apache.qpid.protonj2.client.exceptions.ClientException;

/**
 * Wraps ULID.Value and the Delivery for a streaming object. The framework requires a NativeInfo
 * that has both the key and the value associated with that key. This implementation creates a ULID
 * value for every Delivery from AMQP.
 */
public final class AmqpSourceNativeInfo extends AbstractSourceNativeInfo<ULID.Value, Delivery> {
  /** The ULID to generate keys with */
  private static final ULID ulid = new ULID();

  static ULID.Value nextValue() {
    return ulid.nextValue();
  }
  /**
   * Construct native info for a Delivery from AMQP.
   *
   * @param delivery the AMQP delivery object to process.
   */
  public AmqpSourceNativeInfo(Delivery delivery) {
    super(new NativeInfo<>(ulid.nextValue(), delivery));
  }

  @Override
  public Context getContext() {
    return new Context(nativeInfo.nativeKey());
  }

  @Override
  public InputStream getInputStream() throws IOException {
    byte[] bytes = getMessageBytes();
    if (bytes == null) {
      return InputStream.nullInputStream();
    }
    return new ByteArrayInputStream(bytes);
  }

  @Override
  public long estimateInputStreamLength() {
    try {
      byte[] bytes = getMessageBytes();
      return bytes == null ? 0 : bytes.length;
    } catch (IOException e) {
      return UNKNOWN_STREAM_LENGTH;
    }
  }

  /**
   * Gets the AMQP message.
   *
   * @return the AMQP message
   * @throws ClientException on retrieval error.
   */
  public Message<Object> getMessage() throws ClientException {
    return nativeInfo.nativeItem().message();
  }

  private byte[] getMessageBytes() throws IOException {
    try {
      Message<?> message = getMessage();
      if (message.body() == null) {
        return null;
      }
      return (byte[]) message.body();
    } catch (ClientException e) {
      throw new IOException(e);
    }
  }
}
