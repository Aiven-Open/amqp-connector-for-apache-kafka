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
package io.aiven.kafka.connect.amqp;

import java.util.Map;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.storage.Converter;
import org.apache.qpid.protonj2.buffer.ProtonBuffer;
import org.apache.qpid.protonj2.buffer.impl.ProtonByteArrayBuffer;
import org.apache.qpid.protonj2.client.Message;
import org.apache.qpid.protonj2.client.exceptions.ClientException;
import org.apache.qpid.protonj2.client.impl.ClientMessageSupport;

/**
 * EncoderDecoder to store AMQP messages in binary format.
 *
 * <p>Data from the connector is expected to have a value that is an instance of an Apache QPID
 * ProtonJ2 Message. The schema is ignored. The resulting byte[] if formatted as per the ProtonJ2
 * message format.
 *
 * <p>Data formatted to the connector expects the byte[] to be in QPID ProtonJ2 format. It returns a
 * SchemaAndValue with a null schema and the value a QPID ProtonJ2 Message
 */
public class AmqpConverter implements Converter {

  /** Constructor. */
  public AmqpConverter() {}

  @Override
  public void configure(Map<String, ?> configs, boolean isKey) {}

  @Override
  public byte[] fromConnectData(String topic, Schema schema, Object value) {
    if (value instanceof Message<?> message) {
      try (ProtonBuffer protonBuffer =
          ClientMessageSupport.encodeMessage(message.toAdvancedMessage(), null)) {
        protonBuffer.setReadOffset(0);
        byte[] buffer = new byte[protonBuffer.getReadableBytes()];
        protonBuffer.readBytes(buffer, 0, buffer.length);
        return buffer;
      } catch (ClientException e) {
        throw new DataException("Unable to extract ProtonBuffer from AMQP message");
      }
    } else {
      throw new DataException(
          "Invalid value - not an Apache QPID ProtonJ2 message: " + value.getClass().toString());
    }
  }

  @Override
  public SchemaAndValue toConnectData(String topic, byte[] value) {
    try (ProtonBuffer protonBuffer = new ProtonByteArrayBuffer(value); ) {
      protonBuffer.setWriteOffset(value.length);
      return new SchemaAndValue(null, ClientMessageSupport.decodeMessage(protonBuffer, null));
    } catch (ClientException e) {
      throw new RuntimeException(e);
    }
  }
}
