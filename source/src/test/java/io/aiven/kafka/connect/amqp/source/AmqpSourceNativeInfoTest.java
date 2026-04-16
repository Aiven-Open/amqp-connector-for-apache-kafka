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
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import de.huxhorn.sulky.ulid.ULID;
import io.aiven.commons.kafka.connector.source.task.Context;
import java.io.DataInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import org.apache.commons.io.IOUtils;
import org.apache.qpid.protonj2.client.Delivery;
import org.apache.qpid.protonj2.client.Message;
import org.apache.qpid.protonj2.client.exceptions.ClientException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class AmqpSourceNativeInfoTest {
  private static final String BODY = "Hello world";
  private AmqpSourceNativeInfo underTest;
  private Message<Object> message;

  @BeforeEach
  void setup() throws ClientException {
    message = Message.create();
    message.body(BODY.getBytes(StandardCharsets.UTF_8));
    Delivery delivery = mock(Delivery.class);
    when(delivery.message()).thenReturn(message);
    underTest = new AmqpSourceNativeInfo(delivery);
  }

  @Test
  void getContext() {
    Context context = underTest.getContext();
    assertNotNull(context);
    ULID.Value nativeKey = context.getNativeKey();
    assertThat(nativeKey).isNotNull();
    Context context2 = underTest.getContext();
    ULID.Value nativeKey2 = context.getNativeKey();
    assertThat(nativeKey.compareTo(nativeKey2)).isEqualTo(0);
  }

  @Test
  void getInputStream() throws IOException {

    byte[] result = IOUtils.toByteArray(underTest.getInputStream());
    assertThat(result).isEqualTo(BODY.getBytes(StandardCharsets.UTF_8));
  }

  @Test
  void estimateInputStreamLength() {
    assertThat(underTest.estimateInputStreamLength())
        .isEqualTo(BODY.getBytes(StandardCharsets.UTF_8).length);
  }

  // @Test
  // void rewrite() throws ClientException {
  // Context context = new Context(new ULID().nextValue());
  // AmqpOffsetManagerEntry offsetManagerEntry = new
  // AmqpOffsetManagerEntry((ULID.Value) context.getNativeKey());
  // message.correlationId("correlation1");
  // message.annotation("annotation1", "this is a message");
  // message.footer("footer1", "see AMQP documentation");
  // message.property("an integer", 5);
  // message.property("a long", 10L);
  //
  //
  // EvolvingSourceRecord record = new EvolvingSourceRecord(underTest,
  // offsetManagerEntry, context);
  // record.setValueData(new SchemaAndValue(Schema.STRING_SCHEMA,
  // message.body()));
  // AmqpSourceConfig config = mock(AmqpSourceConfig.class);
  //
  // EvolvingSourceRecord actual = underTest.rewrite(record, config);
  // SchemaAndValue schemaAndValue = actual.getValue();
  // assertThat(schemaAndValue.value()).isInstanceOf(ObjectNode.class);
  // ObjectNode value = (ObjectNode) schemaAndValue.value();
  // for (AmqpHeaderProperties property : AmqpHeaderProperties.values()) {
  // switch (property) {
  // case CORRELATION_ID:
  // assertThat(value.get(property.getSchemaName()).toString()).isEqualTo("correlation1");
  // break;
  // case ABSOLUTE_EXPIRY :
  // case CREATION_TIME :
  // case GROUP_SEQUENCE :
  // case DELIVERY_COUNT:
  // assertThat(value.get(property.getSchemaName()).toString()).isEqualTo("0");
  // break;
  // case DURABLE:
  // case FIRST_ACQUIRER:
  // assertThat(value.get(property.getSchemaName()).toString()).isEqualTo("false");
  // break;
  // default:
  // assertThat(value.get(property.getSchemaName()).toString()).isEqualTo("null");
  // }
  // }
  // assertThat(value.get("annotations")).isInstanceOf(ObjectNode.class);
  // ObjectNode child = (ObjectNode) value.get("annotations");
  // assertThat(child.get("annotation1").toString()).isEqualTo("this is a
  // message");
  //
  // assertThat(value.get("properties")).isInstanceOf(ObjectNode.class);
  // child = (ObjectNode) value.get("properties");
  // assertThat(child.get("an integer").toString()).isEqualTo("5");
  // assertThat(child.get("an long").toString()).isEqualTo("10");
  // assertThat(child.get("schema and value").toString()).isEqualTo("null");
  //
  // assertThat(value.get("footers")).isInstanceOf(ObjectNode.class);
  // child = (ObjectNode) value.get("footers");
  // assertThat(child.get("footer1").toString()).isEqualTo("see AMQP
  // documentation");
  // }
  //
  //
}
