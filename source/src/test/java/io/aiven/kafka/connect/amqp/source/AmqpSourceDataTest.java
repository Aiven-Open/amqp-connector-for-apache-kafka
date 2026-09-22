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
import io.aiven.commons.kafka.connector.source.EvolvingSourceRecord;
import io.aiven.commons.kafka.connector.source.NativeSourceData;
import io.aiven.commons.kafka.connector.source.OffsetManager;
import io.aiven.kafka.connect.amqp.common.config.AmqpCommonConfig;
import io.aiven.kafka.connect.amqp.common.config.AmqpFragment;
import io.aiven.kafka.connect.amqp.source.config.AmqpSourceConfig;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.header.Headers;
import org.apache.qpid.protonj2.client.Client;
import org.apache.qpid.protonj2.client.Connection;
import org.apache.qpid.protonj2.client.Delivery;
import org.apache.qpid.protonj2.client.Message;
import org.apache.qpid.protonj2.client.Receiver;
import org.apache.qpid.protonj2.client.exceptions.ClientException;
import org.apache.qpid.protonj2.client.impl.ClientMessage;
import org.apache.qpid.protonj2.types.UnsignedByte;
import org.apache.qpid.protonj2.types.UnsignedInteger;
import org.apache.qpid.protonj2.types.UnsignedLong;
import org.apache.qpid.protonj2.types.UnsignedShort;
import org.apache.qpid.protonj2.types.messaging.AmqpSequence;
import org.apache.qpid.protonj2.types.messaging.AmqpValue;
import org.apache.qpid.protonj2.types.messaging.Data;
import org.apache.qpid.protonj2.types.messaging.Footer;
import org.apache.qpid.protonj2.types.messaging.MessageAnnotations;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

public class AmqpSourceDataTest {

  private static final BigInteger TWO_TO_THE_SIXTY_FOUR =
      new BigInteger(
          new byte[] {
            (byte) 1, (byte) 0, (byte) 0, (byte) 0, (byte) 0, (byte) 0, (byte) 0, (byte) 0, (byte) 0
          });

  private static final Map<String, String> CONFIG =
      AmqpFragment.setter(new HashMap<>())
          .setHost("localhost")
          .setAddress("address")
          .setUser("user")
          .setPassword("password")
          .data();

  private AmqpSourceConfig sourceConfig;
  private OffsetManager offsetManager;
  private AmqpContext context;

  private final long longValue = 1000L;
  private final int intValue = 500;
  private final short shortValue = 250;
  private final byte byteValue = 0x7f;
  private final BigInteger unsignedLong = TWO_TO_THE_SIXTY_FOUR.add(BigInteger.valueOf(-1L));
  private final long unsignedInt = 0xffffffffL;
  private final int unsignedShort = 0xffff;
  private final short unsignedByte = 0xff;

  @BeforeEach
  void setup() throws ClientException, ExecutionException, InterruptedException {
    Receiver receiver = mock(Receiver.class);
    when(receiver.connection()).thenReturn(mock(Connection.class));
    when(receiver.connection().client()).thenReturn(mock(Client.class));

    sourceConfig = mock(AmqpSourceConfig.class);
    when(sourceConfig.getReceiver()).thenReturn(receiver);

    offsetManager = mock(OffsetManager.class);
    context = new AmqpContext.Builder(new ULID().nextULID(), mock(Delivery.class)).build();
  }

  @Test
  void getSourceName() throws Exception {
    try (AmqpSourceData underTest = new AmqpSourceData(sourceConfig, offsetManager)) {
      assertThat(underTest.getSourceName()).isEqualTo("AMQP Source");
    }
  }

  @Test
  void nativeSerde() throws Exception {
    try (AmqpSourceData underTest = new AmqpSourceData(sourceConfig, offsetManager)) {
      Optional<NativeSourceData.KeySerde<String>> optSerde = underTest.getNativeKeySerde();
      assertThat(optSerde.isPresent()).isTrue();
      NativeSourceData.KeySerde<String> serde = optSerde.get();

      String keyString = serde.toString(context.getNativeKey());
      String value = serde.fromString(keyString);
      assertThat(value).isEqualTo(context.getNativeKey());
    }
  }

  @Test
  void createOffsetManagerEntry() {
    try (AmqpSourceData underTest = new AmqpSourceData(sourceConfig, offsetManager)) {
      OffsetManager.OffsetManagerEntry offsetManagerEntry =
          underTest.createOffsetManagerEntry(context);
      assertThat(offsetManagerEntry.getProperties())
          .containsEntry(AmqpOffsetManagerEntry.PRIMARY_KEY, context.getNativeKey().toString());
    } catch (Exception e) {
      fail(e);
    }
  }

  @Test
  void createOffsetManagerEntryWithMap() throws Exception {
    try (AmqpSourceData underTest = new AmqpSourceData(sourceConfig, offsetManager)) {
      OffsetManager.OffsetManagerEntry offsetManagerEntry =
          underTest.createOffsetManagerEntry(
              Map.of(
                  AmqpOffsetManagerEntry.PRIMARY_KEY,
                  context.getNativeKey(),
                  AmqpOffsetManagerEntry.RECORD_COUNT,
                  5));
      assertThat(offsetManagerEntry.getProperties())
          .containsEntry(AmqpOffsetManagerEntry.PRIMARY_KEY, context.getNativeKey().toString());

      offsetManagerEntry =
          underTest.createOffsetManagerEntry(
              Map.of(
                  AmqpOffsetManagerEntry.PRIMARY_KEY,
                  context.getNativeKey().toString(),
                  AmqpOffsetManagerEntry.RECORD_COUNT,
                  5));
      assertThat(offsetManagerEntry.getProperties())
          .containsEntry(AmqpOffsetManagerEntry.PRIMARY_KEY, context.getNativeKey().toString());

      offsetManagerEntry =
          underTest.createOffsetManagerEntry(
              Map.of(
                  AmqpOffsetManagerEntry.PRIMARY_KEY,
                  "01KKVQF32P85BW8EYKBP1BTQR0",
                  AmqpOffsetManagerEntry.RECORD_COUNT,
                  5));
      assertThat(offsetManagerEntry.getProperties())
          .containsEntry(
              AmqpOffsetManagerEntry.PRIMARY_KEY,
              ULID.parseULID("01KKVQF32P85BW8EYKBP1BTQR0").toString());
    }
  }

  @Test
  void getOffsetManagerKey() throws Exception {
    try (AmqpSourceData underTest = new AmqpSourceData(sourceConfig, offsetManager)) {
      OffsetManager.OffsetManagerKey key = underTest.getOffsetManagerKey(context.getNativeKey());
      assertThat(key.getPartitionMap())
          .containsEntry(AmqpOffsetManagerEntry.PRIMARY_KEY, context.getNativeKey().toString());
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
    when(receiver.connection()).thenReturn(connection);
    when(connection.client()).thenReturn(client);

    AmqpSourceConfig config2 =
        new AmqpSourceConfig(CONFIG) {
          @Override
          public Receiver getReceiver(Connection connection) {
            return receiver;
          }

          @Override
          public Client getClient() {
            return client;
          }

          @Override
          public Connection getConnection(Client client) {
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

  @Test
  void initializeTest() throws Exception {
    final long absoluteExpiry = 1788780705417L;
    final long creationTime = 1788780700417L;
    final long deliveryCount = 14L;
    final int groupSequence = 700;
    final byte priority = 0x2;
    final long timeToLive = 50000L;
    final byte[] userId = "Alice".getBytes(StandardCharsets.UTF_8);
    final UUID uuid = UUID.randomUUID();

    ClientMessage<?> message = ClientMessage.create();

    message
        .absoluteExpiryTime(absoluteExpiry)
        .to("ToPerson")
        .messageId(uuid)
        .contentEncoding("UTF8")
        .contentType("text/plain")
        .correlationId("correlationId")
        .creationTime(creationTime)
        .deliveryCount(deliveryCount)
        .durable(true)
        .firstAcquirer(false)
        .groupId("myGroup")
        .groupSequence(groupSequence)
        .priority(priority)
        .replyTo("replyToMsg")
        .replyToGroupId("replytoGroupId")
        .subject("subject")
        .timeToLive(timeToLive)
        .userId(userId)
        .annotation("long", longValue)
        .annotation("int", intValue)
        .annotation("short", shortValue)
        .annotation("byte", byteValue)
        .annotation("unsignedLong", UnsignedLong.valueOf(unsignedLong))
        .annotation("unsignedInt", UnsignedInteger.valueOf(unsignedInt))
        .annotation("unsignedShort", UnsignedShort.valueOf(unsignedShort))
        .annotation("unsignedByte", UnsignedByte.valueOf((byte) unsignedByte))
        .footer("long", longValue)
        .footer("int", intValue)
        .footer("short", shortValue)
        .footer("byte", byteValue)
        .footer("unsignedLong", UnsignedLong.valueOf(unsignedLong))
        .footer("unsignedInt", UnsignedInteger.valueOf(unsignedInt))
        .footer("unsignedShort", UnsignedShort.valueOf(unsignedShort))
        .footer("unsignedByte", UnsignedByte.valueOf((byte) unsignedByte));

    Delivery delivery = mock(Delivery.class);
    when(delivery.message()).thenReturn((Message<Object>) message);
    context = context.builder().delivery(delivery).build();
    final AmqpSourceNativeInfo sourceNativeInfo = new AmqpSourceNativeInfo(context.getDelivery());
    final OffsetManager.OffsetManagerEntry offsetManagerEntry =
        mock(OffsetManager.OffsetManagerEntry.class);

    try (AmqpSourceData underTest = new AmqpSourceData(sourceConfig, offsetManager)) {

      EvolvingSourceRecord record =
          new EvolvingSourceRecord(sourceNativeInfo, offsetManagerEntry, context);

      EvolvingSourceRecord actual = underTest.initialize(record);

      assertThat(actual.getHeaders()).isNotNull();
      Headers headers = actual.getHeaders();
      List<String> keys = new ArrayList<>();
      headers.forEach(header -> keys.add(header.key()));
      assertThat(keys)
          .containsExactly(
              "amqp.messageId",
              "amqp.userId",
              "amqp.subject",
              "amqp.replyTo",
              "amqp.correlationId",
              "amqp.contentType",
              "amqp.contentEncoding",
              "amqp.absoluteExpiry",
              "amqp.creationTime",
              "amqp.groupSequence",
              "amqp.replyToGroupId",
              "amqp.durable",
              "amqp.firstAcquirer",
              "amqp.deliveryCount",
              "amqp.annotations",
              "amqp.footers");

      headers.forEach(
          header -> {
            switch (header.key()) {
              case "amqp.messageId" ->
                  assertThat(header.value()).as(header.key()).isEqualTo(uuid.toString());
              case "amqp.userId" ->
                  assertThat(header.value()).as(header.key()).isEqualTo("ToPerson");
              case "amqp.subject" ->
                  assertThat(header.value()).as(header.key()).isEqualTo("subject");
              case "amqp.replyTo" ->
                  assertThat(header.value()).as(header.key()).isEqualTo("replyToMsg");
              case "amqp.correlationId" ->
                  assertThat(header.value()).as(header.key()).isEqualTo("correlationId");
              case "amqp.contentType" ->
                  assertThat(header.value()).as(header.key()).isEqualTo("text/plain");
              case "amqp.contentEncoding" ->
                  assertThat(header.value()).as(header.key()).isEqualTo("UTF8");
              case "amqp.absoluteExpiry" ->
                  assertThat(header.value()).as(header.key()).isEqualTo(absoluteExpiry);
              case "amqp.creationTime" ->
                  assertThat(header.value()).as(header.key()).isEqualTo(creationTime);
              case "amqp.groupSequence" ->
                  assertThat(header.value()).as(header.key()).isEqualTo(groupSequence);
              case "amqp.replyToGroupId" ->
                  assertThat(header.value()).as(header.key()).isEqualTo("replytoGroupId");
              case "amqp.durable" ->
                  assertThat(header.value()).as(header.key()).isEqualTo(Boolean.TRUE);
              case "amqp.firstAcquirer" ->
                  assertThat(header.value()).as(header.key()).isEqualTo(Boolean.FALSE);
              case "amqp.deliveryCount" ->
                  assertThat(header.value()).as(header.key()).isEqualTo(deliveryCount);
              case "amqp.annotations" -> {
                assertThat(header.schema().name()).isEqualTo(MessageAnnotations.class.getName());
                verifyHeaderStruct(header.value());
              }
              case "amqp.footers" -> {
                assertThat(header.schema().name()).isEqualTo(Footer.class.getName());
                verifyHeaderStruct(header.value());
              }
              default -> fail("Unknown header: " + header);
            }
          });
    }
  }

  private void verifyHeaderStruct(Object value) {
    Struct struct = (Struct) assertThat(value).isInstanceOf(Struct.class).actual();
    List<String> fieldNames = new ArrayList<>();

    struct.schema().fields().forEach(field -> fieldNames.add(field.name()));
    assertThat(fieldNames)
        .containsExactly(
            "long",
            "int",
            "short",
            "byte",
            "unsignedLong",
            "unsignedInt",
            "unsignedShort",
            "unsignedByte");
    struct
        .schema()
        .fields()
        .forEach(
            field -> {
              switch (field.name()) {
                case "long" -> assertThat(struct.get(field)).as(field.name()).isEqualTo(longValue);
                case "int" -> assertThat(struct.get(field)).as(field.name()).isEqualTo(intValue);
                case "short" ->
                    assertThat(struct.get(field)).as(field.name()).isEqualTo(shortValue);
                case "byte" -> assertThat(struct.get(field)).as(field.name()).isEqualTo(byteValue);
                case "unsignedLong" ->
                    assertThat(struct.get(field))
                        .as(field.name())
                        .isEqualTo(unsignedLong.toString());
                case "unsignedInt" ->
                    assertThat(struct.get(field)).as(field.name()).isEqualTo(unsignedInt);
                case "unsignedShort" ->
                    assertThat(struct.get(field)).as(field.name()).isEqualTo(unsignedShort);
                case "unsignedByte" ->
                    assertThat(struct.get(field)).as(field.name()).isEqualTo(unsignedByte);
                default -> fail("Unexpected field name: " + field);
              }
            });
  }

  @ParameterizedTest
  @MethodSource("bodyValuesTestData")
  void bodyValuesTest(Object value, Schema expectedSchema) throws Exception {
    ClientMessage<?> message = ClientMessage.create(new AmqpValue<>(value));
    Delivery delivery = mock(Delivery.class);
    when(delivery.message()).thenReturn((Message) message);
    final AmqpSourceNativeInfo sourceNativeInfo = new AmqpSourceNativeInfo(delivery);
    final OffsetManager.OffsetManagerEntry offsetManagerEntry =
        mock(OffsetManager.OffsetManagerEntry.class);

    try (AmqpSourceData underTest = new AmqpSourceData(sourceConfig, offsetManager)) {

      EvolvingSourceRecord record =
          new EvolvingSourceRecord(sourceNativeInfo, offsetManagerEntry, context);

      EvolvingSourceRecord actual = underTest.initialize(record);
      assertThat(actual.getValue().schema()).isEqualTo(expectedSchema);
      assertThat(actual.getValue().value()).isInstanceOf(value.getClass()).isEqualTo(value);
    }
  }

  static List<Arguments> bodyValuesTestData() {
    List<Arguments> result = new ArrayList<>();
    result.add(Arguments.of("Hello world", Schema.STRING_SCHEMA));
    result.add(Arguments.of(10L, Schema.INT64_SCHEMA));
    result.add(Arguments.of(5, Schema.INT32_SCHEMA));
    result.add(
        Arguments.of(
            List.of("Hello world", "goodbye cruel world"),
            SchemaBuilder.array(Schema.STRING_SCHEMA).build()));
    return result;
  }

  @ParameterizedTest
  @MethodSource("bodySequenceTestData")
  void bodySequenceTest(List<Object> value) throws Exception {
    ClientMessage<?> message = ClientMessage.create(new AmqpSequence<>(value));
    Delivery delivery = mock(Delivery.class);
    when(delivery.message()).thenReturn((Message) message);
    final AmqpSourceNativeInfo sourceNativeInfo = new AmqpSourceNativeInfo(delivery);
    final OffsetManager.OffsetManagerEntry offsetManagerEntry =
        mock(OffsetManager.OffsetManagerEntry.class);

    try (AmqpSourceData underTest = new AmqpSourceData(sourceConfig, offsetManager)) {

      EvolvingSourceRecord record =
          new EvolvingSourceRecord(sourceNativeInfo, offsetManagerEntry, context);

      EvolvingSourceRecord actual = underTest.initialize(record);
      assertThat(actual.getValue().schema().type()).isEqualTo(Schema.Type.STRUCT);
      assertThat(actual.getValue().schema().name())
          .isEqualTo("java.util.ImmutableCollections$ListN");
      Optional<Object> optObj = AmqpCommonConfig.getCommonConverter().decode(actual.getValue());
      assertThat(optObj).isPresent();
      Struct struct =
          (Struct) assertThat(actual.getValue().value()).isInstanceOf(Struct.class).actual();

      List<Field> fields = struct.schema().fields();
      for (int i = 0; i < value.size(); i++) {
        assertThat(struct.get(fields.get(i))).isEqualTo(value.get(i));
      }
    }
  }

  static List<Arguments> bodySequenceTestData() {
    List<Arguments> result = new ArrayList<>();
    List<Object> lst =
        List.of(
            "Hello world",
            10L,
            5,
            List.of("Hello world", "goodbye cruel world"),
            "this is the end");
    result.add(Arguments.of(lst, Schema.BYTES_SCHEMA));
    return result;
  }

  @ParameterizedTest
  @MethodSource("bodyDataTestData")
  void bodyDataTest(byte[] value) throws Exception {
    ClientMessage<?> message = ClientMessage.create(new Data(value));
    Delivery delivery = mock(Delivery.class);
    when(delivery.message()).thenReturn((Message) message);
    final AmqpSourceNativeInfo sourceNativeInfo = new AmqpSourceNativeInfo(delivery);
    final OffsetManager.OffsetManagerEntry offsetManagerEntry =
        mock(OffsetManager.OffsetManagerEntry.class);

    try (AmqpSourceData underTest = new AmqpSourceData(sourceConfig, offsetManager)) {

      EvolvingSourceRecord record =
          new EvolvingSourceRecord(sourceNativeInfo, offsetManagerEntry, context);

      EvolvingSourceRecord actual = underTest.initialize(record);
      assertThat(actual.getValue().schema().type()).isEqualTo(Schema.Type.BYTES);
      Optional<Object> optObj = AmqpCommonConfig.getCommonConverter().decode(actual.getValue());
      assertThat(optObj).isPresent();
      byte[] actualValue = (byte[]) actual.getValue().value();
      assertThat(actualValue).isEqualTo(value);
    }
  }

  static List<Arguments> bodyDataTestData() {
    List<Arguments> result = new ArrayList<>();
    result.add(Arguments.of("Hello world".getBytes(StandardCharsets.UTF_8)));
    return result;
  }

  @ParameterizedTest
  @MethodSource("bodyMultiSectionTestData")
  void bodyMultiSectionTest(String name, Object obj1, Object obj2) throws Exception {
    ClientMessage<?> message = null;
    if (obj1 instanceof byte[]) {
      message = ClientMessage.create(new Data((byte[]) obj1));
      message.addBodySection(new Data((byte[]) obj2));
    } else {
      message = ClientMessage.create(new AmqpSequence<>((List<?>) obj1));
      message.addBodySection(new AmqpSequence<>((List<?>) obj2));
    }

    Delivery delivery = mock(Delivery.class);
    when(delivery.message()).thenReturn((Message) message);
    final AmqpSourceNativeInfo sourceNativeInfo = new AmqpSourceNativeInfo(delivery);
    final OffsetManager.OffsetManagerEntry offsetManagerEntry =
        mock(OffsetManager.OffsetManagerEntry.class);

    try (AmqpSourceData underTest = new AmqpSourceData(sourceConfig, offsetManager)) {
      EvolvingSourceRecord record =
          new EvolvingSourceRecord(sourceNativeInfo, offsetManagerEntry, context);

      EvolvingSourceRecord actual = underTest.initialize(record);
      assertThat(actual.getValue().schema().type()).isEqualTo(Schema.Type.ARRAY);
      assertThat(actual.getValue().schema().name()).isNull();
      Optional<Object> optObj = AmqpCommonConfig.getCommonConverter().decode(actual.getValue());
      assertThat(optObj).isPresent();
      Object[] actualValue = (Object[]) optObj.get();
      assertThat(actualValue).containsExactly(obj1, obj2);
    }
  }

  static List<Arguments> bodyMultiSectionTestData() {
    List<Arguments> data = new ArrayList<>();
    data.add(
        Arguments.of(
            "byte[]",
            "Hello world".getBytes(StandardCharsets.UTF_8),
            "Now is the time".getBytes(StandardCharsets.UTF_8)));
    data.add(Arguments.of("lists", List.of("Hello World", "Now is the time"), List.of(1, 2)));
    return data;
  }
}
