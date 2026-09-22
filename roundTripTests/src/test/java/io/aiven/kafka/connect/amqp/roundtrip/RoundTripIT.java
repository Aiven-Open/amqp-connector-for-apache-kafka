package io.aiven.kafka.connect.amqp.roundtrip;

import static org.assertj.core.api.Assertions.assertThat;

import io.aiven.commons.kafka.config.fragment.CommonConfigFragment;
import io.aiven.commons.kafka.connector.source.config.SourceConfigFragment;
import io.aiven.commons.kafka.testkit.KafkaIntegrationTestBase;
import io.aiven.commons.kafka.testkit.KafkaManager;
import io.aiven.kafka.connect.amqp.AmqpConverter;
import io.aiven.kafka.connect.amqp.common.config.AmqpCommonConfig;
import io.aiven.kafka.connect.amqp.common.config.AmqpFormat;
import io.aiven.kafka.connect.amqp.common.config.AmqpFragment;
import io.aiven.kafka.connect.amqp.common.data.HeaderExtractor;
import io.aiven.kafka.connect.amqp.common.integration.IntegrationTestSetup;
import io.aiven.kafka.connect.amqp.sink.AmqpSinkConnector;
import io.aiven.kafka.connect.amqp.sink.config.AmqpSinkConfigDef;
import io.aiven.kafka.connect.amqp.sink.config.AmqpStrategy;
import io.aiven.kafka.connect.amqp.source.AmqpSourceConnector;
import java.io.IOException;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.connect.header.ConnectHeaders;
import org.apache.kafka.connect.header.Header;
import org.apache.kafka.connect.runtime.SinkConnectorConfig;
import org.apache.kafka.connect.runtime.SourceConnectorConfig;
import org.apache.kafka.connect.storage.SimpleHeaderConverter;
import org.apache.kafka.connect.storage.StringConverter;
import org.apache.qpid.protonj2.client.Client;
import org.apache.qpid.protonj2.client.Connection;
import org.apache.qpid.protonj2.client.ConnectionOptions;
import org.apache.qpid.protonj2.client.Delivery;
import org.apache.qpid.protonj2.client.Message;
import org.apache.qpid.protonj2.client.Receiver;
import org.apache.qpid.protonj2.client.Sender;
import org.apache.qpid.protonj2.client.Tracker;
import org.apache.qpid.protonj2.client.exceptions.ClientException;
import org.apache.qpid.protonj2.types.UnsignedByte;
import org.apache.qpid.protonj2.types.UnsignedInteger;
import org.apache.qpid.protonj2.types.UnsignedLong;
import org.apache.qpid.protonj2.types.UnsignedShort;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.rabbitmq.RabbitMQContainer;

@Testcontainers
public class RoundTripIT extends KafkaIntegrationTestBase {

  private static final BigInteger TWO_TO_THE_SIXTY_FOUR =
      new BigInteger(
          new byte[] {
            (byte) 1, (byte) 0, (byte) 0, (byte) 0, (byte) 0, (byte) 0, (byte) 0, (byte) 0, (byte) 0
          });

  @Container static RabbitMQContainer rabbit = IntegrationTestSetup.rabbitMQContainer();

  public RoundTripIT() {
    rabbit.start();
  }

  private void setupKafka(AmqpFormat sourceFormat, AmqpFormat sinkFormat, AmqpStrategy sinkStrategy)
      throws IOException {
    KafkaManager kafkaManager = setupKafka(null, Collections.emptyMap());
    kafkaManager.createTopic(getTopic());
    kafkaManager.configureConnector("Amqp-source", sourceConfig(sourceFormat));
    kafkaManager.configureConnector("Amqp-sink", sinkConfig(sinkFormat, sinkStrategy));
  }

  private Map<String, String> amqpConfig(String direction) {
    Map<String, String> data = new HashMap<>();
    AmqpFragment.setter(data)
        .setAddress(getTopic(direction))
        .setHost(rabbit.getHost())
        .setPort(rabbit.getAmqpPort())
        .setUser(rabbit.getAdminUsername())
        .setPassword(rabbit.getAdminPassword());
    return data;
  }

  private Map<String, String> sourceConfig(AmqpFormat amqpFormat) {
    Map<String, String> data = amqpConfig("source");
    AmqpFragment.setter(data).setMessageFormat(amqpFormat);
    SourceConfigFragment.setter(data).targetTopic(getTopic());
    CommonConfigFragment.setter(data).maxTasks(1);
    data.put(SourceConnectorConfig.CONNECTOR_CLASS_CONFIG, AmqpSourceConnector.class.getName());
    data.put(SourceConnectorConfig.KEY_CONVERTER_CLASS_CONFIG, StringConverter.class.getName());
    switch (amqpFormat) {
      case RAW ->
          data.put(
              SourceConnectorConfig.VALUE_CONVERTER_CLASS_CONFIG, AmqpConverter.class.getName());
      case NOT_AMQP, BODY ->
          data.put(
              SourceConnectorConfig.VALUE_CONVERTER_CLASS_CONFIG, StringConverter.class.getName());
    }
    return data;
  }

  private Map<String, String> sinkConfig(AmqpFormat amqpFormat, AmqpStrategy strategy) {
    Map<String, String> data = amqpConfig("sink");
    new AmqpSinkConfigDef.Setter(data).strategy(strategy);

    AmqpFragment.setter(data).setMessageFormat(amqpFormat);

    CommonConfigFragment.setter(data).maxTasks(1);
    data.put(SinkConnectorConfig.CONNECTOR_CLASS_CONFIG, AmqpSinkConnector.class.getName());
    data.put("topics", getTopic());
    data.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    switch (amqpFormat) {
      case RAW ->
          data.put(SinkConnectorConfig.VALUE_CONVERTER_CLASS_CONFIG, AmqpConverter.class.getName());
      case NOT_AMQP, BODY ->
          data.put(
              SinkConnectorConfig.VALUE_CONVERTER_CLASS_CONFIG, StringConverter.class.getName());
    }
    return data;
  }

  @ParameterizedTest
  @MethodSource("roundTripData")
  void roundTrip(AmqpFormat sourceFormat, AmqpFormat sinkFormat, AmqpStrategy sinkStrategy)
      throws ClientException, IOException {
    setupKafka(sourceFormat, sinkFormat, sinkStrategy);
    Message<?> expected = createMessage();

    try (Client client = Client.create();
        Connection connection =
            client.connect(
                rabbit.getHost(),
                rabbit.getAmqpPort(),
                new ConnectionOptions()
                    .user(rabbit.getAdminUsername())
                    .password(rabbit.getAdminPassword()))) {
      // start both source and sink so we don't miss a message
      Sender sender = connection.openSender(getTopic("source"));
      Receiver receiver = connection.openReceiver(getTopic("sink"));

      // send the message and wait for it to be received.
      Tracker sendTracker = sender.send(expected);
      Delivery delivery = receiver.receive(2, TimeUnit.MINUTES);
      Message<?> actual = delivery.message();
      switch (sinkStrategy) {
        case RAW -> assertSameRaw(actual, expected);
        case BODY -> assertSameBody(actual, expected, sinkFormat);
      }
    }
  }

  static List<Arguments> roundTripData() {
    List<Arguments> result = new ArrayList<>();
    result.add(Arguments.of(AmqpFormat.RAW, AmqpFormat.RAW, AmqpStrategy.RAW));
    result.add(Arguments.of(AmqpFormat.RAW, AmqpFormat.RAW, AmqpStrategy.BODY));
    result.add(Arguments.of(AmqpFormat.BODY, AmqpFormat.BODY, AmqpStrategy.BODY));
    return result;
  }

  private Message<?> createMessage() throws ClientException {
    final long absoluteExpiry = 1788780705417L;
    final long creationTime = 1788780700417L;
    final long deliveryCount = 14L;
    final int groupSequence = 700;
    final byte priority = 0x2;
    final long timeToLive = 50000L;
    final UUID uuid = UUID.randomUUID();
    final long longValue = 1000L;
    final int intValue = 500;
    final short shortValue = 250;
    final byte byteValue = 0x7f;
    final BigInteger unsignedLong = TWO_TO_THE_SIXTY_FOUR.add(BigInteger.valueOf(-1L));
    final long unsignedInt = 0xffffffffL;
    final int unsignedShort = 0xffff;
    final short unsignedByte = 0xff;

    Message<?> message =
        Message.create("Hello world")
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
            .userId(rabbit.getAdminUsername().getBytes(StandardCharsets.UTF_8))
            //                .annotation("response‑address‑cookie", "My
            // cookie".getBytes(StandardCharsets.UTF_8))
            //                .annotation("int", intValue)
            //                .annotation("short", shortValue)
            //                .annotation("byte", byteValue)
            //                .annotation("unsignedLong", UnsignedLong.valueOf(unsignedLong))
            //                .annotation("unsignedInt", UnsignedInteger.valueOf(unsignedInt))
            //                .annotation("unsignedShort", UnsignedShort.valueOf(unsignedShort))
            //                .annotation("unsignedByte", UnsignedByte.valueOf((byte) unsignedByte))
            .footer("long", longValue)
            .footer("int", intValue)
            .footer("short", shortValue)
            .footer("byte", byteValue)
            .footer("unsignedLong", UnsignedLong.valueOf(unsignedLong))
            .footer("unsignedInt", UnsignedInteger.valueOf(unsignedInt))
            .footer("unsignedShort", UnsignedShort.valueOf(unsignedShort))
            .footer("unsignedByte", UnsignedByte.valueOf((byte) unsignedByte));
    return message;
  }

  private void assertSameRaw(Message<?> actual, Message<?> expected) throws ClientException {
    assertThat(actual.absoluteExpiryTime())
        .as("absoluteExpiryTime")
        .isEqualTo(expected.absoluteExpiryTime());
    assertThat(actual.to()).as("to").isEqualTo(expected.to());
    assertThat(actual.messageId()).as("messageId").isEqualTo(expected.messageId());
    assertThat(actual.contentEncoding())
        .as("contentEncoding")
        .isEqualTo(expected.contentEncoding());
    assertThat(actual.contentType()).as("contentType").isEqualTo(expected.contentType());
    assertThat(actual.correlationId()).as("correlationId").isEqualTo(expected.correlationId());
    assertThat(actual.creationTime()).as("creationTime").isEqualTo(expected.creationTime());
    // assertThat(actual.deliveryCount()).as("deliveryCount").isEqualTo(expected.deliveryCount());
    assertThat(actual.durable()).as("durable").isEqualTo(expected.durable());
    // assertThat(actual.firstAcquirer()).as("firstAcquirer").isEqualTo(expected.firstAcquirer());
    assertThat(actual.groupId()).as("groupId").isEqualTo(expected.groupId());
    assertThat(actual.groupSequence()).as("groupSequence").isEqualTo(expected.groupSequence());
    assertThat(actual.priority()).as("priority").isEqualTo(expected.priority());
    assertThat(actual.replyTo()).as("replyTo").isEqualTo(expected.replyTo());
    assertThat(actual.replyToGroupId()).as("replyToGroupId").isEqualTo(expected.replyToGroupId());
    assertThat(actual.subject()).as("subject").isEqualTo(expected.subject());
    assertThat(actual.timeToLive()).as("timeToLive").isEqualTo(expected.timeToLive());
    assertThat(actual.userId()).as("userId").isEqualTo(expected.userId());

    Map<String, Object> actualAnnotations = new TreeMap<>();
    actual.forEachAnnotation(actualAnnotations::put);
    Map<String, Object> expectedAnnotations = new TreeMap<>();
    expected.forEachAnnotation(expectedAnnotations::put);
    assertThat(actualAnnotations).containsAllEntriesOf(expectedAnnotations);

    Map<String, Object> actualFooters = new TreeMap<>();
    actual.forEachFooter(actualFooters::put);
    Map<String, Object> expectedFooters = new TreeMap<>();
    expected.forEachFooter(expectedFooters::put);
    assertThat(actualFooters).containsExactlyEntriesOf(expectedFooters);

    assertThat(actual.body()).isEqualTo(expected.body());
  }

  private void assertSameBody(Message<?> actual, Message<?> expected, AmqpFormat sinkFormat)
      throws ClientException {
    assertThat(actual.absoluteExpiryTime())
        .as("absoluteExpiryTime")
        .isEqualTo(expected.absoluteExpiryTime());
    assertThat(actual.to()).as("to").isEqualTo(expected.to());
    assertThat(actual.messageId()).as("messageId").isEqualTo(expected.messageId());
    assertThat(actual.contentEncoding())
        .as("contentEncoding")
        .isEqualTo(expected.contentEncoding());
    assertThat(actual.contentType()).as("contentType").isEqualTo(expected.contentType());
    assertThat(actual.correlationId()).as("correlationId").isEqualTo(expected.correlationId());
    assertThat(actual.creationTime()).as("creationTime").isEqualTo(expected.creationTime());
    //
    // assertThat(actual.deliveryCount()).as("deliveryCount").isEqualTo(expected.deliveryCount());
    assertThat(actual.durable()).as("durable").isEqualTo(expected.durable());
    //
    // assertThat(actual.firstAcquirer()).as("firstAcquirer").isEqualTo(expected.firstAcquirer());
    assertThat(actual.groupId()).as("groupId").isEqualTo(expected.groupId());
    assertThat(actual.groupSequence()).as("groupSequence").isEqualTo(expected.groupSequence());
    // assertThat(actual.priority()).as("priority").isEqualTo(expected.priority());
    assertThat(actual.replyTo()).as("replyTo").isEqualTo(expected.replyTo());
    assertThat(actual.replyToGroupId()).as("replyToGroupId").isEqualTo(expected.replyToGroupId());
    assertThat(actual.subject()).as("subject").isEqualTo(expected.subject());
    //        assertThat(actual.timeToLive()).as("timeToLive").isEqualTo(expected.timeToLive());
    //        assertThat(actual.userId()).as("userId").isEqualTo(expected.userId());

    Map<String, Object> actualAnnotations = new TreeMap<>();
    actual.forEachAnnotation(actualAnnotations::put);
    Map<String, Object> expectedAnnotations = new TreeMap<>();
    expected.forEachAnnotation(expectedAnnotations::put);
    assertThat(actualAnnotations).containsAllEntriesOf(expectedAnnotations);

    Map<String, Object> actualFooters = new TreeMap<>();
    actual.forEachFooter(actualFooters::put);
    Map<String, Object> expectedFooters = new TreeMap<>();
    expected.forEachFooter(expectedFooters::put);
    if (sinkFormat != AmqpFormat.RAW) {
      // expected modifications
      expectedFooters.put("int", (short) 500);
      expectedFooters.put("long", (short) 1000);
      expectedFooters.put("unsignedByte", (short) 255);
      expectedFooters.put("unsignedInt", 4294967295L);
      expectedFooters.put("unsignedLong", "18446744073709551615");
      expectedFooters.put("unsignedShort", 65535);
    }
    assertThat(actualFooters).containsExactlyEntriesOf(expectedFooters);

    assertThat(actual.body()).isEqualTo(expected.body());
  }

  private ProducerRecord<String, String> createProducerRecord(final Message<?> message)
      throws IOException, ClientException {

    ConnectHeaders cHeaders = new ConnectHeaders();
    new HeaderExtractor(AmqpCommonConfig.getCommonConverter()).processHeaders(cHeaders, message);
    RecordHeaders recordHeaders = new RecordHeaders();
    try (SimpleHeaderConverter simpleConverter = new SimpleHeaderConverter()) {
      for (Header h : cHeaders) {
        recordHeaders.add(
            h.key(), simpleConverter.fromConnectHeader(null, h.key(), h.schema(), h.value()));
      }
    }

    return new ProducerRecord<>(
        getTopic(),
        0,
        System.currentTimeMillis(),
        "nonAmqp",
        message.body().toString(),
        recordHeaders);
  }

  @Test
  void notAmqpData() throws ClientException, IOException {
    KafkaManager kafkaManager = setupKafka(null, Collections.emptyMap());
    kafkaManager.createTopic(getTopic());
    kafkaManager.configureConnector(
        "Amqp-sink", sinkConfig(AmqpFormat.NOT_AMQP, AmqpStrategy.BODY));
    Message<?> expected = createMessage();

    // Set up the producer properties
    Properties props = new Properties();
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaManager.bootstrapServers());
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

    ProducerRecord<String, String> producerRecord = createProducerRecord(expected);
    try (Client client = Client.create();
        Connection connection =
            client.connect(
                rabbit.getHost(),
                rabbit.getAmqpPort(),
                new ConnectionOptions()
                    .user(rabbit.getAdminUsername())
                    .password(rabbit.getAdminPassword()))) {

      Receiver receiver = connection.openReceiver(getTopic("sink"));

      // Create the producer
      try (Producer<String, String> producer = new KafkaProducer<>(props); ) {
        // Send the record
        producer.send(producerRecord);
      }

      // send the message and wait for it to be received.
      Delivery delivery = receiver.receive(2, TimeUnit.MINUTES);
      Message<?> actual = delivery.message();
      assertSameBody(actual, expected, AmqpFormat.NOT_AMQP);
    }
  }
}
