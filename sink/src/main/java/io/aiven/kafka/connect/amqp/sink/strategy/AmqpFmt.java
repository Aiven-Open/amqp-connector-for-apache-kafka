package io.aiven.kafka.connect.amqp.sink.strategy;

import com.google.common.annotations.VisibleForTesting;
import io.aiven.kafka.connect.amqp.common.AmqpParseException;
import io.aiven.kafka.connect.amqp.common.KafkaRecordKey;
import io.aiven.kafka.connect.amqp.common.config.AmqpCommonConfig;
import io.aiven.kafka.connect.amqp.common.data.Converter;
import io.aiven.kafka.connect.amqp.sink.errant.ErrantRecordHandler;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.Future;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.header.Header;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.qpid.protonj2.client.Message;
import org.apache.qpid.protonj2.client.Sender;
import org.apache.qpid.protonj2.client.Tracker;
import org.apache.qpid.protonj2.client.exceptions.ClientException;
import org.apache.qpid.protonj2.client.impl.ClientMessage;
import org.apache.qpid.protonj2.types.messaging.AmqpSequence;
import org.apache.qpid.protonj2.types.messaging.AmqpValue;
import org.apache.qpid.protonj2.types.messaging.Data;
import org.apache.qpid.protonj2.types.messaging.Section;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Standard AMQP message strategy. This message assumes that:
 *
 * <ul>
 *   <li>The body is encoded with schema. If no schema is provided bytes are assumed.
 * </ul>
 */
public class AmqpFmt implements Strategy {
  private static final Logger LOGGER = LoggerFactory.getLogger(AmqpFmt.class);
  private final Converter converter;
  private final Sender sender;
  @VisibleForTesting final ConcurrentSkipListMap<KafkaRecordKey, TrackerSinkRecord> commitMap;
  private final ErrantRecordHandler errantRecordHandler;

  public AmqpFmt(Sender sender, ErrantRecordHandler errantRecordHandler) throws ClientException {
    converter = AmqpCommonConfig.getCommonConverter();
    this.sender = sender;
    commitMap = new ConcurrentSkipListMap<>();
    this.errantRecordHandler = errantRecordHandler;
  }

  @Override
  public void write(SinkRecord sinkRecord) {
    try {

      ClientMessage<?> message = createClientMessage(sinkRecord);
      for (Header h : sinkRecord.headers()) {
        parseHeader(message, h);
      }
      Future<Tracker> futureTracker = sender.send(message).settlementFuture();
      commitMap.put(
          new KafkaRecordKey(sinkRecord), new TrackerSinkRecord(futureTracker, sinkRecord));
    } catch (AmqpParseException | ClientException e) {
      errantRecordHandler.reportErrantRecord(sinkRecord, e);
    }
  }

  private ClientMessage<?> createClientMessage(SinkRecord sinkRecord) throws AmqpParseException {
    if (sinkRecord.value() == null) {
      return ClientMessage.create();
    }
    if (sinkRecord.valueSchema() != null) {
      return ClientMessage.create(parseBody(sinkRecord.valueSchema(), sinkRecord.value()));
    }
    // body must be string or byte[]
    if (sinkRecord.value() instanceof byte[] bytes) {
      return ClientMessage.create(new Data(bytes));
    }
    if (sinkRecord.value() instanceof String str) {
      return ClientMessage.create(new AmqpValue<>(str));
    }
    throw new AmqpParseException(
        String.format(
            "body value does not have a schema and is not a String or byte[]: %s",
            sinkRecord.value().getClass()));
  }

  @Override
  public Map<TopicPartition, OffsetAndMetadata> preCommit(
      Map<TopicPartition, OffsetAndMetadata> currentOffsets) {
    Map<TopicPartition, OffsetAndMetadata> result = new HashMap<>();
    currentOffsets.forEach(
        (tp, om) -> {
          KafkaRecordKey key = new KafkaRecordKey(tp, om.offset());
          KafkaRecordKey highestNoBreak = null;
          boolean sawBreak = false;
          /* check the key range remove all completed records before the suggested offset.  If there are any remaining return the
          record that occurred just before that.  If it is the first entry then don't commit any for that topic/partition pair.
          */
          ConcurrentNavigableMap<KafkaRecordKey, TrackerSinkRecord> subMap =
              commitMap.subMap(new KafkaRecordKey(tp, 0), true, key, true);
          for (Map.Entry<KafkaRecordKey, TrackerSinkRecord> entry : subMap.entrySet()) {
            if (entry.getValue().trackerFuture.isDone()) {
              if (entry.getValue().trackerFuture.isCancelled()) {
                errantRecordHandler.reportErrantRecord(
                    entry.getValue().sinkRecord, "Delivery cancelled");
              }
              subMap.remove(entry.getKey());
              if (!sawBreak) {
                highestNoBreak = entry.getKey();
              }
            } else {
              sawBreak = true;
            }
          }

          /* default case in this if block is not to commit any messages for the topic/partition. */
          if (subMap.isEmpty()) {
            result.put(tp, om);
          } else if (highestNoBreak != null) {
            result.put(
                new TopicPartition(highestNoBreak.topic(), highestNoBreak.partition()),
                new OffsetAndMetadata(highestNoBreak.offset()));
          }
        });
    return result;
  }

  @Override
  public void flush(Map<TopicPartition, OffsetAndMetadata> currentOffsets) {
    LOGGER.error("flush() Should not be called from the AmqpFmt strategy");
  }

  private Section<?> parseBody(Schema bodySchema, Object bodyValue) throws AmqpParseException {
    SchemaAndValue schemaAndValue = new SchemaAndValue(bodySchema, bodyValue);
    Optional<Object> optObj = converter.decode(schemaAndValue);
    if (optObj.isPresent()) {
      Object object = optObj.get();
      if (object instanceof List<?> lst) {
        if (lst.size() == 1) {
          if (lst.get(0) instanceof byte[] buffer) {
            return new Data(buffer);
          }
          return new AmqpValue<>(lst.get(0));
        }
        if (lst.size() > 1) {
          return new AmqpSequence<>(lst);
        } else {
          throw new AmqpParseException(
              String.format("Empty list extracted from %s with schema %s", bodyValue, bodySchema));
        }
      } else {
        return new AmqpValue<>(object);
      }
    } else {
      throw new AmqpParseException(
          String.format("Unable to extract data from %s with schema %s", bodyValue, bodySchema));
    }
  }

  private void setValue(String key, Message<?> message, Object value) throws ClientException {
    switch (key) {
      case "amqp.messageId" -> message.messageId(value);
      case "amqp.userId" -> message.to(value.toString());
      case "amqp.subject" -> message.subject(value.toString());
      case "amqp.replyTo" -> message.replyTo(value.toString());
      case "amqp.correlationId" -> message.correlationId(value);
      case "amqp.contentType" -> message.contentType(value.toString());
      case "amqp.contentEncoding" -> message.contentEncoding(value.toString());
      case "amqp.absoluteExpiry" -> {
        Optional<Number> n = getNumber(key, value);
        if (n.isPresent()) {
          message.absoluteExpiryTime(n.get().longValue());
        }
      }
      case "amqp.creationTime" -> {
        Optional<Number> n = getNumber(key, value);
        if (n.isPresent()) {
          message.creationTime(n.get().longValue());
        }
      }
      case "amqp.groupId" -> message.groupId(value.toString());
      case "amqp.groupSequence" -> {
        Optional<Number> n = getNumber(key, value);
        if (n.isPresent()) {
          message.groupSequence(n.get().intValue());
        }
      }
      case "amqp.replyToGroupId" -> message.replyToGroupId(value.toString());
      case "amqp.durable" -> message.durable(getBoolean(key, value));
      case "amqp.firstAcquirer" -> message.firstAcquirer(getBoolean(key, value));
      case "amqp.deliveryCount" -> {
        Optional<Number> n = getNumber(key, value);
        if (n.isPresent()) {
          message.deliveryCount(n.get().longValue());
        }
      }
    }
  }

  private Optional<Number> getNumber(String key, Object object) {
    if (object instanceof Number number) {
      return Optional.of(number);
    }
    LOGGER.error("Key {} is not a Number: {}", key, object.getClass());
    return Optional.empty();
  }

  private boolean getBoolean(String key, Object object) {
    return object instanceof Boolean number ? number : Boolean.parseBoolean(object.toString());
  }

  private void parseHeader(Message<?> message, Header header) throws ClientException {
    if (header.key().startsWith("amqp.")) {
      Optional<Object> decodeResult =
          converter.decode(new SchemaAndValue(header.schema(), header.value()));
      if (decodeResult.isPresent()) {
        setValue(header.key(), message, decodeResult.get());
      }
    }
  }

  record TrackerSinkRecord(Future<Tracker> trackerFuture, SinkRecord sinkRecord) {}
}
