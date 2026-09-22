package io.aiven.kafka.connect.amqp.sink.strategy;

import io.aiven.kafka.connect.amqp.common.AmqpParseException;
import io.aiven.kafka.connect.amqp.sink.errant.ErrantRecordHandler;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

import org.apache.commons.codec.binary.Base64;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.header.Header;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.qpid.protonj2.client.Message;
import org.apache.qpid.protonj2.client.Sender;
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
public class AmqpBodyFmt extends AbstractAmqpStrategy {
  private static final Logger LOGGER = LoggerFactory.getLogger(AmqpBodyFmt.class);

  public AmqpBodyFmt(Sender sender, ErrantRecordHandler errantRecordHandler) throws ClientException {
    super(sender, errantRecordHandler);
  }

  ClientMessage<?> createClientMessage(SinkRecord sinkRecord) throws AmqpParseException, ClientException {
    ClientMessage<?> message = constructMessage(sinkRecord);
    for (Header h : sinkRecord.headers()) {
      parseHeader(message, h);
    }
    return message;
  }

  private ClientMessage<?> constructMessage(SinkRecord sinkRecord) throws AmqpParseException {
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
      case "amqp.messageId" -> {
        if (value instanceof String str) {
          try {
            message.messageId(UUID.fromString((str)));
          } catch (IllegalArgumentException expected) {
            message.messageId(value);
          }
        } else {
          message.messageId(value);
        }
      }
      case "amqp.userId" -> message.userId(Base64.decodeBase64((String)value));
      case "amqp.to" -> message.to(value.toString());
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
      case "amqp.footers" -> {
        Map<String,?> map = (Map<String, ?>) value;

        map.forEach((k, v) -> {
          try {
            message.footer(k, v);
          } catch (ClientException e) {
            LOGGER.error("Unable to write footer {}: {}", key, v);
          }
        });
      }
      case "amqp.annotations" -> {
        Map<String,?> map = (Map<String, ?>) value;

        map.forEach((k, v) -> {
          try {
            message.annotation(k, v);
          } catch (ClientException e) {
            LOGGER.error("Unable to write annotation {}: {}", key, v);
          }
        });
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
}
