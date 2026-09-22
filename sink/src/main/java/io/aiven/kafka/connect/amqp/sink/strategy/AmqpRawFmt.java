package io.aiven.kafka.connect.amqp.sink.strategy;

import io.aiven.kafka.connect.amqp.common.AmqpParseException;
import io.aiven.kafka.connect.amqp.sink.errant.ErrantRecordHandler;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.qpid.protonj2.client.Message;
import org.apache.qpid.protonj2.client.Sender;

public class AmqpRawFmt extends AbstractAmqpStrategy {

  public AmqpRawFmt(Sender sender, ErrantRecordHandler errantRecordHandler) {
    super(sender, errantRecordHandler);
  }

  @Override
  Message<?> createClientMessage(SinkRecord sinkRecord) throws AmqpParseException {
    if (sinkRecord.value() instanceof Message<?> message) {
      return message;
    }
    throw new AmqpParseException(
        String.format("class %s is not an AMQP message", sinkRecord.value().getClass()));
  }
}
