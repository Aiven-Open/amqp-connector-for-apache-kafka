package io.aiven.kafka.connect.amqp.sink;

import io.aiven.kafka.connect.amqp.sink.config.AmqpSinkConfig;
import io.aiven.kafka.connect.amqp.sink.errant.ErrantRecordHandler;
import io.aiven.kafka.connect.amqp.sink.strategy.AmqpBodyFmt;
import io.aiven.kafka.connect.amqp.sink.strategy.AmqpRawFmt;
import io.aiven.kafka.connect.amqp.sink.strategy.Strategy;
import io.aiven.kafka.connect.amqp.source.AmqpSinkVersionInfo;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.apache.kafka.connect.sink.SinkTaskContext;
import org.apache.qpid.protonj2.client.exceptions.ClientException;

/**
 * An AMQP sink task that implements a single strategy.
 */
public class AmqpSinkTask extends SinkTask {
  private Strategy strategy;
  private ErrantRecordHandler errantRecordHandler;
  private AmqpSinkConfig config;

  @Override
  public void initialize(SinkTaskContext context) {
    super.initialize(context);
    errantRecordHandler = new ErrantRecordHandler(context.errantRecordReporter());
  }

  @Override
  public String version() {
    return AmqpSinkVersionInfo.VERSION;
  }

  @Override
  public void start(Map<String, String> props) {
    config = new AmqpSinkConfig(props);
    try {
      switch (config.getWriteStrategy()) {
        case BODY -> strategy = new AmqpBodyFmt(config.getSender(), errantRecordHandler);
        case RAW -> strategy = new AmqpRawFmt(config.getSender(), errantRecordHandler);
      }
    } catch (ClientException | ExecutionException | InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void put(Collection<SinkRecord> records) {
    strategy.write(records);
  }

  @Override
  public Map<TopicPartition, OffsetAndMetadata> preCommit(
      Map<TopicPartition, OffsetAndMetadata> currentOffsets) {
    return strategy.preCommit(currentOffsets);
  }

  @Override
  public void stop() {}
}
