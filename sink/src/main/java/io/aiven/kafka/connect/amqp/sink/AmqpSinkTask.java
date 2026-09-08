package io.aiven.kafka.connect.amqp.sink;

import io.aiven.kafka.connect.amqp.source.AmqpSinkVersionInfo;
import java.util.Collection;
import java.util.Map;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;

public class AmqpSinkTask implements SinkTask {
  @Override
  public String version() {
    return AmqpSinkVersionInfo.VERSION;
  }

  @Override
  public void start(Map<String, String> props) {}

  @Override
  public void put(Collection<SinkRecord> records) {}

  @Override
  public void stop() {}
}
