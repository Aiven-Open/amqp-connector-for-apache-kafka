package io.aiven.kafka.connect.amqp.sink;

import io.aiven.kafka.connect.amqp.source.AmqpSinkVersionInfo;
import java.util.List;
import java.util.Map;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.sink.SinkConnector;

public class AmqpSinkConnector extends SinkConnector {
  @Override
  public void start(Map<String, String> props) {}

  @Override
  public Class<? extends Task> taskClass() {
    return AmqpSinkTask.class;
  }

  @Override
  public List<Map<String, String>> taskConfigs(int maxTasks) {
    return List.of();
  }

  @Override
  public void stop() {}

  @Override
  public ConfigDef config() {
    return null;
  }

  @Override
  public String version() {
    return AmqpSinkVersionInfo.VERSION;
  }
}
