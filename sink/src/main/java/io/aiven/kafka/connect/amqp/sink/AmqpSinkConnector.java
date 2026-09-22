package io.aiven.kafka.connect.amqp.sink;

import io.aiven.commons.kafka.config.fragment.CommonConfigFragment;
import io.aiven.kafka.connect.amqp.sink.config.AmqpSinkConfigDef;
import io.aiven.kafka.connect.amqp.source.AmqpSinkVersionInfo;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.sink.SinkConnector;

public class AmqpSinkConnector extends SinkConnector {
  Map<String, String> props;

  @Override
  public void start(Map<String, String> props) {
    this.props = props;
  }

  @Override
  public Class<? extends Task> taskClass() {
    return AmqpSinkTask.class;
  }

  @Override
  public List<Map<String, String>> taskConfigs(int maxTasks) {
    List<Map<String, String>> result = new ArrayList<>();
    for (int i = 0; i < maxTasks; i++) {
      result.add(CommonConfigFragment.setter(new HashMap<>(props)).taskId(i).data());
    }
    return result;
  }

  @Override
  public void stop() {}

  @Override
  public ConfigDef config() {
    return new AmqpSinkConfigDef();
  }

  @Override
  public String version() {
    return AmqpSinkVersionInfo.VERSION;
  }
}
