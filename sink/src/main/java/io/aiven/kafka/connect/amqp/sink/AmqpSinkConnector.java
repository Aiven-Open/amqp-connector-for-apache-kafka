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
package io.aiven.kafka.connect.amqp.sink;

import io.aiven.commons.kafka.config.fragment.CommonConfigFragment;
import io.aiven.kafka.connect.amqp.sink.config.AmqpSinkConfigDef;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.sink.SinkConnector;

/** The AMQP sink connector. */
public class AmqpSinkConnector extends SinkConnector {
  private Map<String, String> props;

  /** Constructor. */
  public AmqpSinkConnector() {}

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
