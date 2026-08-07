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

import io.aiven.commons.kafka.config.fragment.CommonConfigFragment;
import io.aiven.kafka.connect.amqp.source.config.AmqpSourceConfigDef;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.source.SourceConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** The AMQP source connector */
public final class AmqpSourceConnector extends SourceConnector {
  private static final Logger LOGGER = LoggerFactory.getLogger(AmqpSourceConnector.class);

  /** The map of property name to values as passed during {@link #start(Map)} */
  private Map<String, String> props;

  /** Default constructor */
  public AmqpSourceConnector() {
    LOGGER.debug("AmqpSourceConnector constructed");
  }

  @Override
  public void start(Map<String, String> props) {
    LOGGER.info("{} {} Starting", AmqpSourceVersionInfo.NAME, AmqpSourceVersionInfo.VERSION);
    this.props = props;
  }

  @Override
  public Class<? extends Task> taskClass() {
    return AmqpSourceTask.class;
  }

  @Override
  public List<Map<String, String>> taskConfigs(int maxTasks) {
    CommonConfigFragment.setter(props).maxTasks(maxTasks);
    List<Map<String, String>> configs = new ArrayList<>(maxTasks);
    for (int i = 0; i < maxTasks; i++) {
      Map<String, String> taskConfig = new HashMap<>(props);
      CommonConfigFragment.setter(taskConfig).taskId(i);
      configs.add(taskConfig);
    }
    return configs;
  }

  @Override
  public void stop() {
    LOGGER.info("{} {} Stopping", AmqpSourceVersionInfo.NAME, AmqpSourceVersionInfo.VERSION);
  }

  @Override
  public ConfigDef config() {
    return new AmqpSourceConfigDef();
  }

  @Override
  public String version() {
    return AmqpSourceVersionInfo.VERSION;
  }
}
