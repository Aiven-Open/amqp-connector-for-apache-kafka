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
package io.aiven.kafka.connect.amqp.source.ctest;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.sink.SinkConnector;

public class DummyConnector extends SinkConnector {

  static AtomicInteger startCount = new AtomicInteger();
  static AtomicInteger stopCount = new AtomicInteger();
  static AtomicInteger configCount = new AtomicInteger();

  public DummyConnector() {}

  @Override
  public void start(Map<String, String> props) {
    startCount.incrementAndGet();
  }

  @Override
  public Class<? extends Task> taskClass() {
    return DummyTask.class;
  }

  @Override
  public List<Map<String, String>> taskConfigs(int maxTasks) {
    configCount.incrementAndGet();
    List<Map<String, String>> lst = new ArrayList<>();
    for (int i = 0; i < maxTasks; i++) {
      lst.add(Map.of("task.id", Integer.toString(i)));
    }
    return lst;
  }

  @Override
  public void stop() {
    stopCount.incrementAndGet();
  }

  @Override
  public ConfigDef config() {
    return new ConfigDef();
  }

  @Override
  public String version() {
    return "Dummy Connector";
  }
}
