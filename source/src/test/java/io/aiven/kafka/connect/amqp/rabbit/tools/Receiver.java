package io.aiven.kafka.connect.amqp.rabbit.tools;

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
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.aiven.kafka.connect.amqp.common.config.AmqpFragment;
import io.aiven.kafka.connect.amqp.source.AmqpSourceTask;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTaskContext;
import org.apache.kafka.connect.storage.OffsetStorageReader;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

@Disabled("Not a real test")
public class Receiver {
  Map<String, String> props;
  AmqpSourceTask task;

  Receiver() {
    SourceTaskContext context = mock(SourceTaskContext.class);
    when(context.offsetStorageReader()).thenReturn(mock(OffsetStorageReader.class));
    when(context.offsetStorageReader().offset(anyMap())).thenReturn(null);

    props = new HashMap<>();
    AmqpFragment.setter(props)
        .setHost("localhost")
        .setUser("guest")
        .setPassword("guest")
        .setPort(5672)
        .setAddress("hello");
    task = new AmqpSourceTask();
    task.initialize(context);
  }

  @Test
  void start() {
    task.start(props);
    final List<SourceRecord> result = new ArrayList<>();
    await()
        .atMost(Duration.ofMinutes(5))
        .untilAsserted(
            () -> {
              List<SourceRecord> pollResult = task.poll();
              if (pollResult != null) {
                result.addAll(pollResult);
              }
              assertThat(result).isNotEmpty();
            });
    System.out.println(result.get(0));
  }
}
