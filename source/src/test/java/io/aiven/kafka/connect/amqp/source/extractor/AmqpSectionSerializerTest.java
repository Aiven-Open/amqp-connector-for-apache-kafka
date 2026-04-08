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
package io.aiven.kafka.connect.amqp.source.extractor;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import java.nio.charset.StandardCharsets;
import java.util.List;
import org.apache.qpid.protonj2.types.messaging.AmqpSequence;
import org.apache.qpid.protonj2.types.messaging.AmqpValue;
import org.apache.qpid.protonj2.types.messaging.Data;
import org.apache.qpid.protonj2.types.messaging.Section;
import org.junit.jupiter.api.Test;

public class AmqpSectionSerializerTest {
  private final ObjectMapper objectMapper;

  public AmqpSectionSerializerTest() {
    objectMapper = new ObjectMapper();
    SimpleModule module = new SimpleModule();
    module.addSerializer(Section.class, new AmqpSectionSerializer());
    objectMapper.registerModule(module);
  }

  @Test
  void testData() throws JsonProcessingException {
    Data data = new Data("Hello World".getBytes(StandardCharsets.UTF_8));
    String expected =
        objectMapper.writeValueAsString("Hello World".getBytes(StandardCharsets.UTF_8));
    String actual = objectMapper.writeValueAsString(data);
    assertThat(actual).isEqualTo(expected);
  }

  @Test
  void testAmpqSequence() throws JsonProcessingException {
    List<String> lst = List.of("Hello", "World");
    AmqpSequence<String> seq = new AmqpSequence<>(lst);
    String expected = objectMapper.writeValueAsString(lst);
    String actual = objectMapper.writeValueAsString(seq);
    assertThat(actual).isEqualTo(expected);
  }

  @Test
  void testAmqpValue() throws JsonProcessingException {
    AmqpValue<String> value = new AmqpValue<>("Hello World");
    String expected = objectMapper.writeValueAsString("Hello World");
    String actual = objectMapper.writeValueAsString(value);
    assertThat(actual).isEqualTo(expected);
  }
}
