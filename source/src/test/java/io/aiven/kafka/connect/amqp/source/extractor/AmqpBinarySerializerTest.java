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
import org.apache.qpid.protonj2.types.Binary;
import org.junit.jupiter.api.Test;

public class AmqpBinarySerializerTest {

  private final ObjectMapper objectMapper;

  public AmqpBinarySerializerTest() {
    objectMapper = new ObjectMapper();
    SimpleModule module = new SimpleModule();
    module.addSerializer(Binary.class, new AmqpBinarySerializer());
    objectMapper.registerModule(module);
  }

  @Test
  void nullTest() throws JsonProcessingException {
    Binary binary = new Binary();
    String actual = objectMapper.writeValueAsString(binary);
    assertThat(actual).isEqualTo("null");
  }

  @Test
  void bytesTest() throws JsonProcessingException {
    Binary binary = new Binary("Hello".getBytes(StandardCharsets.UTF_8));
    String actual = objectMapper.writeValueAsString(binary);
    assertThat(actual).isEqualTo("\"SGVsbG8=\"");
  }
}
