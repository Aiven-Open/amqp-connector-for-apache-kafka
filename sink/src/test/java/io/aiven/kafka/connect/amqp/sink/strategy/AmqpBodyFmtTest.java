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
package io.aiven.kafka.connect.amqp.sink.strategy;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.aiven.kafka.connect.amqp.common.KafkaRecordKey;
import io.aiven.kafka.connect.amqp.common.config.AmqpCommonConfig;
import io.aiven.kafka.connect.amqp.common.data.EncoderDecoder;
import io.aiven.kafka.connect.amqp.sink.errant.ErrantRecordHandler;
import io.aiven.kafka.connect.amqp.sink.errant.TestingErrantRecordReporter;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.header.ConnectHeaders;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.qpid.protonj2.client.Message;
import org.apache.qpid.protonj2.client.Sender;
import org.apache.qpid.protonj2.client.Tracker;
import org.apache.qpid.protonj2.client.exceptions.ClientException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

public class AmqpBodyFmtTest {

  private AmqpBodyFmt underTest;
  private Sender sender;
  private TestingErrantRecordReporter reporter;

  @BeforeEach
  void beforeEach() throws ClientException {
    sender = mock(Sender.class);
    reporter = new TestingErrantRecordReporter();
    ErrantRecordHandler errantRecordHandler = new ErrantRecordHandler(reporter);
    underTest = new AmqpBodyFmt(sender, errantRecordHandler);
  }

  @Test
  void writeTest() throws ClientException, ExecutionException, InterruptedException {
    Tracker tracker = mock(Tracker.class);
    when(tracker.settlementFuture()).thenReturn(CompletableFuture.completedFuture(tracker));
    when(sender.send(any(Message.class))).thenReturn(tracker);
    final int partition = 1;
    final int origPartition = 2;
    final long offset = 5000;
    final long origOffset = 4000;
    final long timestamp = System.currentTimeMillis();
    SinkRecord sinkRecord =
        new SinkRecord(
            "topic",
            partition,
            Schema.STRING_SCHEMA,
            "key",
            Schema.STRING_SCHEMA,
            "Hello World",
            offset,
            timestamp,
            TimestampType.CREATE_TIME,
            new ConnectHeaders(),
            "origTopic",
            origPartition,
            origOffset);

    underTest.write(sinkRecord);
    ArgumentCaptor<Message<?>> argumentCaptor = ArgumentCaptor.forClass(Message.class);
    verify(sender).send(argumentCaptor.capture());

    Message<?> message = argumentCaptor.getValue();
    assertThat(message.body()).isEqualTo("Hello World");
    assertThat(message.hasAnnotations()).isFalse();
    assertThat(message.hasFooters()).isFalse();
    assertThat(message.hasProperties()).isFalse();
    assertThat(underTest.commitMap).hasSize(1);
    KafkaRecordKey key = new KafkaRecordKey(sinkRecord);
    AmqpBodyFmt.TrackerSinkRecord trackerRecord =
        assertThat(underTest.commitMap.get(key)).isNotNull().actual();
    assertThat(trackerRecord.trackerFuture().get()).isEqualTo(tracker);
  }

  @Test
  void writeWithHeadersTest() throws ClientException, ExecutionException, InterruptedException {
    EncoderDecoder converter = AmqpCommonConfig.getCommonConverter();
    Tracker tracker = mock(Tracker.class);
    when(tracker.settlementFuture()).thenReturn(CompletableFuture.completedFuture(tracker));
    when(sender.send(any(Message.class))).thenReturn(tracker);
    final int partition = 1;
    final int origPartition = 2;
    final long offset = 5000;
    final long origOffset = 4000;
    final long timestamp = System.currentTimeMillis();

    final String messageId = "The Message Id";
    final String userId = "Bucaroo Bonzai";
    final String subject = "Gig tonight";
    ConnectHeaders headers = new ConnectHeaders();
    headers.add("amqp.messageId", converter.encode(messageId).orElseThrow());
    headers.add(
        "amqp.userId", converter.encode(userId.getBytes(StandardCharsets.UTF_8)).orElseThrow());
    headers.add("amqp.subject", converter.encode(subject).orElseThrow());

    SinkRecord sinkRecord =
        new SinkRecord(
            "topic",
            partition,
            Schema.STRING_SCHEMA,
            "key",
            Schema.STRING_SCHEMA,
            "Hello World",
            offset,
            timestamp,
            TimestampType.CREATE_TIME,
            headers,
            "origTopic",
            origPartition,
            origOffset);

    underTest.write(sinkRecord);
    ArgumentCaptor<Message<?>> argumentCaptor = ArgumentCaptor.forClass(Message.class);
    verify(sender).send(argumentCaptor.capture());

    Message<?> message = argumentCaptor.getValue();
    assertThat(message.body()).isEqualTo("Hello World");
    assertThat(message.hasAnnotations()).isFalse();
    assertThat(message.hasFooters()).isFalse();
    assertThat(message.hasProperties()).isFalse();
    assertThat(message.messageId()).isEqualTo(messageId);
    assertThat(message.userId()).isEqualTo(userId.getBytes(StandardCharsets.UTF_8));
    assertThat(message.subject()).isEqualTo(subject);

    assertThat(underTest.commitMap).hasSize(1);
    KafkaRecordKey key = new KafkaRecordKey(sinkRecord);
    AmqpBodyFmt.TrackerSinkRecord trackerRecord =
        assertThat(underTest.commitMap.get(key)).isNotNull().actual();
    assertThat(trackerRecord.trackerFuture().get()).isEqualTo(tracker);
  }

  @Test
  void errorTest() throws ClientException, ExecutionException, InterruptedException {
    Tracker tracker = mock(Tracker.class);
    when(tracker.settlementFuture()).thenReturn(CompletableFuture.completedFuture(tracker));
    when(sender.send(any(Message.class))).thenThrow(new ClientException("TestException"));
    final int partition = 1;
    final int origPartition = 2;
    final long offset = 5000;
    final long origOffset = 4000;
    final long timestamp = System.currentTimeMillis();
    SinkRecord sinkRecord =
        new SinkRecord(
            "topic",
            partition,
            Schema.STRING_SCHEMA,
            "key",
            Schema.STRING_SCHEMA,
            "Hello World",
            offset,
            timestamp,
            TimestampType.CREATE_TIME,
            new ConnectHeaders(),
            "origTopic",
            origPartition,
            origOffset);

    underTest.write(sinkRecord);
    ArgumentCaptor<Message<?>> argumentCaptor = ArgumentCaptor.forClass(Message.class);
    verify(sender, times(1)).send(argumentCaptor.capture());
    assertThat(underTest.commitMap).hasSize(0);
    List<Pair<SinkRecord, Throwable>> errors = reporter.getErrors();
    assertThat(errors).hasSize(1);
    assertThat(errors.get(0).getRight()).hasMessage("TestException");
    assertThat(errors.get(0).getLeft()).isEqualTo(sinkRecord);
  }
}
