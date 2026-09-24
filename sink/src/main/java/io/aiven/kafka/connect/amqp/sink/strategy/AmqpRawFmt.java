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

import io.aiven.kafka.connect.amqp.common.AmqpParseException;
import io.aiven.kafka.connect.amqp.sink.errant.ErrantRecordHandler;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.qpid.protonj2.client.Message;
import org.apache.qpid.protonj2.client.Sender;

/**
 * A write strategy that expects the AMQP message to be contained in the Kafka value byte stream.
 */
public class AmqpRawFmt extends AbstractAmqpStrategy {

  /**
   * Constructs the strategy.
   *
   * @param sender the sender to use to send AMQP messages.
   * @param errantRecordHandler the handler for sink records with errors.
   */
  public AmqpRawFmt(Sender sender, ErrantRecordHandler errantRecordHandler) {
    super(sender, errantRecordHandler);
  }

  @Override
  Message<?> createClientMessage(SinkRecord sinkRecord) throws AmqpParseException {
    if (sinkRecord.value() instanceof Message<?> message) {
      return message;
    }
    throw new AmqpParseException(
        String.format("class %s is not an AMQP message", sinkRecord.value().getClass()));
  }
}
