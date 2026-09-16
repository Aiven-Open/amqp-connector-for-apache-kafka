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
package io.aiven.kafka.connect.amqp.common.config;

import io.aiven.kafka.connect.amqp.common.data.AmqpConverter;
import io.aiven.kafka.connect.amqp.common.data.CollectionConverter;
import io.aiven.kafka.connect.amqp.common.data.Converter;
import io.aiven.kafka.connect.amqp.common.data.KafkaConverter;
import io.aiven.kafka.connect.amqp.common.data.UniqueTypeConverter;
import java.util.concurrent.ExecutionException;
import org.apache.qpid.protonj2.client.Client;
import org.apache.qpid.protonj2.client.Connection;
import org.apache.qpid.protonj2.client.Receiver;
import org.apache.qpid.protonj2.client.Sender;
import org.apache.qpid.protonj2.client.exceptions.ClientException;

/** The methods that must be implemented by both source and sink. */
public interface AmqpCommonConfig {

  /**
   * Creates a AMQP client. Must be closed when finished.
   *
   * @return a newly constructed client.
   */
  Client getClient();

  /**
   * Creates a new Connection to an AMQP client.
   *
   * @param client the client to connect to.
   * @return the Connection. Must be closed when finished.
   * @throws ClientException if the AMQP connection can not be established.
   */
  Connection getConnection(Client client) throws ClientException;

  /**
   * Creates a new AMQP Receiver.
   *
   * @param connection the AMQP connection to use for the receiver.
   * @return the new AMQP Receiver. Must be closed when finished.
   * @throws ClientException if the AMQP receiver can not be created.
   * @throws ExecutionException If the receiver could not be created.
   * @throws InterruptedException If the remote server was interrupted.
   */
  Receiver getReceiver(Connection connection)
      throws ClientException, ExecutionException, InterruptedException;

  /**
   * Creates a new AMQP Receiver by creating and using a Client and Connection.
   *
   * @return the new AMQP Receiver. Must be closed when finished.
   * @throws ClientException if the AMQP receiver can not be created.
   * @throws ExecutionException If the receiver could not be created.
   * @throws InterruptedException If the remote server was interrupted.
   */
  default Receiver getReceiver() throws ClientException, ExecutionException, InterruptedException {
    return getReceiver(getConnection(getClient()));
  }

  /**
   * Creates a new AMQP Sender.
   *
   * @param connection the AMQP connection to use for the sender.
   * @return the new AMQP sender. Must be closed when finished.
   * @throws ClientException if the AMQP sender can not be created.
   * @throws ExecutionException If the sender could not be created.
   * @throws InterruptedException If the remote server was interrupted.
   */
  Sender getSender(Connection connection)
      throws ClientException, ExecutionException, InterruptedException;

  /**
   * Creates a new AMQP Sender by creating and using a Client and Connection.
   *
   * @return the new AMQP sender. Must be closed when finished.
   * @throws ClientException if the AMQP sender can not be created.
   * @throws ExecutionException If the sender could not be created.
   * @throws InterruptedException If the remote server was interrupted.
   */
  default Sender getSender() throws ClientException, ExecutionException, InterruptedException {
    return getSender(getConnection(getClient()));
  }

  /**
   * Creates the common converter with AMQP, UniqueType, Kafka, and Collection converters.
   *
   * @return the common converter with AMQP, UniqueType, Kafka, and Collection converters.
   */
  static Converter getCommonConverter() {
    return new Converter.ChainedConverter(
        new AmqpConverter(),
        new UniqueTypeConverter(),
        new KafkaConverter(),
        new CollectionConverter());
  }
}
