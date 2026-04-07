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

import org.apache.qpid.protonj2.client.Client;
import org.apache.qpid.protonj2.client.Connection;
import org.apache.qpid.protonj2.client.Receiver;
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
   */
  Receiver getReceiver(Connection connection) throws ClientException;
}
