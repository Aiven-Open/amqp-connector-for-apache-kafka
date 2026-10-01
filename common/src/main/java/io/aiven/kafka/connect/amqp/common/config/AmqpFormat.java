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

/** Definition of the AMQP input/output format. */
public enum AmqpFormat {
  /**
   * AMQP message is encoded into its native byte buffer format and passed to Kafka as a byte[], the
   * Key is the key specified by the AMQP message or an ULID string if none was provided. All
   * properties, annotations, and footers are copied into Kafka message headers with values prefixed
   * by 'amqp.'
   */
  RAW(
      """
            AMQP message is encoded into its native byte buffer format and passed to Kafka as a byte[], the Key is the key specified by the AMQP message or an ULID string if none was provided  All properties, annotations, and footers are copied into Kafka message headers with values prefixed by 'amqp.'"""),
  /**
   * The body of the AMQP message is extracted and placed into the Kafka value, the Kafka value
   * schema is set appropriately for the value, the Key is the key specified by the AMQP message or
   * an ULID string if none was provided. All properties, annotations, and footers are copied into
   * Kafka message headers with values prefixed by 'amqp.'
   */
  BODY(
      """
            The body of the AMQP message is extracted and placed into the Kafka value, the Kafka value schema is set appropriately for the value, the Key is the key specified by the AMQP message or an ULID string if none was provided.  All properties, annotations, and footers are copied into Kafka message headers with values prefixed by 'amqp.'.
            """),
  /**
   * (Only valid for sink) The Kafka message did not an originate as an AMQP message. The AMQP body
   * will be set with the value from the Kafka message, the AMQP key will be set with the key from
   * the Kafka message. All Kafka headers that are prefixed with "amqp." and are associated with
   * AMQP properties, annotations, or footers will be used to set the appropriate values in the AMQP
   * message.
   */
  NOT_AMQP(
      """
            (Only valid for sink) The Kafka message did not an originate as an AMQP message.  The AMQP body will be set with the value from the Kafka message, the AMQP key will be set with the key from the Kafka message.
            All Kafka headers that are prefixed with "amqp." and are associated with AMQP properties, annotations, or footers will be used to set the appropriate values in the AMQP message.
            """);

  final String description;

  /**
   * Constructor.
   *
   * @param description the description for the use of this format.
   */
  AmqpFormat(String description) {
    this.description = description;
  }

  /**
   * Gets the description of the use of this format.
   *
   * @return the description of the use of this format.
   */
  public String getDescription() {
    return description;
  }
}
