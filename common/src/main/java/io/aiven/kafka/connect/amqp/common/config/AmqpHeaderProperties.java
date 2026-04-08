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

import io.aiven.commons.util.strings.CasedString;
import java.util.Locale;
import java.util.Objects;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;

/** The properties found in the message object that are not otherwise handled with maps. */
public enum AmqpHeaderProperties {
  /** the currently set Message Id or null if none set. */
  MESSAGE_ID(Schema.OPTIONAL_BYTES_SCHEMA),
  /** the currently set User ID or null if none set. */
  USER_ID(Schema.OPTIONAL_BYTES_SCHEMA),
  /** the currently set 'To' address which indicates the intended destination of the message. */
  TO(Schema.STRING_SCHEMA),
  /** the currently set subject metadata for this message or null if none set. */
  SUBJECT(Schema.OPTIONAL_STRING_SCHEMA),
  /**
   * the configured address of the node where replies to this message should be sent, or null if not
   * set.
   */
  REPLY_TO(Schema.OPTIONAL_STRING_SCHEMA),
  /** the currently assigned correlation ID or null if none set. */
  CORRELATION_ID(SchemaBuilder.OPTIONAL_BYTES_SCHEMA),
  /** the assigned content type value for the message body section or null if not set. */
  CONTENT_TYPE(Schema.OPTIONAL_STRING_SCHEMA),
  /** the assigned content encoding value for the message body section or null if not set */
  CONTENT_ENCODING(Schema.OPTIONAL_STRING_SCHEMA),
  /** the configured absolute time of expiration for this message */
  ABSOLUTE_EXPIRY(Schema.INT64_SCHEMA),
  /** the absolute time of creation for this message */
  CREATION_TIME(Schema.INT64_SCHEMA),
  /** the assigned group ID for this message or null if not set. */
  GROUP_ID(Schema.OPTIONAL_STRING_SCHEMA),
  /** the assigned group sequence for this message. */
  GROUP_SEQUENCE(Schema.INT32_SCHEMA),
  /**
   * the client-specific id used so that client can send replies to this message to a specific
   * group.
   */
  REPLY_TO_GROUP_ID(Schema.OPTIONAL_STRING_SCHEMA),
  /**
   * For an message being sent this method returns the current state of the durable flag on the
   * message. For a received message this method returns the durable flag value at the time of
   * sending (or false if not set) unless the value is updated after being received by the receiver
   */
  DURABLE(Schema.OPTIONAL_STRING_SCHEMA),
  /** if this message has been acquired by another link previously */
  FIRST_ACQUIRER(Schema.BOOLEAN_SCHEMA),
  /** the number of failed delivery attempts that this message has been part of */
  DELIVERY_COUNT(Schema.INT64_SCHEMA);

  private final Schema schema;
  private final CasedString casedName;

  AmqpHeaderProperties(Schema schema) {
    Objects.requireNonNull(schema);
    this.schema = schema;
    casedName = new CasedString(CasedString.StringCase.SNAKE, this.name().toLowerCase(Locale.ROOT));
  }

  /**
   * Gets the schema for this data item.
   *
   * @return the schema for this data item.
   */
  public Schema getSchema() {
    return this.schema;
  }

  /**
   * Gets the schema name for this data item.
   *
   * @return the schema name for this data item.
   */
  public String getSchemaName() {
    return casedName.toCase(CasedString.StringCase.PASCAL);
  }
}
