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

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;
import java.io.IOException;
import org.apache.qpid.protonj2.types.messaging.Section;

/** Serializes sections of an AMQP message into a JSON format. */
public final class AmqpSectionSerializer extends StdSerializer<Section> {

  /** Default constructor. */
  public AmqpSectionSerializer() {
    this(null);
  }

  /**
   * Constructor for a specific section type.
   *
   * @param t the section type class.
   */
  public AmqpSectionSerializer(Class<Section> t) {
    super(t);
  }

  @Override
  public void serialize(Section section, JsonGenerator gen, SerializerProvider provider)
      throws IOException {
    gen.writeObject(section.getValue());
  }
}
