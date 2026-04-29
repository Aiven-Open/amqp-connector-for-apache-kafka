/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.aiven.kafka.connect.amqp.source.extractor;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;
import java.io.IOException;
import org.apache.qpid.protonj2.types.Decimal32;

/** A AmqpBinarySerializer wrapper that presents an immutable view of a payload. */
public final class AmqpDecimal32Serializer extends StdSerializer<Decimal32> {
  /** Default constructor. */
  public AmqpDecimal32Serializer() {
    super(Decimal32.class);
  }

  @Override
  public void serialize(Decimal32 decimal, JsonGenerator gen, SerializerProvider provider)
      throws IOException {
    gen.writeNumber(decimal.floatValue());
  }
}
