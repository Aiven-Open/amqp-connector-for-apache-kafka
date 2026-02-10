/*
         Copyright 2026 Aiven Oy and project contributors

        Licensed under the Apache License, Version 2.0 (the "License");
        you may not use this file except in compliance with the License.
        You may obtain a copy of the License at

        http://www.apache.org/licenses/LICENSE-2.0

        Unless required by applicable law or agreed to in writing,
        software distributed under the License is distributed on an
        "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
        KIND, either express or implied.  See the License for the
        specific language governing permissions and limitations
        under the License.

        SPDX-License-Identifier: Apache-2
 */
package io.aiven.kafka.connect.amqp.source.config;

import io.aiven.commons.kafka.config.ExtendedConfigKey;
import org.apache.kafka.common.config.ConfigDef;
import org.junit.jupiter.api.Test;

public class AmqpSourceConfigDefTest {

	@Test
	void sinceTest() {
		AmqpSourceConfigDef def = new AmqpSourceConfigDef();

		for (String keyName : def.configKeys().keySet()) {
			ConfigDef.ConfigKey key = def.configKeys().get(keyName);
			if (key instanceof ExtendedConfigKey exKey) {
				System.out.printf("%s: %s%n", keyName, exKey.getSince());
			}
		}
	}
}
