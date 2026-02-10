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

import io.aiven.commons.kafka.config.SinceInfoMapBuilder;
import io.aiven.commons.kafka.config.fragment.FragmentDataAccess;
import io.aiven.commons.kafka.connector.source.config.SourceCommonConfig;
import io.aiven.kafka.connect.amqp.common.config.AmqpFragment;
import org.apache.kafka.common.config.ConfigValue;

import java.util.Map;

public final class AmqpSourceConfigDef extends SourceCommonConfig.SourceCommonConfigDef {

	private AmqpSourceFragment sourceFragment;

	AmqpSourceConfigDef() {
		super();
		AmqpFragment.update(this);
		AmqpSourceFragment.update(this);
		applyOverrideMap();
	}

	private void applyOverrideMap() {
		SinceInfoMapBuilder mapBuilder = new SinceInfoMapBuilder();
		mapBuilder.put("io.aiven.commons:kafka-source-connector-framework:[,1.0.0]", "1.0.0");
		mapBuilder.applyTo(this);
	}

	/**
	 * Validates the Salesforce configuration is correct and meets requirements
	 *
	 * @param valueMap
	 *            the map of configuration names to values.
	 * @return the updated map.
	 */
	@Override
	public Map<String, ConfigValue> multiValidate(final Map<String, ConfigValue> valueMap) {
		Map<String, ConfigValue> values = super.multiValidate(valueMap);
		// validate that the config fragment options are good.
		FragmentDataAccess fragmentDataAccess = FragmentDataAccess.from(valueMap);
		new AmqpFragment(fragmentDataAccess).validate(values);
		return values;
	}

}
