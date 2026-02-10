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
import io.aiven.commons.kafka.config.SinceInfo;
import io.aiven.commons.kafka.config.fragment.AbstractFragmentSetter;
import io.aiven.commons.kafka.config.fragment.ConfigFragment;
import io.aiven.commons.kafka.config.fragment.FragmentDataAccess;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;

import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Stream;

import io.aiven.kafka.connect.amqp.common.config.AmqpHeaderProperties;

public class AmqpSourceFragment extends ConfigFragment {
	private static final String VALUES = "amqp.source.values";
	private static final String KEYS = "amqp.source.keys";
	private static final List<String> acceptableHeaderList = Arrays.stream(AmqpHeaderProperties.values())
			.map(Object::toString).toList();
	public enum Section {
		HEADER("Fields that are part of the message.  One of the following (not case sensitive) "
				+ String.join(", ", acceptableHeaderList)), ANNOTATION(
						"Keys for the annotations map. (case sensitive)"), PROPERTY(
								"Keys for the properties map. (case sensitive)"), FOOTER(
										"Keys for the footer map. (case sensitive)");

		private final String description;

		Section(String description) {
			this.description = description;
		}

		public String description() {
			return description;
		}
	}

	public static final ConfigDef.Validator SECTIONS_VALIDATOR = ConfigDef.LambdaValidator.with((name, value) -> {
		if (Objects.nonNull(value)) {
			@SuppressWarnings("unchecked")
			final List<String> valueList = (List<String>) value;
			final Map<String, Set<String>> map;

			for (final String fieldName : valueList) {
				String[] parts = fieldName.split(":");
				if (parts.length < 2) {
					throw new ConfigException(name, value,
							String.format("'%s' is an invalid entry.  It must contain a ':'", fieldName));
				}
				try {
					Section section = Section.valueOf(parts[0].toUpperCase(Locale.ROOT));
					if (section == Section.HEADER) {
						if (parts.length > 2) {
							throw new ConfigException(name, value, String.format(
									"'%s' is an invalid entry.  The HEADER value must be one of (case insensitive) %s",
									fieldName, String.join(", ", acceptableHeaderList)));
						} else {
							try {
								AmqpHeaderProperties.valueOf(parts[1].toUpperCase(Locale.ROOT));
							} catch (IllegalArgumentException e) {
								throw new ConfigException(name, value, String.format(
										"'%s' is not a valid HEADER value.  The HEADER value must be one of (case insensitive) %s",
										parts[0], String.join(", ", acceptableHeaderList)));
							}
						}
					}
				} catch (IllegalArgumentException e) {
					throw new ConfigException(name, value,
							String.format("'%s' is not a valid section name.  Valid names are: %s.", parts[0],
									String.join(", ", Arrays.stream(Section.values()).map(Section::name).toList())));
				}
			}
		}
	}, () -> "Field names should be of the form 'SECTION:fieldName' where SECTION is one of the following (not case specific): "
			+ String.join(", ",
					Arrays.stream(Section.values())
							.map(section -> String.format("%s (%s)", section.name(), section.description())).toList())
			+ ".  'HEADER' entries are limited to the following (not case specific): "
			+ String.join(", ", acceptableHeaderList));

	/**
	 * Construct the ConfigFragment.
	 *
	 * @param dataAccess
	 *            the FragmentDataAccess that this fragment is associated with.
	 */
	protected AmqpSourceFragment(FragmentDataAccess dataAccess) {
		super(dataAccess);
	}

	private Stream<String> extract(String key, Section section) {
		return getList(key).stream().filter(name -> name.toUpperCase(Locale.ROOT).startsWith(section.name() + ":"))
				.map(s -> s.substring(s.indexOf(':') + 1));
	}

	public Stream<String> getValues(Section section) {
		return extract(VALUES, section);
	}

	public Stream<String> getKeys(Section section) {
		return extract(KEYS, section);
	}

	/**
	 * Adds the configuration options for compression to the configuration
	 * definition.
	 *
	 * @param configDef
	 *            the Configuration definition.
	 * @return the update configuration definition
	 */
	public static ConfigDef update(final ConfigDef configDef) {
		SinceInfo.Builder siBuilder = SinceInfo.builder().groupId("io.aiven")
				.artifactId("amqp-source-connector-for-kafka-connect");
		final String groupName = "Amqp Source";
		int groupCounter = 0;

		final String documentation = "Field names should be of the form 'SECTION:fieldName' where SECTION is one of the following (not case specific): "
				+ String.join(", ", Arrays.stream(Section.values())
						.map(section -> String.format("%s (%s)", section.name(), section.description())).toList())
				+ ".";

		configDef.define(ExtendedConfigKey.builder(VALUES).type(ConfigDef.Type.LIST).validator(SECTIONS_VALIDATOR)
				.group(groupName).orderInGroup(++groupCounter)
				.documentation(
						"The list of Message fields that should be included in the output as values. " + documentation)
				.since(siBuilder.version("1.0.0").build().setVersionOnly()).build());

		configDef.define(ExtendedConfigKey.builder(KEYS).type(ConfigDef.Type.LIST).validator(SECTIONS_VALIDATOR)
				.group(groupName).orderInGroup(++groupCounter)
				.documentation(
						"The list of Message fields that should be included in the output as keys.  The order of the fields is preserved."
								+ documentation)
				.since(siBuilder.version("1.0.0").build().setVersionOnly()).build());

		return configDef;
	}

	/**
	 * Creates the setter for this fragment.
	 * 
	 * @param data
	 *            the data to add values to.
	 * @return the Setter.
	 */
	public static AmqpSourceFragment.Setter setter(Map<String, String> data) {
		return new AmqpSourceFragment.Setter(data);
	}

	public static class Setter extends AbstractFragmentSetter<Setter> {

		private Setter(Map<String, String> data) {
			super(data);
		}

		/**
		 * Sets the list of output fields. The order of output fields will match the
		 * order they are added.
		 *
		 * @param valueFields
		 *            the list of output fields
		 * @return this
		 */
		public Setter withValues(final String... valueFields) {
			return setValue(VALUES, String.join(", ", valueFields));
		}

		/**
		 * Sets the list of output fields. The order of output fields will match the
		 * order they are added.
		 *
		 * @param keyFields
		 *            the list of output fields
		 * @return this
		 */
		public Setter withKeys(final String... keyFields) {
			return setValue(KEYS, String.join(", ", keyFields));
		}
	}
}
