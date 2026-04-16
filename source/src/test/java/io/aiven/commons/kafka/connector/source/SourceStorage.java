/*
 * Copyright 2026 Aiven Oy
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.aiven.commons.kafka.connector.source;

import io.aiven.commons.kafka.connector.common.integration.StorageBase;
import io.aiven.commons.kafka.connector.source.extractor.ExtractorRegistry;
import java.util.Map;
import java.util.function.BiFunction;

/**
 * The definition of the interface to write data to the SourceStorage so that it can be read back
 * through normal channels.
 *
 * @param <K> the native key type
 * @param <N> The native object type.
 */
public interface SourceStorage<K extends Comparable<K>, N> extends StorageBase<K, N> {

  /**
   * Gets the list of supported transformers for the storage.
   *
   * @return A ExtractorRegistry containing the acceptable transformers.
   */
  ExtractorRegistry supportedExtractors();

  /**
   * Create a native key from a topic and partition.
   *
   * @param topic the topic for the key.
   * @param partition the partition for the key.
   * @return a new Key.
   */
  K createKey(String topic, int partition);

  /**
   * Write a record into the storage using the native key.
   *
   * @param nativeKey the native key to use in the storage engine.
   * @param testDataBytes the bytes to associate with the key.
   * @return A WriteResule for the write.
   */
  WriteResult<K> writeWithKey(K nativeKey, byte[] testDataBytes);

  /**
   * Create a connector configuration. This includes any information necessary to connect to the
   * storage engine.
   *
   * @return the connector config map.
   */
  Map<String, String> createConnectorConfig();

  /**
   * Returns a BiFunction that converts OffsetManager key and data into an OffsetManagerEntry for
   * this system.
   *
   * <ul>
   *   <li>The first argument to the method is the {@link
   *       OffsetManager.OffsetManagerEntry#getManagerKey()} value.
   *   <li>The second argument is the {@link OffsetManager.OffsetManagerEntry#getProperties()}
   *       value.
   *   <li>Method should return a proper {@link OffsetManager.OffsetManagerEntry}
   * </ul>
   *
   * @return A BiFunction that crates an OffsetManagerEntry.
   */
  BiFunction<Map<String, Object>, Map<String, Object>, OffsetManager.OffsetManagerEntry>
      offsetManagerEntryFactory();

  /**
   * The result of a successful write.
   *
   * @param <K> the native key type.
   */
  final class WriteResult<K extends Comparable<K>> {
    /** the OffsetManagerKey for the result */
    private final OffsetManager.OffsetManagerKey offsetKey;

    /** The native Key for the result */
    private final K nativeKey;

    /**
     * Constructor.
     *
     * @param offsetKey the OffsetManagerKey for the result.
     * @param nativeKey the native key for the result.
     */
    public WriteResult(final OffsetManager.OffsetManagerKey offsetKey, final K nativeKey) {
      this.offsetKey = offsetKey;
      this.nativeKey = nativeKey;
    }

    /**
     * Gets the OffsetManagerKey.
     *
     * @return the OffsetManagerKey
     */
    public OffsetManager.OffsetManagerKey getOffsetManagerKey() {
      return offsetKey;
    }

    /**
     * Gets the native key.
     *
     * @return the native key.
     */
    public K getNativeKey() {
      return nativeKey;
    }
  }
}
