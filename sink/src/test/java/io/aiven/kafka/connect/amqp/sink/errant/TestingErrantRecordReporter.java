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
package io.aiven.kafka.connect.amqp.sink.errant;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.kafka.connect.sink.ErrantRecordReporter;
import org.apache.kafka.connect.sink.SinkRecord;

/**
 * An ErrantRecordReporter for testing. Captures the errant records and their associated errors for
 * later retrieval.
 */
public class TestingErrantRecordReporter implements ErrantRecordReporter {
  List<Pair<SinkRecord, Throwable>> lst = new ArrayList<>();

  @Override
  public Future<Void> report(SinkRecord record, Throwable error) {
    lst.add(ImmutablePair.of(record, error));
    return CompletableFuture.completedFuture(null);
  }

  /**
   * Get the list of captured errant records and their issues.
   *
   * @return the list of captured errant records and their issues.
   */
  public List<Pair<SinkRecord, Throwable>> getErrors() {
    return lst;
  }
}
