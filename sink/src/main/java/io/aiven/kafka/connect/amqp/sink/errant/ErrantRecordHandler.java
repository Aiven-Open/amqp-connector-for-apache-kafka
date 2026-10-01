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

import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import org.apache.kafka.connect.sink.ErrantRecordReporter;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Handles records that could not be sent. */
public class ErrantRecordHandler {
  private static final Logger logger = LoggerFactory.getLogger(ErrantRecordHandler.class);
  private final ErrantRecordReporter errantRecordReporter;

  /**
   * Constructor.
   *
   * @param errantRecordReporter The record reporter to report to. May be {@code null}.
   */
  public ErrantRecordHandler(ErrantRecordReporter errantRecordReporter) {
    this.errantRecordReporter =
        errantRecordReporter == null ? new LoggingReporter() : errantRecordReporter;
  }

  /**
   * Reports an errant record.
   *
   * @param record the record with the issue.
   * @param reason the description of the issue.
   */
  public void reportErrantRecord(SinkRecord record, String reason) {
    this.reportErrantRecord(record, new Exception(reason));
  }

  /**
   * Reports an errant record.
   *
   * @param record the record with the issue.
   * @param exception the Exception that signaled the error.
   */
  public void reportErrantRecord(SinkRecord record, Exception exception) {
    logger.debug("Sending 1 record to DLQ");
    errantRecordReporter.report(record, exception);
  }

  /**
   * Reports a number of errant records.
   *
   * @param records the records that have issues.
   * @param exception the Exception that signaled the error for all the records.
   */
  public void reportErrantRecords(Set<SinkRecord> records, Exception exception) {
    logger.debug("Sending {} records to DLQ", records.size());
    records.forEach(r -> errantRecordReporter.report(r, exception));
  }

  /**
   * Reports a number of errant records.
   *
   * @param rowToError a map of sink record to the throwable that signaled the error.
   */
  public void reportErrantRecords(Map<SinkRecord, Throwable> rowToError) {
    logger.debug("Sending {} records to DLQ", rowToError.size());
    rowToError.forEach(errantRecordReporter::report);
  }

  /**
   * An implementation of ErrantRecordReporter that simply logs the data. Ths class is used if a
   * logging reporter is not specified when the handler is created.
   */
  public static class LoggingReporter implements ErrantRecordReporter {

    /** Constructor. */
    public LoggingReporter() {
      logger.warn("No ErrantRecordReporter provided.  All DLQ records will be logged instead.");
    }

    @Override
    public Future<Void> report(SinkRecord record, Throwable error) {
      logger.warn("DLQ: {} reason: {}", record, error.getMessage());
      if (logger.isDebugEnabled()) {
        logger.debug("DLQ: {} reason: {}", record, error.getMessage(), error);
      }
      return CompletableFuture.completedFuture(null);
    }
  }
}
