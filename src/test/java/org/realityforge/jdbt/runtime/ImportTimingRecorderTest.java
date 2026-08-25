package org.realityforge.jdbt.runtime;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import java.io.StringWriter;
import java.io.Writer;
import java.util.ArrayDeque;
import java.util.List;
import java.util.function.LongSupplier;
import org.junit.jupiter.api.Test;
import org.realityforge.jdbt.db.SqlTimingObservation;

final class ImportTimingRecorderTest {
    @Test
    void writesDeterministicNestedTerminalObservations() {
        final var output = new StringWriter();
        final var recorder = new ImportTimingRecorder(output, clock(0L, 1_000L, 4_000L, 10_000L));

        recorder.command("command/import", () -> recorder.run("phase/data-import", "phase", () -> {}));

        assertThat(output.toString()).isEqualTo("""
            {"schema_version":1,"sequence":1,"operation_id":"phase/data-import","parent_operation_id":"command/import","kind":"phase","status":"succeeded","elapsed_microseconds":3}
            {"schema_version":1,"sequence":2,"operation_id":"command/import","parent_operation_id":null,"kind":"command","status":"succeeded","elapsed_microseconds":10}
            """);
    }

    @Test
    void recordsFailedLeafAndAncestorsWithoutReplacingPrimaryFailure() {
        final var output = new StringWriter();
        final var recorder = new ImportTimingRecorder(output, clock(0L, 1_000L, 4_000L, 10_000L));
        final var failure = new IllegalStateException("database failure");

        assertThatThrownBy(() -> recorder.command(
                        "command/import",
                        () -> recorder.run("phase/data-import", "phase", () -> {
                            throw failure;
                        })))
                .isSameAs(failure);

        assertThat(output.toString())
                .containsSubsequence(
                        "\"operation_id\":\"phase/data-import\"",
                        "\"status\":\"failed\"",
                        "\"operation_id\":\"command/import\"",
                        "\"status\":\"failed\"");
    }

    @Test
    void attachesWriterFailureAsSecondaryWhenOperationAlreadyFailed() {
        final var recorder = new ImportTimingRecorder(new FailingWriter(), clock(0L, 1_000L));
        final var failure = new IllegalStateException("database failure");

        assertThatThrownBy(() -> recorder.command("command/import", () -> {
                    throw failure;
                }))
                .isSameAs(failure);
        assertThat(failure.getSuppressed()).hasSize(1);
        assertThat(failure.getSuppressed()[0])
                .isInstanceOf(RuntimeExecutionException.class)
                .hasMessage("Unable to write import timing output")
                .hasNoCause();
    }

    @Test
    void failedSqlCompletionKeepsPrimaryAndResetsBatchState() {
        final var output = new StringWriter();
        final var recorder = new ImportTimingRecorder(output, System::nanoTime);
        final var primary = new IllegalStateException("database failure");

        assertThatThrownBy(() -> recorder.command(
                        "command/import",
                        () -> recorder.run("sql-batch/file/1", "sql_batch", () -> {
                            recorder.beginSqlBatch();
                            recorder.recordSqlObservation(new SqlTimingObservation(
                                    1,
                                    "analysis-corruption/Analysis/handwritten%2F1",
                                    "phase/corruption-checks",
                                    "analysis_corruption_check",
                                    "failed",
                                    1));
                            recorder.completeSqlBatchAfterFailure(primary);
                            throw primary;
                        })))
                .isSameAs(primary);

        assertThat(primary.getSuppressed())
                .extracting(Throwable::getMessage)
                .containsExactly("SQL import timing contains an unknown parent");
        recorder.command(
                "command/second",
                () -> recorder.run("sql-batch/file/2", "sql_batch", () -> {
                    recorder.beginSqlBatch();
                    recorder.completeSqlBatch();
                }));
    }

    @Test
    void rejectsDuplicateIdsAndEncodesUtf8Components() {
        final var output = new StringWriter();
        final var recorder = new ImportTimingRecorder(output, clock(0L, 1_000L, 2_000L));

        recorder.command("command/import", () -> {});

        assertThatThrownBy(() -> recorder.command("command/import", () -> {}))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Duplicate import timing operation ID");
        assertThat(ImportTimingRecorder.component("db-hooks/final/100% café.sql"))
                .isEqualTo("db-hooks%2Ffinal%2F100%25%20caf%C3%A9.sql");
    }

    private static LongSupplier clock(final Long... values) {
        final var remaining = new ArrayDeque<>(List.of(values));
        return () -> remaining.removeFirst();
    }

    private static final class FailingWriter extends Writer {
        @Override
        public void write(final char[] buffer, final int offset, final int length) throws IOException {
            throw new IOException("write failed");
        }

        @Override
        public void flush() {}

        @Override
        public void close() {}
    }
}
