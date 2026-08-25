package org.realityforge.jdbt.runtime;

import java.io.IOException;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.Set;
import java.util.function.LongSupplier;
import org.jspecify.annotations.Nullable;
import org.realityforge.jdbt.db.SqlTimingObservation;

public final class ImportTimingRecorder implements AutoCloseable {
    private static final String COMMAND_KIND = "command";
    private static final String SQL_PARENT_TOKEN = "__JDBT_ACTIVE_SQL_BATCH__";
    private static final Set<String> SQL_KINDS =
            Set.of("phase", "module", "analysis_corruption_check", "analysis_constraint_check");
    private static final Set<String> DIRECT_KEY_FAMILIES = Set.of("invariant", "foreign-key", "validation");
    private static final List<String> SIMPLE_CORRUPTION_KEY_PREFIXES =
            List.of("id-namespace/", "relationship/", "reference-multiplicity/");
    private final @Nullable Writer writer;
    private final LongSupplier nanoClock;
    private final Deque<String> activeOperations = new ArrayDeque<>();
    private final Set<String> operationIds = new HashSet<>();
    private long sequence;
    private boolean sqlBatchActive;
    private long expectedSqlOrdinal;
    private int sqlObservationCount;
    private int sqlRootCount;
    private final Set<String> pendingSqlParents = new HashSet<>();

    private ImportTimingRecorder() {
        this.writer = null;
        this.nanoClock = System::nanoTime;
    }

    ImportTimingRecorder(final Writer writer, final LongSupplier nanoClock) {
        this.writer = Objects.requireNonNull(writer);
        this.nanoClock = Objects.requireNonNull(nanoClock);
    }

    public static ImportTimingRecorder disabled() {
        return new ImportTimingRecorder();
    }

    public static ImportTimingRecorder open(final Path outputFile) {
        Objects.requireNonNull(outputFile);
        try {
            final var parent = outputFile.getParent();
            if (null != parent) {
                Files.createDirectories(parent);
            }
            return new ImportTimingRecorder(
                    Files.newBufferedWriter(
                            outputFile,
                            StandardCharsets.UTF_8,
                            StandardOpenOption.CREATE,
                            StandardOpenOption.TRUNCATE_EXISTING,
                            StandardOpenOption.WRITE),
                    System::nanoTime);
        } catch (final IOException ignored) {
            throw new RuntimeExecutionException("Unable to open import timing output");
        }
    }

    public boolean enabled() {
        return null != writer;
    }

    public void command(final String operationId, final Runnable action) {
        runWithParent(operationId, null, COMMAND_KIND, action);
    }

    public void run(final String operationId, final String kind, final Runnable action) {
        runWithParent(operationId, activeOperations.peek(), kind, action);
    }

    public void runWithParent(
            final String operationId,
            final @Nullable String parentOperationId,
            final String kind,
            final Runnable action) {
        Objects.requireNonNull(action);
        if (!enabled()) {
            action.run();
            return;
        }
        validateStart(operationId, parentOperationId, kind);
        final var startedAt = nanoClock.getAsLong();
        activeOperations.push(operationId);
        try {
            action.run();
            writeObservation(operationId, parentOperationId, kind, "succeeded", elapsedMicroseconds(startedAt));
        } catch (final RuntimeException | Error primary) {
            try {
                writeObservation(operationId, parentOperationId, kind, "failed", elapsedMicroseconds(startedAt));
            } catch (final RuntimeException | Error timingFailure) {
                primary.addSuppressed(timingFailure);
            }
            throw primary;
        } finally {
            final var active = activeOperations.pop();
            if (!operationId.equals(active)) {
                throw new IllegalStateException("Import timing operation stack is corrupt");
            }
        }
    }

    public @Nullable String currentOperationId() {
        return activeOperations.peek();
    }

    public void beginSqlBatch() {
        if (!enabled()) {
            return;
        }
        if (sqlBatchActive) {
            throw new IllegalStateException("SQL import timing batch is already active");
        }
        sqlBatchActive = true;
        expectedSqlOrdinal = 1;
        sqlObservationCount = 0;
        sqlRootCount = 0;
        pendingSqlParents.clear();
    }

    public void recordSqlObservation(final SqlTimingObservation observation) {
        if (!sqlBatchActive) {
            throw new IllegalStateException("SQL import timing batch is not active");
        }
        if (expectedSqlOrdinal != observation.ordinal()) {
            throw new IllegalArgumentException("SQL import timing ordinal is not contiguous");
        }
        expectedSqlOrdinal++;
        final var operationId = observation.operationId();
        if (operationId.isBlank()) {
            throw new IllegalArgumentException("SQL import timing operation ID must not be blank");
        }
        if (!SQL_KINDS.contains(observation.kind())) {
            throw new IllegalArgumentException("Invalid SQL import timing operation kind");
        }
        if (!"succeeded".equals(observation.status()) && !"failed".equals(observation.status())) {
            throw new IllegalArgumentException("Invalid SQL import timing operation status");
        }
        if (observation.elapsedMicroseconds() < 0) {
            throw new IllegalArgumentException("SQL import timing elapsed time must not be negative");
        }
        if (!operationIds.add(operationId)) {
            throw new IllegalArgumentException("Duplicate import timing operation ID");
        }
        final var suppliedParent = observation.parentOperationId();
        if (null == suppliedParent || suppliedParent.isBlank()) {
            throw new IllegalArgumentException("SQL import timing operation parent must not be blank");
        }
        validateSqlIdentity(operationId, suppliedParent, observation.kind());
        final String parentOperationId;
        if (SQL_PARENT_TOKEN.equals(suppliedParent)) {
            parentOperationId = activeOperations.peek();
            if (null == parentOperationId) {
                throw new IllegalStateException("SQL import timing has no active batch parent");
            }
            sqlRootCount++;
        } else {
            parentOperationId = suppliedParent;
            if (operationIds.contains(parentOperationId)) {
                throw new IllegalArgumentException("SQL import timing parent is already terminal");
            }
            pendingSqlParents.add(parentOperationId);
        }
        pendingSqlParents.remove(operationId);
        sqlObservationCount++;
        writeObservation(
                operationId,
                parentOperationId,
                observation.kind(),
                observation.status(),
                observation.elapsedMicroseconds());
    }

    public void completeSqlBatch() {
        if (!enabled()) {
            return;
        }
        if (!sqlBatchActive) {
            throw new IllegalStateException("SQL import timing batch is not active");
        }
        sqlBatchActive = false;
        if (!pendingSqlParents.isEmpty()) {
            throw new IllegalArgumentException("SQL import timing contains an unknown parent");
        }
        if (sqlObservationCount > 0 && 1 != sqlRootCount) {
            throw new IllegalArgumentException("SQL import timing must contain one root");
        }
    }

    public void completeSqlBatchAfterFailure(final Throwable primary) {
        Objects.requireNonNull(primary);
        try {
            completeSqlBatch();
        } catch (final RuntimeException | Error timingFailure) {
            primary.addSuppressed(timingFailure);
        }
    }

    private static void validateSqlIdentity(
            final String operationId, final String parentOperationId, final String kind) {
        switch (kind) {
            case "phase" -> {
                final var expectedParent =
                        switch (operationId) {
                            case "phase/final-validation" -> SQL_PARENT_TOKEN;
                            case "phase/corruption-checks", "phase/constraint-checks" -> "phase/final-validation";
                            default -> throw new IllegalArgumentException("Invalid SQL import timing phase identity");
                        };
                if (!expectedParent.equals(parentOperationId)) {
                    throw new IllegalArgumentException("Invalid SQL import timing phase parent");
                }
            }
            case "module" -> {
                final var schema = suffixComponent(operationId, "module/constraint-check/");
                if (!"phase/constraint-checks".equals(parentOperationId) || !isCanonicalComponent(schema)) {
                    throw new IllegalArgumentException("Invalid SQL import timing module identity");
                }
            }
            case "analysis_corruption_check" -> {
                final var suffix = operationId.startsWith("analysis-corruption/")
                        ? operationId.substring("analysis-corruption/".length())
                        : "";
                final var separator = suffix.indexOf('/');
                if (!"phase/corruption-checks".equals(parentOperationId)
                        || separator <= 0
                        || separator != suffix.lastIndexOf('/')
                        || !isCanonicalComponent(suffix.substring(0, separator))
                        || !isCanonicalComponent(suffix.substring(separator + 1))
                        || !hasCorruptionKeyFamily(decodeComponent(suffix.substring(separator + 1)))) {
                    throw new IllegalArgumentException("Invalid SQL import timing corruption identity");
                }
            }
            case "analysis_constraint_check" -> {
                final var directKey = suffixComponent(operationId, "analysis-constraint/");
                final var decoded = isCanonicalComponent(directKey) ? decodeComponent(directKey) : "";
                final var parts = decoded.split("/", 3);
                final var qualifiedTable = 3 == parts.length ? parts[1] : "";
                final var separator = qualifiedTable.indexOf('.');
                final var expectedParent = separator > 0
                                && separator == qualifiedTable.lastIndexOf('.')
                                && separator < qualifiedTable.length() - 1
                        ? "module/constraint-check/" + component(qualifiedTable.substring(0, separator))
                        : "";
                if (3 != parts.length
                        || !DIRECT_KEY_FAMILIES.contains(parts[0])
                        || parts[2].isEmpty()
                        || !expectedParent.equals(parentOperationId)) {
                    throw new IllegalArgumentException("Invalid SQL import timing constraint identity");
                }
            }
            default -> throw new IllegalArgumentException("Invalid SQL import timing operation kind");
        }
    }

    private static String suffixComponent(final String operationId, final String prefix) {
        return operationId.startsWith(prefix) ? operationId.substring(prefix.length()) : "";
    }

    private static boolean hasCorruptionKeyFamily(final String checkKey) {
        if (checkKey.startsWith("entity-validation/")) {
            final var remainder = checkKey.substring("entity-validation/".length());
            final var separator = remainder.indexOf('/');
            return separator > 0 && separator == remainder.lastIndexOf('/') && separator < remainder.length() - 1;
        }
        if (checkKey.startsWith("handwritten/")) {
            final var id = checkKey.substring("handwritten/".length());
            if (id.isEmpty() || ('0' == id.charAt(0) && id.length() > 1)) {
                return false;
            }
            for (int i = 0; i < id.length(); i++) {
                if (id.charAt(i) < '0' || id.charAt(i) > '9') {
                    return false;
                }
            }
            return true;
        }
        for (final var prefix : SIMPLE_CORRUPTION_KEY_PREFIXES) {
            if (checkKey.startsWith(prefix)) {
                final var remainder = checkKey.substring(prefix.length());
                return !remainder.isEmpty() && !remainder.contains("/");
            }
        }
        return false;
    }

    private static boolean isCanonicalComponent(final String value) {
        if (value.isEmpty()) {
            return false;
        }
        for (int i = 0; i < value.length(); i++) {
            final var c = value.charAt(i);
            if (isUnreserved(c)) {
                continue;
            }
            if ('%' != c || i + 2 >= value.length()) {
                return false;
            }
            final var high = Character.digit(value.charAt(i + 1), 16);
            final var low = Character.digit(value.charAt(i + 2), 16);
            if (high < 0
                    || low < 0
                    || Character.toUpperCase(value.charAt(i + 1)) != value.charAt(i + 1)
                    || Character.toUpperCase(value.charAt(i + 2)) != value.charAt(i + 2)
                    || isUnreserved((char) ((high << 4) + low))) {
                return false;
            }
            i += 2;
        }
        return value.equals(component(decodeComponent(value)));
    }

    private static boolean isUnreserved(final char value) {
        return (value >= 'A' && value <= 'Z')
                || (value >= 'a' && value <= 'z')
                || (value >= '0' && value <= '9')
                || '-' == value
                || '.' == value
                || '_' == value
                || '~' == value;
    }

    private static String decodeComponent(final String value) {
        final var bytes = new byte[value.length()];
        var length = 0;
        for (int i = 0; i < value.length(); i++) {
            final var c = value.charAt(i);
            if ('%' == c) {
                bytes[length++] = (byte)
                        ((Character.digit(value.charAt(i + 1), 16) << 4) + Character.digit(value.charAt(i + 2), 16));
                i += 2;
            } else {
                bytes[length++] = (byte) c;
            }
        }
        return new String(bytes, 0, length, StandardCharsets.UTF_8);
    }

    public static String component(final String value) {
        final var bytes = Objects.requireNonNull(value).getBytes(StandardCharsets.UTF_8);
        final var encoded = new StringBuilder(bytes.length);
        for (final byte item : bytes) {
            final var valueByte = item & 0xFF;
            if ((valueByte >= 'A' && valueByte <= 'Z')
                    || (valueByte >= 'a' && valueByte <= 'z')
                    || (valueByte >= '0' && valueByte <= '9')
                    || '-' == valueByte
                    || '.' == valueByte
                    || '_' == valueByte
                    || '~' == valueByte) {
                encoded.append((char) valueByte);
            } else {
                encoded.append('%');
                encoded.append(Character.toUpperCase(Character.forDigit(valueByte >>> 4, 16)));
                encoded.append(Character.toUpperCase(Character.forDigit(valueByte & 0xF, 16)));
            }
        }
        return encoded.toString();
    }

    private void validateStart(final String operationId, final @Nullable String parentOperationId, final String kind) {
        if (operationId.isBlank()) {
            throw new IllegalArgumentException("Import timing operation ID must not be blank");
        }
        if (kind.isBlank()) {
            throw new IllegalArgumentException("Import timing operation kind must not be blank");
        }
        if (!operationIds.add(operationId)) {
            throw new IllegalArgumentException("Duplicate import timing operation ID");
        }
        if (null == parentOperationId) {
            if (!activeOperations.isEmpty()) {
                throw new IllegalArgumentException("Nested import timing operation must have a parent");
            }
        } else if (!activeOperations.contains(parentOperationId)) {
            throw new IllegalArgumentException("Import timing operation parent is not active");
        }
    }

    private long elapsedMicroseconds(final long startedAt) {
        return Math.max(0L, nanoClock.getAsLong() - startedAt) / 1_000L;
    }

    private void writeObservation(
            final String operationId,
            final @Nullable String parentOperationId,
            final String kind,
            final String status,
            final long elapsedMicroseconds) {
        final var line = new StringBuilder(192)
                .append("{\"schema_version\":1,\"sequence\":")
                .append(++sequence)
                .append(",\"operation_id\":\"")
                .append(jsonString(operationId))
                .append("\",\"parent_operation_id\":");
        if (null == parentOperationId) {
            line.append("null");
        } else {
            line.append('"').append(jsonString(parentOperationId)).append('"');
        }
        line.append(",\"kind\":\"")
                .append(jsonString(kind))
                .append("\",\"status\":\"")
                .append(status)
                .append("\",\"elapsed_microseconds\":")
                .append(elapsedMicroseconds)
                .append("}\n");
        try {
            Objects.requireNonNull(writer).write(line.toString());
            writer.flush();
        } catch (final IOException ignored) {
            throw new RuntimeExecutionException("Unable to write import timing output");
        }
    }

    private static String jsonString(final String value) {
        final var output = new StringBuilder(value.length());
        for (int i = 0; i < value.length(); i++) {
            final var c = value.charAt(i);
            switch (c) {
                case '"' -> output.append("\\\"");
                case '\\' -> output.append("\\\\");
                case '\b' -> output.append("\\b");
                case '\f' -> output.append("\\f");
                case '\n' -> output.append("\\n");
                case '\r' -> output.append("\\r");
                case '\t' -> output.append("\\t");
                default -> {
                    if (c < 0x20) {
                        output.append(String.format(Locale.ROOT, "\\u%04X", (int) c));
                    } else {
                        output.append(c);
                    }
                }
            }
        }
        return output.toString();
    }

    @Override
    public void close() {
        if (null == writer) {
            return;
        }
        try {
            writer.close();
        } catch (final IOException ignored) {
            throw new RuntimeExecutionException("Unable to close import timing output");
        }
    }
}
