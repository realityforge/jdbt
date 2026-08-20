package org.realityforge.jdbt.runtime;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.jspecify.annotations.Nullable;
import org.realityforge.jdbt.db.DatabaseConnection;
import org.realityforge.jdbt.db.DbDriver;
import org.realityforge.jdbt.db.QueryResult;
import org.realityforge.jdbt.repository.RepositoryConfig;

public final class DatabaseStatisticsExporter {
    static final String CATALOG_QUERY = """
        WITH partition_rows AS (
          SELECT object_id, index_id, SUM(rows) AS row_count
          FROM sys.partitions
          GROUP BY object_id, index_id
        ), catalog_rows AS (
          SELECT s.name AS schema_name,
                 t.name AS table_name,
                 i.name AS index_name,
                 i.index_id,
                 i.type AS index_type,
                 i.is_disabled,
                 i.is_hypothetical,
                 p.row_count
          FROM sys.tables t
          JOIN sys.schemas s ON s.schema_id = t.schema_id
          JOIN sys.indexes i ON i.object_id = t.object_id
          LEFT JOIN partition_rows p ON p.object_id = i.object_id AND p.index_id = i.index_id
        )
        SELECT CAST(HAS_PERMS_BY_NAME(DB_NAME(), 'DATABASE', 'VIEW DEFINITION') AS int) AS has_view_definition,
               c.schema_name,
               c.table_name,
               c.index_name,
               c.index_id,
               c.index_type,
               c.is_disabled,
               c.is_hypothetical,
               c.row_count
        FROM (VALUES (0)) marker(value)
        LEFT JOIN catalog_rows c ON 1 = 1
        ORDER BY c.schema_name, c.table_name, c.index_id
        """;
    private static final List<String> CATALOG_COLUMNS = List.of(
            "has_view_definition",
            "schema_name",
            "table_name",
            "index_name",
            "index_id",
            "index_type",
            "is_disabled",
            "is_hypothetical",
            "row_count");
    private static final String CSV_HEADER = "object_type,schema,table,index,metric,value\n";

    private final DbDriver dbDriver;

    public DatabaseStatisticsExporter(final DbDriver dbDriver) {
        this.dbDriver = dbDriver;
    }

    public int export(final RepositoryConfig repository, final DatabaseConnection target, final Path outputFile) {
        final QueryResult result;
        dbDriver.open(target, false);
        try {
            result = dbDriver.query(CATALOG_QUERY);
        } finally {
            dbDriver.close();
        }
        final var statistics = validateAndCollect(repository, result);
        replaceAtomically(outputFile, render(statistics));
        return statistics.size();
    }

    private static List<Statistic> validateAndCollect(final RepositoryConfig repository, final QueryResult result) {
        if (!CATALOG_COLUMNS.equals(result.columnLabels())) {
            throw new RuntimeExecutionException("Unexpected database statistics columns. Expected " + CATALOG_COLUMNS
                    + " but received " + result.columnLabels());
        }

        final var errors = new ArrayList<String>();
        final var rowsByTable = new HashMap<TableKey, List<CatalogRow>>();
        final var expectedTables = expectedTables(repository);
        var viewDefinition = false;
        for (final var row : result.rows()) {
            final var permission = integer(row.get(0), "has_view_definition", errors);
            viewDefinition |= null != permission && 1 == permission;
            if (null == row.get(1) && null == row.get(2)) {
                continue;
            }
            final var schema = string(row.get(1), "schema_name", errors);
            final var table = string(row.get(2), "table_name", errors);
            if (null == schema || null == table) {
                continue;
            }
            final var key = new TableKey(schema, table);
            if (!expectedTables.containsKey(key)) {
                continue;
            }
            final var catalogRow = catalogRow(key, row, errors);
            if (null != catalogRow) {
                rowsByTable
                        .computeIfAbsent(catalogRow.table(), ignored -> new ArrayList<>())
                        .add(catalogRow);
            }
        }
        if (!viewDefinition) {
            errors.add("The target user requires VIEW DEFINITION on the database");
        }

        final var statistics = new ArrayList<Statistic>();
        for (final var expected : expectedTables.values().stream()
                .sorted(Comparator.comparing(
                                (ExpectedTable value) -> value.key().schema())
                        .thenComparing(value -> value.key().table()))
                .toList()) {
            final var rows = rowsByTable.getOrDefault(expected.key(), List.of());
            if (rows.isEmpty()) {
                errors.add("Missing modeled table " + expected.key().display());
                continue;
            }

            final var storageRows = rows.stream()
                    .filter(row -> 0 == row.indexId() || 1 == row.indexId())
                    .toList();
            if (1 != storageRows.size()) {
                errors.add("Modeled table " + expected.key().display() + " has " + storageRows.size()
                        + " heap or clustered storage rows; expected exactly one");
            } else {
                final var storage = storageRows.get(0);
                if (validRow(storage, "table " + expected.key().display(), true, errors)) {
                    statistics.add(
                            new Statistic("table", expected.key(), "", Objects.requireNonNull(storage.rowCount())));
                }
            }

            for (final var expectedIndex : expected.indexes().stream().sorted().toList()) {
                final var matching = rows.stream()
                        .filter(row -> expectedIndex.equals(row.indexName()))
                        .toList();
                final var display = "index " + expected.key().display() + '.' + expectedIndex;
                if (matching.isEmpty()) {
                    errors.add("Missing modeled " + display);
                } else if (1 != matching.size()) {
                    errors.add("Modeled " + display + " appears " + matching.size() + " times");
                } else if (validRow(matching.get(0), display, false, errors)) {
                    statistics.add(new Statistic(
                            "index",
                            expected.key(),
                            expectedIndex,
                            Objects.requireNonNull(matching.get(0).rowCount())));
                }
            }
        }

        if (!errors.isEmpty()) {
            throw new RuntimeExecutionException(
                    "Database statistics validation failed:\n - " + String.join("\n - ", errors));
        }
        return List.copyOf(statistics);
    }

    private static Map<TableKey, ExpectedTable> expectedTables(final RepositoryConfig repository) {
        final var expected = new LinkedHashMap<TableKey, ExpectedTable>();
        for (final var module : repository.modules()) {
            for (final var table : repository.tablesForModule(module)) {
                final var identifiers = identifiers(table.name());
                final var key =
                        new TableKey(identifiers.get(identifiers.size() - 2), identifiers.get(identifiers.size() - 1));
                final var value = new ExpectedTable(
                        key,
                        table.indexes().stream()
                                .map(DatabaseStatisticsExporter::unquote)
                                .toList());
                if (null != expected.put(key, value)) {
                    throw new RuntimeExecutionException("Duplicate modeled table " + key.display());
                }
            }
        }
        return expected;
    }

    private static @Nullable CatalogRow catalogRow(
            final TableKey table, final List<Object> row, final List<String> errors) {
        final var indexName = nullableString(row.get(3), "index_name", errors);
        final var indexId = integer(row.get(4), "index_id", errors);
        final var indexType = integer(row.get(5), "index_type", errors);
        final var disabled = flag(row.get(6), "is_disabled", errors);
        final var hypothetical = flag(row.get(7), "is_hypothetical", errors);
        final var rowCount = longValue(row.get(8), "row_count", errors);
        if (null == indexId || null == indexType || null == disabled || null == hypothetical) {
            return null;
        }
        return new CatalogRow(table, indexName, indexId, indexType, disabled, hypothetical, rowCount);
    }

    private static boolean validRow(
            final CatalogRow row, final String display, final boolean tableStorage, final List<String> errors) {
        var valid = true;
        if (row.disabled()) {
            errors.add("Modeled " + display + " is disabled");
            valid = false;
        }
        if (row.hypothetical()) {
            errors.add("Modeled " + display + " is hypothetical");
            valid = false;
        }
        final var supported = tableStorage
                ? 0 == row.indexType() || 1 == row.indexType()
                : 1 == row.indexType() || 2 == row.indexType();
        if (!supported) {
            errors.add("Modeled " + display + " uses unsupported SQL Server index type " + row.indexType());
            valid = false;
        }
        if (null == row.rowCount()) {
            errors.add("Modeled " + display + " has no partition row count");
            valid = false;
        } else if (row.rowCount() < 0) {
            errors.add("Modeled " + display + " has invalid negative row count " + row.rowCount());
            valid = false;
        }
        return valid;
    }

    private static String render(final List<Statistic> statistics) {
        final var csv = new StringBuilder(CSV_HEADER);
        for (final var statistic : statistics) {
            csv.append(csv(statistic.objectType()))
                    .append(',')
                    .append(csv(statistic.table().schema()))
                    .append(',')
                    .append(csv(statistic.table().table()))
                    .append(',')
                    .append(csv(statistic.index()))
                    .append(",approximate_row_count,")
                    .append(statistic.value())
                    .append('\n');
        }
        return csv.toString();
    }

    private static String csv(final String value) {
        if (value.indexOf(',') < 0 && value.indexOf('"') < 0 && value.indexOf('\n') < 0 && value.indexOf('\r') < 0) {
            return value;
        }
        return '"' + value.replace("\"", "\"\"") + '"';
    }

    private static void replaceAtomically(final Path outputFile, final String content) {
        final var absoluteOutput = outputFile.toAbsolutePath().normalize();
        final var parent =
                Objects.requireNonNull(absoluteOutput.getParent(), "Output file must have a parent directory");
        Path temporaryFile = null;
        try {
            Files.createDirectories(parent);
            temporaryFile = Files.createTempFile(parent, ".jdbt-statistics-", ".csv");
            Files.writeString(temporaryFile, content, StandardCharsets.UTF_8);
            try {
                Files.move(
                        temporaryFile,
                        absoluteOutput,
                        StandardCopyOption.ATOMIC_MOVE,
                        StandardCopyOption.REPLACE_EXISTING);
            } catch (final AtomicMoveNotSupportedException exception) {
                throw new RuntimeExecutionException(
                        "Atomic replacement is not supported for " + absoluteOutput, exception);
            }
            temporaryFile = null;
        } catch (final IOException exception) {
            throw new UncheckedIOException("Failed writing database statistics to " + absoluteOutput, exception);
        } finally {
            if (null != temporaryFile) {
                try {
                    Files.deleteIfExists(temporaryFile);
                } catch (final IOException ignored) {
                    // Preserve the primary failure.
                }
            }
        }
    }

    private static List<String> identifiers(final String qualifiedName) {
        final var values = new ArrayList<String>();
        final var value = new StringBuilder();
        var bracketed = false;
        var quoted = false;
        for (var index = 0; index < qualifiedName.length(); index++) {
            final var character = qualifiedName.charAt(index);
            if ('[' == character && !quoted) {
                bracketed = true;
            } else if (']' == character && bracketed) {
                if (index + 1 < qualifiedName.length() && ']' == qualifiedName.charAt(index + 1)) {
                    value.append(character).append(character);
                    index++;
                    continue;
                }
                bracketed = false;
            } else if ('"' == character && !bracketed) {
                if (quoted && index + 1 < qualifiedName.length() && '"' == qualifiedName.charAt(index + 1)) {
                    value.append(character).append(character);
                    index++;
                    continue;
                }
                quoted = !quoted;
            } else if ('.' == character && !bracketed && !quoted) {
                values.add(unquote(value.toString()));
                value.setLength(0);
                continue;
            }
            value.append(character);
        }
        values.add(unquote(value.toString()));
        return values;
    }

    private static String unquote(final String identifier) {
        if (identifier.startsWith("[") && identifier.endsWith("]")) {
            return identifier.substring(1, identifier.length() - 1).replace("]]", "]");
        }
        if (identifier.startsWith("\"") && identifier.endsWith("\"")) {
            return identifier.substring(1, identifier.length() - 1).replace("\"\"", "\"");
        }
        return identifier;
    }

    private static @Nullable String string(
            final @Nullable Object value, final String label, final List<String> errors) {
        if (value instanceof String string) {
            return string;
        }
        errors.add("Catalog column " + label + " must be text but was " + value);
        return null;
    }

    private static @Nullable String nullableString(
            final @Nullable Object value, final String label, final List<String> errors) {
        return null == value ? null : string(value, label, errors);
    }

    private static @Nullable Integer integer(
            final @Nullable Object value, final String label, final List<String> errors) {
        if (value instanceof Number number) {
            return number.intValue();
        }
        errors.add("Catalog column " + label + " must be numeric but was " + value);
        return null;
    }

    private static @Nullable Long longValue(
            final @Nullable Object value, final String label, final List<String> errors) {
        if (value instanceof Number number) {
            return number.longValue();
        }
        if (null != value) {
            errors.add("Catalog column " + label + " must be numeric but was " + value);
        }
        return null;
    }

    private static @Nullable Boolean flag(final @Nullable Object value, final String label, final List<String> errors) {
        if (value instanceof Boolean booleanValue) {
            return booleanValue;
        }
        if (value instanceof Number number && (0 == number.intValue() || 1 == number.intValue())) {
            return 1 == number.intValue();
        }
        errors.add("Catalog column " + label + " must be boolean or 0/1 but was " + value);
        return null;
    }

    private record TableKey(String schema, String table) {
        String display() {
            return schema + '.' + table;
        }
    }

    private record ExpectedTable(TableKey key, List<String> indexes) {}

    private record CatalogRow(
            TableKey table,
            @Nullable String indexName,
            int indexId,
            int indexType,
            boolean disabled,
            boolean hypothetical,
            @Nullable Long rowCount) {}

    private record Statistic(String objectType, TableKey table, String index, long value) {}
}
