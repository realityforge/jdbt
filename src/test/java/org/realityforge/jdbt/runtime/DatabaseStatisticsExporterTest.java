package org.realityforge.jdbt.runtime;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.realityforge.jdbt.config.ImportConfig;
import org.realityforge.jdbt.db.DatabaseConnection;
import org.realityforge.jdbt.db.DatabaseMetadata;
import org.realityforge.jdbt.db.DbDriver;
import org.realityforge.jdbt.db.QueryResult;
import org.realityforge.jdbt.repository.RepositoryConfig;
import org.realityforge.jdbt.repository.RepositoryTable;

final class DatabaseStatisticsExporterTest {
    private static final List<String> COLUMNS = List.of(
            "has_view_definition",
            "schema_name",
            "table_name",
            "index_name",
            "index_id",
            "index_type",
            "is_disabled",
            "is_hypothetical",
            "row_count");
    private static final DatabaseConnection CONNECTION =
            new DatabaseConnection("localhost", 1433, "rose", "sa", "secret");

    @Test
    void exportsDeterministicModeledStatistics(@TempDir final Path tempDir) throws IOException {
        final var driver = new RecordingDriver(new QueryResult(
                COLUMNS,
                List.of(
                        row(1, "Other", "Ignored", "IX_Ignored", 2, 7, 1, 1, null),
                        row(1, "A,Schema", "A\"Table", "IX_Zed", 3, 2, false, false, 4L),
                        row(1, "Zed", "Thing", "PK_Thing", 1, 1, 0, 0, 19L),
                        row(1, "A,Schema", "A\"Table", "PK_A", 1, 1, false, false, 12L),
                        row(1, "A,Schema", "A\"Table", "IX_Alpha", 2, 2, false, false, 7L))));
        final var output = tempDir.resolve("nested/statistics.csv");

        final var count = new DatabaseStatisticsExporter(driver).export(repository(), CONNECTION, output);

        assertThat(count).isEqualTo(6);
        assertThat(output).content(StandardCharsets.UTF_8).isEqualTo("""
            object_type,schema,table,index,metric,value
            table,"A,Schema","A""Table",,approximate_row_count,12
            index,"A,Schema","A""Table",IX_Alpha,approximate_row_count,7
            index,"A,Schema","A""Table",IX_Zed,approximate_row_count,4
            index,"A,Schema","A""Table",PK_A,approximate_row_count,12
            table,Zed,Thing,,approximate_row_count,19
            index,Zed,Thing,PK_Thing,approximate_row_count,19
            """);
        assertThat(driver.events).containsExactly("open:rose", "query", "close");
        assertThat(driver.query).contains("sys.partitions", "SUM(rows)", "HAS_PERMS_BY_NAME");
    }

    @Test
    void aggregatesModeledDriftAndPreservesExistingOutput(@TempDir final Path tempDir) throws IOException {
        final var driver = new RecordingDriver(new QueryResult(
                COLUMNS,
                List.of(
                        row(0, "A,Schema", "A\"Table", "PK_A", 1, 1, 1, 0, 12L),
                        row(0, "A,Schema", "A\"Table", "IX_Alpha", 2, 2, 0, 1, null),
                        row(0, "A,Schema", "A\"Table", "IX_Zed", 3, 7, 0, 0, -1L))));
        final var output = tempDir.resolve("statistics.csv");
        Files.writeString(output, "old\n", StandardCharsets.UTF_8);

        assertThatThrownBy(() -> new DatabaseStatisticsExporter(driver).export(repository(), CONNECTION, output))
                .isInstanceOf(RuntimeExecutionException.class)
                .hasMessageContaining("VIEW DEFINITION")
                .hasMessageContaining("table A,Schema.A\"Table is disabled")
                .hasMessageContaining("IX_Alpha is hypothetical")
                .hasMessageContaining("IX_Alpha has no partition row count")
                .hasMessageContaining("IX_Zed uses unsupported SQL Server index type 7")
                .hasMessageContaining("IX_Zed has invalid negative row count -1")
                .hasMessageContaining("Missing modeled table Zed.Thing");
        assertThat(output).content(StandardCharsets.UTF_8).isEqualTo("old\n");
        assertThat(driver.events).containsExactly("open:rose", "query", "close");
    }

    @Test
    void closesConnectionAndPreservesOutputWhenQueryFails(@TempDir final Path tempDir) throws IOException {
        final var driver = new RecordingDriver(null);
        final var output = tempDir.resolve("statistics.csv");
        Files.writeString(output, "old\n", StandardCharsets.UTF_8);

        assertThatThrownBy(() -> new DatabaseStatisticsExporter(driver).export(repository(), CONNECTION, output))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("query failed");
        assertThat(output).content(StandardCharsets.UTF_8).isEqualTo("old\n");
        assertThat(driver.events).containsExactly("open:rose", "query", "close");
    }

    @Test
    void rejectsUnexpectedColumnsBeforeReplacingOutput(@TempDir final Path tempDir) throws IOException {
        final var driver = new RecordingDriver(new QueryResult(List.of("wrong"), List.of()));
        final var output = tempDir.resolve("statistics.csv");
        Files.writeString(output, "old\n", StandardCharsets.UTF_8);

        assertThatThrownBy(() -> new DatabaseStatisticsExporter(driver).export(repository(), CONNECTION, output))
                .isInstanceOf(RuntimeExecutionException.class)
                .hasMessageContaining("Unexpected database statistics columns");
        assertThat(output).content(StandardCharsets.UTF_8).isEqualTo("old\n");
    }

    @Test
    void preservesDestinationAndRemovesTemporaryFileWhenAtomicMoveFails(@TempDir final Path tempDir)
            throws IOException {
        final var driver = new RecordingDriver(new QueryResult(
                COLUMNS,
                List.of(
                        row(1, "A,Schema", "A\"Table", "PK_A", 1, 1, 0, 0, 12L),
                        row(1, "A,Schema", "A\"Table", "IX_Alpha", 2, 2, 0, 0, 7L),
                        row(1, "A,Schema", "A\"Table", "IX_Zed", 3, 2, 0, 0, 4L),
                        row(1, "Zed", "Thing", "PK_Thing", 1, 1, 0, 0, 19L))));
        final var output = Files.createDirectory(tempDir.resolve("statistics.csv"));
        Files.writeString(output.resolve("sentinel"), "old\n", StandardCharsets.UTF_8);

        assertThatThrownBy(() -> new DatabaseStatisticsExporter(driver).export(repository(), CONNECTION, output))
                .isInstanceOfAny(RuntimeExecutionException.class, UncheckedIOException.class);
        assertThat(output.resolve("sentinel")).content(StandardCharsets.UTF_8).isEqualTo("old\n");
        try (var files = Files.list(tempDir)) {
            assertThat(files.map(path -> path.getFileName().toString()))
                    .noneMatch(name -> name.startsWith(".jdbt-statistics-"));
        }
    }

    private static RepositoryConfig repository() {
        return new RepositoryConfig(
                List.of("A", "Zed"),
                Map.of(),
                Map.of(
                        "A",
                        List.of(new RepositoryTable(
                                "[A,Schema].[A\"Table]", List.of("[ID]"), List.of("[PK_A]", "[IX_Zed]", "[IX_Alpha]"))),
                        "Zed",
                        List.of(new RepositoryTable("[Zed].[Thing]", List.of("[ID]"), List.of("[PK_Thing]")))),
                Map.of("A", List.of(), "Zed", List.of()));
    }

    private static List<Object> row(
            final int permission,
            final String schema,
            final String table,
            final String index,
            final int indexId,
            final int indexType,
            final Object disabled,
            final Object hypothetical,
            final @Nullable Long rowCount) {
        return Arrays.asList(permission, schema, table, index, indexId, indexType, disabled, hypothetical, rowCount);
    }

    private static final class RecordingDriver implements DbDriver {
        private final @Nullable QueryResult result;
        private final ArrayList<String> events = new ArrayList<>();
        private String query = "";

        private RecordingDriver(final @Nullable QueryResult result) {
            this.result = result;
        }

        @Override
        public void open(final DatabaseConnection connection, final boolean openControlDatabase) {
            events.add("open:" + connection.database());
        }

        @Override
        public void close() {
            events.add("close");
        }

        @Override
        public QueryResult query(final String sql) {
            events.add("query");
            query = sql;
            if (null == result) {
                throw new IllegalStateException("query failed");
            }
            return result;
        }

        @Override
        public void drop(final DatabaseMetadata database, final DatabaseConnection connection) {}

        @Override
        public void createDatabase(final DatabaseMetadata database, final DatabaseConnection connection) {}

        @Override
        public void createSchema(final String schemaName) {}

        @Override
        public void dropSchema(final String schemaName, final List<String> tablesInDropOrder) {}

        @Override
        public void execute(final String sql, final boolean executeInControlDatabase) {}

        @Override
        public void preFixtureImport(final String tableName) {}

        @Override
        public void insert(final String tableName, final Map<String, Object> record) {}

        @Override
        public void postFixtureImport(final String tableName) {}

        @Override
        public void updateSequence(final String sequenceName, final long value) {}

        @Override
        public void preTableImport(
                final DatabaseMetadata database, final ImportConfig importConfig, final String tableName) {}

        @Override
        public void postTableImport(
                final DatabaseMetadata database, final ImportConfig importConfig, final String tableName) {}

        @Override
        public void postDataModuleImport(
                final DatabaseMetadata database,
                final ImportConfig importConfig,
                final String moduleName,
                final List<String> tablesInOrder) {}

        @Override
        public void postDatabaseImport(final DatabaseMetadata database, final ImportConfig importConfig) {}

        @Override
        public List<String> columnNamesForTable(final String tableName) {
            return List.of();
        }

        @Override
        public List<String> primaryKeyColumnNamesForTable(final String tableName) {
            return List.of();
        }

        @Override
        public QueryResult verifySchemaConstraints(final String schemaName) {
            return new QueryResult(List.of(), List.of());
        }

        @Override
        public void setupMigrations() {}

        @Override
        public boolean shouldMigrate(final String namespace, final String migrationName) {
            return false;
        }

        @Override
        public void markMigrationAsRun(final String namespace, final String migrationName) {}

        @Override
        public String generateStandardImportSql(
                final String tableName,
                final String targetDatabase,
                final String sourceDatabase,
                final List<String> columns) {
            return "";
        }

        @Override
        public String generateStandardSequenceImportSql(
                final String sequenceName, final String targetDatabase, final String sourceDatabase) {
            return "";
        }

        @Override
        public String generateDefaultSequenceExportSql(final String sequenceName) {
            return "";
        }
    }
}
