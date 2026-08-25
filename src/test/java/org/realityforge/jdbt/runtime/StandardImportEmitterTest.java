package org.realityforge.jdbt.runtime;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.realityforge.jdbt.config.ImportConfig;
import org.realityforge.jdbt.db.DbDriverFactory;
import org.realityforge.jdbt.repository.RepositoryConfig;
import org.realityforge.jdbt.repository.RepositoryTable;
import org.realityforge.jdbt.repository.RowSource;

final class StandardImportEmitterTest {
    @Test
    void emitsOrderedImportTablesAndSequencesAndOmitsDeploymentTables(@TempDir final Path project) throws IOException {
        final var repository = new RepositoryConfig(
                List.of("First", "Second"),
                Map.of(),
                Map.of(
                        "First",
                        List.of(
                                new RepositoryTable("[First].[tblImport]", List.of("[ID]", "[Name]"), RowSource.IMPORT),
                                new RepositoryTable("[First].[tblDeployment]", List.of("[ID]"), RowSource.DEPLOYMENT)),
                        "Second",
                        List.of(new RepositoryTable("[Second].[tblOther]", List.of("[Value]")))),
                Map.of("First", List.of("[First].[ThingSeq]"), "Second", List.of()));
        final var database = database(
                project,
                repository,
                Map.of(
                        "selected",
                        new ImportConfig("selected", List.of("Second", "First"), "import", List.of(), List.of())));

        final var output = new StandardImportEmitter(new DbDriverFactory().create("sqlserver"))
                .emit(database, "selected", Path.of("generated"), false);

        assertThat(output).isEqualTo(project.resolve("generated").toRealPath());
        try (var files = Files.walk(output)) {
            assertThat(files.filter(Files::isRegularFile)
                            .map(output::relativize)
                            .toList())
                    .containsExactlyInAnyOrder(
                            Path.of("Second/import/Second.tblOther.sql"),
                            Path.of("First/import/First.tblImport.sql"),
                            Path.of("First/import/First.ThingSeq.sql"));
        }
        assertThat(output.resolve("First/import/First.tblDeployment.sql")).doesNotExist();
        assertThat(output.resolve("First/import/First.tblImport.sql"))
                .content(StandardCharsets.UTF_8)
                .isEqualTo("""
                    INSERT INTO [__TARGET__].[First].[tblImport]([ID], [Name])
                      SELECT [ID], [Name] FROM [__SOURCE__].[First].[tblImport]
                    """)
                .doesNotContain("IDENTITY_INSERT");
        assertThat(output.resolve("First/import/First.ThingSeq.sql"))
                .content(StandardCharsets.UTF_8)
                .contains("[__SOURCE__].sys.sequences", "USE [__TARGET__]", "[First].[ThingSeq]");
    }

    @Test
    void defaultsOutputToProjectTmpImportsAndReplacesExistingContent(@TempDir final Path project) throws IOException {
        final var database = database(project, repository(), imports());
        final var stale = project.resolve("tmp/imports/stale.txt");
        Files.createDirectories(stale.getParent());
        Files.writeString(stale, "stale", StandardCharsets.UTF_8);

        final var output = new StandardImportEmitter(new DbDriverFactory().create("sqlserver"))
                .emit(database, "default", null, false);

        assertThat(output).isEqualTo(project.resolve("tmp/imports").toRealPath());
        assertThat(stale).doesNotExist();
        assertThat(output.resolve("Core/import/Core.tbl.sql")).exists();
    }

    @Test
    void nonEmptyCustomOutputRequiresReplace(@TempDir final Path project) throws IOException {
        final var database = database(project, repository(), imports());
        final var output = project.resolve("custom");
        Files.createDirectories(output);
        Files.writeString(output.resolve("keep.txt"), "keep", StandardCharsets.UTF_8);
        final var emitter = new StandardImportEmitter(new DbDriverFactory().create("sqlserver"));

        assertThatThrownBy(() -> emitter.emit(database, "default", output, false))
                .isInstanceOf(RuntimeExecutionException.class)
                .hasMessageContaining("--replace");
        assertThat(output.resolve("keep.txt")).content().isEqualTo("keep");

        emitter.emit(database, "default", output, true);
        assertThat(output.resolve("keep.txt")).doesNotExist();
        assertThat(output.resolve("Core/import/Core.tbl.sql")).exists();
    }

    @Test
    void unsupportedDriverFailsBeforeOutputMutation(@TempDir final Path project) throws IOException {
        final var output = project.resolve("custom");
        Files.createDirectories(output);
        Files.writeString(output.resolve("keep.txt"), "keep", StandardCharsets.UTF_8);

        for (final var driver : List.of("postgres", "noop")) {
            assertThatThrownBy(() -> new StandardImportEmitter(new DbDriverFactory().create(driver))
                            .emit(database(project, repository(), imports()), "default", output, true))
                    .isInstanceOf(RuntimeExecutionException.class)
                    .hasMessageContaining("does not support");
        }
        assertThat(output.resolve("keep.txt")).content().isEqualTo("keep");
    }

    @Test
    void rejectsFilesystemRootProjectAndProjectAncestorsEvenWithReplace(@TempDir final Path project) {
        final var emitter = new StandardImportEmitter(new DbDriverFactory().create("sqlserver"));
        final var database = database(project, repository(), imports());

        for (final var forbidden :
                Stream.iterate(project, path -> null != path, Path::getParent).toList()) {
            assertThatThrownBy(() -> emitter.emit(database, "default", forbidden, true))
                    .isInstanceOf(RuntimeExecutionException.class)
                    .hasMessageContaining("must not");
        }
    }

    @Test
    void rejectsSymbolicLinkDestinationBeforeMutation(@TempDir final Path project) throws IOException {
        final var actual = project.resolve("actual");
        Files.createDirectories(actual);
        Files.writeString(actual.resolve("keep.txt"), "keep", StandardCharsets.UTF_8);
        final var link = project.resolve("linked-output");
        Files.createSymbolicLink(link, actual);

        assertThatThrownBy(() -> new StandardImportEmitter(new DbDriverFactory().create("sqlserver"))
                        .emit(database(project, repository(), imports()), "default", link, true))
                .isInstanceOf(RuntimeExecutionException.class)
                .hasMessageContaining("symbolic link");
        assertThat(actual.resolve("keep.txt")).content().isEqualTo("keep");
    }

    @Test
    void resolvesDeepestExistingAncestorForNestedOutput(@TempDir final Path project) throws IOException {
        final var database = database(project, repository(), imports());

        final var output = new StandardImportEmitter(new DbDriverFactory().create("sqlserver"))
                .emit(database, "default", Path.of("new/child/imports"), false);

        assertThat(output).isEqualTo(project.resolve("new/child/imports").toRealPath());
        assertThat(output.resolve("Core/import/Core.tbl.sql")).exists();
    }

    @Test
    void generationFailureLeavesExistingOutputIntact(@TempDir final Path project) throws IOException {
        final var output = project.resolve("custom");
        Files.createDirectories(output);
        Files.writeString(output.resolve("keep.txt"), "keep", StandardCharsets.UTF_8);
        final var repository = new RepositoryConfig(
                List.of("Core"),
                Map.of(),
                Map.of(
                        "Core",
                        List.of(
                                new RepositoryTable("[Core].[tbl]", List.of("[ID]")),
                                new RepositoryTable("\"Core\".\"tbl\"", List.of("\"ID\"")))),
                Map.of("Core", List.of()));

        assertThatThrownBy(() -> new StandardImportEmitter(new DbDriverFactory().create("sqlserver"))
                        .emit(database(project, repository, imports()), "default", output, true))
                .isInstanceOf(RuntimeExecutionException.class)
                .hasMessageContaining("Multiple repository objects");
        assertThat(output.resolve("keep.txt")).content().isEqualTo("keep");
    }

    @Test
    void rejectsRepositoryNamesThatCouldEscapeStaging(@TempDir final Path project) {
        final var unsafeModule = "../escaped";
        final var moduleRepository = new RepositoryConfig(
                List.of(unsafeModule),
                Map.of(),
                Map.of(unsafeModule, List.of(new RepositoryTable("[Core].[tbl]", List.of("[ID]")))),
                Map.of(unsafeModule, List.of()));
        final var emitter = new StandardImportEmitter(new DbDriverFactory().create("sqlserver"));

        assertThatThrownBy(() -> emitter.emit(
                        database(
                                project,
                                moduleRepository,
                                Map.of(
                                        "default",
                                        new ImportConfig(
                                                "default", List.of(unsafeModule), "import", List.of(), List.of()))),
                        "default",
                        Path.of("generated"),
                        false))
                .isInstanceOf(RuntimeExecutionException.class)
                .hasMessageContaining("unsafe path name");
        assertThat(project.resolve("generated")).doesNotExist();
        assertThat(project.resolve("../escaped")).doesNotExist();
    }

    private static RepositoryConfig repository() {
        return new RepositoryConfig(
                List.of("Core"),
                Map.of(),
                Map.of("Core", List.of(new RepositoryTable("[Core].[tbl]", List.of("[ID]")))),
                Map.of("Core", List.of()));
    }

    private static Map<String, ImportConfig> imports() {
        return Map.of("default", new ImportConfig("default", List.of("Core"), "import", List.of(), List.of()));
    }

    private static RuntimeDatabase database(
            final Path project, final RepositoryConfig repository, final Map<String, ImportConfig> imports) {
        return new RuntimeDatabase(
                "default",
                repository,
                List.of(project),
                List.of(),
                List.of(),
                "index.txt",
                List.of(),
                List.of(),
                List.of(),
                List.of(),
                List.of(),
                "fixtures",
                "datasets",
                List.of(),
                List.of(),
                List.of(),
                false,
                false,
                "migrations",
                null,
                null,
                imports,
                Map.of());
    }
}
