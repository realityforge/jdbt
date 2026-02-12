package org.realityforge.jdbt.packaging;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.realityforge.jdbt.config.ImportConfig;
import org.realityforge.jdbt.config.ModuleGroupConfig;
import org.realityforge.jdbt.files.ArtifactContent;
import org.realityforge.jdbt.files.FileResolver;
import org.realityforge.jdbt.repository.RepositoryConfig;
import org.realityforge.jdbt.repository.RepositoryConfigLoader;
import org.realityforge.jdbt.runtime.RuntimeDatabase;
import org.realityforge.jdbt.runtime.RuntimeExecutionException;

final class DatabaseDataPackagerTest {
    @Test
    void packageDatabaseDataCopiesExpectedFilesAndWritesIndexes(@TempDir final Path tempDir) throws IOException {
        createFile(tempDir, "db/MyModule/index.txt", "b.sql\na.sql\n");
        createFile(tempDir, "db/MyModule/a.sql", "A");
        createFile(tempDir, "db/MyModule/b.sql", "B");
        createFile(tempDir, "db/MyModule/down/drop.sql", "DROP");
        createFile(tempDir, "db/MyModule/finalize/final.sql", "FINAL");
        createFile(tempDir, "db/MyModule/fixtures/MyModule.foo.yml", "1:\n  ID: 1\n");
        createFile(tempDir, "db/MyModule/fixtures/Unexpected.yml", "1:\n  ID: 2\n");
        createFile(tempDir, "db/MyModule/import/MyModule.foo.sql", "SELECT 1");
        createFile(tempDir, "db/MyModule/import/Unknown.sql", "SELECT 2");
        createFile(tempDir, "db/MyModule/datasets/seed/MyModule.foo.yml", "2:\n  ID: 2\n");
        createFile(tempDir, "db/MyModule/datasets/seed/Unknown.yml", "x: y\n");

        createFile(tempDir, "db/db-hooks/pre/pre.sql", "PRE");
        createFile(tempDir, "db/db-hooks/post/post.sql", "POST");
        createFile(tempDir, "db/import-hooks/pre/pre.sql", "IPRE");
        createFile(tempDir, "db/import-hooks/post/post.sql", "IPOST");
        createFile(tempDir, "db/datasets/seed/pre/pre.sql", "DSPRE");
        createFile(tempDir, "db/datasets/seed/post/post.sql", "DSPOST");
        createFile(tempDir, "db/migrations/001_a.sql", "M1");

        final var database = runtimeDatabase(
                List.of(tempDir.resolve("db")),
                List.of(),
                List.of(),
                true,
                new ImportConfig(
                        "default",
                        List.of("MyModule"),
                        "import",
                        List.of("import-hooks/pre"),
                        List.of("import-hooks/post")));

        final var output = tempDir.resolve("package");
        new DatabaseDataPackager(new FileResolver()).packageDatabaseData(database, output);

        assertThat(Files.readString(output.resolve("MyModule/index.txt"))).isEqualTo("b.sql\na.sql");
        assertThat(Files.readString(output.resolve("MyModule/down/index.txt"))).isEqualTo("drop.sql");
        assertThat(Files.readString(output.resolve("MyModule/finalize/index.txt")))
                .isEqualTo("final.sql");

        assertThat(output.resolve("MyModule/fixtures/MyModule.foo.yml")).exists();
        assertThat(output.resolve("MyModule/fixtures/Unexpected.yml")).doesNotExist();

        assertThat(output.resolve("MyModule/import/MyModule.foo.sql")).exists();
        assertThat(output.resolve("MyModule/import/Unknown.sql")).doesNotExist();

        assertThat(output.resolve("MyModule/datasets/seed/MyModule.foo.yml")).exists();
        assertThat(output.resolve("MyModule/datasets/seed/Unknown.yml")).doesNotExist();

        assertThat(Files.readString(output.resolve("db-hooks/pre/index.txt"))).isEqualTo("pre.sql");
        assertThat(Files.readString(output.resolve("db-hooks/post/index.txt"))).isEqualTo("post.sql");
        assertThat(Files.readString(output.resolve("import-hooks/pre/index.txt")))
                .isEqualTo("pre.sql");
        assertThat(Files.readString(output.resolve("import-hooks/post/index.txt")))
                .isEqualTo("post.sql");
        assertThat(Files.readString(output.resolve("datasets/seed/pre/index.txt")))
                .isEqualTo("pre.sql");
        assertThat(Files.readString(output.resolve("datasets/seed/post/index.txt")))
                .isEqualTo("post.sql");
        assertThat(Files.readString(output.resolve("migrations/index.txt"))).isEqualTo("001_a.sql");

        final var repositoryYaml = Files.readString(output.resolve("repository.yml"));
        final var loaded = new RepositoryConfigLoader().load(repositoryYaml, "repository.yml");
        assertThat(loaded).isEqualTo(database.repository());
    }

    @Test
    void packageDatabaseDataReadsFromArtifacts(@TempDir final Path tempDir) throws IOException {
        final var artifact = new InMemoryArtifact(
                "post",
                Map.of(
                        "MyModule/a.sql", "A",
                        "MyModule/index.txt", "a.sql",
                        "db-hooks/pre/pre.sql", "PRE",
                        "db-hooks/pre/index.txt", "pre.sql",
                        "migrations/001_a.sql", "M1",
                        "migrations/index.txt", "001_a.sql"));

        final var database = runtimeDatabase(
                List.of(tempDir.resolve("db")),
                List.of(),
                List.of(artifact),
                true,
                new ImportConfig(
                        "default",
                        List.of("MyModule"),
                        "import",
                        List.of("import-hooks/pre"),
                        List.of("import-hooks/post")));

        final var output = tempDir.resolve("package");
        new DatabaseDataPackager(new FileResolver()).packageDatabaseData(database, output);

        assertThat(Files.readString(output.resolve("MyModule/a.sql"))).isEqualTo("A");
        assertThat(Files.readString(output.resolve("db-hooks/pre/pre.sql"))).isEqualTo("PRE");
        assertThat(Files.readString(output.resolve("migrations/001_a.sql"))).isEqualTo("M1");
    }

    @Test
    void packageDatabaseDataSkipsMigrationsWhenDisabled(@TempDir final Path tempDir) throws IOException {
        createFile(tempDir, "db/MyModule/a.sql", "A");
        createFile(tempDir, "db/migrations/001_a.sql", "M1");

        final var database = runtimeDatabase(
                repositoryConfig(),
                List.of(tempDir.resolve("db")),
                List.of(),
                List.of(),
                false,
                Map.of("default", new ImportConfig("default", List.of("MyModule"), "import", List.of(), List.of())));

        final var output = tempDir.resolve("package");
        new DatabaseDataPackager(new FileResolver()).packageDatabaseData(database, output);

        assertThat(output.resolve("migrations")).doesNotExist();
    }

    @Test
    void packageDatabaseDataWritesDeterministicOutputAcrossImportMapOrder(@TempDir final Path tempDir)
            throws IOException {
        createFile(tempDir, "db/MyModule/a.sql", "A");
        createFile(tempDir, "db/import-hooks/pre/a.sql", "A");
        createFile(tempDir, "db/import-hooks/pre/b.sql", "B");
        createFile(tempDir, "db/import-hooks/post/c.sql", "C");

        final var alpha = new ImportConfig(
                "alpha", List.of("MyModule"), "import", List.of("import-hooks/pre"), List.of("import-hooks/post"));
        final var beta =
                new ImportConfig("beta", List.of("MyModule"), "import", List.of("import-hooks/pre"), List.of());

        final var first = runtimeDatabase(
                repositoryConfig(),
                List.of(tempDir.resolve("db")),
                List.of(),
                List.of(),
                true,
                Map.of("alpha", alpha, "beta", beta));
        final var second = runtimeDatabase(
                repositoryConfig(),
                List.of(tempDir.resolve("db")),
                List.of(),
                List.of(),
                true,
                Map.of("beta", beta, "alpha", alpha));

        final var output1 = tempDir.resolve("package-a");
        final var output2 = tempDir.resolve("package-b");
        final var packager = new DatabaseDataPackager(new FileResolver());
        packager.packageDatabaseData(first, output1);
        packager.packageDatabaseData(second, output2);

        final var zip1 = tempDir.resolve("a.zip");
        final var zip2 = tempDir.resolve("b.zip");
        final var zipPackager = new DeterministicZipPackager();
        zipPackager.write(output1, zip1);
        zipPackager.write(output2, zip2);

        assertThat(Files.readAllBytes(zip1)).containsExactly(Files.readAllBytes(zip2));
    }

    @Test
    void packageDatabaseDataPreservesSchemaOverridesInRepositoryYaml(@TempDir final Path tempDir) throws IOException {
        createFile(tempDir, "db/MyModule/a.sql", "A");
        final var repository = new RepositoryConfig(
                List.of("MyModule"),
                Map.of("MyModule", "CustomSchema"),
                Map.of("MyModule", List.of("[CustomSchema].[foo]")),
                Map.of("MyModule", List.of("[CustomSchema].[foo_seq]")));
        final var database = runtimeDatabase(
                repository,
                List.of(tempDir.resolve("db")),
                List.of(),
                List.of(),
                false,
                Map.of("default", new ImportConfig("default", List.of("MyModule"), "import", List.of(), List.of())));

        final var output = tempDir.resolve("package");
        new DatabaseDataPackager(new FileResolver()).packageDatabaseData(database, output);

        final var repositoryYaml = Files.readString(output.resolve("repository.yml"));
        final var loaded = new RepositoryConfigLoader().load(repositoryYaml, "repository.yml");
        assertThat(loaded).isEqualTo(repository);
    }

    private static RuntimeDatabase runtimeDatabase(
            final List<Path> searchDirs,
            final List<ArtifactContent> preArtifacts,
            final List<ArtifactContent> postArtifacts,
            final boolean migrationsEnabled,
            final ImportConfig importConfig) {
        return runtimeDatabase(
                repositoryConfig(),
                searchDirs,
                preArtifacts,
                postArtifacts,
                migrationsEnabled,
                Map.of("default", importConfig));
    }

    private static RuntimeDatabase runtimeDatabase(
            final RepositoryConfig repository,
            final List<Path> searchDirs,
            final List<ArtifactContent> preArtifacts,
            final List<ArtifactContent> postArtifacts,
            final boolean migrationsEnabled,
            final Map<String, ImportConfig> imports) {
        return new RuntimeDatabase(
                "default",
                repository,
                searchDirs,
                preArtifacts,
                postArtifacts,
                "index.txt",
                List.of("."),
                List.of("down"),
                List.of("finalize"),
                List.of("db-hooks/pre"),
                List.of("db-hooks/post"),
                "fixtures",
                "datasets",
                List.of("pre"),
                List.of("post"),
                List.of("seed"),
                migrationsEnabled,
                false,
                "migrations",
                "1",
                "hash",
                imports,
                Map.of("group", new ModuleGroupConfig("group", List.of("MyModule"), false)));
    }

    private static RepositoryConfig repositoryConfig() {
        return new RepositoryConfig(
                List.of("MyModule"),
                Map.of(),
                Map.of("MyModule", List.of("[MyModule].[foo]")),
                Map.of("MyModule", List.of()));
    }

    private static void createFile(final Path root, final String relativePath, final String content)
            throws IOException {
        final var file = root.resolve(relativePath);
        Files.createDirectories(file.getParent());
        Files.writeString(file, content, StandardCharsets.UTF_8);
    }

    private record InMemoryArtifact(String id, Map<String, String> entries) implements ArtifactContent {
        @Override
        public List<String> files() {
            return List.copyOf(entries.keySet());
        }

        @Override
        public String readText(final String path) {
            final var value = entries.get(path);
            if (null == value) {
                throw new RuntimeExecutionException("Missing artifact entry " + path);
            }
            return value;
        }
    }
}
