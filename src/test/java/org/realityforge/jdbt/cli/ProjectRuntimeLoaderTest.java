package org.realityforge.jdbt.cli;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.realityforge.jdbt.config.ConfigException;
import org.realityforge.jdbt.files.FileCollectionException;

final class ProjectRuntimeLoaderTest {
    @Test
    void loadRejectsProjectWithoutConfiguration(@TempDir final Path tempDir) {
        assertThatThrownBy(() -> new ProjectRuntimeLoader(tempDir).validate(null))
                .isInstanceOf(ConfigException.class)
                .hasMessageContaining("jdbt.yml not found")
                .hasMessageContaining(tempDir.toString());
    }

    @Test
    void loadUsesProjectManifestsAndConfigRelativeResourceRoot(@TempDir final Path tempDir) throws IOException {
        final var projectDirectory = tempDir.resolve("profiles/Mail");
        final var resourceRoot = tempDir.resolve("database");
        writeFile(projectDirectory, "jdbt.yml", "resourceRoot: ../../database\n");
        writeFile(projectDirectory, "repository.yml", """
            modules:
              Mail:
                tables: []
                sequences: []
            """);
        writeFile(resourceRoot, "Mail/schema.sql", "SELECT 1");
        writeFile(resourceRoot, "Payments/schema.sql", "SELECT 2");

        final var runtime = new ProjectRuntimeLoader(projectDirectory).load(null);

        assertThat(runtime.database().repository().modules()).containsExactly("Mail");
        assertThat(runtime.database().searchDirs()).containsExactly(resourceRoot);
        assertThat(runtime.projectDirectory()).isEqualTo(projectDirectory);
        assertThat(runtime.database().schemaHash()).isNotBlank();
    }

    @Test
    void loadRejectsMissingResourceRoot(@TempDir final Path tempDir) throws IOException {
        writeFile(tempDir, "jdbt.yml", "resourceRoot: missing\n");
        writeFile(tempDir, "repository.yml", """
            modules:
              A:
                tables: []
                sequences: []
            """);

        assertThatThrownBy(() -> new ProjectRuntimeLoader(tempDir).validate(null))
                .isInstanceOf(ConfigException.class)
                .hasMessageContaining("resourceRoot")
                .hasMessageContaining("is not a directory");
    }

    @Test
    void validateRejectsUnknownImportResource(@TempDir final Path tempDir) throws IOException {
        writeFile(tempDir, "jdbt.yml", """
            imports:
              default:
                modules: [A]
            """);
        writeFile(tempDir, "repository.yml", """
            modules:
              A:
                tables:
                  - name: '[A].[Known]'
                    columns: ['[Id]']
                    indexes: []
                sequences: []
            """);
        writeFile(tempDir, "A/import/A.Unknown.yml", "id: {}\n");

        assertThatThrownBy(() -> new ProjectRuntimeLoader(tempDir).validate(null))
                .isInstanceOf(FileCollectionException.class)
                .hasMessageContaining("Unexpected yml files")
                .hasMessageContaining("A.Unknown.yml");
    }

    @Test
    void validateRejectsOutOfRootResourcePath(@TempDir final Path tempDir) throws IOException {
        writeFile(tempDir, "jdbt.yml", "preCreateDirs: [../shared-hooks]\n");
        writeFile(tempDir, "repository.yml", """
            modules:
              A:
                tables: []
                sequences: []
            """);

        assertThatThrownBy(() -> new ProjectRuntimeLoader(tempDir).validate(null))
                .isInstanceOf(ConfigException.class)
                .hasMessageContaining("must remain beneath resourceRoot")
                .hasMessageContaining("../shared-hooks");
    }

    @Test
    void schemaHashIncludesDatasetResources(@TempDir final Path tempDir) throws IOException {
        writeFile(tempDir, "jdbt.yml", "datasets: [sample]\n");
        writeFile(tempDir, "repository.yml", """
            modules:
              A:
                tables:
                  - name: '[A].[Known]'
                    columns: ['[Id]']
                    indexes: []
                sequences: []
            """);
        writeFile(tempDir, "A/datasets/sample/A.Known.yml", "id: {value: 1}\n");

        final var firstHash =
                new ProjectRuntimeLoader(tempDir).load(null).database().schemaHash();
        writeFile(tempDir, "A/datasets/sample/A.Known.yml", "id: {value: 2}\n");
        final var secondHash =
                new ProjectRuntimeLoader(tempDir).load(null).database().schemaHash();

        assertThat(secondHash).isNotEqualTo(firstHash);
    }

    @Test
    void loadMergesRepositoryFromPreLocalAndPostArtifacts(@TempDir final Path tempDir) throws IOException {
        writeFile(tempDir, "jdbt.yml", """
            preDbArtifacts: [pre.zip]
            postDbArtifacts: [post.zip]
            """);
        writeFile(tempDir, "repository.yml", """
            modules:
              Local:
                tables: [{name: "[Local].[tbl]", columns: ["[ID]"], indexes: []}]
                sequences: []
            """);
        writeArtifact(tempDir.resolve("pre.zip"), "data/repository.yml", """
            modules:
              Pre:
                tables: [{name: "[Pre].[tbl]", columns: ["[ID]"], indexes: []}]
                sequences: []
            """);
        writeArtifact(tempDir.resolve("post.zip"), "data/repository.yml", """
            modules:
              Post:
                tables: [{name: "[Post].[tbl]", columns: ["[ID]"], indexes: []}]
                sequences: []
            """);

        final var runtime = new ProjectRuntimeLoader(tempDir).load(null);

        assertThat(runtime.database().repository().modules()).containsExactly("Pre", "Local", "Post");
        assertThat(runtime.database().preDbArtifacts()).hasSize(1);
        assertThat(runtime.database().postDbArtifacts()).hasSize(1);
        assertThat(runtime.database().schemaHash()).isNotBlank();
    }

    @Test
    void loadRejectsSearchDirsSetting(@TempDir final Path tempDir) throws IOException {
        writeFile(tempDir, "jdbt.yml", """
            searchDirs: [db]
            """);
        writeFile(tempDir, "repository.yml", """
            modules:
              A:
                tables: []
                sequences: []
            """);

        assertThatThrownBy(() -> new ProjectRuntimeLoader(tempDir).load(null))
                .isInstanceOf(ConfigException.class)
                .hasMessageContaining("Unknown key 'searchDirs'");
    }

    @Test
    void loadRejectsResourcePrefixSetting(@TempDir final Path tempDir) throws IOException {
        writeFile(tempDir, "jdbt.yml", """
            resourcePrefix: data
            """);
        writeFile(tempDir, "repository.yml", """
            modules:
              A:
                tables: []
                sequences: []
            """);

        assertThatThrownBy(() -> new ProjectRuntimeLoader(tempDir).load(null))
                .isInstanceOf(ConfigException.class)
                .hasMessageContaining("Unknown key 'resourcePrefix'");
    }

    @Test
    void loadUsesHardcodedDefaultDatabaseKey(@TempDir final Path tempDir) throws IOException {
        writeFile(tempDir, "jdbt.yml", "{}\n");
        writeFile(tempDir, "repository.yml", """
            modules:
              A:
                tables: []
                sequences: []
            """);

        final var runtime = new ProjectRuntimeLoader(tempDir).load(null);
        assertThat(runtime.database().key()).isEqualTo("default");
        assertThat(runtime.database().searchDirs()).containsExactly(tempDir);
    }

    @Test
    void schemaHashChangesWhenSqlContentChanges(@TempDir final Path tempDir) throws IOException {
        writeFile(tempDir, "jdbt.yml", "{}\n");
        writeFile(tempDir, "repository.yml", """
            modules:
              MyModule:
                tables: [{name: "[MyModule].[foo]", columns: ["[ID]"], indexes: []}]
                sequences: []
            """);
        writeFile(tempDir, "MyModule/a.sql", "SELECT 1");

        final var firstHash =
                new ProjectRuntimeLoader(tempDir).load(null).database().schemaHash();

        writeFile(tempDir, "MyModule/a.sql", "SELECT 2");

        final var secondHash =
                new ProjectRuntimeLoader(tempDir).load(null).database().schemaHash();
        assertThat(firstHash).hasSize(32);
        assertThat(secondHash).hasSize(32).isNotEqualTo(firstHash);
    }

    @Test
    void schemaHashDoesNotDependOnAbsoluteProjectLocation(@TempDir final Path tempDir) throws IOException {
        final var first = tempDir.resolve("first");
        final var second = tempDir.resolve("second");
        for (final var project : new Path[] {first, second}) {
            writeFile(project, "jdbt.yml", "{}\n");
            writeFile(project, "repository.yml", """
                modules:
                  MyModule:
                    tables: []
                    sequences: []
                """);
            writeFile(project, "MyModule/a.sql", "SELECT 1");
        }

        assertThat(new ProjectRuntimeLoader(first).load(null).database().schemaHash())
                .isEqualTo(
                        new ProjectRuntimeLoader(second).load(null).database().schemaHash());
    }

    @Test
    void loadRejectsSelectedDatabaseWhenNotHardcodedDefault(@TempDir final Path tempDir) throws IOException {
        writeFile(tempDir, "jdbt.yml", "{}\n");
        writeFile(tempDir, "repository.yml", """
            modules:
              A:
                tables: []
                sequences: []
            """);

        assertThatThrownBy(() -> new ProjectRuntimeLoader(tempDir).load("custom"))
                .isInstanceOf(ConfigException.class)
                .hasMessageContaining("Unable to locate database 'custom'");
    }

    @Test
    void loadRejectsArtifactMissingRepositoryYml(@TempDir final Path tempDir) throws IOException {
        writeFile(tempDir, "jdbt.yml", """
            preDbArtifacts: [pre.zip]
            """);
        writeFile(tempDir, "repository.yml", """
            modules:
              A:
                tables: []
                sequences: []
            """);
        writeArtifact(tempDir.resolve("pre.zip"), "data/not-repository.yml", "x");

        assertThatThrownBy(() -> new ProjectRuntimeLoader(tempDir).load("default"))
                .isInstanceOf(ConfigException.class)
                .hasMessageContaining("does not contain data/repository.yml");
    }

    private static void writeFile(final Path root, final String relativePath, final String content) throws IOException {
        final var file = root.resolve(relativePath);
        Files.createDirectories(file.getParent());
        Files.writeString(file, content, StandardCharsets.UTF_8);
    }

    private static void writeArtifact(final Path zip, final String entryName, final String content) throws IOException {
        try (var output = new ZipOutputStream(Files.newOutputStream(zip), StandardCharsets.UTF_8)) {
            output.putNextEntry(new ZipEntry(entryName));
            output.write(content.getBytes(StandardCharsets.UTF_8));
            output.closeEntry();
        }
    }
}
