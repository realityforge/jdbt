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

final class ProjectRuntimeLoaderTest {
    @Test
    void loadMergesRepositoryFromPreLocalAndPostArtifacts(@TempDir final Path tempDir) throws IOException {
        writeFile(tempDir, "jdbt.yml", """
                defaults:
                  defaultDatabase: default
                  searchDirs: [db]
                databases:
                  default:
                    preDbArtifacts: [pre.zip]
                    postDbArtifacts: [post.zip]
                """);
        writeFile(tempDir, "db/repository.yml", """
                modules:
                  Local:
                    tables: ["[Local].[tbl]"]
                    sequences: []
                """);
        writeArtifact(tempDir.resolve("pre.zip"), "data/repository.yml", """
                modules:
                  Pre:
                    tables: ["[Pre].[tbl]"]
                    sequences: []
                """);
        writeArtifact(tempDir.resolve("post.zip"), "data/repository.yml", """
                modules:
                  Post:
                    tables: ["[Post].[tbl]"]
                    sequences: []
                """);

        final ProjectRuntimeLoader.LoadedRuntime runtime = new ProjectRuntimeLoader(tempDir).load(null);

        assertThat(runtime.database().repository().modules()).containsExactly("Pre", "Local", "Post");
        assertThat(runtime.database().preDbArtifacts()).hasSize(1);
        assertThat(runtime.database().postDbArtifacts()).hasSize(1);
        assertThat(runtime.database().schemaHash()).isNotBlank();
    }

    @Test
    void loadRejectsDuplicateRepositoryFilesInSearchPath(@TempDir final Path tempDir) throws IOException {
        writeFile(tempDir, "jdbt.yml", """
                defaults:
                  defaultDatabase: default
                databases:
                  default:
                    searchDirs: [dbA, dbB]
                """);
        writeFile(tempDir, "dbA/repository.yml", """
                modules:
                  A:
                    tables: []
                    sequences: []
                """);
        writeFile(tempDir, "dbB/repository.yml", """
                modules:
                  B:
                    tables: []
                    sequences: []
                """);

        assertThatThrownBy(() -> new ProjectRuntimeLoader(tempDir).load(null))
                .isInstanceOf(ConfigException.class)
                .hasMessageContaining("Duplicate copies of repository.yml");
    }

    @Test
    void loadRequiresDatabaseSelectionWhenNoDefault(@TempDir final Path tempDir) throws IOException {
        writeFile(tempDir, "jdbt.yml", """
                databases:
                  default:
                    searchDirs: [db]
                """);
        writeFile(tempDir, "db/repository.yml", """
                modules:
                  A:
                    tables: []
                    sequences: []
                """);

        assertThatThrownBy(() -> new ProjectRuntimeLoader(tempDir).load(null))
                .isInstanceOf(ConfigException.class)
                .hasMessageContaining("No database specified via --database");
    }

    @Test
    void loadRejectsArtifactMissingRepositoryYml(@TempDir final Path tempDir) throws IOException {
        writeFile(tempDir, "jdbt.yml", """
                defaults:
                  defaultDatabase: default
                databases:
                  default:
                    searchDirs: [db]
                    preDbArtifacts: [pre.zip]
                """);
        writeFile(tempDir, "db/repository.yml", """
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

    private void writeFile(final Path root, final String relativePath, final String content) throws IOException {
        final Path file = root.resolve(relativePath);
        Files.createDirectories(file.getParent());
        Files.writeString(file, content, StandardCharsets.UTF_8);
    }

    private void writeArtifact(final Path zip, final String entryName, final String content) throws IOException {
        try (var output = new ZipOutputStream(Files.newOutputStream(zip), StandardCharsets.UTF_8)) {
            output.putNextEntry(new ZipEntry(entryName));
            output.write(content.getBytes(StandardCharsets.UTF_8));
            output.closeEntry();
        }
    }
}
