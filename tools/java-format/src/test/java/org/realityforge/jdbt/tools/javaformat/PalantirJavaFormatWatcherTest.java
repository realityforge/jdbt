package org.realityforge.jdbt.tools.javaformat;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.attribute.FileTime;
import java.time.Duration;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

final class PalantirJavaFormatWatcherTest {
    @Test
    void watchesChangesWithoutFormattingAtStartup(@TempDir final Path tempDir) throws Exception {
        final Path sourceRoot = Files.createDirectories(tempDir.resolve("src"));
        Files.createDirectories(tempDir.resolve("tools"));
        final Path existing = write(sourceRoot.resolve("Existing.java"), "class Existing{}\n");
        final var outputBytes = new ByteArrayOutputStream();
        final var errorBytes = new ByteArrayOutputStream();
        final var failure = new AtomicReference<Exception>();
        final Thread thread;
        try (var watcher = watcher(tempDir, outputBytes, errorBytes)) {
            thread = start(watcher, failure);
            TimeUnit.MILLISECONDS.sleep(250);
            assertThat(existing).content(StandardCharsets.UTF_8).isEqualTo("class Existing{}\n");

            write(existing, "class Existing{int value;}\n");
            awaitContent(existing, """
                class Existing {
                    int value;
                }
                """);
            final FileTime formattedTime = Files.getLastModifiedTime(existing);
            TimeUnit.MILLISECONDS.sleep(350);
            assertThat(Files.getLastModifiedTime(existing)).isEqualTo(formattedTime);

            write(existing, "class Existing {");
            awaitText(errorBytes, "Unable to format src/Existing.java");
            assertThat(existing).content(StandardCharsets.UTF_8).isEqualTo("class Existing {");
            write(existing, "class Existing{}\n");
            awaitContent(existing, "class Existing {}\n");

            final Path replacement = write(tempDir.resolve("Replacement.java"), "class Existing{int replaced;}\n");
            Files.move(replacement, existing, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
            awaitContent(existing, """
                class Existing {
                    int replaced;
                }
                """);

            final Path created = write(sourceRoot.resolve("new/package/Created.java"), "class Created{}\n");
            awaitContent(created, "class Created {}\n");
        }
        thread.join(Duration.ofSeconds(5).toMillis());
        assertThat(thread.isAlive()).isFalse();
        assertThat(failure.get()).isNull();
        assertThat(outputBytes.toString(StandardCharsets.UTF_8))
                .contains("Formatted src/Existing.java", "Formatted src/new/package/Created.java");
    }

    @Test
    void rejectsSymlinksAndOutsidePaths(@TempDir final Path tempDir) throws IOException {
        final Path sourceRoot = Files.createDirectories(tempDir.resolve("src"));
        Files.createDirectories(tempDir.resolve("tools"));
        final Path target = write(tempDir.resolve("Target.java"), "class Target{}\n");
        final Path linked = sourceRoot.resolve("Linked.java");
        Files.createSymbolicLink(linked, target);
        final Path outside =
                write(tempDir.resolveSibling(tempDir.getFileName() + "-Outside.java"), "class Outside{}\n");
        final var errorBytes = new ByteArrayOutputStream();

        try (var watcher = watcher(tempDir, new ByteArrayOutputStream(), errorBytes)) {
            assertThat(watcher.formatChangedPath(linked)).isFalse();
            assertThat(watcher.formatChangedPath(outside)).isFalse();
        }

        assertThat(target).content(StandardCharsets.UTF_8).isEqualTo("class Target{}\n");
        assertThat(outside).content(StandardCharsets.UTF_8).isEqualTo("class Outside{}\n");
        assertThat(errorBytes.toString(StandardCharsets.UTF_8))
                .contains("Rejected Java format path: src/Linked.java", outside.toString());
        Files.delete(outside);
    }

    @Test
    void overflowRescansEligibleRoots(@TempDir final Path tempDir) throws IOException {
        final Path sourceRoot = Files.createDirectories(tempDir.resolve("src"));
        Files.createDirectories(tempDir.resolve("tools"));
        final Path source = write(sourceRoot.resolve("Overflow.java"), "class Overflow{}\n");
        final var errorBytes = new ByteArrayOutputStream();

        try (var watcher = watcher(tempDir, new ByteArrayOutputStream(), errorBytes)) {
            watcher.recoverFromOverflow();
        }

        assertThat(source).content(StandardCharsets.UTF_8).isEqualTo("class Overflow {}\n");
        assertThat(errorBytes.toString(StandardCharsets.UTF_8)).contains("overflow; rescanning src/ and tools/");
    }

    private static PalantirJavaFormatWatcher watcher(
            final Path workspace, final ByteArrayOutputStream output, final ByteArrayOutputStream error)
            throws IOException {
        return new PalantirJavaFormatWatcher(
                workspace,
                new PalantirFormatter(),
                new PrintStream(output, true, StandardCharsets.UTF_8),
                new PrintStream(error, true, StandardCharsets.UTF_8));
    }

    private static Thread start(final PalantirJavaFormatWatcher watcher, final AtomicReference<Exception> failure) {
        final var thread = new Thread(
                () -> {
                    try {
                        watcher.watch();
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        failure.set(e);
                    } catch (IOException e) {
                        failure.set(e);
                    }
                },
                "java-format-watch-test");
        thread.start();
        return thread;
    }

    private static Path write(final Path path, final String content) throws IOException {
        Files.createDirectories(path.getParent());
        Files.writeString(path, content, StandardCharsets.UTF_8);
        return path;
    }

    private static void awaitContent(final Path path, final String expected) throws IOException, InterruptedException {
        final long deadline = System.nanoTime() + Duration.ofSeconds(5).toNanos();
        while (System.nanoTime() < deadline) {
            if (expected.equals(Files.readString(path, StandardCharsets.UTF_8))) {
                return;
            }
            TimeUnit.MILLISECONDS.sleep(20);
        }
        assertThat(path).content(StandardCharsets.UTF_8).isEqualTo(expected);
    }

    private static void awaitText(final ByteArrayOutputStream output, final String expected)
            throws InterruptedException {
        final long deadline = System.nanoTime() + Duration.ofSeconds(5).toNanos();
        while (System.nanoTime() < deadline) {
            if (output.toString(StandardCharsets.UTF_8).contains(expected)) {
                return;
            }
            TimeUnit.MILLISECONDS.sleep(20);
        }
        assertThat(output.toString(StandardCharsets.UTF_8)).contains(expected);
    }
}
