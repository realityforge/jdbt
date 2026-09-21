package org.realityforge.jdbt.tools.javaformat;

import static org.assertj.core.api.Assertions.assertThat;

import com.palantir.javaformat.java.FormatterException;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

final class PalantirJavaFormatMainTest {
    @Test
    void formatsOnlyWorkspaceJavaSourcesInDeterministicOrder(@TempDir final Path tempDir)
            throws IOException, FormatterException {
        final Path source = write(tempDir.resolve("src/X.java"), "class X{}\n");
        final Path tool = write(tempDir.resolve("tools/Y.java"), "class Y{}\n");
        final Path ignored = write(tempDir.resolve("other/Z.java"), "class Z{}\n");
        final Path text = write(tempDir.resolve("src/readme.txt"), "class Readme{}\n");
        final Path linkedTarget = write(tempDir.resolve("Linked.java"), "class Linked{}\n");
        Files.createSymbolicLink(tempDir.resolve("src/Linked.java"), linkedTarget);

        assertThat(PalantirJavaFormatMain.discoverSources(tempDir)).containsExactly(source, tool);
        assertThat(PalantirJavaFormatMain.formatWorkspace(tempDir, new PalantirFormatter()))
                .isEqualTo(2);
        assertThat(source).content(StandardCharsets.UTF_8).isEqualTo("class X {}\n");
        assertThat(tool).content(StandardCharsets.UTF_8).isEqualTo("class Y {}\n");
        assertThat(ignored).content(StandardCharsets.UTF_8).isEqualTo("class Z{}\n");
        assertThat(text).content(StandardCharsets.UTF_8).isEqualTo("class Readme{}\n");
        assertThat(linkedTarget).content(StandardCharsets.UTF_8).isEqualTo("class Linked{}\n");
    }

    @Test
    void validatesInvocation(@TempDir final Path tempDir) throws IOException, FormatterException {
        Files.createDirectories(tempDir.resolve("src"));

        assertThat(PalantirJavaFormatMain.run(new String[] {"--write"}, Map.of()))
                .isEqualTo(2);
        assertThat(PalantirJavaFormatMain.run(new String[] {"--check"}, Map.of()))
                .isEqualTo(2);
        assertThat(PalantirJavaFormatMain.run(
                        new String[] {"--write"}, Map.of("BUILD_WORKSPACE_DIRECTORY", tempDir.toString())))
                .isZero();
    }

    private static Path write(final Path path, final String content) throws IOException {
        Files.createDirectories(path.getParent());
        Files.writeString(path, content, StandardCharsets.UTF_8);
        return path;
    }
}
