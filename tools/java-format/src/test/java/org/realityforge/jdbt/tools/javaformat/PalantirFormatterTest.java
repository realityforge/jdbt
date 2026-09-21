package org.realityforge.jdbt.tools.javaformat;

import static org.assertj.core.api.Assertions.assertThat;

import com.palantir.javaformat.java.FormatterException;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

final class PalantirFormatterTest {
    private final PalantirFormatter formatter = new PalantirFormatter();

    @Test
    void formatsSource() throws FormatterException {
        assertThat(formatter.format("package x; class X{void x(){System.out.println(\"x\");}}\n"))
                .isEqualTo("""
                    package x;

                    class X {
                        void x() {
                            System.out.println("x");
                        }
                    }
                    """);
    }

    @Test
    void matchesEstablishedCliGoldenOutputs() throws FormatterException {
        assertThat(formatter.format(
                        "package x; import java.util.Set; import java.util.List; class X{List<String> x=List.of();}\n"))
                .isEqualTo("""
                    package x;

                    import java.util.List;

                    class X {
                        List<String> x = List.of();
                    }
                    """);
        assertThat(formatter.format("package x; class X{String x=\"This is a deliberately long string that should be"
                        + " reflowed by the Palantir formatter because it extends well beyond the normal"
                        + " line width used for Java source code.\";}\n"))
                .isEqualTo("""
                    package x;

                    class X {
                        String x = "This is a deliberately long string that should be reflowed by the Palantir formatter because it extends"
                                + " well beyond the normal line width used for Java source code.";
                    }
                    """);
        assertThat(formatter.format("package x; /** this is documentation that should be formatted consistently by"
                        + " palantir java format. */ class X{}\n"))
                .isEqualTo("""
                    package x;
                    /** this is documentation that should be formatted consistently by palantir java format. */
                    class X {}
                    """);
    }

    @Test
    void writesOnlyWhenContentDiffers(@TempDir final Path tempDir) throws IOException, FormatterException {
        final Path source = tempDir.resolve("X.java");
        Files.writeString(source, "class X{}\n", StandardCharsets.UTF_8);

        assertThat(formatter.formatFile(source)).isTrue();
        assertThat(Files.readString(source, StandardCharsets.UTF_8)).isEqualTo("class X {}\n");
        assertThat(formatter.formatFile(source)).isFalse();
    }
}
