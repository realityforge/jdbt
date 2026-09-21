package org.realityforge.jdbt.tools.javaformat;

import static org.assertj.core.api.Assertions.assertThat;

import com.google.devtools.build.lib.worker.WorkerProtocol.WorkRequest;
import com.google.devtools.build.lib.worker.WorkerProtocol.WorkResponse;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

final class PalantirJavaFormatWorkerMainTest {
    @Test
    void processesRequestsAndPreservesRequestIds(@TempDir final Path tempDir) throws IOException {
        final Path clean = write(tempDir.resolve("Clean.java"), "class Clean {}\n");
        final Path dirty = write(tempDir.resolve("Dirty.java"), "class Dirty{}\n");
        final Path cleanMarker = tempDir.resolve("clean.marker");
        final Path dirtyMarker = tempDir.resolve("dirty.marker");
        final var requests = new ByteArrayOutputStream();
        request(11, cleanMarker, clean).writeDelimitedTo(requests);
        request(12, dirtyMarker, dirty).writeDelimitedTo(requests);
        WorkRequest.newBuilder().setRequestId(13).setCancel(true).build().writeDelimitedTo(requests);
        final var responses = new ByteArrayOutputStream();

        PalantirJavaFormatWorkerMain.runPersistent(
                new PalantirFormatter(), new ByteArrayInputStream(requests.toByteArray()), responses);

        final var responseInput = new ByteArrayInputStream(responses.toByteArray());
        final WorkResponse cleanResponse = WorkResponse.parseDelimitedFrom(responseInput);
        final WorkResponse dirtyResponse = WorkResponse.parseDelimitedFrom(responseInput);
        final WorkResponse cancelResponse = WorkResponse.parseDelimitedFrom(responseInput);
        assertThat(cleanResponse.getRequestId()).isEqualTo(11);
        assertThat(cleanResponse.getExitCode()).isZero();
        assertThat(cleanResponse.getOutput()).isEmpty();
        assertThat(cleanMarker).exists();
        assertThat(dirtyResponse.getRequestId()).isEqualTo(12);
        assertThat(dirtyResponse.getExitCode()).isEqualTo(1);
        assertThat(dirtyResponse.getOutput()).contains(dirty.toString(), "Run tools/java_format.sh write");
        assertThat(dirtyMarker).doesNotExist();
        assertThat(cancelResponse.getRequestId()).isEqualTo(13);
        assertThat(cancelResponse.getWasCancelled()).isTrue();
        assertThat(responseInput.read()).isEqualTo(-1);
    }

    @Test
    void expandsParameterFileForOrdinaryExecution(@TempDir final Path tempDir) throws IOException {
        final Path clean = write(tempDir.resolve("Clean.java"), "class Clean {}\n");
        final Path marker = tempDir.resolve("clean.marker");
        final Path arguments = tempDir.resolve("worker.params");
        Files.write(arguments, List.of("--mode=check", "--marker=" + marker, clean.toString()), StandardCharsets.UTF_8);
        final var errorBytes = new ByteArrayOutputStream();

        final int exitCode = PalantirJavaFormatWorkerMain.runOnce(
                new PalantirFormatter(),
                PalantirJavaFormatWorkerMain.expandArguments(new String[] {"@" + arguments}),
                new PrintStream(errorBytes, true, StandardCharsets.UTF_8));

        assertThat(exitCode).isZero();
        assertThat(errorBytes.toString(StandardCharsets.UTF_8)).isEmpty();
        assertThat(marker).exists();
    }

    @Test
    void reportsInvalidRequestsWithoutWritingMarker(@TempDir final Path tempDir) {
        final Path marker = tempDir.resolve("invalid.marker");
        final var errorBytes = new ByteArrayOutputStream();

        final int exitCode = PalantirJavaFormatWorkerMain.runOnce(
                new PalantirFormatter(),
                List.of("--mode=write", "--marker=" + marker),
                new PrintStream(errorBytes, true, StandardCharsets.UTF_8));

        assertThat(exitCode).isEqualTo(1);
        assertThat(errorBytes.toString(StandardCharsets.UTF_8)).contains("--mode=check");
        assertThat(marker).doesNotExist();
    }

    private static WorkRequest request(final int requestId, final Path marker, final Path source) {
        return WorkRequest.newBuilder()
                .setRequestId(requestId)
                .addArguments("--mode=check")
                .addArguments("--marker=" + marker)
                .addArguments(source.toString())
                .build();
    }

    private static Path write(final Path path, final String content) throws IOException {
        Files.writeString(path, content, StandardCharsets.UTF_8);
        return path;
    }
}
