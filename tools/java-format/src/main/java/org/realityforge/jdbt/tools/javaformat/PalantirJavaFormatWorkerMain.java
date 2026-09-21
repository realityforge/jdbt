package org.realityforge.jdbt.tools.javaformat;

import com.google.devtools.build.lib.worker.WorkerProtocol.WorkRequest;
import com.google.devtools.build.lib.worker.WorkerProtocol.WorkResponse;
import com.palantir.javaformat.java.FormatterException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.jspecify.annotations.Nullable;

public final class PalantirJavaFormatWorkerMain {
    private static final String PERSISTENT_WORKER_ARGUMENT = "--persistent_worker";

    private PalantirJavaFormatWorkerMain() {}

    public static void main(final String[] args) throws IOException {
        final var formatter = new PalantirFormatter();
        if (Arrays.asList(args).contains(PERSISTENT_WORKER_ARGUMENT)) {
            runPersistent(formatter, System.in, System.out);
        } else {
            System.exit(runOnce(formatter, expandArguments(args), System.err));
        }
    }

    static void runPersistent(final PalantirFormatter formatter, final InputStream input, final OutputStream output)
            throws IOException {
        while (true) {
            final @Nullable WorkRequest request = WorkRequest.parseDelimitedFrom(input);
            if (null == request) {
                return;
            }
            final WorkResponse response;
            if (request.getCancel()) {
                response = WorkResponse.newBuilder()
                        .setRequestId(request.getRequestId())
                        .setWasCancelled(true)
                        .build();
            } else {
                final CheckResult result = process(formatter, request.getArgumentsList());
                response = WorkResponse.newBuilder()
                        .setRequestId(request.getRequestId())
                        .setExitCode(result.exitCode())
                        .setOutput(result.output())
                        .build();
            }
            response.writeDelimitedTo(output);
            output.flush();
        }
    }

    static int runOnce(final PalantirFormatter formatter, final List<String> arguments, final PrintStream error) {
        final CheckResult result = process(formatter, arguments);
        if (!result.output().isEmpty()) {
            error.print(result.output());
        }
        return result.exitCode();
    }

    static List<String> expandArguments(final String[] args) throws IOException {
        final List<String> arguments = new ArrayList<>();
        for (int i = 0; i < args.length; i++) {
            final String argument = args[i];
            if (i == args.length - 1 && argument.startsWith("@")) {
                arguments.addAll(Files.readAllLines(Path.of(argument.substring(1)), StandardCharsets.UTF_8));
            } else {
                arguments.add(argument);
            }
        }
        return List.copyOf(arguments);
    }

    private static CheckResult process(final PalantirFormatter formatter, final List<String> arguments) {
        try {
            return check(formatter, CheckRequest.parse(arguments));
        } catch (IOException | FormatterException | IllegalArgumentException e) {
            return new CheckResult(1, errorMessage(e) + "\n");
        }
    }

    private static CheckResult check(final PalantirFormatter formatter, final CheckRequest request)
            throws IOException, FormatterException {
        Files.deleteIfExists(request.marker());
        final List<String> dirty = new ArrayList<>();
        for (final Source source : request.sources()) {
            final String content = Files.readString(source.path(), StandardCharsets.UTF_8);
            if (!content.equals(formatter.format(content))) {
                dirty.add(source.displayPath());
            }
        }
        if (!dirty.isEmpty()) {
            return new CheckResult(
                    1,
                    "Unformatted Java files:\n  " + String.join("\n  ", dirty) + "\nRun tools/java_format.sh write\n");
        }
        final @Nullable Path parent = request.marker().getParent();
        if (null != parent) {
            Files.createDirectories(parent);
        }
        Files.writeString(request.marker(), "", StandardCharsets.UTF_8);
        return new CheckResult(0, "");
    }

    private static String errorMessage(final Exception exception) {
        final @Nullable String message = exception.getMessage();
        return null == message ? exception.getClass().getSimpleName() : message;
    }

    private record CheckResult(int exitCode, String output) {}

    private record CheckRequest(Path marker, List<Source> sources) {
        private static CheckRequest parse(final List<String> arguments) {
            @Nullable String mode = null;
            @Nullable Path marker = null;
            final List<Source> sources = new ArrayList<>();
            for (final String argument : arguments) {
                if (argument.startsWith("--mode=")) {
                    mode = argument.substring("--mode=".length());
                } else if (argument.startsWith("--marker=")) {
                    marker = Path.of(argument.substring("--marker=".length()));
                } else if (argument.startsWith("--")) {
                    throw new IllegalArgumentException("Unknown worker argument: " + argument);
                } else {
                    sources.add(new Source(Path.of(argument), argument));
                }
            }
            if (!"check".equals(mode)) {
                throw new IllegalArgumentException("Worker requires --mode=check");
            }
            if (null == marker) {
                throw new IllegalArgumentException("Worker requires --marker=<path>");
            }
            return new CheckRequest(marker, List.copyOf(sources));
        }
    }

    private record Source(Path path, String displayPath) {}
}
