package org.realityforge.jdbt.tools.javaformat;

import com.palantir.javaformat.java.FormatterException;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import org.jspecify.annotations.Nullable;

public final class PalantirJavaFormatMain {
    private static final List<String> SOURCE_ROOTS = List.of("src", "tools");

    private PalantirJavaFormatMain() {}

    public static void main(final String[] args) throws IOException, FormatterException {
        System.exit(run(args, System.getenv()));
    }

    static int run(final String[] args, final Map<String, String> environment) throws IOException, FormatterException {
        if (1 != args.length || !"--write".equals(args[0])) {
            System.err.println("usage: java_format --write");
            return 2;
        }
        final @Nullable String workspace = environment.get("BUILD_WORKSPACE_DIRECTORY");
        if (null == workspace || workspace.isBlank()) {
            System.err.println("BUILD_WORKSPACE_DIRECTORY is not set");
            return 2;
        }
        formatWorkspace(Path.of(workspace), new PalantirFormatter());
        return 0;
    }

    static int formatWorkspace(final Path workspace, final PalantirFormatter formatter)
            throws IOException, FormatterException {
        int changed = 0;
        for (final Path source : discoverSources(workspace)) {
            if (formatter.formatFile(source)) {
                changed++;
            }
        }
        return changed;
    }

    static List<Path> discoverSources(final Path workspace) throws IOException {
        final Path normalizedWorkspace = workspace.toAbsolutePath().normalize();
        if (!Files.isDirectory(normalizedWorkspace, LinkOption.NOFOLLOW_LINKS)) {
            throw new IllegalArgumentException("Workspace is not a directory: " + normalizedWorkspace);
        }
        final List<Path> sources = new ArrayList<>();
        for (final String rootName : SOURCE_ROOTS) {
            final Path sourceRoot = normalizedWorkspace.resolve(rootName);
            if (!Files.isDirectory(sourceRoot, LinkOption.NOFOLLOW_LINKS)) {
                continue;
            }
            try (Stream<Path> paths = Files.walk(sourceRoot)) {
                paths.filter(path -> Files.isRegularFile(path, LinkOption.NOFOLLOW_LINKS))
                        .filter(path -> path.getFileName().toString().endsWith(".java"))
                        .forEach(sources::add);
            }
        }
        sources.sort(Comparator.comparing(Path::toString));
        return List.copyOf(sources);
    }
}
