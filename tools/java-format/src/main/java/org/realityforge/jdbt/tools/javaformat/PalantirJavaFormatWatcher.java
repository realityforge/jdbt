package org.realityforge.jdbt.tools.javaformat;

import com.palantir.javaformat.java.FormatterException;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.file.ClosedWatchServiceException;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.StandardWatchEventKinds;
import java.nio.file.WatchEvent;
import java.nio.file.WatchKey;
import java.nio.file.WatchService;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;
import org.jspecify.annotations.Nullable;

final class PalantirJavaFormatWatcher implements AutoCloseable {
    private static final long DEBOUNCE_MILLISECONDS = 100;
    private final Path workspace;
    private final List<Path> sourceRoots;
    private final PalantirFormatter formatter;
    private final PrintStream output;
    private final PrintStream error;
    private final WatchService watchService;
    private final Map<WatchKey, Path> watchedDirectories = new HashMap<>();

    PalantirJavaFormatWatcher(
            final Path workspace, final PalantirFormatter formatter, final PrintStream output, final PrintStream error)
            throws IOException {
        this.workspace = workspace.toRealPath();
        this.sourceRoots = List.of(this.workspace.resolve("src"), this.workspace.resolve("tools"));
        this.formatter = formatter;
        this.output = output;
        this.error = error;
        this.watchService = FileSystems.getDefault().newWatchService();
        for (final Path sourceRoot : sourceRoots) {
            if (Files.isDirectory(sourceRoot, LinkOption.NOFOLLOW_LINKS)) {
                if (Files.isSymbolicLink(sourceRoot)) {
                    rejected(sourceRoot);
                } else {
                    registerRecursively(sourceRoot);
                }
            }
        }
    }

    void watch() throws IOException, InterruptedException {
        while (true) {
            final WatchKey firstKey;
            try {
                firstKey = watchService.take();
            } catch (ClosedWatchServiceException ignored) {
                return;
            }
            final var batch = new EventBatch();
            collect(firstKey, batch);
            WatchKey nextKey;
            while (null != (nextKey = watchService.poll(DEBOUNCE_MILLISECONDS, TimeUnit.MILLISECONDS))) {
                collect(nextKey, batch);
            }
            if (batch.overflow()) {
                recoverFromOverflow();
            } else {
                final List<Path> paths = new ArrayList<>(batch.paths());
                paths.sort(Comparator.comparing(Path::toString));
                for (final Path path : paths) {
                    formatChangedPath(path);
                }
            }
        }
    }

    boolean formatChangedPath(final Path candidate) {
        try {
            final @Nullable Path admitted = admit(candidate);
            if (null == admitted) {
                return false;
            }
            final boolean changed = formatter.formatFile(admitted);
            if (changed) {
                output.println("Formatted " + workspace.relativize(admitted));
            }
            return changed;
        } catch (IOException | FormatterException e) {
            error.println("Unable to format " + display(candidate) + ": " + errorMessage(e));
            return false;
        }
    }

    void recoverFromOverflow() {
        error.println("Java format watch overflow; rescanning src/ and tools/");
        try {
            for (final Path source : PalantirJavaFormatMain.discoverSources(workspace)) {
                formatChangedPath(source);
            }
        } catch (IOException e) {
            error.println("Unable to rescan Java sources: " + errorMessage(e));
        }
    }

    @Override
    public void close() throws IOException {
        watchService.close();
    }

    private void collect(final WatchKey key, final EventBatch batch) throws IOException {
        final @Nullable Path directory = watchedDirectories.get(key);
        if (null == directory) {
            key.cancel();
            return;
        }
        for (final WatchEvent<?> event : key.pollEvents()) {
            if (StandardWatchEventKinds.OVERFLOW == event.kind()) {
                batch.markOverflow();
                continue;
            }
            final Path path = directory.resolve((Path) event.context()).normalize();
            if (StandardWatchEventKinds.ENTRY_CREATE == event.kind()
                    && Files.isDirectory(path, LinkOption.NOFOLLOW_LINKS)) {
                if (Files.isSymbolicLink(path) || hasSymbolicLinkComponent(path)) {
                    rejected(path);
                } else {
                    try {
                        registerRecursively(path);
                        addJavaFiles(path, batch.paths());
                    } catch (IOException e) {
                        error.println(
                                "Unable to register Java format watch path " + display(path) + ": " + errorMessage(e));
                    }
                }
            } else if (path.getFileName().toString().endsWith(".java")) {
                batch.paths().add(path);
            }
        }
        if (!key.reset()) {
            watchedDirectories.remove(key);
        }
    }

    private void registerRecursively(final Path root) throws IOException {
        try (Stream<Path> paths = Files.walk(root)) {
            for (final Path directory : paths.filter(path -> Files.isDirectory(path, LinkOption.NOFOLLOW_LINKS))
                    .sorted()
                    .toList()) {
                if (!hasSymbolicLinkComponent(directory)) {
                    final WatchKey key = directory.register(
                            watchService, StandardWatchEventKinds.ENTRY_CREATE, StandardWatchEventKinds.ENTRY_MODIFY);
                    watchedDirectories.put(key, directory);
                }
            }
        }
    }

    private static void addJavaFiles(final Path root, final Set<Path> paths) throws IOException {
        try (Stream<Path> descendants = Files.walk(root)) {
            descendants
                    .filter(path -> Files.isRegularFile(path, LinkOption.NOFOLLOW_LINKS))
                    .filter(path -> path.getFileName().toString().endsWith(".java"))
                    .forEach(paths::add);
        }
    }

    private @Nullable Path admit(final Path candidate) throws IOException {
        final Path normalized = candidate.toAbsolutePath().normalize();
        if (!Files.exists(normalized, LinkOption.NOFOLLOW_LINKS)) {
            return null;
        }
        if (!normalized.startsWith(workspace)
                || !withinSourceRoot(normalized)
                || !normalized.getFileName().toString().endsWith(".java")
                || !Files.isRegularFile(normalized, LinkOption.NOFOLLOW_LINKS)
                || hasSymbolicLinkComponent(normalized)) {
            rejected(normalized);
            return null;
        }
        final Path realPath = normalized.toRealPath();
        if (!realPath.startsWith(workspace) || !withinSourceRoot(realPath)) {
            rejected(normalized);
            return null;
        }
        return realPath;
    }

    private boolean withinSourceRoot(final Path path) {
        return sourceRoots.stream().anyMatch(path::startsWith);
    }

    private boolean hasSymbolicLinkComponent(final Path path) {
        if (!path.startsWith(workspace)) {
            return true;
        }
        Path current = workspace;
        for (final Path component : workspace.relativize(path)) {
            current = current.resolve(component);
            if (Files.isSymbolicLink(current)) {
                return true;
            }
        }
        return false;
    }

    private void rejected(final Path path) {
        error.println("Rejected Java format path: " + display(path));
    }

    private String display(final Path path) {
        final Path normalized = path.toAbsolutePath().normalize();
        return normalized.startsWith(workspace)
                ? workspace.relativize(normalized).toString()
                : normalized.toString();
    }

    private static String errorMessage(final Exception exception) {
        final @Nullable String message = exception.getMessage();
        return null == message ? exception.getClass().getSimpleName() : message;
    }

    private static final class EventBatch {
        private final Set<Path> paths = new LinkedHashSet<>();
        private boolean overflow;

        Set<Path> paths() {
            return paths;
        }

        boolean overflow() {
            return overflow;
        }

        void markOverflow() {
            overflow = true;
        }
    }
}
