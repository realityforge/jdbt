package org.realityforge.jdbt.tools.javaformat;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Map;
import org.jspecify.annotations.Nullable;

public final class PalantirJavaFormatWatchMain {
    private PalantirJavaFormatWatchMain() {}

    public static void main(final String[] args) throws IOException, InterruptedException {
        System.exit(run(args, System.getenv()));
    }

    static int run(final String[] args, final Map<String, String> environment)
            throws IOException, InterruptedException {
        if (0 != args.length) {
            System.err.println("usage: java_format_watch");
            return 2;
        }
        final @Nullable String workspace = environment.get("BUILD_WORKSPACE_DIRECTORY");
        if (null == workspace || workspace.isBlank()) {
            System.err.println("BUILD_WORKSPACE_DIRECTORY is not set");
            return 2;
        }
        try (var watcher =
                new PalantirJavaFormatWatcher(Path.of(workspace), new PalantirFormatter(), System.out, System.err)) {
            System.out.println("Watching Java sources under src/ and tools/");
            watcher.watch();
        }
        return 0;
    }
}
