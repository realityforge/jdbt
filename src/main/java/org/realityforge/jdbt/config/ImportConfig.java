package org.realityforge.jdbt.config;

import java.util.List;
import org.jspecify.annotations.Nullable;

public record ImportConfig(
        String key,
        List<String> modules,
        String dir,
        List<String> preImportDirs,
        List<String> postImportDirs,
        List<String> preLateImportDirs,
        @Nullable String lateImportDir,
        List<String> requiredFiles) {
    public ImportConfig {
        modules = List.copyOf(modules);
        preImportDirs = List.copyOf(preImportDirs);
        postImportDirs = List.copyOf(postImportDirs);
        preLateImportDirs = List.copyOf(preLateImportDirs);
        requiredFiles = List.copyOf(requiredFiles);
    }
}
