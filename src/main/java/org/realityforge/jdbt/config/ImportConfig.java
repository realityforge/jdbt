package org.realityforge.jdbt.config;

import java.util.List;

public record ImportConfig(
        String key, List<String> modules, String dir, List<String> preImportDirs, List<String> postImportDirs) {
    public ImportConfig {
        modules = List.copyOf(modules);
        preImportDirs = List.copyOf(preImportDirs);
        postImportDirs = List.copyOf(postImportDirs);
    }
}
