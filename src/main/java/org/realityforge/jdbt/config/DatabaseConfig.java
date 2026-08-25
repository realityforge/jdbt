package org.realityforge.jdbt.config;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.jspecify.annotations.Nullable;

public record DatabaseConfig(
        List<String> upDirs,
        List<String> downDirs,
        List<String> finalizeDirs,
        List<String> preCreateDirs,
        List<String> postCreateDirs,
        List<String> datasets,
        String datasetsDirName,
        List<String> preDatasetDirs,
        List<String> postDatasetDirs,
        String fixtureDirName,
        boolean migrations,
        boolean migrationsAppliedAtCreate,
        String migrationsDirName,
        @Nullable String version,
        @Nullable String dataPath,
        @Nullable String logPath,
        boolean forceDrop,
        boolean deleteBackupHistory,
        boolean reindexOnImport,
        boolean shrinkOnImport,
        Map<String, FilterPropertyConfig> filterProperties,
        Map<String, ImportConfig> imports,
        Map<String, ModuleGroupConfig> moduleGroups) {

    public DatabaseConfig {
        upDirs = List.copyOf(upDirs);
        downDirs = List.copyOf(downDirs);
        finalizeDirs = List.copyOf(finalizeDirs);
        preCreateDirs = List.copyOf(preCreateDirs);
        postCreateDirs = List.copyOf(postCreateDirs);
        datasets = List.copyOf(datasets);
        preDatasetDirs = List.copyOf(preDatasetDirs);
        postDatasetDirs = List.copyOf(postDatasetDirs);
        filterProperties = Collections.unmodifiableMap(new LinkedHashMap<>(filterProperties));
        imports = Collections.unmodifiableMap(new LinkedHashMap<>(imports));
        moduleGroups = Collections.unmodifiableMap(new LinkedHashMap<>(moduleGroups));
    }
}
