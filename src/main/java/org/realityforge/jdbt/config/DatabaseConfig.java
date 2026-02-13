package org.realityforge.jdbt.config;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.jspecify.annotations.Nullable;

public record DatabaseConfig(
        String key,
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
        List<String> preDbArtifacts,
        List<String> postDbArtifacts,
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
        preDbArtifacts = List.copyOf(preDbArtifacts);
        postDbArtifacts = List.copyOf(postDbArtifacts);
        filterProperties = Collections.unmodifiableMap(new LinkedHashMap<>(filterProperties));
        imports = Map.copyOf(imports);
        moduleGroups = Map.copyOf(moduleGroups);
    }

    public DatabaseConfig(
            final String key,
            final List<String> upDirs,
            final List<String> downDirs,
            final List<String> finalizeDirs,
            final List<String> preCreateDirs,
            final List<String> postCreateDirs,
            final List<String> datasets,
            final String datasetsDirName,
            final List<String> preDatasetDirs,
            final List<String> postDatasetDirs,
            final String fixtureDirName,
            final boolean migrations,
            final boolean migrationsAppliedAtCreate,
            final String migrationsDirName,
            final @Nullable String version,
            final List<String> preDbArtifacts,
            final List<String> postDbArtifacts,
            final Map<String, ImportConfig> imports,
            final Map<String, ModuleGroupConfig> moduleGroups) {
        this(
                key,
                upDirs,
                downDirs,
                finalizeDirs,
                preCreateDirs,
                postCreateDirs,
                datasets,
                datasetsDirName,
                preDatasetDirs,
                postDatasetDirs,
                fixtureDirName,
                migrations,
                migrationsAppliedAtCreate,
                migrationsDirName,
                version,
                preDbArtifacts,
                postDbArtifacts,
                Map.of(),
                imports,
                moduleGroups);
    }
}
