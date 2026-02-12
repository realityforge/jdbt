package org.realityforge.jdbt.config;

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
        @Nullable String resourcePrefix,
        List<String> preDbArtifacts,
        List<String> postDbArtifacts,
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
        imports = Map.copyOf(imports);
        moduleGroups = Map.copyOf(moduleGroups);
    }
}
