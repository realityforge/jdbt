package org.realityforge.jdbt.config;

import java.util.List;
import org.jspecify.annotations.Nullable;

public record DefaultsConfig(
        List<String> searchDirs,
        List<String> upDirs,
        List<String> downDirs,
        List<String> finalizeDirs,
        List<String> preCreateDirs,
        List<String> postCreateDirs,
        List<String> preImportDirs,
        List<String> postImportDirs,
        String importDir,
        String datasetsDirName,
        List<String> preDatasetDirs,
        List<String> postDatasetDirs,
        String fixtureDirName,
        String migrationsDirName,
        String indexFileName,
        String defaultDatabase,
        String defaultImport) {

    public DefaultsConfig {
        searchDirs = List.copyOf(searchDirs);
        upDirs = List.copyOf(upDirs);
        downDirs = List.copyOf(downDirs);
        finalizeDirs = List.copyOf(finalizeDirs);
        preCreateDirs = List.copyOf(preCreateDirs);
        postCreateDirs = List.copyOf(postCreateDirs);
        preImportDirs = List.copyOf(preImportDirs);
        postImportDirs = List.copyOf(postImportDirs);
        preDatasetDirs = List.copyOf(preDatasetDirs);
        postDatasetDirs = List.copyOf(postDatasetDirs);
    }

    public static DefaultsConfig rubyCompatibleDefaults() {
        return new DefaultsConfig(
                List.of(),
                List.of(".", "types", "views", "functions", "stored-procedures", "misc"),
                List.of("down"),
                List.of("triggers", "finalize"),
                List.of("db-hooks/pre"),
                List.of("db-hooks/post"),
                List.of("import-hooks/pre"),
                List.of("import-hooks/post"),
                "import",
                "datasets",
                List.of("pre"),
                List.of("post"),
                "fixtures",
                "migrations",
                "index.txt",
                "default",
                "default");
    }

    public DefaultsConfig merge(
            final @Nullable List<String> searchDirs,
            final @Nullable List<String> upDirs,
            final @Nullable List<String> downDirs,
            final @Nullable List<String> finalizeDirs,
            final @Nullable List<String> preCreateDirs,
            final @Nullable List<String> postCreateDirs,
            final @Nullable List<String> preImportDirs,
            final @Nullable List<String> postImportDirs,
            final @Nullable String importDir,
            final @Nullable String datasetsDirName,
            final @Nullable List<String> preDatasetDirs,
            final @Nullable List<String> postDatasetDirs,
            final @Nullable String fixtureDirName,
            final @Nullable String migrationsDirName,
            final @Nullable String indexFileName,
            final @Nullable String defaultDatabase,
            final @Nullable String defaultImport) {
        return new DefaultsConfig(
                searchDirs == null ? this.searchDirs : searchDirs,
                upDirs == null ? this.upDirs : upDirs,
                downDirs == null ? this.downDirs : downDirs,
                finalizeDirs == null ? this.finalizeDirs : finalizeDirs,
                preCreateDirs == null ? this.preCreateDirs : preCreateDirs,
                postCreateDirs == null ? this.postCreateDirs : postCreateDirs,
                preImportDirs == null ? this.preImportDirs : preImportDirs,
                postImportDirs == null ? this.postImportDirs : postImportDirs,
                importDir == null ? this.importDir : importDir,
                datasetsDirName == null ? this.datasetsDirName : datasetsDirName,
                preDatasetDirs == null ? this.preDatasetDirs : preDatasetDirs,
                postDatasetDirs == null ? this.postDatasetDirs : postDatasetDirs,
                fixtureDirName == null ? this.fixtureDirName : fixtureDirName,
                migrationsDirName == null ? this.migrationsDirName : migrationsDirName,
                indexFileName == null ? this.indexFileName : indexFileName,
                defaultDatabase == null ? this.defaultDatabase : defaultDatabase,
                defaultImport == null ? this.defaultImport : defaultImport);
    }
}
