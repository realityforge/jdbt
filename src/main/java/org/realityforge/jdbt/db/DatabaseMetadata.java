package org.realityforge.jdbt.db;

import org.jspecify.annotations.Nullable;

public record DatabaseMetadata(
        String key,
        @Nullable String version,
        @Nullable String schemaHash,
        @Nullable String dataPath,
        @Nullable String logPath,
        boolean forceDrop,
        boolean deleteBackupHistory,
        boolean reindexOnImport,
        boolean shrinkOnImport) {
    public DatabaseMetadata(final String key, final @Nullable String version, final @Nullable String schemaHash) {
        this(key, version, schemaHash, null, null, false, true, true, false);
    }
}
