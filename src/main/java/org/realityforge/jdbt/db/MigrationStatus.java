package org.realityforge.jdbt.db;

import org.jspecify.annotations.Nullable;

public record MigrationStatus(boolean initialized, @Nullable String databaseVersion) {}
