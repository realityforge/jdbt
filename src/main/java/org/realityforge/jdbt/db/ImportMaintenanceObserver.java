package org.realityforge.jdbt.db;

import org.jspecify.annotations.Nullable;

@FunctionalInterface
public interface ImportMaintenanceObserver {
    void run(String operation, @Nullable String subject, Runnable action);
}
