package org.realityforge.jdbt.db;

import org.jspecify.annotations.Nullable;

@FunctionalInterface
public interface ImportMaintenanceObserver {
    ImportMaintenanceObserver DIRECT = (operation, subject, action) -> action.run();

    void run(String operation, @Nullable String subject, Runnable action);
}
