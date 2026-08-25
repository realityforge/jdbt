package org.realityforge.jdbt.db;

@FunctionalInterface
public interface SqlTimingObserver {
    void observe(SqlTimingObservation observation);
}
