package org.realityforge.jdbt.db;

@FunctionalInterface
public interface SqlTimingObserver {
    SqlTimingObserver NONE = observation -> {};

    void observe(SqlTimingObservation observation);
}
