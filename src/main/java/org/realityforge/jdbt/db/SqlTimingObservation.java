package org.realityforge.jdbt.db;

import org.jspecify.annotations.Nullable;

public record SqlTimingObservation(
        long ordinal,
        String operationId,
        @Nullable String parentOperationId,
        String kind,
        String status,
        long elapsedMicroseconds) {}
