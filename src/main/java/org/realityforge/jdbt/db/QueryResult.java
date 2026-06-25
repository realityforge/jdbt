package org.realityforge.jdbt.db;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public record QueryResult(List<String> columnLabels, List<List<Object>> rows) {
    public QueryResult {
        columnLabels = List.copyOf(columnLabels);
        rows = rows.stream()
                .map(row -> Collections.unmodifiableList(new ArrayList<>(row)))
                .toList();
        for (final var row : rows) {
            if (row.size() != columnLabels.size()) {
                throw new DatabaseException(
                        "Query row has " + row.size() + " values but " + columnLabels.size() + " columns");
            }
        }
    }
}
