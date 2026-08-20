package org.realityforge.jdbt.repository;

import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.regex.Pattern;
import org.realityforge.jdbt.config.ConfigException;

public record RepositoryTable(String name, List<String> columns, List<String> indexes, RowSource rowSource) {
    private static final String BRACKETED_IDENTIFIER = "\\[(?:[^\\]]|\\]\\])+\\]";
    private static final String QUOTED_IDENTIFIER = "\"(?:[^\"]|\"\")+\"";
    private static final String UNQUOTED_IDENTIFIER = "[A-Za-z_][A-Za-z0-9_$]*";
    private static final String SQL_IDENTIFIER =
            "(?:" + BRACKETED_IDENTIFIER + '|' + QUOTED_IDENTIFIER + '|' + UNQUOTED_IDENTIFIER + ')';
    private static final Pattern QUALIFIED_NAME = Pattern.compile("(?:" + SQL_IDENTIFIER + "\\.)+" + SQL_IDENTIFIER);
    private static final Pattern QUOTED_COLUMN =
            Pattern.compile("(?:" + BRACKETED_IDENTIFIER + '|' + QUOTED_IDENTIFIER + ')');

    public RepositoryTable(final String name, final List<String> columns) {
        this(name, columns, List.of(), RowSource.IMPORT);
    }

    public RepositoryTable(final String name, final List<String> columns, final List<String> indexes) {
        this(name, columns, indexes, RowSource.IMPORT);
    }

    public RepositoryTable(final String name, final List<String> columns, final RowSource rowSource) {
        this(name, columns, List.of(), rowSource);
    }

    public RepositoryTable {
        if (name.isBlank()) {
            throw new ConfigException("Repository table name must not be blank.");
        }
        if (!QUALIFIED_NAME.matcher(name).matches()) {
            throw new ConfigException("Repository table name '" + name + "' must be a qualified SQL name.");
        }
        columns = List.copyOf(columns);
        if (columns.isEmpty()) {
            throw new ConfigException("Repository table '" + name + "' must define at least one column.");
        }
        final var uniqueColumns = new HashSet<String>();
        for (final var column : columns) {
            if (column.isBlank()) {
                throw new ConfigException("Repository table '" + name + "' must not contain a blank column.");
            }
            if (!QUOTED_COLUMN.matcher(column).matches()) {
                throw new ConfigException(
                        "Repository table '" + name + "' column '" + column + "' must be a quoted SQL identifier.");
            }
            if (!uniqueColumns.add(column)) {
                throw new ConfigException(
                        "Repository table '" + name + "' contains duplicate column '" + column + "'.");
            }
        }
        indexes = List.copyOf(indexes);
        final var uniqueIndexes = new HashSet<String>();
        for (final var index : indexes) {
            if (index.isBlank()) {
                throw new ConfigException("Repository table '" + name + "' must not contain a blank index.");
            }
            if (!QUOTED_COLUMN.matcher(index).matches()) {
                throw new ConfigException(
                        "Repository table '" + name + "' index '" + index + "' must be a quoted SQL identifier.");
            }
            if (!uniqueIndexes.add(unquote(index))) {
                throw new ConfigException("Repository table '" + name + "' contains duplicate index '" + index + "'.");
            }
        }
        Objects.requireNonNull(rowSource);
    }

    private static String unquote(final String identifier) {
        if (identifier.startsWith("[")) {
            return identifier.substring(1, identifier.length() - 1).replace("]]", "]");
        }
        return identifier.substring(1, identifier.length() - 1).replace("\"\"", "\"");
    }
}
