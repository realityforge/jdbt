package org.realityforge.jdbt.runtime;

final class SqlServerAssertExpander {
    static String expandImportSql(final String sql) {
        var output = replaceArgumentMacro(
                sql, "ASSERT_DATABASE_VERSION", SqlServerAssertExpander::importDatabaseVersionAssertion);
        output = replaceNoArgumentMacro(
                output, "ASSERT_UNCHANGED_ROW_COUNT", SqlServerAssertExpander::unchangedRowCountAssertion);
        return replaceArgumentMacro(output, "ASSERT_ROW_COUNT", SqlServerAssertExpander::rowCountAssertion);
    }

    static String expandCreationSql(final String sql) {
        return replaceArgumentMacro(
                sql, "ASSERT_DATABASE_VERSION", SqlServerAssertExpander::creationDatabaseVersionAssertion);
    }

    @SuppressWarnings("SameParameterValue")
    private static String replaceNoArgumentMacro(
            final String sql, final String macroName, final MacroWithoutArgumentsReplacement replacement) {
        final var token = macroName + '(';
        final var output = new StringBuilder();
        int cursor = 0;
        while (cursor < sql.length()) {
            final int start = sql.indexOf(token, cursor);
            if (-1 == start) {
                output.append(sql, cursor, sql.length());
                break;
            }
            output.append(sql, cursor, start);
            final int end = findMacroEnd(sql, start + token.length());
            final var argument = sql.substring(start + token.length(), end).trim();
            if (!argument.isEmpty()) {
                output.append(sql, start, end + 1);
            } else {
                output.append(replacement.replacementSql());
            }
            cursor = end + 1;
        }
        return output.toString();
    }

    private static String replaceArgumentMacro(
            final String sql, final String macroName, final MacroWithArgumentReplacement replacement) {
        final var token = macroName + '(';
        final var output = new StringBuilder();
        int cursor = 0;
        while (cursor < sql.length()) {
            final int start = sql.indexOf(token, cursor);
            if (-1 == start) {
                output.append(sql, cursor, sql.length());
                break;
            }
            output.append(sql, cursor, start);
            final int end = findMacroEnd(sql, start + token.length());
            final var argument = sql.substring(start + token.length(), end).trim();
            output.append(replacement.replacementSql(argument));
            cursor = end + 1;
        }
        return output.toString();
    }

    private static int findMacroEnd(final String sql, final int index) {
        int depth = 1;
        for (int i = index; i < sql.length(); i++) {
            final char c = sql.charAt(i);
            if ('(' == c) {
                depth++;
            } else if (')' == c) {
                depth--;
                if (0 == depth) {
                    return i;
                }
            }
        }
        throw new RuntimeExecutionException("Unterminated assert expression in SQL: " + sql);
    }

    private static String importDatabaseVersionAssertion(final String expectedVersionExpression) {
        final var escapedVersionForErrorMessage = expectedVersionExpression.replace("'", "''");
        return """
            GO
            BEGIN
              DECLARE @DbVersion VARCHAR(MAX)
              SET @DbVersion = ''
              SELECT @DbVersion = COALESCE(CONVERT(VARCHAR(MAX),value),'')
                FROM [__SOURCE__].sys.fn_listextendedproperty('DatabaseSchemaVersion', default, default, default, default, default, default)
              IF (@DbVersion IS NULL OR @DbVersion = %s)
              BEGIN
                DECLARE @Message VARCHAR(MAX)
                SET @Message = CONCAT('Expected DatabaseSchemaVersion in __SOURCE__ database not to be %s. Actual Value: ', @DbVersion)
                RAISERROR (@Message, 16, 1) WITH SETERROR
              END
            END
            """.formatted(expectedVersionExpression, escapedVersionForErrorMessage)
                + expectedDatabaseVersionAssertion(
                        expectedVersionExpression, escapedVersionForErrorMessage, "[__TARGET__].", "__TARGET__");
    }

    private static String creationDatabaseVersionAssertion(final String expectedVersionExpression) {
        return expectedDatabaseVersionAssertion(
                expectedVersionExpression, expectedVersionExpression.replace("'", "''"), "", "current");
    }

    private static String expectedDatabaseVersionAssertion(
            final String expectedVersionExpression,
            final String escapedVersionForErrorMessage,
            final String databaseReference,
            final String databaseLabel) {
        return """
            GO
            BEGIN
              DECLARE @DbVersion VARCHAR(MAX)
              SET @DbVersion = ''
              SELECT @DbVersion = COALESCE(CONVERT(VARCHAR(MAX),value),'')
                FROM %ssys.fn_listextendedproperty('DatabaseSchemaVersion', default, default, default, default, default, default)
              IF (@DbVersion IS NULL OR @DbVersion != %s)
              BEGIN
                DECLARE @Message VARCHAR(MAX)
                SET @Message = CONCAT('Expected DatabaseSchemaVersion in %s database to be %s. Actual Value: ', @DbVersion)
                RAISERROR (@Message, 16, 1) WITH SETERROR
              END
            END
            GO
            """.formatted(
                        databaseReference, expectedVersionExpression, databaseLabel, escapedVersionForErrorMessage);
    }

    private static String unchangedRowCountAssertion() {
        return """
            GO
            IF (SELECT COUNT(*) FROM [__TARGET__].__TABLE__) != (SELECT COUNT(*) FROM [__SOURCE__].__TABLE__)
            BEGIN
              RAISERROR ('Actual row count for __TABLE__ does not match expected rowcount', 16, 1) WITH SETERROR
            END
            """;
    }

    private static String rowCountAssertion(final String expectedRowCountExpression) {
        return """
            GO
            IF (SELECT COUNT(*) FROM [__TARGET__].__TABLE__) != (%s)
            BEGIN
              RAISERROR ('Actual row count for __TABLE__ does not match expected rowcount', 16, 1) WITH SETERROR
            END
            """.formatted(expectedRowCountExpression);
    }

    private interface MacroWithArgumentReplacement {
        String replacementSql(String argument);
    }

    private interface MacroWithoutArgumentsReplacement {
        String replacementSql();
    }

    private SqlServerAssertExpander() {}
}
