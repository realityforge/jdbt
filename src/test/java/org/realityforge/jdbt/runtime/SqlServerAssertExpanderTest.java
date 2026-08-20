package org.realityforge.jdbt.runtime;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.Test;

final class SqlServerAssertExpanderTest {
    @Test
    void expandsAssertRowCountWithNestedExpression() {
        final var expanded = SqlServerAssertExpander.expandImportSql("ASSERT_ROW_COUNT(SELECT COUNT(*) FROM Foo)");

        assertThat(expanded)
                .contains("IF (SELECT COUNT(*) FROM [__TARGET__].__TABLE__) != (SELECT COUNT(*) FROM Foo)")
                .contains("RAISERROR ('Actual row count for __TABLE__ does not match expected rowcount'");
    }

    @Test
    void expandsAssertDatabaseVersion() {
        final var expanded = SqlServerAssertExpander.expandImportSql("ASSERT_DATABASE_VERSION('Version_2')");

        assertThat(expanded)
                .contains("[__SOURCE__].sys.fn_listextendedproperty")
                .contains("[__TARGET__].sys.fn_listextendedproperty")
                .contains("@DbVersion = 'Version_2'")
                .contains("@DbVersion != 'Version_2'")
                .doesNotContain("GO\nGO");
    }

    @Test
    void expandsAssertUnchangedRowCount() {
        final var expanded = SqlServerAssertExpander.expandImportSql("ASSERT_UNCHANGED_ROW_COUNT()");

        assertThat(expanded)
                .contains("COUNT(*) FROM [__TARGET__].__TABLE__) != (SELECT COUNT(*) FROM [__SOURCE__].__TABLE__)");
    }

    @Test
    void leavesAssertUnchangedRowCountWithArgumentsUnchanged() {
        final var expanded = SqlServerAssertExpander.expandImportSql("ASSERT_UNCHANGED_ROW_COUNT(1)");

        assertThat(expanded).isEqualTo("ASSERT_UNCHANGED_ROW_COUNT(1)");
    }

    @Test
    void rejectsUnterminatedAssertExpression() {
        assertThatThrownBy(() -> SqlServerAssertExpander.expandImportSql("ASSERT_ROW_COUNT(1"))
                .isInstanceOf(RuntimeExecutionException.class)
                .hasMessageContaining("Unterminated assert expression");
    }

    @Test
    void creationExpandsOnlyAssertDatabaseVersionAgainstCurrentDatabase() {
        final var expanded = SqlServerAssertExpander.expandCreationSql(
                "ASSERT_DATABASE_VERSION('Version_2')\nASSERT_ROW_COUNT(1)\nASSERT_UNCHANGED_ROW_COUNT()");

        assertThat(expanded)
                .doesNotContain("ASSERT_DATABASE_VERSION")
                .doesNotContain("__SOURCE__")
                .doesNotContain("__TARGET__")
                .contains("FROM sys.fn_listextendedproperty")
                .contains("@DbVersion != 'Version_2'")
                .contains("Expected DatabaseSchemaVersion in current database")
                .contains("ASSERT_ROW_COUNT(1)")
                .contains("ASSERT_UNCHANGED_ROW_COUNT()");
    }
}
