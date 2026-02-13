package org.realityforge.jdbt.runtime;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.Test;

final class SqlServerImportAssertExpanderTest {
    @Test
    void expandsAssertRowCountWithNestedExpression() {
        final var expanded = SqlServerImportAssertExpander.expand("ASSERT_ROW_COUNT(SELECT COUNT(*) FROM Foo)");

        assertThat(expanded)
                .contains("IF (SELECT COUNT(*) FROM [__TARGET__].__TABLE__) != (SELECT COUNT(*) FROM Foo)")
                .contains("RAISERROR ('Actual row count for __TABLE__ does not match expected rowcount'");
    }

    @Test
    void expandsAssertDatabaseVersion() {
        final var expanded = SqlServerImportAssertExpander.expand("ASSERT_DATABASE_VERSION('Version_2')");

        assertThat(expanded)
                .contains("[__SOURCE__].sys.fn_listextendedproperty")
                .contains("[__TARGET__].sys.fn_listextendedproperty")
                .contains("@DbVersion = 'Version_2'")
                .contains("@DbVersion != 'Version_2'");
    }

    @Test
    void expandsAssertUnchangedRowCount() {
        final var expanded = SqlServerImportAssertExpander.expand("ASSERT_UNCHANGED_ROW_COUNT()");

        assertThat(expanded)
                .contains("COUNT(*) FROM [__TARGET__].__TABLE__) != (SELECT COUNT(*) FROM [__SOURCE__].__TABLE__)");
    }

    @Test
    void leavesAssertUnchangedRowCountWithArgumentsUnchanged() {
        final var expanded = SqlServerImportAssertExpander.expand("ASSERT_UNCHANGED_ROW_COUNT(1)");

        assertThat(expanded).isEqualTo("ASSERT_UNCHANGED_ROW_COUNT(1)");
    }

    @Test
    void rejectsUnterminatedAssertExpression() {
        assertThatThrownBy(() -> SqlServerImportAssertExpander.expand("ASSERT_ROW_COUNT(1"))
                .isInstanceOf(RuntimeExecutionException.class)
                .hasMessageContaining("Unterminated import assert expression");
    }
}
