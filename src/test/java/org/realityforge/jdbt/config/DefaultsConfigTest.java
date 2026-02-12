package org.realityforge.jdbt.config;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import org.junit.jupiter.api.Test;

final class DefaultsConfigTest {
    @Test
    void mergeRetainsExistingValuesWhenOverridesAreNull() {
        final DefaultsConfig defaults = DefaultsConfig.rubyCompatibleDefaults();

        final DefaultsConfig merged = defaults.merge(
                null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null);

        assertThat(merged).isEqualTo(defaults);
    }

    @Test
    void mergeAppliesEveryOverrideValue() {
        final DefaultsConfig merged = DefaultsConfig.rubyCompatibleDefaults()
                .merge(
                        List.of("a"),
                        List.of("b"),
                        List.of("c"),
                        List.of("d"),
                        List.of("e"),
                        List.of("f"),
                        List.of("g"),
                        List.of("h"),
                        "import-x",
                        "datasets-x",
                        List.of("i"),
                        List.of("j"),
                        "fixtures-x",
                        "migrations-x",
                        "index-x.txt",
                        "db-x",
                        "imp-x");

        assertThat(merged.searchDirs()).containsExactly("a");
        assertThat(merged.upDirs()).containsExactly("b");
        assertThat(merged.downDirs()).containsExactly("c");
        assertThat(merged.finalizeDirs()).containsExactly("d");
        assertThat(merged.preCreateDirs()).containsExactly("e");
        assertThat(merged.postCreateDirs()).containsExactly("f");
        assertThat(merged.preImportDirs()).containsExactly("g");
        assertThat(merged.postImportDirs()).containsExactly("h");
        assertThat(merged.importDir()).isEqualTo("import-x");
        assertThat(merged.datasetsDirName()).isEqualTo("datasets-x");
        assertThat(merged.preDatasetDirs()).containsExactly("i");
        assertThat(merged.postDatasetDirs()).containsExactly("j");
        assertThat(merged.fixtureDirName()).isEqualTo("fixtures-x");
        assertThat(merged.migrationsDirName()).isEqualTo("migrations-x");
        assertThat(merged.indexFileName()).isEqualTo("index-x.txt");
        assertThat(merged.defaultDatabase()).isEqualTo("db-x");
        assertThat(merged.defaultImport()).isEqualTo("imp-x");
    }
}
