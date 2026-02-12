package org.realityforge.jdbt.config;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.List;
import java.util.Map;
import java.util.Set;
import org.junit.jupiter.api.Test;

final class YamlMapSupportTest {
    @Test
    void parseRootRejectsNonMapRoot() {
        assertThatThrownBy(() -> YamlMapSupport.parseRoot("- a", "x.yml"))
                .isInstanceOf(ConfigException.class)
                .hasMessageContaining("root YAML object");
    }

    @Test
    void assertKeysRejectsUnknownKeys() {
        assertThatThrownBy(() -> YamlMapSupport.assertKeys(Map.of("bad", true), Set.of("ok"), "root"))
                .isInstanceOf(ConfigException.class)
                .hasMessageContaining("Unknown key 'bad'");
    }

    @Test
    void mapReadersValidateTypesAndPresence() {
        final Map<String, Object> map = Map.of("s", "x", "m", Map.of("k", "v"), "l", List.of("a"), "b", true);

        assertThat(YamlMapSupport.requireMap(map, "m", "root")).containsEntry("k", "v");
        assertThat(YamlMapSupport.optionalMap(map, "m", "root")).containsEntry("k", "v");
        assertThat(YamlMapSupport.requireString(map, "s", "root")).isEqualTo("x");
        assertThat(YamlMapSupport.optionalString(map, "s", "root")).isEqualTo("x");
        assertThat(YamlMapSupport.optionalBoolean(map, "b", "root")).isTrue();
        assertThat(YamlMapSupport.optionalStringList(map, "l", "root", List.of()))
                .containsExactly("a");
        assertThat(YamlMapSupport.requireStringList(map, "l", "root")).containsExactly("a");
    }

    @Test
    void mapReadersRejectInvalidInputs() {
        final Map<String, Object> map = Map.of("s", 1, "m", 1, "l", List.of(1), "b", "x");

        assertThatThrownBy(() -> YamlMapSupport.requireMap(Map.of(), "missing", "root"))
                .isInstanceOf(ConfigException.class)
                .hasMessageContaining("Missing required map key");
        assertThatThrownBy(() -> YamlMapSupport.optionalMap(map, "m", "root"))
                .isInstanceOf(ConfigException.class)
                .hasMessageContaining("Expected map");
        assertThatThrownBy(() -> YamlMapSupport.requireString(Map.of(), "missing", "root"))
                .isInstanceOf(ConfigException.class)
                .hasMessageContaining("Missing required string key");
        assertThatThrownBy(() -> YamlMapSupport.optionalString(map, "s", "root"))
                .isInstanceOf(ConfigException.class)
                .hasMessageContaining("Expected string");
        assertThatThrownBy(() -> YamlMapSupport.optionalBoolean(map, "b", "root"))
                .isInstanceOf(ConfigException.class)
                .hasMessageContaining("Expected boolean");
        assertThatThrownBy(() -> YamlMapSupport.optionalStringList(map, "l", "root", List.of()))
                .isInstanceOf(ConfigException.class)
                .hasMessageContaining("Expected string list entry");
        assertThatThrownBy(() -> YamlMapSupport.requireStringList(Map.of(), "missing", "root"))
                .isInstanceOf(ConfigException.class)
                .hasMessageContaining("Missing required list key");
        assertThatThrownBy(() -> YamlMapSupport.toStringMap(Map.of(1, "x"), "root"))
                .isInstanceOf(ConfigException.class)
                .hasMessageContaining("Expected string map key");
        assertThatThrownBy(() -> YamlMapSupport.toStringList(List.of(1), "root"))
                .isInstanceOf(ConfigException.class)
                .hasMessageContaining("Expected string list entry");
    }
}
