package org.realityforge.jdbt.config;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.jspecify.annotations.Nullable;
import org.snakeyaml.engine.v2.api.ConstructNode;
import org.snakeyaml.engine.v2.api.Load;
import org.snakeyaml.engine.v2.api.LoadSettings;
import org.snakeyaml.engine.v2.exceptions.YamlEngineException;
import org.snakeyaml.engine.v2.nodes.Tag;

public final class YamlMapSupport {
    private static final Tag LOCAL_OMAP_TAG = new Tag("!omap");
    private static final Tag YAML_OMAP_TAG = new Tag("tag:yaml.org,2002:omap");
    private static final ConstructNode REJECT_ORDERED_MAP = ignored -> {
        throw new ConfigException("Ordered-map YAML tags are not supported; use a plain map.");
    };

    private YamlMapSupport() {}

    public static Map<String, Object> parseRoot(final String yaml, final String sourceName) {
        final var loaded = parseDocument(yaml, sourceName);
        if (!(loaded instanceof Map<?, ?> loadedMap)) {
            throw new ConfigException("Expected root YAML object in " + sourceName + " to be a map.");
        }
        return toStringMap(loadedMap, sourceName);
    }

    public static @Nullable Object parseDocument(final String yaml, final String sourceName) {
        final var settings = LoadSettings.builder()
                .setAllowDuplicateKeys(false)
                .setDefaultMap(LinkedHashMap::new)
                .setTagConstructors(Map.of(LOCAL_OMAP_TAG, REJECT_ORDERED_MAP, YAML_OMAP_TAG, REJECT_ORDERED_MAP))
                .setLabel(sourceName)
                .build();
        try {
            return new Load(settings).loadFromString(yaml);
        } catch (final YamlEngineException e) {
            if (e.getCause() instanceof ConfigException configException) {
                throw configException;
            }
            throw new ConfigException("Invalid YAML in " + sourceName + ": " + e.getMessage());
        }
    }

    public static void assertKeys(final Map<String, Object> map, final Set<String> allowedKeys, final String path) {
        for (final var key : map.keySet()) {
            if (!allowedKeys.contains(key)) {
                throw new ConfigException(
                        "Unknown key '" + key + "' at " + path + ". Allowed keys: " + allowedKeys + '.');
            }
        }
    }

    public static Map<String, Object> requireMap(final Map<String, Object> map, final String key, final String path) {
        final var value = optionalMap(map, key, path);
        if (value == null) {
            throw new ConfigException("Missing required map key '" + key + "' at " + path + '.');
        }
        return value;
    }

    public static @Nullable Map<String, Object> optionalMap(
            final Map<String, Object> map, final String key, final String path) {
        final var value = map.get(key);
        if (value == null) {
            return null;
        }
        if (!(value instanceof Map<?, ?> m)) {
            throw new ConfigException("Expected map for key '" + key + "' at " + path + '.');
        }
        return toStringMap(m, path + '.' + key);
    }

    public static String requireString(final Map<String, Object> map, final String key, final String path) {
        final var value = optionalString(map, key, path);
        if (value == null) {
            throw new ConfigException("Missing required string key '" + key + "' at " + path + '.');
        }
        return value;
    }

    public static @Nullable String optionalString(final Map<String, Object> map, final String key, final String path) {
        final var value = map.get(key);
        if (value == null) {
            return null;
        }
        if (!(value instanceof String text)) {
            throw new ConfigException("Expected string for key '" + key + "' at " + path + '.');
        }
        return text;
    }

    public static @Nullable Boolean optionalBoolean(
            final Map<String, Object> map, final String key, final String path) {
        final var value = map.get(key);
        if (value == null) {
            return null;
        }
        if (!(value instanceof Boolean flag)) {
            throw new ConfigException("Expected boolean for key '" + key + "' at " + path + '.');
        }
        return flag;
    }

    public static List<String> optionalStringList(
            final Map<String, Object> map, final String key, final String path, final List<String> defaultValue) {
        final var value = map.get(key);
        if (value == null) {
            return defaultValue;
        }
        if (!(value instanceof List<?> list)) {
            throw new ConfigException("Expected list for key '" + key + "' at " + path + '.');
        }
        return toStringList(list, path + '.' + key);
    }

    public static List<String> requireStringList(final Map<String, Object> map, final String key, final String path) {
        final var value = map.get(key);
        if (value == null) {
            throw new ConfigException("Missing required list key '" + key + "' at " + path + '.');
        }
        if (!(value instanceof List<?> list)) {
            throw new ConfigException("Expected list for key '" + key + "' at " + path + '.');
        }
        return toStringList(list, path + '.' + key);
    }

    public static Map<String, Object> toStringMap(final Map<?, ?> map, final String path) {
        final var result = new LinkedHashMap<String, Object>();
        for (final var entry : map.entrySet()) {
            final var key = entry.getKey();
            if (!(key instanceof String text)) {
                throw new ConfigException("Expected string map key at " + path + " but got: " + key);
            }
            result.put(text, entry.getValue());
        }
        return result;
    }

    public static List<String> toStringList(final List<?> list, final String path) {
        final var result = new ArrayList<String>(list.size());
        for (final var value : list) {
            if (!(value instanceof String text)) {
                throw new ConfigException("Expected string list entry at " + path + " but got: " + value);
            }
            result.add(text);
        }
        return List.copyOf(result);
    }
}
