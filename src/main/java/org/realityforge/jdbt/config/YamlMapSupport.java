package org.realityforge.jdbt.config;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.jspecify.annotations.Nullable;
import org.snakeyaml.engine.v2.api.Load;
import org.snakeyaml.engine.v2.api.LoadSettings;

public final class YamlMapSupport {
    private YamlMapSupport() {}

    public static Map<String, Object> parseRoot(final String yaml, final String sourceName) {
        final LoadSettings settings = LoadSettings.builder()
                .setAllowDuplicateKeys(false)
                .setLabel(sourceName)
                .build();
        final Object loaded = new Load(settings).loadFromString(yaml);
        if (!(loaded instanceof Map<?, ?> loadedMap)) {
            throw new ConfigException("Expected root YAML object in " + sourceName + " to be a map.");
        }
        return toStringMap(loadedMap, sourceName);
    }

    public static void assertKeys(final Map<String, Object> map, final Set<String> allowedKeys, final String path) {
        for (final String key : map.keySet()) {
            if (!allowedKeys.contains(key)) {
                throw new ConfigException(
                        "Unknown key '" + key + "' at " + path + ". Allowed keys: " + allowedKeys + '.');
            }
        }
    }

    public static Map<String, Object> requireMap(final Map<String, Object> map, final String key, final String path) {
        final Map<String, Object> value = optionalMap(map, key, path);
        if (value == null) {
            throw new ConfigException("Missing required map key '" + key + "' at " + path + '.');
        }
        return value;
    }

    public static @Nullable Map<String, Object> optionalMap(
            final Map<String, Object> map, final String key, final String path) {
        final Object value = map.get(key);
        if (value == null) {
            return null;
        }
        if (!(value instanceof Map<?, ?> m)) {
            throw new ConfigException("Expected map for key '" + key + "' at " + path + '.');
        }
        return toStringMap(m, path + '.' + key);
    }

    public static String requireString(final Map<String, Object> map, final String key, final String path) {
        final String value = optionalString(map, key, path);
        if (value == null) {
            throw new ConfigException("Missing required string key '" + key + "' at " + path + '.');
        }
        return value;
    }

    public static @Nullable String optionalString(final Map<String, Object> map, final String key, final String path) {
        final Object value = map.get(key);
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
        final Object value = map.get(key);
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
        final Object value = map.get(key);
        if (value == null) {
            return defaultValue;
        }
        if (!(value instanceof List<?> list)) {
            throw new ConfigException("Expected list for key '" + key + "' at " + path + '.');
        }
        return toStringList(list, path + '.' + key);
    }

    public static List<String> requireStringList(final Map<String, Object> map, final String key, final String path) {
        final Object value = map.get(key);
        if (value == null) {
            throw new ConfigException("Missing required list key '" + key + "' at " + path + '.');
        }
        if (!(value instanceof List<?> list)) {
            throw new ConfigException("Expected list for key '" + key + "' at " + path + '.');
        }
        return toStringList(list, path + '.' + key);
    }

    public static Map<String, Object> toStringMap(final Map<?, ?> map, final String path) {
        final Map<String, Object> result = new LinkedHashMap<>();
        for (final Map.Entry<?, ?> entry : map.entrySet()) {
            final Object key = entry.getKey();
            if (!(key instanceof String text)) {
                throw new ConfigException("Expected string map key at " + path + " but got: " + key);
            }
            result.put(text, entry.getValue());
        }
        return result;
    }

    public static List<String> toStringList(final List<?> list, final String path) {
        final List<String> result = new ArrayList<>(list.size());
        for (final Object value : list) {
            if (!(value instanceof String text)) {
                throw new ConfigException("Expected string list entry at " + path + " but got: " + value);
            }
            result.add(text);
        }
        return List.copyOf(result);
    }
}
