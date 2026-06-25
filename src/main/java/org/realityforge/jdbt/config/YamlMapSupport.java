package org.realityforge.jdbt.config;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import org.jspecify.annotations.Nullable;
import org.snakeyaml.engine.v2.api.ConstructNode;
import org.snakeyaml.engine.v2.api.Load;
import org.snakeyaml.engine.v2.api.LoadSettings;
import org.snakeyaml.engine.v2.constructor.StandardConstructor;
import org.snakeyaml.engine.v2.nodes.MappingNode;
import org.snakeyaml.engine.v2.nodes.Node;
import org.snakeyaml.engine.v2.nodes.ScalarNode;
import org.snakeyaml.engine.v2.nodes.SequenceNode;
import org.snakeyaml.engine.v2.nodes.Tag;

public final class YamlMapSupport {
    private static final Tag LOCAL_OMAP_TAG = new Tag("!omap");
    private static final Tag YAML_OMAP_TAG = new Tag("tag:yaml.org,2002:omap");

    private YamlMapSupport() {}

    public static Map<String, Object> parseRoot(final String yaml, final String sourceName) {
        final var loaded = parseDocument(yaml, sourceName);
        if (!(loaded instanceof Map<?, ?> loadedMap)) {
            throw new ConfigException("Expected root YAML object in " + sourceName + " to be a map.");
        }
        return toStringMap(loadedMap, sourceName);
    }

    public static @Nullable Object parseDocument(final String yaml, final String sourceName) {
        final var omapConstructor = new OmapConstructNode();
        final var settings = LoadSettings.builder()
                .setAllowDuplicateKeys(false)
                .setDefaultMap(LinkedHashMap::new)
                .setTagConstructors(Map.of(LOCAL_OMAP_TAG, omapConstructor, YAML_OMAP_TAG, omapConstructor))
                .setLabel(sourceName)
                .build();
        omapConstructor.setSettings(settings);
        return new Load(settings).loadFromString(yaml);
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

    private static final class OmapConstructNode implements ConstructNode {
        private @Nullable LoadSettings settings;

        @Override
        public Object construct(final Node node) {
            if (node instanceof SequenceNode sequenceNode) {
                final var actualSettings = Objects.requireNonNull(settings);
                return new SequenceConstructor(actualSettings).constructSequenceNode(sequenceNode);
            }
            if (node instanceof MappingNode mappingNode
                    && mappingNode.getValue().isEmpty()) {
                return new LinkedHashMap<>();
            }
            if (node instanceof ScalarNode scalarNode
                    && ("[]".equals(scalarNode.getValue())
                            || scalarNode.getValue().isBlank())) {
                return new LinkedHashMap<>();
            }
            throw new ConfigException("Expected !omap value to be a YAML sequence but got "
                    + node.getClass().getSimpleName() + '.');
        }

        private void setSettings(final LoadSettings settings) {
            this.settings = settings;
        }
    }

    private static final class SequenceConstructor extends StandardConstructor {
        private SequenceConstructor(final LoadSettings settings) {
            super(settings);
        }

        private Map<Object, Object> constructSequenceNode(final SequenceNode node) {
            final var entries = constructSequence(node);
            final var result = new LinkedHashMap<Object, Object>();
            for (final var entry : entries) {
                if (!(entry instanceof Map<?, ?> map)) {
                    throw new ConfigException("Expected !omap entry to be a map.");
                }
                if (map.size() != 1) {
                    throw new ConfigException("Expected !omap entry to contain exactly one key.");
                }
                final var mapEntry = map.entrySet().iterator().next();
                if (result.containsKey(mapEntry.getKey())) {
                    throw new ConfigException("Duplicate !omap key '" + mapEntry.getKey() + "'.");
                }
                result.put(mapEntry.getKey(), mapEntry.getValue());
            }
            return result;
        }
    }
}
