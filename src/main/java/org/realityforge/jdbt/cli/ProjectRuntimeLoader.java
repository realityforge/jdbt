package org.realityforge.jdbt.cli;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.jspecify.annotations.Nullable;
import org.realityforge.jdbt.config.ConfigException;
import org.realityforge.jdbt.config.DatabaseConfig;
import org.realityforge.jdbt.config.DefaultsConfig;
import org.realityforge.jdbt.config.JdbtProjectConfig;
import org.realityforge.jdbt.config.JdbtProjectConfigLoader;
import org.realityforge.jdbt.config.YamlMapSupport;
import org.realityforge.jdbt.files.ArtifactContent;
import org.realityforge.jdbt.files.FileCollectionException;
import org.realityforge.jdbt.files.ZipArtifactContent;
import org.realityforge.jdbt.repository.RepositoryConfig;
import org.realityforge.jdbt.repository.RepositoryConfigLoader;
import org.realityforge.jdbt.repository.RepositoryConfigMerger;
import org.realityforge.jdbt.runtime.RuntimeDatabase;
import org.realityforge.jdbt.runtime.RuntimeDatabaseFactory;

public final class ProjectRuntimeLoader {
    private static final String PROJECT_CONFIG_FILE = "jdbt.yml";
    private static final String REPOSITORY_CONFIG_FILE = "repository.yml";

    private final Path baseDirectory;
    private final RepositoryConfigLoader repositoryConfigLoader = new RepositoryConfigLoader();
    private final JdbtProjectConfigLoader projectConfigLoader = new JdbtProjectConfigLoader();
    private final RuntimeDatabaseFactory runtimeDatabaseFactory = new RuntimeDatabaseFactory();
    private final RepositoryConfigMerger repositoryConfigMerger = new RepositoryConfigMerger();

    public ProjectRuntimeLoader(final Path baseDirectory) {
        this.baseDirectory = baseDirectory;
    }

    public LoadedRuntime load(final @Nullable String selectedDatabaseKey) {
        final String projectYaml = readFile(baseDirectory.resolve(PROJECT_CONFIG_FILE));
        final BootstrapProject bootstrap = loadBootstrap(projectYaml);
        final String databaseKey = resolveDatabaseKey(selectedDatabaseKey, bootstrap);
        final BootstrapDatabase bootstrapDatabase = bootstrap.databases().get(databaseKey);
        if (null == bootstrapDatabase) {
            throw new ConfigException(
                    "Unable to locate database '" + databaseKey + "' in " + PROJECT_CONFIG_FILE + '.');
        }

        final List<ArtifactContent> preDbArtifacts = loadArtifacts(bootstrapDatabase.preDbArtifacts());
        final List<ArtifactContent> postDbArtifacts = loadArtifacts(bootstrapDatabase.postDbArtifacts());
        final RepositoryConfig repository = loadRepository(bootstrapDatabase, preDbArtifacts, postDbArtifacts);
        final JdbtProjectConfig projectConfig = projectConfigLoader.load(projectYaml, PROJECT_CONFIG_FILE, repository);
        final DatabaseConfig database = projectConfig.databases().get(databaseKey);
        if (null == database) {
            throw new ConfigException(
                    "Unable to locate database '" + databaseKey + "' in " + PROJECT_CONFIG_FILE + '.');
        }

        final List<ArtifactContent> resolvedPreDbArtifacts = loadArtifacts(database.preDbArtifacts());
        final List<ArtifactContent> resolvedPostDbArtifacts = loadArtifacts(database.postDbArtifacts());
        final RuntimeDatabase runtimeDatabase = runtimeDatabaseFactory.from(
                database,
                projectConfig.defaults(),
                repository,
                resolvedPreDbArtifacts,
                resolvedPostDbArtifacts,
                schemaHash(repository));
        return new LoadedRuntime(runtimeDatabase, projectConfig.defaults());
    }

    private BootstrapProject loadBootstrap(final String yaml) {
        final Map<String, Object> root = YamlMapSupport.parseRoot(yaml, PROJECT_CONFIG_FILE);
        YamlMapSupport.assertKeys(root, Set.of("databases"), PROJECT_CONFIG_FILE);

        final DefaultsConfig defaults = DefaultsConfig.rubyCompatibleDefaults();

        final Map<String, Object> databasesNode = YamlMapSupport.requireMap(root, "databases", PROJECT_CONFIG_FILE);
        if (databasesNode.isEmpty()) {
            throw new ConfigException("No databases defined in " + PROJECT_CONFIG_FILE + '.');
        }

        final Map<String, BootstrapDatabase> databases = new LinkedHashMap<>();
        for (final Map.Entry<String, Object> entry : databasesNode.entrySet()) {
            final String key = entry.getKey();
            if (!(entry.getValue() instanceof Map<?, ?> body)) {
                throw new ConfigException("Expected map for database '" + key + "' in " + PROJECT_CONFIG_FILE + '.');
            }
            final String path = PROJECT_CONFIG_FILE + ".databases." + key;
            final Map<String, Object> databaseNode = YamlMapSupport.toStringMap(body, path);
            final List<String> searchDirs =
                    YamlMapSupport.optionalStringList(databaseNode, "searchDirs", path, defaults.searchDirs());
            if (searchDirs.isEmpty()) {
                throw new ConfigException("Database '" + key + "' must define non-empty searchDirs.");
            }
            final List<String> preDbArtifacts =
                    YamlMapSupport.optionalStringList(databaseNode, "preDbArtifacts", path, List.of());
            final List<String> postDbArtifacts =
                    YamlMapSupport.optionalStringList(databaseNode, "postDbArtifacts", path, List.of());
            databases.put(key, new BootstrapDatabase(searchDirs, preDbArtifacts, postDbArtifacts));
        }

        return new BootstrapProject(defaults.defaultDatabase(), Map.copyOf(databases));
    }

    private String resolveDatabaseKey(final @Nullable String selectedDatabaseKey, final BootstrapProject bootstrap) {
        return null != selectedDatabaseKey ? selectedDatabaseKey : bootstrap.defaultDatabase();
    }

    private RepositoryConfig loadRepository(
            final BootstrapDatabase bootstrapDatabase,
            final List<ArtifactContent> preDbArtifacts,
            final List<ArtifactContent> postDbArtifacts) {
        final List<RepositoryConfig> preRepositories = repositoryFromArtifacts(preDbArtifacts, "preDbArtifacts");
        final RepositoryConfig localRepository = repositoryFromDisk(bootstrapDatabase.searchDirs());
        final List<RepositoryConfig> postRepositories = repositoryFromArtifacts(postDbArtifacts, "postDbArtifacts");
        final RepositoryConfig repository =
                repositoryConfigMerger.merge(preRepositories, localRepository, postRepositories);
        if (repository.modules().isEmpty()) {
            throw new ConfigException(REPOSITORY_CONFIG_FILE
                    + " not located in base directory of database search path and no modules defined");
        }
        return repository;
    }

    private RepositoryConfig repositoryFromDisk(final List<String> searchDirs) {
        final List<Path> candidates = new ArrayList<>();
        for (final String searchDir : searchDirs) {
            final Path candidate = resolvePath(searchDir).resolve(REPOSITORY_CONFIG_FILE);
            if (Files.exists(candidate)) {
                candidates.add(candidate);
            }
        }
        if (candidates.size() > 1) {
            throw new ConfigException(
                    "Duplicate copies of " + REPOSITORY_CONFIG_FILE + " found in database search path");
        }
        if (candidates.isEmpty()) {
            return new RepositoryConfig(List.of(), Map.of(), Map.of(), Map.of());
        }
        final Path repositoryFile = candidates.get(0);
        return repositoryConfigLoader.load(readFile(repositoryFile), repositoryFile.toString());
    }

    private List<RepositoryConfig> repositoryFromArtifacts(
            final List<ArtifactContent> artifacts, final String artifactSourceName) {
        final List<RepositoryConfig> repositories = new ArrayList<>();
        for (final ArtifactContent artifact : artifacts) {
            try {
                final String content = artifact.readText(REPOSITORY_CONFIG_FILE);
                repositories.add(repositoryConfigLoader.load(content, artifactSourceName + ':' + artifact.id()));
            } catch (final FileCollectionException fce) {
                throw new ConfigException("Database artifact "
                        + artifact.id()
                        + " does not contain data/"
                        + REPOSITORY_CONFIG_FILE
                        + " and is not in the correct format.");
            }
        }
        return List.copyOf(repositories);
    }

    private List<ArtifactContent> loadArtifacts(final List<String> artifactPaths) {
        final List<ArtifactContent> artifacts = new ArrayList<>();
        for (final String artifactPath : artifactPaths) {
            final Path path = resolvePath(artifactPath);
            if (!Files.exists(path)) {
                throw new ConfigException("Unable to locate database artifact " + artifactPath);
            }
            artifacts.add(new ZipArtifactContent(artifactPath, path, "data"));
        }
        return List.copyOf(artifacts);
    }

    private String readFile(final Path path) {
        try {
            return Files.readString(path, StandardCharsets.UTF_8);
        } catch (final IOException ioe) {
            throw new UncheckedIOException("Failed to read " + path, ioe);
        }
    }

    private Path resolvePath(final String path) {
        final Path value = Path.of(path);
        return value.isAbsolute() ? value : baseDirectory.resolve(value);
    }

    private String schemaHash(final RepositoryConfig repository) {
        final StringBuilder buffer = new StringBuilder();
        for (final String module : repository.modules()) {
            buffer.append(module).append('|');
            buffer.append(repository.schemaNameForModule(module)).append('|');
            buffer.append(String.join(",", repository.tableOrdering(module))).append('|');
            buffer.append(String.join(",", repository.sequenceOrdering(module))).append('\n');
        }
        try {
            final MessageDigest digest = MessageDigest.getInstance("SHA-1");
            final byte[] hash = digest.digest(buffer.toString().getBytes(StandardCharsets.UTF_8));
            return java.util.HexFormat.of().formatHex(hash);
        } catch (final NoSuchAlgorithmException nsae) {
            throw new IllegalStateException("Unable to create schema hash", nsae);
        }
    }

    private record BootstrapProject(String defaultDatabase, Map<String, BootstrapDatabase> databases) {}

    private record BootstrapDatabase(
            List<String> searchDirs, List<String> preDbArtifacts, List<String> postDbArtifacts) {
        private BootstrapDatabase {
            searchDirs = List.copyOf(searchDirs);
            preDbArtifacts = List.copyOf(preDbArtifacts);
            postDbArtifacts = List.copyOf(postDbArtifacts);
        }
    }

    public record LoadedRuntime(RuntimeDatabase database, DefaultsConfig defaults) {}
}
