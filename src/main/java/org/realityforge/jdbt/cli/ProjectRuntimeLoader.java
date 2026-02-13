package org.realityforge.jdbt.cli;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.jspecify.annotations.Nullable;
import org.realityforge.jdbt.config.ConfigException;
import org.realityforge.jdbt.config.DefaultsConfig;
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
        final var projectYaml = readFile(baseDirectory.resolve(PROJECT_CONFIG_FILE));
        final var bootstrap = loadBootstrap(projectYaml);
        final var databaseKey = resolveDatabaseKey(selectedDatabaseKey, bootstrap);
        if (!bootstrap.defaultDatabase().equals(databaseKey)) {
            throw new ConfigException(
                    "Unable to locate database '" + databaseKey + "' in " + PROJECT_CONFIG_FILE + '.');
        }
        final var bootstrapDatabase = bootstrap.database();

        final var preDbArtifacts = loadArtifacts(bootstrapDatabase.preDbArtifacts());
        final var postDbArtifacts = loadArtifacts(bootstrapDatabase.postDbArtifacts());
        final var repository = loadRepository(preDbArtifacts, postDbArtifacts);
        final var projectConfig = projectConfigLoader.load(projectYaml, PROJECT_CONFIG_FILE, repository);
        final var database = projectConfig.database();

        final var resolvedPreDbArtifacts = loadArtifacts(database.preDbArtifacts());
        final var resolvedPostDbArtifacts = loadArtifacts(database.postDbArtifacts());
        final var runtimeDatabase = runtimeDatabaseFactory.from(
                database,
                projectConfig.defaults(),
                repository,
                resolvedPreDbArtifacts,
                resolvedPostDbArtifacts,
                schemaHash(repository),
                baseDirectory);
        return new LoadedRuntime(runtimeDatabase, projectConfig.defaults());
    }

    private static BootstrapProject loadBootstrap(final String yaml) {
        final var root = YamlMapSupport.parseRoot(yaml, PROJECT_CONFIG_FILE);
        YamlMapSupport.assertKeys(
                root,
                Set.of(
                        "upDirs",
                        "downDirs",
                        "finalizeDirs",
                        "preCreateDirs",
                        "postCreateDirs",
                        "datasets",
                        "datasetsDirName",
                        "preDatasetDirs",
                        "postDatasetDirs",
                        "fixtureDirName",
                        "migrations",
                        "migrationsAppliedAtCreate",
                        "migrationsDirName",
                        "version",
                        "resourcePrefix",
                        "preDbArtifacts",
                        "postDbArtifacts",
                        "filterProperties",
                        "imports",
                        "moduleGroups"),
                PROJECT_CONFIG_FILE);

        final var defaults = DefaultsConfig.rubyCompatibleDefaults();

        final var preDbArtifacts =
                YamlMapSupport.optionalStringList(root, "preDbArtifacts", PROJECT_CONFIG_FILE, List.of());
        final var postDbArtifacts =
                YamlMapSupport.optionalStringList(root, "postDbArtifacts", PROJECT_CONFIG_FILE, List.of());

        return new BootstrapProject(defaults.defaultDatabase(), new BootstrapDatabase(preDbArtifacts, postDbArtifacts));
    }

    private static String resolveDatabaseKey(
            final @Nullable String selectedDatabaseKey, final BootstrapProject bootstrap) {
        return null != selectedDatabaseKey ? selectedDatabaseKey : bootstrap.defaultDatabase();
    }

    private RepositoryConfig loadRepository(
            final List<ArtifactContent> preDbArtifacts, final List<ArtifactContent> postDbArtifacts) {
        final var preRepositories = repositoryFromArtifacts(preDbArtifacts, "preDbArtifacts");
        final var localRepository = repositoryFromDisk();
        final var postRepositories = repositoryFromArtifacts(postDbArtifacts, "postDbArtifacts");
        final var repository = repositoryConfigMerger.merge(preRepositories, localRepository, postRepositories);
        if (repository.modules().isEmpty()) {
            throw new ConfigException(REPOSITORY_CONFIG_FILE
                    + " not located in base directory of database search path and no modules defined");
        }
        return repository;
    }

    private RepositoryConfig repositoryFromDisk() {
        final var repositoryFile = baseDirectory.resolve(REPOSITORY_CONFIG_FILE);
        if (!Files.exists(repositoryFile)) {
            return new RepositoryConfig(List.of(), Map.of(), Map.of(), Map.of());
        }
        return repositoryConfigLoader.load(readFile(repositoryFile), repositoryFile.toString());
    }

    @SuppressWarnings("UnusedException")
    private List<RepositoryConfig> repositoryFromArtifacts(
            final List<ArtifactContent> artifacts, final String artifactSourceName) {
        final var repositories = new ArrayList<RepositoryConfig>();
        for (final var artifact : artifacts) {
            try {
                final var content = artifact.readText(REPOSITORY_CONFIG_FILE);
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
        final var artifacts = new ArrayList<ArtifactContent>();
        for (final var artifactPath : artifactPaths) {
            final var path = resolvePath(artifactPath);
            if (!Files.exists(path)) {
                throw new ConfigException("Unable to locate database artifact " + artifactPath);
            }
            artifacts.add(new ZipArtifactContent(artifactPath, path, "data"));
        }
        return List.copyOf(artifacts);
    }

    private static String readFile(final Path path) {
        try {
            return Files.readString(path, StandardCharsets.UTF_8);
        } catch (final IOException ioe) {
            throw new UncheckedIOException("Failed to read " + path, ioe);
        }
    }

    private Path resolvePath(final String path) {
        final var value = Path.of(path);
        return value.isAbsolute() ? value : baseDirectory.resolve(value);
    }

    private static String schemaHash(final RepositoryConfig repository) {
        final var buffer = new StringBuilder();
        for (final var module : repository.modules()) {
            buffer.append(module).append('|');
            buffer.append(repository.schemaNameForModule(module)).append('|');
            buffer.append(String.join(",", repository.tableOrdering(module))).append('|');
            buffer.append(String.join(",", repository.sequenceOrdering(module))).append('\n');
        }
        try {
            final var digest = MessageDigest.getInstance("SHA-1");
            final var hash = digest.digest(buffer.toString().getBytes(StandardCharsets.UTF_8));
            return HexFormat.of().formatHex(hash);
        } catch (final NoSuchAlgorithmException nsae) {
            throw new IllegalStateException("Unable to create schema hash", nsae);
        }
    }

    private record BootstrapProject(String defaultDatabase, BootstrapDatabase database) {}

    private record BootstrapDatabase(List<String> preDbArtifacts, List<String> postDbArtifacts) {
        private BootstrapDatabase {
            preDbArtifacts = List.copyOf(preDbArtifacts);
            postDbArtifacts = List.copyOf(postDbArtifacts);
        }
    }

    public record LoadedRuntime(RuntimeDatabase database, DefaultsConfig defaults) {}
}
