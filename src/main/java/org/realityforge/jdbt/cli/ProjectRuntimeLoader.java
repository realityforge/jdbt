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
import java.util.stream.Collectors;
import org.jspecify.annotations.Nullable;
import org.realityforge.jdbt.config.ConfigException;
import org.realityforge.jdbt.config.DefaultsConfig;
import org.realityforge.jdbt.config.JdbtProjectConfigLoader;
import org.realityforge.jdbt.config.YamlMapSupport;
import org.realityforge.jdbt.files.ArtifactContent;
import org.realityforge.jdbt.files.FileCollectionException;
import org.realityforge.jdbt.files.FileResolver;
import org.realityforge.jdbt.files.ZipArtifactContent;
import org.realityforge.jdbt.repository.RepositoryConfig;
import org.realityforge.jdbt.repository.RepositoryConfigLoader;
import org.realityforge.jdbt.repository.RepositoryConfigMerger;
import org.realityforge.jdbt.runtime.RuntimeDatabase;
import org.realityforge.jdbt.runtime.RuntimeDatabaseFactory;

final class ProjectRuntimeLoader {
    private static final String PROJECT_CONFIG_FILE = "jdbt.yml";
    private static final String REPOSITORY_CONFIG_FILE = "repository.yml";

    private final Path projectDirectory;
    private final RepositoryConfigLoader repositoryConfigLoader = new RepositoryConfigLoader();
    private final JdbtProjectConfigLoader projectConfigLoader = new JdbtProjectConfigLoader();
    private final RuntimeDatabaseFactory runtimeDatabaseFactory = new RuntimeDatabaseFactory();
    private final RepositoryConfigMerger repositoryConfigMerger = new RepositoryConfigMerger();
    private final FileResolver fileResolver = new FileResolver();

    ProjectRuntimeLoader(final Path projectDirectory) {
        this.projectDirectory = projectDirectory.toAbsolutePath().normalize();
    }

    LoadedRuntime load(final @Nullable String selectedDatabaseKey) {
        if (!Files.isDirectory(projectDirectory)) {
            throw new ConfigException("Project directory does not exist: " + projectDirectory);
        }
        final var projectConfigFile = projectDirectory.resolve(PROJECT_CONFIG_FILE);
        if (!Files.isRegularFile(projectConfigFile)) {
            throw new ConfigException(PROJECT_CONFIG_FILE + " not found in project directory " + projectDirectory);
        }
        final var projectYaml = readFile(projectConfigFile);
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
        final var projectConfig = projectConfigLoader.load(projectYaml, PROJECT_CONFIG_FILE, repository.modules());
        final var database = projectConfig.database();
        final var resourceRoot = resolveResourceRoot(projectConfig.resourceRoot());

        final var resolvedPreDbArtifacts = loadArtifacts(database.preDbArtifacts());
        final var resolvedPostDbArtifacts = loadArtifacts(database.postDbArtifacts());
        final var runtimeDatabaseWithoutHash = runtimeDatabaseFactory.from(
                database,
                projectConfig.defaults(),
                repository,
                resolvedPreDbArtifacts,
                resolvedPostDbArtifacts,
                null,
                resourceRoot);
        validateLogicalResourcePaths(runtimeDatabaseWithoutHash);
        final var runtimeDatabase = runtimeDatabaseFactory.from(
                database,
                projectConfig.defaults(),
                repository,
                resolvedPreDbArtifacts,
                resolvedPostDbArtifacts,
                schemaHash(runtimeDatabaseWithoutHash),
                resourceRoot);
        return new LoadedRuntime(runtimeDatabase, projectConfig.defaults(), projectDirectory);
    }

    void validate(final @Nullable String selectedDatabaseKey) {
        load(selectedDatabaseKey);
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
                        "dataPath",
                        "logPath",
                        "forceDrop",
                        "deleteBackupHistory",
                        "reindexOnImport",
                        "shrinkOnImport",
                        "preDbArtifacts",
                        "postDbArtifacts",
                        "filterProperties",
                        "imports",
                        "moduleGroups",
                        "resourceRoot"),
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
        final var repositoryFile = projectDirectory.resolve(REPOSITORY_CONFIG_FILE);
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
        return (value.isAbsolute() ? value : projectDirectory.resolve(value)).normalize();
    }

    private Path resolveResourceRoot(final String configuredRoot) {
        if (configuredRoot.isBlank()) {
            throw new ConfigException("resourceRoot in " + PROJECT_CONFIG_FILE + " must not be blank.");
        }
        final var value = Path.of(configuredRoot);
        final var root = (value.isAbsolute() ? value : projectDirectory.resolve(value)).normalize();
        if (!Files.isDirectory(root)) {
            throw new ConfigException("resourceRoot in " + PROJECT_CONFIG_FILE + " is not a directory: " + root);
        }
        return root;
    }

    private String schemaHash(final RuntimeDatabase database) {
        final var buffer = new StringBuilder();
        for (final var path : collectFilesetForHash(database)) {
            buffer.append(logicalResourcePath(database, path))
                    .append(" : ")
                    .append(md5(loadData(database, path)))
                    .append('\n');
        }
        return md5(buffer.toString());
    }

    private static String logicalResourcePath(final RuntimeDatabase database, final String location) {
        if (location.startsWith("zip:")) {
            return location;
        }
        final var path = Path.of(location).toAbsolutePath().normalize();
        final var resourceRoot = database.searchDirs().get(0).toAbsolutePath().normalize();
        if (!path.startsWith(resourceRoot)) {
            throw new ConfigException("Resolved resource is outside resourceRoot: " + path);
        }
        return resourceRoot.relativize(path).toString().replace('\\', '/');
    }

    private static void validateLogicalResourcePaths(final RuntimeDatabase database) {
        final var paths = new ArrayList<String>();
        paths.addAll(database.upDirs());
        paths.addAll(database.downDirs());
        paths.addAll(database.finalizeDirs());
        paths.addAll(database.preCreateDirs());
        paths.addAll(database.postCreateDirs());
        paths.add(database.fixtureDirName());
        paths.add(database.datasetsDirName());
        paths.addAll(database.preDatasetDirs());
        paths.addAll(database.postDatasetDirs());
        if (database.migrationsEnabled()) {
            paths.add(database.migrationsDirName());
        }
        for (final var importConfig : database.imports().values()) {
            paths.add(importConfig.dir());
            paths.addAll(importConfig.preImportDirs());
            paths.addAll(importConfig.postImportDirs());
        }
        for (final var configuredPath : paths) {
            final var path = Path.of(configuredPath);
            if (path.isAbsolute() || path.normalize().startsWith("..")) {
                throw new ConfigException("Logical resource path must remain beneath resourceRoot: " + configuredPath);
            }
        }
    }

    private List<String> collectFilesetForHash(final RuntimeDatabase database) {
        final var files = new ArrayList<String>();
        for (final var dir : database.preCreateDirs()) {
            files.addAll(collectDirSet(database, dir));
        }

        for (final var moduleName : database.repository().modules()) {
            for (final var dirs : List.of(database.upDirs(), database.downDirs(), database.finalizeDirs())) {
                for (final var dir : dirs) {
                    files.addAll(collectDirSet(database, moduleName + '/' + dir));
                }
            }

            final var fixtures = fileResolver.collectFixtures(
                    database.searchDirs(),
                    moduleName,
                    database.fixtureDirName(),
                    database.orderedElementsForModule(moduleName),
                    database.postDbArtifacts(),
                    database.preDbArtifacts());
            for (final var tableName : database.orderedElementsForModule(moduleName)) {
                final var fixture = fixtures.get(tableName);
                if (null != fixture) {
                    files.add(fixture);
                }
            }

            for (final var dataset : database.datasets()) {
                final var datasetFixtures = fileResolver.collectFixtures(
                        database.searchDirs(),
                        moduleName,
                        database.datasetsDirName() + '/' + dataset,
                        database.orderedElementsForModule(moduleName),
                        database.postDbArtifacts(),
                        database.preDbArtifacts());
                for (final var tableName : database.orderedElementsForModule(moduleName)) {
                    final var fixture = datasetFixtures.get(tableName);
                    if (null != fixture) {
                        files.add(fixture);
                    }
                }
            }
        }

        for (final var importConfig : database.imports().values()) {
            for (final var dir : importConfig.preImportDirs()) {
                files.addAll(collectDirSet(database, dir));
            }
            for (final var moduleName : importConfig.modules()) {
                files.addAll(collectElementFiles(database, moduleName, importConfig.dir(), "yml"));
                files.addAll(collectElementFiles(database, moduleName, importConfig.dir(), "sql"));
            }
            for (final var dir : importConfig.postImportDirs()) {
                files.addAll(collectDirSet(database, dir));
            }
        }

        for (final var dir : database.postCreateDirs()) {
            files.addAll(collectDirSet(database, dir));
        }

        for (final var dataset : database.datasets()) {
            final var root = database.datasetsDirName() + '/' + dataset;
            for (final var dir : database.preDatasetDirs()) {
                files.addAll(collectDirSet(database, root + '/' + dir));
            }
            for (final var dir : database.postDatasetDirs()) {
                files.addAll(collectDirSet(database, root + '/' + dir));
            }
        }

        if (database.migrationsEnabled()) {
            files.addAll(collectDirSet(database, database.migrationsDirName()));
        }

        return List.copyOf(files);
    }

    private List<String> collectElementFiles(
            final RuntimeDatabase database, final String moduleName, final String relativeDir, final String extension) {
        final var files = fileResolver.collectFiles(
                database.searchDirs(),
                moduleName + '/' + relativeDir,
                extension,
                database.indexFileName(),
                database.postDbArtifacts(),
                database.preDbArtifacts());
        final var knownElements = database.orderedElementsForModule(moduleName).stream()
                .map(ProjectRuntimeLoader::cleanObjectName)
                .collect(Collectors.toUnmodifiableSet());
        final var unexpected = files.stream()
                .filter(file -> !knownElements.contains(basenameWithoutExtension(file)))
                .toList();
        if (!unexpected.isEmpty()) {
            throw new FileCollectionException("Unexpected "
                    + extension
                    + " files found in import directory for module "
                    + moduleName
                    + ". Files do not match repository elements: "
                    + unexpected);
        }
        return files;
    }

    private List<String> collectDirSet(final RuntimeDatabase database, final String dir) {
        return fileResolver.collectFiles(
                database.searchDirs(),
                dir,
                "sql",
                database.indexFileName(),
                database.postDbArtifacts(),
                database.preDbArtifacts());
    }

    private static String basenameWithoutExtension(final String location) {
        final var slash = Math.max(location.lastIndexOf('/'), location.lastIndexOf('\\'));
        final var basename = -1 == slash ? location : location.substring(slash + 1);
        final var dot = basename.lastIndexOf('.');
        return -1 == dot ? basename : basename.substring(0, dot);
    }

    private static String cleanObjectName(final String value) {
        return value.replace("[", "")
                .replace("]", "")
                .replace("\"", "")
                .replace("'", "")
                .replace(" ", "");
    }

    private static String loadData(final RuntimeDatabase database, final String location) {
        if (location.startsWith("zip:")) {
            final var separator = location.indexOf(':', 4);
            if (-1 == separator) {
                throw new ConfigException("Invalid artifact location " + location);
            }
            final var artifactId = location.substring(4, separator);
            final var path = location.substring(separator + 1);
            final var artifact = database.artifactById(artifactId);
            if (null == artifact) {
                throw new ConfigException("Unable to locate artifact with id '" + artifactId + "'.");
            }
            return artifact.readText(path);
        }
        return readFile(Path.of(location));
    }

    private static String md5(final String content) {
        try {
            final var digest = MessageDigest.getInstance("MD5");
            final var hash = digest.digest(content.getBytes(StandardCharsets.UTF_8));
            return HexFormat.of().formatHex(hash);
        } catch (final NoSuchAlgorithmException nsae) {
            throw new IllegalStateException("Unable to create MD5 digest", nsae);
        }
    }

    private record BootstrapProject(String defaultDatabase, BootstrapDatabase database) {}

    private record BootstrapDatabase(List<String> preDbArtifacts, List<String> postDbArtifacts) {
        private BootstrapDatabase {
            preDbArtifacts = List.copyOf(preDbArtifacts);
            postDbArtifacts = List.copyOf(postDbArtifacts);
        }
    }

    record LoadedRuntime(RuntimeDatabase database, DefaultsConfig defaults, Path projectDirectory) {}
}
