package org.realityforge.jdbt.cli;

import java.nio.file.Path;
import java.util.Objects;
import java.util.concurrent.Callable;
import org.jspecify.annotations.Nullable;
import org.realityforge.jdbt.db.DatabaseConnection;
import picocli.CommandLine;

@CommandLine.Command(
        name = "jdbt",
        mixinStandardHelpOptions = true,
        description = "Java database tooling runtime",
        subcommands = {
            JdbtCommand.StatusCommand.class,
            JdbtCommand.CreateCommand.class,
            JdbtCommand.DropCommand.class,
            JdbtCommand.MigrateCommand.class,
            JdbtCommand.ImportCommand.class,
            JdbtCommand.CreateByImportCommand.class,
            JdbtCommand.LoadDatasetCommand.class,
            JdbtCommand.UpModuleGroupCommand.class,
            JdbtCommand.DownModuleGroupCommand.class,
            JdbtCommand.PackageDataCommand.class,
            JdbtCommand.DumpFixturesCommand.class
        })
public final class JdbtCommand implements Callable<Integer> {
    static final int USAGE_EXIT_CODE = 2;
    private final CommandRunner runner;
    private final PasswordResolver passwordResolver;

    private JdbtCommand(final CommandRunner runner, final PasswordResolver passwordResolver) {
        this.runner = runner;
        this.passwordResolver = passwordResolver;
    }

    public static int execute(final String[] args) {
        return execute(
                args,
                new DefaultCommandRunner(new ProjectRuntimeLoader(Path.of("."))),
                new PasswordResolver(System.getenv(), System.in));
    }

    static int execute(final String[] args, final CommandRunner runner, final PasswordResolver passwordResolver) {
        final var effectiveArgs = 0 == args.length ? new String[] {"--help"} : args;
        return new CommandLine(new JdbtCommand(runner, passwordResolver)).execute(effectiveArgs);
    }

    @Override
    public Integer call() {
        return USAGE_EXIT_CODE;
    }

    private abstract static class BaseCommand implements Callable<Integer> {
        @CommandLine.ParentCommand
        private @Nullable JdbtCommand parent;

        @CommandLine.Mixin
        private @Nullable ExecutionOptions executionOptions;

        protected final CommandRunner runner() {
            return Objects.requireNonNull(parent).runner;
        }

        protected final PasswordResolver passwordResolver() {
            return Objects.requireNonNull(parent).passwordResolver;
        }

        protected final @Nullable String databaseKey() {
            return Objects.requireNonNull(executionOptions).databaseKey;
        }

        protected final String driver() {
            return Objects.requireNonNull(executionOptions).driver;
        }
    }

    private static final class ExecutionOptions {
        @CommandLine.Option(names = "--database", description = "Database key (only 'default' is supported)")
        private @Nullable String databaseKey;

        @CommandLine.Option(
                names = "--driver",
                defaultValue = "sqlserver",
                description = "Database driver. Supported values: sqlserver, postgres")
        private String driver = "sqlserver";
    }

    @SuppressWarnings("FieldCanBeFinal")
    private static final class TargetConnectionOptions {
        @CommandLine.Option(names = "--target-host", required = true, description = "Target database host")
        private String host = "";

        @CommandLine.Option(
                names = "--target-port",
                required = true,
                defaultValue = "1433",
                description = "Target database port")
        private int port;

        @CommandLine.Option(names = "--target-database", required = true, description = "Target database name")
        private String database = "";

        @CommandLine.Option(names = "--target-username", required = true, description = "Target database username")
        private String username = "";

        @CommandLine.ArgGroup(exclusive = true, multiplicity = "1")
        private TargetPasswordOptions password = new TargetPasswordOptions();

        private DatabaseConnection toConnection(final PasswordResolver passwordResolver) {
            return new DatabaseConnection(host, port, database, username, password.resolve(passwordResolver));
        }
    }

    private static final class TargetPasswordOptions {
        @CommandLine.Option(names = "--password", description = "Target password")
        private @Nullable String password;

        @CommandLine.Option(names = "--password-env", description = "Environment variable containing target password")
        private @Nullable String passwordEnv;

        @CommandLine.Option(names = "--password-stdin", description = "Read target password from stdin")
        private boolean passwordStdin;

        private String resolve(final PasswordResolver passwordResolver) {
            if (null != password) {
                return passwordResolver.fromDirectValue(password);
            }
            if (null != passwordEnv) {
                return passwordResolver.fromEnvironment(passwordEnv);
            }
            if (passwordStdin) {
                return passwordResolver.fromStdin();
            }
            throw new IllegalStateException("No target password source selected");
        }
    }

    @SuppressWarnings("FieldCanBeFinal")
    private static final class SourceConnectionOptions {
        @CommandLine.Option(names = "--source-host", required = true, description = "Source database host")
        private String host = "";

        @CommandLine.Option(
                names = "--source-port",
                required = true,
                defaultValue = "1433",
                description = "Source database port")
        private int port;

        @CommandLine.Option(names = "--source-database", required = true, description = "Source database name")
        private String database = "";

        @CommandLine.Option(names = "--source-username", required = true, description = "Source database username")
        private String username = "";

        @CommandLine.ArgGroup(exclusive = true, multiplicity = "1")
        private SourcePasswordOptions password = new SourcePasswordOptions();

        private DatabaseConnection toConnection(final PasswordResolver passwordResolver) {
            return new DatabaseConnection(host, port, database, username, password.resolve(passwordResolver));
        }
    }

    private static final class SourcePasswordOptions {
        @CommandLine.Option(names = "--source-password", description = "Source password")
        private @Nullable String password;

        @CommandLine.Option(
                names = "--source-password-env",
                description = "Environment variable containing source password")
        private @Nullable String passwordEnv;

        @CommandLine.Option(names = "--source-password-stdin", description = "Read source password from stdin")
        private boolean passwordStdin;

        private String resolve(final PasswordResolver passwordResolver) {
            if (null != password) {
                return passwordResolver.fromDirectValue(password);
            }
            if (null != passwordEnv) {
                return passwordResolver.fromEnvironment(passwordEnv);
            }
            if (passwordStdin) {
                return passwordResolver.fromStdin();
            }
            throw new IllegalStateException("No source password source selected");
        }
    }

    @CommandLine.Command(name = "status", description = "Show configured runtime status")
    static final class StatusCommand extends BaseCommand {
        @Override
        public Integer call() {
            runner().status(databaseKey(), driver());
            return 0;
        }
    }

    @CommandLine.Command(name = "create", description = "Create database structures")
    @SuppressWarnings("FieldCanBeFinal")
    static final class CreateCommand extends BaseCommand {
        @CommandLine.Mixin
        private TargetConnectionOptions target = new TargetConnectionOptions();

        @CommandLine.Option(names = "--no-create", description = "Skip dropping and creating the target database")
        private boolean noCreate;

        @Override
        public Integer call() {
            runner().create(databaseKey(), driver(), target.toConnection(passwordResolver()), noCreate);
            return 0;
        }
    }

    @CommandLine.Command(name = "drop", description = "Drop the target database")
    @SuppressWarnings("FieldCanBeFinal")
    static final class DropCommand extends BaseCommand {
        @CommandLine.Mixin
        private TargetConnectionOptions target = new TargetConnectionOptions();

        @Override
        public Integer call() {
            runner().drop(databaseKey(), driver(), target.toConnection(passwordResolver()));
            return 0;
        }
    }

    @CommandLine.Command(name = "migrate", description = "Run migrations")
    @SuppressWarnings("FieldCanBeFinal")
    static final class MigrateCommand extends BaseCommand {
        @CommandLine.Mixin
        private TargetConnectionOptions target = new TargetConnectionOptions();

        @Override
        public Integer call() {
            runner().migrate(databaseKey(), driver(), target.toConnection(passwordResolver()));
            return 0;
        }
    }

    @CommandLine.Command(name = "import", description = "Import data from source to target")
    @SuppressWarnings("FieldCanBeFinal")
    static final class ImportCommand extends BaseCommand {
        @CommandLine.Option(names = "--import", description = "Import key from jdbt.yml")
        private @Nullable String importKey;

        @CommandLine.Option(names = "--module-group", description = "Restrict import to module group")
        private @Nullable String moduleGroup;

        @CommandLine.Option(names = "--resume-at", description = "Resume import at table or sequence")
        private @Nullable String resumeAt;

        @CommandLine.Mixin
        private TargetConnectionOptions target = new TargetConnectionOptions();

        @CommandLine.Mixin
        private SourceConnectionOptions source = new SourceConnectionOptions();

        @Override
        public Integer call() {
            runner().databaseImport(
                            databaseKey(),
                            driver(),
                            importKey,
                            moduleGroup,
                            target.toConnection(passwordResolver()),
                            source.toConnection(passwordResolver()),
                            resumeAt);
            return 0;
        }
    }

    @CommandLine.Command(name = "create-by-import", description = "Create database and import data")
    @SuppressWarnings("FieldCanBeFinal")
    static final class CreateByImportCommand extends BaseCommand {
        @CommandLine.Option(names = "--import", description = "Import key from jdbt.yml")
        private @Nullable String importKey;

        @CommandLine.Option(names = "--resume-at", description = "Resume import at table or sequence")
        private @Nullable String resumeAt;

        @CommandLine.Option(names = "--no-create", description = "Skip dropping and creating the target database")
        private boolean noCreate;

        @CommandLine.Mixin
        private TargetConnectionOptions target = new TargetConnectionOptions();

        @CommandLine.Mixin
        private SourceConnectionOptions source = new SourceConnectionOptions();

        @Override
        public Integer call() {
            runner().createByImport(
                            databaseKey(),
                            driver(),
                            importKey,
                            target.toConnection(passwordResolver()),
                            source.toConnection(passwordResolver()),
                            resumeAt,
                            noCreate);
            return 0;
        }
    }

    @CommandLine.Command(name = "load-dataset", description = "Load fixtures for a dataset")
    @SuppressWarnings("FieldCanBeFinal")
    static final class LoadDatasetCommand extends BaseCommand {
        @CommandLine.Parameters(index = "0", paramLabel = "DATASET", description = "Dataset key")
        private String dataset = "";

        @CommandLine.Mixin
        private TargetConnectionOptions target = new TargetConnectionOptions();

        @Override
        public Integer call() {
            runner().loadDataset(databaseKey(), driver(), dataset, target.toConnection(passwordResolver()));
            return 0;
        }
    }

    @CommandLine.Command(name = "up-module-group", description = "Create objects for module group")
    @SuppressWarnings("FieldCanBeFinal")
    static final class UpModuleGroupCommand extends BaseCommand {
        @CommandLine.Parameters(index = "0", paramLabel = "MODULE_GROUP", description = "Module group key")
        private String moduleGroup = "";

        @CommandLine.Mixin
        private TargetConnectionOptions target = new TargetConnectionOptions();

        @Override
        public Integer call() {
            runner().upModuleGroup(databaseKey(), driver(), moduleGroup, target.toConnection(passwordResolver()));
            return 0;
        }
    }

    @CommandLine.Command(name = "down-module-group", description = "Drop objects for module group")
    @SuppressWarnings("FieldCanBeFinal")
    static final class DownModuleGroupCommand extends BaseCommand {
        @CommandLine.Parameters(index = "0", paramLabel = "MODULE_GROUP", description = "Module group key")
        private String moduleGroup = "";

        @CommandLine.Mixin
        private TargetConnectionOptions target = new TargetConnectionOptions();

        @Override
        public Integer call() {
            runner().downModuleGroup(databaseKey(), driver(), moduleGroup, target.toConnection(passwordResolver()));
            return 0;
        }
    }

    @CommandLine.Command(name = "package-data", description = "Package data resources into a deterministic zip")
    static final class PackageDataCommand extends BaseCommand {
        @CommandLine.Option(names = "--output", required = true, description = "Output zip file")
        private Path outputFile = Path.of("jdbt-data.zip");

        @Override
        public Integer call() {
            runner().packageData(databaseKey(), outputFile);
            return 0;
        }
    }

    @CommandLine.Command(name = "dump-fixtures", description = "Dump fixtures from live database")
    @SuppressWarnings("FieldCanBeFinal")
    static final class DumpFixturesCommand extends BaseCommand {
        @CommandLine.Mixin
        private TargetConnectionOptions target = new TargetConnectionOptions();

        @Override
        public Integer call() {
            runner().dumpFixtures(databaseKey(), driver(), target.toConnection(passwordResolver()));
            return 0;
        }
    }
}
