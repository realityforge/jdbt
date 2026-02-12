package org.realityforge.jdbt.cli;

import java.util.concurrent.Callable;
import picocli.CommandLine;

@CommandLine.Command(name = "jdbt", mixinStandardHelpOptions = true, description = "Java database tooling runtime")
public final class JdbtCommand implements Callable<Integer> {
    static final int USAGE_EXIT_CODE = 2;

    private JdbtCommand() {}

    public static int execute(final String[] args) {
        return new CommandLine(new JdbtCommand()).execute(args);
    }

    @Override
    public Integer call() {
        return USAGE_EXIT_CODE;
    }
}
