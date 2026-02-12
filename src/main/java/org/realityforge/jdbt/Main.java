package org.realityforge.jdbt;

import org.realityforge.jdbt.cli.JdbtCommand;

public final class Main {
    private Main() {}

    public static void main(final String[] args) {
        System.exit(run(args));
    }

    public static int run(final String[] args) {
        return JdbtCommand.execute(args);
    }
}
