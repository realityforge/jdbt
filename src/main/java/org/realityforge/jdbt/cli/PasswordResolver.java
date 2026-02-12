package org.realityforge.jdbt.cli;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import org.realityforge.jdbt.config.ConfigException;

public final class PasswordResolver {
    private final Map<String, String> environment;
    private final BufferedReader stdin;

    public PasswordResolver(final Map<String, String> environment, final InputStream stdin) {
        this.environment = Map.copyOf(environment);
        this.stdin = new BufferedReader(new InputStreamReader(stdin, StandardCharsets.UTF_8));
    }

    public String fromDirectValue(final String value) {
        return value;
    }

    public String fromEnvironment(final String variableName) {
        final String value = environment.get(variableName);
        if (null == value) {
            throw new ConfigException("No value found in environment variable '" + variableName + "'.");
        }
        return value;
    }

    public String fromStdin() {
        try {
            final String value = stdin.readLine();
            if (null == value) {
                throw new ConfigException("No password available on stdin.");
            }
            return value;
        } catch (final IOException ioe) {
            throw new RuntimeException("Failed to read password from stdin", ioe);
        }
    }
}
