package org.realityforge.jdbt.cli;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import org.realityforge.jdbt.config.ConfigException;

final class PasswordResolver {
    private final Map<String, String> environment;
    private final BufferedReader stdin;

    PasswordResolver(final Map<String, String> environment, final InputStream stdin) {
        this.environment = Map.copyOf(environment);
        this.stdin = new BufferedReader(new InputStreamReader(stdin, StandardCharsets.UTF_8));
    }

    String fromDirectValue(final String value) {
        return value;
    }

    String fromEnvironment(final String variableName) {
        final var value = environment.get(variableName);
        if (null == value) {
            throw new ConfigException("No value found in environment variable '" + variableName + "'.");
        }
        return value;
    }

    String fromStdin() {
        try {
            final var value = stdin.readLine();
            if (null == value) {
                throw new ConfigException("No password available on stdin.");
            }
            return value;
        } catch (final IOException ioe) {
            throw new RuntimeException("Failed to read password from stdin", ioe);
        }
    }
}
