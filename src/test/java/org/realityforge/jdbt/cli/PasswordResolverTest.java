package org.realityforge.jdbt.cli;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import org.junit.jupiter.api.Test;
import org.realityforge.jdbt.config.ConfigException;

final class PasswordResolverTest {
    @Test
    void resolvesDirectValue() {
        final PasswordResolver resolver =
                new PasswordResolver(java.util.Map.of(), new ByteArrayInputStream(new byte[0]));
        assertThat(resolver.fromDirectValue("secret")).isEqualTo("secret");
    }

    @Test
    void resolvesEnvironmentValue() {
        final PasswordResolver resolver =
                new PasswordResolver(java.util.Map.of("DB_PASS", "secret"), new ByteArrayInputStream(new byte[0]));
        assertThat(resolver.fromEnvironment("DB_PASS")).isEqualTo("secret");
    }

    @Test
    void environmentValueMustExist() {
        final PasswordResolver resolver =
                new PasswordResolver(java.util.Map.of(), new ByteArrayInputStream(new byte[0]));
        assertThatThrownBy(() -> resolver.fromEnvironment("DB_PASS"))
                .isInstanceOf(ConfigException.class)
                .hasMessageContaining("DB_PASS");
    }

    @Test
    void resolvesPasswordFromStdinLineByLine() {
        final PasswordResolver resolver = new PasswordResolver(
                java.util.Map.of(), new ByteArrayInputStream("first\nsecond\n".getBytes(StandardCharsets.UTF_8)));
        assertThat(resolver.fromStdin()).isEqualTo("first");
        assertThat(resolver.fromStdin()).isEqualTo("second");
    }

    @Test
    void stdinPasswordMustExist() {
        final PasswordResolver resolver =
                new PasswordResolver(java.util.Map.of(), new ByteArrayInputStream(new byte[0]));
        assertThatThrownBy(resolver::fromStdin)
                .isInstanceOf(ConfigException.class)
                .hasMessageContaining("stdin");
    }
}
