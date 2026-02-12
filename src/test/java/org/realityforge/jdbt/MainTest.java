package org.realityforge.jdbt;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

final class MainTest {
    @Test
    void runReturnsSuccessForHelp() {
        assertThat(Main.run(new String[] {"--help"})).isZero();
    }

    @Test
    void runWithoutArgsBehavesLikeHelp() {
        assertThat(Main.run(new String[0])).isZero();
    }
}
