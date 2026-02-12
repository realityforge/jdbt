package org.realityforge.jdbt.db;

import org.realityforge.jdbt.config.ConfigException;

public class DbDriverFactory {
    public DbDriver create(final String driver) {
        if ("sqlserver".equalsIgnoreCase(driver)) {
            return new SqlServerDbDriver();
        }
        if ("postgres".equalsIgnoreCase(driver)) {
            return new PostgresDbDriver();
        }
        if ("noop".equalsIgnoreCase(driver)) {
            return new NoOpDbDriver();
        }
        throw new ConfigException("Unsupported database driver '" + driver + "'. Supported: sqlserver, postgres, noop");
    }
}
