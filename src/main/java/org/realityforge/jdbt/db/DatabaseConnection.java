package org.realityforge.jdbt.db;

public record DatabaseConnection(String host, int port, String database, String username, String password) {}
