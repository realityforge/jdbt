package org.realityforge.jdbt.files;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

public final class ZipArtifactContent implements ArtifactContent {
    private final String id;
    private final Map<String, byte[]> entries;

    public ZipArtifactContent(final Path zipPath) {
        this(zipPath.toString(), zipPath, "data");
    }

    public ZipArtifactContent(final String id, final Path zipPath, final String dataPrefix) {
        this.id = id;
        this.entries = loadEntries(zipPath, dataPrefix);
    }

    @Override
    public String id() {
        return id;
    }

    @Override
    public List<String> files() {
        return List.copyOf(new ArrayList<>(entries.keySet()));
    }

    @Override
    public String readText(final String path) {
        final byte[] data = entries.get(path);
        if (null == data) {
            throw new FileCollectionException("Missing artifact path '" + path + "' in artifact '" + id + "'.");
        }
        return new String(data, StandardCharsets.UTF_8);
    }

    private static Map<String, byte[]> loadEntries(final Path zipPath, final String dataPrefix) {
        final String prefix = dataPrefix + '/';
        final Map<String, byte[]> entries = new LinkedHashMap<>();
        try (ZipFile zipFile = new ZipFile(zipPath.toFile())) {
            final List<? extends ZipEntry> zipEntries = java.util.Collections.list(zipFile.entries());
            for (final ZipEntry entry : zipEntries) {
                if (entry.isDirectory() || !entry.getName().startsWith(prefix)) {
                    continue;
                }
                final String relativeName = entry.getName().substring(prefix.length());
                try (var input = zipFile.getInputStream(entry)) {
                    entries.put(relativeName, input.readAllBytes());
                }
            }
        } catch (final IOException ioe) {
            throw new UncheckedIOException("Failed reading zip artifact: " + zipPath, ioe);
        }
        return entries;
    }
}
