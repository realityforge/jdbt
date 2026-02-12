package org.realityforge.jdbt.packaging;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.zip.CRC32;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

public final class DeterministicZipPackager {
    private static final long FIXED_ENTRY_TIME_MILLIS = 0L;

    public void write(final Path sourceDirectory, final Path zipFile) {
        final var files = collectFiles(sourceDirectory);
        createParentDirectory(zipFile);
        try (var output = Files.newOutputStream(zipFile);
                var zip = new ZipOutputStream(output)) {
            for (final var file : files) {
                writeEntry(sourceDirectory, file, zip);
            }
        } catch (final IOException ioe) {
            throw new UncheckedIOException("Failed to create zip " + zipFile, ioe);
        }
    }

    private static List<Path> collectFiles(final Path sourceDirectory) {
        if (!Files.isDirectory(sourceDirectory)) {
            return List.of();
        }
        try (var stream = Files.walk(sourceDirectory)) {
            final var files = stream.filter(Files::isRegularFile)
                    .sorted(Comparator.comparing(path -> entryName(sourceDirectory, path)))
                    .toList();
            return new ArrayList<>(files);
        } catch (final IOException ioe) {
            throw new UncheckedIOException("Failed to list files in " + sourceDirectory, ioe);
        }
    }

    private static void createParentDirectory(final Path zipFile) {
        final var parent = zipFile.getParent();
        if (null == parent) {
            return;
        }
        try {
            Files.createDirectories(parent);
        } catch (final IOException ioe) {
            throw new UncheckedIOException("Failed to create directory " + parent, ioe);
        }
    }

    private static void writeEntry(final Path sourceDirectory, final Path file, final ZipOutputStream zip)
            throws IOException {
        final var entryName = entryName(sourceDirectory, file);
        final var content = Files.readAllBytes(file);
        final var crc = new CRC32();
        crc.update(content);

        final var entry = new ZipEntry(entryName);
        entry.setMethod(ZipEntry.STORED);
        entry.setSize(content.length);
        entry.setCompressedSize(content.length);
        entry.setCrc(crc.getValue());
        entry.setTime(FIXED_ENTRY_TIME_MILLIS);
        zip.putNextEntry(entry);
        zip.write(content);
        zip.closeEntry();
    }

    private static String entryName(final Path sourceDirectory, final Path file) {
        return sourceDirectory.relativize(file).toString().replace('\\', '/');
    }
}
