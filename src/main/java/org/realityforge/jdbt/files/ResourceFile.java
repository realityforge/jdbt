package org.realityforge.jdbt.files;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

public sealed interface ResourceFile permits ResourceFile.OnDisk, ResourceFile.InArtifact {
    String path();

    String sourceName();

    String identity();

    String readText();

    default String basename() {
        final var path = path();
        final var slash = path.lastIndexOf('/');
        return -1 == slash ? path : path.substring(slash + 1);
    }

    record OnDisk(Path resourceRoot, Path file) implements ResourceFile {
        public OnDisk {
            resourceRoot = resourceRoot.toAbsolutePath().normalize();
            file = file.toAbsolutePath().normalize();
            if (!file.startsWith(resourceRoot)) {
                throw new FileCollectionException("Resolved resource is outside resourceRoot: " + file);
            }
        }

        @Override
        public String path() {
            return resourceRoot.relativize(file).toString().replace('\\', '/');
        }

        @Override
        public String sourceName() {
            return file.toString();
        }

        @Override
        public String identity() {
            return path();
        }

        @Override
        public String readText() {
            try {
                return Files.readString(file, StandardCharsets.UTF_8);
            } catch (final IOException ioe) {
                throw new UncheckedIOException("Failed to read file " + file, ioe);
            }
        }
    }

    record InArtifact(ArtifactContent artifact, String path) implements ResourceFile {
        public InArtifact {
            path = path.replace('\\', '/');
        }

        @Override
        public String sourceName() {
            return artifact.id() + ':' + path;
        }

        @Override
        public String identity() {
            return "zip:" + sourceName();
        }

        @Override
        public String readText() {
            return artifact.readText(path);
        }
    }
}
