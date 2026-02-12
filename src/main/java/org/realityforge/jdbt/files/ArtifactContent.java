package org.realityforge.jdbt.files;

import java.util.List;

public interface ArtifactContent {
    String id();

    List<String> files();

    String readText(String path);
}
