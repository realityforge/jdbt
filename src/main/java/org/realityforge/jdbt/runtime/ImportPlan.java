package org.realityforge.jdbt.runtime;

import java.util.List;
import org.jspecify.annotations.Nullable;
import org.realityforge.jdbt.config.ImportConfig;
import org.realityforge.jdbt.files.ResourceFile;

record ImportPlan(ImportConfig config, boolean fullImport, List<Module> modules) {
    ImportPlan {
        modules = List.copyOf(modules);
    }

    record Module(String name, List<Element> tables, List<Element> sequences) {
        Module {
            tables = List.copyOf(tables);
            sequences = List.copyOf(sequences);
        }
    }

    record Element(
            String name,
            @Nullable ResourceFile fixture,
            @Nullable ResourceFile sql) {}
}
