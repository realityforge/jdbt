package org.realityforge.jdbt.runtime;

import java.util.ArrayList;
import java.util.List;
import org.jspecify.annotations.Nullable;
import org.realityforge.jdbt.config.ImportConfig;
import org.realityforge.jdbt.files.ResourceFile;

record ImportPlan(ImportConfig config, boolean fullImport, List<Module> modules) {
    ImportPlan {
        modules = List.copyOf(modules);
    }

    boolean isLateTable(final String cleanName) {
        return modules.stream()
                .flatMap(module -> module.lateTables().stream())
                .anyMatch(element -> cleanName.equals(RuntimeEngine.cleanObjectName(element.name())));
    }

    record Module(String name, List<Element> tables, List<Element> lateTables, List<Element> sequences) {
        Module {
            tables = List.copyOf(tables);
            lateTables = List.copyOf(lateTables);
            sequences = List.copyOf(sequences);
        }

        List<Element> allTables() {
            final var elements = new ArrayList<>(tables);
            elements.addAll(lateTables);
            return List.copyOf(elements);
        }
    }

    record Element(
            String name,
            @Nullable ResourceFile fixture,
            @Nullable ResourceFile sql) {}
}
