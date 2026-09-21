load("@rules_java//java:defs.bzl", "JavaInfo")

PalantirJavaFormatInfo = provider(
    "Aggregated Java formatting sources and check outputs.",
    fields = {
        "check_outputs": "Formatting check outputs for this target and traversed dependencies.",
        "sources": "Java sources owned by this target and traversed dependencies.",
    },
)

_TRAVERSED_ATTRIBUTES = [
    "deps",
    "runtime_deps",
    "exports",
    "tests",
]

def _dependency_info(ctx):
    sources = []
    check_outputs = []
    for attribute_name in _TRAVERSED_ATTRIBUTES:
        if not hasattr(ctx.rule.attr, attribute_name):
            continue
        for dependency in getattr(ctx.rule.attr, attribute_name):
            if PalantirJavaFormatInfo in dependency:
                info = dependency[PalantirJavaFormatInfo]
                sources.append(info.sources)
                check_outputs.append(info.check_outputs)
    return sources, check_outputs

def _format_aspect_impl(target, ctx):
    transitive_sources, transitive_outputs = _dependency_info(ctx)
    direct_sources = []
    direct_outputs = []
    if JavaInfo in target and ctx.label.repo_name == "" and hasattr(ctx.rule.files, "srcs"):
        direct_sources = [
            source
            for source in ctx.rule.files.srcs
            if source.is_source and source.basename.endswith(".java")
        ]
    if direct_sources:
        marker = ctx.actions.declare_file(ctx.label.name + ".palantir-java-format")
        arguments = ctx.actions.args()
        arguments.add("--mode=check")
        arguments.add("--marker=" + marker.path)
        arguments.add_all(direct_sources)
        arguments.set_param_file_format("multiline")
        arguments.use_param_file("@%s", use_always = True)
        ctx.actions.run(
            arguments = [arguments],
            executable = ctx.executable._worker,
            execution_requirements = {
                "requires-worker-protocol": "proto",
                "supports-workers": "1",
            },
            inputs = direct_sources,
            mnemonic = "PalantirJavaFormat",
            outputs = [marker],
            progress_message = "Checking Java format for %{label}",
        )
        direct_outputs = [marker]
    return [
        PalantirJavaFormatInfo(
            check_outputs = depset(direct = direct_outputs, transitive = transitive_outputs),
            sources = depset(direct = direct_sources, transitive = transitive_sources),
        ),
    ]

palantir_java_format_aspect = aspect(
    implementation = _format_aspect_impl,
    attr_aspects = _TRAVERSED_ATTRIBUTES,
    attrs = {
        "_worker": attr.label(
            default = "//tools/java-format:palantir_java_format_worker",
            cfg = "exec",
            executable = True,
        ),
    },
)

def _java_format_check_impl(ctx):
    return [
        DefaultInfo(files = depset(transitive = [
            target[PalantirJavaFormatInfo].check_outputs
            for target in ctx.attr.targets
        ])),
    ]

java_format_check = rule(
    implementation = _java_format_check_impl,
    attrs = {
        "targets": attr.label_list(aspects = [palantir_java_format_aspect]),
    },
)
