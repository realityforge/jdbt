load("@rules_java//java:defs.bzl", _java_binary = "java_binary", _java_library = "java_library", _java_test = "java_test")

_JAVA_RELEASE = "17"
_JSPECIFY = "//third_party/java:jspecify"
_NULLAWAY_PLUGIN = "//third_party/java:nullaway_plugin"
_JUNIT_PLATFORM_CONSOLE = "//third_party/java:junit_platform_console_standalone"

_ERROR_PRONE_JAVACOPTS = [
    "-XepExcludedPaths:(.*/" + "external/.*|.*/_javac/.*/.*_sources/.*)",
    "-Xep:AlmostJavadoc:ERROR",
    "-Xep:AlreadyChecked:ERROR",
    "-Xep:AmbiguousMethodReference:ERROR",
    "-Xep:AnnotateFormatMethod:ERROR",
    "-Xep:ArrayAsKeyOfSetOrMap:ERROR",
    "-Xep:AssertEqualsArgumentOrderChecker:ERROR",
    "-Xep:AssertThrowsMultipleStatements:ERROR",
    "-Xep:AttemptedNegativeZero:ERROR",
    "-Xep:BadComparable:ERROR",
    "-Xep:BadImport:ERROR",
    "-Xep:BadInstanceof:ERROR",
    "-Xep:BareDotMetacharacter:ERROR",
    "-Xep:BigDecimalEquals:ERROR",
    "-Xep:BigDecimalLiteralDouble:ERROR",
    "-Xep:BoxedPrimitiveConstructor:ERROR",
    "-Xep:ClassCanBeStatic:ERROR",
    "-Xep:ClassName:ERROR",
    "-Xep:DefaultLocale:ERROR",
    "-Xep:DeprecatedVariable:ERROR",
    "-Xep:DuplicateBranches:ERROR",
    "-Xep:EmptyBlockTag:ERROR",
    "-Xep:EmptyCatch:ERROR",
    "-Xep:EmptyIf:ERROR",
    "-Xep:EmptyTopLevelDeclaration:ERROR",
    "-Xep:EqualsBrokenForNull:ERROR",
    "-Xep:EqualsMissingNullable:ERROR",
    "-Xep:FieldCanBeLocal:ERROR",
    "-Xep:FieldCanBeStatic:ERROR",
    "-Xep:Finalize:ERROR",
    "-Xep:ForEachIterable:ERROR",
    "-Xep:InconsistentHashCode:ERROR",
    "-Xep:LongLiteralLowerCaseSuffix:ERROR",
    "-Xep:MissingBraces:ERROR",
    "-Xep:MissingDefault:ERROR",
    "-Xep:MissingRuntimeRetention:ERROR",
    "-Xep:MixedArrayDimensions:ERROR",
    "-Xep:MultiVariableDeclaration:ERROR",
    "-Xep:MultipleTopLevelClasses:ERROR",
    "-Xep:NonOverridingEquals:ERROR",
    "-Xep:NotJavadoc:ERROR",
    "-Xep:NullOptional:ERROR",
    "-Xep:NullablePrimitive:ERROR",
    "-Xep:NullablePrimitiveArray:ERROR",
    "-Xep:NullableTypeParameter:ERROR",
    "-Xep:NullableWildcard:ERROR",
    "-Xep:PackageLocation:ERROR",
    "-Xep:ParameterMissingNullable:ERROR",
    "-Xep:ParameterName:ERROR",
    "-Xep:PublicApiNamedStreamShouldReturnStream:ERROR",
    "-Xep:RedundantOverride:ERROR",
    "-Xep:RedundantThrows:ERROR",
    "-Xep:RemoveUnusedImports:ERROR",
    "-Xep:ReturnAtTheEndOfVoidFunction:ERROR",
    "-Xep:ReturnFromVoid:ERROR",
    "-Xep:ReturnMissingNullable:ERROR",
    "-Xep:ReturnsNullCollection:ERROR",
    "-Xep:SelfAlwaysReturnsThis:ERROR",
    "-Xep:SunApi:ERROR",
    "-Xep:SystemExitOutsideMain:ERROR",
    "-Xep:ToStringReturnsNull:ERROR",
    "-Xep:UnnecessarilyVisible:ERROR",
    "-Xep:UnnecessaryAnonymousClass:ERROR",
    "-Xep:UnnecessaryBoxedAssignment:ERROR",
    "-Xep:UnnecessaryMethodReference:ERROR",
    "-Xep:UnnecessaryOptionalGet:ERROR",
    "-Xep:UnsynchronizedOverridesSynchronized:ERROR",
    "-Xep:UnusedLabel:ERROR",
    "-Xep:UnusedTypeParameter:ERROR",
    "-Xep:UnusedVariable:ERROR",
    "-Xep:UseCorrectAssertInTests:ERROR",
    "-Xep:UsingJsr305CheckReturnValue:ERROR",
    "-Xep:VoidMissingNullable:ERROR",
    "-Xep:FieldCanBeFinal:ERROR",
    "-Xep:FieldMissingNullable:ERROR",
    "-Xep:PrivateConstructorForUtilityClass:ERROR",
    "-Xep:UnnecessaryDefaultInEnumSwitch:ERROR",
    "-Xep:UnnecessarilyFullyQualified:ERROR",
    "-Xep:Varifier:ERROR",
]

_JAVA_JAVACOPTS = [
    "--release",
    _JAVA_RELEASE,
    "-Werror",
    "-Xep:NullAway:ERROR",
    "-Xep:RequireExplicitNullMarking:ERROR",
    "-XepOpt:NullAway:OnlyNullMarked=true",
    "-Xlint:all,-processing,-serial,-path,-options,-classfile,-this-escape",
] + _ERROR_PRONE_JAVACOPTS

_JAVA_TEST_JVM_FLAGS = [
    "-ea",
]

def _with_jspecify(deps):
    return [_JSPECIFY] + deps if _JSPECIFY not in deps else deps

def _with_nullaway(plugins):
    return [_NULLAWAY_PLUGIN] + plugins if _NULLAWAY_PLUGIN not in plugins else plugins

def _has_sources(srcs):
    return len(srcs) > 0

def java_library(name, srcs = [], javacopts = [], deps = [], plugins = [], **kwargs):
    nullaway_enabled = _has_sources(srcs)
    _java_library(
        name = name,
        srcs = srcs,
        deps = _with_jspecify(deps) if nullaway_enabled else deps,
        javacopts = _JAVA_JAVACOPTS + javacopts,
        plugins = _with_nullaway(plugins) if nullaway_enabled else plugins,
        **kwargs
    )

def java_binary(name, srcs = [], javacopts = [], deps = [], plugins = [], **kwargs):
    nullaway_enabled = _has_sources(srcs)
    _java_binary(
        name = name,
        srcs = srcs,
        deps = _with_jspecify(deps) if nullaway_enabled else deps,
        javacopts = _JAVA_JAVACOPTS + javacopts,
        plugins = _with_nullaway(plugins) if nullaway_enabled else plugins,
        **kwargs
    )

def java_test(name, srcs = [], javacopts = [], deps = [], plugins = [], jvm_flags = [], **kwargs):
    nullaway_enabled = _has_sources(srcs)
    _java_test(
        name = name,
        srcs = srcs,
        deps = _with_jspecify(deps) if nullaway_enabled else deps,
        javacopts = _JAVA_JAVACOPTS + javacopts,
        plugins = _with_nullaway(plugins) if nullaway_enabled else plugins,
        jvm_flags = _JAVA_TEST_JVM_FLAGS + jvm_flags,
        **kwargs
    )

def junit5_test(name, srcs = [], test_class = None, test_package = None, args = [], deps = [], runtime_deps = [], **kwargs):
    if test_class and test_package:
        fail("Specify only one of test_class or test_package")
    if not test_class and not test_package:
        fail("Specify test_class or test_package")

    selectors = ["--select-class=" + test_class] if test_class else ["--select-package=" + test_package]
    java_test(
        name = name,
        srcs = srcs,
        main_class = "org.junit.platform.console.ConsoleLauncher",
        use_testrunner = False,
        args = selectors + ["--fail-if-no-tests"] + args,
        deps = deps + [_JUNIT_PLATFORM_CONSOLE],
        runtime_deps = runtime_deps,
        **kwargs
    )
