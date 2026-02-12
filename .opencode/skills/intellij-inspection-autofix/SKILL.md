---
name: intellij-inspection-autofix
description: Fix or suppress IntelliJ inspection XML violations from a reports directory and print a suppression summary
metadata:
  short-description: IntelliJ inspection report fixer/suppressor
---
# Skill: Auto-fix IntelliJ Inspection XML Reports

## Goal
Given a directory containing IntelliJ IDEA inspection results exported as XML:

- Read `.descriptions.xml` to understand each inspection (name, severity, description, and which report files contain its violations).
- For each violation in each report XML:
  1) **Fix it** in the codebase if the violation is **relevant and safe** to fix.
  2) If it is **not relevant**, **suppress** it locally in the source file using an IntelliJ inspection suppression comment (language-appropriate).
- At the end, output a **suppression report** listing every suppression added, including rationale.

## Inputs
- `reports_dir/` — directory containing:
  - `.descriptions.xml` (index of inspections and descriptions)
  - One or more `*.xml` inspection result files (each with `<problems><problem>...</problem></problems>`)
- A working copy of the codebase corresponding to the report paths.

## Output
- Modified source code with fixes and/or suppressions applied.
- A final **Suppression Summary** section printed to the console/log, listing suppressions added this run.

---

## How to Interpret the Report Files

### `.descriptions.xml`
Parse each `<inspection>` element:
- `shortName` → **Inspection ID** (used for suppression markers and to match problem_class id)
- `defaultSeverity`
- `displayName`
- Embedded HTML description (helps decide whether issues are advisory vs high-priority)
- Links (if present) to one or more report files containing violations of that inspection

### `*.xml` report files
Each `<problem>` typically contains:
- `<file>`: a URL-style path, often `file://$PROJECT_DIR$/...`
- `<line>`: line number (1-based)
- `<problem_class id="...">`: inspection ID and severity
- `<description>`: violation summary
- `<highlighted_element>`: the symbol/fragment flagged (may be empty or partial)
- `<offset>` and `<length>`: character offsets (may help pinpoint in-file location)

### Mapping report paths to repo paths
- If `<file>` begins with `file://$PROJECT_DIR$/`, replace `$PROJECT_DIR$` with the repository root.
- Otherwise parse as a file URI and map to a local path.
- If the file does not exist locally, record the violation as **unactionable** and include it in the end report (separate from suppressions).

---

## Core Workflow

### 1) Enumerate inspections and reports
1. Load `.descriptions.xml`
2. Build a map:
   - `inspectionId -> {severity, displayName, descriptionHtml, linkedReportFiles[]}`
3. Identify report XML files in the directory (exclude `.descriptions.xml`), including those referenced by `.descriptions.xml`.

### 2) Process violations in a stable order
For determinism:
- Sort report files by name.
- Within each report file, sort problems by:
  - file path
  - line number
  - inspection id

### 3) For each violation: decide Fix vs Suppress vs Ask
For each `<problem>`:

#### A. Determine relevance
Many inspections are **advisory**: they may not be worth changing if they reflect intentional style, legacy constraints, or domain-specific behavior.

**Relevance heuristic:**
- Prefer fixing when it affects:
  - correctness (bugs, null-safety, resource leaks, concurrency, security)
  - clear maintainability wins with minimal risk (dead code removal, obvious simplification)
- Prefer suppressing when:
  - changing behavior is risky or unclear
  - the project intentionally deviates from the inspection’s advice
  - the “fix” would be large, invasive, or requires design decisions
  - the inspection is primarily stylistic and conflicts with local conventions

Use the inspection description (from `.descriptions.xml`) + the specific `<description>` text to guide the call.

#### B. Attempt an “obvious + safe” fix first
A fix is “obvious + safe” if:
- It can be applied locally (single file / small diff)
- It does not change runtime behavior unexpectedly
- It compiles and tests plausibly still pass (run unit tests if available)

If a fix is possible, implement it and re-check that:
- The violation is resolved (best-effort: ensure the flagged code path changed appropriately)
- No new compilation errors are introduced

#### C. If not obvious, ask the user (one question at a time)
If either is true:
- There is **no obvious fix**, or
- The fix has **significant side-effects** (API changes, behavior changes, broad refactors),

then ask **one targeted question**, offering 2–3 concrete options.

**Question format:**
- Provide: file, line, inspection ID, short description, why it matters, options.
- Ask *one* decision at a time. Do not batch questions.

Example:
- “This inspection suggests X, but fixing it would require choosing between A/B. Which do you prefer?”

Proceed after user guidance.

#### D. If deemed not relevant: suppress locally
If the violation is not relevant (or the user chooses not to fix it), add a suppression comment near the smallest possible scope.

---

## Suppression Rules

### General principles
- Suppress as narrowly as possible:
  - statement-level > method-level > class-level > file-level
- Prefer comments over annotations unless the language makes comment-based suppression impossible at the correct scope.
- Every suppression must include a short reason so future maintainers know why it exists.

### Preferred suppression forms (best-effort)

#### Java / Kotlin / JVM languages with `//` comments
Place immediately above the flagged statement/line:
```java
//noinspection <InspectionId>  // reason: <short rationale>
```

If suppression must apply to a larger block, use the narrowest containing element and keep the reason.

#### Python
```python
# noinspection <InspectionId>  # reason: <short rationale>
```

#### Other languages
Default to IntelliJ’s canonical marker using that language’s comment syntax:
- `//noinspection <InspectionId>` for `//` languages
- `/* noinspection <InspectionId> */` for C-style block comments if needed
- `<!-- noinspection <InspectionId> -->` for XML/HTML-like files if line comments aren’t valid

If the tool cannot confidently determine correct syntax for the language/file type, ask the user **one question** with a best guess and an alternative.

### Mandatory suppression tracking
Every time a suppression is added, append a record to an in-memory list:

- `inspectionId`
- `filePath`
- `line`
- `description` (from `<description>`)
- `suppressionText` (exact comment inserted)
- `reason` (why not fixed)

---

## Fixing Strategy Guidelines

### “Fix” should be minimal and localized
- Make the smallest change that resolves the violation.
- Avoid broad refactors unless the violation is widespread and the fix is clearly mechanical.

### Validate after changes
- If tests are available, run the fastest relevant suite.
- At minimum, ensure code compiles/lints where applicable.
- If validation isn’t possible, state that explicitly in the final run summary.

### Examples: advisory vs must-fix
- “Possible bug” inspections: usually worth fixing unless there’s a domain reason.
- “Style / formatting / naming” inspections: often advisory; suppress if changing would churn code or conflict with conventions.

---

## End-of-Run Reporting

### Suppression Summary (required)
At the end of the run, print:

1) Total suppressions added
2) Grouped by inspection ID, then file

Include for each suppression:
- `InspectionId`
- `File:Line`
- Short reason (1 sentence)
- The exact suppression comment used

Example:
```
Suppression Summary
- Total suppressions added: 3

NullableProblems (2)
  - src/main/java/.../AnkiService.java:11
    reason: parameter nullability is inherited from external API; annotating would mislead callers
    added: //noinspection NullableProblems  // reason: inherited external nullability contract
  - ...

SomeOtherInspection (1)
  - ...
```

### Optional: Unactionable items
If any report paths didn’t map to files or the location couldn’t be found, list them separately as “Unactionable”.

---

## Safety and Interaction Policy
- Do not make sweeping changes without user confirmation.
- If a fix might break public APIs, serialization formats, behavior, or performance, ask the user before proceeding (one question at a time).
- If uncertain whether the inspection is correct, prefer asking the user or suppressing with a reason rather than guessing.

---

## Completion Criteria
This skill run is complete when:
- Every violation in every referenced report has been:
  - fixed, or
  - suppressed with a comment + reason, or
  - marked unactionable with an explanation
- The **Suppression Summary** has been printed.
