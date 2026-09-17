# Create-by-import establishes contributions before late imports

Jdbt exposes create-by-import as its sole source-to-target import lifecycle. Database Contributions execute after
ordinary data establishment and before optional pre-late hooks and Late Imports, allowing independently selected
deployment-owned data to exist before dependent bulk transfers without treating it as schema finalization.

Late-table recovery resumes at the named frontier and does not replay contributions or pre-late hooks. A valid resume
therefore relies on the operator preserving the source, target, artifacts, configuration, filters, and successfully
completed prefix; persisting and proving arbitrary checkpoints would add a second lifecycle-state model without making
ordinary resume intrinsically safe.

Migration does not execute contributions. Late tables remain a suffix of each selected Database Module's import order,
and `requiredFiles` continues to reject incomplete project-specific import plans before mutation.
