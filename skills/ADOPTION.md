# Skill Adoption Guide

This repository includes a reusable Agent Skill at:

- `skills/structured-delivery-workflow/`

The skill is designed to be portable across repositories and agent tools that support `SKILL.md`.

## Option 1: Copy as a Folder

Copy the entire skill folder into your target repository's skills location.

Minimum required content is already present:

- `SKILL.md`
- `references/` templates

## Option 2: Build a `.skill` Archive

Use the packager script:

```bash
python3 skills/package_skill.py skills/structured-delivery-workflow
```

Default output:

- `build/skills/structured-delivery-workflow.skill`

Custom output:

```bash
python3 skills/package_skill.py skills/structured-delivery-workflow \
  --output ./dist/structured-delivery-workflow.skill
```

The archive is deterministic (stable file ordering + fixed timestamps) for reproducible distribution.

## Verify Archive Contents

```bash
unzip -l build/skills/structured-delivery-workflow.skill
```

You should see entries rooted under:

- `structured-delivery-workflow/`

## Recommended Reuse Pattern

1. Start with this skill unchanged in a new repository.
2. Tailor template files in `references/` to project conventions.
3. Keep planning artifact paths stable across projects to reduce process drift.
