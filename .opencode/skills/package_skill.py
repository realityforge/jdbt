#!/usr/bin/env python3
"""Package an Agent Skill directory into a .skill archive."""

from __future__ import annotations

import argparse
import pathlib
import zipfile


FIXED_TIMESTAMP = (1980, 1, 1, 0, 0, 0)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Package a skill directory into a .skill archive"
    )
    parser.add_argument(
        "skill_dir", help="Path to a skill directory that contains SKILL.md"
    )
    parser.add_argument(
        "--output",
        help="Output .skill file path. Defaults to ./build/skills/<skill-name>.skill",
    )
    return parser.parse_args()


def normalize_path(path: pathlib.Path) -> str:
    return str(path).replace("\\", "/")


def collect_files(root: pathlib.Path) -> list[pathlib.Path]:
    files = [p for p in root.rglob("*") if p.is_file()]
    files = [
        p for p in files if ".DS_Store" not in p.name and not p.name.endswith(".skill")
    ]
    files.sort(key=lambda p: normalize_path(p.relative_to(root)))
    return files


def write_archive(skill_dir: pathlib.Path, output: pathlib.Path) -> None:
    skill_name = skill_dir.name
    files = collect_files(skill_dir)
    output.parent.mkdir(parents=True, exist_ok=True)

    with zipfile.ZipFile(output, mode="w") as archive:
        for source in files:
            relative = source.relative_to(skill_dir)
            archived = pathlib.Path(skill_name) / relative
            zip_info = zipfile.ZipInfo(
                filename=normalize_path(archived), date_time=FIXED_TIMESTAMP
            )
            zip_info.compress_type = zipfile.ZIP_DEFLATED
            zip_info.external_attr = 0o644 << 16
            archive.writestr(zip_info, source.read_bytes())


def main() -> int:
    args = parse_args()
    skill_dir = pathlib.Path(args.skill_dir).resolve()
    if not skill_dir.is_dir():
        raise SystemExit(f"Skill directory does not exist: {skill_dir}")
    if not (skill_dir / "SKILL.md").exists():
        raise SystemExit(f"Expected SKILL.md in skill directory: {skill_dir}")

    output = (
        pathlib.Path(args.output).resolve()
        if args.output
        else pathlib.Path("build/skills").resolve() / f"{skill_dir.name}.skill"
    )
    if output.suffix != ".skill":
        raise SystemExit(f"Output path must end with .skill: {output}")

    write_archive(skill_dir, output)
    print(output)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
