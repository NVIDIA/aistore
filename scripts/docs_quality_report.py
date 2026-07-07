#!/usr/bin/env python3
"""Generate a report-only documentation quality snapshot.

This first DQA slice is intentionally non-gating: findings are written to
Markdown and JSON so maintainers can inspect link, frontmatter, template,
style, and drift surfaces before individual checks are promoted to failures.
"""

import argparse
import json
import re
import shutil
import subprocess
import sys
from pathlib import Path
from typing import NamedTuple, Optional
from urllib.parse import unquote, urlsplit

MARKDOWN_SUFFIXES = {".md", ".mdx"}
REPORT_LIMIT = 250

LINK_RE = re.compile(r"(?<!!)\[[^\]]+\]\(([^)\s]+)(?:\s+\"[^\"]*\")?\)")
IMAGE_RE = re.compile(r"!\[[^\]]*\]\(([^)\s]+)(?:\s+\"[^\"]*\")?\)")
HTML_REF_RE = re.compile(r"""(?:href|src)=["']([^"']+)["']""")

TEMPLATE_MARKERS = (
    "AIStore-gap",
    "TODO",
    "FIXME",
    "TBD",
    "template-extension",
    "reader-visible provenance",
    "migration scaffold",
)

FUTURE_CHECKS = (
    "Validate anchors after Fern page generation.",
    "Check external URLs with retry and allowlist support.",
    "Diff generated API/reference docs against source changes.",
    "Promote low-noise changed-file findings to blocking checks.",
)


class Finding(NamedTuple):
    category: str
    path: str
    detail: str
    line: Optional[int] = None


def repo_root() -> Path:
    git_bin = shutil.which("git")
    if git_bin is None:
        raise RuntimeError("git executable not found in PATH")
    try:
        root = subprocess.check_output(
            [git_bin, "rev-parse", "--show-toplevel"],
            text=True,
            timeout=10,
        ).strip()
    except (
        FileNotFoundError,
        subprocess.CalledProcessError,
        subprocess.TimeoutExpired,
    ) as exc:
        raise RuntimeError("failed to locate repository root with git") from exc
    return Path(root)


def rel(path: Path, root: Path) -> str:
    return path.relative_to(root).as_posix()


def read_text(path: Path) -> str:
    try:
        return path.read_text(encoding="utf-8")
    except UnicodeDecodeError as exc:
        raise RuntimeError(f"{path}: not UTF-8 text") from exc


def markdown_sources(root: Path) -> list[Path]:
    source_roots = [
        root / "README.md",
        root / "CONTRIBUTING.md",
        root / "docs",
        root / "python" / "README.md",
    ]
    files: list[Path] = []
    for source in source_roots:
        if source.is_file() and source.suffix in MARKDOWN_SUFFIXES:
            files.append(source)
        elif source.is_dir():
            files.extend(
                path
                for path in source.rglob("*")
                if path.is_file() and path.suffix in MARKDOWN_SUFFIXES
            )
    return sorted(set(files))


def line_number(text: str, index: int) -> int:
    return text.count("\n", 0, index) + 1


def has_frontmatter(text: str) -> bool:
    return text.startswith("---\n") and "\n---\n" in text[4:]


def frontmatter_lines(text: str) -> list[str]:
    if not has_frontmatter(text):
        return []
    return text[4:].split("\n---\n", 1)[0].splitlines()


def frontmatter_keys(text: str) -> set[str]:
    keys: set[str] = set()
    for line in frontmatter_lines(text):
        if not line or line.startswith((" ", "\t", "#", "-")) or ":" not in line:
            continue
        keys.add(line.split(":", 1)[0].strip())
    return keys


def frontmatter_value(text: str, key: str) -> str:
    for line in frontmatter_lines(text):
        if line.startswith(f"{key}:"):
            return line.split(":", 1)[1].strip().strip("\"'")
    return ""


def strip_anchor_and_query(target: str) -> str:
    split = urlsplit(target)
    return unquote(split.path)


def skip_link(target: str) -> bool:
    if not target or target.startswith(("#", "mailto:", "tel:", "data:")):
        return True
    if target.startswith(("http://", "https://")):
        return True
    if target.startswith(("{{", "{%", "<")):
        return True
    return False


def candidate_paths(root: Path, source: Path, target: str) -> list[Path]:
    clean = strip_anchor_and_query(target)
    if not clean:
        return []
    if clean.startswith("/docs/"):
        base = root / clean.lstrip("/")
    elif clean.startswith(("/assets/", "/images/")):
        base = root / "docs" / clean.lstrip("/")
    elif clean.startswith("/blog/"):
        return blog_candidate_paths(root, clean)
    elif clean.startswith("/"):
        base = root / clean.lstrip("/")
    else:
        base = source.parent / clean

    candidates = [base]
    if base.suffix == "":
        candidates.extend(
            [
                base.with_suffix(".md"),
                base.with_suffix(".mdx"),
                base / "index.md",
                base / "index.mdx",
            ]
        )
    return candidates


def blog_candidate_paths(root: Path, clean: str) -> list[Path]:
    slug = clean.rstrip("/").split("/")[-1]
    if not slug:
        return []
    return sorted((root / "docs" / "_posts").glob(f"????-??-??-{slug}.md"))


def is_within_root(path: Path, resolved_root: Path) -> bool:
    resolved_path = path.resolve()
    return resolved_path == resolved_root or resolved_root in resolved_path.parents


def local_link_findings(root: Path, files: list[Path]) -> list[Finding]:
    findings: list[Finding] = []
    resolved_root = root.resolve()
    for path in files:
        text = read_text(path)
        matches = []
        matches.extend(LINK_RE.finditer(text))
        matches.extend(IMAGE_RE.finditer(text))
        matches.extend(HTML_REF_RE.finditer(text))
        seen: set[tuple[str, int]] = set()
        for match in sorted(matches, key=lambda item: item.start()):
            target = match.group(1)
            if skip_link(target):
                continue
            line = line_number(text, match.start())
            key = (target, line)
            if key in seen:
                continue
            seen.add(key)
            candidates = candidate_paths(root, path, target)
            outside_candidates = [
                candidate
                for candidate in candidates
                if not is_within_root(candidate, resolved_root)
            ]
            if outside_candidates:
                findings.append(
                    Finding(
                        category="link.outside-repo-target",
                        path=rel(path, root),
                        line=line,
                        detail=target,
                    )
                )
                continue
            if candidates and not any(candidate.exists() for candidate in candidates):
                findings.append(
                    Finding(
                        category="link.missing-local-target",
                        path=rel(path, root),
                        line=line,
                        detail=target,
                    )
                )
    return findings


def frontmatter_findings(root: Path, files: list[Path]) -> list[Finding]:
    findings: list[Finding] = []
    for path in files:
        text = read_text(path)
        path_rel = rel(path, root)
        if path_rel == "docs/_posts/README.md":
            continue
        keys = frontmatter_keys(text)
        if "/_posts/" in path_rel:
            if not has_frontmatter(text):
                findings.append(
                    Finding(
                        "frontmatter.missing",
                        path_rel,
                        "blog post has no frontmatter",
                        1,
                    )
                )
                continue
            missing = sorted({"title", "date", "author", "categories"} - keys)
            if missing:
                findings.append(
                    Finding(
                        "frontmatter.blog-missing-key",
                        path_rel,
                        ", ".join(missing),
                        1,
                    )
                )
            date_value = frontmatter_value(text, "date")
            if date_value and not re.match(
                r"^[A-Z][a-z]{2} \d{1,2}, \d{4}$", date_value
            ):
                findings.append(
                    Finding("frontmatter.odd-date", path_rel, f"date: {date_value}", 1)
                )
            if re.match(r"^\d{4}-\d{2}-xx-", path.name):
                findings.append(
                    Finding(
                        "frontmatter.placeholder-date-in-filename",
                        path_rel,
                        path.name,
                        1,
                    )
                )
        elif has_frontmatter(text) and "title" not in keys:
            findings.append(
                Finding(
                    "frontmatter.missing-title",
                    path_rel,
                    "frontmatter present without title",
                    1,
                )
            )
    return findings


def template_findings(root: Path, files: list[Path]) -> list[Finding]:
    findings: list[Finding] = []
    for path in files:
        text = read_text(path)
        for index, line in enumerate(text.splitlines(), start=1):
            for marker in TEMPLATE_MARKERS:
                if marker in line:
                    findings.append(
                        Finding(
                            category="template.marker",
                            path=rel(path, root),
                            line=index,
                            detail=marker,
                        )
                    )
    return findings


def style_findings(root: Path, files: list[Path]) -> list[Finding]:
    findings: list[Finding] = []
    for path in files:
        for index, line in enumerate(read_text(path).splitlines(), start=1):
            if line.rstrip(" \t") != line:
                findings.append(
                    Finding(
                        "style.trailing-whitespace",
                        rel(path, root),
                        "trailing whitespace",
                        index,
                    )
                )
            if "\t" in line:
                findings.append(
                    Finding(
                        "style.tab-character", rel(path, root), "tab character", index
                    )
                )
    return findings


def count_files(root: Path, pattern: str) -> int:
    return sum(1 for path in root.glob(pattern) if path.is_file())


def drift_inventory(root: Path) -> dict[str, object]:
    return {
        "cli": {
            "source": "cmd/cli/cli/*.go",
            "docs": "docs/cli/*.md plus docs/cli.md",
            "source_files": count_files(root, "cmd/cli/cli/*.go"),
            "doc_files": count_files(root, "docs/cli/*.md")
            + int((root / "docs" / "cli.md").is_file()),
            "future_check": "compare CLI command and flag names with docs mentions",
        },
        "http_api": {
            "source": "tools/gendocs, api/*.go, AIS handlers",
            "docs": "docs/http_api.md and generated API reference",
            "source_files": count_files(root, "tools/gendocs/*.go")
            + count_files(root, "api/*.go"),
            "doc_files": int((root / "docs" / "http_api.md").is_file()),
            "future_check": "run API doc generation and report diffs",
        },
        "config_env": {
            "source": "cmn/config.go and api/env/*.go",
            "docs": "docs/configuration.md and docs/environment-vars.md",
            "source_files": int((root / "cmn" / "config.go").is_file())
            + count_files(root, "api/env/*.go"),
            "doc_files": sum(
                int((root / "docs" / name).is_file())
                for name in ("configuration.md", "environment-vars.md")
            ),
            "future_check": "compare documented environment variables with source constants",
        },
        "python_sdk": {
            "source": "python/aistore/**/*.py",
            "docs": "python/README.md and generated Fern pages/python",
            "source_files": count_files(root, "python/aistore/**/*.py"),
            "doc_files": int((root / "python" / "README.md").is_file()),
            "future_check": "verify generated Fern SDK pages after `fern docs md generate`",
        },
    }


def summary_counts(findings: list[Finding]) -> dict[str, int]:
    counts: dict[str, int] = {}
    for finding in findings:
        counts[finding.category] = counts.get(finding.category, 0) + 1
    return dict(sorted(counts.items()))


def finding_to_dict(finding: Finding) -> dict[str, object]:
    return {
        "category": finding.category,
        "path": finding.path,
        "detail": finding.detail,
        "line": finding.line,
    }


def escape_cell(value: str) -> str:
    return value.replace("|", "\\|").replace("\n", " ")


def render_markdown(
    root: Path, files: list[Path], findings: list[Finding], inventory: dict[str, object]
) -> str:
    counts = summary_counts(findings)
    lines = [
        "# Docs Quality Report",
        "",
        "> Report-only snapshot. Findings do not fail CI in this initial DQA slice.",
        "",
        "## Summary",
        "",
        f"- Markdown/MDX source files scanned: {len(files)}",
        f"- Findings: {len(findings)}",
    ]
    for category, count in counts.items():
        lines.append(f"- {category}: {count}")

    lines.extend(["", "## Drift Inventory", ""])
    lines.append(
        "| Surface | Source of truth | Documentation | Current counts | Future check |"
    )
    lines.append("|---|---|---|---|---|")
    for surface, data in inventory.items():
        if not isinstance(data, dict):
            raise TypeError(f"unexpected drift inventory entry for {surface}")
        lines.append(
            "| {surface} | {source} | {docs} | source={source_files}, docs={doc_files} | {future_check} |".format(
                surface=surface,
                source=data["source"],
                docs=data["docs"],
                source_files=data["source_files"],
                doc_files=data["doc_files"],
                future_check=data["future_check"],
            )
        )

    lines.extend(["", "## Findings", ""])
    if findings:
        lines.append("| Category | Path | Line | Detail |")
        lines.append("|---|---|---:|---|")
        for finding in findings[:REPORT_LIMIT]:
            line = "" if finding.line is None else str(finding.line)
            lines.append(
                f"| {finding.category} | `{finding.path}` | {line} | `{escape_cell(finding.detail)}` |"
            )
        if len(findings) > REPORT_LIMIT:
            lines.append(
                f"| truncated | | | {len(findings) - REPORT_LIMIT} additional findings in JSON artifact |"
            )
    else:
        lines.append("No findings from the current report-only checks.")

    lines.extend(["", "## TODOs For Future Checks", ""])
    for item in FUTURE_CHECKS:
        lines.append(f"- {item}")
    lines.extend(["", f"Repository: `{root}`", ""])
    return "\n".join(lines)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--output", default="docs-quality-report.md", help="Markdown report path"
    )
    parser.add_argument(
        "--json-output", default="docs-quality-report.json", help="JSON report path"
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    root = repo_root()
    files = markdown_sources(root)
    findings: list[Finding] = []
    findings.extend(frontmatter_findings(root, files))
    findings.extend(local_link_findings(root, files))
    findings.extend(template_findings(root, files))
    findings.extend(style_findings(root, files))
    inventory = drift_inventory(root)

    output = Path(args.output)
    json_output = Path(args.json_output)
    output.write_text(
        render_markdown(root, files, findings, inventory), encoding="utf-8"
    )
    json_output.write_text(
        json.dumps(
            {
                "summary": {
                    "scanned_file_count": len(files),
                    "finding_count": len(findings),
                    "finding_counts": summary_counts(findings),
                    "report_only": True,
                },
                "scanned_files": [rel(path, root) for path in files],
                "findings": [finding_to_dict(finding) for finding in findings],
                "drift_inventory": inventory,
                "future_checks": list(FUTURE_CHECKS),
            },
            indent=2,
            sort_keys=True,
        )
        + "\n",
        encoding="utf-8",
    )
    print(f"wrote {output} and {json_output}; findings={len(findings)}")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        raise
