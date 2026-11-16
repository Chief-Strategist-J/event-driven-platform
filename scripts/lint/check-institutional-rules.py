#!/usr/bin/env python3
import sys
import re
from pathlib import Path

def check_zero_inline_comments(file_path: Path) -> list:
    violations = []
    lines = file_path.read_text(encoding="utf-8").splitlines()
    in_header = True
    in_block_comment = False
    brace_depth = 0

    for idx, line in enumerate(lines, start=1):
        stripped = line.strip()

        if in_header:
            if stripped.startswith("/*"):
                in_block_comment = True
                continue
            if in_block_comment:
                if "*/" in stripped:
                    in_block_comment = False
                continue
            if stripped.startswith("//") or stripped == "":
                continue
            if stripped.startswith("package "):
                in_header = False
                continue

        if stripped.startswith("/*"):
            in_block_comment = True
        if in_block_comment:
            if brace_depth > 0:
                violations.append((idx, line, "Block comment inside function or type body"))
            if "*/" in stripped:
                in_block_comment = False
            continue

        open_braces = line.count("{")
        close_braces = line.count("}")
        
        if brace_depth > 0:
            clean_line = re.sub(r'"(\\"|[^"])*"', '', line)
            clean_line = re.sub(r'`[^`]*`', '', clean_line)
            if "//" in clean_line:
                comment_part = clean_line.split("//", 1)[1].strip()
                if not comment_part.startswith("nolint:") and not comment_part.startswith("go:"):
                    violations.append((idx, line, "Mid-function comment detected"))

        brace_depth += (open_braces - close_braces)
        if brace_depth < 0:
            brace_depth = 0

    return violations

def main():
    root = Path(".")
    targets = list(root.glob("src/**/*.go"))
    all_violations = []

    for path in targets:
        if path.name.endswith("_test.go"):
            continue
        v = check_zero_inline_comments(path)
        if v:
            all_violations.extend([(path, line_no, content, reason) for line_no, content, reason in v])

    if all_violations:
        print(f"Institutional Rule Failure: Zero mid-function comments policy violated in {len(all_violations)} locations:")
        for path, line_no, content, reason in all_violations:
            print(f"  {path}:{line_no}: [{reason}] -> {content.strip()}")
        sys.exit(1)

    print(f"Institutional Rule Verified: 0 mid-function comments found across {len(targets)} Go source files.")
    sys.exit(0)

if __name__ == "__main__":
    main()
