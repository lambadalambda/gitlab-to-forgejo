from __future__ import annotations

from pathlib import Path


def read_ref_shas(refs_path: Path) -> dict[str, str]:
    """Return a mapping of ref name → SHA from GitLab `*.refs` files."""
    out: dict[str, str] = {}
    for raw in refs_path.read_text(encoding="utf-8", errors="replace").splitlines():
        line = raw.strip()
        if not line:
            continue
        try:
            _sha, ref = line.split(maxsplit=1)
        except ValueError:
            raise ValueError(f"invalid refs line: {raw!r}") from None
        if ref == "HEAD":
            continue
        out[ref] = _sha
    return out


def _iter_ref_names(refs_path: Path) -> list[str]:
    return list(read_ref_shas(refs_path))


def list_push_refspecs(refs_path: Path) -> list[str]:
    """Return refspecs suitable for `git push` (branches + tags only)."""
    ref_names = _iter_ref_names(refs_path)
    selected = sorted(
        r for r in ref_names if r.startswith("refs/heads/") or r.startswith("refs/tags/")
    )
    return [f"{r}:{r}" for r in selected]


def guess_default_branch(refs_path: Path) -> str:
    """Best-effort default branch detection from GitLab `*.refs`."""
    ref_names = _iter_ref_names(refs_path)
    branches = sorted(
        r.removeprefix("refs/heads/") for r in ref_names if r.startswith("refs/heads/")
    )
    branch_set = set(branches)
    for preferred in ("main", "master", "develop", "trunk", "stable"):
        if preferred in branch_set:
            return preferred
    if branches:
        return branches[0]
    raise ValueError(f"no branch refs found in {refs_path}")


def list_wiki_push_refspecs(refs_path: Path) -> list[str]:
    """Return refspecs suitable for pushing GitLab wiki bundles to Forgejo.

    Forgejo defaults wiki repos to branch `main`, while GitLab wikis often use `master`.
    This maps the GitLab wiki default branch to `refs/heads/main`.
    """
    try:
        default_branch = guess_default_branch(refs_path)
    except (FileNotFoundError, ValueError):
        return []
    src = f"refs/heads/{default_branch}"
    return [f"{src}:refs/heads/main"]
