from __future__ import annotations

import re
import tarfile
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class GitLabProjectUpload:
    disk_path: str
    upload_hash: str
    filename: str


_GITLAB_UPLOAD_URL_RE = re.compile(
    r"(?P<url>/uploads/(?P<hash>[0-9a-f]{32})/(?P<filename>[^\s)\]\"'>]+))",
    re.IGNORECASE,
)


def iter_gitlab_upload_urls(text: str) -> list[tuple[str, str, str]]:
    """
    Return a list of GitLab `/uploads/<hash>/<filename>` URLs found in `text`.

    The returned tuples are: (url, upload_hash, filename), in the order they appear.
    """
    out: list[tuple[str, str, str]] = []
    for m in _GITLAB_UPLOAD_URL_RE.finditer(text):
        url = m.group("url")
        upload_hash = m.group("hash").lower()
        filename = m.group("filename")
        out.append((url, upload_hash, filename))
    return out


def replace_gitlab_upload_urls(text: str, *, mapping: Mapping[str, str]) -> str:
    def repl(match: re.Match[str]) -> str:
        url = match.group("url")
        return mapping.get(url, url)

    return _GITLAB_UPLOAD_URL_RE.sub(repl, text)


def read_user_avatars_from_uploads(
    uploads_tar_path: Path,
    *,
    desired: Mapping[int, str | None],
) -> dict[int, bytes]:
    """
    Extract user avatar bytes from a GitLab backup `uploads.tar(.gz)` archive.

    GitLab stores avatars under:
      `./-/system/user/avatar/<user_id>/<filename>`
    """
    wanted_by_name: dict[str, int] = {}
    names_by_user_id: dict[int, tuple[str, ...]] = {}
    for user_id, filename in desired.items():
        if not filename:
            continue
        candidates = (
            f"./-/system/user/avatar/{user_id}/{filename}",
            f"-/system/user/avatar/{user_id}/{filename}",
        )
        names_by_user_id[user_id] = candidates
        for name in candidates:
            wanted_by_name[name] = user_id

    if not wanted_by_name:
        return {}

    remaining_user_ids = set(names_by_user_id)
    out: dict[int, bytes] = {}

    mode = "r|gz" if uploads_tar_path.name.endswith(".gz") else "r|"
    with tarfile.open(uploads_tar_path, mode) as tf:
        for member in tf:
            user_id = wanted_by_name.get(member.name)
            if user_id is None or user_id not in remaining_user_ids:
                continue
            if not member.isfile():
                continue
            f = tf.extractfile(member)
            if f is None:
                continue
            with f:
                out[user_id] = f.read()

            remaining_user_ids.remove(user_id)
            for name in names_by_user_id.get(user_id, ()):
                wanted_by_name.pop(name, None)
            if not remaining_user_ids:
                break

    return out


def read_project_uploads_from_uploads(
    uploads_tar_path: Path,
    *,
    desired: set[GitLabProjectUpload],
) -> dict[GitLabProjectUpload, bytes]:
    """
    Extract per-project upload bytes referenced as `/uploads/<hash>/<filename>`.

    In a GitLab backup, these are stored under:
      `./<project_disk_path>/<upload_hash>/<filename>`
    """
    wanted_by_name: dict[str, GitLabProjectUpload] = {}
    names_by_upload: dict[GitLabProjectUpload, tuple[str, ...]] = {}
    for upload in desired:
        disk_path = upload.disk_path.strip().lstrip("./")
        if not disk_path:
            continue
        upload_hash = upload.upload_hash.strip()
        filename = upload.filename.strip()
        if not upload_hash or not filename:
            continue

        candidates = (
            f"./{disk_path}/{upload_hash}/{filename}",
            f"{disk_path}/{upload_hash}/{filename}",
        )
        names_by_upload[upload] = candidates
        for name in candidates:
            wanted_by_name[name] = upload

    if not wanted_by_name:
        return {}

    remaining = set(names_by_upload)
    out: dict[GitLabProjectUpload, bytes] = {}

    mode = "r|gz" if uploads_tar_path.name.endswith(".gz") else "r|"
    with tarfile.open(uploads_tar_path, mode) as tf:
        for member in tf:
            upload = wanted_by_name.get(member.name)
            if upload is None or upload not in remaining:
                continue
            if not member.isfile():
                continue
            f = tf.extractfile(member)
            if f is None:
                continue
            with f:
                out[upload] = f.read()

            remaining.remove(upload)
            for name in names_by_upload.get(upload, ()):
                wanted_by_name.pop(name, None)
            if not remaining:
                break

    return out
