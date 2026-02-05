from __future__ import annotations

import tarfile
from collections.abc import Mapping
from pathlib import Path


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

