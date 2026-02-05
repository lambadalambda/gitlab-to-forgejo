from __future__ import annotations

import io
import tarfile
from pathlib import Path

from gitlab_to_forgejo.gitlab_uploads import (
    GitLabProjectUpload,
    iter_gitlab_upload_urls,
    read_project_uploads_from_uploads,
    replace_gitlab_upload_urls,
)


def test_iter_gitlab_upload_urls_finds_upload_paths() -> None:
    text = "Screenshot: ![screen](/uploads/765b08065cca166722283f5cf5234971/screen.png)"

    urls = iter_gitlab_upload_urls(text)

    assert urls == [
        (
            "/uploads/765b08065cca166722283f5cf5234971/screen.png",
            "765b08065cca166722283f5cf5234971",
            "screen.png",
        )
    ]


def test_replace_gitlab_upload_urls_rewrites_only_matched_urls() -> None:
    original = (
        "a /uploads/aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa/a.png "
        "b /not-uploads/aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa/b.png"
    )

    rewritten = replace_gitlab_upload_urls(
        original,
        mapping={"/uploads/aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa/a.png": "http://x/attachments/123"},
    )

    assert rewritten == (
        "a http://x/attachments/123 b /not-uploads/aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa/b.png"
    )


def test_read_project_uploads_from_uploads_extracts_bytes(tmp_path: Path) -> None:
    uploads = tmp_path / "uploads.tar.gz"
    payload = b"file-bytes"

    disk_path = "@hashed/f4/46/f4466a4b51d21014b34f621813a1ed75f1c750ec328d908d9edc989c64778962"
    upload = GitLabProjectUpload(
        disk_path=disk_path,
        upload_hash="765b08065cca166722283f5cf5234971",
        filename="screen.png",
    )

    with tarfile.open(uploads, "w:gz") as tf:
        info = tarfile.TarInfo(
            name=f"./{disk_path}/{upload.upload_hash}/{upload.filename}",
        )
        info.size = len(payload)
        tf.addfile(info, io.BytesIO(payload))

    extracted = read_project_uploads_from_uploads(uploads, desired={upload})

    assert extracted == {upload: payload}

