from __future__ import annotations

import json

import responses

from gitlab_to_forgejo.forgejo_client import ForgejoClient


@responses.activate
def test_get_user_uses_token_header() -> None:
    client = ForgejoClient(base_url="http://example.test/", token="t0")

    responses.add(
        responses.GET,
        "http://example.test/api/v1/users/alice",
        json={"username": "alice"},
        status=200,
    )

    user = client.get_user("alice")

    assert user["username"] == "alice"
    assert responses.calls[0].request.headers["Authorization"] == "token t0"


@responses.activate
def test_ensure_user_creates_when_missing() -> None:
    client = ForgejoClient(base_url="http://example.test", token="t0")

    responses.add(responses.GET, "http://example.test/api/v1/users/bob", status=404)
    responses.add(
        responses.POST,
        "http://example.test/api/v1/admin/users",
        json={"username": "bob"},
        status=201,
    )

    client.ensure_user(username="bob", email="bob@example.test", full_name="Bob B", password="pw")

    assert [c.request.method for c in responses.calls] == ["GET", "POST"]
    body = json.loads(responses.calls[1].request.body)
    assert body["username"] == "bob"
    assert body["email"] == "bob@example.test"
    assert body["full_name"] == "Bob B"
    assert body["password"] == "pw"
    assert body["must_change_password"] is False
    assert body["send_notify"] is False


@responses.activate
def test_get_owner_team_id_finds_owner_team() -> None:
    client = ForgejoClient(base_url="http://example.test", token="t0")

    responses.add(
        responses.GET,
        "http://example.test/api/v1/orgs/pleroma/teams",
        json=[
            {"id": 1, "name": "Owners", "permission": "owner"},
            {"id": 2, "name": "Maintainers", "permission": "admin"},
        ],
        status=200,
    )

    assert client.get_owner_team_id("pleroma") == 1


@responses.activate
def test_ensure_team_creates_when_missing() -> None:
    client = ForgejoClient(base_url="http://example.test", token="t0")

    responses.add(
        responses.GET,
        "http://example.test/api/v1/orgs/pleroma/teams",
        json=[{"id": 1, "name": "Owners", "permission": "owner"}],
        status=200,
    )
    responses.add(
        responses.POST,
        "http://example.test/api/v1/orgs/pleroma/teams",
        json={"id": 2, "name": "Maintainers", "permission": "admin"},
        status=201,
    )

    team_id = client.ensure_team(
        org="pleroma", name="Maintainers", permission="admin", includes_all_repositories=True
    )

    assert team_id == 2
    assert [c.request.method for c in responses.calls] == ["GET", "POST"]

    body = json.loads(responses.calls[1].request.body)
    assert body["name"] == "Maintainers"
    assert body["permission"] == "admin"
    assert body["includes_all_repositories"] is True


@responses.activate
def test_ensure_team_write_includes_units_map() -> None:
    client = ForgejoClient(base_url="http://example.test", token="t0")

    responses.add(
        responses.GET,
        "http://example.test/api/v1/orgs/pleroma/teams",
        json=[
            {
                "id": 1,
                "name": "Owners",
                "permission": "owner",
                "units": ["repo.code", "repo.issues"],
            }
        ],
        status=200,
    )
    responses.add(
        responses.POST,
        "http://example.test/api/v1/orgs/pleroma/teams",
        json={"id": 2, "name": "Developers", "permission": "write"},
        status=201,
    )

    team_id = client.ensure_team(
        org="pleroma", name="Developers", permission="write", includes_all_repositories=True
    )

    assert team_id == 2
    body = json.loads(responses.calls[1].request.body)
    assert body["permission"] == "write"
    assert body["units"] == ["repo.code", "repo.issues"]
    assert body["units_map"] == {"repo.code": "write", "repo.issues": "write"}


@responses.activate
def test_ensure_org_repo_creates_when_missing() -> None:
    client = ForgejoClient(base_url="http://example.test", token="t0")

    responses.add(responses.GET, "http://example.test/api/v1/repos/pleroma/docs", status=404)
    responses.add(
        responses.POST,
        "http://example.test/api/v1/orgs/pleroma/repos",
        json={"name": "docs"},
        status=201,
    )

    client.ensure_org_repo(org="pleroma", name="docs", private=True, default_branch="master")

    assert [c.request.method for c in responses.calls] == ["GET", "POST"]
    body = json.loads(responses.calls[1].request.body)
    assert body["name"] == "docs"
    assert body["private"] is True
    assert body["auto_init"] is False
    assert body["default_branch"] == "master"


@responses.activate
def test_create_issue_uses_sudo_param() -> None:
    client = ForgejoClient(base_url="http://example.test", token="t0")

    responses.add(
        responses.POST,
        "http://example.test/api/v1/repos/pleroma/docs/issues",
        json={"number": 1},
        status=201,
    )

    issue = client.create_issue(
        owner="pleroma", repo="docs", title="Hello", body="World", sudo="lanodan"
    )

    assert issue["number"] == 1
    assert "sudo=lanodan" in responses.calls[0].request.url
    body = json.loads(responses.calls[0].request.body)
    assert body == {"title": "Hello", "body": "World"}


@responses.activate
def test_create_pull_request_uses_sudo_param() -> None:
    client = ForgejoClient(base_url="http://example.test", token="t0")

    responses.add(
        responses.POST,
        "http://example.test/api/v1/repos/pleroma/docs/pulls",
        json={"number": 2},
        status=201,
    )

    pr = client.create_pull_request(
        owner="pleroma",
        repo="docs",
        title="PR title",
        body="PR body",
        head="features/menu",
        base="master",
        sudo="lanodan",
    )

    assert pr["number"] == 2
    assert "sudo=lanodan" in responses.calls[0].request.url
    body = json.loads(responses.calls[0].request.body)
    assert body == {
        "title": "PR title",
        "body": "PR body",
        "head": "features/menu",
        "base": "master",
    }


@responses.activate
def test_create_issue_comment_uses_sudo_param() -> None:
    client = ForgejoClient(base_url="http://example.test", token="t0")

    responses.add(
        responses.POST,
        "http://example.test/api/v1/repos/pleroma/docs/issues/1/comments",
        json={"id": 123},
        status=201,
    )

    client.create_issue_comment(
        owner="pleroma",
        repo="docs",
        issue_number=1,
        body="Hello",
        sudo="lanodan",
    )

    assert "sudo=lanodan" in responses.calls[0].request.url
    body = json.loads(responses.calls[0].request.body)
    assert body == {"body": "Hello"}


@responses.activate
def test_update_user_avatar_uses_sudo_param() -> None:
    client = ForgejoClient(base_url="http://example.test", token="t0")

    responses.add(
        responses.POST,
        "http://example.test/api/v1/user/avatar",
        status=204,
    )

    client.update_user_avatar(image_b64="aGVsbG8=", sudo="lanodan")

    assert "sudo=lanodan" in responses.calls[0].request.url
    body = json.loads(responses.calls[0].request.body)
    assert body == {"image": "aGVsbG8="}


@responses.activate
def test_create_repo_label_posts_label_definition() -> None:
    client = ForgejoClient(base_url="http://example.test", token="t0")

    responses.add(
        responses.POST,
        "http://example.test/api/v1/repos/pleroma/docs/labels",
        json={"id": 1, "name": "bug"},
        status=201,
    )

    label = client.create_repo_label(
        owner="pleroma",
        repo="docs",
        name="bug",
        color="#ff0000",
        description="Bug label",
    )

    assert label["name"] == "bug"
    body = json.loads(responses.calls[0].request.body)
    assert body == {"name": "bug", "color": "#ff0000", "description": "Bug label"}


@responses.activate
def test_replace_issue_labels_puts_names() -> None:
    client = ForgejoClient(base_url="http://example.test", token="t0")

    responses.add(
        responses.PUT,
        "http://example.test/api/v1/repos/pleroma/docs/issues/1/labels",
        json=[{"id": 1, "name": "bug"}],
        status=200,
    )

    labels = client.replace_issue_labels(
        owner="pleroma",
        repo="docs",
        issue_number=1,
        labels=["bug", "discussion"],
    )

    assert labels[0]["name"] == "bug"
    body = json.loads(responses.calls[0].request.body)
    assert body == {"labels": ["bug", "discussion"]}


@responses.activate
def test_create_issue_attachment_posts_multipart_and_returns_attachment() -> None:
    client = ForgejoClient(base_url="http://example.test", token="t0")

    responses.add(
        responses.POST,
        "http://example.test/api/v1/repos/pleroma/docs/issues/1/assets",
        json={"uuid": "u1", "browser_download_url": "http://example.test/attachments/u1"},
        status=201,
    )

    attachment = client.create_issue_attachment(
        owner="pleroma",
        repo="docs",
        issue_number=1,
        filename="screen.png",
        content=b"png-bytes",
        sudo="lanodan",
    )

    assert attachment["uuid"] == "u1"
    assert "sudo=lanodan" in responses.calls[0].request.url
    body = responses.calls[0].request.body
    assert isinstance(body, (bytes, bytearray))
    assert b"screen.png" in body
    assert b"png-bytes" in body


@responses.activate
def test_create_issue_comment_attachment_posts_multipart() -> None:
    client = ForgejoClient(base_url="http://example.test", token="t0")

    responses.add(
        responses.POST,
        "http://example.test/api/v1/repos/pleroma/docs/issues/comments/123/assets",
        json={"uuid": "u2", "browser_download_url": "http://example.test/attachments/u2"},
        status=201,
    )

    attachment = client.create_issue_comment_attachment(
        owner="pleroma",
        repo="docs",
        comment_id=123,
        filename="screen.png",
        content=b"png-bytes",
        sudo="lanodan",
    )

    assert attachment["uuid"] == "u2"
    assert "sudo=lanodan" in responses.calls[0].request.url


@responses.activate
def test_edit_issue_body_patches_body() -> None:
    client = ForgejoClient(base_url="http://example.test", token="t0")

    responses.add(
        responses.PATCH,
        "http://example.test/api/v1/repos/pleroma/docs/issues/1",
        json={"number": 1},
        status=201,
    )

    client.edit_issue_body(owner="pleroma", repo="docs", issue_number=1, body="new body")

    payload = json.loads(responses.calls[0].request.body)
    assert payload == {"body": "new body"}


@responses.activate
def test_edit_issue_comment_patches_body() -> None:
    client = ForgejoClient(base_url="http://example.test", token="t0")

    responses.add(
        responses.PATCH,
        "http://example.test/api/v1/repos/pleroma/docs/issues/comments/123",
        json={"id": 123},
        status=200,
    )

    client.edit_issue_comment(owner="pleroma", repo="docs", comment_id=123, body="new body")

    payload = json.loads(responses.calls[0].request.body)
    assert payload == {"body": "new body"}
