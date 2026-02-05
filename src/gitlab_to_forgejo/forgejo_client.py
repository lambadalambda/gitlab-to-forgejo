from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import requests


@dataclass
class ForgejoError(Exception):
    method: str
    url: str
    status_code: int
    body: str

    def __str__(self) -> str:  # pragma: no cover (repr only)
        return f"{self.method} {self.url} failed with {self.status_code}: {self.body[:200]}"


class ForgejoNotFound(ForgejoError):
    pass


_DEFAULT_TEAM_UNITS = [
    "repo.actions",
    "repo.code",
    "repo.ext_issues",
    "repo.ext_wiki",
    "repo.issues",
    "repo.packages",
    "repo.projects",
    "repo.pulls",
    "repo.releases",
    "repo.wiki",
]


def _pick_team_units(teams: list[dict[str, Any]]) -> list[str]:
    def _normalize(units: Any) -> list[str] | None:
        if not isinstance(units, list):
            return None
        out: list[str] = []
        for u in units:
            if isinstance(u, str) and u:
                out.append(u)
        return out or None

    for t in teams:
        if t.get("permission") in {"owner", "admin"}:
            if units := _normalize(t.get("units")):
                return units
    for t in teams:
        if units := _normalize(t.get("units")):
            return units
    return list(_DEFAULT_TEAM_UNITS)


class ForgejoClient:
    def __init__(
        self, *, base_url: str, token: str, session: requests.Session | None = None
    ) -> None:
        self._base_url = base_url.rstrip("/")
        self._api_url = f"{self._base_url}/api/v1"
        self._token = token
        self._session = session or requests.Session()

        self._org_teams_cache: dict[str, list[dict[str, Any]]] = {}

    def _url(self, path: str) -> str:
        if not path.startswith("/"):
            raise ValueError(f"path must start with '/': {path!r}")
        return f"{self._api_url}{path}"

    def _request(
        self,
        method: str,
        path: str,
        *,
        params: dict[str, Any] | None = None,
        json: Any | None = None,
        files: Any | None = None,
        data: Any | None = None,
    ) -> requests.Response:
        url = self._url(path)
        headers = {"Authorization": f"token {self._token}"}
        resp = self._session.request(
            method,
            url,
            headers=headers,
            params=params,
            json=json,
            files=files,
            data=data,
            timeout=30,
        )

        if resp.status_code == 404:
            raise ForgejoNotFound(
                method=method, url=url, status_code=resp.status_code, body=resp.text
            )
        if resp.status_code >= 400:
            raise ForgejoError(method=method, url=url, status_code=resp.status_code, body=resp.text)

        return resp

    def _request_json(
        self,
        method: str,
        path: str,
        *,
        params: dict[str, Any] | None = None,
        json: Any | None = None,
    ) -> Any:
        resp = self._request(method, path, params=params, json=json)

        if resp.status_code == 204 or not resp.content:
            return None
        return resp.json()

    def get_user(self, username: str) -> dict[str, Any]:
        data = self._request_json("GET", f"/users/{username}")
        assert isinstance(data, dict)
        return data

    def create_user(
        self,
        *,
        username: str,
        email: str,
        full_name: str,
        password: str,
        must_change_password: bool = False,
        send_notify: bool = False,
    ) -> dict[str, Any]:
        data = self._request_json(
            "POST",
            "/admin/users",
            json={
                "username": username,
                "email": email,
                "full_name": full_name,
                "password": password,
                "must_change_password": must_change_password,
                "send_notify": send_notify,
            },
        )
        assert isinstance(data, dict)
        return data

    def ensure_user(self, *, username: str, email: str, full_name: str, password: str) -> None:
        try:
            self.get_user(username)
            return
        except ForgejoNotFound:
            self.create_user(username=username, email=email, full_name=full_name, password=password)

    def get_org(self, org: str) -> dict[str, Any]:
        data = self._request_json("GET", f"/orgs/{org}")
        assert isinstance(data, dict)
        return data

    def create_org(
        self,
        *,
        org: str,
        full_name: str,
        description: str | None,
        visibility: str = "private",
    ) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "username": org,
            "full_name": full_name,
            "visibility": visibility,
        }
        if description:
            payload["description"] = description

        data = self._request_json("POST", "/orgs", json=payload)
        assert isinstance(data, dict)
        return data

    def ensure_org(self, *, org: str, full_name: str, description: str | None) -> None:
        try:
            self.get_org(org)
            return
        except ForgejoNotFound:
            self.create_org(org=org, full_name=full_name, description=description)

    def list_org_teams(self, org: str, *, force_refresh: bool = False) -> list[dict[str, Any]]:
        if not force_refresh and org in self._org_teams_cache:
            return self._org_teams_cache[org]

        data = self._request_json("GET", f"/orgs/{org}/teams")
        assert isinstance(data, list)
        teams = [t for t in data if isinstance(t, dict)]
        self._org_teams_cache[org] = teams
        return teams

    def get_owner_team_id(self, org: str) -> int:
        teams = self.list_org_teams(org)
        for t in teams:
            if t.get("permission") == "owner":
                return int(t["id"])
        raise ForgejoError(
            method="GET",
            url=self._url(f"/orgs/{org}/teams"),
            status_code=200,
            body="no owner team",
        )

    def ensure_team(
        self,
        *,
        org: str,
        name: str,
        permission: str,
        includes_all_repositories: bool,
    ) -> int:
        teams = self.list_org_teams(org)
        for t in teams:
            if t.get("name") == name:
                return int(t["id"])

        payload: dict[str, Any] = {
            "name": name,
            "permission": permission,
            "includes_all_repositories": includes_all_repositories,
        }
        if permission != "admin":
            units = _pick_team_units(teams)
            payload["units"] = units
            payload["units_map"] = {u: permission for u in units}

        data = self._request_json(
            "POST",
            f"/orgs/{org}/teams",
            json=payload,
        )
        assert isinstance(data, dict)

        # Refresh cached teams on next query.
        self._org_teams_cache.pop(org, None)
        return int(data["id"])

    def add_team_member(self, *, team_id: int, username: str) -> None:
        self._request_json("PUT", f"/teams/{team_id}/members/{username}")

    def get_repo(self, owner: str, repo: str) -> dict[str, Any]:
        data = self._request_json("GET", f"/repos/{owner}/{repo}")
        assert isinstance(data, dict)
        return data

    def create_org_repo(
        self,
        *,
        org: str,
        name: str,
        private: bool,
        default_branch: str | None = None,
        auto_init: bool = False,
    ) -> dict[str, Any]:
        payload: dict[str, Any] = {"name": name, "private": private, "auto_init": auto_init}
        if default_branch:
            payload["default_branch"] = default_branch
        data = self._request_json("POST", f"/orgs/{org}/repos", json=payload)
        assert isinstance(data, dict)
        return data

    def ensure_org_repo(
        self,
        *,
        org: str,
        name: str,
        private: bool,
        default_branch: str | None = None,
    ) -> None:
        try:
            self.get_repo(org, name)
            return
        except ForgejoNotFound:
            self.create_org_repo(
                org=org,
                name=name,
                private=private,
                default_branch=default_branch,
                auto_init=False,
            )

    def create_issue(
        self,
        *,
        owner: str,
        repo: str,
        title: str,
        body: str,
        sudo: str | None = None,
    ) -> dict[str, Any]:
        data = self._request_json(
            "POST",
            f"/repos/{owner}/{repo}/issues",
            params={"sudo": sudo} if sudo else None,
            json={"title": title, "body": body},
        )
        assert isinstance(data, dict)
        return data

    def edit_issue_body(
        self,
        *,
        owner: str,
        repo: str,
        issue_number: int,
        body: str,
        sudo: str | None = None,
    ) -> dict[str, Any]:
        data = self._request_json(
            "PATCH",
            f"/repos/{owner}/{repo}/issues/{issue_number}",
            params={"sudo": sudo} if sudo else None,
            json={"body": body},
        )
        assert isinstance(data, dict)
        return data

    def edit_pull_request_body(
        self,
        *,
        owner: str,
        repo: str,
        pr_number: int,
        body: str,
        sudo: str | None = None,
    ) -> dict[str, Any]:
        data = self._request_json(
            "PATCH",
            f"/repos/{owner}/{repo}/pulls/{pr_number}",
            params={"sudo": sudo} if sudo else None,
            json={"body": body},
        )
        assert isinstance(data, dict)
        return data

    def create_pull_request(
        self,
        *,
        owner: str,
        repo: str,
        title: str,
        body: str,
        head: str,
        base: str,
        sudo: str | None = None,
    ) -> dict[str, Any]:
        data = self._request_json(
            "POST",
            f"/repos/{owner}/{repo}/pulls",
            params={"sudo": sudo} if sudo else None,
            json={
                "title": title,
                "body": body,
                "head": head,
                "base": base,
            },
        )
        assert isinstance(data, dict)
        return data

    def create_issue_comment(
        self,
        *,
        owner: str,
        repo: str,
        issue_number: int,
        body: str,
        sudo: str | None = None,
    ) -> dict[str, Any]:
        data = self._request_json(
            "POST",
            f"/repos/{owner}/{repo}/issues/{issue_number}/comments",
            params={"sudo": sudo} if sudo else None,
            json={"body": body},
        )
        assert isinstance(data, dict)
        return data

    def edit_issue_comment(
        self,
        *,
        owner: str,
        repo: str,
        comment_id: int,
        body: str,
        sudo: str | None = None,
    ) -> dict[str, Any]:
        data = self._request_json(
            "PATCH",
            f"/repos/{owner}/{repo}/issues/comments/{comment_id}",
            params={"sudo": sudo} if sudo else None,
            json={"body": body},
        )
        assert isinstance(data, dict)
        return data

    def create_issue_attachment(
        self,
        *,
        owner: str,
        repo: str,
        issue_number: int,
        filename: str,
        content: bytes,
        sudo: str | None = None,
    ) -> dict[str, Any]:
        resp = self._request(
            "POST",
            f"/repos/{owner}/{repo}/issues/{issue_number}/assets",
            params={"sudo": sudo} if sudo else None,
            files={"attachment": (filename, content)},
        )
        data = resp.json()
        assert isinstance(data, dict)
        return data

    def create_issue_comment_attachment(
        self,
        *,
        owner: str,
        repo: str,
        comment_id: int,
        filename: str,
        content: bytes,
        sudo: str | None = None,
    ) -> dict[str, Any]:
        resp = self._request(
            "POST",
            f"/repos/{owner}/{repo}/issues/comments/{comment_id}/assets",
            params={"sudo": sudo} if sudo else None,
            files={"attachment": (filename, content)},
        )
        data = resp.json()
        assert isinstance(data, dict)
        return data

    def update_user_avatar(self, *, image_b64: str, sudo: str | None = None) -> None:
        self._request_json(
            "POST",
            "/user/avatar",
            params={"sudo": sudo} if sudo else None,
            json={"image": image_b64},
        )

    def list_repo_labels(self, *, owner: str, repo: str) -> list[dict[str, Any]]:
        data = self._request_json("GET", f"/repos/{owner}/{repo}/labels")
        assert isinstance(data, list)
        return [label for label in data if isinstance(label, dict)]

    def create_repo_label(
        self,
        *,
        owner: str,
        repo: str,
        name: str,
        color: str,
        description: str,
    ) -> dict[str, Any]:
        data = self._request_json(
            "POST",
            f"/repos/{owner}/{repo}/labels",
            json={"name": name, "color": color, "description": description},
        )
        assert isinstance(data, dict)
        return data

    def replace_issue_labels(
        self,
        *,
        owner: str,
        repo: str,
        issue_number: int,
        labels: list[str],
        sudo: str | None = None,
    ) -> list[dict[str, Any]]:
        data = self._request_json(
            "PUT",
            f"/repos/{owner}/{repo}/issues/{issue_number}/labels",
            params={"sudo": sudo} if sudo else None,
            json={"labels": labels},
        )
        assert isinstance(data, list)
        return [label for label in data if isinstance(label, dict)]
