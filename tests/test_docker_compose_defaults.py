from pathlib import Path


def test_compose_uses_migration_friendly_notification_defaults() -> None:
    compose = Path("docker-compose.yml").read_text(encoding="utf-8")

    assert 'FORGEJO__service__AUTO_WATCH_NEW_REPOS: "${FORGEJO_AUTO_WATCH_NEW_REPOS:-false}"' in compose
    assert (
        'FORGEJO__service__AUTO_WATCH_ON_CHANGES: '
        '"${FORGEJO_AUTO_WATCH_ON_CHANGES:-false}"'
    ) in compose
    assert 'FORGEJO__actions__ENABLED: "${FORGEJO_ACTIONS_ENABLED:-false}"' in compose
    assert 'FORGEJO__service__ENABLE_NOTIFY_MAIL: "${FORGEJO_ENABLE_NOTIFY_MAIL:-false}"' in compose
    assert 'FORGEJO__other__ENABLE_FEED: "${FORGEJO_ENABLE_FEED:-false}"' in compose
