from __future__ import annotations

from gitlab_to_forgejo.forgejo_db import build_sequence_resync_sql


def test_build_sequence_resync_sql_resets_owned_public_sequences() -> None:
    sql = build_sequence_resync_sql()

    assert "DO $$" in sql
    assert "FOR seq_rec IN" in sql
    assert "c.relkind = 'S'" in sql
    assert "n.nspname = 'public'" in sql
    assert "setval(" in sql
    assert "COALESCE((SELECT MAX(" in sql
    assert "END $$;" in sql
