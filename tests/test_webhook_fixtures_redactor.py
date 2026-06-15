"""Unit tests for the webhook fixture PII redactor.

One test per field-rule in the allowlist documented in
tests/data/webhook_fixtures/README.md.  
"""

from __future__ import annotations

from typing import Any

from _webhook_redactor import (
    _HEADER_REDACTED,
    _REDACTED,
    _REPO_PLACEHOLDER,
    redact_headers,
    redact_payload,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _payload(**kwargs: Any) -> dict[str, Any]:
    """Return a minimal top-level payload dict with the given extra keys."""
    base: dict[str, Any] = {"action": "completed"}
    base.update(kwargs)
    return base


def _user(login: str = "alice", uid: int = 1, email: str = "alice@example.com") -> dict[str, Any]:
    return {"login": login, "id": uid, "email": email, "type": "User"}


def _repo(name: str = "my-repo") -> dict[str, Any]:
    return {
        "id": 42,
        "name": name,
        "full_name": f"alice/{name}",
        "owner": _user(),
        "html_url": f"https://github.com/alice/{name}",
        "ssh_url": f"git@github.com:alice/{name}.git",
        "clone_url": f"https://github.com/alice/{name}.git",
        "git_url": f"git://github.com/alice/{name}.git",
    }


def _commit(message: str = "do the thing") -> dict[str, Any]:
    return {
        "id": "a" * 40,
        "tree_id": "b" * 40,
        "message": message,
        "author": {"name": "Alice", "email": "alice@example.com", "username": "alice"},
        "committer": {"name": "GitHub", "email": "noreply@github.com", "username": "web-flow"},
    }


# ---------------------------------------------------------------------------
# sender / user fields
# ---------------------------------------------------------------------------


def test_sender_login_scrubbed() -> None:
    payload = _payload(sender=_user(login="alice"))
    result = redact_payload(payload)
    assert result["sender"]["login"] == _REDACTED


def test_sender_id_scrubbed() -> None:
    payload = _payload(sender=_user(uid=12345))
    result = redact_payload(payload)
    assert result["sender"]["id"] == _REDACTED


def test_sender_email_scrubbed() -> None:
    payload = _payload(sender=_user(email="alice@example.com"))
    result = redact_payload(payload)
    assert result["sender"]["email"] == _REDACTED


def test_sender_type_preserved() -> None:
    payload = _payload(sender=_user())
    result = redact_payload(payload)
    assert result["sender"]["type"] == "User"


# ---------------------------------------------------------------------------
# repository fields
# ---------------------------------------------------------------------------


def test_repository_owner_login_scrubbed() -> None:
    payload = _payload(repository=_repo())
    result = redact_payload(payload)
    assert result["repository"]["owner"]["login"] == _REDACTED


def test_repository_owner_id_scrubbed() -> None:
    payload = _payload(repository=_repo())
    result = redact_payload(payload)
    assert result["repository"]["owner"]["id"] == _REDACTED


def test_repository_full_name_replaced() -> None:
    payload = _payload(repository=_repo())
    result = redact_payload(payload)
    assert result["repository"]["full_name"] == _REPO_PLACEHOLDER


def test_repository_html_url_scrubbed() -> None:
    payload = _payload(repository=_repo())
    result = redact_payload(payload)
    assert result["repository"]["html_url"] == _REDACTED


def test_repository_ssh_url_scrubbed() -> None:
    payload = _payload(repository=_repo())
    result = redact_payload(payload)
    assert result["repository"]["ssh_url"] == _REDACTED


def test_repository_clone_url_scrubbed() -> None:
    payload = _payload(repository=_repo())
    result = redact_payload(payload)
    assert result["repository"]["clone_url"] == _REDACTED


def test_repository_git_url_scrubbed() -> None:
    payload = _payload(repository=_repo())
    result = redact_payload(payload)
    assert result["repository"]["git_url"] == _REDACTED


def test_repository_name_preserved() -> None:
    payload = _payload(repository=_repo("my-repo"))
    result = redact_payload(payload)
    assert result["repository"]["name"] == "my-repo"


# ---------------------------------------------------------------------------
# commits list
# ---------------------------------------------------------------------------


def test_commits_author_name_scrubbed() -> None:
    payload = _payload(commits=[_commit()])
    result = redact_payload(payload)
    assert result["commits"][0]["author"]["name"] == _REDACTED


def test_commits_author_email_scrubbed() -> None:
    payload = _payload(commits=[_commit()])
    result = redact_payload(payload)
    assert result["commits"][0]["author"]["email"] == _REDACTED


def test_commits_committer_email_scrubbed() -> None:
    payload = _payload(commits=[_commit()])
    result = redact_payload(payload)
    assert result["commits"][0]["committer"]["email"] == _REDACTED


def test_commits_message_scrubbed() -> None:
    payload = _payload(commits=[_commit(message="secret message")])
    result = redact_payload(payload)
    assert result["commits"][0]["message"] == _REDACTED


def test_commits_id_preserved() -> None:
    sha = "a" * 40
    payload = _payload(commits=[_commit()])
    result = redact_payload(payload)
    assert result["commits"][0]["id"] == sha


# ---------------------------------------------------------------------------
# head_commit (top-level, push events)
# ---------------------------------------------------------------------------


def test_head_commit_author_email_scrubbed() -> None:
    payload = _payload(head_commit=_commit())
    result = redact_payload(payload)
    assert result["head_commit"]["author"]["email"] == _REDACTED


def test_head_commit_message_scrubbed() -> None:
    payload = _payload(head_commit=_commit(message="top secret"))
    result = redact_payload(payload)
    assert result["head_commit"]["message"] == _REDACTED


def test_head_commit_null_tolerated() -> None:
    payload = _payload(head_commit=None)
    result = redact_payload(payload)
    assert result["head_commit"] is None


# ---------------------------------------------------------------------------
# workflow_run nested fields
# ---------------------------------------------------------------------------


def test_workflow_run_head_commit_author_scrubbed() -> None:
    wr: dict[str, Any] = {
        "id": 1,
        "head_commit": _commit(),
        "head_sha": "a" * 40,
        "head_branch": "main",
        "status": "completed",
        "conclusion": "success",
    }
    result = redact_payload(_payload(workflow_run=wr))
    assert result["workflow_run"]["head_commit"]["author"]["email"] == _REDACTED


def test_workflow_run_null_head_commit_tolerated() -> None:
    wr: dict[str, Any] = {
        "id": 1,
        "head_commit": None,
        "head_sha": "a" * 40,
        "head_branch": "main",
        "status": "completed",
        "conclusion": "success",
    }
    result = redact_payload(_payload(workflow_run=wr))
    assert result["workflow_run"]["head_commit"] is None


# ---------------------------------------------------------------------------
# installation
# ---------------------------------------------------------------------------


def test_installation_id_scrubbed() -> None:
    payload = _payload(installation={"id": 9999, "app_id": 42})
    result = redact_payload(payload)
    assert result["installation"]["id"] == _REDACTED


def test_installation_app_id_preserved() -> None:
    payload = _payload(installation={"id": 9999, "app_id": 42})
    result = redact_payload(payload)
    assert result["installation"]["app_id"] == 42


# ---------------------------------------------------------------------------
# pull_request
# ---------------------------------------------------------------------------


def test_pull_request_user_login_scrubbed() -> None:
    pr: dict[str, Any] = {"id": 1, "number": 1, "state": "open", "user": _user(), "body": "hi"}
    result = redact_payload(_payload(pull_request=pr))
    assert result["pull_request"]["user"]["login"] == _REDACTED


def test_pull_request_body_scrubbed() -> None:
    pr: dict[str, Any] = {"id": 1, "number": 1, "state": "open", "user": _user(), "body": "secret content"}
    result = redact_payload(_payload(pull_request=pr))
    assert result["pull_request"]["body"] == _REDACTED


def test_pull_request_null_body_tolerated() -> None:
    pr: dict[str, Any] = {"id": 1, "number": 1, "state": "open", "user": _user(), "body": None}
    result = redact_payload(_payload(pull_request=pr))
    assert result["pull_request"]["body"] is None


# ---------------------------------------------------------------------------
# issue
# ---------------------------------------------------------------------------


def test_issue_user_login_scrubbed() -> None:
    issue: dict[str, Any] = {"id": 1, "number": 1, "user": _user(), "body": "content"}
    result = redact_payload(_payload(issue=issue))
    assert result["issue"]["user"]["login"] == _REDACTED


def test_issue_body_scrubbed() -> None:
    issue: dict[str, Any] = {"id": 1, "number": 1, "user": _user(), "body": "secret info"}
    result = redact_payload(_payload(issue=issue))
    assert result["issue"]["body"] == _REDACTED


# ---------------------------------------------------------------------------
# comment
# ---------------------------------------------------------------------------


def test_comment_body_scrubbed() -> None:
    comment: dict[str, Any] = {"id": 1, "user": _user(), "body": "private message"}
    result = redact_payload(_payload(comment=comment))
    assert result["comment"]["body"] == _REDACTED


def test_comment_user_login_scrubbed() -> None:
    comment: dict[str, Any] = {"id": 1, "user": _user(login="bob"), "body": "hi"}
    result = redact_payload(_payload(comment=comment))
    assert result["comment"]["user"]["login"] == _REDACTED


# ---------------------------------------------------------------------------
# release
# ---------------------------------------------------------------------------


def test_release_author_login_scrubbed() -> None:
    release: dict[str, Any] = {"id": 1, "tag_name": "v1.0", "author": _user()}
    result = redact_payload(_payload(release=release))
    assert result["release"]["author"]["login"] == _REDACTED


def test_release_author_email_scrubbed() -> None:
    release: dict[str, Any] = {"id": 1, "tag_name": "v1.0", "author": _user()}
    result = redact_payload(_payload(release=release))
    assert result["release"]["author"]["email"] == _REDACTED


def test_release_tag_name_preserved() -> None:
    release: dict[str, Any] = {"id": 1, "tag_name": "v1.0", "author": _user()}
    result = redact_payload(_payload(release=release))
    assert result["release"]["tag_name"] == "v1.0"


# ---------------------------------------------------------------------------
# header redactor
# ---------------------------------------------------------------------------


def test_hub_signature_256_scrubbed() -> None:
    headers = {"X-Hub-Signature-256": "sha256=abc123", "Content-Type": "application/json"}
    result = redact_headers(headers)
    assert result["X-Hub-Signature-256"] == _HEADER_REDACTED


def test_hub_signature_scrubbed() -> None:
    headers = {"X-Hub-Signature": "sha1=deadbeef"}
    result = redact_headers(headers)
    assert result["X-Hub-Signature"] == _HEADER_REDACTED


def test_github_delivery_scrubbed() -> None:
    headers = {"X-GitHub-Delivery": "abc-123-def"}
    result = redact_headers(headers)
    assert result["X-GitHub-Delivery"] == _HEADER_REDACTED


def test_github_event_scrubbed() -> None:
    headers = {"X-GitHub-Event": "workflow_run"}
    result = redact_headers(headers)
    assert result["X-GitHub-Event"] == _HEADER_REDACTED


def test_github_hook_id_scrubbed() -> None:
    headers = {"X-GitHub-Hook-ID": "12345"}
    result = redact_headers(headers)
    assert result["X-GitHub-Hook-ID"] == _HEADER_REDACTED


def test_github_hook_installation_target_id_scrubbed() -> None:
    headers = {"X-GitHub-Hook-Installation-Target-ID": "99"}
    result = redact_headers(headers)
    assert result["X-GitHub-Hook-Installation-Target-ID"] == _HEADER_REDACTED


def test_github_hook_installation_target_type_scrubbed() -> None:
    headers = {"X-GitHub-Hook-Installation-Target-Type": "repository"}
    result = redact_headers(headers)
    assert result["X-GitHub-Hook-Installation-Target-Type"] == _HEADER_REDACTED


def test_content_type_header_preserved() -> None:
    headers = {"X-Hub-Signature-256": "sha256=abc", "Content-Type": "application/json"}
    result = redact_headers(headers)
    assert result["Content-Type"] == "application/json"


def test_headers_not_mutated() -> None:
    headers = {"X-Hub-Signature-256": "sha256=abc123"}
    original = dict(headers)
    redact_headers(headers)
    assert headers == original


# ---------------------------------------------------------------------------
# Idempotency
# ---------------------------------------------------------------------------


def test_idempotent_sender() -> None:
    payload = _payload(sender=_user())
    once = redact_payload(payload)
    twice = redact_payload(once)
    assert once == twice


def test_idempotent_commits() -> None:
    payload = _payload(commits=[_commit(), _commit(message="another")])
    once = redact_payload(payload)
    twice = redact_payload(once)
    assert once == twice


def test_idempotent_pull_request() -> None:
    pr: dict[str, Any] = {"id": 1, "number": 1, "state": "open", "user": _user(), "body": "text"}
    once = redact_payload(_payload(pull_request=pr))
    twice = redact_payload(once)
    assert once == twice


# ---------------------------------------------------------------------------
# Mutation safety (input dict must not be modified)
# ---------------------------------------------------------------------------


def test_payload_not_mutated() -> None:
    payload = _payload(sender=_user(login="alice"), repository=_repo())
    original_login = payload["sender"]["login"]
    redact_payload(payload)
    assert payload["sender"]["login"] == original_login


def test_commits_not_mutated() -> None:
    commit = _commit(message="original message")
    payload = _payload(commits=[commit])
    original_msg = payload["commits"][0]["message"]
    redact_payload(payload)
    assert payload["commits"][0]["message"] == original_msg
