"""HTTP 接口：认证、授权、校验、版本冲突、限流、幂等触发与可观测端点。"""
from __future__ import annotations

from datetime import timedelta

from app.clock import naive, now
from app.models import Job, Task
from app.security import LIMITER

BASE = "/api/v1/jobs"


def job_payload(**overrides):
    payload = {
        "name": "report-hourly",
        "schedule_type": "cron",
        "cron_expr": "0 * * * *",
        "timezone": "Asia/Shanghai",
        "executor": "shell",
        "command": "echo report",
        "shard_count": 2,
        "max_concurrency": 2,
        "max_attempts": 2,
    }
    payload.update(overrides)
    return payload


def test_missing_token_is_rejected(client):
    assert client.get(BASE).status_code == 401
    assert client.get("/healthz").status_code == 200


def test_create_and_fetch_job(client, auth):
    created = client.post(BASE, json=job_payload(), headers=auth)
    assert created.status_code == 201, created.text
    body = created.json()
    assert body["name"] == "report-hourly"
    assert body["next_fire_at"].endswith("Z")
    assert body["version"] == 1

    fetched = client.get(f"{BASE}/{body['id']}", headers=auth)
    assert fetched.status_code == 200
    assert fetched.json()["id"] == body["id"]

    listed = client.get(BASE, headers=auth).json()
    assert listed["total"] == 1
    assert listed["items"][0]["id"] == body["id"]


def test_duplicate_name_conflicts(client, auth):
    assert client.post(BASE, json=job_payload(), headers=auth).status_code == 201
    again = client.post(BASE, json=job_payload(), headers=auth)
    assert again.status_code == 409


def test_invalid_cron_is_rejected(client, auth):
    response = client.post(BASE, json=job_payload(cron_expr="不是 cron"), headers=auth)
    assert response.status_code == 422


def test_shell_executor_requires_command(client, auth):
    response = client.post(BASE, json=job_payload(command="  "), headers=auth)
    assert response.status_code == 422


def test_pagination(client, auth):
    for idx in range(3):
        assert (
            client.post(BASE, json=job_payload(name=f"job-{idx}"), headers=auth).status_code
            == 201
        )
    page = client.get(BASE, params={"page": 2, "page_size": 2}, headers=auth).json()
    assert page["total"] == 3
    assert len(page["items"]) == 1


def test_optimistic_lock_rejects_stale_update(client, auth):
    job = client.post(BASE, json=job_payload(), headers=auth).json()
    first = client.patch(
        f"{BASE}/{job['id']}", json={"description": "v2", "version": 1}, headers=auth
    )
    assert first.status_code == 200
    assert first.json()["version"] == 2

    stale = client.patch(
        f"{BASE}/{job['id']}", json={"description": "v3", "version": 1}, headers=auth
    )
    assert stale.status_code == 409


def test_manual_trigger_creates_shards(client, auth):
    job = client.post(BASE, json=job_payload(shard_count=3), headers=auth).json()
    response = client.post(f"{BASE}/{job['id']}/run", json={}, headers=auth)
    assert response.status_code == 202
    tasks = response.json()
    assert len(tasks) == 3
    assert {t["shard_index"] for t in tasks} == {0, 1, 2}
    assert all(t["status"] == "PENDING" for t in tasks)

    second = client.post(f"{BASE}/{job['id']}/run", json={"shard_count": 1}, headers=auth)
    assert len(second.json()) == 1


def test_pause_and_resume(client, auth):
    job = client.post(BASE, json=job_payload(), headers=auth).json()
    paused = client.post(f"{BASE}/{job['id']}/pause", headers=auth).json()
    assert paused["enabled"] is False
    assert paused["next_fire_at"] is None
    resumed = client.post(f"{BASE}/{job['id']}/resume", headers=auth).json()
    assert resumed["enabled"] is True
    assert resumed["next_fire_at"] is not None


def test_run_detail_replay_and_cancel(client, auth):
    job = client.post(BASE, json=job_payload(), headers=auth).json()
    task = client.post(f"{BASE}/{job['id']}/run", json={}, headers=auth).json()[0]

    detail = client.get(f"/api/v1/runs/{task['id']}", headers=auth)
    assert detail.status_code == 200
    assert detail.json()["idempotency_key"].startswith(job["id"])

    cancelled = client.post(f"/api/v1/runs/{task['id']}/cancel", headers=auth)
    assert cancelled.status_code == 200
    assert cancelled.json()["status"] == "CANCELLED"

    again = client.post(f"/api/v1/runs/{task['id']}/cancel", headers=auth)
    assert again.status_code == 409

    replayed = client.post(f"/api/v1/runs/{task['id']}/replay", headers=auth)
    assert replayed.status_code == 200
    assert replayed.json()["status"] == "PENDING"

    runs = client.get("/api/v1/runs", params={"job_id": job["id"]}, headers=auth).json()
    assert runs["total"] >= 1

    bad_filter = client.get("/api/v1/runs", params={"status": "NOPE"}, headers=auth)
    assert bad_filter.status_code == 422


def test_delete_running_job_conflicts(client, auth, db):
    job = client.post(BASE, json=job_payload(), headers=auth).json()
    task = client.post(f"{BASE}/{job['id']}/run", json={}, headers=auth).json()[0]

    record = db.get(Task, task["id"])
    record.status = "RUNNING"
    record.lease_expires_at = naive(now()) + timedelta(seconds=30)
    db.commit()

    blocked = client.delete(f"{BASE}/{job['id']}", headers=auth)
    assert blocked.status_code == 409

    record.status = "CANCELLED"
    db.commit()
    assert client.delete(f"{BASE}/{job['id']}", headers=auth).status_code == 204
    assert client.get(f"{BASE}/{job['id']}", headers=auth).status_code == 404


def test_rate_limit_returns_429(client, auth):
    original = LIMITER.capacity
    LIMITER.capacity = 2
    LIMITER.reset()
    try:
        for idx in range(2):
            assert (
                client.post(BASE, json=job_payload(name=f"rl-{idx}"), headers=auth).status_code
                == 201
            )
        blocked = client.post(BASE, json=job_payload(name="rl-3"), headers=auth)
        assert blocked.status_code == 429
        assert "Retry-After" in blocked.headers
    finally:
        LIMITER.capacity = original
        LIMITER.reset()


def test_viewer_cannot_write(client, auth):
    key = client.post(
        "/api/v1/api-keys", json={"name": "dash-viewer", "role": "viewer"}, headers=auth
    ).json()
    headers = {"Authorization": "Bearer " + key["token"]}
    assert client.get(BASE, headers=headers).status_code == 200
    assert client.post(BASE, json=job_payload(), headers=headers).status_code == 403
    assert client.delete(f"/api/v1/api-keys/{key['id']}", headers=headers).status_code == 403


def test_revoked_key_is_rejected(client, auth):
    key = client.post(
        "/api/v1/api-keys", json={"name": "temp", "role": "operator"}, headers=auth
    ).json()
    assert client.delete(f"/api/v1/api-keys/{key['id']}", headers=auth).status_code == 204
    assert (
        client.get(BASE, headers={"Authorization": "Bearer " + key["token"]}).status_code == 401
    )


def test_observability_endpoints(client, auth):
    client.post(BASE, json=job_payload(), headers=auth)
    stats = client.get("/api/v1/stats", headers=auth).json()
    assert stats["jobs"]["total"] == 1
    assert "leader" in stats

    metrics = client.get("/metrics")
    assert metrics.status_code == 200
    assert "tempo_queue_depth" in metrics.text
    assert "tempo_http_requests_total" in metrics.text

    events = client.get("/api/v1/events", headers=auth).json()
    assert any(e["type"] == "JOB_CREATED" for e in events["items"])

    audit = client.get("/api/v1/audit", headers=auth).json()
    assert any(a["action"] == "JOB_CREATE" for a in audit["items"])


def test_force_tick_endpoint(client, auth, db):
    job = client.post(BASE, json=job_payload(), headers=auth).json()
    record = db.get(Job, job["id"])
    record.next_fire_at = naive(now()) - timedelta(seconds=30)
    db.commit()

    response = client.post("/api/v1/admin/tick", headers=auth)
    assert response.status_code == 200
    assert response.json()["leader"] is True
    assert response.json()["tasks_created"] >= 1
