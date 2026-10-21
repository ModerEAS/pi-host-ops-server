#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""pi-host-ops box worker: heartbeat + long-poll → pi_svc / shell → events + result.

Uses per-agent AGENT_TOKEN (id from token). Respects delivery_mode: steer|queue.
Stdlib only. Never stops other pi_svc sessions. Never uses Fast / CloudAgent.
"""

from __future__ import annotations

import json
import logging
import os
import signal
import subprocess
import sys
import tempfile
import time
import urllib.error
import urllib.request
from pathlib import Path
from typing import Any, Optional

LOG = logging.getLogger("host_ops_worker")

MAX_RESULT_BYTES = 512 * 1024
DEFAULT_SHELL_TIMEOUT = 60
DEFAULT_PI_TIMEOUT = 300
POLL_STATUS_INTERVAL = 2.0
BACKOFF_START = 1.0
BACKOFF_MAX = 30.0

# Job types that go through pi_svc (shell prefers local subprocess).
PI_JOB_TYPES = frozenset({"observe", "act", "open", "clipboard", "notify"})


class Config:
    def __init__(self) -> None:
        self.host_ops_url = os.environ.get("HOST_OPS_URL", "http://127.0.0.1:8787").rstrip("/")
        self.agent_token = os.environ.get("AGENT_TOKEN", "").strip()
        self.agent_id = os.environ.get("AGENT_ID", "box-pi").strip() or "box-pi"
        self.pi_svc = os.environ.get("PI_SVC", "/workspace/pi-runs/pi_svc").strip()
        self.pi_session = os.environ.get("PI_SESSION", "host-ops").strip() or "host-ops"
        self.work_dir = os.environ.get(
            "WORK_DIR", "/workspace/pi-host-ops-server/workspace"
        ).strip()
        self.poll_wait_secs = int(os.environ.get("POLL_WAIT_SECS", "20"))
        self.heartbeat_secs = float(os.environ.get("HEARTBEAT_SECS", "20"))
        self.event_interval_secs = float(os.environ.get("EVENT_INTERVAL_SECS", "5"))
        self.provider = os.environ.get("PI_PROVIDER", "opencode-go").strip()
        self.model = os.environ.get("PI_MODEL", "muse-spark-1.3-contributor").strip()
        # Safety: never Fast
        if "fast" in self.model.lower() or "fast" in self.provider.lower():
            raise SystemExit("PI_MODEL/PI_PROVIDER must not be Fast (cost rule)")

    def validate(self) -> None:
        if not self.agent_token:
            raise SystemExit("AGENT_TOKEN is required")
        if not Path(self.pi_svc).is_file():
            raise SystemExit(f"PI_SVC not found: {self.pi_svc}")
        Path(self.work_dir).mkdir(parents=True, exist_ok=True)


_STOP = False


def _on_signal(signum: int, _frame: Any) -> None:
    global _STOP
    LOG.info("signal %s received, will exit after current job", signum)
    _STOP = True


def truncate_result(obj: Any) -> Any:
    """Ensure JSON-serialized result stays under MAX_RESULT_BYTES."""
    raw = json.dumps(obj, ensure_ascii=False)
    if len(raw.encode("utf-8")) <= MAX_RESULT_BYTES:
        return obj
    # Prefer trimming text_tail / stdout / stderr fields
    if isinstance(obj, dict):
        out = dict(obj)
        for key in ("text_tail", "stdout", "stderr", "status", "output"):
            if key in out and isinstance(out[key], str):
                # Keep head + note
                keep = max(1024, MAX_RESULT_BYTES // 4)
                text = out[key]
                if len(text.encode("utf-8")) > keep:
                    out[key] = text[:keep] + "\n…[truncated for MAX_RESULT_BYTES]"
                    out["truncated"] = True
        raw2 = json.dumps(out, ensure_ascii=False)
        if len(raw2.encode("utf-8")) <= MAX_RESULT_BYTES:
            return out
        # Last resort: tiny summary
        return {
            "truncated": True,
            "note": "result exceeded 512KB; see pi_svc logs",
            "keys": list(obj.keys()) if isinstance(obj, dict) else type(obj).__name__,
        }
    return {"truncated": True, "note": "result too large"}


def http_json(
    method: str,
    url: str,
    token: str,
    body: Optional[dict] = None,
    agent_id: Optional[str] = None,
    timeout: float = 60.0,
) -> tuple[int, Any]:
    data = None
    headers = {
        "Authorization": f"Bearer {token}",
        "Accept": "application/json",
    }
    if agent_id:
        headers["X-Agent-Id"] = agent_id
    if body is not None:
        data = json.dumps(body).encode("utf-8")
        headers["Content-Type"] = "application/json"
    req = urllib.request.Request(url, data=data, headers=headers, method=method)
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            status = resp.getcode()
            raw = resp.read()
            if not raw:
                return status, None
            try:
                return status, json.loads(raw.decode("utf-8"))
            except json.JSONDecodeError:
                return status, raw.decode("utf-8", errors="replace")
    except urllib.error.HTTPError as e:
        raw = e.read()
        try:
            parsed = json.loads(raw.decode("utf-8")) if raw else None
        except json.JSONDecodeError:
            parsed = raw.decode("utf-8", errors="replace") if raw else None
        return e.code, parsed
    except urllib.error.URLError as e:
        raise ConnectionError(str(e.reason if hasattr(e, "reason") else e)) from e


def run_cmd(
    argv: list[str],
    timeout: Optional[float] = None,
    cwd: Optional[str] = None,
) -> tuple[int, str, str]:
    try:
        p = subprocess.run(
            argv,
            capture_output=True,
            text=True,
            timeout=timeout,
            cwd=cwd,
            check=False,
        )
        return p.returncode, p.stdout or "", p.stderr or ""
    except subprocess.TimeoutExpired as e:
        out = (e.stdout or "") if isinstance(e.stdout, str) else ""
        err = (e.stderr or "") if isinstance(e.stderr, str) else f"timeout after {timeout}s"
        return 124, out, err


def pi_svc(cfg: Config, *args: str, timeout: float = 120.0) -> tuple[int, str, str]:
    return run_cmd([cfg.pi_svc, *args], timeout=timeout)


def session_running(cfg: Config) -> bool:
    """True if our named session daemon is alive. Never touches other sessions."""
    code, out, _err = pi_svc(cfg, "list", timeout=30.0)
    if code != 0:
        return False
    try:
        sessions = json.loads(out)
    except json.JSONDecodeError:
        return False
    if not isinstance(sessions, list):
        return False
    for s in sessions:
        if isinstance(s, dict) and s.get("name") == cfg.pi_session:
            return bool(s.get("running"))
    return False


def ensure_session(cfg: Config) -> None:
    if session_running(cfg):
        LOG.info("reusing pi_svc session %s", cfg.pi_session)
        return
    LOG.info(
        "starting pi_svc session %s cwd=%s model=%s/%s",
        cfg.pi_session,
        cfg.work_dir,
        cfg.provider,
        cfg.model,
    )
    code, out, err = pi_svc(
        cfg,
        "start",
        cfg.pi_session,
        "--cwd",
        cfg.work_dir,
        "--provider",
        cfg.provider,
        "--model",
        cfg.model,
        timeout=120.0,
    )
    if code != 0:
        raise RuntimeError(f"pi_svc start failed ({code}): {err or out}")
    # Wait briefly for idle/running
    for _ in range(30):
        st = get_status(cfg)
        state = (st or {}).get("status", {}).get("state") or (st or {}).get("state")
        if state in ("idle", "running", "starting"):
            if state != "starting":
                return
        time.sleep(0.5)
    LOG.warning("pi_svc session %s start did not reach idle quickly; continuing", cfg.pi_session)


def get_status(cfg: Config) -> Optional[dict]:
    code, out, err = pi_svc(cfg, "status", cfg.pi_session, timeout=30.0)
    if code != 0:
        LOG.warning("status failed: %s", err or out)
        return None
    try:
        return json.loads(out)
    except json.JSONDecodeError:
        return {"raw": out}


def wait_until_idle(cfg: Config, timeout_secs: float, settled_before: Optional[int] = None) -> dict:
    """Poll status until the prompt settles (idle after work), or error/timeout."""
    deadline = time.monotonic() + max(1.0, timeout_secs)
    last: dict = {}
    saw_running = False
    while time.monotonic() < deadline:
        if _STOP:
            break
        st = get_status(cfg) or {}
        last = st
        inner = st.get("status") if isinstance(st.get("status"), dict) else st
        if not isinstance(inner, dict):
            inner = {}
        state = inner.get("state")
        settled = inner.get("settled_count")
        if state == "running":
            saw_running = True
        if state in ("error", "dead", "stopped"):
            return last
        if state == "idle":
            bumped = isinstance(settled, int) and settled_before is not None and settled > settled_before
            if bumped or saw_running:
                return last
            # Still idle with same settled_count: prompt may not have started yet.
        time.sleep(POLL_STATUS_INTERVAL)
    last["_wait_timeout"] = True
    return last


def build_pi_prompt(job: dict) -> str:
    job_type = job.get("type") or job.get("job_type") or "unknown"
    job_id = job.get("id", "?")
    payload = job.get("payload") or {}
    timeout = job.get("timeout_secs")
    payload_json = json.dumps(payload, ensure_ascii=False, indent=2)
    return f"""你是 host-ops box worker 驱动的 Pi 会话。请完成下面这一条远程任务，然后用简短中文+英文总结结果（不要开新 session，不要 Fast）。

You are driven by the host-ops worker. Complete this single remote job, then summarize briefly in Chinese + English. Do not start other sessions. Do not use Fast.

job_id: {job_id}
type: {job_type}
timeout_secs: {timeout}

payload:
```json
{payload_json}
```

任务类型说明 / type hints:
- observe: 观察当前环境（cwd、关键文件、进程摘要等），回报可读摘要。 Observe environment; return a readable summary.
- act: 按 payload 执行具体操作（编辑/运行等）。 Perform the action described in payload.
- open: 打开/定位 payload 中的路径或 URL（在本机 box 文件系统语义下）。 Open/locate path or URL from payload.
- clipboard: 读写剪贴板相关请求（若本机无 GUI 剪贴板则说明不可用）。 Clipboard get/set if available.
- notify: 记录/发送一条通知类消息到日志或合理通道。 Emit a notification/log of the message.

完成后停在 idle；把关键输出写在最终回复里。 When done, settle idle; put key output in your final reply.
"""



def post_event(
    cfg: Config,
    job_id: str,
    event_type: str,
    message: Optional[str] = None,
    data: Optional[dict] = None,
) -> None:
    if not job_id:
        return
    body: dict[str, Any] = {"type": event_type}
    if message is not None:
        body["message"] = message[:8000]
    if data is not None:
        body["data"] = data
    url = f"{cfg.host_ops_url}/v1/agent/jobs/{job_id}/events"
    try:
        status, resp = http_json(
            "POST", url, cfg.agent_token, body=body, agent_id=cfg.agent_id, timeout=15.0
        )
        if status >= 400:
            LOG.debug("post_event HTTP %s: %s", status, resp)
    except Exception as e:
        LOG.debug("post_event failed: %s", e)


def heartbeat(cfg: Config) -> None:
    url = f"{cfg.host_ops_url}/v1/agent/heartbeat"
    try:
        status, resp = http_json(
            "POST",
            url,
            cfg.agent_token,
            body={"status": "online"},
            agent_id=cfg.agent_id,
            timeout=15.0,
        )
        if status >= 400:
            LOG.warning("heartbeat HTTP %s: %s", status, resp)
    except Exception as e:
        LOG.warning("heartbeat failed: %s", e)


def wait_until_idle_with_events(
    cfg: Config,
    job_id: str,
    timeout_secs: float,
    settled_before: Optional[int] = None,
) -> dict:
    """Poll status until idle; periodically POST text/tool events to the server."""
    deadline = time.monotonic() + max(1.0, timeout_secs)
    last: dict = {}
    saw_running = False
    last_event_at = 0.0
    last_tail = ""
    last_tool = None
    while time.monotonic() < deadline:
        if _STOP:
            break
        st = get_status(cfg) or {}
        last = st
        inner = st.get("status") if isinstance(st.get("status"), dict) else st
        if not isinstance(inner, dict):
            inner = {}
        state = inner.get("state")
        settled = inner.get("settled_count")
        tool = inner.get("last_tool")
        text_tail = inner.get("text_tail") or ""
        if state == "running":
            saw_running = True
        now = time.monotonic()
        if now - last_event_at >= cfg.event_interval_secs:
            if tool and tool != last_tool:
                post_event(cfg, job_id, "tool", str(tool), {"last_tool": tool})
                last_tool = tool
            if text_tail and text_tail != last_tail:
                # Send only the delta-ish tail tip
                tip = text_tail[-1500:]
                post_event(cfg, job_id, "text", tip)
                last_tail = text_tail
            post_event(cfg, job_id, "status", f"state={state}", {"state": state, "settled_count": settled})
            # renew lease while waiting
            try:
                http_json(
                    "POST",
                    f"{cfg.host_ops_url}/v1/agent/jobs/{job_id}/lease",
                    cfg.agent_token,
                    body={},
                    agent_id=cfg.agent_id,
                    timeout=15.0,
                )
            except Exception:
                pass
            last_event_at = now
        if state in ("error", "dead", "stopped"):
            return last
        if state == "idle":
            bumped = isinstance(settled, int) and settled_before is not None and settled > settled_before
            if bumped or saw_running:
                return last
        time.sleep(POLL_STATUS_INTERVAL)
    last["_wait_timeout"] = True
    return last


def run_via_pi(cfg: Config, job: dict) -> tuple[bool, Any, Optional[str]]:
    ensure_session(cfg)
    before = get_status(cfg) or {}
    inner_before = before.get("status") if isinstance(before.get("status"), dict) else before
    settled_before = inner_before.get("settled_count")
    if not isinstance(settled_before, int):
        settled_before = None

    prompt = build_pi_prompt(job)
    timeout_secs = job.get("timeout_secs") or DEFAULT_PI_TIMEOUT
    try:
        timeout_secs = float(timeout_secs)
    except (TypeError, ValueError):
        timeout_secs = float(DEFAULT_PI_TIMEOUT)

    with tempfile.NamedTemporaryFile(
        mode="w",
        encoding="utf-8",
        suffix=".txt",
        prefix=f"host-ops-{job.get('id', 'job')}-",
        dir=cfg.work_dir,
        delete=False,
    ) as f:
        f.write(prompt)
        prompt_path = f.name

    delivery = (job.get("delivery_mode") or "steer").strip().lower()
    job_id = str(job.get("id", ""))

    try:
        st_now = get_status(cfg) or {}
        inner = st_now.get("status") if isinstance(st_now.get("status"), dict) else st_now
        state = inner.get("state") if isinstance(inner, dict) else None

        # Explicit delivery_mode from server (default steer).
        # queue + busy → --follow-up; steer → --steer; queue + idle → normal send.
        send_args = ["send", cfg.pi_session, "--file", prompt_path]
        if delivery == "steer":
            send_args = ["send", cfg.pi_session, "--steer", "--file", prompt_path]
            LOG.info("delivery_mode=steer → send --steer")
        elif state == "running":
            send_args = ["send", cfg.pi_session, "--follow-up", "--file", prompt_path]
            LOG.info("delivery_mode=queue + busy → send --follow-up")
        else:
            LOG.info("delivery_mode=queue + idle → send")

        post_event(cfg, job_id, "status", f"pi_svc {' '.join(send_args[2:4])}", {"delivery_mode": delivery})

        code, out, err = pi_svc(cfg, *send_args, timeout=min(120.0, timeout_secs + 30))
        if code != 0:
            return False, None, f"pi_svc send failed ({code}): {err or out}"

        final = wait_until_idle_with_events(
            cfg, job_id, timeout_secs, settled_before=settled_before
        )
        inner_f = final.get("status") if isinstance(final.get("status"), dict) else final
        text_tail = ""
        if isinstance(inner_f, dict):
            text_tail = inner_f.get("text_tail") or ""
        tcode, tout, _ = pi_svc(cfg, "tail", cfg.pi_session, "--text", "-n", "40", timeout=30.0)
        text_log = tout if tcode == 0 else ""

        result = {
            "via": "pi_svc",
            "session": cfg.pi_session,
            "delivery_mode": delivery,
            "state": (inner_f or {}).get("state") if isinstance(inner_f, dict) else None,
            "settled_count": (inner_f or {}).get("settled_count") if isinstance(inner_f, dict) else None,
            "last_tool": (inner_f or {}).get("last_tool") if isinstance(inner_f, dict) else None,
            "text_tail": text_tail,
            "text_log_tail": text_log[-8000:] if text_log else "",
            "wait_timeout": bool(final.get("_wait_timeout")),
            "send_stdout": out[:2000] if out else "",
        }
        ok = True
        err_msg = None
        if isinstance(inner_f, dict) and inner_f.get("state") in ("error", "dead"):
            ok = False
            err_msg = inner_f.get("error") or f"session state={inner_f.get('state')}"
        elif final.get("_wait_timeout"):
            ok = False
            err_msg = f"timed out waiting for idle after {timeout_secs}s"
        return ok, truncate_result(result), err_msg
    finally:
        try:
            os.unlink(prompt_path)
        except OSError:
            pass


def run_shell_local(job: dict) -> tuple[bool, Any, Optional[str]]:
    payload = job.get("payload") or {}
    command = payload.get("command")
    if not command or not isinstance(command, str):
        return False, None, "shell job missing payload.command string"

    timeout = job.get("timeout_secs") or payload.get("timeout_secs") or DEFAULT_SHELL_TIMEOUT
    try:
        timeout = float(timeout)
    except (TypeError, ValueError):
        timeout = float(DEFAULT_SHELL_TIMEOUT)

    cwd = payload.get("cwd")
    if cwd is not None and not isinstance(cwd, str):
        cwd = None

    LOG.info("local shell: %s (timeout=%s)", command[:200], timeout)
    try:
        p = subprocess.run(
            command,
            shell=True,
            capture_output=True,
            text=True,
            timeout=timeout,
            cwd=cwd,
            check=False,
        )
        result = {
            "via": "local_shell",
            "command": command,
            "cwd": cwd,
            "exit_code": p.returncode,
            "stdout": (p.stdout or "")[-200000:],
            "stderr": (p.stderr or "")[-50000:],
        }
        ok = p.returncode == 0
        err = None if ok else f"exit_code={p.returncode}"
        return ok, truncate_result(result), err
    except subprocess.TimeoutExpired as e:
        return (
            False,
            truncate_result(
                {
                    "via": "local_shell",
                    "command": command,
                    "timeout": True,
                    "stdout": (e.stdout or "") if isinstance(e.stdout, str) else "",
                    "stderr": (e.stderr or "") if isinstance(e.stderr, str) else "",
                }
            ),
            f"shell timed out after {timeout}s",
        )


def execute_job(cfg: Config, job: dict) -> tuple[bool, Any, Optional[str]]:
    job_type = job.get("type") or ""
    LOG.info("execute job id=%s type=%s", job.get("id"), job_type)

    if job_type == "shell":
        payload = job.get("payload") or {}
        if isinstance(payload.get("command"), str) and payload.get("command").strip():
            return run_shell_local(job)
        # No command → ask Pi
        return run_via_pi(cfg, job)

    if job_type in PI_JOB_TYPES:
        return run_via_pi(cfg, job)

    return False, None, f"unknown job type: {job_type}"


def submit_result(
    cfg: Config, job_id: str, ok: bool, result: Any, error: Optional[str]
) -> None:
    body: dict[str, Any] = {"ok": ok}
    if result is not None:
        body["result"] = truncate_result(result)
    if not ok:
        body["error"] = error or "failed"
    elif error:
        body["error"] = error

    url = f"{cfg.host_ops_url}/v1/agent/jobs/{job_id}/result"
    status, resp = http_json(
        "POST",
        url,
        cfg.agent_token,
        body=body,
        agent_id=cfg.agent_id,
        timeout=60.0,
    )
    if status >= 400:
        LOG.error("submit_result HTTP %s: %s", status, resp)
    else:
        LOG.info("submitted result for %s ok=%s status=%s", job_id, ok, status)


def poll_once(cfg: Config) -> Optional[dict]:
    url = f"{cfg.host_ops_url}/v1/agent/poll"
    # Server long-polls up to wait_secs (max 25); client timeout must be larger
    client_timeout = float(cfg.poll_wait_secs) + 15.0
    status, resp = http_json(
        "POST",
        url,
        cfg.agent_token,
        body={"wait_secs": cfg.poll_wait_secs},
        agent_id=cfg.agent_id,
        timeout=client_timeout,
    )
    if status == 204:
        return None
    if status == 200 and isinstance(resp, dict) and "job" in resp:
        return resp["job"]
    if status == 200 and isinstance(resp, dict) and "id" in resp:
        # tolerate bare job object
        return resp
    if status >= 400:
        raise ConnectionError(f"poll HTTP {status}: {resp}")
    return None


def main() -> int:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    cfg = Config()
    cfg.validate()

    signal.signal(signal.SIGINT, _on_signal)
    signal.signal(signal.SIGTERM, _on_signal)

    LOG.info(
        "host_ops_worker starting url=%s agent_id=%s session=%s work_dir=%s",
        cfg.host_ops_url,
        cfg.agent_id,
        cfg.pi_session,
        cfg.work_dir,
    )

    backoff = BACKOFF_START
    last_hb = 0.0
    while not _STOP:
        try:
            now = time.monotonic()
            if now - last_hb >= cfg.heartbeat_secs:
                heartbeat(cfg)
                last_hb = now
            job = poll_once(cfg)
            backoff = BACKOFF_START
            if job is None:
                continue
            job_id = str(job.get("id", ""))
            if not job_id:
                LOG.error("claimed job without id: %s", job)
                continue
            try:
                ok, result, error = execute_job(cfg, job)
            except Exception as e:
                LOG.exception("execute_job failed")
                ok, result, error = False, None, str(e)
            submit_result(cfg, job_id, ok, result, error)
        except ConnectionError as e:
            LOG.warning("connection error: %s; backoff=%.1fs", e, backoff)
            time.sleep(backoff)
            backoff = min(BACKOFF_MAX, backoff * 2)
        except Exception:
            LOG.exception("loop error; backoff=%.1fs", backoff)
            time.sleep(backoff)
            backoff = min(BACKOFF_MAX, backoff * 2)

    LOG.info("worker stopped")
    return 0


if __name__ == "__main__":
    sys.exit(main())
