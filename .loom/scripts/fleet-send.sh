#!/usr/bin/env bash
# fleet-send.sh — post ONE safehouse envelope over the AF_UNIX socket, for
# lifecycle role subagents (Builder / Judge / Doctor) whose Claude-Code tool
# allowlists exclude MCP tools and therefore can't reach the session-injected
# `safehouse_send` MCP tool (issue #4199, phase 2 of #4196 / #3997).
#
# It is a thin JSON-lines client mirroring the daemon's reference implementation
# (loom-daemon/src/safehouse.rs: `hello`, `build_send_request`): connect, send
# the mandatory `hello` handshake, then one `send` request.
#
# HARD degradation contract — this helper MUST NEVER block, stall, or fail a
# role. Absent env vars, an unresolvable socket, an invalid argument, or any
# connect/hello/send failure ⇒ **exit 0 SILENTLY** (no stdout, no stderr).
# Treat "posted" as best-effort; the room is optional, the role's work is not.
#
# Usage:
#   fleet-send.sh --task-id <id> --type chat|task|handoff|ack \
#                 --body <text> [--to <persona|*>] [--room <name>]
#
# Resolution:
#   socket  = $SAFEHOUSED_SOCKET  (fallback $LOOM_SAFEHOUSE_SOCKET)
#   persona = $SAFEHOUSE_PERSONA
#
# Both are exported into the worker session by spawn-claude.sh alongside the
# safehouse MCP injection; a plain shell without them is the common case and
# exits 0. `task_id` should be the repo-qualified form the daemon narrates on
# (`<repo>_<issue>`, e.g. `loom_4199`, post-#4224) so role posts thread with the
# daemon's dispatch narration; the helper only enforces the `[A-Za-z0-9_]`
# charset.

# Deliberately NOT `set -e` — every failure path must fall through to exit 0.
set -u

# Any unexpected error, signal, or EXIT ⇒ silent success. Belt-and-suspenders
# on top of the explicit `exit 0`s below.
trap 'exit 0' ERR
trap 'exit 0' EXIT

# --- Parse args -------------------------------------------------------------
_task_id=""
_type=""
_body=""
_to="*"
_room=""

while [[ $# -gt 0 ]]; do
    case "$1" in
        --task-id) _task_id="${2:-}"; shift 2 || exit 0 ;;
        --task-id=*) _task_id="${1#*=}"; shift ;;
        --type) _type="${2:-}"; shift 2 || exit 0 ;;
        --type=*) _type="${1#*=}"; shift ;;
        --body) _body="${2:-}"; shift 2 || exit 0 ;;
        --body=*) _body="${1#*=}"; shift ;;
        --to) _to="${2:-}"; shift 2 || exit 0 ;;
        --to=*) _to="${1#*=}"; shift ;;
        --room) _room="${2:-}"; shift 2 || exit 0 ;;
        --room=*) _room="${1#*=}"; shift ;;
        *) shift ;; # ignore unknown args — never fail a role over a typo
    esac
done

# --- Resolve socket + persona (silent no-op when absent) --------------------
_socket="${SAFEHOUSED_SOCKET:-${LOOM_SAFEHOUSE_SOCKET:-}}"
_persona="${SAFEHOUSE_PERSONA:-}"

[[ -z "$_socket" || -z "$_persona" ]] && exit 0
[[ -z "$_body" ]] && exit 0

# --- Validate BEFORE opening the socket (matches safehouse.rs) --------------
# `type` must be one of the closed envelope-v1 enum.
case "$_type" in
    chat | task | handoff | ack) ;;
    *) exit 0 ;; # invalid type ⇒ reject locally, no socket write
esac

# `task_id`, when present, must be [A-Za-z0-9_].
if [[ -n "$_task_id" && "$_task_id" =~ [^A-Za-z0-9_] ]]; then
    exit 0
fi

# --- Send over the socket (best-effort; python does the AF_UNIX I/O) --------
SAFEHOUSE_FS_SOCKET="$_socket" \
    SAFEHOUSE_FS_PERSONA="$_persona" \
    SAFEHOUSE_FS_TASK_ID="$_task_id" \
    SAFEHOUSE_FS_TYPE="$_type" \
    SAFEHOUSE_FS_BODY="$_body" \
    SAFEHOUSE_FS_TO="$_to" \
    SAFEHOUSE_FS_ROOM="$_room" \
    "${LOOM_PYTHON:-python3}" - <<'PY' 2>/dev/null || exit 0
import json, os, socket, sys

sock_path = os.environ.get("SAFEHOUSE_FS_SOCKET", "")
persona = os.environ.get("SAFEHOUSE_FS_PERSONA", "")
task_id = os.environ.get("SAFEHOUSE_FS_TASK_ID", "")
kind = os.environ.get("SAFEHOUSE_FS_TYPE", "")
body = os.environ.get("SAFEHOUSE_FS_BODY", "")
to = os.environ.get("SAFEHOUSE_FS_TO", "*") or "*"
room = os.environ.get("SAFEHOUSE_FS_ROOM", "")

ENVELOPE_VERSION = 1


def normalize_to(value):
    # "*" and @matrix-ids pass through; a persona is lowercased and hyphens
    # folded to underscores (the wire form), mirroring safehouse.rs.
    if value == "*" or value.startswith("@"):
        return value
    return value.lower().replace("-", "_")


def read_reply(reader):
    # Skip interleaved async push lines (they carry an `event` key and no `id`)
    # and return the first genuine reply object, or None on EOF/timeout.
    for line in reader:
        line = line.strip()
        if not line:
            continue
        try:
            msg = json.loads(line)
        except Exception:
            continue
        if isinstance(msg, dict) and "event" in msg and "id" not in msg:
            continue
        return msg
    return None


try:
    s = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    s.settimeout(5.0)
    s.connect(sock_path)
except Exception:
    sys.exit(0)

try:
    reader = s.makefile("r", encoding="utf-8")

    def write_line(obj):
        s.sendall((json.dumps(obj) + "\n").encode("utf-8"))

    # Mandatory hello handshake.
    write_line({"id": 0, "op": "hello", "persona": persona})
    reply = read_reply(reader)
    if not (isinstance(reply, dict) and reply.get("ok") is True):
        sys.exit(0)

    # send request (id=1). Emit `v:1` and omit task_id/room when absent, exactly
    # like build_send_request.
    req = {
        "id": 1,
        "op": "send",
        "v": ENVELOPE_VERSION,
        "to": normalize_to(to),
        "type": kind,
        "body": body,
    }
    if task_id:
        req["task_id"] = task_id
    if room:
        req["room"] = room
    write_line(req)
    # Best-effort: read the send reply so safehoused processes the line before
    # we close, but never fail the role on its content.
    read_reply(reader)
except Exception:
    sys.exit(0)
finally:
    try:
        s.close()
    except Exception:
        pass

sys.exit(0)
PY

exit 0
