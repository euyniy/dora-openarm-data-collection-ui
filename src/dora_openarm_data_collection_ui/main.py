# Copyright 2026 Enactic, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""dora-rs node that provides UI to control data collection with OpenArm."""

import argparse
import asyncio
import collections
from contextlib import asynccontextmanager
import dataclasses
import datetime
import dora
from collections.abc import AsyncIterable
from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.sse import EventSourceResponse, ServerSentEvent
from fastapi.templating import Jinja2Templates
import os
import pathlib
import pyarrow as pa
import time
import uvicorn
import yaml

base_dir = os.path.dirname(__file__)
templates = Jinja2Templates(directory=f"{base_dir}/templates")

node = None

auto_open = False
port = None

# The desktop launcher that supervises `dora run`. The task screen sends the
# operator back to it when this dataflow dies, so that the failure is shown
# on screen instead of only in a terminal.
launcher_url = "http://127.0.0.1:8080/"

# Operation manual shown on the task screen (see --manual-file).
manual = None


# Browser openers, most specific first. "open" is macOS, "xdg-open" is Linux.
_BROWSER_COMMANDS = ("xdg-open", "gio", "open")


@asynccontextmanager
async def _lifespan(app: FastAPI):
    """Open a Web browser automatically if requested."""
    if auto_open:
        url = f"http://127.0.0.1:{port}"
        for command in _BROWSER_COMMANDS:
            argv = [command, "open", url] if command == "gio" else [command, url]
            try:
                await asyncio.create_subprocess_exec(*argv)
                break
            except (FileNotFoundError, OSError):
                continue
    yield


app = FastAPI(lifespan=_lifespan)


@dataclasses.dataclass
class State:
    """The current state."""

    collecting: bool = False
    running: bool = True
    episode_number: int = 0
    task_index: int = 0
    task_title: str = ""
    # The dataset language instruction (tasks[].prompt), kept even when
    # task_title shows the Japanese tasks[].prompt_ja.
    task_prompt: str = ""
    task_description: str = ""
    arm_status_right: str = "stopped"
    arm_status_left: str = "stopped"
    # Failures to show in red on the task screen: newest last.
    # [{"time", "source", "message", "count"}]
    errors: list = dataclasses.field(default_factory=list)


state = State()

# Keep the newest failures only; the launcher log has the full history.
MAX_ERRORS = 20


def _record_error(source, message):
    """Remember a failure for the task screen. Returns True when it is new."""
    message = " ".join(str(message).split())
    if not message:
        return False
    now = datetime.datetime.now().strftime("%H:%M:%S")
    for error in state.errors:
        if error["message"] == message:
            # A repeating failure updates its counter instead of piling up.
            error["count"] += 1
            error["time"] = now
            return False
    state.errors.append(
        {"time": now, "source": str(source), "message": message, "count": 1}
    )
    del state.errors[:-MAX_ERRORS]
    return True


_state_changed = asyncio.Condition()

# Monotonically incremented on every state change.
state_version = 0


CAMERA_INPUTS = (
    "camera_wrist_right",
    "camera_wrist_left",
    "camera_head_left",
    "camera_head_right",
    "camera_ceiling",
)

CAMERA_TIMESTAMP_WINDOW = 60
CAMERA_STALE_AFTER_S = 1.0

# dora-openarm status inputs, one per arm. The input id matches the State field
ARM_STATUS_INPUTS = ("arm_status_right", "arm_status_left")

# VR packet arrival times (ns) published by udp-receiver as
# `vr_receive_times` or `vr_recv_ts` (deprecated).
VR_RECEIVE_TIMES_INPUTS = ("vr_receive_times", "vr_recv_ts")

VR_TIMESTAMP_WINDOW = 120  # ~1.6 s of history at 72 Hz
VR_STALE_AFTER_S = 1.0


@dataclasses.dataclass
class CameraStats:
    """Rolling FPS / jitter stats for one camera stream."""

    fps: float = 0.0
    jitter_ms: float = 0.0


camera_stats: dict[str, CameraStats] = {name: CameraStats() for name in CAMERA_INPUTS}
camera_timestamps: dict[str, collections.deque] = {
    name: collections.deque(maxlen=CAMERA_TIMESTAMP_WINDOW) for name in CAMERA_INPUTS
}


@dataclasses.dataclass
class VrStreamStats:
    """Rolling rate / jitter stats for the VR UDP stream."""

    fps: float = 0.0
    jitter_ms: float = 0.0


vr_stats = VrStreamStats()
vr_timestamps: collections.deque = collections.deque(maxlen=VR_TIMESTAMP_WINDOW)


def _event_field(event, name, default=None):
    """Read a field from a dora event, which only supports __getitem__."""
    try:
        value = event[name]
    except (KeyError, TypeError, AttributeError):
        return default
    return default if value is None else value


def _event_ts_to_seconds(ts) -> float:
    """Normalize a dora event timestamp (datetime or ns int) to POSIX seconds."""
    if isinstance(ts, datetime.datetime):
        if ts.tzinfo is None:
            ts = ts.replace(tzinfo=datetime.timezone.utc)
        return ts.timestamp()
    if isinstance(ts, (int, float)):
        return float(ts) / 1e9
    return time.time()


def _update_camera_stats(event_id: str, ts_s: float) -> None:
    series = camera_timestamps[event_id]
    if series and ts_s - series[-1] > CAMERA_STALE_AFTER_S:
        series.clear()
    series.append(ts_s)
    if len(series) < 2:
        return
    span = series[-1] - series[0]
    if span <= 0:
        return
    fps = (len(series) - 1) / span
    diffs = [series[i] - series[i - 1] for i in range(1, len(series))]
    jitter_ms = (max(diffs) - min(diffs)) * 1e3
    stats = camera_stats[event_id]
    stats.fps = fps
    stats.jitter_ms = jitter_ms


def _update_vr_stats(ts_s: float) -> None:
    """Fold one real VR packet arrival time (POSIX seconds) into the rolling stats."""
    series = vr_timestamps
    if series and ts_s - series[-1] > VR_STALE_AFTER_S:
        series.clear()
    series.append(ts_s)
    if len(series) < 2:
        return
    span = series[-1] - series[0]
    if span <= 0:
        return
    vr_stats.fps = (len(series) - 1) / span
    diffs = [series[i] - series[i - 1] for i in range(1, len(series))]
    vr_stats.jitter_ms = (max(diffs) - min(diffs)) * 1e3


async def _notify_state_changed() -> None:
    global state_version
    async with _state_changed:
        state_version += 1
        _state_changed.notify_all()


def apply_task(index):
    """Show the task at the given index, in Japanese when the metadata has it."""
    task = tasks[index]
    state.task_index = index
    # prompt_ja / description_ja are display only: prompt stays English so the
    # recorded dataset keeps its language instruction.
    state.task_title = task.get("prompt_ja") or task.get("prompt", "")
    state.task_prompt = task.get("prompt", "")
    state.task_description = task.get("description_ja") or task.get("description", "")


def next_task():
    """Update the state with the next task."""
    index = state.task_index + 1
    if index >= len(tasks):
        index = 0
    apply_task(index)


def _command_start():
    """Start a new episode."""
    node.send_output(
        "command",
        pa.array(["start"]),
        {
            "episode_number": state.episode_number,
            "task_index": state.task_index,
        },
    )
    state.collecting = True


def _command_success():
    """Finish the current episode successfully."""
    node.send_output("command", pa.array(["success"]))
    state.collecting = False
    state.episode_number += 1
    next_task()


def _command_fail():
    """Finish the current episode unsuccessfully."""
    node.send_output("command", pa.array(["fail"]))
    state.collecting = False
    state.episode_number += 1
    next_task()


def _command_quit():
    """Quit this data collection."""
    node.send_output("command", pa.array(["quit"]))
    state.running = False


def _command_arm_start():
    """Start (power on) the arm(s)."""
    node.send_output("arm_command", pa.array(["start"]))


def _command_arm_stop():
    """Pause (stop) the arm(s)."""
    node.send_output("arm_command", pa.array(["stop"]))


@app.get("/", response_class=HTMLResponse)
def _root(request: Request):
    """Render the main HTML."""
    return templates.TemplateResponse(
        request=request,
        name="root.html",
        context={
            "state": state,
            "state_version": state_version,
            "manual": manual,
            "launcher_url": launcher_url,
        },
    )


@app.post("/api/error")
async def _api_error(request: Request):
    """Accept a failure from outside (the launcher forwards dataflow output)."""
    try:
        payload = await request.json()
    except ValueError:
        payload = {}
    if _record_error(payload.get("source", "dataflow"), payload.get("message", "")):
        await _notify_state_changed()
    return {"ok": True}


@app.post("/errors/clear")
async def _clear_errors(request: Request):
    """Clear the failures shown on the task screen."""
    state.errors.clear()
    await _notify_state_changed()
    return RedirectResponse(request.url_for("_root"), 303)


@app.post("/start")
def _start(request: Request):
    _command_start()
    return RedirectResponse(request.url_for("_root"), 303)


@app.post("/skip")
def _skip(request: Request):
    """Skip the next task."""
    next_task()
    return RedirectResponse(request.url_for("_root"), 303)


@app.post("/success")
def _success(request: Request):
    _command_success()
    return RedirectResponse(request.url_for("_root"), 303)


@app.post("/fail")
def _fail(request: Request):
    _command_fail()
    return RedirectResponse(request.url_for("_root"), 303)


@app.post("/cancel")
def _cancel(request: Request):
    """Cancel the current episode."""
    node.send_output("command", pa.array(["cancel"]))
    state.collecting = False
    state.episode_number += 1
    return RedirectResponse(request.url_for("_root"), 303)


@app.get("/events", response_class=EventSourceResponse)
async def _events(request: Request) -> AsyncIterable[ServerSentEvent]:
    try:
        last_version = int(request.query_params.get("since"))
    except (TypeError, ValueError):
        last_version = state_version
    while state.running:
        async with _state_changed:
            await _state_changed.wait_for(
                lambda: state_version != last_version or not state.running
            )
        if not state.running:
            break
        last_version = state_version
        yield ServerSentEvent(
            data={
                "collecting": state.collecting,
                "episode_number": state.episode_number,
                "task_index": state.task_index,
                "arm_status_right": state.arm_status_right,
                "arm_status_left": state.arm_status_left,
            },
            id=str(state_version),
        )


@app.get("/stats", response_class=EventSourceResponse)
async def _stats() -> AsyncIterable[ServerSentEvent]:
    """Push camera FPS / jitter snapshots to the browser every 500 ms."""
    while state.running:
        now = time.time()
        snapshot = {}
        for name, s in camera_stats.items():
            series = camera_timestamps[name]
            if not series or now - series[-1] > CAMERA_STALE_AFTER_S:
                snapshot[name] = {"fps": 0.0, "jitter_ms": 0.0}
            else:
                snapshot[name] = {"fps": s.fps, "jitter_ms": s.jitter_ms}
        yield ServerSentEvent(data=snapshot)
        await asyncio.sleep(0.5)


@app.get("/vr-stats", response_class=EventSourceResponse)
async def _vr_stats() -> AsyncIterable[ServerSentEvent]:
    """Push the real VR stream Hz / jitter snapshot to the browser every 500 ms."""
    while state.running:
        now = time.time()
        if not vr_timestamps or now - vr_timestamps[-1] > VR_STALE_AFTER_S:
            snapshot = {"fps": 0.0, "jitter_ms": 0.0}
        else:
            snapshot = {"fps": vr_stats.fps, "jitter_ms": vr_stats.jitter_ms}
        yield ServerSentEvent(data=snapshot)
        await asyncio.sleep(0.5)


@app.post("/quit")
def _quit(request: Request):
    _command_quit()
    return RedirectResponse(request.url_for("_root"), 303)


@app.post("/arm/start")
def _arm_start(request: Request):
    """Start (power on) the arm(s)."""
    _command_arm_start()
    return RedirectResponse(request.url_for("_root"), 303)


@app.post("/arm/stop")
def _arm_stop(request: Request):
    """Pause (stop) the arm(s)."""
    _command_arm_stop()
    return RedirectResponse(request.url_for("_root"), 303)


def load_yaml(path):
    """Load a YAML file."""
    with open(path, encoding="utf-8") as f:
        return yaml.safe_load(f)


def load_manual(path):
    """Load the operation manual shown on the task screen.

    The file is YAML:

        title: 操作マニュアル
        sections:
          - title: アームの同期手順
            steps: ["...", "..."]
            note: "..."
    """
    if not path:
        return None
    path = pathlib.Path(path)
    if not path.exists():
        _record_error("ui", f"操作マニュアルが見つかりません: {path}")
        return None
    try:
        loaded = load_yaml(path)
    except yaml.YAMLError as error:
        _record_error("ui", f"操作マニュアルを読み込めません: {error}")
        return None
    if not isinstance(loaded, dict) or not loaded.get("sections"):
        _record_error("ui", f"操作マニュアルに sections がありません: {path}")
        return None
    return loaded


async def _main_uvicorn(server):
    await server.serve()


async def _main_dora(server):
    """Quit the Web application when this dataflow is stopped."""
    # Bring the arm(s) up on boot. dora-openarm no longer auto-starts
    _command_arm_start()
    last_values = {}
    while state.running:
        if node.is_empty():
            await asyncio.sleep(0.001)
            continue
        event = node.next()
        if event["type"] == "STOP":
            state.running = False
        elif event["type"] == "ERROR":
            # Show dora's own failures on the task screen, not just on stderr.
            _record_error("dora", _event_field(event, "error", "unknown dora error"))
            await _notify_state_changed()
        elif event["type"] == "INPUT_CLOSED":
            # A node that ends while we are still running has died.
            _record_error(
                "dora",
                f"入力 {_event_field(event, 'id', '?')} が停止しました"
                "（ノードが終了した可能性があります）",
            )
            await _notify_state_changed()
        elif event["type"] == "INPUT":
            event_id = event["id"]
            if event_id in CAMERA_INPUTS:
                _update_camera_stats(
                    event_id,
                    _event_ts_to_seconds(event["metadata"].get("timestamp")),
                )
                continue
            if event_id in ARM_STATUS_INPUTS:
                value = event["value"][0].as_py()
                # Only notify on an actual change. The follower may publish repeated (heartbeat) status values;
                if getattr(state, event_id) != value:
                    setattr(state, event_id, value)
                    await _notify_state_changed()
                continue
            if event_id in VR_RECEIVE_TIMES_INPUTS:
                for ts_ns in event["value"].to_pylist():
                    _update_vr_stats(float(ts_ns) / 1e9)
                continue
            if event_id not in ("button_a", "button_b"):
                continue

            value = event["value"][0].as_py()
            triggered = value and not last_values.get(event_id, False)
            last_values[event_id] = value
            if not triggered:
                continue

            if state.collecting:
                if event_id == "button_a":
                    _command_success()
                elif event_id == "button_b":
                    _command_fail()
            else:
                if event_id == "button_a":
                    _command_start()
                elif event_id == "button_b":
                    _command_quit()

            await _notify_state_changed()
    server.should_exit = True


async def _main_async():
    config = uvicorn.Config(app, port=port, log_level="info")
    server = uvicorn.Server(config)

    task_uvicorn = asyncio.create_task(_main_uvicorn(server))
    task_dora = asyncio.create_task(_main_dora(server))

    await task_uvicorn
    # Process may linger when dora exits via SIGTERM,
    # as _main_dora() may not receive a STOP event.
    # Set `state.running = False` when task_uvicorn exits
    # so that _main_dora() also exits.
    state.running = False
    await task_dora


def main():
    """Run data collection control Web application."""
    global node
    global tasks

    parser = argparse.ArgumentParser(description="Record data as OpenArm dataset")
    parser.add_argument(
        "--metadata-file",
        default=os.getenv("METADATA_FILE"),
        help="The metadata file",
        type=pathlib.Path,
    )
    parser.add_argument(
        "--auto-open",
        action=argparse.BooleanOptionalAction,
        default=os.getenv("AUTO_OPEN", "") == "yes",
        help="Open a Web browser automatically",
    )
    default_port = 8000
    parser.add_argument(
        "--port",
        default=int(os.getenv("PORT", default_port)),
        help=f"The port for UI ({default_port})",
        type=int,
    )
    parser.add_argument(
        "--manual-file",
        default=os.getenv("MANUAL_FILE"),
        help="The operation manual (YAML) shown on the task screen",
        type=pathlib.Path,
    )
    default_launcher_url = "http://127.0.0.1:8080/"
    parser.add_argument(
        "--launcher-url",
        default=os.getenv("LAUNCHER_URL", default_launcher_url),
        help=f"The desktop launcher URL to fall back to on failure ({default_launcher_url})",
    )
    args = parser.parse_args()
    global auto_open
    auto_open = args.auto_open
    global port
    port = args.port
    global launcher_url
    # The template appends paths ("log", "status") to this.
    launcher_url = args.launcher_url.rstrip("/") + "/"
    global manual
    manual = load_manual(args.manual_file)
    metadata = load_yaml(args.metadata_file)
    tasks = metadata["tasks"]
    apply_task(state.task_index)

    node = dora.Node()
    asyncio.run(_main_async())


if __name__ == "__main__":
    main()
