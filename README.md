# dora-openarm-data-collection-ui

A [dora-rs](https://dora-rs.ai/) node that provides UI for data collection with OpenArm.

The UI is in Japanese, so that operators who do not read English can run a
collection session.

## Configuration

| Environment variable | Option | Description |
| --- | --- | --- |
| `METADATA_FILE` | `--metadata-file` | Metadata with the task list |
| `MANUAL_FILE` | `--manual-file` | Operation manual (YAML) shown on the task screen |
| `LAUNCHER_URL` | `--launcher-url` | Desktop launcher to fall back to when this dataflow dies (default `http://127.0.0.1:8080/`) |
| `PORT` | `--port` | Port of this UI (default `8000`) |
| `AUTO_OPEN` | `--auto-open` | Open a Web browser on start |
| `CAMERA_VIEW_FPS` | `--camera-view-fps` | Rate of the live camera view (default `10`, `0` turns it off) |

Tasks in the metadata may carry `prompt_ja` and `description_ja`. They are shown
instead of `prompt` / `description`, which stay in English because the recorder
writes them to the dataset as its language instruction.

The manual file is:

```yaml
title: 操作マニュアル
sections:
  - title: アームの同期手順
    steps: ["...", "..."]     # numbered
    bullets: ["...", "..."]   # unnumbered
    note: "..."               # highlighted warning
```

## Live camera view

The task screen shows the cameras live next to the task: `camera_ceiling`
always, and below it either the head pair (`camera_head_left`,
`camera_head_right`) or the wrist pair (`camera_wrist_left`,
`camera_wrist_right`), picked with the 頭部 / 手首 tabs. A tab shows ⚠ when
a camera of its pair stops, even while the other pair is on screen. A tile
shows 映像なし once its camera has sent nothing for 2 s.

Frames must be JPEG (`encoding` metadata `jpeg`, or none). They are forwarded
as they are over one WebSocket (`/cameras?group=head|wrist`), newest frame
only, at `CAMERA_VIEW_FPS`. Only the ceiling and the picked pair are sent, and
the page closes the socket while its tab is hidden.

## Errors

Failures are shown in red on the task screen instead of only on a terminal:

- dora `ERROR` events and inputs that stop while the dataflow runs,
- anything posted to `POST /api/error` with `{"source": ..., "message": ...}`
  (the desktop launcher forwards error lines from the dataflow output),
- camera streams that stop delivering frames,
- the loss of this node itself, which points the operator back to the launcher.

`POST /errors/clear` clears the panel.

## License

Licensed under the Apache License 2.0. See [LICENSE](LICENSE) for details.

Copyright 2026 Enactic, Inc.

## Code of Conduct

All participation in the OpenArm project is governed by our [Code of Conduct](CODE_OF_CONDUCT.md).
