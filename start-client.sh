#!/usr/bin/env bash
set -euo pipefail

# Run from repository root regardless of caller working directory.
cd "$(dirname "$0")"

# WSLg's Weston 9.0 compositor can segfault (Broken pipe) with winit/sctk-adwaita Wayland
# surfaces; prefer X11 (XWayland) on WSL2 when DISPLAY is available.
if [[ -n "${WSL_DISTRO_NAME:-}" && -n "${DISPLAY:-}" && -z "${BEVY_USE_WAYLAND:-}" ]]; then
    unset WAYLAND_DISPLAY WAYLAND_SOCKET
fi

exec cargo run -- --mode client "$@"
