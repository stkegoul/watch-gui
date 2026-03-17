#!/usr/bin/env bash

set -euo pipefail

REPO="${BLNK_WATCH_REPO:-blnkfinance/watch}"
INSTALL_DIR="${BLNK_WATCH_INSTALL_DIR:-$HOME/.local/bin}"
REQUESTED_VERSION="${1:-${BLNK_WATCH_VERSION:-}}"

fail() {
  echo "blnk-watch installer: $*" >&2
  exit 1
}

need_cmd() {
  command -v "$1" >/dev/null 2>&1 || fail "missing required command: $1"
}

detect_os() {
  case "$(uname -s)" in
    Linux) echo "linux" ;;
    Darwin) echo "darwin" ;;
    *) fail "unsupported operating system: $(uname -s)" ;;
  esac
}

detect_arch() {
  case "$(uname -m)" in
    x86_64|amd64) echo "amd64" ;;
    arm64|aarch64) echo "arm64" ;;
    *) fail "unsupported architecture: $(uname -m)" ;;
  esac
}

resolve_version() {
  if [[ -n "$REQUESTED_VERSION" ]]; then
    echo "$REQUESTED_VERSION"
    return
  fi

  curl -fsSL "https://api.github.com/repos/${REPO}/releases/latest" \
    | sed -n 's/^[[:space:]]*"tag_name":[[:space:]]*"\([^"]*\)".*/\1/p' \
    | head -n 1
}

verify_checksum() {
  local checksum_file="$1"
  local archive_file="$2"

  if command -v sha256sum >/dev/null 2>&1; then
    (cd "$(dirname "$archive_file")" && sha256sum -c "$(basename "$checksum_file")")
    return
  fi

  if command -v shasum >/dev/null 2>&1; then
    (cd "$(dirname "$archive_file")" && shasum -a 256 -c "$(basename "$checksum_file")")
    return
  fi

  fail "missing sha256 verification tool: install sha256sum or shasum"
}

main() {
  need_cmd curl
  need_cmd tar
  need_cmd mktemp
  need_cmd install

  local os arch version ext asset_name asset_url checksum_url tmpdir archive_file checksum_file
  os="$(detect_os)"
  arch="$(detect_arch)"
  version="$(resolve_version)"

  [[ -n "$version" ]] || fail "could not resolve a release version from GitHub"

  ext="tar.gz"
  asset_name="blnk-watch_${version}_${os}_${arch}.${ext}"
  asset_url="https://github.com/${REPO}/releases/download/${version}/${asset_name}"
  checksum_url="${asset_url}.sha256"

  tmpdir="$(mktemp -d)"
  archive_file="${tmpdir}/${asset_name}"
  checksum_file="${archive_file}.sha256"
  trap 'rm -rf "$tmpdir"' EXIT

  echo "Installing blnk-watch ${version} for ${os}/${arch}..."
  curl -fsSL "$asset_url" -o "$archive_file"
  curl -fsSL "$checksum_url" -o "$checksum_file"
  verify_checksum "$checksum_file" "$archive_file"

  mkdir -p "$INSTALL_DIR"
  tar -xzf "$archive_file" -C "$tmpdir"
  install -m 0755 "${tmpdir}/blnk-watch" "${INSTALL_DIR}/blnk-watch"

  echo "Installed to ${INSTALL_DIR}/blnk-watch"
  case ":$PATH:" in
    *":${INSTALL_DIR}:"*) ;;
    *) echo "Add ${INSTALL_DIR} to your PATH to run 'blnk-watch' from any shell." ;;
  esac
}

main "$@"
