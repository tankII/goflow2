#!/bin/bash
# GoFlow2 部署包构建脚本
# 用法: bash .deploy/build.sh [linux-amd64|linux-arm64|all]
set -euo pipefail

cd "$(dirname "$0")/.."
VERSION=$(git describe --tags --abbrev=0 HEAD 2>/dev/null || git rev-parse --short HEAD)
DATE=$(date +%Y%m%d-%H%M%S)
OUT_DIR=".release"

ARCH="${1:-all}"

build_arch() {
    local arch="$1"
    local os="linux"
    local out_bin="goflow2"
    case "$arch" in
        amd64|x86_64) out_bin="goflow2-linux-amd64" ;;
        arm64|aarch64) out_bin="goflow2-linux-arm64" ;;
    esac
    echo ">>> building $out_bin ..."
    GOOS=$os GOARCH=$arch CGO_ENABLED=0 go build \
        -ldflags "-X main.version=$VERSION -X main.buildinfos=$DATE" \
        -o "$OUT_DIR/bin/$out_bin" \
        ./cmd/goflow2/main.go
    # enricher（可选）
    GOOS=$os GOARCH=$arch CGO_ENABLED=0 go build \
        -ldflags "-X main.version=$VERSION -X main.buildinfos=$DATE" \
        -o "$OUT_DIR/bin/enricher-$out_bin" \
        ./cmd/enricher/main.go
    echo ">>> done $out_bin"
}

echo "=== GoFlow2 deploy package build ==="
echo "version : $VERSION"
echo "date    : $DATE"
echo "arch    : $ARCH"
mkdir -p "$OUT_DIR/bin" "$OUT_DIR/conf"

case "$ARCH" in
    amd64|x86_64) build_arch amd64 ;;
    arm64|aarch64) build_arch arm64 ;;
    all)
        build_arch amd64
        build_arch arm64 ;;
    *)
        echo "unknown arch: $ARCH (use amd64|arm64|all)"
        exit 1 ;;
esac

cp .deploy/conf/* "$OUT_DIR/conf/"

echo ">>> packing tarball ..."
PKG_DIR="goflow2-deploy-${VERSION}-${DATE}"
mkdir -p "$OUT_DIR/$PKG_DIR"
cp -r "$OUT_DIR/bin" "$OUT_DIR/$PKG_DIR/"
cp -r "$OUT_DIR/conf" "$OUT_DIR/$PKG_DIR/"
cp .deploy/*.sh "$OUT_DIR/$PKG_DIR/" 2>/dev/null || true
cp .deploy/README.md "$OUT_DIR/$PKG_DIR/README.md" 2>/dev/null || true
cp README.md "$OUT_DIR/$PKG_DIR/README.md" 2>/dev/null || true
(cd "$OUT_DIR" && tar czf "$PKG_DIR.tar.gz" "$PKG_DIR")
echo ">>> done  $OUT_DIR/$PKG_DIR.tar.gz"
ls -lh "$OUT_DIR/$PKG_DIR.tar.gz"