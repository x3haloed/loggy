#!/bin/bash
set -e

# Log with color
log() {
    local color="\033[0;34m"
    local reset="\033[0m"
    echo -e "${color}[publish] $1${reset}"
}

error() {
    local color="\033[0;31m"
    local reset="\033[0m"
    echo -e "${color}[ERROR] $1${reset}" >&2
}

show_help() {
    echo "Usage: $0 [platform1] [platform2] ..."
    echo ""
    echo "Platforms:"
    echo "  macos     - Build for macOS (both x86_64 and arm64)"
    echo "  windows   - Build for Windows (both x86_64 and arm64)"
    echo "  linux     - Build for Linux (both x86_64 and arm64)"
    echo ""
    echo "If no platforms are specified, defaults to current platform."
    echo "Examples:"
    echo "  $0 macos              # Build for macOS only"
    echo "  $0 macos windows      # Build for macOS and Windows"
    echo "  $0 macos linux arm64  # Build for macOS and Linux, arm64 only"
    exit 0
}

# Check for help flag
if [[ "$1" == "-h" || "$1" == "--help" ]]; then
    show_help
fi

# Check for required tools
if ! command -v rustup &> /dev/null; then
    error "rustup is required but not installed."
    exit 1
fi

if ! command -v cargo &> /dev/null; then
    error "cargo is required but not installed."
    exit 1
fi

# Define all possible targets
ALL_TARGETS=(
    # macOS
    "x86_64-apple-darwin:macos:x86_64"
    "aarch64-apple-darwin:macos:arm64"
    # Windows
    "x86_64-pc-windows-msvc:windows:x86_64"
    "aarch64-pc-windows-msvc:windows:arm64"
    # Linux
    "x86_64-unknown-linux-gnu:linux:x86_64"
    "aarch64-unknown-linux-gnu:linux:arm64"
)

# Detect current platform
CURRENT_PLATFORM=""
case "$(uname -s)" in
    Darwin*)  CURRENT_PLATFORM="macos";;
    Linux*)   CURRENT_PLATFORM="linux";;
    CYGWIN*|MINGW*|MSYS*) CURRENT_PLATFORM="windows";;
    *)        CURRENT_PLATFORM="unknown";;
esac

# Detect current architecture
CURRENT_ARCH=""
case "$(uname -m)" in
    x86_64*)  CURRENT_ARCH="x86_64";;
    arm64*|aarch64*)  CURRENT_ARCH="arm64";;
    *)        CURRENT_ARCH="unknown";;
esac

log "Detected platform: $CURRENT_PLATFORM, architecture: $CURRENT_ARCH"

# Parse command-line arguments
SELECTED_PLATFORMS=()
SELECTED_ARCHS=()

if [ $# -eq 0 ]; then
    # If no arguments, default to current platform
    SELECTED_PLATFORMS+=("$CURRENT_PLATFORM")
else
    for arg in "$@"; do
        case "$arg" in
            macos|windows|linux)
                SELECTED_PLATFORMS+=("$arg")
                ;;
            x86_64|arm64)
                SELECTED_ARCHS+=("$arg")
                ;;
            *)
                error "Unknown platform or architecture: $arg"
                show_help
                ;;
        esac
    done
fi

# If no architectures specified, build for both
if [ ${#SELECTED_ARCHS[@]} -eq 0 ]; then
    SELECTED_ARCHS=("x86_64" "arm64")
fi

log "Building for platforms: ${SELECTED_PLATFORMS[*]}, architectures: ${SELECTED_ARCHS[*]}"

# Filter targets based on selected platforms and architectures
SELECTED_TARGETS=()
for target_info in "${ALL_TARGETS[@]}"; do
    target=$(echo "$target_info" | cut -d':' -f1)
    platform=$(echo "$target_info" | cut -d':' -f2)
    arch=$(echo "$target_info" | cut -d':' -f3)
    
    for selected_platform in "${SELECTED_PLATFORMS[@]}"; do
        if [[ "$platform" == "$selected_platform" ]]; then
            for selected_arch in "${SELECTED_ARCHS[@]}"; do
                if [[ "$arch" == "$selected_arch" ]]; then
                    SELECTED_TARGETS+=("$target")
                    break
                fi
            done
        fi
    done
done

if [ ${#SELECTED_TARGETS[@]} -eq 0 ]; then
    error "No valid targets selected."
    exit 1
fi

# Create directories
PUBLISH_DIR="./target/publish"
mkdir -p "$PUBLISH_DIR"

# Install targets and build for each
for target in "${SELECTED_TARGETS[@]}"; do
    log "Adding target: $target"
    rustup target add "$target" || { error "Failed to add target: $target"; continue; }
    
    log "Building for target: $target"
    cargo build --release --target="$target" || { error "Build failed for target: $target"; continue; }
    
    # Determine binary name and extension based on target
    bin_name="loggy"
    if [[ "$target" == *"windows"* ]]; then
        bin_name="loggy.exe"
    fi
    
    # Source binary path
    src_bin="./target/$target/release/$bin_name"
    
    # Determine platform and architecture for output naming
    platform=""
    arch=""
    
    if [[ "$target" == *"apple"* ]]; then
        platform="macos"
    elif [[ "$target" == *"windows"* ]]; then
        platform="windows"
    elif [[ "$target" == *"linux"* ]]; then
        platform="linux"
    fi
    
    if [[ "$target" == "x86_64"* ]]; then
        arch="x86_64"
    elif [[ "$target" == "aarch64"* ]]; then
        arch="arm64"
    fi
    
    output_dir="$PUBLISH_DIR/$platform-$arch"
    mkdir -p "$output_dir"
    
    # Copy binary to output directory
    log "Copying binary to $output_dir/$bin_name"
    cp "$src_bin" "$output_dir/$bin_name"
    
    # Package the binary
    log "Packaging for $platform-$arch"
    pushd "$output_dir" > /dev/null
    if [[ "$platform" == "windows" ]]; then
        zip -q -r "../loggy-$platform-$arch.zip" .
    else
        tar czf "../loggy-$platform-$arch.tar.gz" .
    fi
    popd > /dev/null
done

# Create SHA256 checksums
log "Generating checksums"
pushd "$PUBLISH_DIR" > /dev/null
archive_files=( *.zip *.tar.gz )
if [ ${#archive_files[@]} -ne 0 ]; then
    if command -v shasum &> /dev/null; then
        shasum -a 256 *.zip *.tar.gz > SHA256SUMS.txt
    elif command -v sha256sum &> /dev/null; then
        sha256sum *.zip *.tar.gz > SHA256SUMS.txt
    else
        error "Neither shasum nor sha256sum found, skipping checksum generation"
    fi
else
    error "No archives found, skipping checksum generation"
fi
popd > /dev/null

log "Done! Artifacts available in $PUBLISH_DIR"
log "Checksums available in $PUBLISH_DIR/SHA256SUMS.txt" 