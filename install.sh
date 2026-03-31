#!/usr/bin/env bash
set -euo pipefail

PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BUILD_DIR="${PROJECT_DIR}/build"

log() {
    printf '[INFO] %s\n' "$1"
}

if [ "$(id -u)" -eq 0 ]; then
    SUDO=""
else
    SUDO="sudo"
fi
APT_GET="${SUDO} apt-get"
export DEBIAN_FRONTEND=noninteractive

if [ "$(id -u)" -eq 0 ] && [ -n "${SUDO_USER:-}" ] && [ "${SUDO_USER}" != "root" ]; then
    PYTHON_OWNER="${SUDO_USER}"
else
    PYTHON_OWNER="$(id -un)"
fi
PYTHON_OWNER_HOME="$(getent passwd "${PYTHON_OWNER}" | cut -d: -f6)"

run_owner_python() {
    if [ "$(id -u)" -eq 0 ] && [ "${PYTHON_OWNER}" != "root" ]; then
        sudo -u "${PYTHON_OWNER}" -H "$@"
    else
        "$@"
    fi
}

python_env_is_externally_managed() {
    run_owner_python python3 - <<'PYCODE'
import pathlib
import sysconfig
stdlib = pathlib.Path(sysconfig.get_path("stdlib") or "")
marker = stdlib / "EXTERNALLY-MANAGED"
raise SystemExit(0 if marker.exists() else 1)
PYCODE
}

pip_install_for_owner() {
    local -a pip_args=(python3 -m pip install --user)
    if python_env_is_externally_managed; then
        pip_args+=(--break-system-packages)
    fi
    run_owner_python "${pip_args[@]}" "$@"
}

owner_python_has_module() {
    local module_name="$1"
    run_owner_python python3 -c "import ${module_name}" >/dev/null 2>&1
}

log "Updating package index"
${APT_GET} update

log "Installing system dependencies"
${APT_GET} install -y \
    build-essential \
    cmake \
    pkg-config \
    git \
    ffmpeg \
    libhackrf-dev \
    hackrf \
    libiio-dev \
    libiio-utils \
    libusb-1.0-0-dev \
    python3 \
    python3-pip \
    python3-numpy \
    python3-paho-mqtt \
    python3-pyqt6 \
    python3-paramiko \
    openssh-client

log "Installing optional LimeSDR / SoapySDR packages when available"
if ! ${APT_GET} install -y \
    libsoapysdr-dev \
    soapysdr-tools \
    soapysdr-module-lms7 \
    limesuite \
    limesuite-udev; then
    log "Optional LimeSDR / SoapySDR packages were not fully available from this distro; Lime support may be disabled at build time"
fi

if ! owner_python_has_module PyQt6; then
    log "PyQt6 not available for ${PYTHON_OWNER}, installing full Python requirements with pip --user"
    pip_install_for_owner -r "${PROJECT_DIR}/requirements.txt"
else
    log "PyQt6 available for ${PYTHON_OWNER}"
fi

log "Checking Python runtime modules required by the GUI for ${PYTHON_OWNER}"
missing_python_modules=()
owner_python_has_module numpy || missing_python_modules+=("numpy>=1.21.0")
owner_python_has_module paho.mqtt.publish || missing_python_modules+=("paho-mqtt>=2.0.0")
owner_python_has_module paramiko || missing_python_modules+=("paramiko>=3.4.0")
if [ ${#missing_python_modules[@]} -gt 0 ]; then
    log "Installing missing Python user modules for ${PYTHON_OWNER}: ${missing_python_modules[*]}"
    pip_install_for_owner "${missing_python_modules[@]}"
else
    log "Required Python runtime modules already available for ${PYTHON_OWNER}; skipping pip install"
fi

log "Building dvbs2_tx"
cmake -S "${PROJECT_DIR}" -B "${BUILD_DIR}"
cmake --build "${BUILD_DIR}" -j"$(nproc)"
cp -f "${BUILD_DIR}/dvbs2_tx" "${PROJECT_DIR}/dvbs2_tx"
chmod +x "${PROJECT_DIR}/dvbs2_tx" "${PROJECT_DIR}/dvbs2_gui.py" "${PROJECT_DIR}/check_dependencies.py"

log "Installation complete"
log "Optional system-wide install: cmake --install "${BUILD_DIR}""
log "Python user packages were installed for ${PYTHON_OWNER} in ${PYTHON_OWNER_HOME}/.local"
printf '\nRun the dependency check with:\n  python3 %s/check_dependencies.py\n\n' "${PROJECT_DIR}"
printf 'Start the GUI with:\n  python3 %s/dvbs2_gui.py\n\n' "${PROJECT_DIR}"
printf 'CLI example (HackRF, recommended 333 kS/s starting point):\n  DATV_TX_GAIN_MODE=fixed DATV_TX_FIXED_GAIN=1.0 ffmpeg -re -i video.ts -f mpegts - | %s/dvbs2_tx QPSK_1/2_N 2407500000 333000 10 1 0.20 1 0 hackrf\n\n' "${PROJECT_DIR}"
printf 'CLI example (ADALM Pluto / Pluto+, direct libiio path at 333 kS/s):\n  DATV_TX_GAIN_MODE=fixed DATV_TX_FIXED_GAIN=1.0 ffmpeg -re -i video.ts -f mpegts - | %s/dvbs2_tx QPSK_1/2_N 2407500000 333000 10 0 0.20 1 0 adalmpluto ip:192.168.2.1\n\n' "${PROJECT_DIR}"
printf 'CLI example (LimeSDR Mini via SoapySDR, recommended 333 kS/s starting point):\n  DATV_TX_GAIN_MODE=fixed DATV_TX_FIXED_GAIN=1.0 ffmpeg -re -i video.ts -f mpegts - | %s/dvbs2_tx QPSK_1/2_N 2407500000 333000 10 0 0.20 1 0 limesdrmini driver=lime\n\n' "${PROJECT_DIR}"
printf 'Note: PlutoDVB2/F5OEO and 0303/2402 firmware-specific MQTT/pass-mode handling is driven by the GUI; the CLI binary remains a direct libiio/Soapy/HackRF backend test path.\n\n'
