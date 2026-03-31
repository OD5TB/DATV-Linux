#!/usr/bin/env python3
"""Quick dependency checker for the DVB-S2 transmitter project."""

from __future__ import annotations

import os
import shutil
import subprocess
import sys
from typing import Iterable, Tuple

PROJECT_DIR = os.path.dirname(os.path.abspath(__file__))
BINARY_CANDIDATES = [
    os.path.join(PROJECT_DIR, "dvbs2_tx"),
    os.path.join(PROJECT_DIR, "build", "dvbs2_tx"),
    os.path.join(PROJECT_DIR, "src", "dvbs2_tx"),
    "/usr/local/bin/dvbs2_tx",
]


def report(ok: bool, label: str, detail: str) -> bool:
    prefix = "[OK]" if ok else "[MISSING]"
    print(f"  {prefix:<10} {label}: {detail}")
    return ok


def check_command(cmd: str, label: str) -> bool:
    return report(shutil.which(cmd) is not None, label, cmd)


def check_library(lib_name: str, label: str) -> bool:
    try:
        result = subprocess.run(["ldconfig", "-p"], capture_output=True, text=True, check=False)
        return report(lib_name in result.stdout, label, lib_name)
    except Exception:
        return report(False, label, f"unable to query {lib_name}")


def check_python_module(module_name: str, label: str) -> bool:
    try:
        __import__(module_name)
        return report(True, label, module_name)
    except ImportError:
        return report(False, label, module_name)


def find_binary() -> Tuple[bool, str]:
    for path in BINARY_CANDIDATES:
        if os.path.exists(path) and os.access(path, os.X_OK):
            return True, path
    return False, BINARY_CANDIDATES[0]


def summarize_missing(name_value_pairs: Iterable[Tuple[str, bool]]) -> int:
    missing = [name for name, ok in name_value_pairs if not ok]
    if missing:
        print("\nMissing items:")
        for name in missing:
            print(f"  - {name}")
    else:
        print("\nAll required dependencies were found.")
    return len(missing)


def main() -> int:
    print("=" * 60)
    print("DVB-S2 transmitter dependency check")
    print("=" * 60)

    results = []

    print("\n[1] System commands")
    for cmd, label in [
        ("g++", "C++ compiler"),
        ("make", "Make"),
        ("cmake", "CMake"),
        ("pkg-config", "pkg-config"),
        ("ffmpeg", "FFmpeg"),
        ("hackrf_info", "HackRF tools"),
        ("iio_info", "libiio utils"),
        ("iio_attr", "libiio attr tool"),
        ("SoapySDRUtil", "SoapySDR utils"),
        ("LimeUtil", "LimeSuite utils"),
        ("python3", "Python 3"),
    ]:
        results.append((label, check_command(cmd, label)))

    print("\n[2] Shared libraries")
    for lib, label in [
        ("libhackrf", "HackRF library"),
        ("libiio", "libiio"),
        ("libSoapySDR", "SoapySDR library"),
        ("libusb", "libusb"),
    ]:
        results.append((label, check_library(lib, label)))

    print("\n[3] Python modules")
    for module, label in [
        ("PyQt6", "PyQt6"),
        ("PyQt6.QtCore", "PyQt6.QtCore"),
        ("PyQt6.QtWidgets", "PyQt6.QtWidgets"),
        ("numpy", "NumPy"),
        ("paho.mqtt.publish", "paho-mqtt"),
    ]:
        results.append((label, check_python_module(module, label)))

    print("\n[4] Project binary")
    binary_ok, binary_path = find_binary()
    results.append(("dvbs2_tx binary", report(binary_ok, "dvbs2_tx binary", binary_path)))

    print("\n[5] Optional Pluto maintenance tools")
    report(shutil.which("ssh") is not None, "SSH client", "ssh (used by Pluto reboot helper)")
    check_python_module("paramiko", "paramiko")

    print("\n[6] Optional webcam and microphone tools")
    report(shutil.which("v4l2-ctl") is not None, "v4l2-ctl", "v4l2-ctl (better webcam probing)")
    report(shutil.which("pactl") is not None, "pactl", "pactl (PulseAudio / PipeWire microphone probing)")
    report(shutil.which("arecord") is not None, "arecord", "arecord (ALSA microphone probing)")

    print("\n[7] Notes")
    report(True, "Source modes", "video file, webcam / camera, and external stream input")
    report(True, "RF outputs", "HackRF One, LimeSDR Mini (SoapySDR), Pluto DVB-S2 F5OEO / Pluto+, ADALM Pluto")

    missing_count = summarize_missing(results)

    print("\nSuggested next steps:")
    if missing_count:
        print(f"  1. Run: {os.path.join(PROJECT_DIR, 'install.sh')}")
        print(f"  2. Build manually if needed: cmake -S {PROJECT_DIR} -B {os.path.join(PROJECT_DIR, 'build')} && cmake --build {os.path.join(PROJECT_DIR, 'build')}")
    else:
        print(f"  1. Start the GUI: python3 {os.path.join(PROJECT_DIR, 'dvbs2_gui.py')}")
        print(f"  2. Or run the CLI binary directly from: {binary_path}")

    return 0 if missing_count == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
