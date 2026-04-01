# DATV-Linux DVB-S2 Transmitter

DATV-Linux is a Linux GUI + transmitter pipeline for amateur-radio DVB-S2 DATV work.
It accepts MPEG-TS on stdin, performs BBFRAME generation, BCH/LDPC encoding, PL framing, RRC shaping, and outputs IQ to supported SDR hardware.

## Supported RF outputs

- HackRF One
- LimeSDR Mini via SoapySDR
- ADALM Pluto / Pluto+
- PlutoDVB2 F5OEO firmware path
- Pluto F5UII 0303/2402 firmware path

## Source modes

- Video File
- Webcam / Camera
- External Stream (OBS / FFmpeg)

## Highlights in v1.0.46

- Fixed the **16APSK / 32APSK payload constellation ring mapping** in `pl_header.cpp` so APSK payload symbols now use the correct multi-ring classes before gamma scaling.
- Moved **RF device probing** and **FFmpeg encoder probing** off the GUI startup path so the window opens promptly and probing continues in the background.
- Hardened **Pluto reboot helpers** by removing the insecure `sshpass` fallback and requiring trusted SSH host keys for the subprocess fallback.
- `cmake --install` now installs **`dvbs2_tx`**, **`dvbs2_gui.py`**, **`check_dependencies.py`**, **`install.sh`**, and the shared support files needed by the GUI.
- Kept the simplified **Pluto Maintenance** panel:
  - Context Model
  - Refresh Pluto Info
  - Reboot Pluto
  - Stop / Exit Policy
- Kept the corrected RF auto-detection so Pluto stock/default, Pluto firmware paths, HackRF, and LimeSDR Mini are only marked detected when their probes confirm real hardware.

## Build

### One-shot install

```bash
sudo chmod +x install.sh   #if the file is in green no need for this step 
sudo ./install.sh
```

### Manual build

```bash
sudo apt install -y build-essential cmake pkg-config \
  libhackrf-dev hackrf \
  libiio-dev libiio-utils \
  libusb-1.0-0-dev \
  libsoapysdr-dev soapysdr-tools soapysdr-module-lms7 limesuite \
  ffmpeg python3 python3-pyqt6

cmake -S . -B build
cmake --build build -j$(nproc)
cp build/dvbs2_tx .
cmake --install build
```

## Run the GUI

```bash
python3 dvbs2_gui.py
```

After `cmake --install`, the GUI script is also installed to your binary path and will locate its shared support files from the installed data directory.

## CLI usage

```bash
./dvbs2_tx <MODCOD> <FREQ_HZ> <SYMBOL_RATE> <TX_LEVEL> <AMP_ENABLE> [ROLLOFF] [PILOTS] [GOLDCODE] [DEVICE] [DEVICE_ARG]
```

Examples:

```bash
ffmpeg -re -i video.ts -f mpegts - | ./dvbs2_tx QPSK_1/2_S 2407500000 250000 35 1 0.20 1 0 hackrf
ffmpeg -re -i video.ts -f mpegts - | ./dvbs2_tx QPSK_1/2_S 2407500000 250000 10 0 0.20 1 0 pluto ip:192.168.2.1
ffmpeg -re -i video.ts -f mpegts - | ./dvbs2_tx QPSK_1/2_S 2407500000 250000 40 0 0.20 1 0 limesdrmini driver=lime
```

## Pluto note

The normal libiio URI remains `ip:192.168.2.1` style.
IIOD commonly listens on TCP 30431, which is useful for detection and diagnostics, but you usually do not need to add the port to the runtime URI.

## Low symbol-rate stabilization

For operation below **250 kS/s**, the GUI exposes **Low SR stabilization (<250 kS/s)** as an optional helper.
It is **off by default** in v1.0.46.

When enabled, it can:

- apply more conservative FFmpeg MPEG-TS timing and queue settings
- add extra TS planning headroom in the GUI bitrate planner
- enable a stronger low-symbol-rate RRC path in `dvbs2_tx`

Leave it off unless you need extra hardening for very low symbol-rate work.

## Notes

- Rebuild `dvbs2_tx` on the target machine before on-air use.
- Validate RF cleanliness on a receiver or spectrum monitor before transmitting on-air.
- Read the Manual PLZ "its important for you to understand that this software architecture is different than any other software you have used , and its flexibility will make your life simpler " .
- Make sure your Firmware is updated to the latest.
## 73 OD5RAL(Radio Amateur Lebanon) Eng.Team
