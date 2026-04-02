# DATV-Linux DVB-S2 Transmitter

DATV-Linux is a Linux-based DVB-S2 transmitter application for radio amateurs. It combines a desktop GUI, FFmpeg-based video/audio preparation, MPEG-TS handling, and a host-side DVB-S2 transmit chain that generates IQ for supported SDR hardware.

The software is designed to be flexible and practical for amateur DATV work. It supports multiple SDR devices from one interface and does **not** require custom Pluto firmware for normal ADALM Pluto / Pluto+ use. If you want to use firmware-specific Pluto integrations, DATV-Linux also supports those through MQTT-assisted handoff modes.

## What DATV-Linux does

DATV-Linux performs the DVB-S2 transmit stages in software on the Linux host, including:

- MPEG-TS intake and transport planning
- BBFRAME preparation
- BCH / LDPC FEC
- PL framing
- constellation mapping
- pulse shaping / RRC filtering
- IQ streaming to the selected SDR

This makes the software portable across several SDR targets while keeping one common workflow.

## Supported SDR devices

- **HackRF One**
- **LimeSDR Mini** (via SoapySDR)
- **ADALM Pluto / Pluto+** with stock manufacturer firmware
- **PlutoDVB2 F5OEO** firmware path
- **Pluto F5UII 0303/2402** firmware path

## Supported source modes

- **Video File**
- **Webcam / Camera**
- **External Stream / FFmpeg source**

## Main advantages

- One Linux workflow for several SDR devices
- Works with **stock ADALM Pluto / Pluto+ firmware**, so custom firmware is **not required**
- Optional support for Pluto firmware ecosystems when you want them
- GUI-based operation for normal use, plus CLI backend for testing and automation
- Flexible enough for file playback, webcam, and external live sources
- Suitable for experimentation, field tests, and public amateur DATV operation

## Download

You can get DATV-Linux in either of these ways:

### Option 1: Download ZIP from GitHub

On the repository page, click:

**Code -> Download ZIP**

Then extract it:

```bash
unzip DATV-Linux-main.zip
cd DATV-Linux-main
```

### Option 2: Clone with Git

```bash
git clone <repository-url>
cd DATV-Linux-main
```

Replace `<repository-url>` with the actual GitHub repository address.

## Install

### Quick install

```bash
chmod +x install.sh
sudo ./install.sh
```

### Manual build

Install the main dependencies:

```bash
sudo apt update
sudo apt install -y build-essential cmake pkg-config \
  libhackrf-dev hackrf \
  libiio-dev libiio-utils \
  libusb-1.0-0-dev \
  libsoapysdr-dev soapysdr-tools soapysdr-module-lms7 limesuite \
  ffmpeg python3 python3-pyqt6
```

Then build:

```bash
cmake -S . -B build
cmake --build build -j$(nproc)
cp build/dvbs2_tx .
cmake --install build
```

## Check dependencies

```bash
python3 check_dependencies.py
```

## Start the GUI

```bash
python3 dvbs2_gui.py
```

After installation, the GUI can also be launched from the installed location if your install path is set up normally.

## Basic use

1. Connect your SDR.
2. Start `dvbs2_gui.py`.
3. Select the output device.
4. Choose the source type:
   - file
   - webcam
   - external stream
5. Set DVB-S2 parameters:
   - modulation
   - FEC rate
   - symbol rate
   - roll-off
   - pilots
6. Set video and audio options.
7. Press **Start**.
8. Monitor the console and receiver for lock, MER, and signal quality.

## Pluto notes

### Stock ADALM Pluto / Pluto+

DATV-Linux works with the **stock manufacturer firmware** on ADALM Pluto / Pluto+ through the normal libiio path.

That means:

- **no custom firmware is required**
- the software can work directly with a normal Pluto setup
- the typical Pluto USB network address is usually:

```text
192.168.2.1
```

### Pluto firmware-specific modes

If you are using **PlutoDVB2 F5OEO** or **Pluto F5UII 0303/2402**, DATV-Linux can coordinate with those firmware paths through MQTT and pass-mode handoff.

## Important note about firmware

**If you are using PlutoDVB2 F5OEO or Pluto F5UII firmware paths, make sure your Pluto+ / ADALM Pluto firmware is updated to the latest version(https://github.com/F5OEO/plutosdr-fw).** Older firmware versions can cause avoidable problems, especially at higher symbol rates.

**If you are using stock ADALM Pluto / Pluto+ firmware, DATV-Linux can still work normally and no custom firmware is required.**

## Manual

**Make sure to read the attached manual: `DATV-Linux_User_Manual.pdf`.**

The manual explains:

- installation
- GUI layout
- SDR selection
- Pluto operating modes
- source setup
- bitrate planning
- troubleshooting

## CLI backend example

```bash
ffmpeg -re -i video.ts -f mpegts - | ./dvbs2_tx QPSK_1/2_S 2407500000 250000 10 0 0.20 1 0 pluto ip:192.168.2.1
```

## Notes

- Rebuild `dvbs2_tx` on the target machine before on-air use.
- Always validate RF cleanliness and receiver lock before transmitting on-air.
- Keep symbol rate, codec settings, and TS bitrate within the safe transport budget.
- Keep Gold Code : 0 (default) as this will be used for future developed DVBS2-receivers.
  
##73 from OD5RAL(Radio Amateur Lebanon) Eng.Team**
