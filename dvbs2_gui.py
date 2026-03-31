#!/usr/bin/env python3
"""
DVB-S2 Transmitter GUI for QO-100
FIXED: DVB-S2 compliant MPEG-TS generation (Strict CBR for Buffer Stability)
"""

import sys
import subprocess
import os
import signal
import time
import threading
import re
import shutil
import json
import math
import tempfile
import shlex
from typing import Optional
from PyQt6.QtWidgets import (
    QApplication, QMainWindow, QWidget, QVBoxLayout, QHBoxLayout,
    QLabel, QLineEdit, QComboBox, QSlider, QPushButton, QGroupBox,
    QGridLayout, QMessageBox, QCheckBox, QFileDialog,
    QTextEdit, QProgressBar, QTabWidget, QSpinBox, QScrollArea, QDoubleSpinBox
)
from PyQt6.QtCore import Qt, QTimer, QThread, pyqtSignal, QSettings
from PyQt6.QtGui import QFont, QTextCursor, QColor

try:
    import paho.mqtt.publish as mqtt_publish
except Exception:
    mqtt_publish = None

try:
    import paho.mqtt.client as mqtt_client
except Exception:
    mqtt_client = None

try:
    import paramiko
except Exception:
    paramiko = None

PLUTO_SAFE_MIN_SAMPLE_RATE = 2083333
HACKRF_MIN_SAMPLE_RATE = 2000000
LIME_MIN_SAMPLE_RATE = 1000000
PLUTO_COMPAT_TARGET_SAMPLE_RATE = 2000000
AUTO_AUDIO_DISABLE_TS_KBPS = 96
SMART_AUDIO_LOW_TS_KBPS = 220
SMART_AUDIO_MID_TS_KBPS = 700
PLUTO_DEFAULT_IIOD_PORT = 30431
PLUTODVB2_MQTT_PORT = 1883
PLUTO_0303_MQTT_PORT = 8883
PLUTO_DEFAULT_SSH_PORT = 22
F5OEO_FEC_TOKEN_MAP = {
    "1/4": "14",
    "1/3": "13",
    "2/5": "25",
    "1/2": "12",
    "3/5": "35",
    "2/3": "23",
    "3/4": "34",
    "4/5": "45",
    "5/6": "56",
    "8/9": "89",
    "9/10": "910",
}

class ProcessWorker(QThread):
    """Thread to run the transmission process with proper pipe handling"""
    output_signal = pyqtSignal(str)
    error_signal = pyqtSignal(str)
    finished_signal = pyqtSignal(int)
    status_signal = pyqtSignal(dict)
    
    def __init__(self, ffmpeg_cmd, tx_cmd, f5oeo_runtime_hook=None, tx_env=None):
        super().__init__()
        self.ffmpeg_cmd = ffmpeg_cmd
        self.tx_cmd = tx_cmd
        self.f5oeo_runtime_hook = f5oeo_runtime_hook or {}
        self.tx_env = tx_env or os.environ.copy()
        self.ffmpeg_process = None
        self.tx_process = None
        self.performance_stats = {
            'frames': 0,
            'buffer_level': 0,
            'buffer_percent': 0,
            'fps': 0,
            'underflows': 0,
            'ts_kbps': 0.0
        }
        self.running = True
        self.debug = False
        self.stop_requested = False
        self.ffmpeg_timeout_reported = False
        self.transport_stop_reason = ""
        self.transport_stop_detail = ""
        
    def run(self):
        try:
            # Start ffmpeg with stdout pipe
            self.ffmpeg_process = subprocess.Popen(
                self.ffmpeg_cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                universal_newlines=False,
                bufsize=0,
                start_new_session=True
            )

            # Start dvbs2_tx with stdin from ffmpeg stdout
            self.tx_process = subprocess.Popen(
                self.tx_cmd,
                stdin=self.ffmpeg_process.stdout,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                universal_newlines=True,
                bufsize=1,
                shell=False,
                start_new_session=True,
                env=self.tx_env
            )

            # Close ffmpeg stdout in parent
            if self.ffmpeg_process.stdout:
                self.ffmpeg_process.stdout.close()

            if self.f5oeo_runtime_hook and mqtt_publish is not None:
                threading.Thread(target=self.run_f5oeo_runtime_hook, daemon=True).start()

            # Read stderr from both processes
            def read_ffmpeg_stderr():
                suppressed = (
                    'broken pipe',
                    'error muxing a packet',
                    'error writing trailer',
                    'error closing file',
                    'task finished with error code: -32',
                    'terminating thread with return code -32',
                )
                ts_warning_seen = set()
                for line in iter(self.ffmpeg_process.stderr.readline, b''):
                    if not line:
                        continue
                    try:
                        line_str = line.decode('utf-8', errors='replace').strip()
                    except Exception:
                        continue
                    if not line_str:
                        continue
                    lowered = line_str.lower()
                    tx_failed = self.tx_process and self.tx_process.poll() not in (None, 0)
                    if tx_failed and any(token in lowered for token in suppressed):
                        continue
                    if (not self.ffmpeg_timeout_reported and any(token in lowered for token in (
                        'connection timed out',
                        'operation timed out',
                        'i/o timeout',
                        'rw_timeout',
                        'resource temporarily unavailable',
                    ))):
                        self.ffmpeg_timeout_reported = True
                        self.error_signal.emit('[FFMPEG] Stop cause: FFmpeg/network timeout while reading the external stream source.')
                    if 'dts < pcr' in lowered or 'non monotonically increasing dts' in lowered:
                        key = 'timestamp'
                        if key not in ts_warning_seen:
                            ts_warning_seen.add(key)
                            self.error_signal.emit(
                                '[FFMPEG] MPEG-TS timestamp warning detected (for example "dts < pcr"). '
                                'This can break DATV decoding even when other startup logs look normal.'
                            )
                        continue
                    if 'cbr hrd requires constant bitrate' in lowered or 'vbv parameters cannot be changed when nal hrd is in use' in lowered:
                        key = 'cbr-hrd'
                        if key not in ts_warning_seen:
                            ts_warning_seen.add(key)
                            self.error_signal.emit(
                                '[FFMPEG] Encoder VBV/HRD configuration is inconsistent. DATV transport will not stay at the requested TS rate until the rate-control settings are corrected.'
                            )
                        continue
                    if 'x265 [info]:' in lowered or 'x265 [warning]:' in lowered:
                        self.output_signal.emit(f"[FFMPEG] {line_str}")
                        continue
                    if 'non-existing' in lowered and 'pps' in lowered:
                        key = 'non-existing-pps'
                        if key in ts_warning_seen:
                            continue
                        ts_warning_seen.add(key)
                    self.error_signal.emit(f"[FFMPEG] {line_str}")

            def read_tx_stdout():
                for line in iter(self.tx_process.stdout.readline, ''):
                    if line:
                        stripped = line.strip()
                        self.output_signal.emit(stripped)
                        self.handle_transport_summary_line(stripped)
                        self.parse_performance_data(line)

            def read_tx_stderr():
                for line in iter(self.tx_process.stderr.readline, ''):
                    if line:
                        self.error_signal.emit(f"[TX] {line.strip()}")

            ffmpeg_stderr_thread = threading.Thread(target=read_ffmpeg_stderr)
            tx_stdout_thread = threading.Thread(target=read_tx_stdout)
            tx_stderr_thread = threading.Thread(target=read_tx_stderr)
            ffmpeg_stderr_thread.daemon = True
            tx_stdout_thread.daemon = True
            tx_stderr_thread.daemon = True
            ffmpeg_stderr_thread.start()
            tx_stdout_thread.start()
            tx_stderr_thread.start()

            ffmpeg_ret = None
            tx_ret = None
            ffmpeg_stopped_due_to_tx_failure = False
            while True:
                if tx_ret is None and self.tx_process:
                    tx_ret = self.tx_process.poll()
                if ffmpeg_ret is None and self.ffmpeg_process:
                    ffmpeg_ret = self.ffmpeg_process.poll()

                if tx_ret is not None and ffmpeg_ret is None and self.ffmpeg_process:
                    ffmpeg_stopped_due_to_tx_failure = True
                    try:
                        self.ffmpeg_process.terminate()
                    except Exception:
                        pass
                    try:
                        ffmpeg_ret = self.ffmpeg_process.wait(timeout=2)
                    except Exception:
                        try:
                            self.ffmpeg_process.kill()
                        except Exception:
                            pass
                        ffmpeg_ret = self.ffmpeg_process.wait() if self.ffmpeg_process else -1

                if ffmpeg_ret is not None and tx_ret is None and self.tx_process:
                    try:
                        tx_ret = self.tx_process.wait(timeout=2)
                    except Exception:
                        try:
                            self.tx_process.terminate()
                        except Exception:
                            pass
                        tx_ret = self.tx_process.wait() if self.tx_process else -1

                if ffmpeg_ret is not None and tx_ret is not None:
                    break
                time.sleep(0.05)

            ffmpeg_ret = -1 if ffmpeg_ret is None else ffmpeg_ret
            tx_ret = -1 if tx_ret is None else tx_ret

            if not self.stop_requested:
                if ffmpeg_ret != 0 and not (ffmpeg_stopped_due_to_tx_failure and tx_ret != 0):
                    self.error_signal.emit(f"FFmpeg exited with code {ffmpeg_ret}")
                if tx_ret != 0:
                    self.error_signal.emit(f"dvbs2_tx exited with code {tx_ret}")

            final_code = 0 if self.stop_requested else (tx_ret if tx_ret != 0 else ffmpeg_ret)
            self.finished_signal.emit(final_code)

        except Exception as e:
            self.error_signal.emit(f"Process error: {str(e)}")
            self.finished_signal.emit(-1)
    
    def handle_transport_summary_line(self, line):
        lowered = line.lower()
        if line.startswith("Transport stop:"):
            self.transport_stop_reason = line.split(":", 1)[1].strip()
            if "stalled waiting for payload" in lowered:
                self.error_signal.emit("[TX] Stop cause: stdin stall after FFmpeg launched.")
            elif "sync lost" in lowered or "re-lock failed" in lowered:
                self.error_signal.emit("[TX] Stop cause: sync loss in MPEG-TS reassembly.")
        elif line.startswith("Detail:"):
            self.transport_stop_detail = line.split(":", 1)[1].strip()
            detail_lowered = self.transport_stop_detail.lower()
            if "stalled waiting for payload" in detail_lowered:
                self.error_signal.emit(f"[TX] Detail: {self.transport_stop_detail}")
            elif "sync" in detail_lowered:
                self.error_signal.emit(f"[TX] Detail: {self.transport_stop_detail}")

    def parse_performance_data(self, data):
        try:
            if 'Frames:' in data:
                frames_match = re.search(r'Frames:\s*(\d+)', data)
                if frames_match:
                    self.performance_stats['frames'] = int(frames_match.group(1))
                
                buffer_match = re.search(r'Buffer:\s*(\d+)\s*KB(?:\s*\((\d+)%\))?', data)
                if buffer_match:
                    self.performance_stats['buffer_level'] = int(buffer_match.group(1))
                    if buffer_match.group(2):
                        self.performance_stats['buffer_percent'] = int(buffer_match.group(2))
                
                underflow_match = re.search(r'Underflows:\s*(\d+)', data)
                if underflow_match:
                    self.performance_stats['underflows'] = int(underflow_match.group(1))
                
                fps_match = re.search(r'Avg:\s*([\d.]+)\s*fps', data)
                if fps_match:
                    self.performance_stats['fps'] = float(fps_match.group(1))

                ts_match = re.search(r'(?:TS|Rate):\s*([\d.]+)\s*kbps', data)
                if ts_match:
                    self.performance_stats['ts_kbps'] = float(ts_match.group(1))

                self.status_signal.emit(self.performance_stats)
        except Exception as e:
            if self.debug:
                print(f"Parse error: {e}")

    def _start_console_mqtt_monitor(self, hook):
        if mqtt_client is None:
            self.output_signal.emit("0303 console diagnostics unavailable: paho.mqtt.client not installed")
            return

        host = str(hook.get("host", "")).strip()
        port = int(hook.get("port", 1883) or 1883)
        topics = list(hook.get("subscribe_topics", []) or [])
        if not host or not topics:
            return

        stop_event = threading.Event()
        self._console_mqtt_stop_event = stop_event

        def worker():
            client_id = f"dvbs2tx-0303-diag-{os.getpid()}-{int(time.time() * 1000) % 100000}"
            client = mqtt_client.Client(client_id=client_id, clean_session=True)
            self._console_mqtt_client = client

            def on_connect(client, userdata, flags, rc):
                if rc == 0:
                    self.output_signal.emit(f"0303 console diagnostics connected to MQTT {host}:{port}")
                    for topic in topics:
                        try:
                            client.subscribe(topic, qos=0)
                            self.output_signal.emit(f"0303 console diagnostics subscribed: {topic}")
                        except Exception as exc:
                            self.output_signal.emit(f"0303 console diagnostics subscribe warning for {topic}: {exc}")
                else:
                    self.output_signal.emit(f"0303 console diagnostics connect failed: rc={rc}")

            def on_message(client, userdata, msg):
                try:
                    payload = msg.payload.decode('utf-8', errors='replace').strip()
                except Exception:
                    payload = repr(msg.payload)
                self.output_signal.emit(f"0303 MQTT RX: {msg.topic} = {payload}")

            def on_disconnect(client, userdata, rc):
                if not stop_event.is_set() and rc != 0:
                    self.output_signal.emit(f"0303 console diagnostics disconnected unexpectedly: rc={rc}")

            client.on_connect = on_connect
            client.on_message = on_message
            client.on_disconnect = on_disconnect

            try:
                client.connect(host, port, keepalive=20)
                client.loop_start()
                while self.running and not stop_event.is_set():
                    time.sleep(0.20)
            except Exception as exc:
                self.output_signal.emit(f"0303 console diagnostics error: {exc}")
            finally:
                stop_event.set()
                try:
                    client.loop_stop()
                except Exception:
                    pass
                try:
                    client.disconnect()
                except Exception:
                    pass

        threading.Thread(target=worker, daemon=True).start()

    def run_f5oeo_runtime_hook(self):
        hook = self.f5oeo_runtime_hook or {}
        if not hook or mqtt_publish is None:
            return

        host = str(hook.get("host", "")).strip()
        port = int(hook.get("port", 1883) or 1883)
        topic_base = str(hook.get("topic_base", "")).strip()
        topic_key = str(hook.get("topic_key", "")).strip()
        hook_mode = str(hook.get("hook_mode", "plutodvb2") or "plutodvb2").strip()
        if not host:
            return
        if not topic_base and topic_key:
            topic_base = f"cmd/pluto/{topic_key}"
        if not topic_base:
            return

        initial_delay = max(float(hook.get("initial_delay", 0.30) or 0.30), 0.0)
        step_delay = max(float(hook.get("step_delay", 0.05) or 0.05), 0.0)
        pass_delay = max(float(hook.get("pass_delay", 0.20) or 0.20), 0.0)
        mode_before = str(hook.get("mode_before", "pass") or "pass").strip() or "pass"
        mode_after = str(hook.get("mode_after", mode_before) or mode_before).strip() or mode_before
        pre_sequence = list(hook.get("pre_sequence", []) or [])
        post_sequence = list(hook.get("post_sequence", []) or [])
        pre_label = str(hook.get("pre_label", "MQTT runtime sync") or "MQTT runtime sync")
        post_label = str(hook.get("post_label", "MQTT runtime resync") or "MQTT runtime resync")

        if hook_mode == "plutodvb_0303":
            self._start_console_mqtt_monitor(hook)

        def publish_one(suffix, payload, label):
            topic = f"{topic_base}/{suffix}"
            mqtt_publish.single(topic, str(payload), hostname=host, port=port, qos=0, retain=False)
            self.output_signal.emit(f"{label}: {topic} = {payload}")

        try:
            time.sleep(initial_delay)
            for suffix, payload in pre_sequence:
                publish_one(suffix, payload, pre_label)
                if step_delay > 0:
                    time.sleep(step_delay)
            if hook_mode == "plutodvb2":
                publish_one("tx/stream/mode", mode_before, "F5OEO runtime pass request")
                if pass_delay > 0:
                    time.sleep(pass_delay)
            for suffix, payload in post_sequence:
                publish_one(suffix, payload, post_label)
                if step_delay > 0:
                    time.sleep(step_delay)
            if hook_mode == "plutodvb2":
                publish_one("tx/stream/mode", mode_after, "F5OEO runtime pass reassert")
        except Exception as exc:
            label = "F5OEO runtime reassert warning" if hook_mode == "plutodvb2" else "0303 runtime reassert warning"
            self.output_signal.emit(f"{label}: {exc}")

    def _terminate_process_tree(self, process_obj):
        if not process_obj:
            return

        try:
            if os.name == "posix":
                try:
                    os.killpg(os.getpgid(process_obj.pid), signal.SIGTERM)
                except Exception:
                    process_obj.terminate()
            else:
                process_obj.terminate()
        except Exception:
            pass

        for _ in range(20):
            try:
                if process_obj.poll() is not None:
                    break
            except Exception:
                break
            time.sleep(0.05)

        try:
            if process_obj.poll() is None:
                if os.name == "posix":
                    try:
                        os.killpg(os.getpgid(process_obj.pid), signal.SIGKILL)
                    except Exception:
                        process_obj.kill()
                else:
                    process_obj.kill()
        except Exception:
            pass

        for stream_name in ("stdin", "stdout", "stderr"):
            try:
                stream = getattr(process_obj, stream_name, None)
                if stream is not None:
                    stream.close()
            except Exception:
                pass

    def stop(self):
        self.running = False
        self.stop_requested = True
        stop_event = getattr(self, '_console_mqtt_stop_event', None)
        if stop_event is not None:
            try:
                stop_event.set()
            except Exception:
                pass
        client = getattr(self, '_console_mqtt_client', None)
        if client is not None:
            try:
                client.disconnect()
            except Exception:
                pass
        self._terminate_process_tree(self.ffmpeg_process)
        self._terminate_process_tree(self.tx_process)

class BackgroundTaskWorker(QThread):
    result_signal = pyqtSignal(str, object)
    error_signal = pyqtSignal(str, str)

    def __init__(self, task_name, task_callable):
        super().__init__()
        self.task_name = task_name
        self.task_callable = task_callable

    def run(self):
        try:
            result = self.task_callable()
            self.result_signal.emit(self.task_name, result)
        except Exception as exc:
            self.error_signal.emit(self.task_name, str(exc))


class Dvbs2TxGui(QMainWindow):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("DATV-Linux")
        self.setMinimumSize(700, 620)
        self.resize(760, 780)
        
        self.process_worker = None
        self.watchdog_timer = QTimer()
        self.watchdog_timer.timeout.connect(self.check_process_health)
        self.stats_timer = QTimer()
        self.stats_timer.timeout.connect(self.update_stats_display)
        self.start_time = None
        self.last_frames = 0
        self.debug_mode = False
        self.ffmpeg_encoder_cache = None
        self.encoder_runtime_cache = None
        self.encoder_probe_cache = {}
        self.runtime_env_cache = None
        self.profile_lock = False
        self.qpsk_fec_rates = ["1/4", "1/3", "2/5", "1/2", "3/5", "2/3", "3/4", "4/5", "5/6", "8/9", "9/10"]
        self.psk8_fec_rates = ["3/5", "2/3", "3/4", "5/6", "8/9", "9/10"]
        self.apsk16_fec_rates = ["2/3", "3/4", "4/5", "5/6", "8/9", "9/10"]
        self.apsk32_fec_rates = ["3/4", "4/5", "5/6", "8/9", "9/10"]
        self.device_status = {"hackrf": False, "lime_mini": False, "pluto_adalm_default": False, "plutodvb2_f5oeo": False, "pluto_0303_2402": False}
        self.window_size_presets = {
            "Raspberry Pi 5": {
                "compact": True,
                "minimum": (680, 600),
                "target": (720, 740),
            },
            "Standard": {
                "compact": False,
                "minimum": (720, 640),
                "target": (820, 860),
            },
            "Large": {
                "compact": False,
                "minimum": (780, 680),
                "target": (980, 980),
            },
        }
        self.tx_ring_buffer_kb = 64 * 1024
        self.overlay_callsign_manual = False
        self.camera_sources = []
        self.audio_sources = []
        self._ignore_next_finished_signal = False
        self._applying_stop_policy = False
        self._closing_in_progress = False
        self.settings = QSettings('OpenAI', 'DATV-Linux')
        self._loading_settings = False
        self.rf_probe_worker = None
        self.encoder_probe_worker = None
        self.rf_probe_pending = False
        self.encoder_probe_pending = False
        self.rf_detect_timer = QTimer(self)
        self.rf_detect_timer.setSingleShot(True)
        self.rf_detect_timer.setInterval(1200)
        self.rf_detect_timer.timeout.connect(self.detect_rf_devices)
        
        self.init_ui()
        self.check_dependencies()
        
    def init_ui(self):
        central_widget = QWidget()
        central_layout = QVBoxLayout(central_widget)
        self.setStyleSheet(self.build_global_stylesheet())
        central_layout.setSpacing(8)
        central_layout.setContentsMargins(8, 8, 8, 8)

        scroll = QScrollArea()
        scroll.setWidgetResizable(True)
        scroll.setHorizontalScrollBarPolicy(Qt.ScrollBarPolicy.ScrollBarAlwaysOff)
        
        main_widget = QWidget()
        main_layout = QVBoxLayout(main_widget)
        main_layout.setSpacing(10)
        main_layout.setContentsMargins(10, 10, 10, 10)
        
        header_widget = QWidget()
        header_layout = QGridLayout(header_widget)
        header_layout.setContentsMargins(0, 0, 0, 0)
        header_layout.setColumnStretch(0, 1)
        header_layout.setColumnStretch(1, 1)
        header_layout.setColumnStretch(2, 1)

        self.header_callsign = QLabel("CALLSIGN")
        self.header_callsign.setAlignment(Qt.AlignmentFlag.AlignLeft | Qt.AlignmentFlag.AlignVCenter)
        self.header_callsign.setStyleSheet("font-size: 18px; font-weight: bold; color: #ecf0f1;")
        header_layout.addWidget(self.header_callsign, 0, 0)

        header_center = QLabel("DATV-Linux")
        header_center.setAlignment(Qt.AlignmentFlag.AlignCenter)
        header_center.setStyleSheet("font-size: 22px; font-weight: bold; color: #ffffff;")
        header_layout.addWidget(header_center, 0, 1)

        window_preset_widget = QWidget()
        window_preset_layout = QHBoxLayout(window_preset_widget)
        window_preset_layout.setContentsMargins(0, 0, 0, 0)
        window_preset_layout.setSpacing(6)

        self.label_window_preset = QLabel("Window Size")
        self.label_window_preset.setAlignment(Qt.AlignmentFlag.AlignRight | Qt.AlignmentFlag.AlignVCenter)
        self.label_window_preset.setStyleSheet("font-size: 13px; font-weight: 700; color: #ffffff;")
        window_preset_layout.addWidget(self.label_window_preset)

        self.combo_window_preset = QComboBox()
        self.combo_window_preset.addItems(list(self.window_size_presets.keys()))
        self.combo_window_preset.setCursor(Qt.CursorShape.PointingHandCursor)
        self.combo_window_preset.setToolTip("Choose an exact window-size preset. Raspberry Pi 5 is the smaller layout for Pi displays, Standard is the normal desktop layout, and Large adds more working space.")
        self.combo_window_preset.setMinimumWidth(160)
        self.combo_window_preset.setStyleSheet("""
            QComboBox {
                color: #ffffff;
                background-color: #2c3e50;
                border: 1px solid #5d6d7e;
                border-radius: 6px;
                padding: 4px 22px 4px 8px;
                font-weight: 700;
            }
            QComboBox:hover {
                border: 1px solid #85c1e9;
            }
            QComboBox QAbstractItemView {
                color: #ffffff;
                background-color: #2c3e50;
                selection-background-color: #3498db;
                selection-color: #ffffff;
                outline: 0;
            }
        """)
        self.combo_window_preset.currentTextChanged.connect(self.on_window_preset_changed)
        self.combo_window_preset.currentTextChanged.connect(self.save_persisted_settings)
        window_preset_layout.addWidget(self.combo_window_preset)

        header_layout.addWidget(window_preset_widget, 0, 2, alignment=Qt.AlignmentFlag.AlignRight | Qt.AlignmentFlag.AlignVCenter)

        header_widget.setStyleSheet("background-color: #2c3e50; border-radius: 8px; padding: 6px;")
        main_layout.addWidget(header_widget)

        station_group = QGroupBox("Station Callsign *")
        station_grid = QGridLayout()
        station_grid.setSpacing(8)

        station_grid.addWidget(QLabel("Station Callsign"), 0, 0)
        self.input_pluto_topic = QLineEdit("")
        self.input_pluto_topic.setPlaceholderText("Enter your station callsign, for example: OD5TB")
        self.input_pluto_topic.setToolTip("Primary station callsign. For F5OEO firmware it must match the callsign / topic key configured in the Pluto firmware. Used for MQTT topics like cmd/pluto/<CALL>/tx/stream/mode.")
        station_grid.addWidget(self.input_pluto_topic, 0, 1)

        self.label_f5oeo_callsign_state = QLabel("")
        self.label_f5oeo_callsign_state.setVisible(False)
        station_grid.addWidget(self.label_f5oeo_callsign_state, 1, 0, 1, 2)

        self.label_f5oeo_topic_preview = QLabel("")
        self.label_f5oeo_topic_preview.setVisible(False)
        station_grid.addWidget(self.label_f5oeo_topic_preview, 2, 0, 1, 2)

        station_group.setLayout(station_grid)
        main_layout.addWidget(station_group)

        tabs = QTabWidget()
        tabs.setDocumentMode(True)
        tabs.setUsesScrollButtons(True)
        self.tabs = tabs
        tabs.setStyleSheet(self.build_tab_stylesheet(False))
        
        # === Main Configuration Tab ===
        main_tab = QWidget()
        main_layout_tab = QVBoxLayout(main_tab)
        main_layout_tab.setSpacing(12)
        main_layout_tab.setContentsMargins(6, 6, 6, 6)
        
        # DVB-S2 Configuration
        dvb_group = QGroupBox("DVB-S2 Physical Layer")
        dvb_grid = QGridLayout()
        dvb_grid.setSpacing(10)
        
        dvb_grid.addWidget(QLabel("Modulation:"), 0, 0)
        self.combo_mode = QComboBox()
        self.combo_mode.addItems(["QPSK", "8PSK", "16APSK", "32APSK"])
        self.combo_mode.setMinimumWidth(140)
        dvb_grid.addWidget(self.combo_mode, 0, 1)
        
        dvb_grid.addWidget(QLabel("FEC Rate:"), 1, 0)
        self.combo_fec = QComboBox()
        self.combo_fec.setMinimumWidth(140)
        dvb_grid.addWidget(self.combo_fec, 1, 1)
        
        dvb_grid.addWidget(QLabel("Frame Size:"), 2, 0)
        self.combo_frame = QComboBox()
        self.combo_frame.addItems(["Short (16200 bits)", "Normal (64800 bits)"])
        self.combo_frame.setMinimumWidth(140)
        dvb_grid.addWidget(self.combo_frame, 2, 1)
        
        dvb_grid.addWidget(QLabel("Symbol Rate (kS/s):"), 3, 0)
        self.combo_sr = QComboBox()
        sr_options = ["25", "35", "50", "66", "100", "125", "150", "200", "250", "333", "500", "1000"]
        self.combo_sr.addItems(sr_options)
        self.combo_sr.setEditable(True)
        self.combo_sr.setCurrentText("250")
        self.combo_sr.setMinimumWidth(140)
        dvb_grid.addWidget(self.combo_sr, 3, 1)
        
        dvb_grid.addWidget(QLabel("Roll-off:"), 4, 0)
        self.combo_rolloff = QComboBox()
        self.combo_rolloff.addItems(["0.20", "0.25", "0.35"])
        self.combo_rolloff.setCurrentText("0.20")
        self.combo_rolloff.setMinimumWidth(140)
        dvb_grid.addWidget(self.combo_rolloff, 4, 1)
        
        self.check_pilots = QCheckBox("Enable Pilots (recommended)")
        self.check_pilots.setChecked(True)
        dvb_grid.addWidget(self.check_pilots, 5, 0, 1, 2)

        dvb_grid.addWidget(QLabel("Gold Code:"), 6, 0)
        self.spin_goldcode = QSpinBox()
        self.spin_goldcode.setRange(0, 262142)
        self.spin_goldcode.setValue(0)
        self.spin_goldcode.setSpecialValueText("0 (default)")
        self.spin_goldcode.setToolTip("Physical-layer scrambling Gold code number")
        self.spin_goldcode.setMinimumWidth(140)
        dvb_grid.addWidget(self.spin_goldcode, 6, 1)
        
        self.bitrate_estimate = QLabel("📊 Est. DVB-S2 TS Rate: --- kbps")
        self.bitrate_estimate.setStyleSheet("color: #2980b9; font-weight: bold;")
        dvb_grid.addWidget(self.bitrate_estimate, 7, 0, 1, 2)

        self.bandwidth_estimate = QLabel("📡 Occupied RF Bandwidth: --- kHz")
        self.bandwidth_estimate.setStyleSheet("color: #16a085; font-weight: bold;")
        dvb_grid.addWidget(self.bandwidth_estimate, 8, 0, 1, 2)
        
        self.combo_mode.currentTextChanged.connect(self.update_fec_options)
        self.combo_mode.currentTextChanged.connect(self.update_stream_guidance)
        self.combo_mode.currentTextChanged.connect(self.refresh_auto_profile_if_needed)
        self.combo_sr.currentTextChanged.connect(self.update_bitrate_estimate)
        self.combo_sr.currentTextChanged.connect(self.update_stream_guidance)
        self.combo_sr.currentTextChanged.connect(self.refresh_auto_profile_if_needed)
        self.combo_sr.currentTextChanged.connect(self.update_rf_sample_rate_hint)
        self.combo_rolloff.currentTextChanged.connect(self.update_bitrate_estimate)
        self.combo_fec.currentTextChanged.connect(self.update_bitrate_estimate)
        self.combo_fec.currentTextChanged.connect(self.update_stream_guidance)
        self.combo_fec.currentTextChanged.connect(self.refresh_auto_profile_if_needed)
        self.check_pilots.stateChanged.connect(self.update_bitrate_estimate)
        self.check_pilots.stateChanged.connect(self.update_stream_guidance)
        self.check_pilots.stateChanged.connect(self.refresh_auto_profile_if_needed)
        self.combo_frame.currentTextChanged.connect(self.update_fec_options)
        self.combo_frame.currentTextChanged.connect(self.update_stream_guidance)
        self.combo_frame.currentTextChanged.connect(self.refresh_auto_profile_if_needed)
        
        dvb_group.setLayout(dvb_grid)
        main_layout_tab.addWidget(dvb_group)
        self.update_fec_options()
        
        # === RF Configuration (shown on Main tab) ===
        rf_group = QGroupBox("RF Transmitter")
        rf_grid = QGridLayout()
        rf_grid.setSpacing(10)

        rf_grid.addWidget(QLabel("Device:"), 0, 0)
        self.combo_device = QComboBox()
        self.combo_device.setSizeAdjustPolicy(QComboBox.SizeAdjustPolicy.AdjustToMinimumContentsLengthWithIcon)
        self.combo_device.setMinimumContentsLength(20)
        self.combo_device.addItems([
            "LimeSDR mini",
            "HackRFone",
            "AdalmPluto",
            "PlutoDVB2 F5OEO",
            "Pluto F5UII 0303/2402",
        ])
        rf_grid.addWidget(self.combo_device, 0, 1)

        rf_grid.addWidget(QLabel("Frequency (MHz):"), 1, 0)
        self.input_freq = QLineEdit("2407.500")
        self.input_freq.setToolTip("QO-100 uplink: 2400-2450 MHz")
        self.input_freq.setMinimumWidth(150)
        rf_grid.addWidget(self.input_freq, 1, 1)

        self.label_tx_level = QLabel("TX Level (dB):")
        rf_grid.addWidget(self.label_tx_level, 2, 0)
        self.tx_level_spin = QDoubleSpinBox()
        self.tx_level_spin.setRange(0.0, 47.0)
        self.tx_level_spin.setDecimals(0)
        self.tx_level_spin.setSingleStep(1.0)
        self.tx_level_spin.setValue(30.0)
        self.tx_level_spin.setSuffix(" dB")
        self.tx_level_spin.setMinimumWidth(150)
        rf_grid.addWidget(self.tx_level_spin, 2, 1)

        self.label_tx_hint = QLabel("HackRFone gain range: 0 to 47 dB")
        self.label_tx_hint.setStyleSheet("color: #7f8c8d;")
        rf_grid.addWidget(self.label_tx_hint, 3, 0, 1, 2)

        self.check_amp = QCheckBox("Enable Internal Amp (+14dB, HackRF only)")
        rf_grid.addWidget(self.check_amp, 4, 0, 1, 2)

        rf_grid.addWidget(QLabel("Pluto IP Address:"), 5, 0)
        self.input_pluto_ip = QLineEdit("192.168.2.1")
        self.input_pluto_ip.setPlaceholderText("192.168.2.1")
        rf_grid.addWidget(self.input_pluto_ip, 5, 1)

        self.label_pluto_ports_info = QLabel("")
        self.label_pluto_ports_info.setVisible(False)

        self.label_lime_mini_status = QLabel("LimeSDR mini: not checked")
        self.label_hackrf_status = QLabel("HackRFone: not checked")
        self.label_pluto_adalm_default_status = QLabel("AdalmPluto: not checked")
        self.label_plutodvb2_f5oeo_status = QLabel("PlutoDVB2 F5OEO: not checked")
        self.label_pluto_0303_status = QLabel("Pluto F5UII 0303/2402: not checked")
        rf_grid.addWidget(self.label_lime_mini_status, 9, 0, 1, 2)
        rf_grid.addWidget(self.label_hackrf_status, 10, 0, 1, 2)
        rf_grid.addWidget(self.label_pluto_adalm_default_status, 11, 0, 1, 2)
        rf_grid.addWidget(self.label_plutodvb2_f5oeo_status, 12, 0, 1, 2)
        rf_grid.addWidget(self.label_pluto_0303_status, 13, 0, 1, 2)

        self.btn_refresh_detection = QPushButton("Refresh Detection")
        rf_grid.addWidget(self.btn_refresh_detection, 14, 0, 1, 2)

        self.label_tx_sr_plan = QLabel("Local TX sample-rate estimate: ---")
        self.label_tx_sr_plan.setWordWrap(True)
        self.label_tx_sr_plan.setStyleSheet("color: #7f8c8d;")
        rf_grid.addWidget(self.label_tx_sr_plan, 15, 0, 1, 2)

        self.combo_device.currentTextChanged.connect(self.update_device_controls)
        self.combo_device.currentTextChanged.connect(self.update_f5oeo_callsign_ui)
        self.combo_device.currentTextChanged.connect(self.update_rf_sample_rate_hint)
        self.combo_device.currentTextChanged.connect(self.update_tx_action_state)
        self.combo_device.currentTextChanged.connect(self.schedule_rf_detection)
        self.combo_device.currentTextChanged.connect(self.save_persisted_settings)
        self.input_pluto_ip.editingFinished.connect(self.on_pluto_ip_editing_finished)
        self.input_pluto_ip.textEdited.connect(self.on_pluto_ip_text_edited)
        self.input_pluto_topic.textChanged.connect(self.update_f5oeo_callsign_ui)
        self.input_pluto_topic.textChanged.connect(self.update_tx_action_state)
        self.input_pluto_topic.textChanged.connect(self.update_callsign_header)
        self.input_pluto_topic.textChanged.connect(self.maybe_fill_overlay_callsign)
        self.input_pluto_topic.textChanged.connect(self.save_persisted_settings)
        self.input_pluto_topic.editingFinished.connect(self.normalize_f5oeo_callsign_entry)
        self.btn_refresh_detection.clicked.connect(self.detect_rf_devices)

        rf_group.setLayout(rf_grid)
        main_layout_tab.addWidget(rf_group)
        main_layout_tab.addStretch()
        
        # Source Selection
        src_group = QGroupBox("Media Source")
        src_layout = QVBoxLayout()
        src_layout.setSpacing(5)
        
        self.source_combo = QComboBox()
        self.source_combo.addItems(["Video File", "Webcam / Camera", "External Stream (OBS / FFmpeg)"])
        self.source_combo.setMinimumWidth(220)
        src_layout.addWidget(self.source_combo)
        self.source_combo.currentTextChanged.connect(self.update_source_controls)
        self.source_combo.currentTextChanged.connect(self.update_audio_controls)
        self.source_combo.currentTextChanged.connect(self.update_stream_guidance)
        self.source_combo.currentTextChanged.connect(self.update_tx_action_state)
        self.source_combo.currentTextChanged.connect(self.save_persisted_settings)
        
        self.file_row_widget = QWidget()
        file_layout = QHBoxLayout(self.file_row_widget)
        file_layout.setContentsMargins(0, 0, 0, 0)
        self.file_path = QLineEdit()
        self.file_path.setPlaceholderText("Browse to a video file or use the DATV test pattern...")
        default_test_pattern = self.get_test_pattern_path()
        if os.path.exists(default_test_pattern):
            self.file_path.setText(default_test_pattern)
        self.file_path.textChanged.connect(self.save_persisted_settings)
        self.file_path.textChanged.connect(self.update_tx_action_state)
        self.btn_browse = QPushButton("Browse")
        self.btn_browse.setMaximumWidth(80)
        self.btn_browse.clicked.connect(self.browse_file)
        self.btn_test_pattern = QPushButton("Use Test Pattern")
        self.btn_test_pattern.setMaximumWidth(140)
        self.btn_test_pattern.clicked.connect(self.use_test_pattern_file)
        file_layout.addWidget(self.file_path)
        file_layout.addWidget(self.btn_browse)
        file_layout.addWidget(self.btn_test_pattern)
        src_layout.addWidget(self.file_row_widget)

        self.stream_row_widget = QWidget()
        stream_layout = QVBoxLayout(self.stream_row_widget)
        stream_layout.setContentsMargins(0, 0, 0, 0)
        self.stream_url = QLineEdit("udp://0.0.0.0:23000?fifo_size=1000000&overrun_nonfatal=1")
        self.stream_url.textChanged.connect(self.save_persisted_settings)
        self.stream_url.textChanged.connect(self.update_tx_action_state)
        self.stream_url.setPlaceholderText("ffmpeg input URL, e.g. udp://0.0.0.0:23000 or srt://0.0.0.0:9000?mode=listener")
        stream_layout.addWidget(self.stream_url)
        self.stream_hint = QLabel("Use any FFmpeg-compatible input URL for external encoders or network feeds, including OBS/FFmpeg pipelines.")
        self.stream_hint.setWordWrap(True)
        self.stream_hint.setStyleSheet("color: #7f8c8d;")
        stream_layout.addWidget(self.stream_hint)
        src_layout.addWidget(self.stream_row_widget)

        self.camera_row_widget = QWidget()
        camera_layout = QHBoxLayout(self.camera_row_widget)
        camera_layout.setContentsMargins(0, 0, 0, 0)
        self.camera_device_combo = QComboBox()
        self.camera_device_combo.setMinimumWidth(260)
        self.camera_device_combo.currentIndexChanged.connect(self.save_persisted_settings)
        self.camera_device_combo.currentIndexChanged.connect(self.update_tx_action_state)
        self.camera_device_combo.currentIndexChanged.connect(self.update_camera_probe_details)
        self.btn_refresh_cameras = QPushButton("Refresh Cameras")
        self.btn_refresh_cameras.setMaximumWidth(140)
        self.btn_refresh_cameras.clicked.connect(self.refresh_camera_sources)
        camera_layout.addWidget(self.camera_device_combo, 1)
        camera_layout.addWidget(self.btn_refresh_cameras)
        src_layout.addWidget(self.camera_row_widget)

        self.camera_hint = QLabel("Detected Linux V4L2 capture devices are listed here when a webcam is connected.")
        self.camera_hint.setWordWrap(True)
        self.camera_hint.setStyleSheet("color: #7f8c8d;")
        src_layout.addWidget(self.camera_hint)

        self.camera_options_widget = QWidget()
        camera_options_layout = QGridLayout(self.camera_options_widget)
        camera_options_layout.setContentsMargins(0, 0, 0, 0)
        camera_options_layout.setHorizontalSpacing(8)
        camera_options_layout.setVerticalSpacing(6)

        self.camera_probe_details = QLabel("Webcam probe details will appear here after detection.")
        self.camera_probe_details.setWordWrap(True)
        self.camera_probe_details.setStyleSheet("color: #7f8c8d;")
        camera_options_layout.addWidget(self.camera_probe_details, 0, 0, 1, 4)

        self.check_camera_mic = QCheckBox("Route microphone / line input with webcam")
        self.check_camera_mic.setChecked(False)
        self.check_camera_mic.toggled.connect(self.update_audio_controls)
        self.check_camera_mic.toggled.connect(self.update_stream_guidance)
        self.check_camera_mic.toggled.connect(self.update_tx_action_state)
        self.check_camera_mic.toggled.connect(self.save_persisted_settings)
        camera_options_layout.addWidget(self.check_camera_mic, 1, 0, 1, 4)

        camera_options_layout.addWidget(QLabel("Mic Source:"), 2, 0)
        self.camera_audio_source_combo = QComboBox()
        self.camera_audio_source_combo.setMinimumWidth(260)
        self.camera_audio_source_combo.currentIndexChanged.connect(self.update_audio_controls)
        self.camera_audio_source_combo.currentIndexChanged.connect(self.update_tx_action_state)
        self.camera_audio_source_combo.currentIndexChanged.connect(self.save_persisted_settings)
        camera_options_layout.addWidget(self.camera_audio_source_combo, 2, 1, 1, 2)

        self.btn_refresh_audio_sources = QPushButton("Refresh Mics")
        self.btn_refresh_audio_sources.setMaximumWidth(140)
        self.btn_refresh_audio_sources.clicked.connect(self.refresh_audio_sources)
        camera_options_layout.addWidget(self.btn_refresh_audio_sources, 2, 3)

        self.camera_audio_hint = QLabel("Choose a PulseAudio or ALSA capture source for webcam audio. Audio is only used if both this route and the main Audio checkbox are enabled.")
        self.camera_audio_hint.setWordWrap(True)
        self.camera_audio_hint.setStyleSheet("color: #7f8c8d;")
        camera_options_layout.addWidget(self.camera_audio_hint, 3, 0, 1, 4)

        camera_options_layout.addWidget(QLabel("Custom Video Input:"), 4, 0)
        self.camera_custom_video_input = QLineEdit("")
        self.camera_custom_video_input.setPlaceholderText("Optional FFmpeg webcam input override, e.g. -f v4l2 -input_format mjpeg -framerate 25 -video_size 640x480 -i /dev/video0")
        self.camera_custom_video_input.textChanged.connect(self.update_camera_probe_details)
        self.camera_custom_video_input.textChanged.connect(self.update_tx_action_state)
        self.camera_custom_video_input.textChanged.connect(self.save_persisted_settings)
        camera_options_layout.addWidget(self.camera_custom_video_input, 4, 1, 1, 3)

        camera_options_layout.addWidget(QLabel("Custom Mic Input:"), 5, 0)
        self.camera_custom_audio_input = QLineEdit("")
        self.camera_custom_audio_input.setPlaceholderText("Optional FFmpeg microphone override, e.g. -f pulse -i default")
        self.camera_custom_audio_input.textChanged.connect(self.update_audio_controls)
        self.camera_custom_audio_input.textChanged.connect(self.update_tx_action_state)
        self.camera_custom_audio_input.textChanged.connect(self.save_persisted_settings)
        camera_options_layout.addWidget(self.camera_custom_audio_input, 5, 1, 1, 3)

        self.camera_custom_hint = QLabel("Leave the custom input strings empty to use automatic webcam probing. Custom strings must include the full FFmpeg input arguments, including -f and -i.")
        self.camera_custom_hint.setWordWrap(True)
        self.camera_custom_hint.setStyleSheet("color: #7f8c8d;")
        camera_options_layout.addWidget(self.camera_custom_hint, 6, 0, 1, 4)
        src_layout.addWidget(self.camera_options_widget)
        
        src_group.setLayout(src_layout)
        self.media_source_group = src_group
        
        # Performance Stats
        stats_group = QGroupBox("Performance Statistics")
        stats_layout = QGridLayout()
        stats_layout.setSpacing(10)
        
        stats_layout.addWidget(QLabel("Frames Sent:"), 0, 0)
        self.frame_counter = QLabel("0")
        self.frame_counter.setStyleSheet("font-weight: bold; font-size: 14px; color: #27ae60;")
        stats_layout.addWidget(self.frame_counter, 0, 1)
        
        stats_layout.addWidget(QLabel("Buffer Level:"), 0, 2)
        self.buffer_level = QLabel("0%")
        self.buffer_level.setStyleSheet("font-weight: bold; font-size: 14px;")
        stats_layout.addWidget(self.buffer_level, 0, 3)
        
        stats_layout.addWidget(QLabel("Frame Rate:"), 1, 0)
        self.fps_label = QLabel("0 fps")
        self.fps_label.setStyleSheet("font-weight: bold; font-size: 14px;")
        stats_layout.addWidget(self.fps_label, 1, 1)
        
        stats_layout.addWidget(QLabel("Bitrate:"), 1, 2)
        self.bitrate_label = QLabel("0 kbps")
        self.bitrate_label.setStyleSheet("font-weight: bold; font-size: 14px;")
        stats_layout.addWidget(self.bitrate_label, 1, 3)
        
        stats_layout.addWidget(QLabel("Underflows:"), 2, 0)
        self.underflow_label = QLabel("0")
        self.underflow_label.setStyleSheet("font-weight: bold; font-size: 14px;")
        stats_layout.addWidget(self.underflow_label, 2, 1)
        
        self.buffer_bar = QProgressBar()
        self.buffer_bar.setRange(0, 100)
        stats_layout.addWidget(self.buffer_bar, 3, 0, 1, 4)
        
        stats_group.setLayout(stats_layout)
        self.performance_stats_group = stats_group
        
        tabs.addTab(main_tab, "Main")
        
        # === Encoder Settings Tab ===
        encoder_tab = QWidget()
        encoder_layout = QVBoxLayout(encoder_tab)
        encoder_layout.setSpacing(12)
        encoder_layout.setContentsMargins(6, 6, 6, 6)
        
        video_group = QGroupBox("Video Encoder")
        video_grid = QGridLayout()
        video_grid.setSpacing(10)

        video_grid.addWidget(QLabel("Profile:"), 0, 0)
        self.combo_profile = QComboBox()
        self.combo_profile.addItems(["Auto Detect", "Custom", "Pi 5 Safe", "Linux PC Balanced", "Low SR DATV", "Higher SR DATV"])
        video_grid.addWidget(self.combo_profile, 0, 1)

        self.check_low_sr_stabilization = QCheckBox("Low SR stabilization (<250 kS/s)")
        self.check_low_sr_stabilization.setChecked(False)
        self.check_low_sr_stabilization.setToolTip("Adds conservative low-symbol-rate FFmpeg mux/encoder timing settings and enables a stronger RRC pulse-shaping path in dvbs2_tx. Applies to both software and hardware video encoders.")
        self.check_low_sr_stabilization.toggled.connect(self.update_stream_guidance)
        self.check_low_sr_stabilization.toggled.connect(self.mark_profile_custom)
        self.check_low_sr_stabilization.toggled.connect(self.save_persisted_settings)
        video_grid.addWidget(self.check_low_sr_stabilization, 1, 0, 1, 2)

        self.low_sr_hint = QLabel("Optional low-rate helper for operation below 250 kS/s. Leave it off unless you need the extra FFmpeg/RRC hardening for very low symbol-rate work.")
        self.low_sr_hint.setWordWrap(True)
        self.low_sr_hint.setStyleSheet("color: #7f8c8d;")
        video_grid.addWidget(self.low_sr_hint, 2, 0, 1, 2)

        video_grid.addWidget(QLabel("Resolution:"), 3, 0)
        self.combo_res = QComboBox()
        self.combo_res.addItems(["320x240", "480x270", "640x360", "854x480", "1280x720", "1920x1080"])
        self.combo_res.setCurrentText("640x360")
        video_grid.addWidget(self.combo_res, 3, 1)

        video_grid.addWidget(QLabel("FPS:"), 4, 0)
        self.combo_fps = QComboBox()
        self.combo_fps.addItems(["10", "15", "20", "25", "30", "50", "60"])
        self.combo_fps.setCurrentText("25")
        video_grid.addWidget(self.combo_fps, 4, 1)

        video_grid.addWidget(QLabel("Codec:"), 5, 0)
        self.combo_vcodec = QComboBox()
        video_grid.addWidget(self.combo_vcodec, 5, 1)

        self.label_encoder_hint = QLabel("Detecting FFmpeg video encoders…")
        self.label_encoder_hint.setWordWrap(True)
        self.label_encoder_hint.setStyleSheet("color: #7f8c8d;")
        video_grid.addWidget(self.label_encoder_hint, 6, 0, 1, 2)

        video_grid.addWidget(QLabel("Preset:"), 7, 0)
        self.combo_preset = QComboBox()
        self.combo_preset.addItems(["ultrafast", "superfast", "veryfast", "faster", "fast", "medium"])
        self.combo_preset.setCurrentText("veryfast")
        video_grid.addWidget(self.combo_preset, 7, 1)

        video_grid.addWidget(QLabel("Tune:"), 8, 0)
        self.combo_tune = QComboBox()
        self.combo_tune.addItems(["zerolatency", "film", "animation", "grain", "stillimage", "fastdecode"])
        self.combo_tune.setCurrentText("zerolatency")
        video_grid.addWidget(self.combo_tune, 8, 1)

        video_grid.addWidget(QLabel("Video Bitrate (% of net):"), 9, 0)
        self.video_br_slider = QSlider(Qt.Orientation.Horizontal)
        self.video_br_slider.setRange(50, 85)
        self.video_br_slider.setValue(70)
        self.video_br_label = QLabel("70%")
        self.video_br_label.setMinimumWidth(40)
        video_br_layout = QHBoxLayout()
        video_br_layout.addWidget(self.video_br_slider)
        video_br_layout.addWidget(self.video_br_label)
        video_grid.addLayout(video_br_layout, 9, 1)
        self.video_br_slider.valueChanged.connect(lambda v: self.video_br_label.setText(f"{v}%"))
        self.video_br_slider.valueChanged.connect(self.update_stream_guidance)

        video_grid.addWidget(QLabel("GOP Size:"), 10, 0)
        self.gop_size = QSpinBox()
        self.gop_size.setRange(1, 120)
        self.gop_size.setValue(30)
        video_grid.addWidget(self.gop_size, 10, 1)

        self.stream_guidance = QLabel("Stream guidance will appear here after bitrate estimation.")
        self.stream_guidance.setWordWrap(True)
        self.stream_guidance.setStyleSheet("color: #2c3e50; background: #ecf0f1; padding: 6px; border-radius: 4px;")
        video_grid.addWidget(self.stream_guidance, 11, 0, 1, 2)

        self.combo_profile.currentTextChanged.connect(self.apply_video_profile)
        self.combo_res.currentTextChanged.connect(self.update_stream_guidance)
        self.combo_fps.currentTextChanged.connect(self.update_stream_guidance)
        self.combo_vcodec.currentTextChanged.connect(self.update_video_encoder_controls)
        self.combo_vcodec.currentTextChanged.connect(self.update_stream_guidance)
        self.combo_preset.currentTextChanged.connect(self.mark_profile_custom)
        self.combo_tune.currentTextChanged.connect(self.update_stream_guidance)
        self.combo_tune.currentTextChanged.connect(self.mark_profile_custom)
        self.video_br_slider.valueChanged.connect(self.mark_profile_custom)
        self.gop_size.valueChanged.connect(self.mark_profile_custom)
        self.combo_res.currentTextChanged.connect(self.mark_profile_custom)
        self.combo_fps.currentTextChanged.connect(self.mark_profile_custom)
        self.combo_vcodec.currentTextChanged.connect(self.mark_profile_custom)

        video_group.setLayout(video_grid)
        encoder_layout.addWidget(video_group)
        
        # Audio Encoder Settings
        audio_group = QGroupBox("Audio Encoder")
        audio_grid = QGridLayout()
        audio_grid.setSpacing(10)
        
        self.check_audio = QCheckBox("Enable Audio")
        self.check_audio.setChecked(False)
        audio_grid.addWidget(self.check_audio, 0, 0, 1, 2)

        audio_grid.addWidget(QLabel("Bitrate (kbps):"), 1, 0)
        self.combo_a_br = QComboBox()
        self.combo_a_br.addItems(["16", "24", "32", "40", "48", "56", "64", "80", "96", "112", "128", "160", "192"])
        self.combo_a_br.setCurrentText("32")
        audio_grid.addWidget(self.combo_a_br, 1, 1)

        audio_grid.addWidget(QLabel("Codec:"), 2, 0)
        self.combo_a_codec = QComboBox()
        self.combo_a_codec.addItems(["AAC-LC", "HE-AAC (AAC+)", "MP2", "AC3", "E-AC3", "Opus"])
        self.combo_a_codec.setCurrentText("AAC-LC")
        audio_grid.addWidget(self.combo_a_codec, 2, 1)


        self.check_audio.toggled.connect(self.update_audio_controls)
        self.check_audio.toggled.connect(self.update_stream_guidance)
        self.combo_a_br.currentTextChanged.connect(self.update_stream_guidance)
        self.combo_a_codec.currentTextChanged.connect(self.update_audio_controls)
        self.combo_a_codec.currentTextChanged.connect(self.update_stream_guidance)
        
        self.check_audio.toggled.connect(self.mark_profile_custom)
        self.combo_a_br.currentTextChanged.connect(self.mark_profile_custom)
        self.combo_a_codec.currentTextChanged.connect(self.mark_profile_custom)

        audio_group.setLayout(audio_grid)
        encoder_layout.addWidget(audio_group)

        service_group = QGroupBox("MPEG-TS Service")
        service_grid = QGridLayout()
        service_grid.setSpacing(10)

        service_grid.addWidget(QLabel("Service Name:"), 0, 0)
        self.service_name_input = QLineEdit("OD5TB")
        self.service_name_input.setPlaceholderText("Shown by SDRangel as the MPEG-TS service name")
        self.service_name_input.textChanged.connect(self.save_persisted_settings)
        service_grid.addWidget(self.service_name_input, 0, 1)

        service_grid.addWidget(QLabel("Service Provider:"), 1, 0)
        self.service_provider_input = QLineEdit("OD5TB")
        self.service_provider_input.setPlaceholderText("MPEG-TS service provider name")
        self.service_provider_input.textChanged.connect(self.save_persisted_settings)
        service_grid.addWidget(self.service_provider_input, 1, 1)

        self.service_hint = QLabel("FFmpeg will write service_name and service_provider into the transport stream for SDRangel and compatible receivers.")
        self.service_hint.setWordWrap(True)
        self.service_hint.setStyleSheet("color: #7f8c8d;")
        service_grid.addWidget(self.service_hint, 2, 0, 1, 2)

        service_group.setLayout(service_grid)
        encoder_layout.addWidget(service_group)
        
        tabs.addTab(encoder_tab, "Encoder")
        
        # === Source / Overlay Tab ===
        overlay_tab = QWidget()
        overlay_layout = QVBoxLayout(overlay_tab)
        overlay_layout.setSpacing(12)
        overlay_layout.setContentsMargins(6, 6, 6, 6)
        
        overlay_group = QGroupBox("Video Overlay")
        overlay_grid = QGridLayout()
        overlay_grid.setSpacing(10)
        
        self.check_overlay = QCheckBox("Enable Text Overlay")
        self.check_overlay.setChecked(True)
        overlay_grid.addWidget(self.check_overlay, 0, 0, 1, 2)
        
        overlay_grid.addWidget(QLabel("Overlay Text:"), 1, 0)
        self.callsign_input = QLineEdit("")
        self.callsign_input.textChanged.connect(self.on_overlay_callsign_changed)
        self.callsign_input.textChanged.connect(self.save_persisted_settings)
        overlay_grid.addWidget(self.callsign_input, 1, 1)
        
        overlay_grid.addWidget(QLabel("Position:"), 2, 0)
        self.combo_position = QComboBox()
        self.combo_position.addItems(["Top Left", "Top Right", "Bottom Left", "Bottom Right"])
        self.combo_position.setCurrentText("Top Left")
        overlay_grid.addWidget(self.combo_position, 2, 1)

        overlay_grid.addWidget(QLabel("Font Size:"), 3, 0)
        self.font_size = QComboBox()
        self.font_size.addItems(["16", "20", "24", "28", "32", "36", "48"])
        self.font_size.setCurrentText("24")
        overlay_grid.addWidget(self.font_size, 3, 1)

        overlay_grid.addWidget(QLabel("Font Color:"), 4, 0)
        self.combo_color = QComboBox()
        self.combo_color.addItems(["white", "yellow", "red", "green", "cyan"])
        self.combo_color.setCurrentText("white")
        overlay_grid.addWidget(self.combo_color, 4, 1)

        self.overlay_hint = QLabel("Overlay text is rendered inside FFmpeg. Leave this blank to use the top Callsign automatically.")
        self.overlay_hint.setWordWrap(True)
        self.overlay_hint.setStyleSheet("color: #7f8c8d;")
        overlay_grid.addWidget(self.overlay_hint, 5, 0, 1, 2)
        
        overlay_group.setLayout(overlay_grid)
        overlay_layout.addWidget(self.media_source_group)
        overlay_layout.addWidget(overlay_group)
        
        tabs.addTab(overlay_tab, "Source / Overlay")
        
        # === Console Tab ===
        console_tab = QWidget()
        console_layout = QVBoxLayout(console_tab)
        console_layout.setSpacing(12)
        console_layout.setContentsMargins(6, 6, 6, 6)
        
        self.console = QTextEdit()
        self.console.setReadOnly(True)
        self.console.setFont(QFont("Monospace", 9))
        self.console.setMaximumHeight(300)

        pluto_tools_group = QGroupBox("Pluto Maintenance")
        pluto_tools_grid = QGridLayout()
        pluto_tools_grid.setSpacing(8)

        self.btn_pluto_refresh_info = QPushButton("Refresh Pluto Info")
        self.btn_pluto_refresh_info.clicked.connect(self.refresh_pluto_device_info)
        pluto_tools_grid.addWidget(self.btn_pluto_refresh_info, 0, 0)

        self.btn_pluto_reboot = QPushButton("Reboot Pluto")
        self.btn_pluto_reboot.clicked.connect(self.reboot_pluto_device)
        pluto_tools_grid.addWidget(self.btn_pluto_reboot, 0, 1)

        self.label_pluto_info_status = QLabel("Refresh Pluto Info reads the context model over libiio.")
        self.label_pluto_info_status.setWordWrap(True)
        self.label_pluto_info_status.setStyleSheet("color: #7f8c8d;")
        pluto_tools_grid.addWidget(self.label_pluto_info_status, 1, 0, 1, 4)

        pluto_tools_grid.addWidget(QLabel("Stop / Exit Policy:"), 2, 0)
        self.combo_pluto_stop_policy = QComboBox()
        self.combo_pluto_stop_policy.addItem("Pass only", "pass_only")
        self.combo_pluto_stop_policy.addItem("Mute Pluto TX", "mute")
        self.combo_pluto_stop_policy.addItem("Reboot Pluto", "reboot")
        self.combo_pluto_stop_policy.setToolTip("Applied when STOP is pressed and when the GUI closes for Pluto-based backends.")
        self.combo_pluto_stop_policy.currentIndexChanged.connect(self.update_device_controls)
        self.combo_pluto_stop_policy.currentIndexChanged.connect(self.save_persisted_settings)
        pluto_tools_grid.addWidget(self.combo_pluto_stop_policy, 2, 1, 1, 1)

        self.label_pluto_stop_policy_hint = QLabel("Pass only leaves Pluto reachable after TX stops. Mute sets Pluto TX attenuation to a safe value. Reboot uses the same helper as the manual reboot button.")
        self.label_pluto_stop_policy_hint.setWordWrap(True)
        self.label_pluto_stop_policy_hint.setStyleSheet("color: #7f8c8d;")
        pluto_tools_grid.addWidget(self.label_pluto_stop_policy_hint, 2, 2, 1, 2)

        pluto_tools_grid.addWidget(QLabel("Context Model:"), 3, 0)
        self.value_pluto_hw_model = QLabel("—")
        self.value_pluto_hw_model.setTextInteractionFlags(Qt.TextInteractionFlag.TextSelectableByMouse)
        pluto_tools_grid.addWidget(self.value_pluto_hw_model, 3, 1, 1, 3)

        pluto_tools_group.setLayout(pluto_tools_grid)
        
        btn_layout = QHBoxLayout()
        self.btn_clear = QPushButton("Clear")
        self.btn_clear.setMaximumWidth(80)
        self.btn_clear.clicked.connect(lambda: self.console.clear())
        self.btn_save = QPushButton("Save Log")
        self.btn_save.setMaximumWidth(80)
        self.btn_save.clicked.connect(self.save_log)
        self.debug_check = QCheckBox("Debug Mode")
        self.debug_check.toggled.connect(lambda v: setattr(self, 'debug_mode', v))
        btn_layout.addWidget(self.btn_clear)
        btn_layout.addWidget(self.btn_save)
        btn_layout.addWidget(self.debug_check)
        btn_layout.addStretch()
        
        console_layout.addWidget(self.performance_stats_group)
        console_layout.addWidget(pluto_tools_group)
        console_layout.addWidget(self.console)
        console_layout.addLayout(btn_layout)
        
        tabs.addTab(console_tab, "Console")
        
        # Control Buttons
        btn_control_layout = QHBoxLayout()
        btn_control_layout.setSpacing(20)
        
        self.btn_verify = QPushButton("VERIFY TS")
        self.btn_verify.setMinimumHeight(50)
        self.btn_verify.setStyleSheet("""
            QPushButton {
                background-color: #2980b9;
                color: white;
                font-size: 15px;
                font-weight: bold;
                border-radius: 8px;
                padding: 10px;
            }
            QPushButton:hover {
                background-color: #3498db;
            }
            QPushButton:disabled {
                background-color: #95a5a6;
            }
        """)
        self.btn_verify.clicked.connect(self.verify_ts_configuration)
        self.btn_verify.setVisible(False)

        self.btn_start = QPushButton("▶ START")
        self.btn_start.setMinimumSize(180, 52)
        self.btn_start.setStyleSheet("""
            QPushButton {
                background-color: #27ae60;
                color: white;
                font-size: 16px;
                font-weight: bold;
                border-radius: 8px;
                padding: 10px;
            }
            QPushButton:hover {
                background-color: #2ecc71;
            }
            QPushButton:disabled {
                background-color: #95a5a6;
            }
        """)
        self.btn_start.clicked.connect(self.start_transmission)
        
        self.btn_stop = QPushButton("■ STOP")
        self.btn_stop.setMinimumSize(180, 52)
        self.btn_stop.setStyleSheet("""
            QPushButton {
                background-color: #c0392b;
                color: white;
                font-size: 16px;
                font-weight: bold;
                border-radius: 8px;
                padding: 10px;
            }
            QPushButton:hover {
                background-color: #e74c3c;
            }
            QPushButton:disabled {
                background-color: #95a5a6;
            }
        """)
        self.btn_stop.clicked.connect(self.stop_transmission)
        self.btn_stop.setEnabled(False)
        
        btn_control_layout.addStretch(1)
        btn_control_layout.addWidget(self.btn_start)
        btn_control_layout.addWidget(self.btn_stop)
        btn_control_layout.addStretch(1)

        self.tx_action_hint = QLabel("START will be enabled after RF, source, and bitrate checks pass.")
        self.tx_action_hint.setWordWrap(True)
        self.tx_action_hint.setStyleSheet("color: #7f8c8d; padding-top: 4px;")

        self.label_footer_license = QLabel("OD5RAL 2026")
        self.label_footer_license.setAlignment(Qt.AlignmentFlag.AlignRight | Qt.AlignmentFlag.AlignVCenter)
        self.label_footer_license.setStyleSheet("color: #7f8c8d; font-size: 11px; font-weight: 800; padding-top: 2px; letter-spacing: 0.2px;")

        main_layout.addWidget(tabs)
        
        scroll.setWidget(main_widget)
        central_layout.addWidget(scroll, 1)

        action_widget = QWidget()
        action_layout = QVBoxLayout(action_widget)
        action_layout.setContentsMargins(0, 0, 0, 0)
        action_layout.setSpacing(6)
        action_layout.addLayout(btn_control_layout)
        action_layout.addWidget(self.tx_action_hint)
        footer_layout = QHBoxLayout()
        footer_layout.setContentsMargins(0, 0, 0, 0)
        footer_layout.addStretch(1)
        footer_layout.addWidget(self.label_footer_license)
        action_layout.addLayout(footer_layout)
        central_layout.addWidget(action_widget, 0)

        self.setCentralWidget(central_widget)
        self.apply_window_size_preset("Standard")
        
        self.status_bar = self.statusBar()
        self.refresh_camera_sources()
        self.refresh_audio_sources()
        self.load_persisted_settings()
        self.stats_timer.start(1000)
        self.update_source_controls()
        self.update_audio_controls()
        self.update_device_controls()
        self.update_rf_sample_rate_hint()
        self.prepare_probe_placeholders()
        self.detect_rf_devices()
        self.populate_video_encoder_options(skip_runtime_probe=True)
        self.start_background_encoder_probe()
        self.combo_profile.setCurrentText("Auto Detect")
        self.update_bitrate_estimate()
        self.update_video_encoder_controls()
        self.update_stream_guidance()
        self.update_f5oeo_callsign_ui()
        self.update_callsign_header()
        self.update_tx_action_state()
        
    def build_global_stylesheet(self):
        return """
            QLabel, QCheckBox, QRadioButton, QPushButton, QTabBar::tab,
            QLineEdit, QComboBox, QSpinBox, QDoubleSpinBox {
                font-weight: 600;
            }
            QGroupBox {
                font-weight: 700;
            }
            QTextEdit, QPlainTextEdit {
                font-weight: 400;
            }
            QScrollBar:vertical {
                background: #d6dde5;
                width: 14px;
                margin: 2px;
                border-radius: 7px;
            }
            QScrollBar::handle:vertical {
                background: #5d6d7e;
                min-height: 28px;
                border-radius: 7px;
                border: 1px solid #455566;
            }
            QScrollBar::handle:vertical:hover {
                background: #3f5468;
            }
            QScrollBar::add-line:vertical, QScrollBar::sub-line:vertical {
                height: 0px;
                background: transparent;
            }
            QScrollBar::add-page:vertical, QScrollBar::sub-page:vertical {
                background: transparent;
            }
        """

    def build_tab_stylesheet(self, compact=False):
        tab_padding = "8px 12px" if compact else "10px 18px"
        tab_min_width = 92 if compact else 120
        return f"""
            QTabWidget::pane {{
                border: 1px solid #d0d7de;
                border-radius: 6px;
                background: white;
            }}
            QTabBar::tab {{
                padding: {tab_padding};
                min-width: {tab_min_width}px;
                font-weight: 600;
            }}
            QTabBar::tab:selected {{
                font-weight: bold;
            }}
            QGroupBox {{
                font-weight: bold;
                margin-top: 10px;
            }}
            QGroupBox::title {{
                subcontrol-origin: margin;
                left: 8px;
                padding: 0 4px;
            }}
        """

    def apply_compact_layout(self, compact):
        compact = bool(compact)
        if hasattr(self, "tabs"):
            self.tabs.setStyleSheet(self.build_tab_stylesheet(compact))
        if hasattr(self, "console"):
            self.console.setMaximumHeight(220 if compact else 300)
        if hasattr(self, "btn_verify"):
            self.btn_verify.setMinimumHeight(44 if compact else 50)
        if hasattr(self, "btn_start"):
            self.btn_start.setMinimumSize(156 if compact else 180, 46 if compact else 52)
        if hasattr(self, "btn_stop"):
            self.btn_stop.setMinimumSize(156 if compact else 180, 46 if compact else 52)

    def normalize_window_preset_name(self, preset_name):
        value = (preset_name or "").strip()
        if value == "Compact":
            return "Raspberry Pi 5"
        if value in self.window_size_presets:
            return value
        return "Standard"

    def apply_window_size_preset(self, preset_name):
        normalized_name = self.normalize_window_preset_name(preset_name)
        preset = self.window_size_presets.get(normalized_name, self.window_size_presets["Standard"])
        compact = bool(preset.get("compact", False))
        self.apply_compact_layout(compact)
        min_w, min_h = preset["minimum"]
        target_w, target_h = preset["target"]
        self.setMinimumSize(min_w, min_h)
        if not self.isMaximized():
            self.resize(target_w, target_h)

    def on_window_preset_changed(self, preset_name):
        if not preset_name:
            return
        self.apply_window_size_preset(preset_name)

    def _pluto_host_entry_looks_complete(self, value):
        value = (value or "").strip()
        if not value:
            return False
        lowered = value.lower()
        if lowered.startswith(("ip:", "usb:", "serial:")):
            return True
        if value.count(".") == 3:
            parts = value.split(".")
            if all(part.isdigit() and part != "" for part in parts):
                return True
        if any(ch.isalpha() for ch in value) and len(value) >= 3:
            return True
        return False

    def on_pluto_ip_text_edited(self, _text):
        self.save_persisted_settings()
        if self._pluto_host_entry_looks_complete(self.input_pluto_ip.text()):
            self.schedule_rf_detection()
        elif hasattr(self, "rf_detect_timer"):
            self.rf_detect_timer.stop()

    def on_pluto_ip_editing_finished(self):
        self.save_persisted_settings()
        if hasattr(self, "rf_detect_timer"):
            self.rf_detect_timer.stop()
        self.detect_rf_devices()

    def schedule_rf_detection(self):
        if hasattr(self, "rf_detect_timer"):
            self.rf_detect_timer.start()
        else:
            self.detect_rf_devices()

    def log_message(self, message, is_error=False):
        timestamp = time.strftime("%H:%M:%S")
        if is_error:
            self.console.append(f"<span style='color:#e74c3c'>[{timestamp}] ERROR: {message}</span>")
        else:
            self.console.append(f"<span style='color:#3498db'>[{timestamp}]</span> {message}")
        self.console.moveCursor(QTextCursor.MoveOperation.End)
        
    def update_stats_display(self):
        if self.process_worker:
            stats = self.process_worker.performance_stats
            frames = stats.get('frames', 0)
            self.frame_counter.setText(str(frames))
            
            ts_kbps = stats.get('ts_kbps', 0.0)
            if ts_kbps > 0:
                self.bitrate_label.setText(f"{ts_kbps:.0f} kbps")
            elif self.start_time and frames > self.last_frames:
                bitrate = self.update_bitrate_estimate()
                if bitrate > 0:
                    self.bitrate_label.setText(f"{bitrate:.0f} kbps")
                self.last_frames = frames
            
            fps = stats.get('fps', 0)
            if fps > 0:
                self.fps_label.setText(f"{fps:.1f} fps")
            
            buffer_level = stats.get('buffer_level', 0)
            percent = self.calculate_buffer_percent(buffer_level, stats.get('buffer_percent'))
            self.buffer_level.setText(f"{percent:.0f}%")
            self.buffer_bar.setValue(int(percent))
            
            underflows = stats.get('underflows', 0)
            self.underflow_label.setText(str(underflows))
            if underflows > 0:
                self.underflow_label.setStyleSheet("color: #e74c3c; font-weight: bold;")
            else:
                self.underflow_label.setStyleSheet("font-weight: bold;")
        
    def update_occupied_bandwidth_estimate(self):
        try:
            sr_ksps = float(self.combo_sr.currentText())
            rolloff = float(self.combo_rolloff.currentText())
            occupied_khz = sr_ksps * (1.0 + rolloff)
            self.bandwidth_estimate.setText(f"📡 Occupied RF Bandwidth: {occupied_khz:.1f} kHz")
        except Exception:
            self.bandwidth_estimate.setText("📡 Occupied RF Bandwidth: --- kHz")

    def update_bitrate_estimate(self):
        try:
            sr = int(float(self.combo_sr.currentText()) * 1000)
            fec_str = self.combo_fec.currentText()
            frame_short = "Short" in self.combo_frame.currentText()
            pilots = self.check_pilots.isChecked()
            bits_per_symbol = self.get_current_bits_per_symbol()
            self.update_occupied_bandwidth_estimate()

            kbch_map = {
                (True,  "1/4"): 3072,  (True,  "1/3"): 5232,  (True,  "2/5"): 6312,
                (True,  "1/2"): 7032,  (True,  "3/5"): 9552,  (True,  "2/3"): 10632,
                (True,  "3/4"): 11712, (True,  "4/5"): 12432, (True,  "5/6"): 13152,
                (True,  "8/9"): 14232,
                (False, "1/4"): 16008, (False, "1/3"): 21408, (False, "2/5"): 25728,
                (False, "1/2"): 32208, (False, "3/5"): 38688, (False, "2/3"): 43040,
                (False, "3/4"): 48408, (False, "4/5"): 51648, (False, "5/6"): 53840,
                (False, "8/9"): 57472, (False, "9/10"): 58192,
            }

            if frame_short and fec_str == "9/10":
                self.bitrate_estimate.setText("📊 Est. DVB-S2 TS Rate: unsupported")
                return 0

            kbch = kbch_map[(frame_short, fec_str)]
            nldpc = 16200 if frame_short else 64800
            slots = nldpc // (90 * bits_per_symbol)
            pilot_syms = ((slots - 1) // 16) * 36 if pilots else 0
            total_syms = 90 + (nldpc // bits_per_symbol) + pilot_syms
            net_br = int(sr * (kbch - 80) / total_syms / 1000)
            self.bitrate_estimate.setText(f"📊 Est. DVB-S2 TS Rate: {net_br} kbps")
            return net_br
        except Exception:
            self.bitrate_estimate.setText("📊 Est. DVB-S2 TS Rate: --- kbps")
            self.update_occupied_bandwidth_estimate()
            return 0
            
    def browse_file(self):
        path, _ = QFileDialog.getOpenFileName(self, "Select Video File", "", 
                                              "Video Files (*.mp4 *.ts *.mkv *.avi *.mov);;All Files (*.*)")
        if path:
            self.file_path.setText(path)
            
    def save_log(self):
        path, _ = QFileDialog.getSaveFileName(self, "Save Log", "dvbs2_tx_log.txt", "Text Files (*.txt)")
        if path:
            try:
                with open(path, 'w') as f:
                    f.write(self.console.toPlainText())
                self.log_message(f"Log saved to {path}")
            except Exception as e:
                self.log_message(f"Failed to save log: {e}", True)
            
    def check_dependencies(self):
        try:
            result = subprocess.run(["ffmpeg", "-version"], capture_output=True, timeout=2)
            if result.returncode != 0:
                self.log_message("⚠️ ffmpeg not found or not working!", True)
                return False
        except:
            self.log_message("⚠️ ffmpeg not found!", True)
            return False
            
        root_dir = os.path.dirname(os.path.abspath(__file__))
        possible_paths = [
            os.path.join(root_dir, "dvbs2_tx"),
            os.path.join(root_dir, "build", "dvbs2_tx"),
            os.path.join(root_dir, "src", "dvbs2_tx"),
            "/usr/local/bin/dvbs2_tx",
            os.path.expanduser("~/Documents/dvbs2_pi_tx/dvbs2_tx"),
        ]
        
        tx_path = None
        for path in possible_paths:
            if os.path.exists(path) and os.access(path, os.X_OK):
                tx_path = path
                break
                
        if not tx_path:
            self.log_message("⚠️ dvbs2_tx binary not found! Please compile first.", True)
            self.log_message("Run: make  or  cmake -S . -B build && cmake --build build", True)
            return False

        if self.selected_requires_f5oeo() and mqtt_publish is None:
            self.log_message("⚠️ F5OEO firmware control requires paho-mqtt. Run install.sh or install requirements.txt.", True)
            return False
            
        self.log_message(f"✅ Found dvbs2_tx at: {tx_path}")
        return True
        

    def locate_support_file(self, filename):
        root_dir = os.path.dirname(os.path.abspath(__file__))
        candidates = [
            os.path.join(root_dir, filename),
            os.path.normpath(os.path.join(root_dir, "..", "share", "dvbs2_tx", filename)),
            os.path.join("/usr/local/share/dvbs2_tx", filename),
            os.path.join("/usr/share/dvbs2_tx", filename),
        ]
        for candidate in candidates:
            if os.path.exists(candidate):
                return candidate
        return candidates[0]

    def get_test_pattern_path(self):
        return self.locate_support_file("test_pattern_datv.mp4")

    def run_optional_capture(self, cmd, timeout=5):
        try:
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout, check=False)
        except Exception:
            return ""
        if result.returncode != 0:
            return ""
        return (result.stdout or "").strip()

    def probe_v4l2_device(self, devnode):
        info = {
            "node": devnode,
            "name": os.path.basename(devnode),
            "capture": False,
            "formats": [],
            "bus_info": "",
        }
        if shutil.which("v4l2-ctl") is None:
            return info
        dump_all = self.run_optional_capture(["v4l2-ctl", "-d", devnode, "--all"], timeout=4)
        if dump_all:
            lowered = dump_all.lower()
            info["capture"] = any(token in lowered for token in ("video capture", "video capture multiplanar"))
            bus_match = re.search(r"^\s*bus info\s*:\s*(.+)$", dump_all, re.IGNORECASE | re.MULTILINE)
            if bus_match:
                info["bus_info"] = bus_match.group(1).strip()
        dump_formats = self.run_optional_capture(["v4l2-ctl", "-d", devnode, "--list-formats-ext"], timeout=4)
        if dump_formats:
            seen = []
            for fmt in re.findall(r"\[\d+\]:\s*'([^']+)'", dump_formats):
                if fmt not in seen:
                    seen.append(fmt)
            info["formats"] = seen
            if seen:
                info["capture"] = True
        return info

    def list_camera_sources(self):
        devices = []
        seen_nodes = set()

        def append_device(name, devnode):
            if not devnode or devnode in seen_nodes or not os.path.exists(devnode):
                return
            seen_nodes.add(devnode)
            probe = self.probe_v4l2_device(devnode)
            if shutil.which("v4l2-ctl") is not None and not probe.get("capture"):
                return
            clean_name = (name or probe.get("name") or os.path.basename(devnode)).strip()
            formats = probe.get("formats") or []
            display = f"{clean_name} ({devnode})"
            if formats:
                display += f" — {', '.join(formats[:3])}"
            devices.append({
                "node": devnode,
                "name": clean_name,
                "display": display,
                "formats": formats,
                "bus_info": probe.get("bus_info", ""),
            })

        if shutil.which("v4l2-ctl") is not None:
            listing = self.run_optional_capture(["v4l2-ctl", "--list-devices"], timeout=6)
            if listing:
                current_name = ""
                for raw_line in listing.splitlines():
                    line = raw_line.rstrip()
                    if not line:
                        continue
                    if not raw_line[:1].isspace():
                        current_name = line.rstrip(":")
                        continue
                    node = line.strip()
                    if node.startswith("/dev/video"):
                        append_device(current_name, node)

        if not devices:
            base_dir = "/sys/class/video4linux"
            ignore_tokens = ("codec", "encoder", "decoder", "m2m", "mem2mem", "isp", "rpivid", "bcm2835", "vim2m")
            if os.path.isdir(base_dir):
                for entry in sorted(os.listdir(base_dir)):
                    if not entry.startswith("video"):
                        continue
                    devnode = f"/dev/{entry}"
                    if not os.path.exists(devnode):
                        continue
                    try:
                        with open(os.path.join(base_dir, entry, "name"), "r", encoding="utf-8", errors="ignore") as handle:
                            name = handle.read().strip() or entry
                    except OSError:
                        name = entry
                    lowered = name.lower()
                    if any(token in lowered for token in ignore_tokens):
                        continue
                    append_device(name, devnode)
        return devices

    def list_audio_sources(self):
        sources = []
        seen_ids = set()

        def add_source(label, args, source_id, backend):
            if not source_id or source_id in seen_ids:
                return
            seen_ids.add(source_id)
            sources.append({"label": label, "args": list(args), "id": source_id, "backend": backend})

        if shutil.which("pactl") is not None:
            short_sources = self.run_optional_capture(["pactl", "list", "short", "sources"], timeout=5)
            pulse_rows = []
            monitor_rows = []
            for line in short_sources.splitlines():
                parts = [item for item in line.split('\t') if item]
                if len(parts) < 2:
                    continue
                source_name = parts[1].strip()
                if not source_name:
                    continue
                label = f"PulseAudio: {source_name}"
                target = monitor_rows if source_name.endswith('.monitor') else pulse_rows
                target.append((label, ["-f", "pulse", "-i", source_name], f"pulse:{source_name}"))
            for label, args, source_id in pulse_rows + ([] if pulse_rows else monitor_rows):
                add_source(label, args, source_id, "pulse")
            if pulse_rows:
                add_source("PulseAudio: default", ["-f", "pulse", "-i", "default"], "pulse:default", "pulse")

        if shutil.which("arecord") is not None:
            arecord_list = self.run_optional_capture(["arecord", "-l"], timeout=5)
            for line in arecord_list.splitlines():
                match = re.search(r"card\s+(\d+):\s*([^,]+),\s*device\s+(\d+):\s*([^\[]+)", line)
                if not match:
                    continue
                card_num, card_name, dev_num, dev_name = match.groups()
                hw_name = f"hw:{card_num},{dev_num}"
                label = f"ALSA: {hw_name} — {card_name.strip()} / {dev_name.strip()}"
                add_source(label, ["-f", "alsa", "-i", hw_name], f"alsa:{hw_name}", "alsa")
            add_source("ALSA: default", ["-f", "alsa", "-i", "default"], "alsa:default", "alsa")
        return sources

    def refresh_audio_sources(self):
        current_source_id = self.get_selected_audio_source_id()
        sources = self.list_audio_sources()
        self.audio_sources = sources
        if hasattr(self, "camera_audio_source_combo"):
            self.camera_audio_source_combo.blockSignals(True)
            self.camera_audio_source_combo.clear()
            for item in sources:
                self.camera_audio_source_combo.addItem(item["label"], item)
            if not sources:
                self.camera_audio_source_combo.addItem("No microphone source detected", {})
            restore_index = -1
            for idx in range(self.camera_audio_source_combo.count()):
                data = self.camera_audio_source_combo.itemData(idx) or {}
                if isinstance(data, dict) and data.get("id") == current_source_id:
                    restore_index = idx
                    break
            self.camera_audio_source_combo.setCurrentIndex(restore_index if restore_index >= 0 else 0)
            self.camera_audio_source_combo.blockSignals(False)
        if hasattr(self, "camera_audio_hint"):
            if sources:
                self.camera_audio_hint.setText(f"{len(sources)} microphone source(s) detected. Enable the route below and keep the main Audio checkbox enabled to send webcam audio.")
            else:
                self.camera_audio_hint.setText("No microphone source detected automatically. You can still enter a custom FFmpeg microphone input string below.")
        self.update_audio_controls()

    def get_selected_audio_source_id(self):
        if not hasattr(self, "camera_audio_source_combo"):
            return ""
        data = self.camera_audio_source_combo.currentData() or {}
        return data.get("id", "") if isinstance(data, dict) else ""

    def get_selected_audio_source(self):
        if not hasattr(self, "camera_audio_source_combo"):
            return {}
        data = self.camera_audio_source_combo.currentData() or {}
        return data if isinstance(data, dict) else {}

    def refresh_camera_sources(self):
        current_camera = self.get_selected_camera_device()
        devices = self.list_camera_sources()
        self.camera_sources = devices
        if hasattr(self, "camera_device_combo"):
            self.camera_device_combo.blockSignals(True)
            self.camera_device_combo.clear()
            for item in devices:
                self.camera_device_combo.addItem(item["display"], item["node"])
            if not devices:
                self.camera_device_combo.addItem("No camera detected (custom FFmpeg input still available)", "")
            restore_index = self.camera_device_combo.findData(current_camera)
            self.camera_device_combo.setCurrentIndex(restore_index if restore_index >= 0 else 0)
            self.camera_device_combo.blockSignals(False)
        if hasattr(self, "camera_hint"):
            if devices:
                self.camera_hint.setText(f"{len(devices)} webcam/camera source(s) detected. Select 'Webcam / Camera' to use live capture or enter a custom FFmpeg input override below.")
            else:
                self.camera_hint.setText("No V4L2 webcam detected automatically. Connect a webcam and press Refresh Cameras, or use a custom FFmpeg webcam input string.")
        self.update_camera_probe_details()
        self.update_source_controls()

    def get_selected_camera_device(self):
        if not hasattr(self, "camera_device_combo"):
            return ""
        return self.camera_device_combo.currentData() or ""

    def get_selected_camera_metadata(self):
        selected = self.get_selected_camera_device()
        for item in getattr(self, "camera_sources", []):
            if item.get("node") == selected:
                return item
        return {}

    def update_camera_probe_details(self):
        if not hasattr(self, "camera_probe_details"):
            return
        info = self.get_selected_camera_metadata()
        custom_video = self.camera_custom_video_input.text().strip() if hasattr(self, "camera_custom_video_input") else ""
        if custom_video:
            self.camera_probe_details.setText("Using a custom FFmpeg webcam input override. Automatic V4L2 probing is bypassed for the video path.")
        elif info:
            fmt_text = ", ".join(info.get("formats", [])[:4]) if info.get("formats") else "formats not reported"
            bus_text = info.get("bus_info") or "bus info unavailable"
            self.camera_probe_details.setText(f"Selected camera: {info.get('name', info.get('node', 'n/a'))} | node={info.get('node', 'n/a')} | formats={fmt_text} | {bus_text}")
        else:
            self.camera_probe_details.setText("No camera selected yet. Pick a detected V4L2 device or enter a custom FFmpeg webcam input override.")

    def get_pluto_host(self):
        if hasattr(self, "input_pluto_ip"):
            return self.input_pluto_ip.text().strip() or "192.168.2.1"
        return "192.168.2.1"

    def get_pluto_ssh_user(self):
        return (os.environ.get("DATV_PLUTO_SSH_USER", "root") or "root").strip() or "root"

    def get_entered_pluto_ssh_password(self):
        return (os.environ.get("DATV_PLUTO_SSH_PASSWORD", "") or "").strip()

    def format_temperature(self, value_c):
        if value_c is None:
            return "—"
        return f"{value_c:.1f} °C"

    def set_pluto_info_values(self, *, status=None, status_color=None, hw_model=None, fw_version=None, rf_ic=None, rf_temp=None, soc_temp=None):
        if status is not None and hasattr(self, "label_pluto_info_status"):
            self.label_pluto_info_status.setText(status)
            if status_color:
                self.label_pluto_info_status.setStyleSheet(f"color: {status_color};")
        if hw_model is not None and hasattr(self, "value_pluto_hw_model"):
            self.value_pluto_hw_model.setText(hw_model or "—")
        if fw_version is not None and hasattr(self, "value_pluto_fw_version"):
            self.value_pluto_fw_version.setText(fw_version or "—")
        if rf_ic is not None and hasattr(self, "value_pluto_rf_ic"):
            self.value_pluto_rf_ic.setText(rf_ic or "—")
        if rf_temp is not None and hasattr(self, "value_pluto_rf_temp"):
            self.value_pluto_rf_temp.setText(self.format_temperature(rf_temp) if isinstance(rf_temp, (int, float)) else (rf_temp or "—"))
        if soc_temp is not None and hasattr(self, "value_pluto_soc_temp"):
            self.value_pluto_soc_temp.setText(self.format_temperature(soc_temp) if isinstance(soc_temp, (int, float)) else (soc_temp or "—"))

    def extract_last_number(self, text):
        if not text:
            return None
        matches = re.findall(r"[-+]?\d+(?:\.\d+)?", str(text))
        if not matches:
            return None
        try:
            return float(matches[-1])
        except ValueError:
            return None

    def run_command_capture(self, cmd, timeout=5):
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout, check=False)
        stdout = (result.stdout or "").strip()
        stderr = (result.stderr or "").strip()
        if result.returncode != 0:
            detail = stderr or stdout or f"command failed with code {result.returncode}"
            raise RuntimeError(detail)
        return stdout

    def run_iio_attr_read(self, mode, *args, timeout=5):
        uri = self.get_pluto_uri()
        cmd = ["iio_attr", "-q", "-u", uri, mode, *[str(item) for item in args]]
        return self.run_command_capture(cmd, timeout=timeout)

    def run_iio_attr_write(self, mode, *args, timeout=5):
        uri = self.get_pluto_uri()
        cmd = ["iio_attr", "-q", "-u", uri, mode, *[str(item) for item in args]]
        return self.run_command_capture(cmd, timeout=timeout)

    def run_iio_info_dump(self, timeout=6):
        uri = self.get_pluto_uri()
        return self.run_command_capture(["iio_info", "-u", uri], timeout=timeout)

    def read_context_attr(self, attr_name):
        try:
            value = self.run_iio_attr_read("-C", attr_name, timeout=4)
            return (value or "").strip()
        except Exception:
            return ""

    def read_channel_attr_value(self, device_name, channel_name, attr_name):
        value = self.run_iio_attr_read("-c", device_name, channel_name, attr_name, timeout=4)
        return self.extract_last_number(value)

    def read_pluto_rf_temperature(self):
        try:
            raw = self.read_channel_attr_value("ad9361-phy", "temp0", "input")
            if raw is None:
                return None
            if abs(raw) > 200.0:
                raw /= 1000.0
            return raw
        except Exception:
            return None

    def read_pluto_soc_temperature(self):
        try:
            direct = self.read_channel_attr_value("xadc", "temp0", "input")
            if direct is not None:
                if abs(direct) > 200.0:
                    direct /= 1000.0
                return direct
        except Exception:
            pass
        try:
            raw = self.read_channel_attr_value("xadc", "temp0", "raw")
            offset = self.read_channel_attr_value("xadc", "temp0", "offset")
            scale = self.read_channel_attr_value("xadc", "temp0", "scale")
            if raw is None or offset is None or scale is None:
                return None
            value = (raw + offset) * scale / 1000.0
            return value
        except Exception:
            return None

    def parse_rf_ic_from_hw_model(self, hw_model):
        if not hw_model:
            return ""
        match = re.search(r"(AD936[1-4])", hw_model, re.IGNORECASE)
        if match:
            return match.group(1).upper()
        match = re.search(r"(Z7010-AD936[1-4])", hw_model, re.IGNORECASE)
        if match:
            return match.group(1).upper()
        return ""

    def collect_pluto_device_info(self):
        if shutil.which("iio_attr") is None:
            raise RuntimeError("iio_attr is not installed. Install libiio-utils to read the Pluto model.")
        info = {
            "host": self.get_pluto_host(),
            "uri": self.get_pluto_uri(),
            "hw_model": "",
        }
        info["hw_model"] = self.read_context_attr("hw_model")
        if not info["hw_model"] and shutil.which("iio_info") is not None:
            try:
                dump = self.run_iio_info_dump(timeout=6)
                for attr_name in ("hw_model", "model"):
                    match = re.search(rf"^\s*{attr_name}:\s*(.+)$", dump, re.MULTILINE)
                    if match:
                        info["hw_model"] = match.group(1).strip()
                        break
            except Exception:
                pass
        return info

    def refresh_pluto_device_info(self):
        try:
            info = self.collect_pluto_device_info()
            model = info["hw_model"] or "—"
            self.set_pluto_info_values(
                status=f"Pluto info OK on {info['host']}",
                status_color="#155724",
                hw_model=model,
            )
            self.log_message(f"Pluto model: {model}")
        except Exception as exc:
            self.set_pluto_info_values(status=f"Pluto info failed: {exc}", status_color="#c0392b")
            self.log_message(f"Pluto info failed: {exc}", True)

    def build_reboot_probe_command(self):
        return (
            "sh -lc '(command -v reboot >/dev/null 2>&1 && (nohup reboot >/dev/null 2>&1 &) && echo reboot) "
            "|| (command -v device_reboot >/dev/null 2>&1 && (nohup device_reboot reset >/dev/null 2>&1 &) && echo device_reboot) "
            "|| (command -v restart >/dev/null 2>&1 && (nohup restart >/dev/null 2>&1 &) && echo restart)'"
        )

    def try_reboot_pluto_via_paramiko(self, host, username, password: Optional[str]):
        if paramiko is None:
            raise RuntimeError("paramiko is not installed")
        client = paramiko.SSHClient()
        client.load_system_host_keys()
        client.set_missing_host_key_policy(paramiko.RejectPolicy())
        try:
            client.connect(
                hostname=host,
                port=PLUTO_DEFAULT_SSH_PORT,
                username=username,
                password=password,
                timeout=5,
                banner_timeout=5,
                auth_timeout=5,
                look_for_keys=password is None,
                allow_agent=password is None,
            )
            stdin, stdout, stderr = client.exec_command(self.build_reboot_probe_command(), timeout=6)
            token = (stdout.read().decode("utf-8", errors="ignore") + " " + stderr.read().decode("utf-8", errors="ignore")).strip()
            exit_status = stdout.channel.recv_exit_status()
            if exit_status == 0 and any(key in token for key in ("reboot", "device_reboot", "restart")):
                return "SSH/paramiko"
            raise RuntimeError(token or f"remote reboot helper exited with code {exit_status}")
        except Exception as exc:
            message = str(exc) or exc.__class__.__name__
            if "not found in known_hosts" in message.lower() or "server" in message.lower() and "not found" in message.lower():
                raise RuntimeError("Pluto SSH host key is not trusted yet. Add it to known_hosts first, then retry the reboot.")
            raise
        finally:
            try:
                client.close()
            except Exception:
                pass

    def try_reboot_pluto_via_ssh(self, host, username, password: Optional[str]):
        if password is not None:
            raise RuntimeError("Password-based SSH fallback is disabled for safety. Use paramiko with a trusted host key or key-based SSH.")
        if shutil.which("ssh") is None:
            raise RuntimeError("ssh client is not installed")
        cmd = [
            "ssh",
            "-o", "BatchMode=yes",
            "-o", "StrictHostKeyChecking=yes",
            "-o", "ConnectTimeout=5",
            f"{username}@{host}",
            self.build_reboot_probe_command(),
        ]
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=12, check=False)
        detail = ((result.stdout or "") + " " + (result.stderr or "")).strip()
        if result.returncode == 0 and any(key in detail for key in ("reboot", "device_reboot", "restart")):
            return "SSH subprocess"
        lowered = detail.lower()
        if "host key verification failed" in lowered or "remote host identification has changed" in lowered or "no matching host key type found" in lowered:
            raise RuntimeError("Pluto SSH host key is not trusted yet. Add it to known_hosts first, then retry the reboot.")
        raise RuntimeError(detail or f"ssh reboot helper exited with code {result.returncode}")

    def request_pluto_reboot(self, host, username):
        entered_password = self.get_entered_pluto_ssh_password()
        attempts = [entered_password] if entered_password else [None]
        errors = []
        for password in attempts:
            try:
                return self.try_reboot_pluto_via_paramiko(host, username, password)
            except Exception as exc:
                errors.append(str(exc))
            try:
                return self.try_reboot_pluto_via_ssh(host, username, password)
            except Exception as exc:
                errors.append(str(exc))
        raise RuntimeError("; ".join(item for item in errors if item) or "no reboot method succeeded")

    def set_pluto_tx_attenuation(self, attenuation_db):
        if shutil.which("iio_attr") is None:
            raise RuntimeError("iio_attr is not installed. Install libiio-utils to apply the Pluto mute policy.")
        target = -abs(float(attenuation_db))
        success = False
        last_error = None
        for channel_name in ("voltage0", "voltage1"):
            try:
                self.run_iio_attr_write("-c", "ad9361-phy", channel_name, "hardwaregain", f"{target:.2f}", timeout=4)
                success = True
            except Exception as exc:
                last_error = exc
        if not success:
            raise RuntimeError(str(last_error) if last_error else "unable to set Pluto TX attenuation")
        return f"{abs(target):.2f} dB"

    def apply_pluto_stop_exit_policy(self, trigger="stop"):
        if self._applying_stop_policy or self.selected_backend_device() != "pluto":
            return False
        policy = self.get_selected_pluto_stop_policy()
        action_name = "exit" if trigger == "exit" else "stop"
        self._applying_stop_policy = True
        try:
            if policy == "pass_only":
                if self.selected_requires_plutodvb2() and mqtt_publish is not None and self.get_f5oeo_topic_key():
                    publish_result = self.publish_f5oeo_mqtt("tx/stream/mode", "pass")
                    self.log_message(f"Pluto {action_name} policy: pass-only via {publish_result['topic']} = {publish_result['payload']}")
                elif self.selected_requires_plutodvb2():
                    self.log_message(f"Pluto {action_name} policy: pass-only selected, but MQTT pass-mode signaling is unavailable or the callsign/topic key is blank. Only the local TX stream was stopped.", True)
                elif self.selected_requires_pluto_0303():
                    self.log_message(f"Pluto {action_name} policy: pass-only selected. 0303/2402 has no separate pass-mode command, so TX is simply stopped.")
                else:
                    self.log_message(f"Pluto {action_name} policy: pass-only selected. No additional stock-Pluto action is required after stopping TX.")
                return True
            if policy == "mute":
                applied = self.set_pluto_tx_attenuation(89.75)
                self.log_message(f"Pluto {action_name} policy: mute applied via libiio (TX attenuation {applied}).")
                if self.selected_requires_plutodvb2() and mqtt_publish is not None and self.get_f5oeo_topic_key():
                    try:
                        publish_result = self.publish_f5oeo_mqtt("tx/stream/mode", "pass")
                        self.log_message(f"Pluto {action_name} policy: pass reassert after mute via {publish_result['topic']} = {publish_result['payload']}")
                    except Exception as exc:
                        self.log_message(f"Pluto {action_name} policy note: mute succeeded but pass reassert failed: {exc}", True)
                return True
            if policy == "reboot":
                host = self.get_pluto_host()
                username = self.get_pluto_ssh_user()
                method = self.request_pluto_reboot(host, username)
                self.log_message(f"Pluto {action_name} policy: reboot requested on {host} via {method}")
                self.set_pluto_info_values(status=f"Pluto reboot requested on {host} via {method}.", status_color="#155724")
                return True
            return False
        finally:
            self._applying_stop_policy = False

    def reboot_pluto_device(self):
        host = self.get_pluto_host()
        username = self.get_pluto_ssh_user()
        answer = QMessageBox.question(
            self,
            "Reboot Pluto",
            f"Request a reboot for Pluto at {host}?\n\nThis uses SSH. Save your on-air work first.",
            QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No,
            QMessageBox.StandardButton.No,
        )
        if answer != QMessageBox.StandardButton.Yes:
            return
        try:
            method = self.request_pluto_reboot(host, username)
            self.set_pluto_info_values(
                status=f"Pluto reboot requested on {host} via {method}. Wait a few seconds, then press Refresh Pluto Info.",
                status_color="#856404",
            )
            self.log_message(f"Pluto reboot requested on {host} via {method}")
        except Exception as exc:
            self.set_pluto_info_values(status=f"Pluto reboot failed: {exc}", status_color="#c0392b")
            self.log_message(f"Pluto reboot failed: {exc}", True)
            QMessageBox.warning(self, "Reboot Pluto", f"Reboot request failed.\n\n{exc}")

    def ensure_test_pattern_file(self, force_regenerate=False):
        target = self.get_test_pattern_path()
        if os.path.exists(target) and not force_regenerate:
            return target

        try:
            cmd = [
                "ffmpeg", "-hide_banner", "-loglevel", "error", "-y",
                "-f", "lavfi", "-i", "testsrc2=size=640x360:rate=25",
                "-f", "lavfi", "-i", "sine=frequency=1000:sample_rate=48000",
                "-t", "12",
                "-c:v", "libx264",
                "-preset", "veryfast",
                "-pix_fmt", "yuv420p",
                "-g", "25",
                "-c:a", "aac",
                "-b:a", "32k",
                "-ar", "48000",
                "-ac", "2",
                "-shortest",
                target,
            ]
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=45)
            if result.returncode != 0:
                detail = (result.stderr or result.stdout or "Unknown FFmpeg error").strip()
                raise RuntimeError(detail)
            return target
        except Exception as exc:
            self.log_message(f"⚠️ Could not create DATV test pattern file: {exc}", True)
            return ""

    def use_test_pattern_file(self):
        path = self.ensure_test_pattern_file()
        if not path:
            QMessageBox.warning(self, "Test Pattern", "Unable to create the DATV test pattern file. Check that FFmpeg and libx264 are installed.")
            return
        self.source_combo.setCurrentText("Video File")
        self.file_path.setText(path)
        self.log_message(f"ℹ️ Using DATV test pattern file: {path}")

    def sanitize_service_value(self, value, default="OD5TB"):
        cleaned = re.sub(r"[\r\n]+", " ", str(value or "")).strip()
        return cleaned or default

    def get_service_metadata(self):
        service_name = self.sanitize_service_value(self.service_name_input.text() if hasattr(self, "service_name_input") else "OD5TB", "OD5TB")
        service_provider = self.sanitize_service_value(self.service_provider_input.text() if hasattr(self, "service_provider_input") else service_name, service_name)
        return service_name, service_provider

    def has_v4l2m2m_device(self):
        return any(os.path.exists(path) for path in ["/dev/video10", "/dev/video11", "/dev/video12"])

    def detect_vaapi_device(self):
        for path in ["/dev/dri/renderD128", "/dev/dri/renderD129", "/dev/dri/renderD130"]:
            if os.path.exists(path):
                return path
        return ""

    def detect_runtime_environment(self, refresh=False):
        if self.runtime_env_cache is not None and not refresh:
            return dict(self.runtime_env_cache)

        model = ""
        for candidate in ["/proc/device-tree/model", "/sys/firmware/devicetree/base/model"]:
            try:
                if os.path.exists(candidate):
                    with open(candidate, "rb") as handle:
                        model = handle.read().decode("utf-8", errors="ignore").replace("\x00", "").strip()
                        if model:
                            break
            except OSError:
                pass

        is_pi = "raspberry pi" in model.lower()
        is_pi5 = "raspberry pi 5" in model.lower()
        vaapi_device = self.detect_vaapi_device()
        nvidia_present = any(os.path.exists(path) for path in ["/dev/nvidia0", "/dev/nvidiactl"]) or shutil.which("nvidia-smi") is not None
        env = {
            "model": model,
            "is_pi": is_pi,
            "is_pi5": is_pi5,
            "has_v4l2": self.has_v4l2m2m_device(),
            "vaapi_device": vaapi_device,
            "has_vaapi": bool(vaapi_device),
            "has_nvidia": nvidia_present,
        }
        if is_pi5:
            env["label"] = "Raspberry Pi 5"
        elif is_pi:
            env["label"] = model or "Raspberry Pi"
        else:
            env["label"] = "Linux PC"
        self.runtime_env_cache = dict(env)
        return env

    def get_available_ffmpeg_encoders(self, refresh=False):
        if self.ffmpeg_encoder_cache is not None and not refresh:
            return set(self.ffmpeg_encoder_cache)
        encoders = set()
        try:
            result = subprocess.run(["ffmpeg", "-hide_banner", "-encoders"], capture_output=True, text=True, timeout=5)
            combined = (result.stdout or "") + "\n" + (result.stderr or "")
            for line in combined.splitlines():
                match = re.match(r"^\s*[A-Z\.]{6}\s+([^\s]+)", line)
                if match:
                    encoders.add(match.group(1).strip())
        except Exception:
            encoders = set()
        self.ffmpeg_encoder_cache = sorted(encoders)
        return encoders

    def get_known_video_encoders(self):
        return [
            ("Auto", "Auto"),
            ("libx264 (CPU / H.264)", "libx264"),
            ("libx265 (CPU / HEVC)", "libx265"),
            ("h264_v4l2m2m (HW / V4L2 H.264)", "h264_v4l2m2m"),
            ("hevc_v4l2m2m (HW / V4L2 HEVC)", "hevc_v4l2m2m"),
            ("h264_nvenc (GPU / NVIDIA H.264)", "h264_nvenc"),
            ("hevc_nvenc (GPU / NVIDIA HEVC)", "hevc_nvenc"),
            ("h264_qsv (GPU / Intel QSV H.264)", "h264_qsv"),
            ("hevc_qsv (GPU / Intel QSV HEVC)", "hevc_qsv"),
            ("h264_amf (GPU / AMD AMF H.264)", "h264_amf"),
            ("hevc_amf (GPU / AMD AMF HEVC)", "hevc_amf"),
            ("h264_vaapi (GPU / VAAPI H.264)", "h264_vaapi"),
            ("hevc_vaapi (GPU / VAAPI HEVC)", "hevc_vaapi"),
        ]

    def probe_ffmpeg_encoder(self, encoder, refresh=False):
        if not refresh and encoder in self.encoder_probe_cache:
            return dict(self.encoder_probe_cache[encoder])

        available = self.get_available_ffmpeg_encoders(refresh=refresh)
        env = self.detect_runtime_environment(refresh=refresh)
        if encoder not in available:
            result = {"usable": False, "reason": "Not built into this FFmpeg"}
            self.encoder_probe_cache[encoder] = dict(result)
            return result

        if encoder.endswith("_v4l2m2m") and not env["has_v4l2"]:
            result = {"usable": False, "reason": "No V4L2 mem2mem device found (/dev/video10-12)"}
            self.encoder_probe_cache[encoder] = dict(result)
            return result
        if encoder.endswith("_vaapi") and not env["has_vaapi"]:
            result = {"usable": False, "reason": "No VAAPI render node detected"}
            self.encoder_probe_cache[encoder] = dict(result)
            return result
        if encoder.endswith("_nvenc") and not env["has_nvidia"]:
            result = {"usable": False, "reason": "No NVIDIA runtime or GPU detected"}
            self.encoder_probe_cache[encoder] = dict(result)
            return result

        cmd = ["ffmpeg", "-hide_banner", "-loglevel", "error"]
        if encoder.endswith("_vaapi"):
            cmd.extend(["-vaapi_device", env["vaapi_device"]])
        cmd.extend([
            "-f", "lavfi", "-i", "testsrc2=size=320x240:rate=10",
            "-frames:v", "1",
        ])
        if encoder.endswith("_vaapi"):
            cmd.extend(["-vf", "format=nv12,hwupload"])
        elif encoder.endswith("_qsv") or encoder.endswith("_amf"):
            cmd.extend(["-pix_fmt", "nv12"])
        else:
            cmd.extend(["-pix_fmt", "yuv420p"])
        cmd.extend(["-c:v", encoder])
        if encoder.endswith("_nvenc"):
            cmd.extend(["-rc", "cbr", "-b:v", "400k", "-maxrate", "400k", "-bufsize", "800k"])
        cmd.extend(["-f", "null", "-"])

        try:
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=8)
            if result.returncode == 0:
                detail = "Runtime probe passed"
                if encoder.endswith("_vaapi") and env["vaapi_device"]:
                    detail = f"Runtime probe passed ({os.path.basename(env['vaapi_device'])})"
                probe = {"usable": True, "reason": detail}
            else:
                stderr = (result.stderr or result.stdout or "Runtime probe failed").strip().splitlines()
                probe = {"usable": False, "reason": stderr[-1] if stderr else "Runtime probe failed"}
        except Exception as exc:
            probe = {"usable": False, "reason": f"Runtime probe failed: {exc}"}

        self.encoder_probe_cache[encoder] = dict(probe)
        return probe

    def get_encoder_runtime_status(self, refresh=False):
        if self.encoder_runtime_cache is not None and not refresh:
            return dict(self.encoder_runtime_cache)

        if refresh:
            self.encoder_probe_cache = {}

        available = self.get_available_ffmpeg_encoders(refresh=refresh)
        env = self.detect_runtime_environment(refresh=refresh)
        status = {}
        usable_count = 0

        for label, encoder in self.get_known_video_encoders():
            if encoder == "Auto":
                continue
            present = encoder in available
            hardware = encoder not in {"libx264", "libx265"}
            if not hardware:
                usable = present
                reason = "Software encoder ready" if present else "Not built into this FFmpeg"
            else:
                probe = self.probe_ffmpeg_encoder(encoder, refresh=refresh)
                usable = probe["usable"]
                reason = probe["reason"]

            if usable:
                usable_count += 1
            status[encoder] = {
                "label": label,
                "present": present,
                "usable": usable,
                "reason": reason,
                "hardware": hardware,
                "color": "#1e8449" if usable else "#c0392b",
            }

        status["Auto"] = {
            "label": "Auto",
            "present": True,
            "usable": usable_count > 0,
            "reason": f"Automatically picks the first runtime-tested usable encoder on {env['label']}",
            "hardware": False,
            "color": "#1e8449" if usable_count > 0 else "#c0392b",
        }
        self.encoder_runtime_cache = dict(status)
        return status

    def get_supported_video_encoder_options(self):
        return self.get_known_video_encoders()

    def refresh_encoder_option_styles(self):
        statuses = self.encoder_runtime_cache if self.encoder_runtime_cache is not None else {}
        for index in range(self.combo_vcodec.count()):
            encoder = self.combo_vcodec.itemData(index) or self.combo_vcodec.itemText(index)
            meta = statuses.get(encoder, statuses.get("Auto")) if statuses else None
            if meta:
                self.combo_vcodec.setItemData(index, QColor(meta["color"]), Qt.ItemDataRole.ForegroundRole)
                self.combo_vcodec.setItemData(index, meta["reason"], Qt.ItemDataRole.ToolTipRole)
            else:
                self.combo_vcodec.setItemData(index, QColor("#dfe6e9"), Qt.ItemDataRole.ForegroundRole)
                self.combo_vcodec.setItemData(index, "Encoder capability probe is running in the background.", Qt.ItemDataRole.ToolTipRole)

    def populate_video_encoder_options(self, skip_runtime_probe=False):
        current = self.combo_vcodec.currentData() or self.combo_vcodec.currentText() or "Auto"
        self.combo_vcodec.blockSignals(True)
        self.combo_vcodec.clear()
        for label, encoder in self.get_supported_video_encoder_options():
            self.combo_vcodec.addItem(label, encoder)
        if not skip_runtime_probe or self.encoder_runtime_cache is not None:
            self.refresh_encoder_option_styles()
        else:
            for index in range(self.combo_vcodec.count()):
                self.combo_vcodec.setItemData(index, QColor("#dfe6e9"), Qt.ItemDataRole.ForegroundRole)
                self.combo_vcodec.setItemData(index, "Encoder capability probe is running in the background.", Qt.ItemDataRole.ToolTipRole)
        index = self.combo_vcodec.findData(current)
        if index < 0:
            index = self.combo_vcodec.findData("Auto")
        if index >= 0:
            self.combo_vcodec.setCurrentIndex(index)
        self.combo_vcodec.blockSignals(False)

    def get_selected_video_encoder_choice(self):
        return self.combo_vcodec.currentData() or self.combo_vcodec.currentText() or "Auto"

    def get_auto_encoder_order(self):
        env = self.detect_runtime_environment()
        if env["is_pi5"]:
            return ["h264_v4l2m2m", "hevc_v4l2m2m", "h264_vaapi", "hevc_vaapi", "h264_qsv", "hevc_qsv", "h264_amf", "hevc_amf", "libx264", "libx265", "h264_nvenc", "hevc_nvenc"]
        if env["is_pi"]:
            return ["h264_v4l2m2m", "hevc_v4l2m2m", "h264_vaapi", "hevc_vaapi", "h264_qsv", "hevc_qsv", "h264_amf", "hevc_amf", "libx264", "libx265", "h264_nvenc", "hevc_nvenc"]
        return ["h264_nvenc", "hevc_nvenc", "h264_qsv", "hevc_qsv", "h264_amf", "hevc_amf", "h264_vaapi", "hevc_vaapi", "h264_v4l2m2m", "hevc_v4l2m2m", "libx264", "libx265"]

    def build_pending_encoder_info(self, requested=None):
        requested = requested or self.get_selected_video_encoder_choice()
        encoder = "libx264" if requested == "Auto" else requested
        hardware = encoder not in {"libx264", "libx265"}
        reason = "Encoder capability probe is still running in the background."
        return {
            "requested": requested,
            "encoder": encoder,
            "hardware": hardware,
            "auto": requested == "Auto",
            "usable": not hardware,
            "fallback": False,
            "reason": reason,
            "pending": True,
        }

    def resolve_video_encoder(self, requested=None, allow_fallback=True, allow_probe=True):
        requested = requested or self.get_selected_video_encoder_choice()
        if (self.encoder_runtime_cache is None or self.encoder_probe_pending) and not allow_probe:
            return self.build_pending_encoder_info(requested)
        statuses = self.get_encoder_runtime_status()

        if requested == "Auto":
            for encoder in self.get_auto_encoder_order():
                meta = statuses.get(encoder)
                if meta and meta["usable"]:
                    return {
                        "requested": requested,
                        "encoder": encoder,
                        "hardware": meta["hardware"],
                        "auto": True,
                        "usable": True,
                        "fallback": False,
                        "reason": meta["reason"],
                    }
            fallback = next((enc for enc in ["libx264", "libx265"] if statuses.get(enc, {}).get("usable")), None)
            return {
                "requested": requested,
                "encoder": fallback or "libx264",
                "hardware": False,
                "auto": True,
                "usable": bool(fallback),
                "fallback": False,
                "reason": "No usable encoder detected on this system.",
            }

        meta = statuses.get(requested, {"usable": False, "reason": "Unknown encoder", "hardware": requested not in {"libx264", "libx265"}})
        if meta.get("usable"):
            return {
                "requested": requested,
                "encoder": requested,
                "hardware": meta["hardware"],
                "auto": False,
                "usable": True,
                "fallback": False,
                "reason": meta["reason"],
            }

        fallback = next((enc for enc in ["libx264", "libx265"] if statuses.get(enc, {}).get("usable")), None)
        if allow_fallback and fallback:
            return {
                "requested": requested,
                "encoder": fallback,
                "hardware": False,
                "auto": False,
                "usable": False,
                "fallback": True,
                "reason": meta.get("reason", "Encoder unavailable on this system"),
            }

        return {
            "requested": requested,
            "encoder": requested,
            "hardware": meta.get("hardware", requested not in {"libx264", "libx265"}),
            "auto": False,
            "usable": False,
            "fallback": False,
            "reason": meta.get("reason", "Encoder unavailable on this system"),
        }

    def update_video_encoder_controls(self):
        info = self.resolve_video_encoder(allow_fallback=False, allow_probe=not self.encoder_probe_pending)
        encoder = info["encoder"]
        selected = self.get_selected_video_encoder_choice()
        platform_label = self.detect_runtime_environment().get("label", "Linux")
        software_controls = encoder in {"libx264", "libx265"} and (info["usable"] or info.get("pending"))
        self.combo_preset.setEnabled(software_controls)
        self.combo_tune.setEnabled(software_controls)

        if info.get("pending"):
            hint = f"{platform_label}: probing FFmpeg encoder capabilities in background. Auto will use a validated encoder when probing finishes."
            style = "color: #856404;"
        elif info["auto"]:
            if info["usable"]:
                hint = f"{platform_label}: Auto will use {encoder}. {info['reason']}"
                style = "color: #155724;"
            else:
                hint = f"{platform_label}: Auto could not find a usable encoder. Install FFmpeg software encoders or GPU drivers."
                style = "color: #c0392b;"
        elif not info["usable"]:
            hint = f"{platform_label}: {selected} is not usable here. {info['reason']} Select a green codec or switch back to Auto."
            style = "color: #c0392b;"
        elif info["hardware"]:
            hint = f"{platform_label}: {selected} is usable. Preset/tune are handled by the hardware driver; bitrate and GOP stay enforced by the GUI."
            style = "color: #155724;"
        else:
            hint = f"{platform_label}: {encoder} is usable. Full software preset/tune controls are active."
            style = "color: #155724;"

        self.label_encoder_hint.setStyleSheet(style)
        self.label_encoder_hint.setText(hint)

    def is_hevc_encoder(self, encoder_name):
        if not encoder_name:
            return False
        value = str(encoder_name).lower()
        return value == "libx265" or "hevc" in value or "265" in value

    def recommend_gop_size(self, fps, ts_rate_kbps=None, video_br_kbps=None):
        fps = max(1, int(fps))
        ts_rate = float(ts_rate_kbps or 0)
        video_br = float(video_br_kbps or 0)
        if ts_rate > 0 and (ts_rate < 180 or (video_br > 0 and video_br < 90)):
            return min(fps * 2, 60)
        return min(fps, 60)

    def get_dynamic_profile_names(self):
        return {"Auto Detect", "Pi 5 Safe", "Low SR DATV"}

    def get_smart_audio_defaults(self, ts_rate_kbps, preferred_audio_br="32"):
        preferred = max(24, int(preferred_audio_br or 32))
        if ts_rate_kbps < AUTO_AUDIO_DISABLE_TS_KBPS:
            return {
                "audio": False,
                "audio_br": "24",
                "audio_codec": "AAC-LC",
                "note": f"Smart profiles auto-disable audio below {AUTO_AUDIO_DISABLE_TS_KBPS} kbps TS to keep the transport valid.",
            }
        if ts_rate_kbps < SMART_AUDIO_LOW_TS_KBPS:
            return {
                "audio": True,
                "audio_br": "24",
                "audio_codec": "AAC-LC",
                "note": "Smart profiles reduce audio to 24 kbps AAC-LC at very low TS rates.",
            }
        if ts_rate_kbps < SMART_AUDIO_MID_TS_KBPS:
            return {
                "audio": True,
                "audio_br": "32",
                "audio_codec": "AAC-LC",
                "note": "Smart profiles keep audio conservative at 32 kbps AAC-LC in low-rate DATV mode.",
            }
        return {
            "audio": True,
            "audio_br": str(max(preferred, 32)),
            "audio_codec": "AAC-LC",
            "note": "",
        }

    def build_profile_config(self, base_cfg, ts_rate_kbps, preferred_audio_br="32"):
        cfg = dict(base_cfg)
        smart_audio = self.get_smart_audio_defaults(ts_rate_kbps, preferred_audio_br=preferred_audio_br)
        cfg.update({
            "audio": smart_audio["audio"],
            "audio_br": smart_audio["audio_br"],
            "audio_codec": smart_audio["audio_codec"],
            "note": smart_audio.get("note", ""),
        })
        suggested_video_br = max(32, int(ts_rate_kbps * (cfg["video_pct"] / 100.0)))
        cfg["gop"] = self.recommend_gop_size(cfg["fps"], ts_rate_kbps, suggested_video_br)
        return cfg

    def choose_auto_profile_settings(self):
        ts_rate = self.update_bitrate_estimate()
        env = self.detect_runtime_environment()
        if env["is_pi5"]:
            tiers = [
                (120, {"res": "320x240", "fps": "10", "codec": "Auto", "preset": "veryfast", "tune": "zerolatency", "video_pct": 60}),
                (260, {"res": "320x240", "fps": "15", "codec": "Auto", "preset": "veryfast", "tune": "zerolatency", "video_pct": 62}),
                (450, {"res": "320x240", "fps": "15", "codec": "Auto", "preset": "veryfast", "tune": "zerolatency", "video_pct": 64}),
                (850, {"res": "480x270", "fps": "20", "codec": "Auto", "preset": "veryfast", "tune": "zerolatency", "video_pct": 68}),
                (1600, {"res": "640x360", "fps": "25", "codec": "Auto", "preset": "veryfast", "tune": "zerolatency", "video_pct": 72}),
                (999999, {"res": "854x480", "fps": "25", "codec": "Auto", "preset": "faster", "tune": "zerolatency", "video_pct": 74}),
            ]
        elif env["is_pi"]:
            tiers = [
                (120, {"res": "320x240", "fps": "10", "codec": "Auto", "preset": "superfast", "tune": "zerolatency", "video_pct": 60}),
                (260, {"res": "320x240", "fps": "15", "codec": "Auto", "preset": "superfast", "tune": "zerolatency", "video_pct": 62}),
                (450, {"res": "320x240", "fps": "15", "codec": "Auto", "preset": "veryfast", "tune": "zerolatency", "video_pct": 64}),
                (850, {"res": "480x270", "fps": "20", "codec": "Auto", "preset": "veryfast", "tune": "zerolatency", "video_pct": 68}),
                (1600, {"res": "640x360", "fps": "25", "codec": "Auto", "preset": "veryfast", "tune": "zerolatency", "video_pct": 72}),
                (999999, {"res": "854x480", "fps": "25", "codec": "Auto", "preset": "veryfast", "tune": "zerolatency", "video_pct": 74}),
            ]
        else:
            tiers = [
                (120, {"res": "320x240", "fps": "10", "codec": "Auto", "preset": "veryfast", "tune": "zerolatency", "video_pct": 60}),
                (260, {"res": "320x240", "fps": "15", "codec": "Auto", "preset": "veryfast", "tune": "zerolatency", "video_pct": 62}),
                (450, {"res": "320x240", "fps": "15", "codec": "Auto", "preset": "veryfast", "tune": "zerolatency", "video_pct": 64}),
                (850, {"res": "640x360", "fps": "20", "codec": "Auto", "preset": "veryfast", "tune": "zerolatency", "video_pct": 68}),
                (1600, {"res": "854x480", "fps": "25", "codec": "Auto", "preset": "veryfast", "tune": "zerolatency", "video_pct": 72}),
                (3200, {"res": "1280x720", "fps": "25", "codec": "Auto", "preset": "faster", "tune": "zerolatency", "video_pct": 76}),
                (999999, {"res": "1280x720", "fps": "30", "codec": "Auto", "preset": "fast", "tune": "zerolatency", "video_pct": 78}),
            ]
        selected = next(settings for limit, settings in tiers if ts_rate <= limit)
        return self.build_profile_config(selected, ts_rate, preferred_audio_br="32")

    def choose_pi5_safe_profile_settings(self):
        ts_rate = self.update_bitrate_estimate()
        tiers = [
            (120, {"res": "320x240", "fps": "10", "codec": "libx264", "preset": "veryfast", "tune": "zerolatency", "video_pct": 62}),
            (260, {"res": "320x240", "fps": "15", "codec": "libx264", "preset": "veryfast", "tune": "zerolatency", "video_pct": 64}),
            (450, {"res": "320x240", "fps": "15", "codec": "libx264", "preset": "veryfast", "tune": "zerolatency", "video_pct": 66}),
            (850, {"res": "480x270", "fps": "20", "codec": "libx264", "preset": "veryfast", "tune": "zerolatency", "video_pct": 68}),
            (1600, {"res": "640x360", "fps": "20", "codec": "libx264", "preset": "veryfast", "tune": "zerolatency", "video_pct": 68}),
            (999999, {"res": "854x480", "fps": "25", "codec": "libx264", "preset": "faster", "tune": "zerolatency", "video_pct": 72}),
        ]
        selected = next(settings for limit, settings in tiers if ts_rate <= limit)
        return self.build_profile_config(selected, ts_rate, preferred_audio_br="32")

    def choose_low_sr_profile_settings(self):
        ts_rate = self.update_bitrate_estimate()
        tiers = [
            (120, {"res": "320x240", "fps": "10", "codec": "libx264", "preset": "veryfast", "tune": "zerolatency", "video_pct": 60}),
            (260, {"res": "320x240", "fps": "15", "codec": "libx264", "preset": "veryfast", "tune": "zerolatency", "video_pct": 62}),
            (450, {"res": "320x240", "fps": "15", "codec": "libx264", "preset": "veryfast", "tune": "zerolatency", "video_pct": 64}),
            (850, {"res": "480x270", "fps": "15", "codec": "libx264", "preset": "veryfast", "tune": "zerolatency", "video_pct": 66}),
            (999999, {"res": "480x270", "fps": "20", "codec": "libx264", "preset": "veryfast", "tune": "zerolatency", "video_pct": 68}),
        ]
        selected = next(settings for limit, settings in tiers if ts_rate <= limit)
        return self.build_profile_config(selected, ts_rate, preferred_audio_br="24")

    def get_profile_settings(self, profile_name):
        profiles = {
            "Auto Detect": self.choose_auto_profile_settings(),
            "Pi 5 Safe": self.choose_pi5_safe_profile_settings(),
            "Linux PC Balanced": {
                "res": "854x480", "fps": "25", "codec": "libx264", "preset": "veryfast", "tune": "zerolatency", "video_pct": 72, "gop": 25, "audio": True, "audio_br": "32", "audio_codec": "AAC-LC", "note": "",
            },
            "Low SR DATV": self.choose_low_sr_profile_settings(),
            "Higher SR DATV": {
                "res": "854x480", "fps": "25", "codec": "libx264", "preset": "veryfast", "tune": "zerolatency", "video_pct": 72, "gop": 25, "audio": True, "audio_br": "32", "audio_codec": "AAC-LC", "note": "",
            },
        }
        return profiles.get(profile_name)

    def get_active_profile_note(self, ts_rate_kbps):
        if self.combo_profile.currentText() in self.get_dynamic_profile_names() and ts_rate_kbps < AUTO_AUDIO_DISABLE_TS_KBPS:
            return f"{self.combo_profile.currentText()}: Smart profile auto-disabled audio below {AUTO_AUDIO_DISABLE_TS_KBPS} kbps TS to keep the transport valid."
        cfg = self.get_profile_settings(self.combo_profile.currentText())
        if cfg and cfg.get("note"):
            return cfg["note"]
        return ""

    def mark_profile_custom(self, *_args):
        if getattr(self, "profile_lock", False):
            return
        if hasattr(self, "combo_profile") and self.combo_profile.currentText() != "Custom":
            self.combo_profile.blockSignals(True)
            self.combo_profile.setCurrentText("Custom")
            self.combo_profile.blockSignals(False)

    def apply_video_profile(self, profile_name, silent=False):
        cfg = self.get_profile_settings(profile_name)
        if cfg is None:
            return
        self.profile_lock = True
        try:
            self.combo_res.setCurrentText(cfg["res"])
            self.combo_fps.setCurrentText(cfg["fps"])
            idx = self.combo_vcodec.findData(cfg["codec"])
            if idx < 0:
                idx = self.combo_vcodec.findText(cfg["codec"])
            if idx >= 0:
                self.combo_vcodec.setCurrentIndex(idx)
            self.combo_preset.setCurrentText(cfg["preset"])
            self.combo_tune.setCurrentText(cfg["tune"])
            self.video_br_slider.setValue(cfg["video_pct"])
            self.gop_size.setValue(cfg["gop"])
            self.check_audio.setChecked(cfg["audio"])
            self.combo_a_br.setCurrentText(cfg["audio_br"])
            self.combo_a_codec.setCurrentText(cfg["audio_codec"])
        finally:
            self.profile_lock = False
        self.update_audio_controls()
        self.update_video_encoder_controls()
        self.update_stream_guidance()
        if not silent:
            self.log_message(f"Loaded encoder profile: {profile_name}")

    def refresh_auto_profile_if_needed(self, *_args):
        if getattr(self, "profile_lock", False):
            return
        if hasattr(self, "combo_profile") and self.combo_profile.currentText() in self.get_dynamic_profile_names():
            self.apply_video_profile(self.combo_profile.currentText(), silent=True)

    def parse_resolution(self, resolution):
        width, height = resolution.split("x", 1)
        return int(width), int(height)

    def evaluate_stream_profile(self, ts_rate_kbps, resolution, fps, encoder_name=None):
        width, height = self.parse_resolution(resolution)
        profile_tiers = [
            {"limit": 90, "resolution": "320x240", "fps": 10, "label": "Ultra-low symbol-rate DATV"},
            {"limit": 180, "resolution": "320x240", "fps": 12, "label": "Very low symbol-rate DATV"},
            {"limit": 260, "resolution": "320x240", "fps": 15, "label": "Low symbol-rate DATV"},
            {"limit": 420, "resolution": "480x270", "fps": 20, "label": "Low symbol-rate DATV"},
            {"limit": 900, "resolution": "640x360", "fps": 25, "label": "Balanced DATV"},
            {"limit": 1800, "resolution": "854x480", "fps": 25, "label": "Higher-rate DATV"},
            {"limit": 4500, "resolution": "1280x720", "fps": 30, "label": "HD DATV"},
            {"limit": float("inf"), "resolution": "1920x1080", "fps": 30, "label": "High-capacity DATV"},
        ]
        tier = next((item for item in profile_tiers if ts_rate_kbps <= item["limit"]), profile_tiers[-1])
        target_width, target_height = self.parse_resolution(tier["resolution"])
        target_pixels = max(target_width * target_height, 1)
        actual_pixels = width * height
        pixel_ratio = actual_pixels / float(target_pixels)
        fps_ratio = fps / float(max(tier["fps"], 1))
        hevc_bonus = self.is_hevc_encoder(encoder_name)
        low_motion = self.combo_tune.currentText() == "stillimage" or fps <= 20
        advisory_pixel_factor = 1.55 + (0.35 if low_motion else 0.0) + (0.25 if hevc_bonus else 0.0)
        advisory_fps_factor = 1.15 + (0.15 if low_motion else 0.0) + (0.10 if hevc_bonus else 0.0)

        within_recommended = pixel_ratio <= 1.0 and fps_ratio <= 1.0
        within_advisory = actual_pixels <= int(target_pixels * advisory_pixel_factor) and fps <= int(tier["fps"] * advisory_fps_factor)
        borderline = (not within_recommended) and within_advisory
        ok = within_recommended

        status = "OK" if within_recommended else ("Advisory" if within_advisory else "Caution")
        message = (
            f"For about {ts_rate_kbps:.0f} kbps TS, a conservative starting point is {tier['resolution']} @ {tier['fps']} fps "
            f"({tier['label']})."
        )
        if borderline:
            message += (
                f" Current choice {resolution} @ {fps} fps is above that guideline, but it can still work for still or low-motion content. "
                "Verify on the actual receiver and watch spare bitrate, CPU/GPU load, and underflows."
            )
        elif not within_recommended:
            message += (
                f" Current choice {resolution} @ {fps} fps is well above the conservative guideline. "
                "It may still work only on very simple content, so test carefully before on-air use."
            )
        return {
            "ok": ok,
            "borderline": borderline,
            "status": status,
            "message": message,
            "tier": tier,
            "severity": "ok" if within_recommended else ("advisory" if within_advisory else "warning"),
            "pixel_ratio": pixel_ratio,
            "fps_ratio": fps_ratio,
        }

    def update_stream_guidance(self):
        if not hasattr(self, "stream_guidance"):
            return
        match = re.search(r"([\d.]+)\s*kbps", self.bitrate_estimate.text() if hasattr(self, "bitrate_estimate") else "")
        ts_rate = float(match.group(1)) if match else 0
        neutral_style = "color: #2c3e50; background: #ecf0f1; padding: 6px; border-radius: 4px;"
        if ts_rate <= 0:
            self.stream_guidance.setText("Select a valid DVB-S2 mode, FEC, frame size and symbol rate to see stream guidance.")
            self.stream_guidance.setStyleSheet(neutral_style)
            self.update_tx_action_state()
            return

        encoder = self.resolve_video_encoder(allow_probe=not self.encoder_probe_pending).get("encoder", "libx264")
        profile = self.evaluate_stream_profile(ts_rate, self.combo_res.currentText(), int(self.combo_fps.currentText()), encoder)
        bitrate_plan = self.calculate_service_bitrates(ts_rate)

        if not bitrate_plan.get("ok"):
            style = "color: #721c24; background: #f8d7da; padding: 6px; border-radius: 4px;"
        elif profile["severity"] == "ok":
            style = "color: #155724; background: #d4edda; padding: 6px; border-radius: 4px;"
        elif profile["severity"] == "advisory":
            style = "color: #856404; background: #fff3cd; padding: 6px; border-radius: 4px;"
        else:
            style = "color: #721c24; background: #f8d7da; padding: 6px; border-radius: 4px;"

        status_prefix = profile["status"]
        if not bitrate_plan.get("ok"):
            status_prefix = "Blocked"
        lines = [f"{status_prefix}: {profile['message']}"]

        profile_note = self.get_active_profile_note(ts_rate)
        if profile_note:
            lines.append(profile_note)
        if ts_rate <= 90:
            lines.append("Ultra-low-rate DATV below about 90 kbps TS is usually video-only. Start with audio disabled and 10 fps, then verify lock on the receiver.")
        encoder_hint = self.get_low_sr_encoder_hint(encoder)
        if encoder_hint:
            lines.append(encoder_hint)

        if bitrate_plan.get("ok"):
            lines.append(
                f"Planned TS budget: total {bitrate_plan['ts_rate']:.0f}k | video {bitrate_plan['video']}k | audio {bitrate_plan['audio']}k | overhead {bitrate_plan['mux_overhead']}k | guard {bitrate_plan.get('transport_guard', 0)}k | spare {bitrate_plan['spare']}k."
            )
            if self.check_audio.isChecked() and bitrate_plan.get("audio_adjustment_reason"):
                lines.append(bitrate_plan["audio_adjustment_reason"])
            if bitrate_plan.get("video_adjustment_reason"):
                lines.append(bitrate_plan["video_adjustment_reason"])
            if self.check_audio.isChecked() and bitrate_plan["requested_audio"] != bitrate_plan["audio"] and bitrate_plan["audio"] < bitrate_plan.get("effective_audio_request", bitrate_plan["requested_audio"]):
                lines.append(
                    f"Audio request is limited to {bitrate_plan['audio']}k by the DVB-S2 transport budget. Reduce resolution/FPS, pick a more efficient codec, or raise the symbol rate if you need more audio bitrate."
                )
        else:
            lines.append(bitrate_plan.get("reason", "Bitrate plan unavailable."))
            if self.check_audio.isChecked():
                lines.append(
                    f"Current audio setting {self.combo_a_codec.currentText()} at {self.combo_a_br.currentText()} kbps cannot fit this DVB-S2 TS budget. Disable audio, lower the bitrate, or raise the symbol rate."
                )

        self.stream_guidance.setStyleSheet(style)
        self.stream_guidance.setText("\n".join(lines))
        self.update_tx_action_state()

    def is_low_symbol_rate_active(self):
        try:
            sr = int(float(self.combo_sr.currentText()) * 1000)
        except (TypeError, ValueError):
            return False
        return sr < 250000 and getattr(self, "check_low_sr_stabilization", None) is not None and self.check_low_sr_stabilization.isChecked()

    def get_low_sr_extra_transport_guard_kbps(self, ts_rate_kbps):
        if not self.is_low_symbol_rate_active():
            return 0
        if ts_rate_kbps <= 140:
            return 18
        if ts_rate_kbps <= 260:
            return 14
        if ts_rate_kbps <= 420:
            return 10
        return 6

    def get_low_sr_encoder_hint(self, encoder_name):
        if not self.is_low_symbol_rate_active():
            return ""
        if encoder_name in {"libx264", "libx265"}:
            return "Low SR stabilization is active: software encoder running with tighter VBV / no-B-frame timing for low-rate DATV."
        if encoder_name.endswith(("_nvenc", "_qsv", "_amf", "_vaapi", "_v4l2m2m")):
            return "Low SR stabilization is active: the selected hardware encoder keeps its device path while using conservative low-delay timing settings for low-rate DATV."
        return "Low SR stabilization is active for this encoder."

    def get_video_filter_chain(self, resolution, fps, encoder_name):
        video_filter = f"scale={resolution.replace('x', ':')},fps={fps}"
        overlay = self.get_overlay_filter()
        if overlay:
            video_filter += overlay
        if encoder_name.endswith("_qsv") or encoder_name.endswith("_amf"):
            video_filter += ",format=nv12"
        elif encoder_name.endswith("_vaapi"):
            video_filter += ",format=nv12,hwupload"
        return video_filter

    def get_nvenc_preset(self, preset_name):
        preset_map = {
            "ultrafast": "p1",
            "superfast": "p1",
            "veryfast": "p2",
            "faster": "p2",
            "fast": "p3",
            "medium": "p4",
        }
        return preset_map.get(preset_name, "p2")

    def build_video_encoder_args(self, encoder_name, video_br, gop, preset, tune, video_filter, low_sr_active=False):
        base_rate = [
            "-g", str(gop),
            "-b:v:0", f"{video_br}k",
            "-vf", video_filter,
        ]
        capped_rate = [
            "-minrate:v:0", f"{video_br}k",
            "-maxrate:v:0", f"{video_br}k",
            "-bufsize:v:0", f"{video_br * 2}k",
        ]
        if encoder_name == "libx264":
            # Keep x264 VBV ownership stream-specific for the encoded video stream.
            # Reusing generic output-level -minrate/-maxrate/-bufsize later in the command
            # can override these and trigger the exact HRD/VBV warnings seen in the field logs.
            x264_params = (
                f"nal-hrd=cbr:force-cfr=1:repeat-headers=1:keyint={gop}:min-keyint={gop}:scenecut=0"
            )
            if low_sr_active:
                x264_params += ":bframes=0:vbv-init=1:sync-lookahead=0:sliced-threads=1"
            return [
                "-c:v", encoder_name,
                "-preset", preset,
                "-tune", tune,
                "-pix_fmt", "yuv420p",
                "-keyint_min", str(gop),
                "-sc_threshold", "0",
                "-x264-params", x264_params,
            ] + base_rate + capped_rate
        if encoder_name == "libx265":
            x265_params = f"hrd=1:repeat-headers=1:keyint={gop}:min-keyint={gop}:scenecut=0:vbv-maxrate={video_br}:vbv-bufsize={video_br * 2}:open-gop=0"
            if low_sr_active:
                x265_params += ":bframes=0:rc-lookahead=0"
            return [
                "-c:v", encoder_name,
                "-preset", preset,
                "-tune", tune,
                "-pix_fmt", "yuv420p",
                "-forced-idr", "1",
                "-x265-params", x265_params,
            ] + base_rate
        if encoder_name.endswith("_nvenc"):
            args = [
                "-c:v", encoder_name,
                "-preset", self.get_nvenc_preset(preset),
                "-pix_fmt", "yuv420p",
                "-rc", "cbr",
                "-forced-idr", "1",
            ]
            if tune == "zerolatency" or low_sr_active:
                args.extend(["-tune", "ll"])
            if low_sr_active:
                args.extend(["-bf", "0", "-rc-lookahead", "0"])
            return args + base_rate + capped_rate
        if encoder_name.endswith("_qsv"):
            args = [
                "-c:v", encoder_name,
                "-pix_fmt", "nv12",
                "-preset", preset,
                "-async_depth", "1",
                "-forced_idr", "1",
            ]
            if tune == "zerolatency" or low_sr_active:
                args.extend(["-low_delay_brc", "1", "-bf", "0"])
            return args + base_rate + capped_rate
        if encoder_name.endswith("_amf"):
            return ["-c:v", encoder_name, "-pix_fmt", "nv12"] + base_rate + capped_rate
        if encoder_name.endswith("_vaapi"):
            return ["-c:v", encoder_name] + base_rate + capped_rate
        args = ["-c:v", encoder_name, "-pix_fmt", "yuv420p"]
        return args + base_rate + capped_rate

    def needs_tighter_mpegts_timing(self, ts_rate_kbps, low_sr_active=False):
        try:
            ts_rate = int(ts_rate_kbps)
        except (TypeError, ValueError):
            return bool(low_sr_active)
        return bool(low_sr_active) or ts_rate <= 400

    def normalize_local_file_path(self, path_value):
        path = os.path.expanduser((path_value or "").strip())
        if not path:
            return ""
        return os.path.abspath(path)

    def build_ffmpeg_command(self, output_target="-", preview_seconds=None, force_encoder=None, verification_mode=False):
        net_br_kbps = self.update_bitrate_estimate()
        if "Short" in self.combo_frame.currentText() and self.combo_fec.currentText() == "9/10":
            return {"ok": False, "reason": "DVB-S2 does not define short-frame 9/10 coding."}
        if net_br_kbps == 0:
            return {"ok": False, "reason": "Cannot calculate bitrate. Check your settings."}

        bitrate_plan = self.calculate_service_bitrates(net_br_kbps)
        if not bitrate_plan["ok"]:
            return {"ok": False, "reason": bitrate_plan["reason"]}

        source_type = self.source_combo.currentText()
        source_desc = ""
        requested_encoder = force_encoder or self.get_selected_video_encoder_choice()
        encoder_info = self.resolve_video_encoder(requested_encoder, allow_fallback=not force_encoder)
        actual_encoder = encoder_info["encoder"]
        if not encoder_info.get("usable") and requested_encoder != "Auto" and not force_encoder:
            return {"ok": False, "reason": f"Selected encoder is not usable on this system. {encoder_info['reason']}"}
        if actual_encoder.endswith("_vaapi") and not self.detect_vaapi_device():
            return {"ok": False, "reason": "VAAPI encoder selected but no /dev/dri/renderD* device is available."}
        if actual_encoder.endswith("_v4l2m2m") and not self.has_v4l2m2m_device():
            return {"ok": False, "reason": "V4L2 mem2mem encoder selected but no Raspberry Pi style /dev/video10-12 device was found."}

        res = self.combo_res.currentText()
        fps = int(self.combo_fps.currentText())
        gop = self.gop_size.value()
        preset = self.combo_preset.currentText()
        tune = self.combo_tune.currentText()
        validation = self.evaluate_stream_profile(net_br_kbps, res, fps, actual_encoder)
        low_sr_active = self.is_low_symbol_rate_active()

        ffmpeg_cmd = ["ffmpeg", "-hide_banner", "-loglevel", "warning"]
        if actual_encoder.endswith("_vaapi"):
            ffmpeg_cmd.extend(["-vaapi_device", self.detect_vaapi_device()])

        if source_type == "Video File":
            video_file = self.normalize_local_file_path(self.file_path.text())
            if not os.path.exists(video_file):
                return {"ok": False, "reason": f"File not found: {video_file}"}
            ffmpeg_cmd.extend(self.get_file_input_options(low_sr_active=low_sr_active))
            ffmpeg_cmd.extend(["-stream_loop", "-1"])
            if preview_seconds is None:
                ffmpeg_cmd.extend(["-re"])
            ffmpeg_cmd.extend(["-i", video_file])
            source_desc = video_file
        elif source_type == "Webcam / Camera":
            try:
                ffmpeg_cmd.extend(self.build_webcam_video_input_args(res, fps, low_sr_active=low_sr_active))
            except Exception as exc:
                return {"ok": False, "reason": str(exc)}
            source_desc = "custom webcam FFmpeg input" if self.uses_webcam_custom_video_override() else self.get_selected_camera_device()
        else:
            stream_url = self.stream_url.text().strip()
            if not stream_url:
                return {"ok": False, "reason": "Enter an external stream URL or listen address."}
            normalized_stream_url = self.normalize_external_stream_url(stream_url)
            ffmpeg_cmd.extend(self.get_external_input_options(normalized_stream_url, verification_mode=verification_mode, low_sr_active=low_sr_active))
            ffmpeg_cmd.extend(["-i", normalized_stream_url])
            source_desc = normalized_stream_url

        audio_allowed = source_type != "Webcam / Camera" or self.is_webcam_audio_route_enabled()
        if source_type == "Webcam / Camera" and self.check_audio.isChecked() and audio_allowed:
            try:
                ffmpeg_cmd.extend(self.build_webcam_audio_input_args(low_sr_active=low_sr_active))
            except Exception as exc:
                return {"ok": False, "reason": str(exc)}

        if preview_seconds is not None:
            ffmpeg_cmd.extend(["-t", str(preview_seconds)])

        ffmpeg_cmd.extend(self.build_video_encoder_args(actual_encoder, bitrate_plan["video"], gop, preset, tune, self.get_video_filter_chain(res, fps, actual_encoder), low_sr_active=low_sr_active))

        audio_rate = "48000"
        audio_channels = "2"
        audio_desc = "Off"
        if self.check_audio.isChecked() and audio_allowed:
            audio_codec = self.get_audio_codec_settings()
            if source_type == "Webcam / Camera":
                ffmpeg_cmd.extend(["-map", "0:v:0", "-map", "1:a:0?"])
            else:
                ffmpeg_cmd.extend(["-map", "0:v:0", "-map", "0:a:0?"])
            ffmpeg_cmd.extend([
                "-c:a", audio_codec["codec"],
                "-b:a:0", f"{bitrate_plan['audio']}k",
                "-ar", audio_rate,
                "-ac", audio_channels,
            ])
            ffmpeg_cmd.extend(audio_codec["extra"])
            if self.needs_tighter_mpegts_timing(net_br_kbps, low_sr_active=low_sr_active):
                ffmpeg_cmd.extend(["-af", "aresample=async=1:first_pts=0:min_hard_comp=0.100"])
            audio_desc = audio_codec["display"]
        else:
            ffmpeg_cmd.extend(["-an"])

        service_name, service_provider = self.get_service_metadata()
        ffmpeg_cmd.extend([
            "-metadata", f"service_name={service_name}",
            "-metadata", f"service_provider={service_provider}",
            "-f", "mpegts",
            "-muxrate", f"{net_br_kbps}k",
            "-flush_packets", "1",
            "-max_muxing_queue_size", "4096",
            "-avoid_negative_ts", "make_zero",
            "-mpegts_flags", "+resend_headers+system_b+initial_discontinuity",
            "-mpegts_copyts", "0",
            "-pcr_period", "20",
            output_target,
        ])
        if self.needs_tighter_mpegts_timing(net_br_kbps, low_sr_active=low_sr_active):
            ffmpeg_cmd[-1:-1] = ["-max_interleave_delta", "0", "-pat_period", "0.10", "-sdt_period", "0.25"]

        return {
            "ok": True,
            "ffmpeg_cmd": ffmpeg_cmd,
            "bitrate_plan": bitrate_plan,
            "net_br_kbps": net_br_kbps,
            "video_br": bitrate_plan["video"],
            "audio_br": bitrate_plan["audio"],
            "fps": fps,
            "gop": gop,
            "resolution": res,
            "requested_encoder": requested_encoder,
            "actual_encoder": actual_encoder,
            "preset": preset,
            "tune": tune,
            "source_type": source_type,
            "source_desc": source_desc,
            "service_name": service_name,
            "service_provider": service_provider,
            "audio_desc": audio_desc,
            "audio_adjustment_reason": bitrate_plan.get("audio_adjustment_reason", ""),
            "validation": validation,
            "encoder_info": encoder_info,
            "low_sr_active": low_sr_active,
        }

    def verify_ts_configuration(self):
        requested = self.get_selected_video_encoder_choice()
        attempts = []
        if requested == "Auto":
            attempts.append(("Auto", None))
            attempts.append(("libx264 fallback", "libx264"))
        else:
            attempts.append((requested, requested))
            if requested not in {"libx264", "libx265"}:
                attempts.append(("libx264 fallback", "libx264"))

        try:
            subprocess.run(["ffprobe", "-version"], capture_output=True, timeout=3)
        except Exception:
            QMessageBox.warning(self, "TS Verification", "ffprobe was not found. Install FFmpeg tools to use verification.")
            return

        tmp_fd, tmp_path = tempfile.mkstemp(prefix="dvbs2_verify_", suffix=".ts")
        os.close(tmp_fd)
        QApplication.setOverrideCursor(Qt.CursorShape.WaitCursor)
        last_error = "Verification failed before FFmpeg started."
        used_plan = None
        used_label = None
        try:
            for attempt_label, forced_encoder in attempts:
                plan = self.build_ffmpeg_command(output_target="__preview__", preview_seconds=3, force_encoder=forced_encoder, verification_mode=True)
                if not plan.get("ok"):
                    last_error = plan.get("reason", "Unable to build FFmpeg preview command.")
                    continue

                ffmpeg_cmd = list(plan["ffmpeg_cmd"])
                ffmpeg_cmd[-1] = tmp_path
                try:
                    render = subprocess.run(ffmpeg_cmd, capture_output=True, text=True, timeout=25)
                except subprocess.TimeoutExpired:
                    last_error = f"{attempt_label} timed out while generating the preview TS. Check the input source and hardware encoder support."
                    self.log_message(f"⚠️ TS verify attempt failed: {last_error}", True)
                    continue

                if render.returncode != 0:
                    stderr = (render.stderr or render.stdout or "Unknown FFmpeg error").strip()
                    last_error = f"{attempt_label} failed to generate preview TS.\n\n{stderr}"
                    self.log_message(f"⚠️ TS verify attempt failed using {plan['actual_encoder']}: {stderr}", True)
                    continue

                probe = subprocess.run([
                    "ffprobe", "-v", "error", "-show_programs", "-show_streams", "-show_entries",
                    "format=bit_rate:program_tags=service_name,service_provider:format_tags=service_name,service_provider:stream=index,codec_name,codec_type,width,height,avg_frame_rate",
                    "-of", "json", tmp_path
                ], capture_output=True, text=True, timeout=10)
                if probe.returncode != 0:
                    stderr = (probe.stderr or probe.stdout or "Unknown ffprobe error").strip()
                    last_error = f"ffprobe could not inspect the preview TS.\n\n{stderr}"
                    self.log_message(f"⚠️ TS verify probe failed: {stderr}", True)
                    continue

                used_plan = plan
                used_label = attempt_label
                data = json.loads(probe.stdout or "{}")
                streams = data.get("streams", [])
                format_tags = ((data.get("format") or {}).get("tags") or {})
                program_tags = ((data.get("programs") or [{}])[0].get("tags") or {})
                video_stream = next((s for s in streams if s.get("codec_type") == "video"), {})
                audio_stream = next((s for s in streams if s.get("codec_type") == "audio"), {})
                detected_service = program_tags.get("service_name") or format_tags.get("service_name", "(not found)")
                detected_provider = program_tags.get("service_provider") or format_tags.get("service_provider", "(not found)")
                frame_rate = video_stream.get("avg_frame_rate", "0/0")
                summary_lines = [
                    f"Service Name: {detected_service}",
                    f"Service Provider: {detected_provider}",
                    f"Video: {video_stream.get('codec_name', 'n/a')} {video_stream.get('width', '?')}x{video_stream.get('height', '?')} @ {frame_rate}",
                    f"Audio: {audio_stream.get('codec_name', 'off')}",
                    f"Mux target: {plan['net_br_kbps']:.0f} kbps",
                    f"Encoder used: {plan['actual_encoder']}",
                    f"Low SR stabilization: {'on' if plan.get('low_sr_active') else 'off'}",
                ]
                if used_label != requested and requested != "Auto":
                    summary_lines.append(f"Fallback used: {used_label}")
                QMessageBox.information(self, "TS Verification", "\n".join(summary_lines))
                self.log_message(f"✅ TS verify passed | Service: {detected_service} | Provider: {detected_provider} | Encoder: {plan['actual_encoder']}")
                if used_label != requested and requested != "Auto":
                    self.log_message(f"ℹ️ Verification fell back from {requested} to {plan['actual_encoder']} for the preview test.")
                return

            QMessageBox.warning(self, "TS Verification", last_error)
        except Exception as exc:
            QMessageBox.warning(self, "TS Verification", f"Verification failed: {exc}")
        finally:
            QApplication.restoreOverrideCursor()
            try:
                os.remove(tmp_path)
            except OSError:
                pass

    def calculate_buffer_percent(self, buffer_level_kb, reported_percent=None):
        if reported_percent is not None:
            try:
                return max(0.0, min(100.0, float(reported_percent)))
            except (TypeError, ValueError):
                pass
        if buffer_level_kb <= 0:
            return 0.0
        return max(0.0, min(100.0, (buffer_level_kb / float(self.tx_ring_buffer_kb)) * 100.0))

    def get_audio_codec_settings(self):
        codec_name = self.combo_a_codec.currentText() if hasattr(self, "combo_a_codec") else "AAC-LC"
        codec_map = {
            "AAC-LC": {"codec": "aac", "extra": [], "display": "AAC-LC", "minimum_bitrate": 24},
            "HE-AAC (AAC+)": {"codec": "aac", "extra": ["-profile:a", "aac_he"], "display": "HE-AAC (AAC+)", "minimum_bitrate": 24},
            "MP2": {"codec": "mp2", "extra": [], "display": "MP2", "minimum_bitrate": 32},
            "AC3": {"codec": "ac3", "extra": [], "display": "AC3", "minimum_bitrate": 96},
            "E-AC3": {"codec": "eac3", "extra": [], "display": "E-AC3", "minimum_bitrate": 48},
            "Opus": {"codec": "libopus", "extra": ["-application", "audio", "-frame_duration", "20"], "display": "Opus", "minimum_bitrate": 16},
        }
        return codec_map.get(codec_name, codec_map["AAC-LC"])

    def estimate_minimum_video_bitrate(self, ts_rate_kbps, resolution, fps, encoder_name=None):
        width, height = self.parse_resolution(resolution)
        complexity = (width * height * max(fps, 10)) / float(640 * 360 * 25)
        if ts_rate_kbps <= 400:
            base_floor = 48
        elif ts_rate_kbps <= 800:
            base_floor = 96
        elif ts_rate_kbps <= 1500:
            base_floor = 180
        else:
            base_floor = 320
        codec_factor = 0.82 if self.is_hevc_encoder(encoder_name) else 1.0
        floor = int(base_floor * max(0.6, complexity) * codec_factor)
        floor = max(32, floor)
        floor = min(floor, max(int(ts_rate_kbps * 0.78), 32))
        return floor

    def estimate_transport_guard_kbps(self, ts_rate_kbps, audio_enabled):
        if not audio_enabled:
            return 0
        if ts_rate_kbps <= 260:
            return 32
        if ts_rate_kbps <= 380:
            return 18
        if ts_rate_kbps <= 550:
            return 10
        return 0

    def calculate_service_bitrates(self, ts_rate_kbps):
        audio_enabled = self.check_audio.isChecked() if hasattr(self, "check_audio") else False
        requested_audio = int(self.combo_a_br.currentText()) if (audio_enabled and hasattr(self, "combo_a_br")) else 0
        audio_codec = self.get_audio_codec_settings() if audio_enabled else {"minimum_bitrate": 0, "display": "Off"}
        codec_min_audio = int(audio_codec.get("minimum_bitrate", 0))
        effective_audio_request = max(requested_audio, codec_min_audio) if audio_enabled else 0

        mux_overhead = max(12, min(80, int(ts_rate_kbps * 0.035) + 8))
        payload_budget = max(ts_rate_kbps - mux_overhead, 16)
        transport_guard = self.estimate_transport_guard_kbps(ts_rate_kbps, audio_enabled)
        low_sr_extra_guard = self.get_low_sr_extra_transport_guard_kbps(ts_rate_kbps)
        transport_guard += low_sr_extra_guard
        planning_payload = max(payload_budget - transport_guard, 16)
        encoder_name = self.resolve_video_encoder(allow_probe=not self.encoder_probe_pending).get("encoder", "libx264")
        minimum_video = self.estimate_minimum_video_bitrate(ts_rate_kbps, self.combo_res.currentText(), int(self.combo_fps.currentText()), encoder_name)
        minimum_video = min(minimum_video, max(payload_budget - 16, 16))

        audio_br = 0
        recommended_audio = 0
        audio_adjustment_reason = ""
        video_adjustment_reason = ""
        if audio_enabled:
            if payload_budget <= 220:
                recommended_audio = 24
            elif payload_budget <= 420:
                recommended_audio = 32
            elif payload_budget <= 700:
                recommended_audio = 48
            else:
                recommended_audio = 64
            max_audio = max(0, payload_budget - minimum_video)
            target_audio = effective_audio_request
            if requested_audio < codec_min_audio:
                audio_adjustment_reason = f"Raised audio bitrate from {requested_audio}k to {codec_min_audio}k because {audio_codec['display']} needs at least {codec_min_audio} kbps for reliable FFmpeg encoding."
            audio_br = min(target_audio, max_audio)
            if transport_guard > 0:
                video_adjustment_reason = (
                    f"Applied a conservative {transport_guard} kbps transport guard so FFmpeg MPEG-TS CBR stays more stable at this TS rate."
                )
            elif low_sr_extra_guard > 0:
                video_adjustment_reason = (
                    f"Applied an extra {low_sr_extra_guard} kbps low-symbol-rate guard so MPEG-TS timing stays steadier below 250 kS/s."
                )

        if audio_enabled and audio_br < max(codec_min_audio, 1):
            return {
                "ok": False,
                "reason": f"Not enough bitrate for {audio_codec['display']} audio at {ts_rate_kbps} kbps TS rate. {audio_codec['display']} needs at least {codec_min_audio} kbps plus video overhead. Choose a lower-overhead codec, disable audio, or raise the symbol rate.",
                "ts_rate": ts_rate_kbps,
                "payload_budget": payload_budget,
                "planning_payload": planning_payload,
                "transport_guard": transport_guard,
                "low_sr_extra_guard": low_sr_extra_guard,
                "mux_overhead": mux_overhead,
                "audio": 0,
                "video": max(payload_budget, 16),
                "requested_audio": requested_audio,
                "recommended_audio": recommended_audio,
                "minimum_video": minimum_video,
                "audio_codec": audio_codec["display"],
                "codec_min_audio": codec_min_audio,
                "audio_adjustment_reason": audio_adjustment_reason,
                "video_adjustment_reason": video_adjustment_reason,
            }

        max_video = max(payload_budget - audio_br, 16)
        safe_max_video = max(max(planning_payload - audio_br, 16), minimum_video)
        video_target = max(int(planning_payload * (self.video_br_slider.value() / 100.0)), minimum_video)
        video_br = min(video_target, max_video, safe_max_video)

        remaining_budget = max(payload_budget - (video_br + audio_br), 0)
        return {
            "ok": video_br > 0,
            "reason": "",
            "ts_rate": ts_rate_kbps,
            "payload_budget": payload_budget,
            "planning_payload": planning_payload,
            "transport_guard": transport_guard,
            "low_sr_extra_guard": low_sr_extra_guard,
            "mux_overhead": mux_overhead,
            "audio": audio_br,
            "video": video_br,
            "requested_audio": requested_audio,
            "effective_audio_request": effective_audio_request,
            "recommended_audio": recommended_audio,
            "minimum_video": minimum_video,
            "spare": remaining_budget,
            "audio_codec": audio_codec["display"] if audio_enabled else "Off",
            "codec_min_audio": codec_min_audio,
            "audio_adjustment_reason": audio_adjustment_reason,
            "video_adjustment_reason": video_adjustment_reason,
        }

    def get_current_bits_per_symbol(self):
        return {
            "QPSK": 2,
            "8PSK": 3,
            "16APSK": 4,
            "32APSK": 5,
        }.get(self.combo_mode.currentText(), 2)

    def valid_fec_rates(self):
        frame_short = "Short" in self.combo_frame.currentText()
        mode = self.combo_mode.currentText()
        if mode == "8PSK":
            rates = list(self.psk8_fec_rates)
        elif mode == "16APSK":
            rates = list(self.apsk16_fec_rates)
        elif mode == "32APSK":
            rates = list(self.apsk32_fec_rates)
        else:
            rates = list(self.qpsk_fec_rates)
        if frame_short and "9/10" in rates:
            rates.remove("9/10")
        return rates

    def update_fec_options(self):
        current = self.combo_fec.currentText()
        allowed = self.valid_fec_rates()
        self.combo_fec.blockSignals(True)
        self.combo_fec.clear()
        self.combo_fec.addItems(allowed)
        if current in allowed:
            self.combo_fec.setCurrentText(current)
        elif self.combo_mode.currentText() == "8PSK":
            self.combo_fec.setCurrentText("3/4" if "3/4" in allowed else allowed[0])
        elif self.combo_mode.currentText() == "16APSK":
            self.combo_fec.setCurrentText("2/3" if "2/3" in allowed else allowed[0])
        elif self.combo_mode.currentText() == "32APSK":
            self.combo_fec.setCurrentText("3/4" if "3/4" in allowed else allowed[0])
        else:
            self.combo_fec.setCurrentText("1/2" if "1/2" in allowed else allowed[0])
        self.combo_fec.blockSignals(False)
        self.update_bitrate_estimate()

    def get_modcod_string(self):
        mode = self.combo_mode.currentText()
        fec = self.combo_fec.currentText()
        frame = "S" if "Short" in self.combo_frame.currentText() else "N"
        return f"{mode}_{fec}_{frame}"
    
    def selected_device_key(self):
        device = self.combo_device.currentText()
        mapping = {
            "LimeSDR mini": "lime_mini",
            "HackRFone": "hackrf",
            "AdalmPluto": "pluto_adalm_default",
            "PlutoDVB2 F5OEO": "plutodvb2_f5oeo",
            "Pluto F5UII 0303/2402": "pluto_0303_2402",
        }
        return mapping.get(device, "hackrf")

    def selected_backend_device(self):
        key = self.selected_device_key()
        if key == "hackrf":
            return "hackrf"
        if key == "lime_mini":
            return "lime"
        return "pluto"

    def selected_requires_plutodvb2(self):
        return self.selected_device_key() == "plutodvb2_f5oeo"

    def selected_requires_pluto_0303(self):
        return self.selected_device_key() == "pluto_0303_2402"

    def selected_requires_f5oeo(self):
        return self.selected_requires_plutodvb2() or self.selected_requires_pluto_0303()

    def should_enable_f5oeo_runtime_resync(self):
        value = os.environ.get("DATV_F5OEO_RUNTIME_RESYNC", "")
        return str(value).strip().lower() in {"1", "true", "yes", "on"}

    def should_enable_pluto_0303_runtime_resync(self):
        value = os.environ.get("DATV_PLUTO_0303_RUNTIME_RESYNC", "")
        return str(value).strip().lower() in {"1", "true", "yes", "on"}

    def get_selected_pluto_stop_policy(self):
        if hasattr(self, "combo_pluto_stop_policy"):
            return str(self.combo_pluto_stop_policy.currentData() or "pass_only")
        return "pass_only"

    def uses_webcam_custom_video_override(self):
        return bool(hasattr(self, "camera_custom_video_input") and self.camera_custom_video_input.text().strip())

    def uses_webcam_custom_audio_override(self):
        return bool(hasattr(self, "camera_custom_audio_input") and self.camera_custom_audio_input.text().strip())

    def is_webcam_audio_route_enabled(self):
        if not hasattr(self, "check_camera_mic") or not self.check_camera_mic.isChecked():
            return False
        if self.uses_webcam_custom_audio_override():
            return True
        source = self.get_selected_audio_source() if hasattr(self, "get_selected_audio_source") else {}
        return bool(source.get("args"))

    def get_selected_tx_gain_value(self):
        value = self.tx_level_spin.value()
        if self.selected_backend_device() == "pluto":
            return round(value, 2)
        if self.selected_backend_device() == "lime":
            return round(value, 2)
        return int(round(value))

    def get_pluto_uri(self):
        host = self.input_pluto_ip.text().strip() or "192.168.2.1"
        return f"ip:{host}"

    def get_lime_device_args(self):
        return "driver=lime"

    def get_fixed_pluto_iiod_port(self):
        return PLUTO_DEFAULT_IIOD_PORT

    def get_fixed_f5oeo_mqtt_port(self):
        return PLUTODVB2_MQTT_PORT

    def get_fixed_pluto_0303_mqtt_port(self):
        return PLUTO_0303_MQTT_PORT

    def get_f5oeo_topic_key(self):
        return self.input_pluto_topic.text().strip().upper()

    def update_callsign_header(self):
        if not hasattr(self, "header_callsign"):
            return
        callsign = self.get_f5oeo_topic_key() or "CALLSIGN"
        self.header_callsign.setText(callsign)

    def normalize_callsign_text(self, text):
        return (text or "").strip().upper()

    def on_overlay_callsign_changed(self, _text):
        if getattr(self, "_loading_settings", False):
            return
        manual = self.callsign_input.text().strip() if hasattr(self, "callsign_input") else ""
        self.overlay_callsign_manual = bool(manual)

    def load_persisted_settings(self):
        self._loading_settings = True
        try:
            callsign = self.normalize_callsign_text(self.settings.value("station_callsign", "", type=str))
            if callsign:
                self.input_pluto_topic.setText(callsign)
            pluto_ip = (self.settings.value("pluto_ip", "192.168.2.1", type=str) or "192.168.2.1").strip()
            self.input_pluto_ip.setText(pluto_ip)
            pluto_stop_policy = (self.settings.value("pluto_stop_policy", "pass_only", type=str) or "pass_only").strip()
            if hasattr(self, "combo_pluto_stop_policy"):
                idx = self.combo_pluto_stop_policy.findData(pluto_stop_policy)
                self.combo_pluto_stop_policy.setCurrentIndex(idx if idx >= 0 else 0)
            service_name = (self.settings.value("service_name", "", type=str) or "").strip()
            if service_name:
                self.service_name_input.setText(service_name)
            service_provider = (self.settings.value("service_provider", "", type=str) or "").strip()
            if service_provider:
                self.service_provider_input.setText(service_provider)
            overlay_text = self.settings.value("overlay_text", None)
            if overlay_text is not None:
                self.callsign_input.setText((overlay_text or "").strip())
                self.overlay_callsign_manual = bool((overlay_text or "").strip())
            selected_device = (self.settings.value("device_name", "", type=str) or "").strip()
            if selected_device == "Pluto F5OEO 0303 2402":
                selected_device = "Pluto F5UII 0303/2402"
            if selected_device:
                idx = self.combo_device.findText(selected_device)
                if idx >= 0:
                    self.combo_device.setCurrentIndex(idx)
            window_preset = (self.settings.value("window_size_preset", "", type=str) or "").strip()
            if not window_preset:
                compact_layout = bool(self.settings.value("compact_layout", False, type=bool))
                if compact_layout:
                    window_preset = "Raspberry Pi 5"
                else:
                    window_preset = "Raspberry Pi 5" if self.detect_runtime_environment().get("is_pi5") else "Standard"
            window_preset = self.normalize_window_preset_name(window_preset)
            if hasattr(self, "combo_window_preset"):
                idx = self.combo_window_preset.findText(window_preset)
                if idx < 0:
                    idx = self.combo_window_preset.findText("Standard")
                self.combo_window_preset.setCurrentIndex(max(idx, 0))
            low_sr_stabilization = bool(self.settings.value("low_sr_stabilization", False, type=bool))
            if hasattr(self, "check_low_sr_stabilization"):
                self.check_low_sr_stabilization.setChecked(low_sr_stabilization)
            file_path = (self.settings.value("file_path", "", type=str) or "").strip()
            if file_path:
                self.file_path.setText(file_path)
            stream_url = (self.settings.value("stream_url", "", type=str) or "").strip()
            if stream_url:
                self.stream_url.setText(stream_url)
            camera_device = (self.settings.value("camera_device", "", type=str) or "").strip()
            if camera_device and hasattr(self, "camera_device_combo"):
                idx = self.camera_device_combo.findData(camera_device)
                if idx >= 0:
                    self.camera_device_combo.setCurrentIndex(idx)
            camera_mic_enabled = bool(self.settings.value("camera_mic_enabled", False, type=bool))
            if hasattr(self, "check_camera_mic"):
                self.check_camera_mic.setChecked(camera_mic_enabled)
            camera_audio_source_id = (self.settings.value("camera_audio_source_id", "", type=str) or "").strip()
            if camera_audio_source_id and hasattr(self, "camera_audio_source_combo"):
                for idx in range(self.camera_audio_source_combo.count()):
                    data = self.camera_audio_source_combo.itemData(idx) or {}
                    if isinstance(data, dict) and data.get("id") == camera_audio_source_id:
                        self.camera_audio_source_combo.setCurrentIndex(idx)
                        break
            camera_custom_video_input = (self.settings.value("camera_custom_video_input", "", type=str) or "").strip()
            if camera_custom_video_input and hasattr(self, "camera_custom_video_input"):
                self.camera_custom_video_input.setText(camera_custom_video_input)
            camera_custom_audio_input = (self.settings.value("camera_custom_audio_input", "", type=str) or "").strip()
            if camera_custom_audio_input and hasattr(self, "camera_custom_audio_input"):
                self.camera_custom_audio_input.setText(camera_custom_audio_input)
            selected_source = (self.settings.value("source_mode", "", type=str) or "").strip()
            if selected_source:
                idx = self.source_combo.findText(selected_source)
                if idx >= 0:
                    self.source_combo.setCurrentIndex(idx)
        finally:
            self._loading_settings = False
        self.update_callsign_header()
        self.maybe_fill_overlay_callsign()

    def save_persisted_settings(self, *_args):
        if getattr(self, "_loading_settings", False) or not hasattr(self, "settings"):
            return
        self.settings.setValue("station_callsign", self.normalize_callsign_text(self.input_pluto_topic.text()))
        self.settings.setValue("pluto_ip", (self.input_pluto_ip.text().strip() or "192.168.2.1"))
        if hasattr(self, "combo_pluto_stop_policy"):
            self.settings.setValue("pluto_stop_policy", self.combo_pluto_stop_policy.currentData() or "pass_only")
        self.settings.setValue("service_name", self.service_name_input.text().strip())
        self.settings.setValue("service_provider", self.service_provider_input.text().strip())
        self.settings.setValue("overlay_text", self.callsign_input.text().strip())
        self.settings.setValue("device_name", self.combo_device.currentText())
        self.settings.remove("sample_strategy")
        self.settings.remove("pluto_identity_override")
        if hasattr(self, "combo_window_preset"):
            normalized_preset = self.normalize_window_preset_name(self.combo_window_preset.currentText())
            self.settings.setValue("window_size_preset", normalized_preset)
            self.settings.setValue("compact_layout", normalized_preset == "Raspberry Pi 5")
        if hasattr(self, "check_low_sr_stabilization"):
            self.settings.setValue("low_sr_stabilization", self.check_low_sr_stabilization.isChecked())
        self.settings.setValue("source_mode", self.source_combo.currentText())
        self.settings.setValue("camera_device", self.get_selected_camera_device())
        if hasattr(self, "check_camera_mic"):
            self.settings.setValue("camera_mic_enabled", self.check_camera_mic.isChecked())
        if hasattr(self, "camera_audio_source_combo"):
            self.settings.setValue("camera_audio_source_id", self.get_selected_audio_source_id())
        if hasattr(self, "camera_custom_video_input"):
            self.settings.setValue("camera_custom_video_input", self.camera_custom_video_input.text().strip())
        if hasattr(self, "camera_custom_audio_input"):
            self.settings.setValue("camera_custom_audio_input", self.camera_custom_audio_input.text().strip())
        self.settings.setValue("file_path", self.file_path.text().strip())
        self.settings.setValue("stream_url", self.stream_url.text().strip())
        self.settings.sync()

    def maybe_fill_overlay_callsign(self):
        if not hasattr(self, "callsign_input"):
            return
        current = self.callsign_input.text().strip()
        top_callsign = self.get_f5oeo_topic_key()
        if not current or current == getattr(self, "last_synced_callsign", ""):
            self.callsign_input.blockSignals(True)
            self.callsign_input.setText(top_callsign)
            self.callsign_input.blockSignals(False)
            self.last_synced_callsign = top_callsign

    def get_effective_overlay_text(self):
        manual = self.callsign_input.text().strip() if hasattr(self, "callsign_input") else ""
        return manual or self.get_f5oeo_topic_key()


    def normalize_f5oeo_callsign_entry(self):
        normalized = self.normalize_callsign_text(self.input_pluto_topic.text())
        if self.input_pluto_topic.text() != normalized:
            self.input_pluto_topic.setText(normalized)
        self.save_persisted_settings()
        self.update_f5oeo_callsign_ui()

    def update_callsign_field_state(self):
        if not hasattr(self, "input_pluto_topic"):
            return
        required = self.selected_requires_plutodvb2() if hasattr(self, "combo_device") else False
        has_value = bool(self.get_f5oeo_topic_key())
        style = ""
        if required and not has_value:
            style = "border: 2px solid #c0392b; border-radius: 4px; padding: 4px;"
        elif required and has_value:
            style = "border: 2px solid #27ae60; border-radius: 4px; padding: 4px;"
        self.input_pluto_topic.setStyleSheet(style)

    def get_f5oeo_requirement_status(self):
        if not hasattr(self, "combo_device") or not self.selected_requires_f5oeo():
            return True, ""
        if mqtt_publish is None:
            return False, "Pluto MQTT control requires paho-mqtt. Run install.sh or: pip install -r requirements.txt"
        if self.selected_requires_plutodvb2():
            topic_key = self.get_f5oeo_topic_key()
            if not topic_key:
                return False, "Mandatory for PlutoDVB2 F5OEO: enter the callsign / topic key configured in the Pluto firmware."
        return True, ""

    def get_tx_blockers(self):
        blockers = []

        ok, reason = self.get_f5oeo_requirement_status()
        if not ok and reason:
            blockers.append(reason)

        if hasattr(self, "combo_device") and hasattr(self, "device_status"):
            device_key = self.selected_device_key()
            if device_key and not self.device_status.get(device_key, False):
                blockers.append(f"Selected RF device not detected: {self.combo_device.currentText()}")

        if "Short" in self.combo_frame.currentText() and self.combo_fec.currentText() == "9/10":
            blockers.append("DVB-S2 short frame does not support FEC 9/10.")

        match = re.search(r"([\d.]+)\s*kbps", self.bitrate_estimate.text() if hasattr(self, "bitrate_estimate") else "")
        ts_rate = float(match.group(1)) if match else 0
        if ts_rate <= 0:
            blockers.append("Invalid DVB-S2 symbol rate / FEC combination.")
        else:
            bitrate_plan = self.calculate_service_bitrates(ts_rate)
            if not bitrate_plan.get("ok"):
                blockers.append(bitrate_plan.get("reason", "Encoder / transport bitrate plan is invalid."))

        source_type = self.source_combo.currentText() if hasattr(self, "source_combo") else "Video File"
        if source_type == "Video File":
            file_path = self.file_path.text().strip() if hasattr(self, "file_path") else ""
            if not file_path:
                blockers.append("Select a video file or use the DATV test pattern.")
            elif not os.path.exists(file_path):
                blockers.append("Selected video file does not exist.")
        elif source_type == "External Stream (OBS / FFmpeg)":
            if not (self.stream_url.text().strip() if hasattr(self, "stream_url") else ""):
                blockers.append("Enter an FFmpeg-compatible input URL for the external stream source.")
        elif source_type == "Webcam / Camera":
            if not self.uses_webcam_custom_video_override() and not self.get_selected_camera_device():
                blockers.append("No webcam / camera device is selected, and no custom webcam FFmpeg input override was provided.")
            if self.check_audio.isChecked() and not self.is_webcam_audio_route_enabled():
                blockers.append("Webcam audio is enabled, but no microphone route or custom FFmpeg microphone input is configured.")

        encoder_info = self.resolve_video_encoder(allow_probe=not self.encoder_probe_pending)
        if encoder_info.get("auto") and not encoder_info.get("usable"):
            blockers.append("Auto codec detection could not find a usable FFmpeg video encoder on this system.")
        elif not encoder_info.get("auto") and not encoder_info.get("usable") and not encoder_info.get("fallback"):
            blockers.append(f"Selected codec {encoder_info.get('requested', 'Unknown')} is not usable here.")

        deduped = []
        for item in blockers:
            if item and item not in deduped:
                deduped.append(item)
        return deduped

    def update_tx_action_state(self):
        if not hasattr(self, "btn_start"):
            return
        if hasattr(self, "btn_stop") and self.btn_stop.isEnabled():
            self.btn_start.setEnabled(False)
            self.btn_start.setToolTip("Transmission is already running.")
            if hasattr(self, "tx_action_hint"):
                self.tx_action_hint.setStyleSheet("color: #856404; padding-top: 4px;")
                self.tx_action_hint.setText("Transmission is active. Stop the current stream before changing RF or encoder settings.")
            return

        blockers = self.get_tx_blockers()
        ready = not blockers
        self.btn_start.setEnabled(ready)
        tooltip = "\n".join(blockers)
        self.btn_start.setToolTip(tooltip if blockers else "Ready to start transmission.")
        if hasattr(self, "tx_action_hint"):
            if ready:
                self.tx_action_hint.setStyleSheet("color: #155724; padding-top: 4px;")
                self.tx_action_hint.setText("Ready: RF device, source selection, and bitrate plan all look valid.")
            else:
                self.tx_action_hint.setStyleSheet("color: #721c24; padding-top: 4px;")
                self.tx_action_hint.setText(f"START blocked: {' | '.join(blockers)}")

    def build_f5oeo_mqtt_context(self):
        topic_key = self.get_f5oeo_topic_key()
        host = self.input_pluto_ip.text().strip() or "192.168.2.1"
        port = self.get_fixed_f5oeo_mqtt_port()
        topic = f"cmd/pluto/{topic_key}/tx/stream/mode"
        return {
            "topic_key": topic_key,
            "host": host,
            "port": port,
            "topic": topic,
            "topic_base": f"cmd/pluto/{topic_key}",
            "hook_mode": "plutodvb2",
        }

    def build_pluto_0303_mqtt_context(self):
        host = self.input_pluto_ip.text().strip() or "192.168.2.1"
        port = self.get_fixed_pluto_0303_mqtt_port()
        return {
            "host": host,
            "port": port,
            "topic_base": "plutodvb",
            "hook_mode": "plutodvb_0303",
        }

    def get_sample_rate_strategy_key(self):
        return "auto"

    def get_pluto_identity_override_key(self):
        return "auto"

    def get_minimum_sample_rate_for_backend(self, backend_device=None):
        backend = str(backend_device or self.selected_backend_device() or "hackrf").strip().lower()
        if backend == "pluto":
            return PLUTO_SAFE_MIN_SAMPLE_RATE
        if backend == "lime":
            return LIME_MIN_SAMPLE_RATE
        return HACKRF_MIN_SAMPLE_RATE

    def choose_even_sps_with_floor(self, symbol_rate, minimum_rate, min_sps=8):
        sr = max(int(symbol_rate), 1)
        sps = max(int(min_sps), int(math.ceil(float(minimum_rate) / float(sr))))
        if sps % 2:
            sps += 1
        return sps

    def choose_even_sps_near_target(self, symbol_rate, target_rate, min_sps=4, max_sps=64):
        sr = max(int(symbol_rate), 1)
        start = int(min_sps) if int(min_sps) % 2 == 0 else int(min_sps) + 1
        candidates = [value for value in range(start, int(max_sps) + 1, 2)]
        if not candidates:
            return self.choose_even_sps_with_floor(sr, target_rate, min_sps=min_sps)
        return min(candidates, key=lambda value: (abs((sr * value) - int(target_rate)), value))

    def choose_even_sps_near_target_with_floor(self, symbol_rate, target_rate, minimum_rate, min_sps=4, max_sps=64):
        sr = max(int(symbol_rate), 1)
        start = int(min_sps) if int(min_sps) % 2 == 0 else int(min_sps) + 1
        best = None
        for value in range(start, int(max_sps) + 1, 2):
            sample_rate = sr * value
            if sample_rate < int(minimum_rate):
                continue
            score = (abs(sample_rate - int(target_rate)), sample_rate, value)
            if best is None or score < best[0]:
                best = (score, value)
        if best is not None:
            return best[1]
        return self.choose_even_sps_with_floor(sr, minimum_rate, min_sps=min_sps)

    def estimate_tx_sample_plan(self, symbol_rate, backend_device=None):
        sr = max(int(symbol_rate), 1)
        backend = str(backend_device or self.selected_backend_device() or "hackrf").strip().lower()
        minimum_rate = self.get_minimum_sample_rate_for_backend(backend)

        if backend == "pluto":
            preferred_min_sps = 10 if sr <= 500000 else 8
            sps = self.choose_even_sps_with_floor(sr, minimum_rate, min_sps=preferred_min_sps)
            strategy = "auto_safe_pluto_clean"
            if preferred_min_sps >= 10:
                note = "Automatic Pluto clean-mode plan. DATV-Linux keeps Pluto at SPS 10 up to 500 kS/s for a more robust constellation."
            else:
                note = "Automatic Pluto safe-floor plan above 500 kS/s."
        elif backend == "lime":
            preferred_min_sps = 10 if sr >= 100000 else 8
            sps = self.choose_even_sps_with_floor(sr, minimum_rate, min_sps=preferred_min_sps)
            strategy = "auto_lime_usable"
            note = "Automatic LimeSDR usable-range plan. SPS 10 is held from 100 kS/s upward while lower symbol rates still respect the 1.0 MS/s floor."
        else:
            preferred_min_sps = 10 if sr >= 200000 else 8
            sps = self.choose_even_sps_with_floor(sr, minimum_rate, min_sps=preferred_min_sps)
            strategy = "auto_hackrf_usable"
            note = "Automatic HackRF usable-range plan. SPS 10 is held from 200 kS/s upward for cleaner shaping while keeping lower rates floor-safe."

        sample_rate = sr * sps
        return {
            "symbol_rate": sr,
            "backend": backend,
            "requested_strategy": "auto",
            "strategy": strategy,
            "minimum_rate": minimum_rate,
            "sps": sps,
            "sample_rate": sample_rate,
            "note": note,
        }

    def estimate_tx_sample_rate(self, symbol_rate, backend_device=None):
        return int(self.estimate_tx_sample_plan(symbol_rate, backend_device)["sample_rate"])

    def update_rf_sample_rate_hint(self):
        if not hasattr(self, "label_tx_sr_plan"):
            return
        try:
            sr = int(float(self.combo_sr.currentText()) * 1000)
        except (TypeError, ValueError):
            self.label_tx_sr_plan.setText("Local TX sample-rate estimate: enter a valid symbol rate.")
            self.label_tx_sr_plan.setStyleSheet("color: #c0392b;")
            return
        plan = self.estimate_tx_sample_plan(sr, self.selected_backend_device())
        color = "#7f8c8d"
        if plan.get("strategy") in {"pluto_f5oeo_compat_floor_guard", "auto_safe_pluto_clean", "auto_lime_usable", "auto_hackrf_usable"}:
            color = "#b9770e"
        self.label_tx_sr_plan.setStyleSheet(f"color: {color};")
        self.label_tx_sr_plan.setText(
            f"Local TX sample-rate estimate: {plan['sample_rate'] / 1.0e6:.3f} MS/s (SPS {plan['sps']}). {plan['note']}"
        )

    def get_f5oeo_constellation_value(self):
        return {
            "QPSK": "qpsk",
            "8PSK": "8psk",
            "16APSK": "16apsk",
            "32APSK": "32apsk",
        }.get(self.combo_mode.currentText(), "qpsk")

    def get_f5oeo_frame_value(self):
        return "short" if "Short" in self.combo_frame.currentText() else "long"

    def get_f5oeo_fec_value(self):
        gui_fec = self.combo_fec.currentText()
        return F5OEO_FEC_TOKEN_MAP.get(gui_fec, gui_fec.replace("/", ""))

    def get_f5oeo_tx_sample_rate_value(self, symbol_rate):
        return str(int(self.estimate_tx_sample_plan(symbol_rate, "pluto")["sample_rate"]))

    def get_file_input_options(self, low_sr_active=False):
        queue = "16384" if low_sr_active else "8192"
        opts = [
            "-thread_queue_size", queue,
            "-fflags", "+genpts+igndts+discardcorrupt",
            "-err_detect", "ignore_err",
        ]
        if low_sr_active:
            opts.extend(["-reinit_filter", "0"])
        return opts

    def get_camera_input_options(self, resolution, fps, low_sr_active=False):
        queue = "16384" if low_sr_active else "8192"
        opts = [
            "-thread_queue_size", queue,
            "-use_wallclock_as_timestamps", "1",
            "-fflags", "+genpts+discardcorrupt",
            "-f", "v4l2",
            "-framerate", str(int(fps)),
            "-video_size", resolution,
        ]
        if low_sr_active:
            opts.extend(["-reinit_filter", "0"])
        return opts

    def parse_custom_ffmpeg_input(self, text, label):
        try:
            args = shlex.split(text)
        except ValueError as exc:
            raise RuntimeError(f"Invalid {label} input override: {exc}") from exc
        if not args:
            raise RuntimeError(f"{label} input override is empty.")
        if "-i" not in args:
            raise RuntimeError(f"{label} input override must include -i.")
        return args

    def build_webcam_video_input_args(self, resolution, fps, low_sr_active=False):
        custom = self.camera_custom_video_input.text().strip() if hasattr(self, "camera_custom_video_input") else ""
        if custom:
            return self.parse_custom_ffmpeg_input(custom, "webcam video")
        camera_device = self.get_selected_camera_device()
        if not camera_device or not os.path.exists(camera_device):
            raise RuntimeError("No webcam / camera device detected. Connect a webcam, refresh detection, or enter a custom FFmpeg webcam input override.")
        return self.get_camera_input_options(resolution, fps, low_sr_active=low_sr_active) + ["-i", camera_device]

    def build_webcam_audio_input_args(self, low_sr_active=False):
        custom = self.camera_custom_audio_input.text().strip() if hasattr(self, "camera_custom_audio_input") else ""
        queue = "16384" if low_sr_active else "8192"
        prefix = ["-thread_queue_size", queue, "-use_wallclock_as_timestamps", "1"]
        if custom:
            return prefix + self.parse_custom_ffmpeg_input(custom, "webcam microphone")
        source = self.get_selected_audio_source() if hasattr(self, "get_selected_audio_source") else {}
        args = list(source.get("args", [])) if isinstance(source, dict) else []
        if not args:
            raise RuntimeError("No microphone source is configured for webcam audio. Select one or enter a custom FFmpeg microphone input override.")
        return prefix + args

    def get_stream_scheme(self, stream_url):
        if not stream_url or "://" not in stream_url:
            return ""
        return stream_url.split("://", 1)[0].strip().lower()

    def normalize_external_stream_url(self, stream_url):
        if not stream_url:
            return stream_url
        lowered = stream_url.lower()
        if not lowered.startswith("udp://"):
            return stream_url
        base, sep, query = stream_url.partition("?")
        if not sep:
            return f"{base}?fifo_size=1000000&overrun_nonfatal=1&timeout=5000000"
        params = [item for item in query.split("&") if item]
        keys = {item.split("=", 1)[0].lower() for item in params if item}
        if "fifo_size" not in keys:
            params.append("fifo_size=1000000")
        if "overrun_nonfatal" not in keys:
            params.append("overrun_nonfatal=1")
        if "timeout" not in keys:
            params.append("timeout=5000000")
        return f"{base}?{'&'.join(params)}"

    def get_external_input_options(self, stream_url=None, verification_mode=False, low_sr_active=False):
        scheme = self.get_stream_scheme(stream_url)
        opts = [
            "-thread_queue_size", "32768" if low_sr_active else "16384",
            "-use_wallclock_as_timestamps", "1",
            "-fflags", "+genpts+igndts+discardcorrupt",
            "-flags", "+low_delay",
            "-err_detect", "ignore_err",
            "-max_delay", "500000",
            "-analyzeduration", "10000000",
            "-probesize", "15000000",
        ]
        if low_sr_active:
            opts.extend(["-max_interleave_delta", "0", "-reinit_filter", "0"])
        if scheme == "udp":
            opts.extend(["-f", "mpegts"])
        if scheme in {"srt", "tcp", "udp"}:
            opts.extend(["-rw_timeout", "5000000"])
        return opts

    def get_f5oeo_resync_sequence(self, frequency_hz, symbol_rate):
        attenuation = abs(float(self.get_selected_tx_gain_value()))
        tx_sample_rate = self.get_f5oeo_tx_sample_rate_value(symbol_rate)
        return [
            ("tx/dvbs2/sr", str(int(symbol_rate))),
            ("tx/sr", tx_sample_rate),
            ("tx/frequency", str(int(frequency_hz))),
            ("tx/gain", f"{-attenuation:.2f}"),
            ("tx/nco", "0"),
            ("tx/dvbs2/fec", self.get_f5oeo_fec_value()),
            ("tx/dvbs2/constel", self.get_f5oeo_constellation_value()),
            ("tx/dvbs2/pilots", "1" if self.check_pilots.isChecked() else "0"),
            ("tx/dvbs2/frame", self.get_f5oeo_frame_value()),
        ]

    def build_f5oeo_tx_sequence(self, frequency_hz, symbol_rate):
        attenuation = abs(float(self.get_selected_tx_gain_value()))
        tx_sample_rate = self.get_f5oeo_tx_sample_rate_value(symbol_rate)
        return [
            ("tx/gain", f"{-attenuation:.2f}"),
            ("tx/frequency", str(int(frequency_hz))),
            ("tx/dvbs2/sr", str(int(symbol_rate))),
            ("tx/sr", tx_sample_rate),
            ("tx/nco", "0"),
            ("tx/dvbs2/fec", self.get_f5oeo_fec_value()),
            ("tx/dvbs2/constel", self.get_f5oeo_constellation_value()),
            ("tx/dvbs2/pilots", "1" if self.check_pilots.isChecked() else "0"),
            ("tx/dvbs2/frame", self.get_f5oeo_frame_value()),
        ]

    def publish_f5oeo_sequence(self, sequence, settle_delay=0.08, log_each=False):
        results = []
        for suffix, payload in sequence:
            publish_result = self.publish_f5oeo_mqtt(suffix, payload)
            results.append(publish_result)
            if log_each:
                self.log_message(f"MQTT publish OK: {publish_result['topic']} = {publish_result['payload']}")
            if settle_delay > 0:
                time.sleep(settle_delay)
        return results

    def update_f5oeo_callsign_ui(self):
        if not hasattr(self, "input_pluto_topic"):
            return
        if self.selected_requires_plutodvb2() if hasattr(self, "combo_device") else False:
            self.input_pluto_topic.setPlaceholderText("Enter your station callsign / topic key (required for PlutoDVB2 F5OEO)")
            self.input_pluto_topic.setToolTip("Required for PlutoDVB2 F5OEO MQTT topics like cmd/pluto/<CALL>/tx/stream/mode.")
            self.label_f5oeo_callsign_state.setVisible(True)
            self.label_f5oeo_callsign_state.setText("PlutoDVB2 F5OEO needs the exact callsign/topic key configured in the firmware.")
        elif self.selected_requires_pluto_0303() if hasattr(self, "combo_device") else False:
            self.input_pluto_topic.setPlaceholderText("Enter your station callsign (optional for overlays/service labels)")
            self.input_pluto_topic.setToolTip("Optional for the Pluto F5UII 0303/2402 path. MQTT control uses the documented plutodvb/subvar topics instead of cmd/pluto/<CALL>/...")
            self.label_f5oeo_callsign_state.setVisible(True)
            self.label_f5oeo_callsign_state.setText("0303/2402 control path is separate and uses plutodvb/subvar MQTT topics on port 8883.")
        else:
            self.input_pluto_topic.setPlaceholderText("Enter your station callsign")
            self.input_pluto_topic.setToolTip("Optional station callsign used for overlays and service naming.")
            self.label_f5oeo_callsign_state.setVisible(False)
            self.label_f5oeo_callsign_state.setText("")
        self.update_callsign_field_state()
        self.update_tx_action_state()

    def publish_f5oeo_mqtt(self, suffix, payload):
        ok, reason = self.get_f5oeo_requirement_status()
        if not ok:
            raise RuntimeError(reason)
        ctx = self.build_f5oeo_mqtt_context()
        topic = f"cmd/pluto/{ctx['topic_key']}/{suffix}"
        mqtt_publish.single(topic, str(payload), hostname=ctx["host"], port=ctx["port"], qos=0, retain=False)
        return {
            "host": ctx["host"],
            "port": ctx["port"],
            "topic": topic,
            "payload": str(payload),
        }

    def publish_pluto_0303_mqtt(self, suffix, payload):
        ok, reason = self.get_f5oeo_requirement_status()
        if not ok:
            raise RuntimeError(reason)
        ctx = self.build_pluto_0303_mqtt_context()
        topic = f"{ctx['topic_base']}/{suffix}"
        mqtt_publish.single(topic, str(payload), hostname=ctx["host"], port=ctx["port"], qos=0, retain=False)
        return {"host": ctx["host"], "port": ctx["port"], "topic": topic, "payload": str(payload)}

    def get_pluto_0303_var_sequence(self, frequency_hz, symbol_rate):
        fec = self.get_f5oeo_fec_value()
        constellation = self.get_f5oeo_constellation_value()
        frame = self.get_f5oeo_frame_value()
        pilots = "true" if self.check_pilots.isChecked() else "false"
        return [
            ("subvar/frequency", str(int(frequency_hz))),
            ("subvar/freq", str(int(frequency_hz))),
            ("subvar/sr", str(int(symbol_rate))),
            ("subvar/fec", fec),
            ("subvar/constel", constellation),
            ("subvar/modulation", constellation.upper()),
            ("subvar/frame", frame),
            ("subvar/pilots", pilots),
            ("subvar/rolloff", self.combo_rolloff.currentText()),
        ]

    def publish_pluto_0303_sequence(self, sequence, settle_delay=0.06, log_each=False):
        results = []
        for suffix, payload in sequence:
            publish_result = self.publish_pluto_0303_mqtt(suffix, payload)
            results.append(publish_result)
            if log_each:
                self.log_message(f"0303 MQTT publish OK: {publish_result['topic']} = {publish_result['payload']}")
            if settle_delay > 0:
                time.sleep(settle_delay)
        return results

    def build_pluto_0303_runtime_hook(self, frequency_hz, symbol_rate):
        ctx = self.build_pluto_0303_mqtt_context()
        post_sequence = []
        post_label = "0303 runtime monitor"
        initial_delay = 0.0
        if self.should_enable_pluto_0303_runtime_resync():
            post_sequence = self.get_pluto_0303_var_sequence(frequency_hz, symbol_rate) + [("modulator/apply", "1")]
            post_label = "0303 runtime resync"
            initial_delay = 0.30
        return {
            "host": ctx["host"],
            "port": ctx["port"],
            "topic_base": ctx["topic_base"],
            "hook_mode": ctx["hook_mode"],
            "initial_delay": initial_delay,
            "step_delay": 0.05,
            "pre_sequence": [],
            "post_sequence": post_sequence,
            "post_label": post_label,
            "subscribe_topics": [
                "plutodvb/var",
                "plutodvb/var/#",
                "plutodvb/status/#",
                "plutodvb/buffer/#",
                "plutodvb/ts/#",
            ],
        }

    def prepare_pluto_0303_control(self, frequency_hz, symbol_rate):
        ctx = self.build_pluto_0303_mqtt_context()
        sample_plan = self.estimate_tx_sample_plan(symbol_rate, "pluto")
        sequence = self.get_pluto_0303_var_sequence(frequency_hz, symbol_rate)
        self.log_message(f"Pluto F5UII 0303/2402 selected: using the documented PlutoDVB MQTT control path on {ctx['host']}:{ctx['port']} with topic base {ctx['topic_base']}")
        self.log_message("0303 console diagnostics: console-only MQTT subscribe monitor enabled for plutodvb/var, plutodvb/status/#, plutodvb/buffer/#, and plutodvb/ts/#")
        self.log_message(f"0303 TX sync: freq={frequency_hz} Hz | sr={symbol_rate} S/s | local libiio sample_rate request={sample_plan['sample_rate']} S/s | sps={sample_plan['sps']} | strategy={sample_plan['strategy']} | fec={self.get_f5oeo_fec_value()} | frame={self.get_f5oeo_frame_value()} | pilots={'1' if self.check_pilots.isChecked() else '0'}")
        if not self.should_enable_pluto_0303_runtime_resync():
            self.log_message("0303 runtime MQTT reapply is disabled by default. Set DATV_PLUTO_0303_RUNTIME_RESYNC=1 only if your firmware needs a post-start modulator/apply reassert.")
        last_error = None
        for attempt in range(1, 4):
            try:
                self.publish_pluto_0303_sequence(sequence, settle_delay=0.04, log_each=(attempt == 1))
                apply_result = self.publish_pluto_0303_mqtt("modulator/apply", "1")
                self.log_message(f"0303 apply OK: {apply_result['topic']} = {apply_result['payload']}")
                time.sleep(0.35 + (attempt - 1) * 0.10)
                self.log_message("0303 handoff uses the dedicated plutodvb/subvar topic family and a modulator/apply request rather than the PlutoDVB2 pass-mode sequence.")
                return self.build_pluto_0303_runtime_hook(frequency_hz, symbol_rate)
            except Exception as exc:
                last_error = exc
                if attempt < 3:
                    self.log_message(f"0303 MQTT setup attempt {attempt}/3 failed: {exc}. Retrying...", True)
                    time.sleep(0.75)
        raise RuntimeError(
            "Unable to synchronize Pluto F5UII 0303/2402 settings via the documented PlutoDVB MQTT topics.\n"
            f"Host: {ctx['host']}\nPort: {ctx['port']}\nTopic base: {ctx['topic_base']}\nFrequency: {frequency_hz} Hz\nSymbolRate: {symbol_rate} S/s\nReason: {last_error}\n"
            "Check that the 0303/2402 firmware exposes the PlutoDVB MQTT broker on TCP 8883 and that the requested variables are supported by the active controller page."
        )

    def build_f5oeo_runtime_hook(self, frequency_hz, symbol_rate):
        if not self.should_enable_f5oeo_runtime_resync():
            return {}
        ctx = self.build_f5oeo_mqtt_context()
        resync_sequence = [
            ("tx/nco", "0"),
        ]
        return {
            "host": ctx["host"],
            "port": ctx["port"],
            "topic_key": ctx["topic_key"],
            "topic_base": ctx["topic_base"],
            "hook_mode": ctx["hook_mode"],
            "initial_delay": 0.22,
            "step_delay": 0.04,
            "pass_delay": 0.25,
            "mode_before": "pass",
            "mode_after": "pass",
            "pre_sequence": [],
            "post_sequence": resync_sequence,
            "post_label": "F5OEO runtime resync",
        }

    def prepare_f5oeo_passthrough(self, frequency_hz, symbol_rate):
        ctx = self.build_f5oeo_mqtt_context()
        sample_plan = self.estimate_tx_sample_plan(symbol_rate, "pluto")
        sample_rate = sample_plan["sample_rate"]
        tx_sequence = self.build_f5oeo_tx_sequence(frequency_hz, symbol_rate)
        resync_sequence = self.get_f5oeo_resync_sequence(frequency_hz, symbol_rate)
        self.log_message(
            f"PlutoDVB2 F5OEO selected: publishing TX context via MQTT on {ctx['host']}:{ctx['port']} and then requesting pass mode on {ctx['topic']}"
        )
        self.log_message(
            f"F5OEO TX sync: freq={frequency_hz} Hz | sr={symbol_rate} S/s | tx/sr={self.get_f5oeo_tx_sample_rate_value(symbol_rate)} S/s | local libiio sample_rate request={sample_rate} S/s | sps={sample_plan['sps']} | strategy={sample_plan['strategy']} | constel={self.get_f5oeo_constellation_value()} | fec={self.get_f5oeo_fec_value()} (GUI {self.combo_fec.currentText()}) | frame={self.get_f5oeo_frame_value()} | pilots={'1' if self.check_pilots.isChecked() else '0'}"
        )
        if self.combo_rolloff.currentText() not in {"0.20", "0.15"}:
            self.log_message(
                f"ℹ️ F5OEO public MQTT examples document DVB roll-off values 0.20/0.15. GUI roll-off {self.combo_rolloff.currentText()} stays active in the local modulator path, but is not mirrored over MQTT."
            )
        runtime_hook = self.build_f5oeo_runtime_hook(frequency_hz, symbol_rate)
        if not runtime_hook:
            self.log_message("F5OEO runtime MQTT reassert is disabled by default. Set DATV_F5OEO_RUNTIME_RESYNC=1 only if your firmware needs post-start pass-mode reasserts.")
        last_error = None
        for attempt in range(1, 4):
            try:
                self.publish_f5oeo_sequence(tx_sequence, settle_delay=0.03, log_each=(attempt == 1))
                self.log_message("F5OEO sequence: applied gain, frequency, DVB-S2 SR, tx/sr, and NCO before pass-mode request")
                time.sleep(0.25)
                publish_result = self.publish_f5oeo_mqtt("tx/stream/mode", "pass")
                self.log_message(f"MQTT pass request OK: {publish_result['topic']} = {publish_result['payload']}")
                time.sleep(0.28)
                self.publish_f5oeo_sequence(resync_sequence, settle_delay=0.04, log_each=(attempt == 1))
                self.log_message("F5OEO sequence: reasserted tx/dvbs2/sr, tx/sr, frequency, and NCO after pass-mode settle")
                time.sleep(0.12)
                publish_result = self.publish_f5oeo_mqtt("tx/stream/mode", "pass")
                self.log_message(f"MQTT pass reassert OK: {publish_result['topic']} = {publish_result['payload']}")
                settle = 0.45 + (attempt - 1) * 0.15
                self.log_message("F5OEO handoff uses an explicit set-rate -> pass -> delayed resync -> pass reassert sequence without pre-start mute")
                self.log_message(f"F5OEO handoff settle delay: {settle:.2f} s before libiio TX starts")
                time.sleep(settle)
                return runtime_hook
            except Exception as exc:
                last_error = exc
                if attempt < 3:
                    self.log_message(f"MQTT handoff attempt {attempt}/3 failed: {exc}. Retrying...", True)
                    time.sleep(0.75)
        raise RuntimeError(
            "Unable to synchronize PlutoDVB2 F5OEO TX settings and switch the firmware into pass mode via MQTT.\n"
            f"Host: {ctx['host']}\n"
            f"Port: {ctx['port']}\n"
            f"Topic: {ctx['topic']}\n"
            f"Frequency: {frequency_hz} Hz\n"
            f"SymbolRate: {symbol_rate} S/s\n"
            f"Local libiio sample_rate request: {sample_rate} S/s\n"
            f"Reason: {last_error}\n"
            "Check that the callsign / topic key matches the value configured in the PlutoDVB2 firmware, that MQTT TCP 1883 is reachable, and that the radio really entered pass mode before libiio TX starts."
        )

    def update_source_controls(self):
        source_type = self.source_combo.currentText() if hasattr(self, "source_combo") else "Video File"
        is_file = source_type == "Video File"
        is_camera = source_type == "Webcam / Camera"
        if hasattr(self, "file_row_widget"):
            self.file_row_widget.setVisible(is_file)
        if hasattr(self, "stream_row_widget"):
            self.stream_row_widget.setVisible(not is_file and not is_camera)
        if hasattr(self, "camera_row_widget"):
            self.camera_row_widget.setVisible(is_camera)
        if hasattr(self, "camera_hint"):
            camera_available = hasattr(self, "camera_device_combo") and self.camera_device_combo.count() > 0 and bool(self.camera_device_combo.itemData(0))
            self.camera_hint.setVisible(is_camera or camera_available)
        if hasattr(self, "camera_options_widget"):
            self.camera_options_widget.setVisible(is_camera)
        self.update_camera_probe_details()
        self.update_audio_controls()
        self.update_tx_action_state()

    def update_audio_controls(self):
        source_type = self.source_combo.currentText() if hasattr(self, "source_combo") else "Video File"
        webcam_mode = source_type == "Webcam / Camera"
        webcam_route_enabled = self.is_webcam_audio_route_enabled() if webcam_mode else False
        audio_supported = (not webcam_mode) or webcam_route_enabled
        if hasattr(self, "check_audio"):
            if not audio_supported and self.check_audio.isChecked() and webcam_mode and not webcam_route_enabled:
                self.check_audio.blockSignals(True)
                self.check_audio.setChecked(False)
                self.check_audio.blockSignals(False)
            self.check_audio.setEnabled(audio_supported)
            if not webcam_mode:
                self.check_audio.setToolTip("")
            elif webcam_route_enabled:
                self.check_audio.setToolTip("Webcam microphone routing is available. Keep this checkbox enabled to encode the routed microphone audio.")
            else:
                self.check_audio.setToolTip("Enable microphone routing in the webcam section or enter a custom FFmpeg microphone input string to use audio with the webcam source.")
        enabled = audio_supported and self.check_audio.isChecked()
        if hasattr(self, "combo_a_br"):
            self.combo_a_br.setEnabled(enabled)
        if hasattr(self, "combo_a_codec"):
            self.combo_a_codec.setEnabled(enabled)
        if hasattr(self, "camera_audio_source_combo"):
            mic_route_checked = hasattr(self, "check_camera_mic") and self.check_camera_mic.isChecked()
            self.camera_audio_source_combo.setEnabled(webcam_mode and mic_route_checked and not self.uses_webcam_custom_audio_override())
        if hasattr(self, "btn_refresh_audio_sources"):
            self.btn_refresh_audio_sources.setEnabled(webcam_mode)
        if hasattr(self, "camera_custom_audio_input"):
            self.camera_custom_audio_input.setEnabled(webcam_mode and hasattr(self, "check_camera_mic") and self.check_camera_mic.isChecked())
        if hasattr(self, "camera_audio_hint"):
            self.camera_audio_hint.setVisible(webcam_mode)
        self.update_tx_action_state()

    def update_device_controls(self):
        if not hasattr(self, "combo_device"):
            return
        backend = self.selected_backend_device()
        is_pluto = backend == "pluto"
        is_lime = backend == "lime"
        is_f5oeo = self.selected_requires_f5oeo()

        self.input_pluto_ip.setEnabled(is_pluto)
        self.input_pluto_topic.setEnabled(True)
        self.check_amp.setEnabled(backend == "hackrf")
        if hasattr(self, "combo_pluto_stop_policy"):
            self.combo_pluto_stop_policy.setEnabled(is_pluto)
        if hasattr(self, "label_pluto_stop_policy_hint"):
            self.label_pluto_stop_policy_hint.setVisible(is_pluto)
        if hasattr(self, "btn_pluto_refresh_info"):
            self.btn_pluto_refresh_info.setEnabled(is_pluto)
        if hasattr(self, "btn_pluto_reboot"):
            self.btn_pluto_reboot.setEnabled(is_pluto)
        if hasattr(self, "label_pluto_ports_info"):
            self.label_pluto_ports_info.clear()
            self.label_pluto_ports_info.setVisible(False)

        if is_pluto:
            self.label_tx_level.setText("Pluto TX Attenuation (dB):")
            self.label_tx_hint.setText("Pluto attenuation range: 0.00 to 89.75 dB")
            self.tx_level_spin.setRange(0.0, 89.75)
            self.tx_level_spin.setDecimals(2)
            self.tx_level_spin.setSingleStep(0.25)
            self.tx_level_spin.setSuffix(" dB")
            if self.tx_level_spin.value() > 89.75:
                self.tx_level_spin.setValue(10.0)
        elif is_lime:
            self.label_tx_level.setText("LimeSDR TX Gain (dB):")
            self.label_tx_hint.setText("LimeSDR mini gain range: 0.00 to 64.00 dB via SoapySDR")
            self.tx_level_spin.setRange(0.0, 64.0)
            self.tx_level_spin.setDecimals(2)
            self.tx_level_spin.setSingleStep(1.0)
            self.tx_level_spin.setSuffix(" dB")
            if self.tx_level_spin.value() > 64.0:
                self.tx_level_spin.setValue(40.0)
        else:
            self.label_tx_level.setText("HackRF TX Gain (dB):")
            self.label_tx_hint.setText("HackRFone gain range: 0 to 47 dB")
            self.tx_level_spin.setRange(0.0, 47.0)
            self.tx_level_spin.setDecimals(0)
            self.tx_level_spin.setSingleStep(1.0)
            self.tx_level_spin.setSuffix(" dB")
            if self.tx_level_spin.value() > 47.0:
                self.tx_level_spin.setValue(30.0)

        if hasattr(self, "label_pluto_stop_policy_hint") and is_pluto:
            policy = self.get_selected_pluto_stop_policy()
            if policy == "pass_only":
                self.label_pluto_stop_policy_hint.setText("Pass only leaves Pluto reachable after TX stops. PlutoDVB2 gets a final pass-mode request; stock Pluto simply stops the local stream.")
            elif policy == "mute":
                self.label_pluto_stop_policy_hint.setText("Mute uses libiio to set Pluto TX attenuation to 89.75 dB after STOP or GUI exit. PlutoDVB2 also gets a final pass-mode request when possible.")
            else:
                self.label_pluto_stop_policy_hint.setText("Reboot requests a Pluto restart after STOP or GUI exit. Choose this only if you want Pluto restarted automatically each time.")
        self.update_f5oeo_callsign_ui()
        self.update_rf_sample_rate_hint()
        self.update_tx_action_state()

    def prepare_probe_placeholders(self):
        self.rf_probe_pending = False
        self.encoder_probe_pending = True
        self._set_pending_status_label(self.label_lime_mini_status, "LimeSDR mini", "RF detection will start in background.")
        self._set_pending_status_label(self.label_hackrf_status, "HackRFone", "RF detection will start in background.")
        self._set_pending_status_label(self.label_pluto_adalm_default_status, "AdalmPluto", "RF detection will start in background.")
        self._set_pending_status_label(self.label_plutodvb2_f5oeo_status, "PlutoDVB2 F5OEO", "RF detection will start in background.")
        self._set_pending_status_label(self.label_pluto_0303_status, "Pluto F5UII 0303/2402", "RF detection will start in background.")
        if hasattr(self, "btn_refresh_detection"):
            self.btn_refresh_detection.setEnabled(True)
        if hasattr(self, "label_encoder_hint"):
            platform_label = self.detect_runtime_environment().get("label", "Linux")
            self.label_encoder_hint.setStyleSheet("color: #856404;")
            self.label_encoder_hint.setText(f"{platform_label}: probing FFmpeg encoder capabilities in background...")

    def _set_pending_status_label(self, label, prefix, detail=""):
        label.setText(f"{prefix}: probing...")
        label.setToolTip(detail or "")
        label.setStyleSheet("color: #856404; font-weight: bold;")

    def _set_status_label(self, label, prefix, ok, detail=""):
        color = "#27ae60" if ok else "#c0392b"
        state = "detected" if ok else "not detected"
        label.setText(f"{prefix}: {state}")
        label.setToolTip(detail or "")
        label.setStyleSheet(f"color: {color}; font-weight: bold;")

    def _join_probe_output(self, *parts):
        return "\n".join(part for part in parts if part).strip()

    def _short_probe_note(self, value, *, limit=180):
        snippet = (value or "").strip().replace("\n", " | ")
        if len(snippet) > limit:
            snippet = snippet[: limit - 3] + "..."
        return snippet

    def _text_has_any(self, text, tokens):
        lowered = (text or "").lower()
        return any(token in lowered for token in tokens)

    def _looks_like_pluto_iio_context(self, text):
        return self._text_has_any(text, [
            "ad9361-phy",
            "cf-ad9361",
            "xadc",
            "adalm-pluto",
            "pluto+",
            "plutoplus",
            "pluto rev",
            "pluto sdr",
        ])

    def identify_pluto_family_from_iio(self, pluto_uri, timeout=3):
        candidates = []
        iio_attr = shutil.which("iio_attr")
        if iio_attr is not None:
            candidates.extend([
                [iio_attr, "-u", pluto_uri, "-d", "ad9361-phy", "compatible"],
                [iio_attr, "-u", pluto_uri, "-C", "hw_model"],
                [iio_attr, "-u", pluto_uri, "-C", "compatible"],
            ])
        iio_info = shutil.which("iio_info")
        if iio_info is not None:
            candidates.append([iio_info, "-u", pluto_uri])

        observed = []
        for cmd in candidates:
            try:
                result = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)
            except Exception:
                continue
            combined = self._join_probe_output(result.stdout, result.stderr)
            if not combined:
                continue
            observed.append(combined)
            lowered = combined.lower()
            if any(token in lowered for token in ["pluto+", "pluto plus", "plutoplus"]):
                return {
                    "family": "pluto_plus",
                    "detail": "IIO identity: Pluto+",
                    "raw": combined,
                }
            if any(token in lowered for token in ["adalm-pluto", "adalm pluto", "pluto sdr", "pluto rev"]):
                return {
                    "family": "pluto_adalm",
                    "detail": "IIO identity: ADALM Pluto",
                    "raw": combined,
                }

        detail = "IIO identity query unavailable"
        if observed:
            detail = f"IIO identity inconclusive ({self._short_probe_note(observed[0])})"
        return {
            "family": "unknown",
            "detail": detail,
            "raw": "\n\n".join(observed),
        }

    def _probe_tcp_port(self, host, port, *, timeout=1.2, attempts=2):
        last_error = ""
        for _ in range(max(1, attempts)):
            try:
                import socket
                with socket.create_connection((host, port), timeout=timeout):
                    return True, f"TCP {port} reachable"
            except Exception as exc:
                last_error = str(exc)
        return False, (last_error or f"TCP {port} not reachable")

    def _probe_hackrf(self):
        hackrf_info = shutil.which("hackrf_info")
        if hackrf_info is None:
            return False, "hackrf tools missing"
        try:
            result = subprocess.run([hackrf_info], capture_output=True, text=True, timeout=3)
        except subprocess.TimeoutExpired:
            return False, "hackrf_info timeout"
        except Exception:
            return False, "hackrf_info probe failed"

        combined = self._join_probe_output(result.stdout, result.stderr)
        lowered = combined.lower()
        if result.returncode != 0:
            return False, self._short_probe_note(combined) or "hackrf_info probe failed"
        if any(token in lowered for token in ["no hackrf boards found", "no hackrf board found", "not found"]):
            return False, "hackrf_info found no device"
        if any(token in lowered for token in ["found hackrf", "board id number", "serial number"]):
            return True, "connected"
        return False, self._short_probe_note(combined) or "hackrf_info returned no device signature"

    def _probe_lime(self):
        soapy_find = shutil.which("SoapySDRUtil")
        lime_util = shutil.which("LimeUtil")
        if soapy_find is None and lime_util is None:
            return False, "SoapySDR/Lime tools missing"

        notes = []
        if soapy_find is not None:
            try:
                result = subprocess.run([soapy_find, "--find=driver=lime"], capture_output=True, text=True, timeout=4)
                combined = self._join_probe_output(result.stdout, result.stderr)
                lowered = combined.lower()
                if result.returncode == 0:
                    if any(token in lowered for token in ["no devices found", "no device found"]):
                        notes.append("SoapySDR found no Lime device")
                    elif any(token in lowered for token in ["found device", "limesdr-mini", "lime microsystems", "hardware = limesdr", "label = limesdr", "serial =", "media = usb"]):
                        return True, "connected via SoapySDR"
                    else:
                        notes.append(self._short_probe_note(combined) or "SoapySDR returned no Lime device signature")
                else:
                    notes.append(self._short_probe_note(combined) or "SoapySDR probe failed")
            except subprocess.TimeoutExpired:
                notes.append("SoapySDR probe timeout")
            except Exception:
                notes.append("SoapySDR probe failed")

        if lime_util is not None:
            try:
                result = subprocess.run([lime_util, "--find"], capture_output=True, text=True, timeout=4)
                combined = self._join_probe_output(result.stdout, result.stderr)
                lowered = combined.lower()
                if result.returncode == 0:
                    if any(token in lowered for token in ["no devices found", "no device found"]):
                        notes.append("LimeUtil found no device")
                    elif any(token in lowered for token in ["limesdr-mini", "lime microsystems", "device #", "serial number"]):
                        return True, "connected via LimeUtil"
                    else:
                        notes.append(self._short_probe_note(combined) or "LimeUtil returned no device signature")
                else:
                    notes.append(self._short_probe_note(combined) or "LimeUtil probe failed")
            except subprocess.TimeoutExpired:
                notes.append("LimeUtil probe timeout")
            except Exception:
                notes.append("LimeUtil probe failed")

        cleaned = [item for item in notes if item]
        return False, cleaned[0] if cleaned else "Lime probe failed"

    def probe_pluto_iio_stack(self, pluto_uri, pluto_host, pluto_port):
        details = []
        iio_tools_present = False

        iio_info = shutil.which("iio_info")
        if iio_info is not None:
            iio_tools_present = True
            try:
                result = subprocess.run([iio_info, "-u", pluto_uri], capture_output=True, text=True, timeout=3)
                combined = self._join_probe_output(result.stdout, result.stderr)
                if result.returncode == 0 and combined and self._looks_like_pluto_iio_context(combined):
                    return True, "libiio OK via iio_info"
                if result.returncode == 0 and combined:
                    details.append("iio_info reached a non-Pluto IIO context")
                else:
                    details.append("iio_info failed")
            except subprocess.TimeoutExpired:
                details.append("iio_info timeout")
            except Exception:
                details.append("iio_info probe failed")

        iio_attr = shutil.which("iio_attr")
        if iio_attr is not None:
            iio_tools_present = True
            attr_queries = [
                ("hw_model", [iio_attr, "-q", "-u", pluto_uri, "-C", "hw_model"], True),
                ("ad9361-phy", [iio_attr, "-q", "-u", pluto_uri, "-d", "ad9361-phy", "ensm_mode"], False),
            ]
            for label, cmd, require_pluto_text in attr_queries:
                try:
                    result = subprocess.run(cmd, capture_output=True, text=True, timeout=3)
                    combined = self._join_probe_output(result.stdout, result.stderr)
                    if result.returncode == 0 and combined:
                        if not require_pluto_text or self._text_has_any(combined, ["pluto", "adalm"]):
                            return True, f"libiio OK via iio_attr ({label})"
                    elif result.returncode != 0:
                        details.append(f"iio_attr {label} failed")
                except subprocess.TimeoutExpired:
                    details.append(f"iio_attr {label} timeout")
                    break
                except Exception:
                    details.append(f"iio_attr {label} probe failed")
                    break

        tcp_ok, tcp_detail = self._probe_tcp_port(pluto_host, pluto_port, timeout=1.0, attempts=1)
        if tcp_ok:
            if iio_tools_present:
                details.append(f"IIOD {tcp_detail} but Pluto identity was not confirmed")
            else:
                details.append(f"IIOD {tcp_detail} but iio_info/iio_attr is missing, so detection stays conservative")
        else:
            details.append(tcp_detail)

        cleaned = [item for item in details if item]
        return False, "; ".join(cleaned[:3]) if cleaned else f"check IP or IIOD TCP {pluto_port}"

    def _compute_rf_detection_snapshot(self, pluto_uri, pluto_host, pluto_port):
        hackrf_ok, hackrf_detail = self._probe_hackrf()
        lime_ok, lime_detail = self._probe_lime()

        pluto_iio_ok, pluto_iio_detail = self.probe_pluto_iio_stack(pluto_uri, pluto_host, pluto_port)
        mqtt1883_ok, mqtt1883_detail = self._probe_tcp_port(pluto_host, self.get_fixed_f5oeo_mqtt_port(), timeout=1.0, attempts=2)
        mqtt8883_ok, mqtt8883_detail = self._probe_tcp_port(pluto_host, self.get_fixed_pluto_0303_mqtt_port(), timeout=1.0, attempts=2)
        mqtt_ambiguous = mqtt1883_ok and mqtt8883_ok
        pluto_identity = self.identify_pluto_family_from_iio(pluto_uri, timeout=3) if pluto_iio_ok else {"family": "unknown", "detail": ""}
        family_detail = pluto_identity.get("detail", "")

        default_detail = f"{pluto_iio_detail}; default profile uses IIOD TCP {pluto_port}"
        if family_detail:
            default_detail += f"; {family_detail}"
        if mqtt_ambiguous:
            default_detail += "; both firmware MQTT ports are reachable, so firmware/default auto-detection is treated as ambiguous"
        elif mqtt1883_ok or mqtt8883_ok:
            default_detail += "; firmware MQTT detected, so the default Pluto profile is not auto-selected"

        f5oeo_detail = (
            f"{pluto_iio_detail}; MQTT TCP {self.get_fixed_f5oeo_mqtt_port()} {'reachable' if mqtt1883_ok else 'not reachable'}"
            f" ({mqtt1883_detail}); firmware detection is port-based"
        )
        if family_detail:
            f5oeo_detail += f"; {family_detail}"
        if mqtt_ambiguous:
            f5oeo_detail += "; MQTT 1883 and 8883 are both reachable, so F5OEO vs 0303 remains ambiguous"

        fw0303_detail = (
            f"{pluto_iio_detail}; MQTT TCP {self.get_fixed_pluto_0303_mqtt_port()} {'reachable' if mqtt8883_ok else 'not reachable'}"
            f" ({mqtt8883_detail}); firmware detection is port-based"
        )
        if family_detail:
            fw0303_detail += f"; {family_detail}"
        if mqtt_ambiguous:
            fw0303_detail += "; MQTT 1883 and 8883 are both reachable, so F5OEO vs 0303 remains ambiguous"

        return {
            "status": {
                "hackrf": hackrf_ok,
                "lime_mini": lime_ok,
                "pluto_adalm_default": pluto_iio_ok and not mqtt1883_ok and not mqtt8883_ok,
                "plutodvb2_f5oeo": pluto_iio_ok and mqtt1883_ok and not mqtt8883_ok,
                "pluto_0303_2402": pluto_iio_ok and mqtt8883_ok and not mqtt1883_ok,
            },
            "details": {
                "hackrf": hackrf_detail,
                "lime_mini": lime_detail,
                "pluto_adalm_default": default_detail,
                "plutodvb2_f5oeo": f5oeo_detail,
                "pluto_0303_2402": fw0303_detail,
            },
        }

    def _apply_rf_detection_snapshot(self, snapshot):
        status = snapshot.get("status", {})
        details = snapshot.get("details", {})
        self.device_status.update({
            "hackrf": bool(status.get("hackrf", False)),
            "lime_mini": bool(status.get("lime_mini", False)),
            "pluto_adalm_default": bool(status.get("pluto_adalm_default", False)),
            "plutodvb2_f5oeo": bool(status.get("plutodvb2_f5oeo", False)),
            "pluto_0303_2402": bool(status.get("pluto_0303_2402", False)),
        })
        self._set_status_label(self.label_lime_mini_status, "LimeSDR mini", self.device_status["lime_mini"], details.get("lime_mini", ""))
        self._set_status_label(self.label_hackrf_status, "HackRFone", self.device_status["hackrf"], details.get("hackrf", ""))
        self._set_status_label(self.label_pluto_adalm_default_status, "AdalmPluto", self.device_status["pluto_adalm_default"], details.get("pluto_adalm_default", ""))
        self._set_status_label(self.label_plutodvb2_f5oeo_status, "PlutoDVB2 F5OEO", self.device_status["plutodvb2_f5oeo"], details.get("plutodvb2_f5oeo", ""))
        self._set_status_label(self.label_pluto_0303_status, "Pluto F5UII 0303/2402", self.device_status["pluto_0303_2402"], details.get("pluto_0303_2402", ""))
        self.update_device_controls()

    def detect_rf_devices(self):
        if self.rf_probe_worker is not None and self.rf_probe_worker.isRunning():
            self.rf_detect_timer.start()
            return
        pluto_uri = self.get_pluto_uri()
        pluto_host = self.input_pluto_ip.text().strip() or "192.168.2.1"
        pluto_port = self.get_fixed_pluto_iiod_port()
        self.rf_probe_pending = True
        if hasattr(self, "btn_refresh_detection"):
            self.btn_refresh_detection.setEnabled(False)
        pending_detail = f"Checking HackRF, LimeSDR Mini, and Pluto on {pluto_host} in the background."
        self._set_pending_status_label(self.label_lime_mini_status, "LimeSDR mini", pending_detail)
        self._set_pending_status_label(self.label_hackrf_status, "HackRFone", pending_detail)
        self._set_pending_status_label(self.label_pluto_adalm_default_status, "AdalmPluto", pending_detail)
        self._set_pending_status_label(self.label_plutodvb2_f5oeo_status, "PlutoDVB2 F5OEO", pending_detail)
        self._set_pending_status_label(self.label_pluto_0303_status, "Pluto F5UII 0303/2402", pending_detail)
        worker = BackgroundTaskWorker("rf_probe", lambda: self._compute_rf_detection_snapshot(pluto_uri, pluto_host, pluto_port))
        worker.result_signal.connect(self.handle_background_task_result)
        worker.error_signal.connect(self.handle_background_task_error)
        worker.finished.connect(lambda: self._clear_background_worker("rf_probe"))
        self.rf_probe_worker = worker
        worker.start()

    def start_background_encoder_probe(self, refresh=False):
        if self.encoder_probe_worker is not None and self.encoder_probe_worker.isRunning():
            return
        self.encoder_probe_pending = True
        if hasattr(self, "label_encoder_hint"):
            platform_label = self.detect_runtime_environment().get("label", "Linux")
            self.label_encoder_hint.setStyleSheet("color: #856404;")
            self.label_encoder_hint.setText(f"{platform_label}: probing FFmpeg encoder capabilities in background...")
        worker = BackgroundTaskWorker("encoder_probe", lambda: self.get_encoder_runtime_status(refresh=refresh))
        worker.result_signal.connect(self.handle_background_task_result)
        worker.error_signal.connect(self.handle_background_task_error)
        worker.finished.connect(lambda: self._clear_background_worker("encoder_probe"))
        self.encoder_probe_worker = worker
        worker.start()

    def _clear_background_worker(self, task_name):
        if task_name == "rf_probe":
            self.rf_probe_worker = None
        elif task_name == "encoder_probe":
            self.encoder_probe_worker = None

    def handle_background_task_result(self, task_name, result):
        if task_name == "rf_probe":
            self.rf_probe_pending = False
            if hasattr(self, "btn_refresh_detection"):
                self.btn_refresh_detection.setEnabled(True)
            self._apply_rf_detection_snapshot(result if isinstance(result, dict) else {})
            return
        if task_name == "encoder_probe":
            self.encoder_probe_pending = False
            if isinstance(result, dict):
                self.encoder_runtime_cache = dict(result)
            self.refresh_encoder_option_styles()
            self.update_video_encoder_controls()
            self.update_stream_guidance()
            self.update_tx_action_state()
            return

    def handle_background_task_error(self, task_name, detail):
        if task_name == "rf_probe":
            self.rf_probe_pending = False
            if hasattr(self, "btn_refresh_detection"):
                self.btn_refresh_detection.setEnabled(True)
            self.device_status.update({
                "hackrf": False,
                "lime_mini": False,
                "pluto_adalm_default": False,
                "plutodvb2_f5oeo": False,
                "pluto_0303_2402": False,
            })
            self._set_status_label(self.label_lime_mini_status, "LimeSDR mini", False, detail)
            self._set_status_label(self.label_hackrf_status, "HackRFone", False, detail)
            self._set_status_label(self.label_pluto_adalm_default_status, "AdalmPluto", False, detail)
            self._set_status_label(self.label_plutodvb2_f5oeo_status, "PlutoDVB2 F5OEO", False, detail)
            self._set_status_label(self.label_pluto_0303_status, "Pluto F5UII 0303/2402", False, detail)
            self.update_device_controls()
            self.log_message(f"RF detection failed: {detail}", True)
            return
        if task_name == "encoder_probe":
            self.encoder_probe_pending = False
            self.encoder_runtime_cache = {}
            self.refresh_encoder_option_styles()
            if hasattr(self, "label_encoder_hint"):
                self.label_encoder_hint.setStyleSheet("color: #c0392b;")
                self.label_encoder_hint.setText(f"Encoder probe failed: {detail}")
            self.log_message(f"Encoder probe failed: {detail}", True)
            self.update_video_encoder_controls()
            self.update_stream_guidance()
            self.update_tx_action_state()
            return

    def get_overlay_filter(self):
        call = self.get_effective_overlay_text()
        if not self.check_overlay.isChecked() or not call:
            return ""
        fontsize = self.font_size.currentText()
        color = self.combo_color.currentText()
        
        pos_map = {
            "Top Left": "x=10:y=10",
            "Top Right": "x=w-tw-10:y=10",
            "Bottom Left": "x=10:y=h-th-10",
            "Bottom Right": "x=w-tw-10:y=h-th-10"
        }
        
        position = pos_map.get(self.combo_position.currentText(), "x=10:y=10")

        text = call.replace("'", r"\'")
        return f",drawtext=text='{text}':fontcolor={color}:fontsize={fontsize}:{position}:box=1:boxcolor=black@0.5"
        
    def start_transmission(self):
        blockers = self.get_tx_blockers()
        if blockers:
            QMessageBox.warning(self, "Cannot Start Transmission", "\n\n".join(blockers))
            return
        ok, reason = self.get_f5oeo_requirement_status()
        if not ok:
            self.update_callsign_field_state()
            QMessageBox.warning(self, "Station Callsign Required", reason)
            return
        if not self.check_dependencies():
            if self.selected_requires_f5oeo() and mqtt_publish is None:
                QMessageBox.warning(self, "Missing F5OEO Dependency", "F5OEO firmware control requires paho-mqtt.\n\nRun install.sh or install requirements.txt, then restart the GUI.")
            else:
                QMessageBox.warning(self, "Missing Dependencies", "Required binaries not found!\n\nPlease compile dvbs2_tx first:\nmake\n\nor\ncmake -S . -B build && cmake --build build")
            return

        try:
            plan = self.build_ffmpeg_command()
            if not plan.get("ok"):
                QMessageBox.warning(self, "Encoder / Stream Settings", plan.get("reason", "Unable to build FFmpeg command."))
                return

            bitrate_plan = plan["bitrate_plan"]
            if self.check_audio.isChecked() and bitrate_plan["requested_audio"] != plan["audio_br"]:
                self.log_message(f"⚠️ Adjusted audio bitrate from {bitrate_plan['requested_audio']}k to {plan['audio_br']}k to fit the DVB-S2 TS budget.")
            if bitrate_plan["spare"] > 0:
                self.log_message(f"ℹ️ Reserved {bitrate_plan['mux_overhead']}k for TS/PES overhead and left {bitrate_plan['spare']}k headroom.")
            if plan["validation"].get("severity") != "ok":
                self.log_message(f"⚠️ {plan['validation']['message']}")

            sr_ksps = self.combo_sr.currentText()
            sr_sps = int(float(sr_ksps) * 1000)
            modcod = self.get_modcod_string()

            try:
                freq_hz = int(float(self.input_freq.text()) * 1_000_000)
            except ValueError:
                self.log_message("Invalid frequency", True)
                return

            device_key = self.selected_device_key()
            backend_device = self.selected_backend_device()
            if not self.device_status.get(device_key, False):
                QMessageBox.warning(self, "Device Not Detected", f"Selected device not detected: {self.combo_device.currentText()}")
                return

            gain = self.get_selected_tx_gain_value()
            amp = 1 if (backend_device == "hackrf" and self.check_amp.isChecked()) else 0

            root_dir = os.path.dirname(os.path.abspath(__file__))
            tx_path = os.path.join(root_dir, "dvbs2_tx")
            if not os.path.exists(tx_path):
                tx_path = os.path.join(root_dir, "build", "dvbs2_tx")
            if not os.path.exists(tx_path):
                tx_path = os.path.join(root_dir, "src", "dvbs2_tx")
            if not os.path.exists(tx_path):
                tx_path = "/usr/local/bin/dvbs2_tx"

            tx_cmd = [
                tx_path, modcod, str(freq_hz), str(sr_sps), str(gain), str(amp),
                self.combo_rolloff.currentText(), "1" if self.check_pilots.isChecked() else "0",
                str(self.spin_goldcode.value()), backend_device
            ]
            tx_env = os.environ.copy()
            sample_plan = self.estimate_tx_sample_plan(sr_sps, backend_device)
            tx_env["DATV_TX_SAMPLE_RATE_MODE"] = self.get_sample_rate_strategy_key()
            tx_env["DATV_LOW_SR_STABILIZE"] = "1" if plan.get("low_sr_active") else "0"
            if plan["source_type"] != "Video File":
                tx_env["DATV_INPUT_STALL_MS"] = "5000"
            else:
                tx_env.pop("DATV_INPUT_STALL_MS", None)
            if backend_device == "pluto":
                tx_cmd.append(self.get_pluto_uri())
            elif backend_device == "lime":
                tx_cmd.append(self.get_lime_device_args())

            self.log_message("=" * 60)
            self.log_message(f"MODCOD: {modcod}")
            if backend_device == "pluto":
                gain_desc = f"{gain:.2f} dB atten"
            elif backend_device == "lime":
                gain_desc = f"{gain:.2f} dB gain"
            else:
                gain_desc = f"{int(gain)} dB gain"
            self.log_message(f"Freq: {freq_hz/1e6:.3f} MHz | SR: {sr_sps/1000:.0f} kS/s | Gold: {self.spin_goldcode.value()}")
            if backend_device == "pluto":
                self.log_message(
                    f"Local Pluto sample_rate request: {sample_plan['sample_rate'] / 1.0e6:.3f} MS/s (SPS {sample_plan['sps']}, strategy {sample_plan['strategy']})"
                )
                if sample_plan.get("note"):
                    self.log_message(f"Sample-rate plan: {sample_plan['note']}")
            elif backend_device == "lime":
                self.log_message(
                    f"Local Lime sample_rate request: {sample_plan['sample_rate'] / 1.0e6:.3f} MS/s (SPS {sample_plan['sps']}, SoapySDR)"
                )
            else:
                self.log_message(
                    f"Local HackRF sample_rate request: {sample_plan['sample_rate'] / 1.0e6:.3f} MS/s (SPS {sample_plan['sps']})"
                )
            self.log_message(f"Device: {self.combo_device.currentText()} | TX level: {gain_desc}")
            if plan["source_type"] == "Video File":
                self.log_message(f"Source: file | {plan['source_desc']}")
            else:
                self.log_message(f"Source: external stream | {plan['source_desc']}")
            if backend_device == "pluto":
                self.log_message(f"Pluto IP: {self.input_pluto_ip.text().strip() or '192.168.2.1'} | IIOD probe port: {self.get_fixed_pluto_iiod_port()}")
                if self.selected_requires_plutodvb2():
                    self.log_message(f"PlutoDVB2 MQTT: {self.get_fixed_f5oeo_mqtt_port()} | Topic key: {self.get_f5oeo_topic_key() or '<missing>'}")
                elif self.selected_requires_pluto_0303():
                    self.log_message(f"Pluto 0303 MQTT: {self.get_fixed_pluto_0303_mqtt_port()} | Topic base: plutodvb/subvar")
            elif backend_device == "lime":
                self.log_message(f"LimeSDR device args: {self.get_lime_device_args()} | Soapy detection required")
            self.log_message(f"FPS: {plan['fps']} | Resolution: {plan['resolution']} | Encoder: {plan['actual_encoder']} | Requested: {plan['requested_encoder']}")
            encoder_kind = "hardware" if plan.get("encoder_info", {}).get("hardware") else "software"
            if plan['requested_encoder'] == "Auto":
                self.log_message(f"Auto resolved to {plan['actual_encoder']} ({encoder_kind}) | {plan.get('encoder_info', {}).get('reason', 'runtime probe result')} ")
            elif plan.get("encoder_info", {}).get("fallback"):
                self.log_message(f"Requested encoder fallback: {plan['requested_encoder']} -> {plan['actual_encoder']} ({encoder_kind}) | {plan.get('encoder_info', {}).get('reason', 'runtime probe result')} ")
            self.log_message(f"Preset: {plan['preset']} | Tune: {plan['tune']} | Audio codec: {plan['audio_desc']}")
            self.log_message(f"Service Name: {plan['service_name']} | Provider: {plan['service_provider']}")
            self.log_message(f"TS: {plan['net_br_kbps']:.0f} kbps | ES budget: {bitrate_plan['payload_budget']} kbps | Video: {plan['video_br']} kbps | Audio: {plan['audio_br']} kbps")
            if plan.get("low_sr_active"):
                self.log_message(f"Low SR stabilization: active below 250 kS/s | selected encoder {plan['actual_encoder']} kept in place.")
            if plan.get("audio_adjustment_reason"):
                self.log_message(f"Audio guard: {plan['audio_adjustment_reason']}")
            self.log_message("=" * 60)

            if self.debug_mode:
                self.log_message(f"[DEBUG] FFmpeg command: {' '.join(plan['ffmpeg_cmd'])}")
                self.log_message(f"[DEBUG] TX command: {' '.join(tx_cmd)}")

            runtime_hook = None
            if self.selected_requires_plutodvb2():
                try:
                    runtime_hook = self.prepare_f5oeo_passthrough(freq_hz, sr_sps)
                except Exception as exc:
                    detail = str(exc)
                    self.log_message(detail, True)
                    QMessageBox.warning(self, "PlutoDVB2 F5OEO Handoff Failed", detail)
                    return
            elif self.selected_requires_pluto_0303():
                try:
                    runtime_hook = self.prepare_pluto_0303_control(freq_hz, sr_sps)
                except Exception as exc:
                    detail = str(exc)
                    self.log_message(detail, True)
                    QMessageBox.warning(self, "Pluto F5UII 0303/2402 Setup Failed", detail)
                    return

            self.start_time = time.time()
            self.last_frames = 0
            self._ignore_next_finished_signal = False

            self.process_worker = ProcessWorker(plan["ffmpeg_cmd"], tx_cmd, runtime_hook, tx_env=tx_env)
            self.process_worker.debug = self.debug_mode
            self.process_worker.output_signal.connect(lambda x: self.log_message(x))
            self.process_worker.error_signal.connect(lambda x: self.log_message(x, True))
            self.process_worker.status_signal.connect(self.update_performance_stats)
            self.process_worker.finished_signal.connect(self.on_process_finished)
            self.process_worker.start()

            self.btn_verify.setEnabled(False)
            self.btn_start.setEnabled(False)
            self.btn_stop.setEnabled(True)
            self.update_tx_action_state()
            self.watchdog_timer.start(5000)
            self.status_bar.showMessage(f"🔴 TX: {modcod} @ {sr_ksps} kS/s")
            self.log_message("✅ Transmission started")

        except Exception as e:
            self.log_message(f"Failed: {str(e)}", True)

    def update_performance_stats(self, stats):
        frames = stats.get('frames', 0)
        self.frame_counter.setText(str(frames))
        
        ts_kbps = stats.get('ts_kbps', 0.0)
        if ts_kbps > 0:
            self.bitrate_label.setText(f"{ts_kbps:.0f} kbps")
        elif self.start_time and frames > 0:
            elapsed = time.time() - self.start_time
            if elapsed > 0:
                bitrate = (frames * max(self.update_bitrate_estimate(), 1)) / max(frames, 1)
                self.bitrate_label.setText(f"{bitrate:.0f} kbps")

        buffer_level = stats.get('buffer_level', 0)
        percent = self.calculate_buffer_percent(buffer_level, stats.get('buffer_percent'))
        self.buffer_level.setText(f"{percent:.0f}%")
        self.buffer_bar.setValue(int(percent))
        
        underflows = stats.get('underflows', 0)
        self.underflow_label.setText(str(underflows))
            
    def on_process_finished(self, returncode):
        if getattr(self, "_ignore_next_finished_signal", False):
            self._ignore_next_finished_signal = False
            return
        if self.process_worker:
            if self.process_worker.ffmpeg_timeout_reported:
                self.log_message("Final stop cause: FFmpeg/network timeout.", True)
            elif self.process_worker.transport_stop_reason:
                self.log_message(f"Final stop cause: {self.process_worker.transport_stop_reason}", True)
                if self.process_worker.transport_stop_detail:
                    self.log_message(f"Final detail: {self.process_worker.transport_stop_detail}", True)
        if returncode != 0:
            self.log_message(f"Process exited with code {returncode}", True)
        self.stop_transmission(apply_policy=True)

    def _stop_background_workers(self, force=False):
        if hasattr(self, "rf_detect_timer"):
            self.rf_detect_timer.stop()
        for attr_name in ("rf_probe_worker", "encoder_probe_worker"):
            worker = getattr(self, attr_name, None)
            if worker is None:
                continue
            try:
                worker.requestInterruption()
            except Exception:
                pass
            try:
                worker.quit()
            except Exception:
                pass
            if force and not worker.wait(150):
                try:
                    worker.terminate()
                except Exception:
                    pass
                worker.wait(500)
            setattr(self, attr_name, None)

    def stop_transmission(self, apply_policy=True, closing=False):
        had_worker = bool(self.process_worker)
        if self.process_worker:
            self._ignore_next_finished_signal = True
            worker = self.process_worker
            self.process_worker = None
            worker.stop()
            wait_ms = 1500 if closing else 3000
            if not worker.wait(wait_ms):
                if closing:
                    self.log_message("TX worker did not stop within the shutdown grace period; forcing thread termination.", True)
                    try:
                        worker.terminate()
                    except Exception:
                        pass
                    worker.wait(1000)
                else:
                    self.log_message("TX worker is still shutting down in the background.", True)

        self.btn_verify.setEnabled(True)
        self.btn_stop.setEnabled(False)
        self.watchdog_timer.stop()
        if hasattr(self, "stats_timer"):
            self.stats_timer.stop()
        self.status_bar.showMessage("⚫ TX Stopped")
        self.update_tx_action_state()

        policy = self.get_selected_pluto_stop_policy()
        should_apply_policy = apply_policy and (had_worker or policy in {"mute", "reboot"})
        if should_apply_policy:
            try:
                if closing and policy == "pass_only":
                    self.log_message("GUI exit: local TX stopped without an extra Pluto pass-only reassert to keep shutdown responsive.")
                else:
                    self.apply_pluto_stop_exit_policy("exit" if closing else "stop")
            except Exception as exc:
                self.log_message(f"Pluto stop/exit policy failed: {exc}", True)
                self.set_pluto_info_values(status=f"Pluto stop/exit policy failed: {exc}", status_color="#c0392b")

        if had_worker:
            self.log_message("Transmission stopped")

    def check_process_health(self):
        if self.process_worker and not self.process_worker.isRunning():
            self.log_message("Process died unexpectedly!", True)
            self.stop_transmission(apply_policy=True)

    def closeEvent(self, event):
        if self._closing_in_progress:
            event.accept()
            return
        self._closing_in_progress = True
        self.save_persisted_settings()
        if hasattr(self, "watchdog_timer"):
            self.watchdog_timer.stop()
        if hasattr(self, "stats_timer"):
            self.stats_timer.stop()
        self._stop_background_workers(force=True)
        self.stop_transmission(apply_policy=True, closing=True)
        event.accept()

if __name__ == "__main__":
    app = QApplication(sys.argv)
    app.setStyle('Fusion')
    gui = Dvbs2TxGui()
    gui.show()
    sys.exit(app.exec())
