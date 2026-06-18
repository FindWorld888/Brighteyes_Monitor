#!/usr/bin/env python3
"""
BrightEyes Vision Platform 控制面板
Web-based GUI: 配置管理 / 进程控制 / 线库标定 / 模型转换
"""

import os
import sys
import json
import shlex
import socket
import subprocess
import threading
import time
import logging
import shutil
import zipfile
import pty
try:
    import paho.mqtt.client as paho_mqtt
    _HAS_PAHO = True
except ImportError:
    _HAS_PAHO = False
import select
import psutil
import signal
from pathlib import Path
from flask import Flask, jsonify, request, render_template, Response, stream_with_context, send_file

# ─── 基础设置 ─────────────────────────────────────────────────────────────────

if getattr(sys, 'frozen', False):
    # PyInstaller onefile: resources are extracted under _MEIPASS, runtime files should stay beside executable.
    SCRIPT_DIR = os.path.dirname(os.path.abspath(sys.executable))
    _BUNDLE_DIR = getattr(sys, '_MEIPASS', SCRIPT_DIR)
else:
    SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
    _BUNDLE_DIR = SCRIPT_DIR

CONFIG_FILE = os.path.join(SCRIPT_DIR, 'control_panel_config.json')
TEMPLATE_DIR = os.path.join(_BUNDLE_DIR, 'templates')
STATIC_DIR = os.path.join(_BUNDLE_DIR, 'static')

DEFAULT_CONFIG = {
    "base_dir": "/home/clousy/application/docker_test/brighteyes/VisionNavPlatform_web/usr/src/bevp5.0",
    "main_program_cmd": "./VP_SERVER",
    "main_program_name": "VP_SERVER",
    "calibration_program": "./tools/calib",
    "model_conversion_program": "./model/onnx2trt",
    "glob_config_path": "./config/glob_config.json",
    "camera_config_path": "./config/camera_config.json",
    "memory_guard_enabled": False,
    "memory_limit_mb": 4096,
    "memory_guard_check_sec": 5,
    "memory_guard_restart_cooldown_sec": 30,
    "memory_guard_max_restarts_window": 5,
    "memory_guard_window_sec": 600,
    "cpu_monitor_enabled": True,
    "cpu_warn_threshold": 85,
    "cpu_check_interval_sec": 30,
    "network_monitor_enabled": True,
    "network_monitor_port": 8545,
    "network_check_interval_sec": 30,
    "network_disconnect_threshold": 3,
    "gpu_monitor_enabled": True,
    "gpu_warn_threshold": 90,
    "gpu_mem_warn_percent": 90,
    "gpu_check_interval_sec": 30,
    "resource_log_cooldown_sec": 120,
    "autostart_enabled": False,
    "camera_monitor_enabled": False,
    "camera_monitor_interval_sec": 10,
    "process_log_persist_enabled": True,
    "process_log_line_threshold": 2000,
    "process_log_cleanup_days": 7,
    "process_log_maintenance_sec": 10,
    "process_log_dir": "./log/control_panel/process_output",
    "control_panel_heartbeat_sec": 120,
    "main_hang_timeout_sec": 300,
    "web_host": "0.0.0.0",
    "web_port": 8888,
    "lt_sample_interval_sec": 1800,
    "lt_cpu_delta_thresh": 10.0,
    "lt_mem_delta_mb_thresh": 100.0,
    "lt_adaptive_min_sec": 120,
    "lt_max_records": 4000,
}

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
    ],
)
logger = logging.getLogger(__name__)
logging.getLogger('werkzeug').setLevel(logging.ERROR)
_file_log_handler: logging.Handler | None = None

app = Flask(__name__, template_folder=TEMPLATE_DIR, static_folder=STATIC_DIR, static_url_path='/static')
app.logger.disabled = True

# ─── 配置加载 ─────────────────────────────────────────────────────────────────

_config: dict = {}


def load_panel_config() -> dict:
    global _config
    _config = dict(DEFAULT_CONFIG)
    if os.path.exists(CONFIG_FILE):
        try:
            with open(CONFIG_FILE, 'r', encoding='utf-8') as f:
                _config.update(json.load(f))
            # 兼容旧配置：自动迁移旧的进程日志目录到 base_dir/log/control_panel/process_output
            if _config.get('process_log_dir') in ('./control_panel_process_logs', 'control_panel_process_logs'):
                _config['process_log_dir'] = DEFAULT_CONFIG['process_log_dir']
                save_panel_config({'process_log_dir': DEFAULT_CONFIG['process_log_dir']})
        except Exception as e:
            logger.warning("加载控制面板配置失败: %s", e)
    return _config


def save_panel_config(data: dict):
    global _config
    _config.update(data)
    with open(CONFIG_FILE, 'w', encoding='utf-8') as f:
        json.dump(_config, f, indent=2, ensure_ascii=False)


def cfg(key: str, default=None):
    return _config.get(key, DEFAULT_CONFIG.get(key, default))


def cfg_path(key: str, default=None) -> str:
    """获取路径类配置项，若为相对路径则基于 base_dir 解析为绝对路径。"""
    val = cfg(key, default)
    if not val:
        return val
    p = Path(val).expanduser()
    if p.is_absolute():
        return str(p)
    base = Path(cfg('base_dir', SCRIPT_DIR)).expanduser().resolve()
    return str((base / p).resolve())


# ─── 进程管理 ─────────────────────────────────────────────────────────────────

_main_proc: subprocess.Popen | None = None
_calib_proc: subprocess.Popen | None = None
_proc_op_lock = threading.Lock()

# 缓存目标进程的 psutil.Process 对象，避免每次重建导致 cpu_percent() 始终返回 0
_cached_psutil_proc: psutil.Process | None = None
_cached_psutil_pid: int | None = None

_main_output: list[str] = []
_main_output_lock = threading.Lock()

_conv_proc: subprocess.Popen | None = None
_conv_output: list[str] = []
_conv_lock = threading.Lock()

_calib_output: list[str] = []
_calib_lock = threading.Lock()
_mem_guard_thread: threading.Thread | None = None
_main_log_maint_thread: threading.Thread | None = None
_main_log_pending: list[str] = []
_main_log_pending_lock = threading.Lock()
_main_log_cursor = 0
_main_log_last_cleanup_day = ''
_runtime_monitor_thread: threading.Thread | None = None
_main_last_output_ts = time.time()
_main_hang_logged = False
_mem_restart_events: list[float] = []
_last_cpu_check_at = 0.0
_last_net_check_at = 0.0
_last_gpu_check_at = 0.0
_network_disconnect_count = 0
_network_last_ok = True
_last_cpu_warn_ts = 0.0
_last_net_warn_ts = 0.0
_last_net_error_ts = 0.0
_last_gpu_warn_ts = 0.0
_external_output_notice_pid: int | None = None

# MQTT 客户端
_mqtt_client: object | None = None
_mqtt_node_id: str = 'node_test'

# 电源/关机监测
_power_events: list[dict] = []  # 记录断电/关机事件
_power_events_lock = threading.Lock()
_HEARTBEAT_FILE = os.path.join(SCRIPT_DIR, '.control_panel_heartbeat')
_POWER_LOG_FILE = os.path.join(SCRIPT_DIR, '.power_events.json')

# ─── 长期历史采样 ─────────────────────────────────────────────────────────────
_lt_history: list[dict] = []   # {'ts': float, 'cpu': float, 'mem': float}
_lt_history_lock = threading.Lock()
_lt_sampler_thread: threading.Thread | None = None
_lt_last_save_ts: float = 0.0


def _target_log_dir() -> Path:
    """目标程序日志根目录(base_dir/log)。"""
    return Path(cfg('base_dir', SCRIPT_DIR)).expanduser().resolve() / 'log'


def _control_panel_log_dir() -> Path:
    """控制面板日志目录(base_dir/log/control_panel)。"""
    return _target_log_dir() / 'control_panel'


def _control_panel_log_file() -> Path:
    return _control_panel_log_dir() / 'control_panel.log'


def _sync_external_output_notice(stats: dict | None = None):
    """外部已运行目标程序没有可接管的 stdout，给输出区补一条状态说明。"""
    global _external_output_notice_pid
    try:
        if stats is None:
            stats = _get_main_proc_stats()
        if not stats.get('running') or stats.get('source') != 'external':
            _external_output_notice_pid = None
            return

        pid = int(stats.get('pid') or 0)
        if pid <= 0 or _external_output_notice_pid == pid:
            return

        with _main_output_lock:
            if _external_output_notice_pid == pid:
                return
            if not _main_output:
                _main_output.append(f'[控制面板] 检测到目标程序已在外部运行 PID={pid}')
                _main_output.append('[控制面板] 该进程不是由当前控制面板启动，无法接管已有终端 stdout；资源统计和停止操作仍会同步到该进程。')
            _external_output_notice_pid = pid
    except Exception:
        pass


def _setup_file_logging():
    """把控制面板日志写入 base_dir/log/control_panel/control_panel.log。"""
    global _file_log_handler
    try:
        log_dir = _control_panel_log_dir()
        log_dir.mkdir(parents=True, exist_ok=True)
        log_file = _control_panel_log_file()

        if _file_log_handler is not None:
            root_logger = logging.getLogger()
            root_logger.removeHandler(_file_log_handler)
            _file_log_handler.close()
            _file_log_handler = None

        fh = logging.FileHandler(str(log_file), encoding='utf-8')
        fh.setLevel(logging.INFO)
        fh.setFormatter(logging.Formatter('%(asctime)s [%(levelname)s] %(message)s'))
        logging.getLogger().addHandler(fh)
        _file_log_handler = fh
        logger.info('控制面板日志文件: %s', log_file)
    except Exception as e:
        logger.warning('配置控制面板日志文件失败: %s', e)


def _safe_resolve_path(input_path: str | None) -> Path:
    """解析用户传入路径，空值时回退到 base_dir。"""
    base = Path(cfg('base_dir', SCRIPT_DIR)).expanduser().resolve()
    if not input_path:
        return base
    try:
        p = Path(input_path).expanduser()
        return p.resolve() if p.is_absolute() else (base / p).resolve()
    except Exception:
        return base


def _process_log_dir() -> Path:
    return Path(cfg_path('process_log_dir', './log/control_panel/process_output')).expanduser()


def _runtime_monitor_loop():
    """低频运行心跳 + 主程序退出/疑似卡死告警。"""
    global _main_hang_logged, _last_cpu_check_at, _last_net_check_at, _last_gpu_check_at
    global _network_disconnect_count, _network_last_ok
    global _last_cpu_warn_ts, _last_net_warn_ts, _last_gpu_warn_ts
    global _last_net_error_ts
    last_hb = 0.0
    while True:
        time.sleep(10)
        try:
            now = time.time()
            hb_sec = max(30, int(cfg('control_panel_heartbeat_sec', 120) or 120))
            hang_sec = max(60, int(cfg('main_hang_timeout_sec', 300) or 300))
            cooldown_sec = max(10, int(cfg('resource_log_cooldown_sec', 120) or 120))

            if now - last_hb >= hb_sec:
                running = _main_proc is not None and _main_proc.poll() is None
                if running and _main_proc is not None:
                    logger.info('控制面板运行正常: 目标程序运行中 pid=%d', _main_proc.pid)
                else:
                    ext_proc = _find_process_by_name(cfg('main_program_name', 'VP_SERVER'))
                    if ext_proc is not None:
                        try:
                            logger.info('控制面板运行正常: 目标程序运行中(外部启动) pid=%d', ext_proc.pid)
                        except (psutil.NoSuchProcess, psutil.AccessDenied):
                            logger.info('控制面板运行正常: 目标程序未运行')
                    else:
                        logger.info('控制面板运行正常: 目标程序未运行')
                last_hb = now

            if _main_proc is None or _main_proc.poll() is not None:
                _main_hang_logged = False
                _network_disconnect_count = 0
                _network_last_ok = True
                continue

            idle_sec = now - _main_last_output_ts
            if idle_sec < hang_sec or _main_hang_logged:
                pass
            else:
                status = 'unknown'
                try:
                    act_proc = _get_target_child_proc(_main_proc.pid)
                    proc = act_proc if act_proc is not None else psutil.Process(_main_proc.pid)
                    status = proc.status()
                except Exception:
                    pass
                logger.error('主程序疑似卡死: pid=%d, status=%s, %.0f秒无新输出', _main_proc.pid, status, idle_sec)
                _main_hang_logged = True

            # CPU 高占用日志
            cpu_check_sec = max(5, int(cfg('cpu_check_interval_sec', 30) or 30))
            if bool(cfg('cpu_monitor_enabled', True)) and (now - _last_cpu_check_at >= cpu_check_sec):
                _last_cpu_check_at = now
                try:
                    act_proc = _get_target_child_proc(_main_proc.pid)
                    proc = act_proc if act_proc is not None else psutil.Process(_main_proc.pid)
                    cpu = _safe_get_proc_cpu(proc)
                    cpu_warn = float(cfg('cpu_warn_threshold', 85) or 85)
                    if cpu > cpu_warn and (now - _last_cpu_warn_ts >= cooldown_sec):
                        _last_cpu_warn_ts = now
                        logger.warning('主程序CPU占用过高: %.1f%% (阈值 %.1f%%)', cpu, cpu_warn)
                except Exception:
                    pass

            # 端口连通性日志
            net_check_sec = max(5, int(cfg('network_check_interval_sec', 30) or 30))
            if bool(cfg('network_monitor_enabled', True)) and (now - _last_net_check_at >= net_check_sec):
                _last_net_check_at = now
                port = int(cfg('network_monitor_port', 8545) or 8545)
                ok = False
                try:
                    with socket.create_connection(('127.0.0.1', port), timeout=2):
                        ok = True
                except Exception:
                    ok = False

                if ok:
                    if not _network_last_ok:
                        logger.info('目标程序网络连接已恢复: 127.0.0.1:%d', port)
                    _network_last_ok = True
                    _network_disconnect_count = 0
                else:
                    _network_last_ok = False
                    _network_disconnect_count += 1
                    if now - _last_net_warn_ts >= net_check_sec:
                        _last_net_warn_ts = now
                        logger.warning('目标程序网络连接断联: 127.0.0.1:%d (连续%d次)', port, _network_disconnect_count)
                    if _network_disconnect_count >= int(cfg('network_disconnect_threshold', 3) or 3) and (now - _last_net_error_ts >= cooldown_sec):
                        _last_net_error_ts = now
                        logger.error('目标程序网络持续断联: 127.0.0.1:%d (连续%d次)', port, _network_disconnect_count)

            # GPU 高占用日志
            gpu_check_sec = max(5, int(cfg('gpu_check_interval_sec', 30) or 30))
            if bool(cfg('gpu_monitor_enabled', True)) and (now - _last_gpu_check_at >= gpu_check_sec):
                _last_gpu_check_at = now
                gpus = _get_gpu_stats()
                if gpus:
                    util_warn = int(cfg('gpu_warn_threshold', 90) or 90)
                    mem_warn = int(cfg('gpu_mem_warn_percent', 90) or 90)
                    over = []
                    for g in gpus:
                        mem_pct = int((g.get('memory_used_mb', 0) * 100) / max(1, g.get('memory_total_mb', 1)))
                        if g.get('utilization', 0) >= util_warn or mem_pct >= mem_warn:
                            over.append((g.get('index'), g.get('utilization', 0), mem_pct))
                    if over and (now - _last_gpu_warn_ts >= cooldown_sec):
                        _last_gpu_warn_ts = now
                        desc = ', '.join([f'GPU{idx}: util={util}%, mem={mem}%' for idx, util, mem in over])
                        logger.warning('GPU占用过高: %s (util阈值=%d%%, mem阈值=%d%%)', desc, util_warn, mem_warn)
        except Exception as e:
            logger.warning('运行监测线程异常: %s', e)


def _start_runtime_monitor_once():
    global _runtime_monitor_thread
    if _runtime_monitor_thread and _runtime_monitor_thread.is_alive():
        return
    _runtime_monitor_thread = threading.Thread(target=_runtime_monitor_loop, daemon=True, name='runtime-monitor')
    _runtime_monitor_thread.start()


def _flush_main_log_pending(reason: str = ''):
    if not bool(cfg('process_log_persist_enabled', True)):
        return

    with _main_log_pending_lock:
        if not _main_log_pending:
            return
        lines = list(_main_log_pending)
        _main_log_pending.clear()

    try:
        log_dir = _process_log_dir()
        log_dir.mkdir(parents=True, exist_ok=True)
        log_file = log_dir / f'process_{time.strftime("%Y%m%d")}.log'
        with open(log_file, 'a', encoding='utf-8') as f:
            for line in lines:
                f.write(f'{line}\n')
        if reason:
            logger.info('主程序日志已落盘: %s, 行数=%d, 原因=%s', log_file, len(lines), reason)
    except Exception as e:
        # 落盘失败时把数据放回 pending，避免日志丢失
        with _main_log_pending_lock:
            _main_log_pending[:0] = lines
        logger.warning('主程序日志落盘失败: %s', e)


def _cleanup_old_process_logs():
    days = int(cfg('process_log_cleanup_days', 7) or 7)
    if days <= 0:
        return

    log_dir = _process_log_dir()
    if not log_dir.exists():
        return

    cutoff_ts = time.time() - days * 86400
    removed = 0
    for file in log_dir.glob('process_*.log'):
        try:
            if file.is_file() and file.stat().st_mtime < cutoff_ts:
                file.unlink()
                removed += 1
        except OSError:
            pass
    if removed:
        logger.info('主程序历史日志清理完成: 删除 %d 个文件, 保留天数=%d', removed, days)


def _process_log_maintenance_loop():
    global _main_log_cursor, _main_log_last_cleanup_day

    while True:
        interval = max(2, int(cfg('process_log_maintenance_sec', 10) or 10))
        time.sleep(interval)
        try:
            enabled = bool(cfg('process_log_persist_enabled', True))
            threshold = max(100, int(cfg('process_log_line_threshold', 2000) or 2000))

            with _main_output_lock:
                total = len(_main_output)
                if _main_log_cursor > total:
                    _main_log_cursor = 0
                new_lines = _main_output[_main_log_cursor:total]
                _main_log_cursor = total

            if enabled and new_lines:
                with _main_log_pending_lock:
                    _main_log_pending.extend(new_lines)
            elif not enabled:
                with _main_log_pending_lock:
                    _main_log_pending.clear()

            with _main_log_pending_lock:
                pending_count = len(_main_log_pending)

            running = _main_proc is not None and _main_proc.poll() is None
            if enabled and pending_count >= threshold:
                _flush_main_log_pending('line-threshold')
            elif enabled and (not running) and pending_count > 0:
                _flush_main_log_pending('process-exit')

            today = time.strftime('%Y%m%d')
            if today != _main_log_last_cleanup_day:
                _main_log_last_cleanup_day = today
                _cleanup_old_process_logs()
        except Exception as e:
            logger.warning('主程序日志维护线程异常: %s', e)


def _start_process_log_maintenance_once():
    global _main_log_maint_thread
    if _main_log_maint_thread and _main_log_maint_thread.is_alive():
        return

    _main_log_maint_thread = threading.Thread(
        target=_process_log_maintenance_loop,
        daemon=True,
        name='process-log-maintainer',
    )
    _main_log_maint_thread.start()


# ─── 长期历史采样 ─────────────────────────────────────────────────────────────

def _lt_history_file() -> Path:
    return _process_log_dir() / 'lt_history.json'


def _load_lt_history():
    global _lt_history
    f = _lt_history_file()
    if not f.exists():
        return
    try:
        with open(f, 'r', encoding='utf-8') as fp:
            data = json.load(fp)
        if isinstance(data, list):
            with _lt_history_lock:
                _lt_history = [r for r in data if isinstance(r, dict) and 'ts' in r]
            logger.info('长期历史已从磁盘加载: %d 条记录', len(_lt_history))
    except Exception as e:
        logger.warning('加载长期历史失败: %s', e)


def _save_lt_history():
    global _lt_last_save_ts
    try:
        f = _lt_history_file()
        f.parent.mkdir(parents=True, exist_ok=True)
        with _lt_history_lock:
            data = list(_lt_history)
        with open(f, 'w', encoding='utf-8') as fp:
            json.dump(data, fp, ensure_ascii=False)
        _lt_last_save_ts = time.time()
    except Exception as e:
        logger.warning('保存长期历史失败: %s', e)


def _sample_lt_stats() -> dict | None:
    """采样目标进程当前 CPU+内存快照，复用已缓存的 psutil 对象"""
    proc = None
    if _cached_psutil_proc is not None:
        try:
            if _cached_psutil_proc.is_running():
                proc = _cached_psutil_proc
        except Exception:
            pass
    if proc is None:
        found = _find_process_by_name(cfg('main_program_name', 'VP_SERVER'))
        if found:
            proc = found
    if proc is None:
        return None
    try:
        cpu = _safe_get_proc_cpu(proc)   # 使用线程安全的限频基线采集，避免多线程并发错乱
        mem_mb = round(proc.memory_info().rss / 1024 / 1024, 1)
        sys_cpu = round(_safe_get_sys_cpu(), 1)
        vm = psutil.virtual_memory()
        return {'ts': round(time.time(), 1), 'cpu': round(cpu, 1), 'mem': mem_mb,
                'sys_cpu': sys_cpu, 'sys_mem': round(vm.used / 1024 / 1024, 1)}
    except (psutil.NoSuchProcess, psutil.AccessDenied):
        return None


def _lt_sampler_loop():
    global _lt_history, _lt_last_save_ts
    last_base_ts = 0.0
    last_adaptive_ts = 0.0
    last_cpu: float | None = None
    last_mem: float | None = None

    SAVE_INTERVAL = 600  # 每10分钟自动保存一次

    while True:
        time.sleep(30)  # 每30秒轮检一次
        try:
            now = time.time()
            base_interval = max(60, int(cfg('lt_sample_interval_sec', 1800) or 1800))
            cpu_thresh = float(cfg('lt_cpu_delta_thresh', 10.0) or 10.0)
            mem_thresh = float(cfg('lt_mem_delta_mb_thresh', 100.0) or 100.0)
            adaptive_min = max(60, int(cfg('lt_adaptive_min_sec', 120) or 120))
            max_records = max(100, int(cfg('lt_max_records', 4000) or 4000))

            stats = _sample_lt_stats()
            if stats is None:
                continue

            cpu = stats['cpu']
            mem = stats['mem']

            should_sample = False
            reason = ''

            # 1. 定时基础采样
            if now - last_base_ts >= base_interval:
                should_sample = True
                reason = 'periodic'
            # 2. 自适应采样：CPU 或内存突变超过阈值
            elif last_cpu is not None and last_mem is not None:
                if now - last_adaptive_ts >= adaptive_min:
                    if abs(cpu - last_cpu) >= cpu_thresh or abs(mem - last_mem) >= mem_thresh:
                        should_sample = True
                        reason = 'adaptive'

            if should_sample:
                record = {'ts': stats['ts'], 'cpu': cpu, 'mem': mem,
                          'sys_cpu': stats.get('sys_cpu', 0), 'sys_mem': stats.get('sys_mem', 0)}
                with _lt_history_lock:
                    _lt_history.append(record)
                    if len(_lt_history) > max_records:
                        _lt_history[:] = _lt_history[-max_records:]

                last_cpu = cpu
                last_mem = mem
                if reason == 'periodic':
                    last_base_ts = now
                else:
                    last_adaptive_ts = now

            # 定期落盘
            if now - _lt_last_save_ts >= SAVE_INTERVAL:
                _save_lt_history()

        except Exception as e:
            logger.warning('长期历史采样异常: %s', e)


def _start_lt_sampler_once():
    global _lt_sampler_thread
    if _lt_sampler_thread and _lt_sampler_thread.is_alive():
        return
    _lt_sampler_thread = threading.Thread(target=_lt_sampler_loop, daemon=True, name='lt-sampler')
    _lt_sampler_thread.start()
    logger.info('长期历史采样线程已启动 (基础间隔=%ds)', int(cfg('lt_sample_interval_sec', 1800)))


def _get_target_child_proc(parent_pid: int) -> psutil.Process | None:
    """获取 shell 进程下运行的实际子进程 (排除 shell 自身和控制面板自身)"""
    try:
        parent = psutil.Process(parent_pid)
        children = parent.children(recursive=True)
        target_name = cfg('main_program_name', 'VP_SERVER').lower()
        
        # 1. 优先匹配命令行或进程名包含 target_name 的子进程
        for child in children:
            try:
                cname = child.name().lower()
                cmdline = ' '.join(child.cmdline()).lower()
                # 排除 shell (sh, bash) 和我们当前的 control_panel 自身
                if cname in ('sh', 'bash', 'dash', 'zsh'):
                    continue
                if 'control_panel' in cname or 'control_panel' in cmdline:
                    continue
                if target_name in cname or target_name in cmdline:
                    return child
            except (psutil.NoSuchProcess, psutil.AccessDenied):
                continue
                
        # 2. 如果没有找到完美匹配的，返回第一个非 shell、非 control_panel 的子进程
        for child in children:
            try:
                cname = child.name().lower()
                cmdline = ' '.join(child.cmdline()).lower()
                if cname not in ('sh', 'bash', 'dash', 'zsh') and 'control_panel' not in cname and 'control_panel' not in cmdline:
                    return child
            except (psutil.NoSuchProcess, psutil.AccessDenied):
                continue
    except (psutil.NoSuchProcess, psutil.AccessDenied):
        pass
    return None


def _resolve_target_proc(proc: psutil.Process) -> psutil.Process | None:
    """把 shell/wrapper 进程解析为真正的业务子进程，保证重启控制面板前后 PID 一致。"""
    try:
        child = _get_target_child_proc(proc.pid)
        if child is not None:
            return child
        pname = proc.name().lower()
        if pname in ('sh', 'bash', 'dash', 'zsh'):
            return None
        return proc
    except (psutil.NoSuchProcess, psutil.AccessDenied):
        return None


def _find_process_by_name(name: str) -> psutil.Process | None:
    """按进程名/命令行查找进程，严格排除控制面板自身以防误判"""
    name_lower = name.lower()
    self_pid = os.getpid()
    
    # 建立一个特殊排除词列表，排除与业务进程无关的桌面和编译软件
    exclude_keywords = ['control_panel', 'gedit', 'vscode', 'python', 'feishu', 'firefox', 'chrome', 'msedge']
    
    for proc in psutil.process_iter(['pid', 'name', 'cmdline']):
        try:
            if proc.pid == self_pid:
                continue
            pname = (proc.info.get('name') or '').lower()
            cmdline_list = proc.info.get('cmdline') or []
            cmdline = ' '.join(cmdline_list).lower()
            
            # 严格防止匹配到任何控制面板或开发、通讯常用软件
            if any(kw in pname or kw in cmdline for kw in exclude_keywords):
                continue
                
            # 1. 优先检查可执行文件（第一个命令行参数的主文件名）是否完全匹配或包含目标名
            if cmdline_list:
                first_arg = os.path.basename(cmdline_list[0]).lower()
                if name_lower in first_arg:
                    resolved = _resolve_target_proc(proc)
                    if resolved is not None:
                        return resolved
                    continue
            
            # 2. 如果在 pname (进程名) 里直接匹配
            if name_lower in pname:
                resolved = _resolve_target_proc(proc)
                if resolved is not None:
                    return resolved
                continue
                
            # 3. 检查命令行中是否独立包含目标程序名称
            for arg in cmdline_list[1:]:
                arg_lower = arg.lower()
                if name_lower in arg_lower:
                    # 如果仅仅是开发目录/打包目录路径包含了这个词，则丢弃
                    if 'visionnavplatform_web' in arg_lower or 'bevp5.0' in arg_lower:
                        continue
                    resolved = _resolve_target_proc(proc)
                    if resolved is not None:
                        return resolved
                    break
        except (psutil.NoSuchProcess, psutil.AccessDenied):
            pass
    return None


def _find_processes_by_name(name: str) -> list[psutil.Process]:
    """查找所有目标业务进程，停止外部残留进程时使用。"""
    name_lower = name.lower()
    self_pid = os.getpid()
    exclude_keywords = ['control_panel', 'gedit', 'vscode', 'python', 'feishu', 'firefox', 'chrome', 'msedge']
    matches: list[psutil.Process] = []
    seen: set[int] = set()

    for proc in psutil.process_iter(['pid', 'name', 'cmdline']):
        try:
            if proc.pid == self_pid or proc.pid in seen:
                continue
            pname = (proc.info.get('name') or '').lower()
            cmdline_list = proc.info.get('cmdline') or []
            cmdline = ' '.join(cmdline_list).lower()
            if any(kw in pname or kw in cmdline for kw in exclude_keywords):
                continue

            matched = False
            if cmdline_list:
                first_arg = os.path.basename(cmdline_list[0]).lower()
                matched = name_lower in first_arg
            matched = matched or name_lower in pname
            if not matched:
                for arg in cmdline_list[1:]:
                    arg_lower = arg.lower()
                    if name_lower in arg_lower and 'visionnavplatform_web' not in arg_lower and 'bevp5.0' not in arg_lower:
                        matched = True
                        break
            if matched:
                resolved = _resolve_target_proc(proc)
                if resolved is not None and resolved.pid not in seen:
                    matches.append(resolved)
                    seen.add(resolved.pid)
        except (psutil.NoSuchProcess, psutil.AccessDenied):
            pass
    return matches


def _terminate_process_tree(proc: psutil.Process, timeout: float = 5.0) -> bool:
    """停止指定进程及其子进程，尽量覆盖外部启动和 shell 启动两种情况。"""
    try:
        targets = proc.children(recursive=True) + [proc]
    except (psutil.NoSuchProcess, psutil.AccessDenied):
        return True

    alive_targets = []
    for p in targets:
        try:
            if p.is_running() and p.status() != psutil.STATUS_ZOMBIE:
                alive_targets.append(p)
        except (psutil.NoSuchProcess, psutil.AccessDenied):
            continue

    for p in alive_targets:
        try:
            p.terminate()
        except (psutil.NoSuchProcess, psutil.AccessDenied, OSError):
            pass

    _, alive = psutil.wait_procs(alive_targets, timeout=timeout)
    for p in alive:
        try:
            p.kill()
        except (psutil.NoSuchProcess, psutil.AccessDenied, OSError):
            pass
    _, alive = psutil.wait_procs(alive, timeout=2)
    return len(alive) == 0


def _get_main_proc_stats() -> dict:
    """获取主程序实时统计"""
    global _cached_psutil_proc, _cached_psutil_pid

    proc: psutil.Process | None = None
    source = 'unknown'

    # 优先使用控制面板拉起的 Popen 进程；如果它是 shell，则统计实际业务子进程。
    if _main_proc is not None and _main_proc.poll() is None:
        try:
            act = _get_target_child_proc(_main_proc.pid)
            resolved_proc = act if act is not None else psutil.Process(_main_proc.pid)
            source = 'managed-child' if act is not None else 'managed'
            if _cached_psutil_pid != resolved_proc.pid or _cached_psutil_proc is None:
                _cached_psutil_proc = resolved_proc
                _cached_psutil_pid = resolved_proc.pid
                _cached_psutil_proc.cpu_percent()
            proc = _cached_psutil_proc
        except (psutil.NoSuchProcess, psutil.AccessDenied):
            _cached_psutil_proc = None
            _cached_psutil_pid = None
    else:
        # 目标程序不是由控制面板启动的，或者控制面板未记录到它，则到外部系统里全网搜索！
        found = _find_process_by_name(cfg('main_program_name', 'VP_SERVER'))
        if found is None:
            _cached_psutil_proc = None
            _cached_psutil_pid = None
            return {'running': False}
        source = 'external'
        if _cached_psutil_pid != found.pid or _cached_psutil_proc is None:
            _cached_psutil_proc = found
            _cached_psutil_pid = found.pid
            try:
                found.cpu_percent()  # 建立基线
            except (psutil.NoSuchProcess, psutil.AccessDenied):
                pass
        proc = _cached_psutil_proc

    if proc is None:
        return {'running': False}

    # 检验进程是否依然存活，可能处于僵尸状态或已不存在
    try:
        if not proc.is_running() or proc.status() == psutil.STATUS_ZOMBIE:
            _cached_psutil_proc = None
            _cached_psutil_pid = None
            return {'running': False}
    except Exception:
        _cached_psutil_proc = None
        _cached_psutil_pid = None
        return {'running': False}

    try:
        mem = proc.memory_info()
        threads = proc.num_threads()
        # cpu_percent() 此时已有历史基线，可以返回真实值
        cpu = _safe_get_proc_cpu(proc)
        if hasattr(proc, 'net_connections'):
            conns = proc.net_connections(kind='all')
        else:
            conns = proc.connections(kind='all')
        thread_list = []
        try:
            for t in proc.threads()[:20]:   # 最多显示 20 条
                thread_list.append({'id': t.id, 'user_time': round(t.user_time, 2),
                                    'system_time': round(t.system_time, 2)})
        except (psutil.NoSuchProcess, psutil.AccessDenied):
            pass

        return {
            'running': True,
            'pid': proc.pid,
            'source': source,
            'status': proc.status(),
            'cpu_percent': round(cpu, 1),
            'memory_rss_mb': round(mem.rss / 1024 / 1024, 1),
            'memory_vms_mb': round(mem.vms / 1024 / 1024, 1),
            'threads': threads,
            'thread_list': thread_list,
            'net_connections': {
                'total': len(conns),
                'established': sum(1 for c in conns if getattr(c, 'status', '') == 'ESTABLISHED'),
                'listen': sum(1 for c in conns if getattr(c, 'status', '') == 'LISTEN'),
            },
        }
    except (psutil.NoSuchProcess, psutil.AccessDenied):
        _cached_psutil_proc = None
        _cached_psutil_pid = None
        return {'running': False}


def _start_main_process_nolock() -> tuple[bool, str]:
    global _main_proc, _main_last_output_ts, _main_hang_logged, _external_output_notice_pid
    if _find_process_by_name(cfg('main_program_name', 'VP_SERVER')):
        return False, '进程已在运行中'

    cmd = cfg('main_program_cmd')
    if not cmd:
        return False, '未配置 main_program_cmd'

    work_dir = cfg('base_dir', SCRIPT_DIR)

    with _main_output_lock:
        _main_output.clear()
        _external_output_notice_pid = None
        _main_output.append(f'[控制面板] 工作目录: {work_dir}')
        _main_output.append(f'[控制面板] 启动命令: {cmd}')
        _main_output.append('')

    try:
        # 使用 pty 捕获子进程所有终端输出
        master_fd, slave_fd = pty.openpty()

        # 设置 LD_LIBRARY_PATH，与 run.sh 行为一致
        env = os.environ.copy()
        lib_path = os.path.join(work_dir, 'lib')
        env['LD_LIBRARY_PATH'] = lib_path + ':' + env.get('LD_LIBRARY_PATH', '')

        _main_proc = subprocess.Popen(
            cmd,
            shell=True,
            cwd=work_dir,
            stdout=slave_fd,
            stderr=slave_fd,
            stdin=subprocess.DEVNULL,
            close_fds=True,
            env=env,
            preexec_fn=os.setsid,
        )
        os.close(slave_fd)
        logger.info("主程序已启动: %s (pid=%d, cwd=%s)", cmd, _main_proc.pid, work_dir)
        _main_last_output_ts = time.time()
        _main_hang_logged = False

        # 后台线程持续读取输出
        _local_proc = _main_proc  # 捕获当前进程的本地引用，避免全局变量被重启覆盖

        def _read_main_output():
            proc = _local_proc
            buf = ''
            while True:
                try:
                    ready, _, _ = select.select([master_fd], [], [], 0.5)
                    if ready:
                        data = os.read(master_fd, 4096)
                        if not data:
                            break
                        text = data.decode('utf-8', errors='replace')
                        buf += text
                        while '\n' in buf or '\r' in buf:
                            if '\n' in buf:
                                line, buf = buf.split('\n', 1)
                                line = line.rstrip('\r')
                            else:
                                line, buf = buf.rsplit('\r', 1)
                            if line:
                                with _main_output_lock:
                                    _main_output.append(line)
                                    if len(_main_output) > 5000:
                                        _main_output[:] = _main_output[-3000:]
                                _main_last_output_ts = time.time()
                    elif proc.poll() is not None:
                        break
                except OSError:
                    break

            if buf.strip():
                with _main_output_lock:
                    _main_output.append(buf.strip())

            try:
                os.close(master_fd)
            except OSError:
                pass

            rc = proc.wait() if proc else -1
            # 只有当前进程仍是活跃进程时才追加退出信息（避免重启后旧线程覆盖新输出）
            if _main_proc is None or _main_proc is proc:
                with _main_output_lock:
                    _main_output.append('')
                    _main_output.append(f'[控制面板] 进程已退出，返回码: {rc}')
            if rc == 0:
                logger.warning('主程序已退出，返回码: %s', rc)
            else:
                logger.error('主程序异常退出，返回码: %s', rc)
            _main_hang_logged = False

        threading.Thread(target=_read_main_output, daemon=True, name='main-output-reader').start()
        return True, f'已启动，Shell PID: {_main_proc.pid}'
    except Exception as e:
        logger.exception("启动主程序失败")
        return False, str(e)


def _stop_main_process_nolock() -> tuple[bool, str]:
    global _main_proc, _main_hang_logged, _cached_psutil_proc, _cached_psutil_pid, _external_output_notice_pid

    # 优先停止相关的 systemd 开机自启服务，从而防止 systemd 在瞬间自动拉起
    try:
        sys_status = _get_autostart_status()
        if sys_status.get('active', False):
            with _main_output_lock:
                _main_output.append(f'[控制面板] 检测到目标程序跑在 Systemd 服务中，正在执行 systemctl stop {AUTOSTART_SERVICE_NAME} ...')
            subprocess.run(['systemctl', 'stop', AUTOSTART_SERVICE_NAME], capture_output=True, timeout=5)
    except Exception as e:
        logger.warning("通过 systemctl 停止服务失败: %s", e)

    # 优先通过进程组杀死整棵进程树（启动时用了 os.setsid）
    if _main_proc and _main_proc.poll() is None:
        pgid = None
        try:
            pgid = os.getpgid(_main_proc.pid)
        except OSError:
            pass

        with _main_output_lock:
            _main_output.append(f'[控制面板] 正在停止进程组 PGID={pgid or _main_proc.pid} ...')

        try:
            if pgid:
                os.killpg(pgid, 15)  # SIGTERM
            else:
                _main_proc.terminate()
            try:
                _main_proc.wait(timeout=6)
            except subprocess.TimeoutExpired:
                if pgid:
                    os.killpg(pgid, 9)  # SIGKILL
                else:
                    _main_proc.kill()
                _main_proc.wait(timeout=3)
        except (OSError, subprocess.TimeoutExpired):
            pass

    # 再兜底：按进程名查找并清理所有残留/外部启动的目标进程。
    target_name = cfg('main_program_name', 'VP_SERVER')
    for _ in range(3):
        procs = _find_processes_by_name(target_name)
        if not procs:
            break
        for proc in procs:
            try:
                with _main_output_lock:
                    _main_output.append(f'[控制面板] 清理目标进程 PID={proc.pid} ...')
                _terminate_process_tree(proc)
            except (psutil.NoSuchProcess, psutil.AccessDenied, OSError):
                pass
        time.sleep(0.5)

    _main_proc = None
    _cached_psutil_proc = None
    _cached_psutil_pid = None
    _external_output_notice_pid = None

    remaining = _find_processes_by_name(target_name)
    if remaining:
        pids = ','.join(str(p.pid) for p in remaining)
        msg = f'目标进程仍在运行: PID={pids}'
        with _main_output_lock:
            _main_output.append(f'[控制面板] {msg}')
        logger.warning(msg)
        _main_hang_logged = False
        return False, msg

    with _main_output_lock:
        _main_output.append('[控制面板] 进程已停止')
    logger.info("主程序已停止")
    _main_hang_logged = False
    return True, '进程已停止'


def _memory_guard_loop():
    global _mem_restart_events
    last_restart_at = 0.0
    while True:
        check_sec = max(1, int(cfg('memory_guard_check_sec', 5) or 5))
        time.sleep(check_sec)

        if not bool(cfg('memory_guard_enabled', False)):
            continue

        limit_mb = float(cfg('memory_limit_mb', 0) or 0)
        if limit_mb <= 0:
            continue

        proc = _find_process_by_name(cfg('main_program_name', 'VP_SERVER'))
        if proc is None:
            continue

        try:
            rss_mb = proc.memory_info().rss / 1024 / 1024
        except (psutil.NoSuchProcess, psutil.AccessDenied):
            continue

        if rss_mb <= limit_mb:
            continue

        now = time.time()
        cooldown = max(5, int(cfg('memory_guard_restart_cooldown_sec', 30) or 30))
        if now - last_restart_at < cooldown:
            continue

        # 防止短时间频繁重启
        window_sec = max(60, int(cfg('memory_guard_window_sec', 600) or 600))
        max_restarts = max(1, int(cfg('memory_guard_max_restarts_window', 5) or 5))
        _mem_restart_events = [t for t in _mem_restart_events if now - t <= window_sec]
        if len(_mem_restart_events) >= max_restarts:
            logger.error(
                '内存守护暂停重启: %d秒内已重启%d次(上限%d次), 当前RSS=%.1fMB, 限制=%.1fMB',
                window_sec, len(_mem_restart_events), max_restarts, rss_mb, limit_mb,
            )
            continue

        logger.warning(
            "内存守护触发: 当前RSS=%.1fMB, 限制=%.1fMB，准备重启主程序",
            rss_mb, limit_mb,
        )

        with _proc_op_lock:
            ok_stop, msg_stop = _stop_main_process_nolock()
            if not ok_stop:
                logger.warning("内存守护停止主程序失败: %s", msg_stop)
                continue

            time.sleep(1)
            ok_start, msg_start = _start_main_process_nolock()
            if ok_start:
                last_restart_at = time.time()
                _mem_restart_events.append(last_restart_at)
                logger.warning("内存守护重启成功: %s", msg_start)
            else:
                logger.error("内存守护重启失败: %s", msg_start)


def _start_memory_guard_once():
    global _mem_guard_thread
    if _mem_guard_thread and _mem_guard_thread.is_alive():
        return
    _mem_guard_thread = threading.Thread(target=_memory_guard_loop, daemon=True, name='memory-guard')
    _mem_guard_thread.start()
    logger.info("内存守护线程已启动")


_prev_net_io: dict = {}
_prev_net_time: float = 0.0
_prev_lo_io: dict = {}
_sys_thread_cache: int = 0
_sys_thread_cache_ts: float = 0.0

# 线程安全的 CPU 基线采集缓存，解决并发调用导致 psutil 百分比错乱（如 0% 或 800% 飙高）
_last_proc_cpu_val: float = 0.0
_last_proc_cpu_ts: float = 0.0
_proc_cpu_lock = threading.Lock()

_last_sys_cpu_val: float = 0.0
_last_sys_cpu_ts: float = 0.0
_sys_cpu_lock = threading.Lock()


def _safe_get_proc_cpu(proc) -> float:
    """线程安全、限频采集目标进程的 CPU 使用率，并除以 CPU 核心数以归一化到系统整体占比"""
    global _last_proc_cpu_val, _last_proc_cpu_ts
    now = time.time()
    with _proc_cpu_lock:
        if now - _last_proc_cpu_ts >= 0.8:
            try:
                val = proc.cpu_percent()
                # 统一度量：进程的 cpu_percent 是针对单核的（最高可以是 cpu_count * 100%），
                # 我们除以核心数，使得进程 CPU 的最大值也是 100%，与系统 CPU 的占比规则完全一致。
                cores = psutil.cpu_count() or 1
                _last_proc_cpu_val = val / cores
                _last_proc_cpu_ts = now
            except Exception:
                pass
        return _last_proc_cpu_val


def _safe_get_sys_cpu() -> float:
    """线程安全、限频采集服务器整体的 CPU 使用率"""
    global _last_sys_cpu_val, _last_sys_cpu_ts
    now = time.time()
    with _sys_cpu_lock:
        if now - _last_sys_cpu_ts >= 0.8:
            try:
                _last_sys_cpu_val = psutil.cpu_percent(interval=None)
                _last_sys_cpu_ts = now
            except Exception:
                pass
        return _last_sys_cpu_val


def _get_net_rate() -> dict:
    """计算系统网络收发速率 (KB/s)，含全接口聚合和本地回环(lo)"""
    global _prev_net_io, _prev_net_time, _prev_lo_io
    try:
        per = psutil.net_io_counters(pernic=True)
        cur = psutil.net_io_counters()   # 全接口聚合 (含lo)
        lo  = per.get('lo') or per.get('lo0')   # Linux=lo / macOS=lo0
        now = time.time()
        result: dict = {
            'bytes_sent': cur.bytes_sent,
            'bytes_recv': cur.bytes_recv,
            'rx_kbps': 0.0,
            'tx_kbps': 0.0,
            'lo_rx_kbps': 0.0,
            'lo_tx_kbps': 0.0,
        }
        if _prev_net_io and _prev_net_time:
            elapsed = now - _prev_net_time
            if elapsed > 0:
                result['rx_kbps'] = round(
                    (cur.bytes_recv - _prev_net_io.get('bytes_recv', 0)) / elapsed / 1024, 1)
                result['tx_kbps'] = round(
                    (cur.bytes_sent - _prev_net_io.get('bytes_sent', 0)) / elapsed / 1024, 1)
                if lo and _prev_lo_io:
                    result['lo_rx_kbps'] = round(
                        (lo.bytes_recv - _prev_lo_io.get('bytes_recv', 0)) / elapsed / 1024, 1)
                    result['lo_tx_kbps'] = round(
                        (lo.bytes_sent - _prev_lo_io.get('bytes_sent', 0)) / elapsed / 1024, 1)
        _prev_net_io = {'bytes_recv': cur.bytes_recv, 'bytes_sent': cur.bytes_sent}
        _prev_net_time = now
        if lo:
            _prev_lo_io = {'bytes_recv': lo.bytes_recv, 'bytes_sent': lo.bytes_sent}
        return result
    except Exception:
        return {}


def _get_sys_stats() -> dict:
    """采集服务器整体 CPU / 内存 / 系统线程总数"""
    global _sys_thread_cache, _sys_thread_cache_ts
    try:
        vm = psutil.virtual_memory()
        sys_cpu = round(_safe_get_sys_cpu(), 1)
        # 系统线程总数每10秒刷新一次，避免每2s遍历所有进程
        now = time.time()
        if now - _sys_thread_cache_ts > 10:
            try:
                total = sum(
                    (p.info.get('num_threads') or 0)
                    for p in psutil.process_iter(['num_threads'])
                )
                _sys_thread_cache = total
                _sys_thread_cache_ts = now
            except Exception:
                pass
        return {
            'sys_cpu_percent': sys_cpu,
            'sys_mem_used_mb': round(vm.used / 1024 / 1024, 1),
            'sys_mem_total_mb': round(vm.total / 1024 / 1024, 1),
            'sys_mem_percent': round(vm.percent, 1),
            'sys_threads': _sys_thread_cache,
        }
    except Exception:
        return {}


# ─── GPU 监测 ─────────────────────────────────────────────────────────────────

def _get_target_pid() -> int | None:
    """获取真正的目标业务进程 PID"""
    if _main_proc is not None and _main_proc.poll() is None:
        act = _get_target_child_proc(_main_proc.pid)
        if act is not None:
            return act.pid
        return _main_proc.pid
    found = _find_process_by_name(cfg('main_program_name', 'VP_SERVER'))
    if found is not None:
        return found.pid
    return None


def _get_target_gpu_stats(pid: int) -> dict | None:
    """通过 nvidia-smi 查询特定 PID 的 GPU 内存和利用率"""
    try:
        # 1. 查询显存占用
        res_mem = subprocess.run(
            ['nvidia-smi', '--query-compute-apps=gpu_uuid,pid,used_memory', '--format=csv,noheader,nounits'],
            capture_output=True, text=True, timeout=3
        )
        gpu_uuid = None
        used_mem = 0
        if res_mem.returncode == 0:
            for line in res_mem.stdout.strip().split('\n'):
                if not line.strip():
                    continue
                parts = [p.strip() for p in line.split(',')]
                if len(parts) >= 3:
                    try:
                        curr_pid = int(parts[1])
                        if curr_pid == pid:
                            gpu_uuid = parts[0]
                            used_mem = int(parts[2])
                            break
                    except ValueError:
                        continue
        
        # 2. 查询 UUID 对应的 GPU 索引
        gpu_index = -1
        if gpu_uuid:
            res_idx = subprocess.run(
                ['nvidia-smi', '--query-gpu=index,uuid', '--format=csv,noheader'],
                capture_output=True, text=True, timeout=3
            )
            if res_idx.returncode == 0:
                for line in res_idx.stdout.strip().split('\n'):
                    parts = [p.strip() for p in line.split(',')]
                    if len(parts) >= 2 and parts[1] == gpu_uuid:
                        try:
                            gpu_index = int(parts[0])
                        except ValueError:
                            pass
                        break

        # 3. 运行 pmon 获取利用率 (SM %, ENC %, DEC %)
        sm_util = 0
        enc_util = 0
        dec_util = 0
        res_pmon = subprocess.run(
            ['nvidia-smi', 'pmon', '-c', '1'],
            capture_output=True, text=True, timeout=3
        )
        if res_pmon.returncode == 0:
            lines = res_pmon.stdout.strip().split('\n')
            for line in lines:
                if line.startswith('#') or 'Idx' in line or 'gpu' in line or not line.strip():
                    continue
                parts = line.split()
                if len(parts) >= 8:
                    try:
                        curr_pid = int(parts[1])
                        if curr_pid == pid:
                            def parse_pct(val):
                                return int(val) if val.isdigit() else 0
                            sm_util = parse_pct(parts[3])
                            enc_util = parse_pct(parts[5])
                            dec_util = parse_pct(parts[6])
                            break
                    except ValueError:
                        continue

        if gpu_uuid is not None or used_mem > 0 or sm_util > 0:
            return {
                'gpu_index': gpu_index if gpu_index != -1 else 0,
                'used_memory_mb': used_mem,
                'sm_utilization': sm_util,
                'enc_utilization': enc_util,
                'dec_utilization': dec_util,
            }
    except Exception:
        pass
    return None


def _get_gpu_stats() -> list[dict]:
    """通过 nvidia-smi 获取 GPU 状态并追加目标程序的 GPU 段"""
    try:
        result = subprocess.run(
            ['nvidia-smi', '--query-gpu=index,name,utilization.gpu,memory.used,memory.total,temperature.gpu',
             '--format=csv,noheader,nounits'],
            capture_output=True, text=True, timeout=5,
        )
        if result.returncode != 0:
            return []
        
        gpus = []
        for line in result.stdout.strip().split('\n'):
            parts = [p.strip() for p in line.split(',')]
            if len(parts) >= 6:
                gpus.append({
                    'index': int(parts[0]),
                    'name': parts[1],
                    'utilization': int(parts[2]),
                    'memory_used_mb': int(parts[3]),
                    'memory_total_mb': int(parts[4]),
                    'temperature': int(parts[5]),
                })
        
        # 挂载目标程序的 GPU 数据
        target_pid = _get_target_pid()
        target_gpu_info = _get_target_gpu_stats(target_pid) if target_pid else None
        
        for gpu in gpus:
            if target_gpu_info and gpu['index'] == target_gpu_info['gpu_index']:
                gpu['target'] = target_gpu_info
            else:
                gpu['target'] = None
                
        return gpus
    except (FileNotFoundError, subprocess.TimeoutExpired, Exception):
        return []


# ─── 相机联通监测 ──────────────────────────────────────────────────────────────

_camera_status: dict = {}  # {camera_ip: {'reachable': bool, 'last_check': float}}
_camera_monitor_thread: threading.Thread | None = None


def _ping_host(ip: str, timeout: float = 2.0) -> bool:
    """Ping 一个 IP 地址，返回是否可达"""
    try:
        result = subprocess.run(
            ['ping', '-c', '1', '-W', str(int(timeout)), ip],
            capture_output=True, timeout=timeout + 1,
        )
        return result.returncode == 0
    except (subprocess.TimeoutExpired, Exception):
        return False


def _camera_monitor_loop():
    """后台线程：定期 ping 所有相机 IP"""
    global _camera_status
    while True:
        interval = max(3, int(cfg('camera_monitor_interval_sec', 10) or 10))
        time.sleep(interval)

        if not bool(cfg('camera_monitor_enabled', False)):
            continue

        # 读取相机配置获取 IP 列表
        cam_path = cfg_path('camera_config_path')
        if not cam_path or not os.path.isfile(cam_path):
            continue

        try:
            with open(cam_path, 'r', encoding='utf-8') as f:
                cam_data = json.load(f)
            cam_list = cam_data.get('camera_config', [])
        except Exception:
            continue

        for cam in cam_list:
            ip = (cam.get('camera_ip') or '').strip()
            if not ip:
                continue
            reachable = _ping_host(ip)
            _camera_status[ip] = {
                'reachable': reachable,
                'last_check': time.time(),
            }


def _start_camera_monitor_once():
    global _camera_monitor_thread
    if _camera_monitor_thread and _camera_monitor_thread.is_alive():
        return
    _camera_monitor_thread = threading.Thread(target=_camera_monitor_loop, daemon=True, name='camera-monitor')
    _camera_monitor_thread.start()
    logger.info("相机联通监测线程已启动")


# ─── Systemd 日志流监测 ────────────────────────────────────────────────────────

_systemd_log_proc: subprocess.Popen | None = None
_systemd_log_monitor_thread: threading.Thread | None = None


def _systemd_log_monitor_loop():
    global _systemd_log_proc, _main_last_output_ts
    while True:
        time.sleep(2)
        try:
            # 1. 如果控制面板自身已经拉起了子进程，则坚决不采用 systemd 日志读取，避免日志重复并泄露管道
            if _main_proc is not None and _main_proc.poll() is None:
                if _systemd_log_proc is not None:
                    try:
                        _systemd_log_proc.terminate()
                        _systemd_log_proc.wait(timeout=2)
                    except Exception:
                        pass
                    _systemd_log_proc = None
                continue

            # 2. 检查 systemd 服务是否处于 active 状态
            status = _get_autostart_status()
            if not status.get('active', False):
                if _systemd_log_proc is not None:
                    try:
                        _systemd_log_proc.terminate()
                        _systemd_log_proc.wait(timeout=2)
                    except Exception:
                        pass
                    _systemd_log_proc = None
                continue

            # 3. 如果服务确实在运行，且我们还没有对应的 journalctl 读取进程，则启动它
            if _systemd_log_proc is None or _systemd_log_proc.poll() is not None:
                logger.info("检测到目标程序运行在 Systemd 自动拉起服务中，启动 journalctl 日志流对接...")
                with _main_output_lock:
                    _main_output.append('[控制面板] 检测到目标程序通过 Systemd 自启/自动拉起，正在对接系统日志...')

                # 使用 -o cat 只获取原始输出，不包含日志元数据（时间、主机、进程名等）
                _systemd_log_proc = subprocess.Popen(
                    ['journalctl', '-u', AUTOSTART_SERVICE_NAME, '-f', '-n', '100', '-o', 'cat'],
                    stdout=subprocess.PIPE,
                    stderr=subprocess.STDOUT,
                    text=True,
                    bufsize=1,
                )

                def _read_journal_lines(proc):
                    global _main_last_output_ts
                    try:
                        for line in iter(proc.stdout.readline, ''):
                            if not line:
                                break
                            clean_line = line.rstrip('\r\n')
                            with _main_output_lock:
                                _main_output.append(clean_line)
                                if len(_main_output) > 5000:
                                    _main_output[:] = _main_output[-3000:]
                            _main_last_output_ts = time.time()
                    except Exception:
                        pass
                    finally:
                        try:
                            proc.stdout.close()
                        except Exception:
                            pass

                threading.Thread(target=_read_journal_lines, args=(_systemd_log_proc,), daemon=True, name='systemd-log-reader').start()

        except Exception as e:
            logger.warning("Systemd 日志流监控循环异常: %s", e)


def _start_systemd_log_monitor_once():
    global _systemd_log_monitor_thread
    if _systemd_log_monitor_thread and _systemd_log_monitor_thread.is_alive():
        return
    _systemd_log_monitor_thread = threading.Thread(target=_systemd_log_monitor_loop, daemon=True, name='systemd-log-monitor')
    _systemd_log_monitor_thread.start()
    logger.info("Systemd 日志流监控守护线程已启动")


# ─── 开机自启管理 ──────────────────────────────────────────────────────────────

AUTOSTART_SERVICE_NAME = "bevp-target-program.service"
AUTOSTART_UNIT_PATH = f"/etc/systemd/system/{AUTOSTART_SERVICE_NAME}"


def _get_autostart_status() -> dict:
    """获取目标程序开机自启状态"""
    try:
        result = subprocess.run(
            ['systemctl', 'is-enabled', AUTOSTART_SERVICE_NAME],
            capture_output=True, text=True, timeout=5,
        )
        enabled = result.stdout.strip() == 'enabled'
    except Exception:
        enabled = False

    try:
        result = subprocess.run(
            ['systemctl', 'is-active', AUTOSTART_SERVICE_NAME],
            capture_output=True, text=True, timeout=5,
        )
        active = result.stdout.strip() == 'active'
    except Exception:
        active = False

    return {'enabled': enabled, 'active': active, 'service_name': AUTOSTART_SERVICE_NAME}


def _sudo_prefix() -> list[str]:
    """非 root 时返回 ['sudo', '-n']，用于需要特权的子命令前缀"""
    return [] if os.geteuid() == 0 else ['sudo', '-n']


def _install_autostart_service() -> tuple[bool, str]:
    """安装目标程序开机自启 systemd 服务"""
    cmd = cfg('main_program_cmd', '')
    if not cmd:
        return False, '未配置主程序启动命令'

    work_dir = cfg('base_dir', SCRIPT_DIR)
    user = os.environ.get('SUDO_USER') or os.environ.get('USER', 'root')
    work_dir_q = shlex.quote(work_dir)
    cmd_for_shell = cmd.replace("'", "'\"'\"'")

    unit_content = f"""[Unit]
Description=BrightEyes Target Program Auto-Start
After=network.target
StartLimitIntervalSec=300
StartLimitBurst=10

[Service]
Type=simple
WorkingDirectory={work_dir}
ExecStart=/bin/bash -lc 'cd {work_dir_q} && export LD_LIBRARY_PATH={work_dir_q}/lib:$LD_LIBRARY_PATH && exec {cmd_for_shell}'
Restart=always
RestartSec=5
KillMode=control-group
TimeoutStopSec=10
User={user}
Environment=PYTHONUNBUFFERED=1

[Install]
WantedBy=multi-user.target
"""
    sudo = _sudo_prefix()
    try:
        if sudo:
            # 非 root：通过 sudo tee 写入 unit 文件
            tee = subprocess.run(
                sudo + ['tee', AUTOSTART_UNIT_PATH],
                input=unit_content, capture_output=True, text=True, timeout=10,
            )
            if tee.returncode != 0:
                err = tee.stderr.strip()
                if 'sudo' in err.lower() or 'password' in err.lower() or err == '':
                    return False, '权限不足，请在安装脚本中配置 sudoers 免密规则（见 install_control_panel_service.sh）'
                return False, err or '写入 unit 文件失败'
        else:
            with open(AUTOSTART_UNIT_PATH, 'w') as f:
                f.write(unit_content)

        d = subprocess.run(sudo + ['systemctl', 'daemon-reload'], capture_output=True, text=True, timeout=10)
        if d.returncode != 0:
            return False, d.stderr.strip() or 'systemctl daemon-reload 失败'

        e = subprocess.run(sudo + ['systemctl', 'enable', AUTOSTART_SERVICE_NAME], capture_output=True, text=True, timeout=10)
        if e.returncode != 0:
            return False, e.stderr.strip() or 'systemctl enable 失败'

        s = subprocess.run(sudo + ['systemctl', 'start', AUTOSTART_SERVICE_NAME], capture_output=True, text=True, timeout=15)
        if s.returncode != 0:
            return False, s.stderr.strip() or 'systemctl start 失败'

        logger.info("目标程序开机自启服务已安装: %s", AUTOSTART_SERVICE_NAME)
        return True, '开机自启已启用'
    except PermissionError:
        return False, '权限不足，请在安装脚本中配置 sudoers 免密规则'
    except Exception as e:
        return False, str(e)


def _remove_autostart_service() -> tuple[bool, str]:
    """移除目标程序开机自启 systemd 服务"""
    sudo = _sudo_prefix()
    try:
        d = subprocess.run(sudo + ['systemctl', 'disable', '--now', AUTOSTART_SERVICE_NAME],
                           capture_output=True, text=True, timeout=10)
        if d.returncode != 0 and 'not loaded' not in (d.stderr or ''):
            return False, d.stderr.strip() or 'systemctl disable --now 失败'
        if os.path.exists(AUTOSTART_UNIT_PATH):
            if sudo:
                subprocess.run(sudo + ['rm', '-f', AUTOSTART_UNIT_PATH],
                               capture_output=True, text=True, timeout=10)
            else:
                os.remove(AUTOSTART_UNIT_PATH)
        r = subprocess.run(sudo + ['systemctl', 'daemon-reload'], capture_output=True, text=True, timeout=10)
        if r.returncode != 0:
            return False, r.stderr.strip() or 'systemctl daemon-reload 失败'
        logger.info("目标程序开机自启服务已移除: %s", AUTOSTART_SERVICE_NAME)
        return True, '开机自启已禁用'
    except PermissionError:
        return False, '权限不足，请在安装脚本中配置 sudoers 免密规则'
    except Exception as e:
        return False, str(e)


# ─── 电源/断电监测 ───────────────────────────────────────────────────────────

def _write_heartbeat():
    """写入心跳文件，标记控制面板正在运行"""
    try:
        data = {
            'status': 'running',
            'ts': time.time(),
            'datetime': time.strftime('%Y-%m-%d %H:%M:%S'),
            'pid': os.getpid(),
        }
        with open(_HEARTBEAT_FILE, 'w', encoding='utf-8') as f:
            json.dump(data, f)
    except Exception:
        pass


def _write_heartbeat_shutdown():
    """正常关机时写入 clean_shutdown 状态"""
    try:
        data = {
            'status': 'clean_shutdown',
            'ts': time.time(),
            'datetime': time.strftime('%Y-%m-%d %H:%M:%S'),
            'pid': os.getpid(),
        }
        with open(_HEARTBEAT_FILE, 'w', encoding='utf-8') as f:
            json.dump(data, f)
    except Exception:
        pass


def _load_power_events():
    """加载历史电源事件记录"""
    global _power_events
    if os.path.isfile(_POWER_LOG_FILE):
        try:
            with open(_POWER_LOG_FILE, 'r', encoding='utf-8') as f:
                _power_events = json.load(f)
            # 只保留最近 200 条
            if len(_power_events) > 200:
                _power_events = _power_events[-200:]
        except Exception:
            _power_events = []


def _save_power_events():
    """持久化电源事件记录"""
    try:
        with open(_POWER_LOG_FILE, 'w', encoding='utf-8') as f:
            json.dump(_power_events[-200:], f, ensure_ascii=False, indent=2)
    except Exception as e:
        logger.warning('保存电源事件记录失败: %s', e)


def _check_last_shutdown():
    """
    启动时检查上次关机类型:
    - heartbeat 文件 status='running' → 上次异常断电/崩溃
    - heartbeat 文件 status='clean_shutdown' → 上次正常关机
    - heartbeat 文件不存在 → 首次运行或文件被清理
    """
    event = {
        'ts': time.time(),
        'datetime': time.strftime('%Y-%m-%d %H:%M:%S'),
        'type': 'startup',
        'last_shutdown': 'unknown',
        'detail': '',
    }

    if os.path.isfile(_HEARTBEAT_FILE):
        try:
            with open(_HEARTBEAT_FILE, 'r', encoding='utf-8') as f:
                hb = json.load(f)
            last_status = hb.get('status', 'unknown')
            last_ts = hb.get('ts', 0)
            last_dt = hb.get('datetime', '')

            if last_status == 'running':
                # 上次未正常关闭 → 断电或崩溃
                event['last_shutdown'] = 'power_loss'
                event['detail'] = f'上次心跳时间: {last_dt} (状态仍为running，判定为异常断电或进程崩溃)'
                logger.warning('⚡ 检测到上次异常关机（疑似断电）: 最后心跳 %s', last_dt)
            elif last_status == 'clean_shutdown':
                event['last_shutdown'] = 'graceful'
                event['detail'] = f'上次正常关机时间: {last_dt}'
                logger.info('上次为正常关机: %s', last_dt)
            else:
                event['last_shutdown'] = 'unknown'
                event['detail'] = f'心跳文件状态异常: {last_status}'
                logger.warning('心跳文件状态未知: %s', last_status)
        except Exception as e:
            event['last_shutdown'] = 'unknown'
            event['detail'] = f'心跳文件读取失败: {e}'
            logger.warning('心跳文件读取失败: %s', e)
    else:
        event['last_shutdown'] = 'first_run'
        event['detail'] = '首次运行或心跳文件不存在'
        logger.info('首次启动，无历史心跳记录')

    # 补充 systemd journal 信息（如果可用）
    try:
        r = subprocess.run(
            ['journalctl', '--list-boots', '-n', '2', '--no-pager'],
            capture_output=True, text=True, timeout=5
        )
        if r.returncode == 0 and r.stdout.strip():
            event['boot_info'] = r.stdout.strip().split('\n')[:2]
    except Exception:
        pass

    # 尝试通过 last 命令获取上次关机记录
    try:
        r = subprocess.run(
            ['last', '-x', 'shutdown', 'reboot', '-n', '3'],
            capture_output=True, text=True, timeout=5
        )
        if r.returncode == 0 and r.stdout.strip():
            event['last_cmd_info'] = [l for l in r.stdout.strip().split('\n') if l.strip()][:3]
    except Exception:
        pass

    with _power_events_lock:
        _power_events.append(event)
    _save_power_events()
    return event


def _heartbeat_writer_loop():
    """后台线程：定期写入心跳文件"""
    while True:
        _write_heartbeat()
        time.sleep(30)  # 每 30 秒写一次


def _start_power_monitor_once():
    """启动电源监测：检查上次关机 + 启动心跳写入 + 注册退出信号"""
    _load_power_events()
    _check_last_shutdown()
    _write_heartbeat()

    # 注册优雅退出信号处理
    def _on_shutdown_signal(signum, frame):
        logger.info('收到退出信号 %d，标记正常关机', signum)
        _write_heartbeat_shutdown()
        sys.exit(0)

    signal.signal(signal.SIGTERM, _on_shutdown_signal)
    signal.signal(signal.SIGINT, _on_shutdown_signal)

    # 启动心跳写入线程
    t = threading.Thread(target=_heartbeat_writer_loop, daemon=True, name='power-heartbeat')
    t.start()
    logger.info('电源监测已启动（心跳文件: %s）', _HEARTBEAT_FILE)


# ─── MQTT 订阅：目标程序重启 ──────────────────────────────────────────────────

AGV_REQUEST_PREFIX = 'agv/v1/s/'
AGV_RESPONSE_PREFIX = 'agv/v1/c/'


def _mqtt_on_connect(client, userdata, flags, rc):
    """MQTT 连接/重连成功后订阅所有 topic（broker 重启后自动恢复订阅）"""
    if rc != 0:
        logger.error('[MQTT] 连接失败, rc=%d', rc)
        return
    logger.info('[MQTT] 已连接 broker (flags=%s)', flags)
    topic = f'{AGV_REQUEST_PREFIX}cluster/nodes/{_mqtt_node_id}/restart_node'
    client.subscribe(topic, qos=1)
    logger.info('[MQTT] 已订阅: %s', topic)


def _mqtt_on_disconnect(client, userdata, rc):
    """MQTT 断开连接回调，paho 会自动重连"""
    if rc == 0:
        logger.info('[MQTT] 已主动断开连接')
    else:
        logger.warning('[MQTT] 与 broker 断开连接 (rc=%d)，将自动重连...', rc)


def _mqtt_on_message(client, userdata, msg):
    """处理 MQTT 消息 —— 在独立线程执行，避免阻塞网络循环导致订阅失效"""
    threading.Thread(target=_handle_mqtt_restart, args=(client, msg), daemon=True).start()


def _handle_mqtt_restart(client, msg):
    """处理 MQTT 重启消息（在独立线程中运行）"""
    try:
        data = msg.payload.decode('utf-8', errors='replace')
        logger.info('[MQTT] 收到消息 topic=%s payload=%s', msg.topic, data)

        # 解析请求 JSON（参照 main_server.cpp 的消息格式）
        try:
            jin = json.loads(data)
        except json.JSONDecodeError:
            logger.error('[MQTT] 无效 JSON: %s', data)
            return

        version = jin.get('version', '1.0.1')
        seq = jin.get('seq', '')
        src_addr = jin.get('srcAddr', '')

        # 执行重启：先停止再启动
        with _proc_op_lock:
            with _main_output_lock:
                _main_output.append('[控制面板] 收到 MQTT 重启指令，正在执行...')
            ok_stop, msg_stop = _stop_main_process_nolock()
            if ok_stop:
                time.sleep(1)
                with _main_output_lock:
                    _main_output.append('[控制面板] 停止成功，正在重新启动...')
                ok_start, msg_start = _start_main_process_nolock()
                if not ok_start:
                    with _main_output_lock:
                        _main_output.append(f'[控制面板] 启动失败: {msg_start}')
            else:
                ok_start, msg_start = False, f'停止失败: {msg_stop}'
                with _main_output_lock:
                    _main_output.append(f'[控制面板] 停止失败: {msg_stop}')

        success = ok_stop and ok_start
        code = 0 if success else -1
        detail = msg_start if ok_stop else msg_stop
        logger.info('[MQTT] 重启结果: success=%s, detail=%s', success, detail)

        # 构建响应（参照 main_server.cpp 的响应格式）
        resp = {
            'version': version,
            'seq': seq,
            'srcAddr': src_addr,
            'ack': {'code': code},
            'payload': {
                'code': code,
                'message': 'success' if success else 'fail',
                'detail': detail,
            },
        }

        # 发布响应: agv/v1/c/{srcAddr}/cluster/nodes/{node_id}/program/restart
        raw_topic = msg.topic
        if raw_topic.startswith(AGV_REQUEST_PREFIX):
            raw_topic = raw_topic[len(AGV_REQUEST_PREFIX):]
        pub_topic = f'{AGV_RESPONSE_PREFIX}{src_addr}/{raw_topic}' if src_addr else f'{AGV_RESPONSE_PREFIX}{raw_topic}'
        client.publish(pub_topic, json.dumps(resp, ensure_ascii=False), qos=1)
        logger.info('[MQTT] 响应已发布到 %s', pub_topic)

    except Exception:
        logger.exception('[MQTT] 处理重启消息时异常')


def _start_mqtt_client_once():
    """读取 glob_config.json 中的 mqtt_params，启动 MQTT 客户端"""
    global _mqtt_client, _mqtt_node_id

    if not _HAS_PAHO:
        logger.warning('[MQTT] paho-mqtt 未安装，跳过 MQTT 订阅')
        return

    # 读取 glob_config
    glob_path = cfg_path('glob_config_path')
    broker_uri = '127.0.0.1'
    broker_port = 1883
    client_id = f'monitor_brighteyes_{_mqtt_node_id}_{int(time.time())}'

    if glob_path and os.path.isfile(glob_path):
        try:
            with open(glob_path, 'r', encoding='utf-8') as f:
                gc = json.load(f)
            mp = gc.get('glob_config', {}).get('mqtt_params', {})
            broker_uri = mp.get('URI', broker_uri)
            broker_port = int(mp.get('Port', broker_port))
            _mqtt_node_id = gc.get('glob_config', {}).get('node_id', _mqtt_node_id)
            client_id = f'monitor_brighteyes_{_mqtt_node_id}_{int(time.time())}'
        except Exception as e:
            logger.warning('[MQTT] 读取 glob_config 失败: %s，使用默认值', e)
    else:
        logger.warning('[MQTT] glob_config 不存在: %s，使用默认值', glob_path)

    try:
        _mqtt_client = paho_mqtt.Client(client_id=client_id, clean_session=True)
        _mqtt_client.on_connect = _mqtt_on_connect
        _mqtt_client.on_disconnect = _mqtt_on_disconnect
        _mqtt_client.on_message = _mqtt_on_message
        _mqtt_client.reconnect_delay_set(min_delay=1, max_delay=30)
        _mqtt_client.connect_async(broker_uri, broker_port, keepalive=60)
        _mqtt_client.loop_start()  # 后台线程维持连接并自动重连
        logger.info('[MQTT] 正在连接 %s:%d (clientId=%s, node_id=%s)',
                    broker_uri, broker_port, client_id, _mqtt_node_id)
    except Exception as e:
        logger.error('[MQTT] 启动 MQTT 客户端失败: %s', e)
        _mqtt_client = None


# ─── API: 控制面板配置 ────────────────────────────────────────────────────────

@app.route('/api/config/panel', methods=['GET'])
def api_panel_config_get():
    return jsonify(_config)


@app.route('/api/config/panel', methods=['POST'])
def api_panel_config_post():
    data = request.get_json(force=True)
    if not isinstance(data, dict):
        return jsonify({'ok': False, 'msg': '无效请求体'}), 400
    # 只允许更新已知键，防止注入任意配置
    allowed = set(DEFAULT_CONFIG.keys())
    filtered = {k: v for k, v in data.items() if k in allowed}
    save_panel_config(filtered)
    return jsonify({'ok': True})


@app.route('/api/config/panel/open-file', methods=['POST'])
def api_panel_config_open_file():
    config_path = Path(CONFIG_FILE).expanduser().resolve()
    try:
        config_path.parent.mkdir(parents=True, exist_ok=True)
        if not config_path.exists():
            save_panel_config({})

        opened = False
        msg = '配置文件已准备'
        if shutil.which('xdg-open'):
            subprocess.Popen(['xdg-open', str(config_path)], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            opened = True
            msg = '已尝试打开配置文件'
        else:
            msg = '未找到 xdg-open，请手动打开配置文件'

        return jsonify({'ok': True, 'opened': opened, 'path': str(config_path), 'msg': msg})
    except Exception as e:
        return jsonify({'ok': False, 'msg': str(e), 'path': str(config_path)}), 500


@app.route('/api/config/panel/download', methods=['GET'])
def api_panel_config_download():
    """下载控制面板配置文件到浏览器"""
    config_path = Path(CONFIG_FILE).expanduser().resolve()
    if not config_path.exists():
        return jsonify({'ok': False, 'msg': '配置文件不存在'}), 404
    return send_file(str(config_path), as_attachment=True, download_name='control_panel_config.json', mimetype='application/json')


# ─── API: 全局配置 ────────────────────────────────────────────────────────────

@app.route('/api/config/glob', methods=['GET'])
def api_glob_config_get():
    path = cfg_path('glob_config_path')
    if not path or not os.path.isfile(path):
        return jsonify({'error': f'配置文件不存在: {path}'}), 404
    with open(path, 'r', encoding='utf-8') as f:
        return jsonify(json.load(f))


@app.route('/api/config/glob', methods=['POST'])
def api_glob_config_post():
    global _mqtt_node_id
    path = cfg_path('glob_config_path')
    if not path or not os.path.isfile(path):
        return jsonify({'ok': False, 'msg': f'配置文件不存在: {path}'}), 404
    data = request.get_json(force=True)
    if not isinstance(data, dict):
        return jsonify({'ok': False, 'msg': '无效JSON'}), 400
    # 备份原文件
    backup = path + '.bak'
    try:
        import shutil
        shutil.copy2(path, backup)
        with open(path, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent='\t', ensure_ascii=False)

        # 检测 node_id 是否变更，若变更则重新订阅 MQTT topic
        new_node_id = data.get('glob_config', {}).get('node_id', '')
        if new_node_id and new_node_id != _mqtt_node_id and _mqtt_client is not None:
            old_topic = f'{AGV_REQUEST_PREFIX}cluster/nodes/{_mqtt_node_id}/restart_node'
            _mqtt_node_id = new_node_id
            new_topic = f'{AGV_REQUEST_PREFIX}cluster/nodes/{_mqtt_node_id}/restart_node'
            try:
                _mqtt_client.unsubscribe(old_topic)
                _mqtt_client.subscribe(new_topic, qos=1)
                logger.info('[MQTT] node_id 已变更，重新订阅: %s -> %s', old_topic, new_topic)
            except Exception as e:
                logger.warning('[MQTT] 重新订阅失败: %s', e)

        return jsonify({'ok': True, 'backup': backup})
    except Exception as e:
        return jsonify({'ok': False, 'msg': str(e)}), 500


@app.route('/api/config/glob/test_mqtt', methods=['POST'])
def api_glob_config_test_mqtt():
    """测试指定 MQTT Broker 代理服务器的 TCP/Socket 联通性"""
    data = request.get_json(force=True) or {}
    uri = data.get('uri', '').strip()
    port_val = data.get('port')
    
    if not uri:
        return jsonify({'ok': False, 'msg': 'MQTT地址不能为空'})
    try:
        port = int(port_val) if port_val is not None else 1883
    except ValueError:
        return jsonify({'ok': False, 'msg': '端口必须是有效的整数'})
        
    try:
        # 去掉协议前缀（如 tcp://, mqtt://, ws://）
        if '://' in uri:
            uri = uri.split('://')[-1]
        # 去掉偶尔写错的路径/尾缀
        uri = uri.split('/')[0].split(':')[0]
        
        # 尝试使用 socket 进行 TCP 握手，超时设为 4s 避免过度卡住
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(4.0)
        sock.connect((uri, port))
        sock.close()
        return jsonify({'ok': True, 'msg': f'联通测试成功！TCP 握手成功通过 {uri}:{port}'})
    except socket.timeout:
        return jsonify({'ok': False, 'msg': f'联通测试超时 (4s)。无法连接至 {uri}:{port}'})
    except Exception as e:
        return jsonify({'ok': False, 'msg': f'连接失败: {str(e)}'})


@app.route('/api/config/glob/backup', methods=['POST'])
def api_glob_config_backup():
    path = cfg_path('glob_config_path')
    if not path or not os.path.isfile(path):
        return jsonify({'ok': False, 'msg': f'配置文件不存在: {path}'}), 404

    ts = time.strftime('%Y%m%d_%H%M%S')
    backup = f"{path}.bak_{ts}"
    try:
        import shutil
        shutil.copy2(path, backup)
        return jsonify({'ok': True, 'backup': backup})
    except Exception as e:
        return jsonify({'ok': False, 'msg': str(e)}), 500


@app.route('/api/config/glob/export', methods=['GET'])
def api_glob_config_export():
    path = cfg_path('glob_config_path')
    if not path or not os.path.isfile(path):
        return jsonify({'ok': False, 'msg': f'配置文件不存在: {path}'}), 404

    try:
        export_name = f"glob_config_{time.strftime('%Y%m%d_%H%M%S')}.json"
        return send_file(path, as_attachment=True, download_name=export_name, mimetype='application/json')
    except Exception as e:
        return jsonify({'ok': False, 'msg': str(e)}), 500


@app.route('/api/config/camera', methods=['GET'])
def api_camera_config_get():
    path = cfg_path('camera_config_path')
    if not path or not os.path.isfile(path):
        return jsonify({'error': f'相机配置文件不存在: {path}'}), 404
    with open(path, 'r', encoding='utf-8') as f:
        return jsonify(json.load(f))


@app.route('/api/config/camera', methods=['POST'])
def api_camera_config_post():
    path = cfg_path('camera_config_path')
    if not path or not os.path.isfile(path):
        return jsonify({'ok': False, 'msg': f'相机配置文件不存在: {path}'}), 404

    data = request.get_json(force=True)
    if not isinstance(data, dict):
        return jsonify({'ok': False, 'msg': '无效JSON'}), 400

    camera_list = data.get('camera_config')
    if not isinstance(camera_list, list):
        return jsonify({'ok': False, 'msg': 'camera_config 必须为数组'}), 400

    backup = path + '.bak'
    try:
        import shutil
        shutil.copy2(path, backup)
        with open(path, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent='\t', ensure_ascii=False)
        return jsonify({'ok': True, 'backup': backup})
    except Exception as e:
        return jsonify({'ok': False, 'msg': str(e)}), 500


@app.route('/api/logs/open-folder', methods=['POST'])
def api_logs_open_folder():
    log_dir = _target_log_dir()
    log_dir.mkdir(parents=True, exist_ok=True)

    opened = False
    msg = '日志目录已准备'
    try:
        if shutil.which('xdg-open'):
            subprocess.Popen(['xdg-open', str(log_dir)], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            opened = True
            msg = '已尝试打开日志目录'
        else:
            msg = '未找到 xdg-open，请手动打开目录'
    except Exception as e:
        msg = f'打开失败: {e}'

    return jsonify({'ok': True, 'opened': opened, 'path': str(log_dir), 'msg': msg})


@app.route('/api/logs/list', methods=['GET'])
def api_logs_list():
    """列出日志目录下的文件，支持远程浏览"""
    log_dir = _target_log_dir()
    sub = (request.args.get('sub') or '').strip().replace('\\', '/')
    # 防止路径穿越
    if '..' in sub:
        return jsonify({'ok': False, 'msg': '非法路径'}), 400
    target = (log_dir / sub).resolve()
    if not str(target).startswith(str(log_dir.resolve())):
        return jsonify({'ok': False, 'msg': '非法路径'}), 400
    if not target.exists():
        return jsonify({'ok': True, 'path': sub, 'entries': []})

    entries = []
    try:
        for p in sorted(target.iterdir(), key=lambda x: (x.is_file(), x.name)):
            rel = p.relative_to(log_dir)
            entry = {
                'name': p.name,
                'path': str(rel).replace('\\', '/'),
                'is_dir': p.is_dir(),
            }
            if p.is_file():
                entry['size'] = p.stat().st_size
                entry['mtime'] = int(p.stat().st_mtime)
            entries.append(entry)
    except Exception as e:
        return jsonify({'ok': False, 'msg': str(e)}), 500

    return jsonify({'ok': True, 'path': sub, 'entries': entries})


@app.route('/api/logs/download-file', methods=['GET'])
def api_logs_download_file():
    """下载日志目录下的单个文件"""
    rel_path = (request.args.get('path') or '').strip().replace('\\', '/')
    if not rel_path or '..' in rel_path:
        return jsonify({'ok': False, 'msg': '非法路径'}), 400

    log_dir = _target_log_dir()
    file_path = (log_dir / rel_path).resolve()
    if not str(file_path).startswith(str(log_dir.resolve())):
        return jsonify({'ok': False, 'msg': '非法路径'}), 400
    if not file_path.exists() or not file_path.is_file():
        return jsonify({'ok': False, 'msg': '文件不存在'}), 404

    return send_file(str(file_path), as_attachment=True, download_name=file_path.name)


@app.route('/api/logs/export', methods=['POST'])
def api_logs_export():
    log_root = _target_log_dir()

    ts = time.strftime('%Y%m%d_%H%M%S')
    zip_name = f'log_{ts}.zip'
    zip_path = log_root / zip_name

    try:
        log_root.mkdir(parents=True, exist_ok=True)
        with zipfile.ZipFile(zip_path, 'w', compression=zipfile.ZIP_DEFLATED) as zf:
            for p in log_root.rglob('*'):
                if not p.is_file() or p.resolve() == zip_path.resolve():
                    continue
                rel = p.relative_to(log_root)
                zf.write(p, arcname=str(Path('log') / rel))

        return jsonify({
            'ok': True,
            'zip_path': str(zip_path),
            'download_url': f'/api/logs/download?name={zip_name}',
        })
    except Exception as e:
        return jsonify({'ok': False, 'msg': str(e)}), 500


@app.route('/api/logs/download', methods=['GET'])
def api_logs_download():
    name = (request.args.get('name') or '').strip()
    if not name or '/' in name or '\\' in name or '..' in name or not name.endswith('.zip'):
        return jsonify({'ok': False, 'msg': '非法文件名'}), 400

    file_path = _target_log_dir() / name
    if not file_path.exists() or not file_path.is_file():
        return jsonify({'ok': False, 'msg': '文件不存在'}), 404

    return send_file(str(file_path), as_attachment=True, download_name=name, mimetype='application/zip')


# ─── API: 进程控制 ────────────────────────────────────────────────────────────

@app.route('/api/process/status', methods=['GET'])
def api_process_status():
    stats = _get_main_proc_stats()
    _sync_external_output_notice(stats)
    stats['net'] = _get_net_rate()
    stats['sys'] = _get_sys_stats()
    return jsonify(stats)


@app.route('/api/process/start', methods=['POST'])
def api_process_start():
    with _proc_op_lock:
        ok, msg = _start_main_process_nolock()
    return jsonify({'ok': ok, 'msg': msg}), (200 if ok else 400)


@app.route('/api/process/stop', methods=['POST'])
def api_process_stop():
    with _proc_op_lock:
        ok, msg = _stop_main_process_nolock()
    return jsonify({'ok': ok, 'msg': msg}), (200 if ok else 400)


@app.route('/api/process/output', methods=['GET'])
def api_process_output():
    """获取主程序输出日志（支持增量拉取）"""
    offset = request.args.get('offset', 0, type=int)
    _sync_external_output_notice()
    with _main_output_lock:
        total = len(_main_output)
        if offset >= total:
            lines = []
        else:
            lines = _main_output[offset:]
        return jsonify({'lines': lines, 'offset': total})


@app.route('/api/process/output/clear', methods=['POST'])
def api_process_output_clear():
    global _external_output_notice_pid
    with _main_output_lock:
        _main_output.clear()
        _external_output_notice_pid = None
    return jsonify({'ok': True})


CONTROL_PANEL_SERVICE_NAME = "bevp-control-panel.service"


@app.route('/api/monitor/restart', methods=['POST'])
def api_monitor_restart():
    """重启监控服务（控制面板自身的 systemd 服务）"""
    try:
        # 检查服务是否存在
        check = subprocess.run(
            ['systemctl', 'is-active', CONTROL_PANEL_SERVICE_NAME],
            capture_output=True, text=True, timeout=5,
        )
        if check.returncode != 0 and 'inactive' not in check.stdout:
            return jsonify({'ok': False, 'msg': f'监控服务 {CONTROL_PANEL_SERVICE_NAME} 未安装或状态异常'}), 400

        # 使用 systemctl restart 重启
        sudo = [] if os.geteuid() == 0 else ['sudo', '-n']
        result = subprocess.run(
            sudo + ['systemctl', 'restart', CONTROL_PANEL_SERVICE_NAME],
            capture_output=True, text=True, timeout=15,
        )
        if result.returncode != 0:
            err = result.stderr.strip()
            if 'password' in err.lower() or 'sudo' in err.lower():
                return jsonify({'ok': False, 'msg': '权限不足，请在安装脚本中配置 sudoers 免密规则（见 install_control_panel_service.sh）'}), 403
            return jsonify({'ok': False, 'msg': f'重启失败: {err}'}), 500
        return jsonify({'ok': True, 'msg': '监控服务正在重启'})
    except subprocess.TimeoutExpired:
        return jsonify({'ok': True, 'msg': '监控服务正在重启（命令超时，可能因为服务自身被重启）'})
    except Exception as e:
        return jsonify({'ok': False, 'msg': f'重启监控服务异常: {e}'}), 500


# ─── API: Web 终端 ────────────────────────────────────────────────────────────

import base64
import struct
import fcntl
import termios
import uuid as _uuid

_web_terminals: dict[str, dict] = {}  # id -> {fd, pid, created}
_WEB_TERM_MAX = 4  # 最多同时开 4 个终端


def _cleanup_dead_terminals():
    """清理已退出的终端会话"""
    dead = []
    for tid, info in _web_terminals.items():
        try:
            pid_result = os.waitpid(info['pid'], os.WNOHANG)
            if pid_result[0] != 0:
                dead.append(tid)
        except ChildProcessError:
            dead.append(tid)
    for tid in dead:
        info = _web_terminals.pop(tid, None)
        if info:
            try:
                os.close(info['fd'])
            except OSError:
                pass


@app.route('/api/terminal/create', methods=['POST'])
def api_terminal_create():
    """创建一个新的 PTY 终端会话"""
    _cleanup_dead_terminals()
    if len(_web_terminals) >= _WEB_TERM_MAX:
        return jsonify({'ok': False, 'msg': f'终端数量已达上限({_WEB_TERM_MAX})'}), 400

    # 在 base_dir 下启动 shell
    work_dir = str(Path(cfg('base_dir', SCRIPT_DIR)).expanduser().resolve())
    shell = os.environ.get('SHELL', '/bin/bash')

    pid, fd = pty.fork()
    if pid == 0:
        # 子进程
        os.chdir(work_dir)
        os.execvpe(shell, [shell, '--login'], os.environ)
    else:
        # 父进程：设置非阻塞
        flags = fcntl.fcntl(fd, fcntl.F_GETFL)
        fcntl.fcntl(fd, fcntl.F_SETFL, flags | os.O_NONBLOCK)

        tid = str(_uuid.uuid4())
        _web_terminals[tid] = {'fd': fd, 'pid': pid, 'created': time.time()}
        logger.info('[WebTerm] 已创建终端 id=%s pid=%d', tid, pid)
        return jsonify({'ok': True, 'id': tid})


@app.route('/api/terminal/input/<tid>', methods=['POST'])
def api_terminal_input(tid):
    """向终端发送输入"""
    info = _web_terminals.get(tid)
    if not info:
        return jsonify({'ok': False, 'msg': '终端不存在'}), 404

    data = request.get_data()
    try:
        os.write(info['fd'], data)
    except OSError as e:
        return jsonify({'ok': False, 'msg': f'写入失败: {e}'}), 500
    return jsonify({'ok': True})


@app.route('/api/terminal/resize/<tid>', methods=['POST'])
def api_terminal_resize(tid):
    """调整终端大小"""
    info = _web_terminals.get(tid)
    if not info:
        return jsonify({'ok': False, 'msg': '终端不存在'}), 404

    body = request.get_json(force=True)
    cols = int(body.get('cols', 80))
    rows = int(body.get('rows', 24))
    try:
        winsize = struct.pack('HHHH', rows, cols, 0, 0)
        fcntl.ioctl(info['fd'], termios.TIOCSWINSZ, winsize)
    except OSError:
        pass
    return jsonify({'ok': True})


@app.route('/api/terminal/stream/<tid>')
def api_terminal_stream(tid):
    """SSE 流式输出终端内容"""
    info = _web_terminals.get(tid)
    if not info:
        return jsonify({'ok': False, 'msg': '终端不存在'}), 404

    def generate():
        fd = info['fd']
        while True:
            try:
                r, _, _ = select.select([fd], [], [], 0.5)
                if r:
                    data = os.read(fd, 4096)
                    if not data:
                        yield f"event: exit\ndata: closed\n\n"
                        break
                    # 用 base64 编码避免 SSE 换行问题
                    encoded = base64.b64encode(data).decode('ascii')
                    yield f"data: {encoded}\n\n"
            except OSError:
                yield f"event: exit\ndata: closed\n\n"
                break

    return Response(
        stream_with_context(generate()),
        mimetype='text/event-stream',
        headers={'Cache-Control': 'no-cache', 'X-Accel-Buffering': 'no'},
    )


@app.route('/api/terminal/destroy/<tid>', methods=['POST'])
def api_terminal_destroy(tid):
    """销毁终端会话"""
    info = _web_terminals.pop(tid, None)
    if not info:
        return jsonify({'ok': False, 'msg': '终端不存在'}), 404
    try:
        os.kill(info['pid'], signal.SIGTERM)
    except OSError:
        pass
    try:
        os.close(info['fd'])
    except OSError:
        pass
    logger.info('[WebTerm] 已销毁终端 id=%s', tid)
    return jsonify({'ok': True})


@app.route('/api/terminal/list', methods=['GET'])
def api_terminal_list():
    """列出活跃终端"""
    _cleanup_dead_terminals()
    terms = [{'id': k, 'created': v['created']} for k, v in _web_terminals.items()]
    return jsonify({'ok': True, 'terminals': terms})


# ─── API: SSE 实时状态流 ──────────────────────────────────────────────────────

@app.route('/api/stats/stream')
def api_stats_stream():
    def generate():
        while True:
            time.sleep(2)
            try:
                stats = _get_main_proc_stats()
                stats['net'] = _get_net_rate()
                stats['gpu'] = _get_gpu_stats()
                stats['sys'] = _get_sys_stats()
                yield f"data: {json.dumps(stats, ensure_ascii=False)}\n\n"
            except Exception as e:
                yield f"data: {json.dumps({'error': str(e)})}\n\n"

    return Response(
        stream_with_context(generate()),
        mimetype='text/event-stream',
        headers={'Cache-Control': 'no-cache', 'X-Accel-Buffering': 'no'},
    )


# ─── API: 线库标定 ────────────────────────────────────────────────────────────

@app.route('/api/fs/list', methods=['GET'])
def api_fs_list():
    """列出目录内容，用于前端路径选择。"""
    req_path = (request.args.get('path') or '').strip()
    mode = (request.args.get('mode') or 'all').strip().lower()  # all | file | dir
    current = _safe_resolve_path(req_path)

    if not current.exists():
        return jsonify({'ok': False, 'msg': f'路径不存在: {current}'}), 404
    if not current.is_dir():
        current = current.parent

    try:
        entries = []
        for item in current.iterdir():
            is_dir = item.is_dir()
            if mode == 'file' and is_dir:
                pass
            elif mode == 'dir' and not is_dir:
                pass
            else:
                entries.append({
                    'name': item.name,
                    'path': str(item),
                    'type': 'dir' if is_dir else 'file',
                })
        entries.sort(key=lambda x: (0 if x['type'] == 'dir' else 1, x['name'].lower()))
        parent = str(current.parent) if current.parent != current else str(current)
        return jsonify({'ok': True, 'cwd': str(current), 'parent': parent, 'entries': entries})
    except Exception as e:
        return jsonify({'ok': False, 'msg': str(e)}), 500


# ─── API: 线库标定 ────────────────────────────────────────────────────────────

@app.route('/api/calibration/start', methods=['POST'])
def api_calibration_start():
    global _calib_proc, _calib_output
    prog = cfg_path('calibration_program')
    work_dir = cfg('base_dir', '.')

    if not prog or not os.path.isfile(prog):
        return jsonify({'ok': False, 'msg': f'标定程序不存在: {prog}'}), 400

    if _calib_proc and _calib_proc.poll() is None:
        return jsonify({'ok': False, 'msg': '标定程序已在运行'})

    with _calib_lock:
        _calib_output = [f'[启动] {prog}', '']

    def _run():
        global _calib_proc
        try:
            master_fd, slave_fd = pty.openpty()
            env = os.environ.copy()
            lib_path = os.path.join(work_dir, 'lib')
            env['LD_LIBRARY_PATH'] = lib_path + ':' + env.get('LD_LIBRARY_PATH', '')
            _calib_proc = subprocess.Popen(
                [prog],
                cwd=work_dir,
                stdout=slave_fd,
                stderr=slave_fd,
                stdin=subprocess.DEVNULL,
                close_fds=True,
                env=env,
            )
            os.close(slave_fd)
            logger.info('标定程序已启动: %s (pid=%d)', prog, _calib_proc.pid)

            buf = ''
            while True:
                try:
                    ready, _, _ = select.select([master_fd], [], [], 0.5)
                    if ready:
                        data = os.read(master_fd, 4096)
                        if not data:
                            break
                        text = data.decode('utf-8', errors='replace')
                        buf += text
                        while '\n' in buf or '\r' in buf:
                            if '\n' in buf:
                                line, buf = buf.split('\n', 1)
                                line = line.rstrip('\r')
                            else:
                                line, buf = buf.rsplit('\r', 1)
                            if line:
                                with _calib_lock:
                                    _calib_output.append(line)
                                    if len(_calib_output) > 2000:
                                        _calib_output[:] = _calib_output[-1000:]
                    elif _calib_proc.poll() is not None:
                        break
                except OSError:
                    break

            if buf.strip():
                with _calib_lock:
                    _calib_output.append(buf.strip())

            os.close(master_fd)
            _calib_proc.wait()
            rc = _calib_proc.returncode
            status = '正常退出' if rc == 0 else f'异常退出 (返回码 {rc})'
            with _calib_lock:
                _calib_output.append('')
                _calib_output.append(f'[标定程序{status}]')
            logger.info('标定程序已退出，返回码: %d', rc)
        except Exception as e:
            with _calib_lock:
                _calib_output.append(f'[错误] {e}')
            logger.exception('标定程序运行异常')

    threading.Thread(target=_run, daemon=True, name='calib-proc').start()
    return jsonify({'ok': True, 'msg': f'标定程序已启动: {prog}'})


@app.route('/api/calibration/stop', methods=['POST'])
def api_calibration_stop():
    global _calib_proc
    if _calib_proc is None or _calib_proc.poll() is not None:
        return jsonify({'ok': False, 'msg': '标定程序未在运行'})
    try:
        pid = _calib_proc.pid
        try:
            os.killpg(os.getpgid(pid), signal.SIGTERM)
        except (ProcessLookupError, PermissionError):
            _calib_proc.terminate()
        try:
            _calib_proc.wait(timeout=5)
        except subprocess.TimeoutExpired:
            try:
                os.killpg(os.getpgid(pid), signal.SIGKILL)
            except Exception:
                _calib_proc.kill()
        logger.info('标定程序已被用户关闭 (pid=%d)', pid)
        return jsonify({'ok': True, 'msg': '标定程序已关闭'})
    except Exception as e:
        logger.exception('关闭标定程序失败')
        return jsonify({'ok': False, 'msg': str(e)}), 500


@app.route('/api/calibration/status', methods=['GET'])
def api_calibration_status():
    running = _calib_proc is not None and _calib_proc.poll() is None
    returncode = _calib_proc.returncode if _calib_proc and not running else None
    pid = _calib_proc.pid if _calib_proc else None
    with _calib_lock:
        output = list(_calib_output)
    return jsonify({'running': running, 'returncode': returncode, 'pid': pid, 'output': output})


# ─── API: 模型转换 ────────────────────────────────────────────────────────────

# 模型类型编号 → 字符串名称
MODEL_TYPE_MAP = {0: 'yolov5', 4: 'yolov12', 5: 'yolo26', 6: 'dfine'}
DEFAULT_NAMES_CONTENT = "Goods\nForklift\nHuman\n"


@app.route('/api/model/convert', methods=['POST'])
def api_model_convert():
    global _conv_proc, _conv_output
    data = request.get_json(force=True)

    input_file = (data.get('input_file') or '').strip()
    model_type = int(data.get('model_type', 0))
    precision = int(data.get('precision', 1))
    batch_size = int(data.get('batch_size', 4))
    backup_enabled = bool(data.get('backup_enabled', False))

    if not input_file:
        return jsonify({'ok': False, 'msg': '请填写输入 ONNX 文件路径'}), 400

    if model_type not in MODEL_TYPE_MAP:
        return jsonify({'ok': False, 'msg': f'模型类型仅支持: {", ".join(f"{v}({k})" for k, v in MODEL_TYPE_MAP.items())}'}), 400

    if precision not in (0, 1):
        return jsonify({'ok': False, 'msg': '精度必须为 0(FP32) / 1(FP16)'}), 400

    if batch_size < 1 or batch_size > 64:
        return jsonify({'ok': False, 'msg': 'batch_size 范围: 1~64'}), 400

    prog = cfg_path('model_conversion_program')
    work_dir = cfg('base_dir', '.')

    if not prog or not os.path.isfile(prog):
        return jsonify({'ok': False, 'msg': f'模型转换程序不存在: {prog}'}), 400

    if _conv_proc and _conv_proc.poll() is None:
        return jsonify({'ok': False, 'msg': '已有转换任务正在进行中'}), 409

    # 自动生成输出路径: base_dir/model/{模型类型}_model/best.trtmodel
    type_name = MODEL_TYPE_MAP[model_type]
    model_dir = Path(work_dir) / 'model' / f'{type_name}_model'
    output_path = model_dir / 'best.trtmodel'

    pre_output_msgs: list[str] = []

    # 确保目标目录存在
    model_dir.mkdir(parents=True, exist_ok=True)
    pre_output_msgs.append(f'[提示] 输出目录: {model_dir}')

    # 确保 best.names 存在
    names_path = model_dir / 'best.names'
    if not names_path.exists():
        names_path.write_text(DEFAULT_NAMES_CONTENT, encoding='utf-8')
        pre_output_msgs.append(f'[提示] 已创建默认类别文件: {names_path}')

    # 处理旧模型文件
    if output_path.exists():
        if backup_enabled:
            ts = time.strftime('%Y%m%d_%H%M%S')
            backup_path = model_dir / f'best_{ts}.trtmodel.bak'
            try:
                import shutil
                shutil.move(str(output_path), str(backup_path))
                pre_output_msgs.append(f'[提示] 已备份旧模型: {backup_path}')
            except Exception as e:
                return jsonify({'ok': False, 'msg': f'备份旧模型失败: {e}'}), 500
        else:
            try:
                output_path.unlink()
                pre_output_msgs.append(f'[提示] 已删除旧模型: {output_path}')
            except Exception as e:
                return jsonify({'ok': False, 'msg': f'删除旧模型失败: {e}'}), 500

    output_file = str(output_path)
    cmd = [prog, input_file, output_file, str(precision), str(batch_size), str(model_type)]

    with _conv_lock:
        _conv_output = pre_output_msgs + [f'[命令] {" ".join(cmd)}', '']

    def _run():
        global _conv_proc
        try:
            # 使用伪终端(pty)让子进程以为连接到真实终端，
            # 强制行缓冲，捕获所有直接打印到终端的输出
            master_fd, slave_fd = pty.openpty()
            env = os.environ.copy()
            lib_path = os.path.join(work_dir, 'lib')
            env['LD_LIBRARY_PATH'] = lib_path + ':' + env.get('LD_LIBRARY_PATH', '')
            _conv_proc = subprocess.Popen(
                cmd,
                cwd=work_dir,
                stdout=slave_fd,
                stderr=slave_fd,
                stdin=subprocess.DEVNULL,
                close_fds=True,
                env=env,
            )
            os.close(slave_fd)  # 父进程关闭 slave 端

            # 从 master 端读取子进程输出
            buf = ''
            while True:
                try:
                    ready, _, _ = select.select([master_fd], [], [], 0.5)
                    if ready:
                        data = os.read(master_fd, 4096)
                        if not data:
                            break
                        text = data.decode('utf-8', errors='replace')
                        # 处理 \r 进度条：按 \r 或 \n 分割
                        buf += text
                        while '\n' in buf or '\r' in buf:
                            # 优先按 \n 分割
                            if '\n' in buf:
                                line, buf = buf.split('\n', 1)
                                line = line.rstrip('\r')
                            else:
                                line, buf = buf.rsplit('\r', 1)
                            if line:
                                with _conv_lock:
                                    _conv_output.append(line)
                                    if len(_conv_output) > 2000:
                                        _conv_output[:] = _conv_output[-1000:]
                    elif _conv_proc.poll() is not None:
                        break
                except OSError:
                    break

            # 处理残余 buffer
            if buf.strip():
                with _conv_lock:
                    _conv_output.append(buf.strip())

            os.close(master_fd)
            _conv_proc.wait()
            rc = _conv_proc.returncode
            status = '成功' if rc == 0 else f'失败 (返回码 {rc})'
            with _conv_lock:
                _conv_output.append('')
                _conv_output.append(f'[转换{status}]')
            logger.info("模型转换完成，返回码: %d", rc)
        except Exception as e:
            with _conv_lock:
                _conv_output.append(f'[错误] {e}')
            logger.exception("模型转换异常")

    threading.Thread(target=_run, daemon=True).start()
    return jsonify({'ok': True, 'msg': f'模型转换已启动: {" ".join(cmd)}'})


@app.route('/api/model/output', methods=['GET'])
def api_model_output():
    with _conv_lock:
        running = _conv_proc is not None and _conv_proc.poll() is None
        rc = _conv_proc.returncode if _conv_proc and not running else None
        return jsonify({
            'running': running,
            'returncode': rc,
            'output': list(_conv_output),
        })


@app.route('/api/model/stop', methods=['POST'])
def api_model_stop():
    if _conv_proc and _conv_proc.poll() is None:
        _conv_proc.terminate()
        return jsonify({'ok': True, 'msg': '已停止转换任务'})
    return jsonify({'ok': False, 'msg': '无转换任务运行中'})


@app.route('/api/model/names', methods=['GET'])
def api_model_names_get():
    """读取指定模型类型的 best.names 文件"""
    model_type = request.args.get('model_type', 0, type=int)
    if model_type not in MODEL_TYPE_MAP:
        return jsonify({'ok': False, 'msg': '无效模型类型'}), 400
    type_name = MODEL_TYPE_MAP[model_type]
    work_dir = cfg('base_dir', '.')
    names_path = Path(work_dir) / 'model' / f'{type_name}_model' / 'best.names'
    if not names_path.exists():
        return jsonify({'ok': True, 'content': DEFAULT_NAMES_CONTENT.strip(), 'exists': False})
    content = names_path.read_text(encoding='utf-8')
    return jsonify({'ok': True, 'content': content.strip(), 'exists': True, 'path': str(names_path)})


@app.route('/api/model/names', methods=['POST'])
def api_model_names_post():
    """保存指定模型类型的 best.names 文件"""
    data = request.get_json(force=True)
    model_type = int(data.get('model_type', 0))
    content = data.get('content', '')
    if model_type not in MODEL_TYPE_MAP:
        return jsonify({'ok': False, 'msg': '无效模型类型'}), 400
    if not isinstance(content, str):
        return jsonify({'ok': False, 'msg': '内容必须为字符串'}), 400
    type_name = MODEL_TYPE_MAP[model_type]
    work_dir = cfg('base_dir', '.')
    model_dir = Path(work_dir) / 'model' / f'{type_name}_model'
    model_dir.mkdir(parents=True, exist_ok=True)
    names_path = model_dir / 'best.names'
    # 确保以换行结尾
    if content and not content.endswith('\n'):
        content += '\n'
    names_path.write_text(content, encoding='utf-8')
    return jsonify({'ok': True, 'path': str(names_path)})


# ─── API: 开机自启 ────────────────────────────────────────────────────────────

@app.route('/api/autostart/status', methods=['GET'])
def api_autostart_status():
    return jsonify(_get_autostart_status())


@app.route('/api/autostart/enable', methods=['POST'])
def api_autostart_enable():
    ok, msg = _install_autostart_service()
    if ok:
        save_panel_config({'autostart_enabled': True})
    return jsonify({'ok': ok, 'msg': msg}), (200 if ok else 500)


@app.route('/api/autostart/disable', methods=['POST'])
def api_autostart_disable():
    ok, msg = _remove_autostart_service()
    if ok:
        save_panel_config({'autostart_enabled': False})
    return jsonify({'ok': ok, 'msg': msg}), (200 if ok else 500)


# ─── API: 相机联通监测 ────────────────────────────────────────────────────────

@app.route('/api/camera/connectivity', methods=['GET'])
def api_camera_connectivity():
    """获取所有相机的联通状态"""
    return jsonify({
        'enabled': bool(cfg('camera_monitor_enabled', False)),
        'status': _camera_status,
    })


@app.route('/api/camera/monitor/toggle', methods=['POST'])
def api_camera_monitor_toggle():
    """开关相机监测"""
    data = request.get_json(force=True)
    enabled = bool(data.get('enabled', False))
    save_panel_config({'camera_monitor_enabled': enabled})
    if not enabled:
        global _camera_status
        _camera_status = {}
    return jsonify({'ok': True, 'enabled': enabled})


@app.route('/api/camera/ping', methods=['POST'])
def api_camera_ping_single():
    """手动 ping 单个相机"""
    data = request.get_json(force=True)
    ip = (data.get('ip') or '').strip()
    if not ip:
        return jsonify({'ok': False, 'msg': '缺少 IP'}), 400
    reachable = _ping_host(ip)
    _camera_status[ip] = {'reachable': reachable, 'last_check': time.time()}
    return jsonify({'ok': True, 'ip': ip, 'reachable': reachable})


# ─── API: 长期历史 ────────────────────────────────────────────────────────────

@app.route('/api/history/longterm', methods=['GET'])
def api_lt_history_get():
    with _lt_history_lock:
        data = list(_lt_history)
    return jsonify({'data': data, 'count': len(data)})


@app.route('/api/history/longterm/export', methods=['GET'])
def api_lt_history_export():
    fmt = (request.args.get('fmt') or 'csv').lower()
    with _lt_history_lock:
        data = list(_lt_history)
    ts_str = time.strftime('%Y%m%d_%H%M%S')
    if fmt == 'json':
        out = json.dumps(data, ensure_ascii=False, indent=2)
        fname = f'cpu_mem_history_{ts_str}.json'
        return Response(out, mimetype='application/json',
                        headers={'Content-Disposition': f'attachment; filename={fname}'})
    else:
        lines = ['timestamp,datetime,proc_cpu_percent,proc_mem_rss_mb,sys_cpu_percent,sys_mem_used_mb']
        for r in data:
            dt = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(r['ts']))
            lines.append(f"{r['ts']},{dt},{r['cpu']},{r['mem']},{r.get('sys_cpu','')},{r.get('sys_mem','')}")
        out = '\n'.join(lines)
        fname = f'cpu_mem_history_{ts_str}.csv'
        return Response(out, mimetype='text/csv; charset=utf-8',
                        headers={'Content-Disposition': f'attachment; filename={fname}'})


@app.route('/api/history/longterm/clear', methods=['POST'])
def api_lt_history_clear():
    global _lt_history
    with _lt_history_lock:
        _lt_history.clear()
    _save_lt_history()
    return jsonify({'ok': True})


# ─── API: 电源/断电监测 ───────────────────────────────────────────────────────

@app.route('/api/power/events', methods=['GET'])
def api_power_events():
    """获取电源事件历史（最近N条）"""
    n = request.args.get('n', 50, type=int)
    with _power_events_lock:
        events = _power_events[-n:]
    return jsonify({'events': events, 'total': len(_power_events)})


@app.route('/api/power/last_shutdown', methods=['GET'])
def api_power_last_shutdown():
    """获取上次关机类型"""
    with _power_events_lock:
        # 找最近一次 startup 事件
        for ev in reversed(_power_events):
            if ev.get('type') == 'startup':
                return jsonify(ev)
    return jsonify({'last_shutdown': 'unknown', 'detail': '无记录'})


@app.route('/api/power/events/clear', methods=['POST'])
def api_power_events_clear():
    """清空电源事件记录"""
    global _power_events
    with _power_events_lock:
        _power_events.clear()
    _save_power_events()
    return jsonify({'ok': True})


# ─── 主页 ─────────────────────────────────────────────────────────────────────

@app.route('/')
def index():
    return render_template('index.html')


@app.route('/subscription')
def subscription_monitor():
    return render_template('subscription_monitor.html')


# ─── 入口 ─────────────────────────────────────────────────────────────────────

if __name__ == '__main__':
    load_panel_config()
    _setup_file_logging()
    _load_lt_history()
    _start_power_monitor_once()
    _start_memory_guard_once()
    _start_camera_monitor_once()
    _start_systemd_log_monitor_once()
    _start_process_log_maintenance_once()
    _start_lt_sampler_once()
    _start_runtime_monitor_once()
    _start_mqtt_client_once()
    host = cfg('web_host', '0.0.0.0')
    port = int(cfg('web_port', 8888))
    logger.info("BrightEyes 控制面板启动: http://%s:%d", host, port)
    app.run(host=host, port=port, debug=False, threaded=True)
