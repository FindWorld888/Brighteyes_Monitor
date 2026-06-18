#!/usr/bin/env bash
set -euo pipefail

SERVICE_NAME="bevp-control-panel.service"
BIN_NAME="control_panel"
UNIT_FILE="/etc/systemd/system/${SERVICE_NAME}"
CTL_BIN="/usr/local/bin/cpanelctl"

THIS_DIR="$(cd "$(dirname "$0")" && pwd)"
SRC_BIN_DEFAULT="${THIS_DIR}/${BIN_NAME}"

# 按优先级查找配置文件：脚本同目录 > dist/ 子目录
_find_src_cfg() {
  local candidates=(
    "${THIS_DIR}/control_panel_config.json"
    "${THIS_DIR}/dist/control_panel_config.json"
  )
  for f in "${candidates[@]}"; do
    if [[ -f "$f" ]]; then
      echo "$f"
      return
    fi
  done
  echo ""
}
SRC_CFG_DEFAULT="$(_find_src_cfg)"

# 从配置文件读取 base_dir，作为默认安装目录
_read_base_dir_from_cfg() {
  local cfg="${1:-}"
  if [[ -z "$cfg" || ! -f "$cfg" ]]; then
    echo ""
    return
  fi
  python3 -c "
import json,sys
try:
    with open('${cfg}','r',encoding='utf-8') as f:
        print(json.load(f).get('base_dir',''))
except: print('')
" 2>/dev/null || echo ""
}

usage() {
  cat <<'EOF'
Usage:
  sudo ./install_control_panel_service.sh install [--install-dir DIR] [BIN_PATH]
  sudo ./install_control_panel_service.sh uninstall
  ./install_control_panel_service.sh status
  ./install_control_panel_service.sh logs

Options:
  --install-dir DIR  将控制面板安装到指定目录（默认读取 config 中的 base_dir）。
                     不填则自动使用 config 中 base_dir 的值（即 bevp5.0 目录）。

Examples:
  # 自动安装到 config 中 base_dir 指向的 bevp5.0 目录：
  sudo ./install_control_panel_service.sh install

  # 指定安装到自定义目录：
  sudo ./install_control_panel_service.sh install --install-dir /opt/bevp5.0

After install:
  cpanelctl start|stop|restart|status|logs|open|enable|disable
EOF
}

require_root_for_mutation() {
  local action="$1"
  if [[ "$action" == "install" || "$action" == "uninstall" ]]; then
    if [[ "${EUID}" -ne 0 ]]; then
      echo "Error: '$action' requires root. Please run with sudo."
      exit 1
    fi
  fi
}

choose_run_user() {
  if [[ -n "${SUDO_USER:-}" && "${SUDO_USER}" != "root" ]]; then
    echo "${SUDO_USER}"
    return
  fi
  local who
  who="$(logname 2>/dev/null || true)"
  if [[ -n "${who}" && "${who}" != "root" ]]; then
    echo "${who}"
  else
    echo "root"
  fi
}

install_service() {
  local src_bin=""
  local override_install_dir=""

  # 解析参数：--install-dir <path> 和可选的 BIN_PATH
  while [[ $# -gt 0 ]]; do
    case "$1" in
      --install-dir)
        shift
        override_install_dir="${1:-}"
        ;;
      *)
        src_bin="$1"
        ;;
    esac
    shift
  done

  src_bin="${src_bin:-${SRC_BIN_DEFAULT}}"

  if [[ ! -f "$src_bin" ]]; then
    echo "Error: executable not found: $src_bin"
    echo "Hint: build first with: pyinstaller --onefile --add-data \"templates:templates\" control_panel.py"
    exit 1
  fi

  # 确定安装目录：命令行指定 > config 中的 base_dir > 兜底 /opt/bevp-control-panel
  local INSTALL_DIR=""
  if [[ -n "$override_install_dir" ]]; then
    INSTALL_DIR="$override_install_dir"
  else
    INSTALL_DIR="$(_read_base_dir_from_cfg "$SRC_CFG_DEFAULT")"
    if [[ -z "$INSTALL_DIR" ]]; then
      INSTALL_DIR="/opt/bevp-control-panel"
      echo "Warning: config 中 base_dir 为空，回退到默认目录: $INSTALL_DIR"
    fi
  fi

  local run_user
  run_user="$(choose_run_user)"

  echo "Install directory: $INSTALL_DIR"
  mkdir -p "$INSTALL_DIR"
  install -m 0755 "$src_bin" "$INSTALL_DIR/$BIN_NAME"

  if [[ -n "$SRC_CFG_DEFAULT" ]]; then
    if [[ -f "$INSTALL_DIR/control_panel_config.json" ]]; then
      cp "$INSTALL_DIR/control_panel_config.json" \
         "$INSTALL_DIR/control_panel_config.json.bak"
      echo "Config backed up: $INSTALL_DIR/control_panel_config.json.bak"
    fi
    install -m 0644 "$SRC_CFG_DEFAULT" "$INSTALL_DIR/control_panel_config.json"
    echo "Config installed from: $SRC_CFG_DEFAULT"
    # 确保 config 中 base_dir 与安装目录一致
    python3 - "$INSTALL_DIR/control_panel_config.json" "$INSTALL_DIR" <<'PY'
import json, sys
cfg_file, install_dir = sys.argv[1], sys.argv[2]
with open(cfg_file, 'r', encoding='utf-8') as f:
    cfg = json.load(f)
cfg['base_dir'] = install_dir
with open(cfg_file, 'w', encoding='utf-8') as f:
    json.dump(cfg, f, indent=2, ensure_ascii=False)
print(f"[OK] config base_dir aligned to install dir: {install_dir}")
PY
  else
    echo "Warning: no control_panel_config.json found, program will use built-in defaults."
    echo "  Expected locations:"
    echo "    ${THIS_DIR}/control_panel_config.json"
    echo "    ${THIS_DIR}/dist/control_panel_config.json"
  fi

  # 权限修复：最小化调整，仅保证服务用户可读配置、可执行程序、可写日志目录
  if id "$run_user" >/dev/null 2>&1; then
    if [[ -f "$INSTALL_DIR/$BIN_NAME" ]]; then
      chown "$run_user:$run_user" "$INSTALL_DIR/$BIN_NAME" || true
      chmod 0755 "$INSTALL_DIR/$BIN_NAME" || true
    fi
    if [[ -f "$INSTALL_DIR/control_panel_config.json" ]]; then
      chown "$run_user:$run_user" "$INSTALL_DIR/control_panel_config.json" || true
      chmod 0644 "$INSTALL_DIR/control_panel_config.json" || true
    fi
    mkdir -p "$INSTALL_DIR/log/control_panel/process_output"
    chown -R "$run_user:$run_user" "$INSTALL_DIR/log/control_panel" || true
    chmod 0755 "$INSTALL_DIR/log" "$INSTALL_DIR/log/control_panel" "$INSTALL_DIR/log/control_panel/process_output" || true
    touch "$INSTALL_DIR/log/control_panel/control_panel.log" || true
    chown "$run_user:$run_user" "$INSTALL_DIR/log/control_panel/control_panel.log" || true
    chmod 0644 "$INSTALL_DIR/log/control_panel/control_panel.log" || true
  fi

  # 安装后权限自检，提前发现“能启动但读不到配置/写不了日志”的问题
  if ! sudo -u "$run_user" test -r "$INSTALL_DIR/control_panel_config.json" 2>/dev/null; then
    echo "Warning: user '$run_user' cannot read $INSTALL_DIR/control_panel_config.json"
  fi
  if ! sudo -u "$run_user" test -w "$INSTALL_DIR/log/control_panel" 2>/dev/null; then
    echo "Warning: user '$run_user' cannot write $INSTALL_DIR/log/control_panel"
  fi

  cat > "$UNIT_FILE" <<EOF
[Unit]
Description=BrightEyes Control Panel (Web)
After=network.target

[Service]
Type=simple
WorkingDirectory=${INSTALL_DIR}
ExecStart=${INSTALL_DIR}/${BIN_NAME}
Restart=always
RestartSec=3
User=${run_user}
KillMode=process
Environment=PYTHONUNBUFFERED=1

[Install]
WantedBy=multi-user.target
EOF

  # cpanelctl 需要知道实际安装目录，写入时替换
  cat > "$CTL_BIN" <<EOF
#!/usr/bin/env bash
set -euo pipefail
SERVICE_NAME="${SERVICE_NAME}"
INSTALL_DIR="${INSTALL_DIR}"
CFG_FILE="\${INSTALL_DIR}/control_panel_config.json"

port_from_cfg() {
  if [[ -f "\${CFG_FILE}" ]]; then
    python3 - "\${CFG_FILE}" <<'PY'
import json,sys
p=8888
try:
    with open(sys.argv[1],'r',encoding='utf-8') as f:
        p=int(json.load(f).get('web_port',8888))
except Exception:
    pass
print(p)
PY
  else
    echo 8888
  fi
}

cmd="\${1:-status}"
case "\$cmd" in
  start)    systemctl start "\${SERVICE_NAME}" ;;
  stop)     systemctl stop "\${SERVICE_NAME}" ;;
  restart)  systemctl restart "\${SERVICE_NAME}" ;;
  enable)   systemctl enable "\${SERVICE_NAME}" ;;
  disable)  systemctl disable "\${SERVICE_NAME}" ;;
  status)   systemctl --no-pager --full status "\${SERVICE_NAME}" || true ;;
  logs)     journalctl -u "\${SERVICE_NAME}" -n 100 --no-pager || true ;;
  open)
    port="\$(port_from_cfg)"
    url="http://127.0.0.1:\${port}"
    echo "Open Control Panel: \${url}"
    if command -v xdg-open >/dev/null 2>&1; then
      xdg-open "\$url" >/dev/null 2>&1 || true
    fi
    ;;
  *)
    echo "Usage: cpanelctl start|stop|restart|enable|disable|status|logs|open"
    exit 1
    ;;
esac
EOF
  chmod 0755 "$CTL_BIN"

  # 为服务用户配置 sudoers 免密规则，允许控制面板通过 Web 界面管理开机自启及重启自身
  local SUDOERS_FILE="/etc/sudoers.d/bevp-control-panel"
  local AUTOSTART_SVC="bevp-target-program.service"
  cat > "$SUDOERS_FILE" <<EOF
# BrightEyes Control Panel: allow service user to manage target-program autostart
${run_user} ALL=(root) NOPASSWD: /usr/bin/tee ${AUTOSTART_UNIT_PATH:-/etc/systemd/system/${AUTOSTART_SVC}}
${run_user} ALL=(root) NOPASSWD: /bin/systemctl daemon-reload
${run_user} ALL=(root) NOPASSWD: /bin/systemctl enable ${AUTOSTART_SVC}
${run_user} ALL=(root) NOPASSWD: /bin/systemctl disable --now ${AUTOSTART_SVC}
${run_user} ALL=(root) NOPASSWD: /bin/systemctl start ${AUTOSTART_SVC}
${run_user} ALL=(root) NOPASSWD: /bin/systemctl stop ${AUTOSTART_SVC}
${run_user} ALL=(root) NOPASSWD: /bin/rm -f /etc/systemd/system/${AUTOSTART_SVC}
# Allow control panel to restart itself
${run_user} ALL=(root) NOPASSWD: /bin/systemctl restart ${SERVICE_NAME}
${run_user} ALL=(root) NOPASSWD: /bin/systemctl start ${SERVICE_NAME}
${run_user} ALL=(root) NOPASSWD: /bin/systemctl stop ${SERVICE_NAME}
EOF
  chmod 0440 "$SUDOERS_FILE"
  # 验证语法，避免破坏 sudo
  if ! visudo -c -f "$SUDOERS_FILE" >/dev/null 2>&1; then
    echo "Warning: sudoers syntax check failed, removing $SUDOERS_FILE"
    rm -f "$SUDOERS_FILE"
  else
    echo "Sudoers rule installed: $SUDOERS_FILE (user: ${run_user})"
  fi

  systemctl daemon-reload
  systemctl enable "$SERVICE_NAME"
  systemctl restart "$SERVICE_NAME"

  echo "Installed: $SERVICE_NAME"
  echo "Executable: $INSTALL_DIR/$BIN_NAME"
  echo "Config: $INSTALL_DIR/control_panel_config.json"
  echo "Control command: cpanelctl status | start | stop | open"
  port=$(python3 -c "
import json
try:
    with open('$INSTALL_DIR/control_panel_config.json','r',encoding='utf-8') as f:
        print(int(json.load(f).get('web_port',8888)))
except: print(8888)
" 2>/dev/null || echo 8888)
  echo "Web UI: http://$(hostname -I | awk '{print $1}'):${port}"
  echo ""
  systemctl --no-pager --full status "$SERVICE_NAME" || true
}

uninstall_service() {
  # 从 unit 文件读取实际安装目录
  local installed_dir="/opt/bevp-control-panel"
  if [[ -f "$UNIT_FILE" ]]; then
    local wd
    wd=$(grep -Po '(?<=WorkingDirectory=).*' "$UNIT_FILE" 2>/dev/null || true)
    [[ -n "$wd" ]] && installed_dir="$wd"
  fi
  systemctl disable --now "$SERVICE_NAME" || true
  rm -f "$UNIT_FILE"
  rm -f "$CTL_BIN"
  rm -f "${installed_dir}/${BIN_NAME}"
  rm -f "/etc/sudoers.d/bevp-control-panel"
  systemctl daemon-reload
  echo "Removed service: $SERVICE_NAME"
  echo "Executable removed: ${installed_dir}/${BIN_NAME}"
  echo "Config and data kept in: ${installed_dir}"
}

show_status() {
  systemctl --no-pager --full status "$SERVICE_NAME" || true
}

show_logs() {
  journalctl -u "$SERVICE_NAME" -n 100 --no-pager || true
}

action="${1:-}"
arg2="${2:-}"

if [[ -z "$action" ]]; then
  usage
  exit 1
fi

case "$action" in
  install|uninstall|status|logs)
    ;;
  *)
    usage
    exit 1
    ;;
esac

require_root_for_mutation "$action"

case "$action" in
  install)
    shift  # 移除 'install'
    install_service "$@"
    ;;
  uninstall)
    uninstall_service
    ;;
  status)
    show_status
    ;;
  logs)
    show_logs
    ;;
esac
