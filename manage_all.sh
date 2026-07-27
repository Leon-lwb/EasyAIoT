#!/bin/bash
# EasyAIoT 全部服务管理脚本
# 管理所有 Docker Compose 项目 + 告警转发服务
# 用法: ./manage_all.sh {up|down|restart|status}

set -e

RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'; BLUE='\033[0;34m'; CYAN='\033[0;36m'; NC='\033[0m'

PROJECT_ROOT="/root/easyaiot"
ALERT_FORWARDER="$PROJECT_ROOT/alert-forwarder"

# Compose 项目（按依赖顺序）
COMPOSE_PROJECTS=(
    "docker|$PROJECT_ROOT/.scripts/docker"
    "web|$PROJECT_ROOT/WEB"
    "app|$PROJECT_ROOT/APP"
    "device|$PROJECT_ROOT/DEVICE"
    "ai|$PROJECT_ROOT/AI"
    "video|$PROJECT_ROOT/VIDEO"
)

compose_up() {
    local name="$1" dir="$2"
    echo -e "${BLUE}[${name}]${NC} 启动..."
    cd "$dir"
    docker compose --env-file "$PROJECT_ROOT/.env" up -d 2>&1 | while IFS= read -r line; do
        echo -e "  ${CYAN}[${name}]${NC} $line"
    done
    echo -e "${GREEN}[${name}]${NC} 已启动"
}

compose_down() {
    local name="$1" dir="$2"
    echo -e "${YELLOW}[${name}]${NC} 停止..."
    cd "$dir"
    docker compose --env-file "$PROJECT_ROOT/.env" down 2>&1 | while IFS= read -r line; do
        echo -e "  ${CYAN}[${name}]${NC} $line"
    done
    echo -e "${GREEN}[${name}]${NC} 已停止"
}

start_alert_forwarder() {
    echo -e "${BLUE}[alert-forwarder]${NC} 启动..."
    if [ -f "$ALERT_FORWARDER/start.sh" ]; then
        cd "$ALERT_FORWARDER"
        bash start.sh start
        echo -e "${GREEN}[alert-forwarder]${NC} 已启动"
    else
        echo -e "${YELLOW}[alert-forwarder]${NC} 脚本不存在，跳过"
    fi
}

stop_alert_forwarder() {
    echo -e "${YELLOW}[alert-forwarder]${NC} 停止..."
    if [ -f "$ALERT_FORWARDER/start.sh" ]; then
        cd "$ALERT_FORWARDER"
        bash start.sh stop 2>/dev/null || true
        echo -e "${GREEN}[alert-forwarder]${NC} 已停止"
    fi
}

show_status() {
    echo ""
    echo -e "${CYAN}=== Docker 容器状态 ===${NC}"
    docker ps --format "table {{.Names}}\t{{.Status}}" | sort | head -30
    echo ""
    echo -e "${CYAN}=== 告警转发服务 ===${NC}"
    if [ -f "$ALERT_FORWARDER/start.sh" ]; then
        cd "$ALERT_FORWARDER"
        bash start.sh status
    fi
    echo ""
}

cmd_up() {
    echo -e "${CYAN}=== 启动所有服务 ===${NC}"
    for entry in "${COMPOSE_PROJECTS[@]}"; do
        IFS='|' read -r name dir <<< "$entry"
        compose_up "$name" "$dir"
    done
    start_alert_forwarder
    echo ""
    echo -e "${GREEN}所有服务启动完成${NC}"
    show_status
}

cmd_down() {
    echo -e "${CYAN}=== 停止所有服务 ===${NC}"
    stop_alert_forwarder
    for ((i=${#COMPOSE_PROJECTS[@]}-1; i>=0; i--)); do
        IFS='|' read -r name dir <<< "${COMPOSE_PROJECTS[i]}"
        compose_down "$name" "$dir"
    done
    echo -e "${GREEN}所有服务已停止${NC}"
}

cmd_restart() {
    cmd_down
    sleep 3
    cmd_up
}

case "${1:-status}" in
    up)       cmd_up ;;
    down)     cmd_down ;;
    restart)  cmd_restart ;;
    status)   show_status ;;
    *)
        echo "用法: $0 {up|down|restart|status}"
        echo ""
        echo "  up       启动所有 Docker Compose 项目 + 告警转发服务"
        echo "  down     停止所有服务"
        echo "  restart  重启所有服务"
        echo "  status   查看状态"
        ;;
esac
