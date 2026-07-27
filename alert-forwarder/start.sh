#!/bin/bash
# 启动告警转发服务的脚本
# 使用: ./start.sh [start|stop|status|logs]

SERVICE_DIR="/root/easyaiot/alert-forwarder"
VENV="$SERVICE_DIR/venv"
PID_FILE="$SERVICE_DIR/service.pid"
LOG_FILE="$SERVICE_DIR/service.log"

start() {
    if [ -f "$PID_FILE" ] && kill -0 $(cat "$PID_FILE") 2>/dev/null; then
        echo "服务已在运行 (PID: $(cat $PID_FILE))"
        return 1
    fi

    echo "启动告警转发服务..."
    source "$VENV/bin/activate"
    cd "$SERVICE_DIR"

    export KAFKA_BOOTSTRAP_SERVERS="${KAFKA_BOOTSTRAP_SERVERS:-localhost:9092}"
    export KAFKA_TOPIC="${KAFKA_TOPIC:-iot-alert-notification}"
    export KAFKA_GROUP_ID="${KAFKA_GROUP_ID:-alert-forwarder-group}"
    export TARGET_URL="${TARGET_URL:-https://testcy.xqbg.net/testapi/api/TransparentKitchen/PutEventList}"
    export ALERT_FORWARDER_API_ID="${ALERT_FORWARDER_API_ID:-}"
    export ALERT_FORWARDER_SECRET="${ALERT_FORWARDER_SECRET:?请先设置ALERT_FORWARDER_SECRET}"
    export MINIO_PUBLIC_BASE="${MINIO_PUBLIC_BASE:-}"
    export MINIO_BUCKET="${MINIO_BUCKET:-alert-images}"

    nohup python3 -u main.py >> "$LOG_FILE" 2>&1 &
    echo $! > "$PID_FILE"
    sleep 2
    if kill -0 $(cat "$PID_FILE") 2>/dev/null; then
        echo "服务已启动 (PID: $(cat $PID_FILE))"
    else
        echo "启动失败，请查看日志: $LOG_FILE"
    fi
}

stop() {
    if [ ! -f "$PID_FILE" ]; then
        echo "PID 文件不存在"
        return 1
    fi
    PID=$(cat "$PID_FILE")
    if kill -0 "$PID" 2>/dev/null; then
        kill "$PID"
        echo "服务已停止 (PID: $PID)"
        rm -f "$PID_FILE"
    else
        echo "进程不存在"
        rm -f "$PID_FILE"
    fi
}

status() {
    if [ -f "$PID_FILE" ] && kill -0 $(cat "$PID_FILE") 2>/dev/null; then
        echo "运行中 (PID: $(cat $PID_FILE))"
    else
        echo "未运行"
    fi
}

logs() {
    tail -f "$LOG_FILE"
}

case "${1:-start}" in
    start)   start ;;
    stop)    stop ;;
    restart) stop; sleep 1; start ;;
    status)  status ;;
    logs)    logs ;;
    *)       echo "用法: $0 {start|stop|restart|status|logs}" ;;
esac
