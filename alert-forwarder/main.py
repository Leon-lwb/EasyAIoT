#!/usr/bin/env python3
"""
明厨亮灶告警转发微服务
订阅 Kafka iot-alert-notification 主题，将告警消息转换为 TransparentKitchen 格式
并推送至目标平台。
"""

import json
import time
import hashlib
import logging
import os
import sys
from datetime import datetime
from typing import Optional, Dict, Any

import requests
from kafka import KafkaConsumer
from kafka.errors import KafkaError

# ── 日志配置 ──────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("alert-forwarder")

# ── 环境变量配置 ──────────────────────────────────────────
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka-server:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "iot-alert-notification")
KAFKA_GROUP_ID = os.getenv("KAFKA_GROUP_ID", "alert-forwarder-group")
KAFKA_CONSUMER_TIMEOUT_MS = int(os.getenv("KAFKA_CONSUMER_TIMEOUT_MS", "30000"))

# 目标平台配置
TARGET_URL = os.getenv(
    "TARGET_URL",
    "https://testcy.xqbg.net/testapi/api/TransparentKitchen/PutEventList",
)
API_ID = os.getenv("API_ID") or os.getenv("ALERT_FORWARDER_API_ID", "")
SECRET = os.getenv("SECRET") or os.getenv("ALERT_FORWARDER_SECRET", "")

# 启动时强制校验：生产环境必须显式设置凭证
if not API_ID or not SECRET:
    log.critical("缺少 API_ID 或 SECRET 环境变量，拒绝启动。请设置 ALERT_FORWARDER_API_ID 和 ALERT_FORWARDER_SECRET。")
    sys.exit(1)
REQUEST_TIMEOUT = int(os.getenv("REQUEST_TIMEOUT", "30"))
MAX_RETRIES = int(os.getenv("MAX_RETRIES", "3"))
RETRY_DELAY = int(os.getenv("RETRY_DELAY", "5"))

# MinIO 公网访问基础 URL（用于将容器内路径转为可访问 URL）
MINIO_PUBLIC_BASE = os.getenv("MINIO_PUBLIC_BASE", "")
MINIO_BUCKET = os.getenv("MINIO_BUCKET", "alert-images")

# 图片上传配置：是否将告警图片上传到 MinIO 后使用公网 URL
UPLOAD_IMAGE_TO_MINIO = os.getenv("UPLOAD_IMAGE_TO_MINIO", "false").lower() == "true"


def safe_json_loads(data: bytes) -> Optional[Dict[str, Any]]:
    """安全 JSON 反序列化，跳过损坏的消息"""
    try:
        return json.loads(data.decode("utf-8"))
    except (json.JSONDecodeError, UnicodeDecodeError, AttributeError) as e:
        log.warning("跳过损坏的消息: %s (data[:100]=%s)", e, data[:100])
        return None


def build_sign(api_id: str, secret: str, timestamp: str) -> str:
    """生成签名：MD5(ApiID + Secret + timestamp) 32位大写"""
    raw = f"{api_id}{secret}{timestamp}"
    return hashlib.md5(raw.encode("utf-8")).hexdigest().upper()


def build_headers() -> Dict[str, str]:
    """构建带认证的 HTTP 请求头"""
    ts = str(int(time.time()))
    sign = build_sign(API_ID, SECRET, ts)
    return {
        "Content-Type": "application/json",
        "ApiID": API_ID,
        "Timestamp": ts,
        "Sign": sign,
    }


def resolve_image_url(alert: Dict[str, Any]) -> Optional[str]:
    """
    将容器内的图片路径解析为可公网访问的 URL。
    如果配置了 MINIO_PUBLIC_BASE，尝试从路径中提取对象名并拼接 URL。
    """
    image_path = alert.get("imagePath") or ""
    if not image_path:
        return None

    # 如果已经是 URL，直接返回
    if image_path.startswith("http://") or image_path.startswith("https://"):
        return image_path

    # 从路径提取 task_id/device_id/filename
    # 格式: /app/alert_images/task_1/gb28181_.../20260722_024541_frame1_track1_person.jpg
    if MINIO_PUBLIC_BASE:
        parts = image_path.lstrip("/").split("/")
        # 跳过 /app/alert_images/ 前缀
        if "alert_images" in parts:
            idx = parts.index("alert_images")
            object_key = "/".join(parts[idx + 1:])
            return f"{MINIO_PUBLIC_BASE.rstrip('/')}/{MINIO_BUCKET}/{object_key}"

    return image_path


def transform_alert(kafka_msg: Dict[str, Any]) -> Dict[str, Any]:
    """
    将 Kafka 告警消息转换为 TransparentKitchen 事件格式。
    可根据实际对接平台要求修改此映射。
    """
    alert = kafka_msg.get("alert", {})
    info_str = alert.get("information", "{}")
    try:
        info = json.loads(info_str) if isinstance(info_str, str) else info_str
    except (json.JSONDecodeError, TypeError):
        info = {}

    # 提取检测到的对象列表
    detections = info.get("detections", [])
    objects_detected = list(set(d.get("class_name", "") for d in detections))
    object_str = ",".join(objects_detected) if objects_detected else alert.get("object", "")

    # 提取最高置信度
    max_confidence = max(
        (d.get("confidence", 0) for d in detections), default=0
    )

    image_url = resolve_image_url(alert) or ""

    return {
        "DeviceId": kafka_msg.get("deviceId", ""),
        "DeviceName": kafka_msg.get("deviceName", ""),
        "EventType": alert.get("object", "unknown"),
        "EventTypeName": object_str,
        "EventTime": alert.get("time", datetime.now().strftime("%Y-%m-%d %H:%M:%S")),
        "EventPicUrl": image_url,
        "EventVideoUrl": "",  # 如有录像路径可在此映射
        "EventDescription": alert.get("event", ""),
        "Confidence": round(max_confidence, 4),
        "TaskId": kafka_msg.get("taskId", ""),
        "TaskName": kafka_msg.get("taskName", ""),
        "CorrelationId": kafka_msg.get("correlationId", ""),
        "SourceTimestamp": kafka_msg.get("timestamp", ""),
        "TotalCount": info.get("total_count", 1),
        "Detections": detections,
    }


def send_to_target(events: list) -> bool:
    """发送事件列表到目标平台，返回是否成功"""
    payload = {"EventList": events}

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            headers = build_headers()
            log.info(
                "推送请求: attempt=%d/%d events=%d url=%s",
                attempt, MAX_RETRIES, len(events), TARGET_URL,
            )
            log.debug("请求体: %s", json.dumps(payload, ensure_ascii=False)[:500])
            log.debug("请求头: ApiID=%s Timestamp=%s Sign=%s",
                      headers.get("ApiID"), headers.get("Timestamp"), headers.get("Sign"))

            resp = requests.post(
                TARGET_URL,
                json=payload,
                headers=headers,
                timeout=REQUEST_TIMEOUT,
            )
            log.info(
                "HTTP %d | attempt=%d/%d | Events=%d | body=%s",
                resp.status_code,
                attempt,
                MAX_RETRIES,
                len(events),
                resp.text[:500],
            )

            if resp.status_code == 200:
                try:
                    body = resp.json()
                    code = body.get("code")
                    if code == 0 or code == 200:
                        log.info("推送成功: %d 条事件, Yddh=%s", len(events), body.get("Yddh"))
                        return True
                    elif code == 500:
                        # 服务端内部错误（如 NullReferenceException），记录详细日志
                        log.warning(
                            "目标平台服务端错误 (code=500): %s | 请求体=%s",
                            body.get("message", "unknown"),
                            json.dumps(payload, ensure_ascii=False)[:300],
                        )
                    else:
                        log.warning("目标平台返回错误 code=%s: %s", code, body.get("message", resp.text[:200]))
                except json.JSONDecodeError:
                    log.warning("响应非JSON: %s", resp.text[:200])
            elif resp.status_code == 401 or resp.status_code == 403:
                log.error("认证失败 HTTP %d: %s", resp.status_code, resp.text[:500])
            else:
                log.warning("HTTP %d: %s", resp.status_code, resp.text[:200])

        except requests.exceptions.Timeout:
            log.error("请求超时 (attempt %d/%d)", attempt, MAX_RETRIES)
        except requests.exceptions.ConnectionError as e:
            log.error("连接失败 (attempt %d/%d): %s", attempt, MAX_RETRIES, e)
        except Exception as e:
            log.error("请求异常 (attempt %d/%d): %s", attempt, MAX_RETRIES, e)

        if attempt < MAX_RETRIES:
            time.sleep(RETRY_DELAY * attempt)

    log.error("推送失败，已重试 %d 次", MAX_RETRIES)
    return False


def create_consumer() -> KafkaConsumer:
    """创建 Kafka 消费者"""
    while True:
        try:
            consumer = KafkaConsumer(
                KAFKA_TOPIC,
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS.split(","),
                group_id=KAFKA_GROUP_ID,
                auto_offset_reset="latest",
                enable_auto_commit=True,
                auto_commit_interval_ms=5000,
                value_deserializer=lambda m: safe_json_loads(m),
                consumer_timeout_ms=KAFKA_CONSUMER_TIMEOUT_MS,
                session_timeout_ms=30000,
                max_poll_interval_ms=300000,
            )
            log.info(
                "Kafka 消费者已就绪: topic=%s group=%s broker=%s",
                KAFKA_TOPIC,
                KAFKA_GROUP_ID,
                KAFKA_BOOTSTRAP_SERVERS,
            )
            return consumer
        except KafkaError as e:
            log.error("Kafka 连接失败: %s，10秒后重试...", e)
            time.sleep(10)


def main():
    log.info("=== 明厨亮灶告警转发服务启动 ===")
    log.info("Kafka: %s  Topic: %s  Group: %s", KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPIC, KAFKA_GROUP_ID)
    log.info("Target: %s  ApiID: %s", TARGET_URL, API_ID)
    log.info("MinIO Public Base: %s  Bucket: %s", MINIO_PUBLIC_BASE, MINIO_BUCKET)

    consumer = create_consumer()

    try:
        for message in consumer:
            try:
                kafka_msg = message.value
                if kafka_msg is None:
                    # 损坏的消息，已由 safe_json_loads 跳过
                    continue

                log.info(
                    "收到告警: device=%s object=%s task=%s correlationId=%s",
                    kafka_msg.get("deviceId", ""),
                    kafka_msg.get("alert", {}).get("object", ""),
                    kafka_msg.get("taskName", ""),
                    kafka_msg.get("correlationId", ""),
                )

                # 转换并发送
                event = transform_alert(kafka_msg)
                success = send_to_target([event])

                if not success:
                    log.warning(
                        "告警转发失败，offset=%d partition=%d",
                        message.offset,
                        message.partition,
                    )

            except json.JSONDecodeError as e:
                log.error("JSON 解析失败: %s", e)
            except Exception as e:
                log.error("处理消息异常: %s", e, exc_info=True)

    except KeyboardInterrupt:
        log.info("收到终止信号，退出中...")
    finally:
        consumer.close()
        log.info("服务已停止")


if __name__ == "__main__":
    main()
