import os
import re
import argparse
import logging
from datetime import datetime
from collections import Counter

import requests

# -------- 配置区 --------
APP_ID = os.getenv("FEISHU_APP_ID", "")
APP_SECRET = os.getenv("FEISHU_APP_SECRET", "")
READ_APP_TOKEN = os.getenv("FEISHU_READ_APP_TOKEN", "")
READ_TABLE_ID = os.getenv("FEISHU_READ_TABLE_ID", "")
WRITE_APP_TOKEN = os.getenv("FEISHU_WRITE_APP_TOKEN", "")
WRITE_TABLE_ID = os.getenv("FEISHU_WRITE_TABLE_ID", "")
STUDENT_NAME = os.getenv("STUDENT_NAME", "")

LOG_FILE = os.getenv("LOG_FILE", os.path.join(os.path.dirname(__file__), "task.log"))
HTTP_TIMEOUT_SECONDS = float(os.getenv("HTTP_TIMEOUT_SECONDS", "30"))
PAGE_SIZE = int(os.getenv("BITABLE_PAGE_SIZE", "50"))

RUN_MODE = os.getenv("RUN_MODE", "once")
SCHEDULE_INTERVAL_MINUTES = int(os.getenv("SCHEDULE_INTERVAL_MINUTES", "2"))

ENABLE_WEBHOOK = os.getenv("ENABLE_WEBHOOK", "0") in {"1", "true", "True", "YES", "yes"}
WEBHOOK_URL = os.getenv("WEBHOOK_URL", "")

FILTER_PROFESSION = os.getenv("FILTER_PROFESSION", "")


def setup_logging(log_file: str) -> None:
    """初始化日志：同时输出到控制台与文件。"""
    root_logger = logging.getLogger()
    root_logger.setLevel(logging.INFO)

    formatter = logging.Formatter(
        fmt="%(asctime)s %(levelname)s %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    file_handler = logging.FileHandler(log_file, encoding="utf-8")
    file_handler.setLevel(logging.INFO)
    file_handler.setFormatter(formatter)

    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(formatter)

    if not root_logger.handlers:
        root_logger.addHandler(file_handler)
        root_logger.addHandler(console_handler)


def validate_config() -> None:
    """校验配置项：缺少关键配置时直接报错，避免带空参数请求。"""
    required = {
        "FEISHU_APP_ID": APP_ID,
        "FEISHU_APP_SECRET": APP_SECRET,
        "FEISHU_READ_APP_TOKEN": READ_APP_TOKEN,
        "FEISHU_READ_TABLE_ID": READ_TABLE_ID,
        "FEISHU_WRITE_APP_TOKEN": WRITE_APP_TOKEN,
        "FEISHU_WRITE_TABLE_ID": WRITE_TABLE_ID,
    }
    missing = [k for k, v in required.items() if not v]
    if missing:
        raise ValueError(f"缺少必要配置：{', '.join(missing)}（建议用环境变量或 .env 提供）")

    if ENABLE_WEBHOOK and not WEBHOOK_URL:
        raise ValueError("已开启 ENABLE_WEBHOOK，但 WEBHOOK_URL 为空")


def send_feishu_webhook(webhook_url: str, text: str) -> None:
    """向飞书群机器人 Webhook 推送文本消息。"""
    payload = {"msg_type": "text", "content": {"text": text}}
    resp = requests.post(webhook_url, json=payload, timeout=HTTP_TIMEOUT_SECONDS)
    resp.raise_for_status()


def get_access_token(app_id: str, app_secret: str) -> str:
    """获取 tenant_access_token。"""
    url = "https://open.feishu.cn/open-apis/auth/v3/tenant_access_token/internal/"
    data = {"app_id": app_id, "app_secret": app_secret}
    resp = requests.post(url, json=data, timeout=HTTP_TIMEOUT_SECONDS)
    resp.raise_for_status()
    result = resp.json()
    if result.get("code") != 0:
        raise RuntimeError(f"获取 access_token 失败: {result.get('msg')}")
    return result["tenant_access_token"]


def get_all_records(app_token: str, table_id: str, access_token: str) -> list[dict]:
    """分页获取 Bitable 表中的所有记录。"""
    headers = {"Authorization": f"Bearer {access_token}"}
    url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{app_token}/tables/{table_id}/records"
    records: list[dict] = []
    page_token = None

    while True:
        params = {"page_size": PAGE_SIZE}
        if page_token:
            params["page_token"] = page_token

        resp = requests.get(url, headers=headers, params=params, timeout=HTTP_TIMEOUT_SECONDS)
        resp.raise_for_status()
        data = resp.json()
        if data.get("code") != 0:
            raise RuntimeError(f"获取记录失败: {data.get('msg')}")
        records.extend(data.get("data", {}).get("items", []))
        page_token = data.get("data", {}).get("page_token")
        if not page_token:
            break

    return records


def parse_skill_types(skill_field) -> set[str]:
    """解析“技能类型”字段，返回该条记录里包含的技能类型集合。"""
    if skill_field is None:
        return set()
    if isinstance(skill_field, str):
        normalized = re.sub(r"[，、；/]+", ",", skill_field)
        return {s.strip() for s in normalized.split(",") if s.strip()}
    if isinstance(skill_field, list):
        return {str(s).strip() for s in skill_field if str(s).strip()}
    return set()


def analyze_heroes(records: list[dict]) -> dict:
    """分析英雄数据：按英雄聚合技能类型，并统计职业分布与技能分布。"""
    if FILTER_PROFESSION:
        records = [r for r in records if r.get("fields", {}).get("英雄职业") == FILTER_PROFESSION]

    unique_heroes: dict = {}
    for record in records:
        hero_id = record.get("fields", {}).get("英雄ID")
        if hero_id is None:
            continue
        entry = unique_heroes.setdefault(hero_id, {"fields": record["fields"], "skills": set()})
        entry["skills"].update(parse_skill_types(record["fields"].get("技能类型")))

    total = len(unique_heroes)
    professions = [
        entry["fields"].get("英雄职业")
        for entry in unique_heroes.values()
        if entry["fields"].get("英雄职业")
    ]

    prof_counts = Counter(professions)
    skill_counts = Counter()
    for entry in unique_heroes.values():
        for skill_type in entry["skills"]:
            skill_counts[skill_type] += 1

    prof_text = "|".join([f"{k}:{v}" for k, v in prof_counts.items()])
    skill_text = "|".join([f"{k}:{v}" for k, v in skill_counts.items()])
    max_profession = max(prof_counts, key=prof_counts.get) if prof_counts else "无"

    return {
        "总英雄数": total,
        "职业分布": prof_text,
        "技能分布": skill_text,
        "职业占比最高": max_profession,
    }


def upsert_summary_record(
    summary_records: list[dict],
    app_token: str,
    table_id: str,
    access_token: str,
    student_name: str,
    analysis: dict,
) -> dict:
    """更新或新增统计记录到汇总表。"""
    existing_record = next(
        (rec for rec in summary_records if rec.get("fields", {}).get("学生姓名") == student_name), None
    )

    fields = {
        "学生姓名": student_name,
        "总英雄数": analysis["总英雄数"],
        "职业分布": analysis["职业分布"],
        "技能分布": analysis["技能分布"],
        "职业占比最高": analysis["职业占比最高"],
    }

    headers = {"Authorization": f"Bearer {access_token}"}
    url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{app_token}/tables/{table_id}/records"

    if existing_record:
        record_id = existing_record["record_id"]
        resp = requests.put(
            url + f"/{record_id}",
            headers=headers,
            json={"fields": fields},
            timeout=HTTP_TIMEOUT_SECONDS,
        )
    else:
        resp = requests.post(url, headers=headers, json={"fields": fields}, timeout=HTTP_TIMEOUT_SECONDS)

    resp.raise_for_status()
    result = resp.json()
    if result.get("code") != 0:
        raise RuntimeError(f"写入汇总表失败: {result.get('msg')}")
    return result


def run_task() -> None:
    """执行一次完整任务：拉取数据、分析、写回，并按配置可选推送 Webhook。"""
    logger = logging.getLogger(__name__)
    logger.info("任务开始：%s", datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    validate_config()

    logger.info("获取访问令牌...")
    access_token = get_access_token(APP_ID, APP_SECRET)
    logger.info("成功获取访问令牌")

    logger.info("正在读取英雄数据...")
    hero_records = get_all_records(READ_APP_TOKEN, READ_TABLE_ID, access_token)
    logger.info("英雄记录已读取，共 %s 条", len(hero_records))

    logger.info("正在分析数据...")
    analysis_result = analyze_heroes(hero_records)
    logger.info("数据分析完成：%s", analysis_result)

    logger.info("正在查询汇总表记录...")
    summary_records = get_all_records(WRITE_APP_TOKEN, WRITE_TABLE_ID, access_token)
    logger.info("查询完成")

    logger.info("正在更新或新增记录到汇总表...")
    upsert_summary_record(summary_records, WRITE_APP_TOKEN, WRITE_TABLE_ID, access_token, STUDENT_NAME, analysis_result)
    logger.info("汇总表更新完成，更新人：%s", STUDENT_NAME)

    if ENABLE_WEBHOOK:
        message = f"学生：{STUDENT_NAME}\n分析结果：{analysis_result}"
        logger.info("准备推送 Webhook...")
        send_feishu_webhook(WEBHOOK_URL, message)
        logger.info("Webhook 推送完成")

    logger.info("任务结束：%s", datetime.now().strftime("%Y-%m-%d %H:%M:%S"))


def parse_args() -> argparse.Namespace:
    """解析命令行参数，用于演示/验证。"""
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--mode",
        choices=["once", "schedule"],
        default=RUN_MODE,
        help="执行模式：once 单次执行；schedule 定时执行",
    )
    parser.add_argument(
        "--interval-minutes",
        type=int,
        default=SCHEDULE_INTERVAL_MINUTES,
        help="定时任务执行间隔（分钟）",
    )
    return parser.parse_args()


def main():
    """主流程：单次执行或按 APScheduler 定时执行。"""
    setup_logging(LOG_FILE)
    logger = logging.getLogger(__name__)

    args = parse_args()
    logger.info("启动模式：%s", args.mode)

    if args.mode == "schedule":
        from apscheduler.schedulers.blocking import BlockingScheduler

        scheduler = BlockingScheduler(timezone="Asia/Shanghai")
        scheduler.add_job(
            run_task,
            trigger="interval",
            minutes=args.interval_minutes,
            id="lesson6_job",
            max_instances=1,
            coalesce=True,
            replace_existing=True,
        )
        logger.info("已启动定时任务：每 %s 分钟执行一次", args.interval_minutes)
        logger.info("等待触发中...（可查看 %s）", LOG_FILE)
        scheduler.start()
        return

    run_task()


if __name__ == "__main__":
    main()
