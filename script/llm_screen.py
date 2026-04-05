# -*- coding: utf-8 -*-
# @Author  : hanyx9010@163.com
# @Time    : 2026/3/18 16:18
# @File    : llm_screen.py
# @Description    :
import json
import os
import time
from pathlib import Path
from typing import Dict, Any
from openai import OpenAI
import argparse

# =========================
# 路径
# =========================
# BASE_DIR = Path(__file__).resolve().parent.parent
# INPUT_FILE = BASE_DIR / "data" / "screening_tasks.jsonl"
# OUTPUT_FILE = BASE_DIR / "data" / "screening_results.jsonl"


def parse_args():
    parser = argparse.ArgumentParser(description="Run LLM screening for screening tasks jsonl.")
    parser.add_argument("--input", required=True, help="输入的screening_tasks.jsonl路径")
    parser.add_argument("--output", required=True, help="输出的screening_results.jsonl路径")
    parser.add_argument("--sleep", type=float, default=1.0, help="每次调用后的休眠秒数，默认1秒")
    parser.add_argument("--temperature", type=float, default=0.0, help="模型温度，默认0")
    parser.add_argument("--max-retries", type=int, default=3, help="单条任务最大重试次数，默认3")
    return parser.parse_args()


# =========================
# API
# =========================
MY_API_KEY = os.getenv("MY_API_KEY", "这里修改为API-KEY")  
API_BASE_URL = os.getenv("MY_API_BASE", https://api.deepseek.com")
# client = OpenAI(api_key=MY_API_KEY, base_url=API_BASE_URL)

MODEL_NAME = os.getenv("MY_MODEL", "deepseek-v3")


def get_client() -> OpenAI:
    if not MY_API_KEY:
        raise ValueError("环境变量 MY_API_KEY 未设置。")
    return OpenAI(api_key=MY_API_KEY, base_url=API_BASE_URL)


# =========================
# Prompt模板
# =========================
def build_prompt(input_text: str) -> str:
    return f"""
You are an expert in digital economy and data factor research.

Your task is to classify the following academic paper into one of three categories:

A = Highly relevant to "data factor institutions"
B = Possibly relevant
C = Not relevant

========================
Definition:

"Data factor institutions" refer to institutional arrangements, governance mechanisms, and policy frameworks related to data as a production factor, including but not limited to:

- Data ownership and property rights
- Data sharing and data openness
- Data trading and data markets
- Data circulation and allocation
- Data governance and regulation
- Data security and privacy (as institutional issues)
- Data assetization and value realization

A paper is considered highly relevant (A) ONLY IF:
- It explicitly focuses on data as an economic resource or production factor
AND
- It discusses institutional, governance, regulatory, or mechanism design issues

========================
Classification rules:

A:
- Focus on data governance, data markets, data property rights, or institutional design

B:
- Mentions data-related topics (e.g., data sharing, privacy, AI data use)
- But lacks a clear institutional or mechanism perspective

C:
- Focuses on applications (AI, healthcare, engineering, etc.)
- Data is only used as input or background
- No discussion of governance, institutions, or policy

========================
Paper:
{input_text}

========================
Output format (STRICT JSON):

{{
  "label": "A or B or C",
  "reason": "Brief explanation in one sentence"
}}
"""


# =========================
# 调用LLM
# =========================
def call_llm(client: OpenAI, prompt: str, temperature: float) -> Dict[str, Any]:
    response = client.chat.completions.create(
        model=MODEL_NAME,
        messages=[{"role": "user", "content": prompt}],
        temperature=temperature
    )

    content = response.choices[0].message.content

    try:
        return json.loads(content)
    except:
        return {"label": "ERROR", "reason": content}


def call_llm_with_retry(client: OpenAI, prompt: str, temperature: float, max_retries: int) -> Dict[str, Any]:
    last_error = None

    for attempt in range(1, max_retries + 1):
        try:
            return call_llm(client, prompt, temperature)
        except Exception as e:
            last_error = e
            print(f"调用失败，第{attempt}/{max_retries}次重试：{e}")
            time.sleep(min(2 * attempt, 10))

    return {"label": "ERROR", "reason": f"LLM调用失败：{last_error}"}


def load_done_ids(file_path: Path) -> set:
    done_ids = set()
    if not file_path.exists():
        return done_ids

    with file_path.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                record = json.loads(line)
                if "id" in record:
                    done_ids.add(record["id"])
            except Exception:
                continue
    return done_ids

# =========================
# 主流程
# =========================
# def main():
#     OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)
#
#     with INPUT_FILE.open("r", encoding="utf-8") as f, \
#             OUTPUT_FILE.open("w", encoding="utf-8") as out:
#         for i, line in enumerate(f):
#             task = json.loads(line)
#
#             prompt = build_prompt(task["input_text"])
#             result = call_llm(prompt)
#
#             output = {
#                 "id": task["id"],
#                 "label": result.get("label", ""),
#                 "reason": result.get("reason", "")
#             }
#
#             out.write(json.dumps(output, ensure_ascii=False) + "\n")
#             out.flush()
#
#             print(f"已处理第{i + 1}条：{task['id']}")
#             time.sleep(1)
#
#     print("筛选完成")


def main():
    args = parse_args()

    input_file = Path(args.input)
    output_file = Path(args.output)
    sleep_seconds = args.sleep
    temperature = args.temperature
    max_retries = args.max_retries

    if not input_file.exists():
        raise FileNotFoundError(f"未找到输入文件：{input_file}")

    output_file.parent.mkdir(parents=True, exist_ok=True)

    client = get_client()
    done_ids = load_done_ids(output_file)

    with input_file.open("r", encoding="utf-8") as f, \
         output_file.open("a", encoding="utf-8") as out:

        for i, line in enumerate(f, start=1):
            line = line.strip()
            if not line:
                continue

            task = json.loads(line)
            task_id = task.get("id", "")

            if task_id in done_ids:
                print(f"跳过第{i}条，已存在结果：{task_id}")
                continue

            prompt = build_prompt(task["input_text"])
            result = call_llm_with_retry(client, prompt, temperature, max_retries)

            output = {
                "id": task_id,
                "label": result.get("label", ""),
                "reason": result.get("reason", "")
            }

            out.write(json.dumps(output, ensure_ascii=False) + "\n")
            out.flush()

            print(f"已处理第{i}条：{task_id}")
            time.sleep(sleep_seconds)

    print("筛选完成")

if __name__ == "__main__":
    main()
