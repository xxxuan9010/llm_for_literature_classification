# -*- coding: utf-8 -*-
# @Author  : hanyx9010@163.com
# @Time    : 2026/3/18 16:12
# @File    : build_screening_tasks.py
# @Description    :
import json
from pathlib import Path
from typing import Any, Dict, List
import argparse

# =========================
# 路径配置
# =========================
# BASE_DIR = Path(__file__).resolve().parent.parent
# INPUT_FILE = BASE_DIR / "data" / "screening_records.json"
# OUTPUT_FILE = BASE_DIR / "data" / "screening_tasks.jsonl"
def parse_args():
    parser = argparse.ArgumentParser(description="Build screening tasks jsonl from screening records json.")
    parser.add_argument("--input", required=True, help="输入的screening_records.json路径")
    parser.add_argument("--output", required=True, help="输出的screening_tasks.jsonl路径")
    parser.add_argument("--max-abstract-chars", type=int, default=DEFAULT_MAX_ABSTRACT_CHARS,
                        help="摘要最大截断字符数，默认2000")
    return parser.parse_args()

# =========================
# 参数配置
# =========================
DEFAULT_MAX_ABSTRACT_CHARS = 2000


# =========================
# 工具函数
# =========================
def normalize_text(text: str) -> str:
    """压缩多余空白，避免换行和连续空格影响输入稳定性。"""
    if not text:
        return ""
    return " ".join(text.split()).strip()


def truncate_text(text: str, max_chars: int) -> str:
    """截断过长文本，控制token成本。"""
    text = normalize_text(text)
    if len(text) <= max_chars:
        return text
    return text[:max_chars].rstrip() + "..."


def list_to_semicolon_text(value: Any) -> str:
    """
    把关键词列表转成分号分隔字符串。
    若为空，则返回空字符串。
    """
    if value is None:
        return ""

    if isinstance(value, list):
        cleaned = [normalize_text(str(x)) for x in value if normalize_text(str(x))]
        return "; ".join(cleaned)

    return normalize_text(str(value))


def build_input_text(record: Dict[str, Any], max_abstract_chars: int) -> str:
    """
    把一条screening record转成适合LLM处理的统一文本块。
    """
    title = normalize_text(record.get("title", ""))
    abstract = truncate_text(record.get("abstract", ""), max_abstract_chars)
    author_keywords = list_to_semicolon_text(record.get("author_keywords", []))
    keywords_plus = list_to_semicolon_text(record.get("keywords_plus", []))

    input_text = (
        f"Title: {title}\n"
        f"Abstract: {abstract}\n"
        f"Author keywords: {author_keywords}\n"
        f"Keywords plus: {keywords_plus}"
    )

    return input_text


def load_json(file_path: Path) -> List[Dict[str, Any]]:
    with file_path.open("r", encoding="utf-8") as f:
        return json.load(f)


def export_jsonl(records: List[Dict[str, Any]], file_path: Path) -> None:
    file_path.parent.mkdir(parents=True, exist_ok=True)
    with file_path.open("w", encoding="utf-8") as f:
        for record in records:
            f.write(json.dumps(record, ensure_ascii=False) + "\n")


# =========================
# 主逻辑
# =========================
def main() -> None:
    args = parse_args()

    input_file = Path(args.input)
    output_file = Path(args.output)
    max_abstract_chars = args.max_abstract_chars

    if not input_file.exists():
        raise FileNotFoundError(f"未找到输入文件：{input_file}")

    screening_records = load_json(input_file)

    tasks = []
    for record in screening_records:
        task = {
            "id": record.get("id", ""),
            "input_text": build_input_text(record, max_abstract_chars)
        }
        tasks.append(task)

    export_jsonl(tasks, output_file)

    print(f"任务构建完成，共生成{len(tasks)}条LLM任务。")
    print(f"输出文件：{output_file}")

    if tasks:
        print("\n第一条任务示例：")
        print(json.dumps(tasks[0], ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()