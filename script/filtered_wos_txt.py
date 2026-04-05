# -*- coding: utf-8 -*-
# @Author  : hanyx9010@163.com
# @Time    : 2026/3/18 17:00
# @File    : filtered_wos_txt.py
# @Description    :
import json
from pathlib import Path
import argparse

# =========================
# 路径配置
# =========================
# BASE_DIR = Path(__file__).resolve().parent.parent
#
# RAW_WOS_FILE = BASE_DIR / "data" / "batches_raw.txt"
# RESULT_FILE = BASE_DIR / "data" / "screening_results.jsonl"
#
# OUTPUT_A = BASE_DIR / "data" / "filtered_wos_A.txt"
# OUTPUT_B = BASE_DIR / "data" / "filtered_wos_B.txt"
# OUTPUT_C = BASE_DIR / "data" / "filtered_wos_C.txt"


def parse_args():
    parser = argparse.ArgumentParser(
        description="Export original WOS txt records into A/B/C files based on screening results."
    )
    parser.add_argument("--batches_raw", required=True, help="原始WOS txt文件路径")
    parser.add_argument("--result", required=True, help="LLM筛选结果jsonl路径")
    parser.add_argument("--output-a", required=True, help="A类输出txt路径")
    parser.add_argument("--output-b", required=True, help="B类输出txt路径")
    parser.add_argument("--output-c", required=True, help="C类输出txt路径")
    parser.add_argument("--output-error", required=True, help="ERROR类输出txt路径")
    return parser.parse_args()


def load_id_to_label(result_file: Path) -> dict[str, str]:
    id_to_label = {}
    bad_lines = 0

    with result_file.open("r", encoding="utf-8") as f:
        for lineno, line in enumerate(f, start=1):
            line = line.strip()
            if not line:
                continue

            try:
                record = json.loads(line)
            except json.JSONDecodeError:
                bad_lines += 1
                print(f"警告：第{lineno}行不是合法JSON，已跳过")
                continue

            rec_id = str(record.get("id", "")).strip()
            label = str(record.get("label", "")).strip().upper()

            if rec_id and label in {"A", "B", "C", "ERROR"}:
                id_to_label[rec_id] = label

    print(f"读取完成：有效记录={len(id_to_label)}，损坏行={bad_lines}")
    return id_to_label


def export_wos_by_label(
    raw_file: Path,
    id_to_label: dict[str, str],
    output_a: Path,
    output_b: Path,
    output_c: Path,
    output_error: Path
) -> dict[str, int]:
    """
    按 label 将原始 WOS txt 记录分别导出到 A/B/C/ERROR 四个文件。
    依赖每条记录中的 UT 与 result.jsonl 中的 id 对应。
    仅导出 id_to_label 中存在且标签属于 A/B/C/ERROR 的记录。
    """
    counts = {"A": 0, "B": 0, "C": 0, "ERROR": 0}

    output_a.parent.mkdir(parents=True, exist_ok=True)
    output_b.parent.mkdir(parents=True, exist_ok=True)
    output_c.parent.mkdir(parents=True, exist_ok=True)
    output_error.parent.mkdir(parents=True, exist_ok=True)

    current_record_lines = []
    current_ut = None
    in_record = False

    with raw_file.open("r", encoding="utf-8", errors="ignore") as fin, \
         output_a.open("w", encoding="utf-8") as fout_a, \
         output_b.open("w", encoding="utf-8") as fout_b, \
         output_c.open("w", encoding="utf-8") as fout_c, \
         output_error.open("w", encoding="utf-8") as fout_error:

        for raw_line in fin:
            line = raw_line.rstrip("\n")

            # 新记录开始
            if line.startswith("PT "):
                current_record_lines = [line]
                current_ut = None
                in_record = True
                continue

            # 还没进入正式文献记录时，跳过文件头
            if not in_record:
                continue

            current_record_lines.append(line)

            # 提取 UT
            if line.startswith("UT "):
                current_ut = line[3:].strip()

            # 一条记录结束
            if line == "ER":
                if current_ut and current_ut in id_to_label:
                    label = str(id_to_label[current_ut]).strip().upper()
                    record_text = "\n".join(current_record_lines) + "\n\n"

                    if label == "A":
                        fout_a.write(record_text)
                        counts["A"] += 1
                    elif label == "B":
                        fout_b.write(record_text)
                        counts["B"] += 1
                    elif label == "C":
                        fout_c.write(record_text)
                        counts["C"] += 1
                    elif label == "ERROR":
                        fout_error.write(record_text)
                        counts["ERROR"] += 1

                current_record_lines = []
                current_ut = None
                in_record = False

    return counts


def main():
    args = parse_args()

    raw_wos_file = Path(args.batches_raw)
    result_file = Path(args.result)

    output_a = Path(args.output_a)
    output_b = Path(args.output_b)
    output_c = Path(args.output_c)
    output_error = Path(args.output_error)

    # 基本检查
    if not raw_wos_file.exists():
        raise FileNotFoundError(f"未找到原始WOS文件：{raw_wos_file}")
    if not result_file.exists():
        raise FileNotFoundError(f"未找到筛选结果文件：{result_file}")

    print("========== 开始处理 ==========")
    print(f"原始文件：{raw_wos_file}")
    print(f"筛选结果：{result_file}")

    # 读取 id -> label
    id_to_label = load_id_to_label(result_file)
    print(f"加载标签完成，共 {len(id_to_label)} 条记录")

    # 导出
    counts = export_wos_by_label(
        raw_wos_file,
        id_to_label,
        output_a,
        output_b,
        output_c,
        output_error
    )

    print("========== 导出完成 ==========")
    print(f"A类：{counts['A']} 条 -> {output_a}")
    print(f"B类：{counts['B']} 条 -> {output_b}")
    print(f"C类：{counts['C']} 条 -> {output_c}")
    print(f"ERROR类：{counts['ERROR']} 条 -> {output_error}")


if __name__ == "__main__":
    main()