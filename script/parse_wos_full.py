# -*- coding: utf-8 -*-
# @Author  : hanyx9010@163.com
# @Time    : 2026/3/18 15:40
# @File    : parse_wos_full.py
# @Description    :
import json
import re
from pathlib import Path
from typing import Any, Dict, List, Optional
import argparse

# 获取项目根目录（scripts的上一级）
BASE_DIR = Path(__file__).resolve().parent.parent

# =========================
# 路径配置
# =========================
# INPUT_FILE = BASE_DIR / "data" / "batches_raw.txt"
# OUTPUT_RAW_JSON = BASE_DIR / "data" / "raw_records.json"
# OUTPUT_SCREENING_JSON = BASE_DIR / "data" / "screening_records.json"

def parse_args():
    parser = argparse.ArgumentParser(description="Parse WOS full-text export into batches_raw and screening JSON files.")
    parser.add_argument("--input", required=True, help="输入的WOS txt文件路径")
    parser.add_argument("--output-raw", required=True, help="输出的完整记录JSON路径")
    parser.add_argument("--output-screening", required=True, help="输出的筛选记录JSON路径")
    return parser.parse_args()

# =========================
# 字段配置
# =========================

# 这些字段通常会重复出现，或者本身就是多值字段，统一保存为list
REPEATABLE_FIELDS = {
    "AU", "AF", "C1", "C3", "CR", "EM", "OI", "FU", "FX",
    "WC", "SC"
}

# 这些字段虽然在原文中可能只出现一次，但内部往往以分号分隔多个值
SEMICOLON_SPLIT_FIELDS = {
    "DE", "ID", "WC", "SC", "EM", "OI", "FU"
}

# 想在完整版中保留的字段。若为None，则保留全部解析到的字段
KEEP_ALL_FIELDS = True
FIELDS_TO_KEEP = {
    "PT", "AU", "AF", "TI", "SO", "LA", "DT", "DE", "ID", "AB",
    "C1", "C3", "RP", "EM", "OI", "FU", "FX", "CR", "NR", "TC",
    "Z9", "U1", "U2", "PU", "PI", "PA", "SN", "EI", "J9", "JI",
    "PD", "PY", "VL", "IS", "AR", "DI", "PG", "WC", "WE", "SC",
    "GA", "UT", "PM", "OA", "DA"
}


# =========================
# 基础工具函数
# =========================

def normalize_whitespace(text: str) -> str:
    """压缩多余空白，但保留基本阅读性。"""
    return re.sub(r"\s+", " ", text).strip()


def split_semicolon_values(value: str) -> List[str]:
    """把以分号分隔的字段拆分为列表。"""
    if not value:
        return []
    parts = [normalize_whitespace(x) for x in value.split(";")]
    return [x for x in parts if x]


def safe_add_field(record: Dict[str, Any], field: str, value: str) -> None:
    """
    向record中写入字段。
    若字段可重复，则存为list。
    若字段已存在且不是list，则自动转为list。
    """
    value = value.rstrip()

    if field in REPEATABLE_FIELDS:
        if field not in record:
            record[field] = []
        record[field].append(value)
    else:
        if field not in record:
            record[field] = value
        else:
            # 理论上非重复字段不该重复出现，但为了稳妥，仍转为list保留
            existing = record[field]
            if isinstance(existing, list):
                existing.append(value)
            else:
                record[field] = [existing, value]


def finalize_record(record: Dict[str, Any], raw_lines: Optional[List[str]] = None) -> Dict[str, Any]:
    """
    对一条记录做后处理：
    1．清理空白
    2．把分号字段拆成list
    3．必要时过滤字段
    4．可选保留原始文本
    """
    finalized: Dict[str, Any] = {}

    for field, value in record.items():
        if not KEEP_ALL_FIELDS and field not in FIELDS_TO_KEEP:
            continue

        if isinstance(value, list):
            cleaned_list = [normalize_whitespace(v) for v in value if normalize_whitespace(v)]

            if field in SEMICOLON_SPLIT_FIELDS:
                expanded = []
                for item in cleaned_list:
                    expanded.extend(split_semicolon_values(item))
                finalized[field] = expanded
            else:
                finalized[field] = cleaned_list

        else:
            cleaned_value = normalize_whitespace(value)

            if field in SEMICOLON_SPLIT_FIELDS:
                finalized[field] = split_semicolon_values(cleaned_value)
            else:
                finalized[field] = cleaned_value

    if raw_lines:
        finalized["raw_text"] = "\n".join(raw_lines).strip()

    return finalized


def build_screening_record(raw_record: Dict[str, Any]) -> Dict[str, Any]:
    """
    从完整记录派生出供LLM筛选的精简版记录。
    """
    screening = {
        "id": raw_record.get("id", ""),
        "title": raw_record.get("TI", ""),
        "abstract": raw_record.get("AB", ""),
        "author_keywords": raw_record.get("DE", []) if isinstance(raw_record.get("DE"), list)
        else split_semicolon_values(raw_record.get("DE", "")),
        "keywords_plus": raw_record.get("ID", []) if isinstance(raw_record.get("ID"), list)
        else split_semicolon_values(raw_record.get("ID", "")),
    }
    return screening

# =========================
# 主解析逻辑
# =========================

def parse_wos_txt(file_path: Path) -> List[Dict[str, Any]]:
    """
    解析WOS纯文本文件。
    规则：
    1．以ER结尾表示一条文献记录结束
    2．形如'XX value'或单独'XX'表示新字段，XX为两位大写字母数字标签
    3．以空格开头的行为续行，拼接到上一字段
    """
    text = file_path.read_text(encoding="utf-8", errors="ignore")
    lines = text.splitlines()

    records: List[Dict[str, Any]] = []
    current_record: Dict[str, Any] = {}
    current_field: Optional[str] = None
    current_raw_lines: List[str] = []

    # 既能匹配 "TI xxx"，也能匹配单独的 "ER"
    new_field_pattern = re.compile(r"^([A-Z0-9]{2})(?:\s(.*))?$")

    # 文件头信息，且当前尚未进入文献记录时，直接跳过
    file_header_fields = {"FN", "VR"}

    for line in lines:
        stripped = line.rstrip("\n")

        if not stripped.strip():
            continue

        match = new_field_pattern.match(stripped)

        if match:
            field = match.group(1)
            content = match.group(2) or ""

            # 文件开头的元信息，不属于具体文献
            if not current_record and field in file_header_fields:
                continue

            # 一条记录结束
            if field == "ER":
                if current_record:
                    finalized = finalize_record(current_record, raw_lines=current_raw_lines)
                    records.append(finalized)
                current_record = {}
                current_field = None
                current_raw_lines = []
                continue

            # 开始新字段
            safe_add_field(current_record, field, content)
            current_field = field
            current_raw_lines.append(stripped)

        else:
            # 续行
            if current_field is None:
                continue

            continuation = stripped.strip()
            current_raw_lines.append(stripped)

            existing_value = current_record[current_field]

            if isinstance(existing_value, list):
                if existing_value:
                    existing_value[-1] = existing_value[-1].rstrip() + " " + continuation
                else:
                    existing_value.append(continuation)
            else:
                current_record[current_field] = existing_value.rstrip() + " " + continuation

    # 万一最后一条没有ER，也尽量保留
    if current_record:
        finalized = finalize_record(current_record, raw_lines=current_raw_lines)
        records.append(finalized)

    return records


def export_json(data: Any, file_path: Path) -> None:
    file_path.parent.mkdir(parents=True, exist_ok=True)
    with file_path.open("w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


def build_record_id(raw_record: Dict[str, Any], index: int) -> str:
    """
    为每条文献生成稳定且可唯一匹配的id。
    优先级：UT > DI > 内部编号
    """
    ut = raw_record.get("UT", "")
    if isinstance(ut, str) and ut.strip():
        return ut.strip()

    doi = raw_record.get("DI", "")
    if isinstance(doi, str) and doi.strip():
        return doi.strip()

    return f"REC_{index:06d}"

# def main() -> None:
#     if not INPUT_FILE.exists():
#         raise FileNotFoundError(f"未找到输入文件：{INPUT_FILE}")
#
#     raw_records = parse_wos_txt(INPUT_FILE)
#
#     for i, record in enumerate(raw_records, start=1):
#         record["id"] = build_record_id(record, i)
#
#     screening_records = [build_screening_record(r) for r in raw_records]
#
#     export_json(raw_records, OUTPUT_RAW_JSON)
#     export_json(screening_records, OUTPUT_SCREENING_JSON)
#
#     print(f"解析完成，共得到{len(raw_records)}条文献记录。")
#     print(f"完整版已保存到：{OUTPUT_RAW_JSON}")
#     print(f"筛选版已保存到：{OUTPUT_SCREENING_JSON}")
#
#     if raw_records:
#         sample = raw_records[0]
#         print("\n第一条文献示例：")
#         print("UT：", sample.get("UT", ""))
#         print("TI：", sample.get("TI", ""))
#         print("PY：", sample.get("PY", ""))
#         print("DI：", sample.get("DI", ""))


def main() -> None:
    args = parse_args()

    input_file = Path(args.input)
    output_raw_json = Path(args.output_raw)
    output_screening_json = Path(args.output_screening)

    if not input_file.exists():
        raise FileNotFoundError(f"未找到输入文件：{input_file}")

    raw_records = parse_wos_txt(input_file)

    for i, record in enumerate(raw_records, start=1):
        record["id"] = build_record_id(record, i)

    screening_records = [build_screening_record(r) for r in raw_records]

    export_json(raw_records, output_raw_json)
    export_json(screening_records, output_screening_json)

    print(f"解析完成，共得到{len(raw_records)}条文献记录。")
    print(f"完整版已保存到：{output_raw_json}")
    print(f"筛选版已保存到：{output_screening_json}")

    if raw_records:
        sample = raw_records[0]
        print("\n第一条文献示例：")
        print("UT：", sample.get("UT", ""))
        print("TI：", sample.get("TI", ""))
        print("PY：", sample.get("PY", ""))
        print("DI：", sample.get("DI", ""))

if __name__ == "__main__":
    main()