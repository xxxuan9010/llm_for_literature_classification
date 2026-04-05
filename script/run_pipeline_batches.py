# -*- coding: utf-8 -*-
# @Author  : hanyx9010@163.com
# @Time    : 2026/3/18 22:15
# @File    : run_pipeline_batches.py
# @Description    :
import argparse
import re
import subprocess
import sys
from pathlib import Path


def parse_args():
    parser = argparse.ArgumentParser(
        description="Run the whole WOS screening pipeline batch by batch."
    )
    parser.add_argument(
        "--data-dir",
        default=None,
        help="data目录路径，默认自动定位为 scripts 的上一级目录下的 data"
    )
    parser.add_argument(
        "--raw-dir",
        default=None,
        help="原始批次txt目录，默认 data/batches_raw"
    )
    parser.add_argument(
        "--sleep",
        type=float,
        default=1.0,
        help="传给 llm_screen.py 的 sleep 参数，默认 1.0"
    )
    parser.add_argument(
        "--temperature",
        type=float,
        default=0.0,
        help="传给 llm_screen.py 的 temperature 参数，默认 0.0"
    )
    parser.add_argument(
        "--max-retries",
        type=int,
        default=3,
        help="传给 llm_screen.py 的 max-retries 参数，默认 3"
    )
    parser.add_argument(
        "--start-batch",
        type=int,
        default=None,
        help="只跑从第几个批次开始（按排序后的序号，从1开始）"
    )
    parser.add_argument(
        "--end-batch",
        type=int,
        default=None,
        help="只跑到第几个批次结束（按排序后的序号，从1开始）"
    )
    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="即使最终输出已存在，也重新执行该批次"
    )
    return parser.parse_args()


def ensure_dir(path: Path):
    path.mkdir(parents=True, exist_ok=True)


def find_batch_files(raw_dir: Path):
    # pattern = re.compile(r"^(\d+)-(\d+)\.txt$")
    pattern = re.compile(r"^CNKI(\d+)_(\d+)\.txt$")
    files = []

    for path in raw_dir.iterdir():
        if not path.is_file():
            continue
        m = pattern.match(path.name)
        if m:
            start = int(m.group(1))
            end = int(m.group(2))
            files.append((start, end, path))

    files.sort(key=lambda x: x[0])
    return files


def run_command(cmd):
    print("\n>>> 执行命令：")
    print(" ".join(str(x) for x in cmd))
    subprocess.run(cmd, check=True)


def main():
    args = parse_args()

    script_dir = Path(__file__).resolve().parent
    project_root = script_dir.parent

    data_dir = Path(args.data_dir) if args.data_dir else project_root / "data"
    raw_dir = Path(args.raw_dir) if args.raw_dir else data_dir / "batches_raw_cn"

    if not raw_dir.exists():
        raise FileNotFoundError(f"未找到原始批次目录：{raw_dir}")

    # 各阶段输出目录
    raw_records_dir = data_dir / "raw_records"
    screening_records_dir = data_dir / "screening_records"
    screening_tasks_dir = data_dir / "screening_tasks"
    screening_results_dir = data_dir / "screening_results"
    filtered_root_dir = data_dir / "filtered_wos"
    filtered_a_dir = filtered_root_dir / "A"
    filtered_b_dir = filtered_root_dir / "B"
    filtered_c_dir = filtered_root_dir / "C"
    filtered_error_dir = filtered_root_dir / "ERROR"

    for d in [
        raw_records_dir,
        screening_records_dir,
        screening_tasks_dir,
        screening_results_dir,
        filtered_a_dir,
        filtered_b_dir,
        filtered_c_dir,
    ]:
        ensure_dir(d)

    # 四个核心脚本路径
    parse_script = script_dir / "parse_wos_full.py"
    build_tasks_script = script_dir / "build_screening_tasks.py"
    llm_script = script_dir / "llm_screen.py"
    export_script = script_dir / "filtered_wos_txt.py"

    for p in [parse_script, build_tasks_script, llm_script, export_script]:
        if not p.exists():
            raise FileNotFoundError(f"未找到脚本：{p}")

    batch_files = find_batch_files(raw_dir)
    if not batch_files:
        raise ValueError(f"在目录 {raw_dir} 下没有找到形如 1-500.txt 的批次文件。")

    # 按批次序号切片
    start_idx = 1 if args.start_batch is None else args.start_batch
    end_idx = len(batch_files) if args.end_batch is None else args.end_batch

    if start_idx < 1 or end_idx > len(batch_files) or start_idx > end_idx:
        raise ValueError("start-batch 或 end-batch 参数不合法。")

    selected = batch_files[start_idx - 1:end_idx]

    print(f"共发现 {len(batch_files)} 个批次文件。")
    print(f"本次执行第 {start_idx} 到第 {end_idx} 个批次，共 {len(selected)} 个。")

    python_exe = sys.executable

    for batch_no, (start_num, end_num, raw_txt_path) in enumerate(selected, start=start_idx):
        stem = f"{start_num}-{end_num}"

        raw_records_path = raw_records_dir / f"{stem}.json"
        screening_records_path = screening_records_dir / f"{stem}.json"
        screening_tasks_path = screening_tasks_dir / f"{stem}.jsonl"
        screening_results_path = screening_results_dir / f"{stem}.jsonl"
        output_a_path = filtered_a_dir / f"A_{stem}.txt"
        output_b_path = filtered_b_dir / f"B_{stem}.txt"
        output_c_path = filtered_c_dir / f"C_{stem}.txt"
        output_error_path = filtered_error_dir / f"E_{stem}.txt"

        print("\n" + "=" * 80)
        print(f"开始处理第 {batch_no} 个批次：{raw_txt_path.name}")
        print("=" * 80)

        # 若最终产物都存在，则默认跳过
        if (
            not args.overwrite
            and raw_records_path.exists()
            and screening_records_path.exists()
            and screening_tasks_path.exists()
            and screening_results_path.exists()
            and output_a_path.exists()
            and output_b_path.exists()
            and output_c_path.exists()
        ):
            print(f"该批次已完成，跳过：{stem}")
            continue

        # Step 1: parse_wos_full.py
        run_command([
            python_exe,
            str(parse_script),
            "--input", str(raw_txt_path),
            "--output-raw", str(raw_records_path),
            "--output-screening", str(screening_records_path),
        ])

        # Step 2: build_screening_tasks.py
        run_command([
            python_exe,
            str(build_tasks_script),
            "--input", str(screening_records_path),
            "--output", str(screening_tasks_path),
        ])

        # Step 3: llm_screen.py
        run_command([
            python_exe,
            str(llm_script),
            "--input", str(screening_tasks_path),
            "--output", str(screening_results_path),
            "--sleep", str(args.sleep),
            "--temperature", str(args.temperature),
            "--max-retries", str(args.max_retries),
        ])

        # Step 4: filtered_wos_txt.py
        run_command([
            python_exe,
            str(export_script),
            "--batches_raw", str(raw_txt_path),
            "--result", str(screening_results_path),
            "--output-a", str(output_a_path),
            "--output-b", str(output_b_path),
            "--output-c", str(output_c_path),
            "--output-error", str(output_error_path)
        ])

        print(f"\n批次完成：{stem}")

    print("\n全部批次执行结束。")


if __name__ == "__main__":
    main()