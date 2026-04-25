from __future__ import annotations

import argparse
from pathlib import Path
from typing import Dict

from processing_data_aligned import DataProcessor
from Transform import Transform
from export_business import ExportBusiness

def _resolve_existing_dir(dir_name: str | Path) -> Path:
    """Tìm thư mục input theo cwd, thư mục script, rồi project root."""
    dir_name = str(dir_name)
    current_file_dir = Path(__file__).resolve().parent
    candidates = [
        Path.cwd() / dir_name,
        current_file_dir.parent.parent / dir_name,
        current_file_dir.parent / dir_name,
        current_file_dir / dir_name,
    ]
    for candidate in candidates:
        if candidate.exists() and candidate.is_dir():
            return candidate.resolve()
    return candidates[-1].resolve()


def _resolve_output_dir(dir_name: str | Path) -> Path:
    """Ưu tiên output cạnh project root khi run_pipeline nằm trong processing/."""
    dir_name = str(dir_name)
    current_file_dir = Path(__file__).resolve().parent
    candidates = [
        Path.cwd() / dir_name,
        current_file_dir.parent.parent / dir_name,
        current_file_dir.parent / dir_name,
        current_file_dir / dir_name,
    ]
    for candidate in candidates:
        if candidate.parent.exists():
            return candidate.resolve()
    return candidates[0].resolve()


def _print_banner(title: str) -> None:
    """In tiêu đề từng bước để theo dõi pipeline rõ ràng hơn."""
    print("\n" + "=" * 80)
    print(title)
    print("=" * 80)


def _validate_input_dir(raw_dir: str | Path) -> Path:
    """Kiểm tra thư mục raw input có tồn tại hay không."""
    raw_path = _resolve_existing_dir(raw_dir)
    if not raw_path.exists():
        raise FileNotFoundError(f"Không tìm thấy thư mục raw data: {raw_path}")
    if not raw_path.is_dir():
        raise NotADirectoryError(f"Đường dẫn raw_data không phải thư mục: {raw_path}")
    return raw_path


def _summarize_processed(processor: DataProcessor) -> None:
    """In tóm tắt tầng process."""
    report: Dict[str, Dict] = getattr(processor, "quality_report", {}) or {}
    if not report:
        print("[PROCESS] Chưa có quality report.")
        return

    total_raw_rows = sum(int(v.get("raw_rows", 0)) for v in report.values())
    total_processed_rows = sum(int(v.get("processed_rows", 0)) for v in report.values())
    total_raw_missing = sum(int(v.get("raw_missing_cells", 0)) for v in report.values())
    total_processed_missing = sum(int(v.get("processed_missing_cells", 0)) for v in report.values())

    print(f"[PROCESS] Số bảng đã xử lý: {len(report)}")
    print(f"[PROCESS] Tổng raw rows: {total_raw_rows:,}")
    print(f"[PROCESS] Tổng processed rows: {total_processed_rows:,}")
    print(f"[PROCESS] Raw missing cells: {total_raw_missing:,}")
    print(f"[PROCESS] Processed missing cells: {total_processed_missing:,}")


def _summarize_transform(transformer: Transform) -> None:
    """In tóm tắt tầng transform."""
    tables = getattr(transformer, "table", {}) or {}
    if not tables:
        print("[TRANSFORM] Chưa có bảng transform nào được build.")
        return

    dim_count = sum(1 for name in tables if name.startswith("dim_"))
    fact_count = sum(1 for name in tables if name.startswith("fact_"))
    mart_count = sum(1 for name in tables if name.startswith("mart_"))

    print(f"[TRANSFORM] Tổng số bảng chính: {len(tables)}")
    print(f"[TRANSFORM] Dimensions: {dim_count}")
    print(f"[TRANSFORM] Facts: {fact_count}")
    print(f"[TRANSFORM] Marts: {mart_count}")

    if hasattr(transformer, "get_sales_reconciliation"):
        reconciliation = transformer.get_sales_reconciliation()
        if reconciliation is not None and not reconciliation.empty:
            print(f"[TRANSFORM] Validation table sales_reconciliation: {len(reconciliation):,} dòng")


def build_argument_parser() -> argparse.ArgumentParser:
    """Tạo parser cho phép chạy pipeline linh hoạt từ CLI."""
    parser = argparse.ArgumentParser(
        description=(
            "Chạy full business ETL pipeline: raw_data -> process -> transform -> export."
        )
    )
    parser.add_argument(
        "--raw-dir",
        default="data",
        help="Thư mục chứa dữ liệu nguồn CSV. Mặc định: raw_data",
    )
    parser.add_argument(
        "--processed-dir",
        default="output/processed_data",
        help="Thư mục staging clean data sau bước process. Mặc định: processed_data",
    )
    parser.add_argument(
        "--clean-dir",
        default="output/Clean_Data",
        help="Thư mục output cuối cùng chứa processed + datamart. Mặc định: Clean_Data",
    )
    parser.add_argument(
        "--skip-processed-export",
        action="store_true",
        help="Nếu bật, exporter sẽ không copy processed layer vào Clean_Data/processed.",
    )
    parser.add_argument(
        "--skip-validation",
        action="store_true",
        help="Nếu bật, exporter sẽ không xuất validation tables.",
    )
    return parser


def run_pipeline(
    raw_dir: str | Path = "data",
    processed_dir: str | Path = "output/processed_data",
    clean_dir: str | Path = "output/Clean_Data",
    include_processed_export: bool = True,
    include_validation: bool = True,
) -> None:
    """
    Chạy full pipeline doanh nghiệp.

    Luồng dữ liệu:
        raw_data -> process -> transform -> export

    Parameters
    ----------
    raw_dir : str | Path
        Thư mục dữ liệu nguồn.
    processed_dir : str | Path
        Thư mục staging sau bước process.
    clean_dir : str | Path
        Thư mục output cuối cùng cho Power BI / EDA / forecasting.
    include_processed_export : bool
        Có export lại processed layer vào Clean_Data/processed hay không.
    include_validation : bool
        Có export validation layer (sales_reconciliation) hay không.
    """
    raw_path = _validate_input_dir(raw_dir)
    processed_path = _resolve_output_dir(processed_dir)
    clean_path = _resolve_output_dir(clean_dir)

    _print_banner("BƯỚC 1 - PROCESS RAW DATA")
    processor = DataProcessor(data_dir=raw_path)
    processor.load_data()
    processor.process_all_data()
    processor.export_processed_data(output_dir=processed_path)
    _summarize_processed(processor)
    print(f"[PROCESS] Đã export staging clean data vào: {processed_path.resolve()}")

    _print_banner("BƯỚC 2 - TRANSFORM SANG DATA WAREHOUSE / DATA MART")
    transformer = Transform(raw_path=Transform.get_path(str(processed_path)))
    transformer.transform()
    _summarize_transform(transformer)

    _print_banner("BƯỚC 3 - EXPORT CLEAN LAYER CHO POWER BI / EDA / FORECAST")
    exporter = ExportBusiness(
        processor=processor,
        transformer=transformer,
        root_path=str(clean_path),
    )
    exporter.run_export_all(
        include_processed=include_processed_export,
        include_validation=include_validation,
    )

    print(f"[EXPORT] Đã tạo thư mục clean tại: {clean_path.resolve()}")
    print(f"[EXPORT] Processed layer: {clean_path / 'processed'}")
    print(f"[EXPORT] Datamart layer: {clean_path / 'datamart'}")

    _print_banner("PIPELINE HOÀN TẤT")
    print("Luồng chạy thành công: raw_data -> process -> transform -> export")


if __name__ == "__main__":
    parser = build_argument_parser()
    args = parser.parse_args()

    run_pipeline(
        raw_dir=args.raw_dir,
        processed_dir=args.processed_dir,
        clean_dir=args.clean_dir,
        include_processed_export=not args.skip_processed_export,
        include_validation=not args.skip_validation,
    )
