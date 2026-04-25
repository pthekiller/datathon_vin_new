from __future__ import annotations

from pathlib import Path
from typing import Dict, Optional

import pandas as pd

from processing_data_aligned import DataProcessor
from Transform import Transform

def _resolve_existing_dir(dir_name: str) -> Path:
    """Tìm thư mục input theo cwd, thư mục script, rồi project root."""
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


def _resolve_output_dir(dir_name: str) -> Path:
    """Ưu tiên export ra project root nếu script nằm trong processing/."""
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


class ExportBusiness:
    """
    Export layer cho pipeline business.

    Pipeline chuẩn:
        Raw -> processing_data_aligned.py -> Transform.py -> export_business.py

    Mục tiêu:
    - Giữ vai trò tương tự Export.py: làm tầng export cuối pipeline.
    - Xuất toàn bộ bảng mà Transform.transform() tạo ra ra thư mục clean.
    - Tạo cấu trúc thư mục rõ ràng để Power BI / EDA / forecasting dùng trực tiếp.
    - Không ép người dùng phải export thủ công từng bảng.

    Cấu trúc output mặc định:
        Clean_Data/
          processed/
            products.csv
            customers.csv
            ...
            quality_report.csv
            processed_table_summary.csv
          datamart/
            dim/
              dim_date.csv
              dim_product.csv
              ...
            fact/
              fact_sales_item.csv
              fact_orders.csv
              ...
            mart/
              mart_sales_overview.csv
              mart_customer_insight.csv
              ...
            validation/
              sales_reconciliation.csv
            metadata/
              table_inventory.csv
              build_notes.txt

    Ghi chú:
    - Export toàn bộ 24 bảng chính từ Transform.table.
    - Export thêm bảng validation sales_reconciliation nếu có.
    - Export thêm metadata để tiện kiểm tra số dòng/số cột khi nạp vào Power BI.
    """

    __slots__ = (
        "_processor",
        "_transformer",
        "_root_path",
        "_tables",
        "_validation_tables",
    )

    # ==================== CONSTRUCTOR ====================
    def __init__(
        self,
        processor: Optional[DataProcessor] = None,
        transformer: Optional[Transform] = None,
        root_path: str = "output/Clean_Data",
    ) -> None:
        self._processor = processor
        self._transformer = transformer
        self._root_path = ""
        self._tables: Optional[Dict[str, pd.DataFrame]] = None
        self._validation_tables: Dict[str, pd.DataFrame] = {}
        self.root_path = root_path

    # ==================== GETTER / SETTER ====================
    @property
    def processor(self) -> Optional[DataProcessor]:
        return self._processor

    @processor.setter
    def processor(self, value: Optional[DataProcessor]) -> None:
        if value is not None and not isinstance(value, DataProcessor):
            raise TypeError("processor phải là DataProcessor hoặc None.")
        self._processor = value

    @property
    def transformer(self) -> Optional[Transform]:
        return self._transformer

    @transformer.setter
    def transformer(self, value: Optional[Transform]) -> None:
        if value is not None and not isinstance(value, Transform):
            raise TypeError("transformer phải là Transform hoặc None.")
        self._transformer = value
        self._tables = None

    @property
    def root_path(self) -> str:
        return self._root_path

    @root_path.setter
    def root_path(self, value: str) -> None:
        if not isinstance(value, str):
            raise TypeError("root_path phải là string.")
        resolved = _resolve_output_dir(value)
        self._root_path = str(resolved)
        resolved.mkdir(parents=True, exist_ok=True)

    # ==================== INTERNAL HELPERS ====================
    def _ensure_processor_ready(self) -> None:
        """Đảm bảo processor đã có processed_data để export."""
        if self.processor is None:
            return

        if not getattr(self.processor, "processed_data", None):
            self.processor.load_data()
            self.processor.process_all_data()

    def _ensure_transform_ready(self) -> None:
        """Đảm bảo transformer đã build xong toàn bộ bảng."""
        if self.transformer is None:
            raise ValueError("transformer đang là None. Hãy truyền Transform vào ExportBusiness.")

        if not getattr(self.transformer, "table", None):
            self.transformer.transform()

        self._tables = self.transformer.table

        # validation tables ngoài 24 bảng chính
        self._validation_tables = {}
        if hasattr(self.transformer, "get_sales_reconciliation"):
            reconciliation = self.transformer.get_sales_reconciliation()
            if isinstance(reconciliation, pd.DataFrame) and not reconciliation.empty:
                self._validation_tables["sales_reconciliation"] = reconciliation

    @staticmethod
    def _save_csv(df: pd.DataFrame, output_path: Path) -> None:
        """Lưu DataFrame ra CSV, tự tạo thư mục cha nếu cần."""
        if df is None:
            raise ValueError(f"Không có dữ liệu để lưu: {output_path.name}")
        if not isinstance(df, pd.DataFrame):
            raise TypeError(f"Dữ liệu cần lưu phải là DataFrame. Got: {type(df)}")

        output_path.parent.mkdir(parents=True, exist_ok=True)
        df.to_csv(output_path, index=False)

    def _build_path(self, layer: str, table_name: str) -> Path:
        """
        Tạo path export theo layer.

        layer hợp lệ:
        - processed
        - dim
        - fact
        - mart
        - validation
        - metadata
        """
        layer = str(layer).strip().lower()
        root = Path(self.root_path)

        if layer == "processed":
            return root / "processed" / f"{table_name}.csv"
        if layer in {"dim", "fact", "mart"}:
            return root / "datamart" / layer / f"{table_name}.csv"
        if layer == "validation":
            return root / "datamart" / "validation" / f"{table_name}.csv"
        if layer == "metadata":
            return root / "datamart" / "metadata" / f"{table_name}.csv"

        raise ValueError(
            "layer không hợp lệ. Hợp lệ: processed/dim/fact/mart/validation/metadata"
        )

    def _categorize_transform_table(self, table_name: str) -> str:
        """Phân loại bảng transform theo prefix tên bảng."""
        if table_name.startswith("dim_"):
            return "dim"
        if table_name.startswith("fact_"):
            return "fact"
        if table_name.startswith("mart_"):
            return "mart"
        raise ValueError(
            f"Không phân loại được bảng '{table_name}'. Tên bảng nên bắt đầu bằng dim_/fact_/mart_."
        )

    def _export_processed_layer(self, include_quality_report: bool = True) -> None:
        """
        Export toàn bộ processed tables từ processing_data_aligned.

        Mục đích:
        - Giữ lại staging clean layer để debug pipeline.
        - Có thể dùng kiểm đối chiếu trước khi vào transform.
        """
        if self.processor is None:
            return

        self._ensure_processor_ready()
        processed_data = getattr(self.processor, "processed_data", {})
        quality_report = getattr(self.processor, "quality_report", {})

        if not processed_data:
            return

        summary_rows = []
        for table_name, df in processed_data.items():
            self._save_csv(df, self._build_path("processed", table_name))
            summary_rows.append(
                {
                    "layer": "processed",
                    "table_name": table_name,
                    "rows": int(df.shape[0]),
                    "columns": int(df.shape[1]),
                }
            )

        summary_df = pd.DataFrame(summary_rows).sort_values(["layer", "table_name"]).reset_index(drop=True)
        self._save_csv(summary_df, self._build_path("processed", "processed_table_summary"))

        if include_quality_report and quality_report:
            report_rows = []
            for table_name, report in quality_report.items():
                report_rows.append(
                    {
                        "table_name": table_name,
                        "raw_rows": report.get("raw_rows"),
                        "processed_rows": report.get("processed_rows"),
                        "rows_removed": report.get("rows_removed"),
                        "raw_columns": report.get("raw_columns"),
                        "processed_columns": report.get("processed_columns"),
                        "raw_duplicate_rows": report.get("raw_duplicate_rows"),
                        "processed_duplicate_rows": report.get("processed_duplicate_rows"),
                        "raw_missing_cells": report.get("raw_missing_cells"),
                        "processed_missing_cells": report.get("processed_missing_cells"),
                    }
                )
            quality_df = pd.DataFrame(report_rows).sort_values("table_name").reset_index(drop=True)
            self._save_csv(quality_df, self._build_path("processed", "quality_report"))

    def _export_transform_layer(self) -> None:
        """Export toàn bộ bảng Transform ra dim/fact/mart."""
        self._ensure_transform_ready()
        if not self._tables:
            raise ValueError("Transform chưa sinh ra bảng nào để export.")

        for table_name, df in self._tables.items():
            layer = self._categorize_transform_table(table_name)
            self._save_csv(df, self._build_path(layer, table_name))

    def _export_validation_layer(self) -> None:
        """Export các bảng kiểm định/đối chiếu ngoài 24 bảng chính."""
        for table_name, df in self._validation_tables.items():
            self._save_csv(df, self._build_path("validation", table_name))

    def _export_metadata(self) -> None:
        """Export metadata inventory + build notes để dễ kiểm soát pipeline."""
        if not self._tables:
            return

        inventory_rows = []
        for table_name, df in self._tables.items():
            inventory_rows.append(
                {
                    "table_name": table_name,
                    "layer": self._categorize_transform_table(table_name),
                    "rows": int(df.shape[0]),
                    "columns": int(df.shape[1]),
                }
            )

        for table_name, df in self._validation_tables.items():
            inventory_rows.append(
                {
                    "table_name": table_name,
                    "layer": "validation",
                    "rows": int(df.shape[0]),
                    "columns": int(df.shape[1]),
                }
            )

        inventory_df = pd.DataFrame(inventory_rows).sort_values(["layer", "table_name"]).reset_index(drop=True)
        self._save_csv(inventory_df, self._build_path("metadata", "table_inventory"))

        notes_path = Path(self.root_path) / "datamart" / "metadata" / "build_notes.txt"
        notes_path.parent.mkdir(parents=True, exist_ok=True)
        notes = []
        if self.transformer is not None and hasattr(self.transformer, "get_build_notes"):
            notes = self.transformer.get_build_notes()
        notes_path.write_text("\n".join(notes), encoding="utf-8")

    # ==================== PUBLIC API ====================
    def run_export_all(self, include_processed: bool = True, include_validation: bool = True) -> None:
        """
        Chạy full export business.

        Output:
            Clean_Data/
              processed/
              datamart/dim/
              datamart/fact/
              datamart/mart/
              datamart/validation/
              datamart/metadata/
        """
        if include_processed:
            self._export_processed_layer(include_quality_report=True)

        self._export_transform_layer()

        if include_validation:
            self._export_validation_layer()

        self._export_metadata()

    def export_only_transform_tables(self) -> None:
        """Chỉ export phần output của Transform, bỏ qua processed layer."""
        self._export_transform_layer()
        self._export_validation_layer()
        self._export_metadata()


if __name__ == "__main__":
    # Ví dụ chạy độc lập:
    #   python export_business.py
    # Sau khi chạy sẽ tạo thư mục Clean_Data/ chứa toàn bộ processed + datamart tables.

    processor = DataProcessor(data_dir=_resolve_existing_dir("raw_data"))
    processor.load_data()
    processor.process_all_data()

    transformer = Transform(raw_path=Transform.get_path("processed_data"))
    transformer.transform()

    exporter = ExportBusiness(
        processor=processor,
        transformer=transformer,
        root_path=str(_resolve_output_dir("Clean_Data")),
    )
    exporter.run_export_all(include_processed=True, include_validation=True)

    print("Export_business hoàn tất: đã tạo thư mục Clean_Data chứa toàn bộ bảng transform.")
