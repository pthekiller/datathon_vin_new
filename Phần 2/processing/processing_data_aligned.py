from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, List

import numpy as np
import pandas as pd


class DataProcessor:
    """
    Lớp tiền xử lý dữ liệu đầu vào theo hướng tương thích với Transform.py.

    Mục tiêu chính:
    - Giữ nguyên schema gốc để Transform có thể đọc lại trực tiếp.
    - Không loại outlier hàng loạt bằng thống kê chung cho mọi bảng.
    - Ưu tiên làm sạch kiểu dữ liệu, ngày tháng, khoảng trắng, giá trị âm bất hợp lý.
    - Impute theo ngữ nghĩa nghiệp vụ ở mức an toàn; tránh bơm mean vào mọi cột số.
    - Bao gồm luôn sales.csv để pipeline Load -> Process -> Transform khép kín.

    Đầu ra của tầng process nên được xem như "staging clean data":
    - Tên file giữ nguyên.
    - Tên cột giữ nguyên.
    - Không thêm cột feature mới để tránh làm lệch tầng transform.
    """

    __slots__ = (
        "_data_dir",
        "_paths",
        "_raw_data",
        "_processed_data",
        "_quality_report",
    )

    # =========================
    # PROPERTY
    # =========================
    @property
    def data_dir(self) -> Path:
        """Thư mục chứa dữ liệu raw."""
        return self._data_dir

    @data_dir.setter
    def data_dir(self, value: str | Path) -> None:
        """Thiết lập thư mục chứa dữ liệu raw."""
        if not isinstance(value, (str, Path)):
            raise TypeError("data_dir phải là str hoặc Path.")
        self._data_dir = Path(value)

    @property
    def paths(self) -> Dict[str, Path]:
        """Từ điển tên bảng -> đường dẫn file CSV."""
        return self._paths

    @paths.setter
    def paths(self, value: Dict[str, Path]) -> None:
        """Thiết lập từ điển đường dẫn dữ liệu."""
        if not isinstance(value, dict):
            raise TypeError("paths phải là dictionary.")
        self._paths = value

    @property
    def raw_data(self) -> Dict[str, pd.DataFrame]:
        """Dữ liệu raw đã load."""
        return self._raw_data

    @raw_data.setter
    def raw_data(self, value: Dict[str, pd.DataFrame]) -> None:
        """Thiết lập dictionary raw_data."""
        if not isinstance(value, dict):
            raise TypeError("raw_data phải là dictionary.")
        self._raw_data = value

    @property
    def processed_data(self) -> Dict[str, pd.DataFrame]:
        """Dữ liệu sau xử lý."""
        return self._processed_data

    @processed_data.setter
    def processed_data(self, value: Dict[str, pd.DataFrame]) -> None:
        """Thiết lập dictionary processed_data."""
        if not isinstance(value, dict):
            raise TypeError("processed_data phải là dictionary.")
        self._processed_data = value

    @property
    def quality_report(self) -> Dict[str, Dict[str, Any]]:
        """Báo cáo chất lượng dữ liệu theo từng bảng."""
        return self._quality_report

    @quality_report.setter
    def quality_report(self, value: Dict[str, Dict[str, Any]]) -> None:
        """Thiết lập quality report."""
        if not isinstance(value, dict):
            raise TypeError("quality_report phải là dictionary.")
        self._quality_report = value

    # =========================
    # INIT
    # =========================
    def __init__(self, data_dir: str | Path = "data") -> None:
        self._data_dir = Path()
        self._paths = {}
        self._raw_data = {}
        self._processed_data = {}
        self._quality_report = {}

        self.data_dir = data_dir
        self.paths = self._build_paths()

    # =========================
    # STATIC CONFIG
    # =========================
    @staticmethod
    def get_path(root_dir: str | Path = "data") -> Dict[str, str]:
        """Trả về mapping đường dẫn giống tinh thần Transform.get_path()."""
        root = Path(root_dir)
        return {
            "products": str(root / "products.csv"),
            "customers": str(root / "customers.csv"),
            "promotions": str(root / "promotions.csv"),
            "geography": str(root / "geography.csv"),
            "orders": str(root / "orders.csv"),
            "order_items": str(root / "order_items.csv"),
            "payments": str(root / "payments.csv"),
            "shipments": str(root / "shipments.csv"),
            "returns": str(root / "returns.csv"),
            "reviews": str(root / "reviews.csv"),
            "sales": str(root / "sales.csv"),
            "inventory": str(root / "inventory.csv"),
            "web_traffic": str(root / "web_traffic.csv"),
        }

    # =========================
    # INTERNAL HELPERS
    # =========================
    def _resolve_file_path(self, file_name: str) -> Path:
        """Tìm file dữ liệu theo nhiều vị trí, kể cả thư mục forecast ở project root."""
        candidates = [
            self.data_dir / file_name,
            Path.cwd() / file_name,
            Path.cwd() / "raw_data" / file_name,
            Path(__file__).resolve().parent / file_name,
            Path(__file__).resolve().parent.parent / file_name,
            Path(__file__).resolve().parent.parent.parent / "data" / file_name,
        ]
        for candidate in candidates:
            if candidate.exists() and candidate.is_file():
                return candidate.resolve()

        project_root = Path(__file__).resolve().parent.parent
        recursive_matches = list(project_root.rglob(file_name))
        if recursive_matches:
            return recursive_matches[0].resolve()

        return (self.data_dir / file_name).resolve()

    def _build_paths(self) -> Dict[str, Path]:
        """Xây dựng dictionary chứa đường dẫn các file dữ liệu nguồn."""
        return {
            "products": self._resolve_file_path("products.csv"),
            "customers": self._resolve_file_path("customers.csv"),
            "promotions": self._resolve_file_path("promotions.csv"),
            "geography": self._resolve_file_path("geography.csv"),
            "orders": self._resolve_file_path("orders.csv"),
            "order_items": self._resolve_file_path("order_items.csv"),
            "payments": self._resolve_file_path("payments.csv"),
            "shipments": self._resolve_file_path("shipments.csv"),
            "returns": self._resolve_file_path("returns.csv"),
            "reviews": self._resolve_file_path("reviews.csv"),
            "sales": self._resolve_file_path("sales.csv"),
            "inventory": self._resolve_file_path("inventory.csv"),
            "web_traffic": self._resolve_file_path("web_traffic.csv"),
        }

    def _build_empty_optional_table(self, table_name: str) -> pd.DataFrame:
        """Tạo bảng rỗng cho các nguồn chưa có để pipeline vẫn chạy tiếp."""
        empty_schema = {
            "shipments": ["order_id", "ship_date", "delivery_date", "shipping_fee"],
            "sales": ["Date", "Revenue", "COGS"],
            "web_traffic": [
                "date", "traffic_source", "sessions", "unique_visitors",
                "page_views", "bounce_rate", "avg_session_duration_sec"
            ],
        }
        return pd.DataFrame(columns=empty_schema.get(table_name, []))

    def _load_raw_data(self) -> Dict[str, pd.DataFrame]:
        """Load toàn bộ dữ liệu từ các file CSV."""
        loaded_data: Dict[str, pd.DataFrame] = {}
        optional_tables = {"shipments", "sales", "web_traffic"}
        for table_name, file_path in self.paths.items():
            if file_path.exists():
                loaded_data[table_name] = pd.read_csv(file_path, low_memory=False)
            elif table_name in optional_tables:
                print(f"[WARN] Không tìm thấy {table_name}: {file_path}. Sẽ dùng bảng rỗng để pipeline tiếp tục.")
                loaded_data[table_name] = self._build_empty_optional_table(table_name)
            else:
                raise FileNotFoundError(f"Không tìm thấy file bắt buộc cho bảng '{table_name}': {file_path}")
        return loaded_data

    def _get_datetime_columns(self, df: pd.DataFrame) -> List[str]:
        """Lấy các cột thời gian theo quy ước tên cột."""
        return [col for col in df.columns if "date" in col.lower()]

    def _get_numeric_columns_by_table(self, table_name: str) -> List[str]:
        """Khai báo cột số theo từng bảng để ép kiểu an toàn, không phụ thuộc infer tự động."""
        mapping: Dict[str, List[str]] = {
            "products": ["price", "cogs"],
            "customers": ["zip"],
            "promotions": ["discount_value", "min_order_value"],
            "geography": ["zip"],
            "order_items": ["quantity", "unit_price", "discount_amount"],
            "payments": ["payment_value", "installments"],
            "returns": ["return_quantity", "refund_amount"],
            "reviews": ["rating"],
            "sales": ["Revenue", "COGS"],
            "shipments": ["shipping_fee"],
            "inventory": [
                "stock_on_hand",
                "units_received",
                "units_sold",
                "stockout_days",
                "days_of_supply",
                "fill_rate",
                "stockout_flag",
                "overstock_flag",
                "reorder_flag",
                "sell_through_rate",
                "year",
                "month",
            ],
            "web_traffic": [
                "sessions",
                "unique_visitors",
                "page_views",
                "bounce_rate",
                "avg_session_duration_sec",
            ],
        }
        return [col for col in mapping.get(table_name, []) if col in self.raw_data.get(table_name, pd.DataFrame()).columns]

    def _standardize_text_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Chuẩn hoá cột text:
        - strip khoảng trắng đầu/cuối
        - đổi chuỗi rỗng thành NA
        """
        out = df.copy()
        for col in out.select_dtypes(include=["object", "string"]).columns:
            out[col] = out[col].astype("string").str.strip()
            out[col] = out[col].replace({"": pd.NA, "nan": pd.NA, "None": pd.NA})
        return out

    def _coerce_numeric_columns(self, table_name: str, df: pd.DataFrame) -> pd.DataFrame:
        """Ép kiểu các cột số theo cấu trúc từng bảng."""
        out = df.copy()
        for col in self._get_numeric_columns_by_table(table_name):
            out[col] = pd.to_numeric(out[col], errors="coerce")
        return out

    def _process_datetime(self, df: pd.DataFrame) -> pd.DataFrame:
        """Chuyển các cột ngày về datetime, không sinh thêm cột phụ."""
        out = df.copy()
        for col in self._get_datetime_columns(out):
            out[col] = pd.to_datetime(out[col], errors="coerce")
        return out

    def _drop_full_duplicates(self, df: pd.DataFrame) -> pd.DataFrame:
        """Chỉ loại bỏ duplicate toàn dòng; không động đến các dòng extreme nhưng hợp lệ."""
        return df.drop_duplicates().reset_index(drop=True)

    def _clip_or_nullify_invalid_numeric(self, table_name: str, df: pd.DataFrame) -> pd.DataFrame:
        """
        Xử lý các giá trị âm / ngoài miền hợp lý theo luật nghiệp vụ.

        Triết lý:
        - Không drop dòng.
        - Đưa giá trị bất hợp lý về NaN rồi mới fill chọn lọc ở bước sau.
        """
        out = df.copy()

        non_negative_rules: Dict[str, List[str]] = {
            "products": ["price", "cogs"],
            "promotions": ["discount_value", "min_order_value"],
            "order_items": ["quantity", "unit_price", "discount_amount"],
            "payments": ["payment_value", "installments"],
            "returns": ["return_quantity", "refund_amount"],
            "reviews": ["rating"],
            "sales": ["Revenue", "COGS"],
            "shipments": ["shipping_fee"],
            "inventory": [
                "stock_on_hand",
                "units_received",
                "units_sold",
                "stockout_days",
                "days_of_supply",
                "fill_rate",
                "sell_through_rate",
            ],
            "web_traffic": [
                "sessions",
                "unique_visitors",
                "page_views",
                "bounce_rate",
                "avg_session_duration_sec",
            ],
        }

        for col in non_negative_rules.get(table_name, []):
            if col in out.columns:
                out.loc[out[col] < 0, col] = np.nan

        if table_name == "reviews" and "rating" in out.columns:
            out.loc[(out["rating"] < 1) | (out["rating"] > 5), "rating"] = np.nan

        if table_name == "web_traffic" and "bounce_rate" in out.columns:
            out.loc[out["bounce_rate"] > 1, "bounce_rate"] = np.nan

        if table_name == "inventory":
            for flag_col in ["stockout_flag", "overstock_flag", "reorder_flag"]:
                if flag_col in out.columns:
                    out.loc[~out[flag_col].isin([0, 1, np.nan]), flag_col] = np.nan

        return out

    def _impute_business_missing(self, table_name: str, df: pd.DataFrame) -> pd.DataFrame:
        """
        Điền missing theo ngữ nghĩa nghiệp vụ an toàn.

        Nguyên tắc:
        - Chỉ fill những cột mà NA gần như chắc chắn mang nghĩa "không có / 0".
        - Các cột dimension/attribute để nguyên NA nếu đó là unknown thật sự.
        """
        out = df.copy()

        zero_fill_map: Dict[str, List[str]] = {
            "order_items": ["discount_amount"],
            "shipments": ["shipping_fee"],
            "returns": ["refund_amount"],
            "promotions": ["min_order_value"],
            "inventory": ["stockout_days", "stockout_flag", "overstock_flag", "reorder_flag"],
        }

        for col in zero_fill_map.get(table_name, []):
            if col in out.columns:
                out[col] = out[col].fillna(0)

        # Các ID promo thiếu nên giữ là NA; đây là tín hiệu "không có khuyến mãi".
        # applicable_category thiếu trong promotions cũng giữ nguyên vì có thể là promo toàn cục.

        if table_name == "inventory":
            for flag_col in ["stockout_flag", "overstock_flag", "reorder_flag"]:
                if flag_col in out.columns:
                    out[flag_col] = out[flag_col].fillna(0).astype("Int64")

        if table_name == "customers" and "zip" in out.columns:
            # zip là key join quan trọng, giữ numeric nullable thay vì ép mean.
            out["zip"] = out["zip"].astype("Int64")

        if table_name == "geography" and "zip" in out.columns:
            out["zip"] = out["zip"].astype("Int64")

        return out

    def _sort_for_stability(self, table_name: str, df: pd.DataFrame) -> pd.DataFrame:
        """Sắp xếp nhẹ để kết quả export ổn định giữa các lần chạy."""
        out = df.copy()
        sort_keys: Dict[str, List[str]] = {
            "products": ["product_id"],
            "customers": ["customer_id"],
            "promotions": ["promo_id"],
            "geography": ["zip"],
            "orders": ["order_date", "order_id"],
            "order_items": ["order_id", "product_id"],
            "payments": ["order_id", "payment_method"],
            "shipments": ["ship_date", "order_id"],
            "returns": ["return_date", "return_id"],
            "reviews": ["review_date", "review_id"],
            "sales": ["Date"],
            "inventory": ["snapshot_date", "product_id"],
            "web_traffic": ["date", "traffic_source"],
        }
        keys = [col for col in sort_keys.get(table_name, []) if col in out.columns]
        if keys:
            out = out.sort_values(keys, kind="stable").reset_index(drop=True)
        return out

    def _build_table_report(
        self,
        table_name: str,
        raw_df: pd.DataFrame,
        processed_df: pd.DataFrame,
    ) -> Dict[str, Any]:
        """Tạo báo cáo chất lượng cho từng bảng."""
        raw_missing = raw_df.isna().sum()
        processed_missing = processed_df.isna().sum()
        report: Dict[str, Any] = {
            "table_name": table_name,
            "raw_rows": int(len(raw_df)),
            "processed_rows": int(len(processed_df)),
            "rows_removed": int(len(raw_df) - len(processed_df)),
            "raw_columns": int(raw_df.shape[1]),
            "processed_columns": int(processed_df.shape[1]),
            "raw_duplicate_rows": int(raw_df.duplicated().sum()),
            "processed_duplicate_rows": int(processed_df.duplicated().sum()),
            "raw_missing_cells": int(raw_missing.sum()),
            "processed_missing_cells": int(processed_missing.sum()),
            "top_raw_missing": {k: int(v) for k, v in raw_missing[raw_missing > 0].sort_values(ascending=False).head(5).to_dict().items()},
            "top_processed_missing": {k: int(v) for k, v in processed_missing[processed_missing > 0].sort_values(ascending=False).head(5).to_dict().items()},
        }
        return report

    def _process_single_table(self, table_name: str, df: pd.DataFrame) -> pd.DataFrame:
        """
        Xử lý một bảng đơn lẻ theo hướng tương thích với Transform:
        1. Chuẩn hóa text
        2. Ép numeric đúng cột
        3. Ép datetime đúng cột
        4. Loại duplicate toàn dòng
        5. Đưa giá trị bất hợp lý về NaN
        6. Fill missing chọn lọc theo nghiệp vụ
        7. Sắp xếp ổn định
        """
        out = df.copy()
        original_columns = out.columns.tolist()

        out = self._standardize_text_columns(out)
        out = self._coerce_numeric_columns(table_name, out)
        out = self._process_datetime(out)
        out = self._drop_full_duplicates(out)
        out = self._clip_or_nullify_invalid_numeric(table_name, out)
        out = self._impute_business_missing(table_name, out)
        out = self._sort_for_stability(table_name, out)

        # Bảo toàn đúng thứ tự cột gốc để Transform đọc ổn định.
        return out[original_columns]

    # =========================
    # PUBLIC API
    # =========================
    def load_data(self) -> None:
        """Load toàn bộ dữ liệu raw từ thư mục nguồn."""
        self.raw_data = self._load_raw_data()

    def process_all_data(self) -> None:
        """Xử lý toàn bộ dữ liệu đã load."""
        if not self.raw_data:
            raise ValueError("raw_data đang rỗng. Hãy gọi load_data() trước.")

        processed: Dict[str, pd.DataFrame] = {}
        reports: Dict[str, Dict[str, Any]] = {}

        for table_name, df in self.raw_data.items():
            print(f"Processing {table_name}...")
            cleaned_df = self._process_single_table(table_name, df)
            processed[table_name] = cleaned_df
            reports[table_name] = self._build_table_report(table_name, df, cleaned_df)

        self.processed_data = processed
        self.quality_report = reports

    def export_processed_data(self, output_dir: str | Path = "processed_data") -> None:
        """
        Export dữ liệu đã xử lý ra thư mục mới.

        Tên file được giữ nguyên để Transform.get_path(output_dir) có thể dùng trực tiếp.
        """
        if not self.processed_data:
            raise ValueError("processed_data đang rỗng. Hãy gọi process_all_data() trước.")

        output_path = Path(output_dir)
        output_path.mkdir(parents=True, exist_ok=True)

        for table_name, df in self.processed_data.items():
            df.to_csv(output_path / f"{table_name}.csv", index=False)

        report_rows = []
        for table_name, report in self.quality_report.items():
            report_rows.append({
                "table_name": table_name,
                "raw_rows": report["raw_rows"],
                "processed_rows": report["processed_rows"],
                "rows_removed": report["rows_removed"],
                "raw_duplicate_rows": report["raw_duplicate_rows"],
                "processed_duplicate_rows": report["processed_duplicate_rows"],
                "raw_missing_cells": report["raw_missing_cells"],
                "processed_missing_cells": report["processed_missing_cells"],
            })
        pd.DataFrame(report_rows).sort_values("table_name").to_csv(output_path / "quality_report.csv", index=False)

    def get_processed_table(self, table_name: str) -> pd.DataFrame:
        """Lấy một bảng đã xử lý theo tên."""
        if table_name not in self.processed_data:
            raise KeyError(f"Không tìm thấy bảng '{table_name}' trong processed_data.")
        return self.processed_data[table_name]

    def run(self, output_dir: str | Path | None = None) -> None:
        """
        Chạy toàn bộ pipeline process.

        Parameters
        ----------
        output_dir : str | Path | None
            Nếu truyền vào, dữ liệu processed sẽ được export ra thư mục này.
        """
        self.load_data()
        self.process_all_data()

        if output_dir is not None:
            self.export_processed_data(output_dir=output_dir)

        print("\n=== PROCESS SUMMARY ===")
        for table_name in sorted(self.quality_report):
            rep = self.quality_report[table_name]
            print(
                f"{table_name}: raw={rep['raw_rows']}, processed={rep['processed_rows']}, "
                f"removed={rep['rows_removed']}, raw_missing={rep['raw_missing_cells']}, "
                f"processed_missing={rep['processed_missing_cells']}"
            )


if __name__ == "__main__":
    processor = DataProcessor(data_dir="data")
    processor.run(output_dir="output/processed_data")
