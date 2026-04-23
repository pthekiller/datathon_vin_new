from __future__ import annotations

from pathlib import Path
from typing import Dict, List, Optional

import numpy as np
import pandas as pd

def _resolve_existing_dir(dir_name: str | Path) -> Path:
    """Tìm thư mục dữ liệu theo cwd, thư mục script, rồi project root."""
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


class Transform:
    """
    Lớp xây dựng bộ 24 bảng Data Warehouse / Data Mart cho bài toán doanh nghiệp.

    Mục tiêu thiết kế:
    - Đọc dữ liệu từ thư mục raw/processed với đường dẫn tương tự file Processing.
    - Xây 24 bảng phục vụ EDA, trực quan hoá Power BI, rút insight bằng pandas.
    - Giữ riêng hai nhánh độc lập:
        + sales: dùng cho forecast / feature engineering, không nối vào khung giao dịch chính.
        + web_traffic: dùng để đánh giá web funnel riêng, chỉ nối dim_date và dim_traffic_source.
    - Ưu tiên tính ổn định pipeline ETL hơn là mô hình quá phức tạp.

    Cấu trúc bảng đầu ra:
    - 7 dimension tables
    - 8 fact tables
    - 9 mart tables
    = 24 bảng tổng cộng

    Lưu ý nghiệp vụ quan trọng:
    - Doanh thu, COGS, lợi nhuận của khung giao dịch chính được xây từ orders + order_items + products + payments.
    - sales là bảng time-series độc lập để train mô hình dự báo.
    - web_traffic là bảng phân tích website độc lập, không ép relationship với orders.
    """

    __slots__ = (
        "_raw_path",
        "_table",
        "_build_notes",
        # raw sources
        "_products",
        "_customers",
        "_promotions",
        "_geography",
        "_orders",
        "_order_items",
        "_payments",
        "_shipments",
        "_returns",
        "_reviews",
        "_sales",
        "_inventory",
        "_web_traffic",
        # dimensions
        "_dim_date",
        "_dim_product",
        "_dim_customer",
        "_dim_geography",
        "_dim_promotion",
        "_dim_return_reason",
        "_dim_traffic_source",
        # facts
        "_fact_sales_item",
        "_fact_orders",
        "_fact_returns",
        "_fact_shipments",
        "_fact_reviews",
        "_fact_inventory_snapshot",
        "_fact_sales_series",
        "_fact_web_traffic",
        "_sales_reconciliation",
        # marts
        "_mart_sales_overview",
        "_mart_customer_insight",
        "_mart_product_performance",
        "_mart_operations",
        "_mart_promotion_effectiveness",
        "_mart_return_diagnostics",
        "_mart_shipping_service",
        "_mart_inventory_risk",
        "_mart_sales_forecast_ready",
    )

    def __init__(self, raw_path: Dict[str, str]) -> None:
        self.raw_path = raw_path
        self.table = {}
        self.build_notes = []

        # raw sources
        self._products = pd.DataFrame()
        self._customers = pd.DataFrame()
        self._promotions = pd.DataFrame()
        self._geography = pd.DataFrame()
        self._orders = pd.DataFrame()
        self._order_items = pd.DataFrame()
        self._payments = pd.DataFrame()
        self._shipments = pd.DataFrame()
        self._returns = pd.DataFrame()
        self._reviews = pd.DataFrame()
        self._sales = pd.DataFrame()
        self._inventory = pd.DataFrame()
        self._web_traffic = pd.DataFrame()

        # outputs
        self._dim_date = pd.DataFrame()
        self._dim_product = pd.DataFrame()
        self._dim_customer = pd.DataFrame()
        self._dim_geography = pd.DataFrame()
        self._dim_promotion = pd.DataFrame()
        self._dim_return_reason = pd.DataFrame()
        self._dim_traffic_source = pd.DataFrame()

        self._fact_sales_item = pd.DataFrame()
        self._fact_orders = pd.DataFrame()
        self._fact_returns = pd.DataFrame()
        self._fact_shipments = pd.DataFrame()
        self._fact_reviews = pd.DataFrame()
        self._fact_inventory_snapshot = pd.DataFrame()
        self._fact_sales_series = pd.DataFrame()
        self._fact_web_traffic = pd.DataFrame()
        self._sales_reconciliation = pd.DataFrame()

        self._mart_sales_overview = pd.DataFrame()
        self._mart_customer_insight = pd.DataFrame()
        self._mart_product_performance = pd.DataFrame()
        self._mart_operations = pd.DataFrame()
        self._mart_promotion_effectiveness = pd.DataFrame()
        self._mart_return_diagnostics = pd.DataFrame()
        self._mart_shipping_service = pd.DataFrame()
        self._mart_inventory_risk = pd.DataFrame()
        self._mart_sales_forecast_ready = pd.DataFrame()

    @property
    def raw_path(self) -> Dict[str, str]:
        """Dictionary chứa đường dẫn dữ liệu nguồn."""
        return self._raw_path

    @raw_path.setter
    def raw_path(self, value: Dict[str, str]) -> None:
        self._raw_path = value

    @property
    def table(self) -> Dict[str, pd.DataFrame]:
        """Dictionary chứa toàn bộ bảng đầu ra của tầng transform."""
        return self._table

    @table.setter
    def table(self, value: Dict[str, pd.DataFrame]) -> None:
        self._table = value

    @property
    def build_notes(self) -> List[str]:
        """Danh sách ghi chú về các join/logic bị giới hạn bởi raw data."""
        return self._build_notes

    @build_notes.setter
    def build_notes(self, value: List[str]) -> None:
        self._build_notes = value

    @staticmethod
    def get_path(root_dir: str = "data") -> Dict[str, str]:
        """
        Trả về dictionary đường dẫn các file nguồn.

        Parameters
        ----------
        root_dir : str
            Thư mục chứa các file csv nguồn hoặc dữ liệu đã xử lý.
        """
        root = _resolve_existing_dir(root_dir)
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
    # Internal helpers
    # =========================
    def _log(self, message: str) -> None:
        """Lưu ghi chú build để export cùng pipeline."""
        self.build_notes.append(message)

    @staticmethod
    def _empty_df(columns: List[str]) -> pd.DataFrame:
        """Tạo DataFrame rỗng có schema xác định."""
        return pd.DataFrame(columns=columns)

    @staticmethod
    def _to_datetime(df: pd.DataFrame, columns: List[str]) -> pd.DataFrame:
        """Chuẩn hoá các cột ngày tháng sang datetime."""
        for col in columns:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], errors="coerce")
        return df

    @staticmethod
    def _to_numeric(df: pd.DataFrame, columns: List[str]) -> pd.DataFrame:
        """Chuẩn hoá numeric columns sang kiểu số."""
        for col in columns:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce")
        return df

    @staticmethod
    def _safe_divide(numerator: pd.Series, denominator: pd.Series) -> pd.Series:
        """Chia an toàn, trả về NaN khi mẫu số bằng 0."""
        return np.where(denominator.fillna(0) != 0, numerator / denominator, np.nan)

    @staticmethod
    def _month_key_from_date(series: pd.Series) -> pd.Series:
        """Sinh month_key dạng YYYYMM từ cột datetime."""
        s = pd.to_datetime(series, errors="coerce")
        return (s.dt.year * 100 + s.dt.month).astype("Int64")

    @staticmethod
    def _date_key_from_date(series: pd.Series) -> pd.Series:
        """Sinh date_key dạng YYYYMMDD từ cột datetime."""
        s = pd.to_datetime(series, errors="coerce")
        return (s.dt.year * 10000 + s.dt.month * 100 + s.dt.day).astype("Int64")

    def _read_csv(self, name: str) -> pd.DataFrame:
        """Đọc một file csv từ path đã cấu hình."""
        path = Path(self.raw_path[name])
        if not path.exists():
            self._log(f"Thiếu file nguồn: {name} -> {path}")
            return pd.DataFrame()
        return pd.read_csv(path, low_memory=False)

    def _prepare_source_data(self) -> None:
        """Đọc và chuẩn hoá toàn bộ dữ liệu nguồn."""
        self._products = self._read_csv("products")
        self._customers = self._read_csv("customers")
        self._promotions = self._read_csv("promotions")
        self._geography = self._read_csv("geography")
        self._orders = self._read_csv("orders")
        self._order_items = self._read_csv("order_items")
        self._payments = self._read_csv("payments")
        self._shipments = self._read_csv("shipments")
        self._returns = self._read_csv("returns")
        self._reviews = self._read_csv("reviews")
        self._sales = self._read_csv("sales")
        self._inventory = self._read_csv("inventory")
        self._web_traffic = self._read_csv("web_traffic")

        self._products = self._to_numeric(self._products, ["product_id", "price", "cogs"])

        self._customers = self._to_datetime(self._customers, ["signup_date"])
        self._customers = self._to_numeric(self._customers, ["customer_id", "zip"])

        self._promotions = self._to_datetime(self._promotions, ["start_date", "end_date"])
        self._promotions = self._to_numeric(self._promotions, ["discount_value", "stackable_flag", "min_order_value"])

        self._geography = self._to_numeric(self._geography, ["zip"])

        self._orders = self._to_datetime(self._orders, ["order_date"])
        self._orders = self._to_numeric(self._orders, ["order_id", "customer_id", "zip"])

        self._order_items = self._to_numeric(
            self._order_items,
            ["order_id", "product_id", "quantity", "unit_price", "discount_amount"],
        )

        self._payments = self._to_numeric(self._payments, ["order_id", "payment_value", "installments"])

        self._shipments = self._to_datetime(self._shipments, ["ship_date", "delivery_date"])
        self._shipments = self._to_numeric(self._shipments, ["order_id", "shipping_fee"])

        self._returns = self._to_datetime(self._returns, ["return_date"])
        self._returns = self._to_numeric(self._returns, ["order_id", "product_id", "return_quantity", "refund_amount"])

        self._reviews = self._to_datetime(self._reviews, ["review_date"])
        self._reviews = self._to_numeric(self._reviews, ["order_id", "product_id", "customer_id", "rating"])

        if "Date" in self._sales.columns:
            self._sales["Date"] = pd.to_datetime(self._sales["Date"], errors="coerce")
        self._sales = self._to_numeric(self._sales, ["Revenue", "COGS"])

        self._inventory = self._to_datetime(self._inventory, ["snapshot_date"])
        self._inventory = self._to_numeric(
            self._inventory,
            [
                "product_id", "stock_on_hand", "units_received", "units_sold", "stockout_days",
                "days_of_supply", "fill_rate", "stockout_flag", "overstock_flag", "reorder_flag",
                "sell_through_rate", "year", "month"
            ],
        )

        self._web_traffic = self._to_datetime(self._web_traffic, ["date"])
        self._web_traffic = self._to_numeric(
            self._web_traffic,
            ["sessions", "unique_visitors", "page_views", "bounce_rate", "avg_session_duration_sec"],
        )

    # =========================
    # Build dimensions
    # =========================
    def _build_dim_date(self) -> pd.DataFrame:
        """Dimension thời gian dùng chung cho toàn bộ warehouse."""
        date_series = []
        for df, cols in [
            (self._orders, ["order_date"]),
            (self._shipments, ["ship_date", "delivery_date"]),
            (self._returns, ["return_date"]),
            (self._reviews, ["review_date"]),
            (self._sales, ["Date"]),
            (self._inventory, ["snapshot_date"]),
            (self._web_traffic, ["date"]),
            (self._customers, ["signup_date"]),
            (self._promotions, ["start_date", "end_date"]),
        ]:
            for col in cols:
                if col in df.columns:
                    date_series.append(pd.to_datetime(df[col], errors="coerce"))

        if not date_series:
            return self._empty_df([
                "date_key", "full_date", "day", "month", "quarter", "year", "week_of_year", "day_of_week", "is_weekend"
            ])

        all_dates = pd.concat(date_series, ignore_index=True).dropna().drop_duplicates().sort_values()
        out = pd.DataFrame({"full_date": all_dates})
        out["date_key"] = self._date_key_from_date(out["full_date"])
        out["day"] = out["full_date"].dt.day
        out["month"] = out["full_date"].dt.month
        out["quarter"] = out["full_date"].dt.quarter
        out["year"] = out["full_date"].dt.year
        out["week_of_year"] = out["full_date"].dt.isocalendar().week.astype(int)
        out["day_of_week"] = out["full_date"].dt.dayofweek + 1
        out["is_weekend"] = out["full_date"].dt.dayofweek >= 5
        return out.sort_values("full_date").reset_index(drop=True)

    def _build_dim_product(self) -> pd.DataFrame:
        """Dimension sản phẩm."""
        if self._products.empty:
            return self._empty_df(["product_id", "product_name", "category", "segment", "size", "color", "price", "cogs"])
        out = self._products[["product_id", "product_name", "category", "segment", "size", "color", "price", "cogs"]].drop_duplicates("product_id")
        return out.sort_values("product_id").reset_index(drop=True)

    def _build_dim_customer(self) -> pd.DataFrame:
        """Dimension khách hàng."""
        if self._customers.empty:
            return self._empty_df(["customer_id", "zip", "city", "signup_date", "gender", "age_group", "acquisition_channel"])
        out = self._customers[["customer_id", "zip", "city", "signup_date", "gender", "age_group", "acquisition_channel"]].drop_duplicates("customer_id")
        return out.sort_values("customer_id").reset_index(drop=True)

    def _build_dim_geography(self) -> pd.DataFrame:
        """Dimension địa lý theo zip."""
        if self._geography.empty:
            return self._empty_df(["zip", "city", "region", "district"])
        out = self._geography[["zip", "city", "region", "district"]].drop_duplicates("zip")
        return out.sort_values("zip").reset_index(drop=True)

    def _build_dim_promotion(self) -> pd.DataFrame:
        """Dimension khuyến mãi."""
        if self._promotions.empty:
            return self._empty_df([
                "promo_id", "promo_name", "promo_type", "discount_value", "start_date", "end_date",
                "applicable_category", "promo_channel", "stackable_flag", "min_order_value"
            ])
        cols = [
            "promo_id", "promo_name", "promo_type", "discount_value", "start_date", "end_date",
            "applicable_category", "promo_channel", "stackable_flag", "min_order_value"
        ]
        out = self._promotions[cols].drop_duplicates("promo_id")
        return out.sort_values("promo_id").reset_index(drop=True)

    def _build_dim_return_reason(self) -> pd.DataFrame:
        """Dimension lý do hoàn trả, chuẩn hoá từ bảng returns."""
        if self._returns.empty or "return_reason" not in self._returns.columns:
            return self._empty_df(["return_reason_key", "return_reason"])
        reasons = self._returns[["return_reason"]].dropna().drop_duplicates().sort_values("return_reason").reset_index(drop=True)
        reasons["return_reason_key"] = np.arange(1, len(reasons) + 1)
        return reasons[["return_reason_key", "return_reason"]]

    def _build_dim_traffic_source(self) -> pd.DataFrame:
        """Dimension nguồn traffic cho nhánh web riêng."""
        if self._web_traffic.empty or "traffic_source" not in self._web_traffic.columns:
            return self._empty_df(["traffic_source_key", "traffic_source"])
        src = self._web_traffic[["traffic_source"]].dropna().drop_duplicates().sort_values("traffic_source").reset_index(drop=True)
        src["traffic_source_key"] = np.arange(1, len(src) + 1)
        return src[["traffic_source_key", "traffic_source"]]

    # =========================
    # Build facts
    # =========================
    def _build_fact_sales_item(self) -> pd.DataFrame:
        """Fact lõi: 1 dòng = 1 item trong 1 order."""
        if self._order_items.empty or self._orders.empty:
            return self._empty_df([
                "order_id", "product_id", "customer_id", "zip", "order_date", "date_key", "month_key", "year", "month", "quarter",
                "order_status", "payment_method", "device_type", "order_source", "quantity", "unit_price", "discount_amount",
                "promo_id", "promo_id_2", "product_name", "category", "segment", "size", "color", "list_price", "cogs",
                "payment_value", "gross_sales", "net_sales", "line_cogs", "line_profit", "region", "city", "district"
            ])

        orders = self._orders[["order_id", "order_date", "customer_id", "zip", "order_status", "payment_method", "device_type", "order_source"]]
        items = self._order_items[["order_id", "product_id", "quantity", "unit_price", "discount_amount", "promo_id", "promo_id_2"]]
        products = self._products[["product_id", "product_name", "category", "segment", "size", "color", "price", "cogs"]] if not self._products.empty else self._empty_df(["product_id"])
        payments = self._payments[["order_id", "payment_value"]] if not self._payments.empty else self._empty_df(["order_id", "payment_value"])
        geo = self._geography[["zip", "city", "region", "district"]] if not self._geography.empty else self._empty_df(["zip"])

        base = items.merge(orders, on="order_id", how="left")
        base = base.merge(products, on="product_id", how="left")
        base = base.merge(payments, on="order_id", how="left")
        base = base.merge(geo, on="zip", how="left", suffixes=("", "_geo"))

        base["date_key"] = self._date_key_from_date(base["order_date"])
        base["month_key"] = self._month_key_from_date(base["order_date"])
        base["year"] = pd.to_datetime(base["order_date"], errors="coerce").dt.year
        base["month"] = pd.to_datetime(base["order_date"], errors="coerce").dt.month
        base["quarter"] = pd.to_datetime(base["order_date"], errors="coerce").dt.quarter

        base["gross_sales"] = base["quantity"].fillna(0) * base["unit_price"].fillna(0)
        base["net_sales"] = base["gross_sales"] - base["discount_amount"].fillna(0)
        base["line_cogs"] = base["quantity"].fillna(0) * base["cogs"].fillna(0)
        base["line_profit"] = base["net_sales"] - base["line_cogs"]
        base = base.rename(columns={"price": "list_price"})

        return base[[
            "order_id", "product_id", "customer_id", "zip", "order_date", "date_key", "month_key", "year", "month", "quarter",
            "order_status", "payment_method", "device_type", "order_source", "quantity", "unit_price", "discount_amount",
            "promo_id", "promo_id_2", "product_name", "category", "segment", "size", "color", "list_price", "cogs",
            "payment_value", "gross_sales", "net_sales", "line_cogs", "line_profit", "region", "city", "district"
        ]].sort_values(["order_date", "order_id"]).reset_index(drop=True)

    def _build_fact_orders(self) -> pd.DataFrame:
        """Fact đơn hàng: 1 dòng = 1 order."""
        base = self._fact_sales_item
        if base.empty:
            return self._empty_df([
                "order_id", "customer_id", "zip", "order_date", "date_key", "month_key", "year", "month", "quarter", "order_status",
                "payment_method", "device_type", "order_source", "region", "city", "district", "distinct_products", "total_quantity",
                "gross_sales", "discount_amount", "net_sales", "order_cogs", "order_profit", "payment_value"
            ])

        out = base.groupby("order_id", as_index=False, sort=False).agg(
            customer_id=("customer_id", "first"),
            zip=("zip", "first"),
            order_date=("order_date", "first"),
            date_key=("date_key", "first"),
            month_key=("month_key", "first"),
            year=("year", "first"),
            month=("month", "first"),
            quarter=("quarter", "first"),
            order_status=("order_status", "first"),
            payment_method=("payment_method", "first"),
            device_type=("device_type", "first"),
            order_source=("order_source", "first"),
            region=("region", "first"),
            city=("city", "first"),
            district=("district", "first"),
            distinct_products=("product_id", "nunique"),
            total_quantity=("quantity", "sum"),
            gross_sales=("gross_sales", "sum"),
            discount_amount=("discount_amount", "sum"),
            net_sales=("net_sales", "sum"),
            order_cogs=("line_cogs", "sum"),
            order_profit=("line_profit", "sum"),
            payment_value=("payment_value", "first"),
        )
        return out.sort_values(["order_date", "order_id"]).reset_index(drop=True)

    def _build_fact_returns(self) -> pd.DataFrame:
        """Fact hoàn trả: 1 dòng = 1 return record."""
        if self._returns.empty:
            return self._empty_df([
                "return_id", "order_id", "product_id", "return_date", "return_date_key", "return_month_key", "return_year", "return_month", "return_quarter",
                "return_reason_key", "return_reason", "return_quantity", "refund_amount", "category", "segment", "product_name", "region", "city"
            ])
        ret = self._returns.copy()
        if not self.dim_return_reason.empty:
            ret = ret.merge(self.dim_return_reason, on="return_reason", how="left")
        if not self.dim_product.empty:
            ret = ret.merge(self.dim_product[["product_id", "product_name", "category", "segment"]], on="product_id", how="left")
        if not self._orders.empty:
            ret = ret.merge(self._orders[["order_id", "zip"]], on="order_id", how="left")
        if not self.dim_geography.empty and "zip" in ret.columns:
            ret = ret.merge(self.dim_geography[["zip", "city", "region"]], on="zip", how="left")
        ret["return_date_key"] = self._date_key_from_date(ret["return_date"])
        ret["return_month_key"] = self._month_key_from_date(ret["return_date"])
        ret["return_year"] = ret["return_date"].dt.year
        ret["return_month"] = ret["return_date"].dt.month
        ret["return_quarter"] = ret["return_date"].dt.quarter
        cols = [
            "return_id", "order_id", "product_id", "return_date", "return_date_key", "return_month_key", "return_year", "return_month", "return_quarter",
            "return_reason_key", "return_reason", "return_quantity", "refund_amount", "category", "segment", "product_name", "region", "city"
        ]
        return ret[cols].sort_values(["return_date", "return_id"]).reset_index(drop=True)

    def _build_fact_shipments(self) -> pd.DataFrame:
        """Fact giao hàng: 1 dòng = 1 shipment record / order."""
        if self._shipments.empty:
            return self._empty_df([
                "order_id", "ship_date", "delivery_date", "ship_date_key", "delivery_date_key", "ship_month_key", "delivery_month_key",
                "shipping_fee", "delivery_days", "region", "city", "order_source", "device_type", "order_status"
            ])
        shp = self._shipments.copy()
        if not self._orders.empty:
            shp = shp.merge(self._orders[["order_id", "zip", "order_source", "device_type", "order_status"]], on="order_id", how="left")
        if not self.dim_geography.empty and "zip" in shp.columns:
            shp = shp.merge(self.dim_geography[["zip", "city", "region"]], on="zip", how="left")
        shp["ship_date_key"] = self._date_key_from_date(shp["ship_date"])
        shp["delivery_date_key"] = self._date_key_from_date(shp["delivery_date"])
        shp["ship_month_key"] = self._month_key_from_date(shp["ship_date"])
        shp["delivery_month_key"] = self._month_key_from_date(shp["delivery_date"])
        shp["delivery_days"] = (shp["delivery_date"] - shp["ship_date"]).dt.days
        cols = [
            "order_id", "ship_date", "delivery_date", "ship_date_key", "delivery_date_key", "ship_month_key", "delivery_month_key",
            "shipping_fee", "delivery_days", "region", "city", "order_source", "device_type", "order_status"
        ]
        return shp[cols].sort_values(["ship_date", "order_id"]).reset_index(drop=True)

    def _build_fact_reviews(self) -> pd.DataFrame:
        """Fact review: 1 dòng = 1 review."""
        if self._reviews.empty:
            return self._empty_df([
                "review_id", "order_id", "product_id", "customer_id", "review_date", "review_date_key", "review_month_key",
                "rating", "review_title", "category", "segment", "product_name", "region", "city"
            ])
        rev = self._reviews.copy()
        if not self.dim_product.empty:
            rev = rev.merge(self.dim_product[["product_id", "product_name", "category", "segment"]], on="product_id", how="left")
        if not self._orders.empty:
            rev = rev.merge(self._orders[["order_id", "zip"]], on="order_id", how="left")
        if not self.dim_geography.empty and "zip" in rev.columns:
            rev = rev.merge(self.dim_geography[["zip", "city", "region"]], on="zip", how="left")
        rev["review_date_key"] = self._date_key_from_date(rev["review_date"])
        rev["review_month_key"] = self._month_key_from_date(rev["review_date"])
        cols = [
            "review_id", "order_id", "product_id", "customer_id", "review_date", "review_date_key", "review_month_key",
            "rating", "review_title", "category", "segment", "product_name", "region", "city"
        ]
        return rev[cols].sort_values(["review_date", "review_id"]).reset_index(drop=True)

    def _build_fact_inventory_snapshot(self) -> pd.DataFrame:
        """Fact snapshot tồn kho: 1 dòng = 1 product × 1 snapshot_date."""
        if self._inventory.empty:
            return self._empty_df([
                "snapshot_date", "snapshot_date_key", "month_key", "year", "month", "quarter", "product_id", "product_name", "category", "segment",
                "stock_on_hand", "units_received", "units_sold", "stockout_days", "days_of_supply", "fill_rate", "stockout_flag",
                "overstock_flag", "reorder_flag", "sell_through_rate"
            ])
        inv = self._inventory.copy()
        inv["snapshot_date_key"] = self._date_key_from_date(inv["snapshot_date"])
        inv["month_key"] = self._month_key_from_date(inv["snapshot_date"])
        inv["quarter"] = pd.to_datetime(inv["snapshot_date"], errors="coerce").dt.quarter
        cols = [
            "snapshot_date", "snapshot_date_key", "month_key", "year", "month", "quarter", "product_id", "product_name", "category", "segment",
            "stock_on_hand", "units_received", "units_sold", "stockout_days", "days_of_supply", "fill_rate", "stockout_flag",
            "overstock_flag", "reorder_flag", "sell_through_rate"
        ]
        return inv[cols].sort_values(["snapshot_date", "product_id"]).reset_index(drop=True)

    def _build_fact_sales_series(self) -> pd.DataFrame:
        """Fact chuỗi thời gian sales độc lập cho forecast."""
        if self._sales.empty:
            return self._empty_df(["date", "date_key", "month_key", "year", "month", "quarter", "revenue", "cogs", "profit"])
        sales = self._sales.rename(columns={"Date": "date", "Revenue": "revenue", "COGS": "cogs"}).copy()
        sales["date_key"] = self._date_key_from_date(sales["date"])
        sales["month_key"] = self._month_key_from_date(sales["date"])
        sales["year"] = sales["date"].dt.year
        sales["month"] = sales["date"].dt.month
        sales["quarter"] = sales["date"].dt.quarter
        sales["profit"] = sales["revenue"].fillna(0) - sales["cogs"].fillna(0)
        return sales[["date", "date_key", "month_key", "year", "month", "quarter", "revenue", "cogs", "profit"]].sort_values("date").reset_index(drop=True)

    def _build_fact_web_traffic(self) -> pd.DataFrame:
        """Fact web traffic độc lập, không nối với orders."""
        if self._web_traffic.empty:
            return self._empty_df([
                "date", "date_key", "month_key", "year", "month", "quarter", "traffic_source_key", "traffic_source",
                "sessions", "unique_visitors", "page_views", "bounce_rate", "avg_session_duration_sec"
            ])
        web = self._web_traffic.copy()
        if not self.dim_traffic_source.empty:
            web = web.merge(self.dim_traffic_source, on="traffic_source", how="left")
        web["date_key"] = self._date_key_from_date(web["date"])
        web["month_key"] = self._month_key_from_date(web["date"])
        web["year"] = web["date"].dt.year
        web["month"] = web["date"].dt.month
        web["quarter"] = web["date"].dt.quarter
        cols = [
            "date", "date_key", "month_key", "year", "month", "quarter", "traffic_source_key", "traffic_source",
            "sessions", "unique_visitors", "page_views", "bounce_rate", "avg_session_duration_sec"
        ]
        return web[cols].sort_values(["date", "traffic_source"]).reset_index(drop=True)

    def _build_sales_reconciliation(self) -> pd.DataFrame:
        """
        Bảng đối chiếu sales.csv với doanh thu/COGS dựng lại từ transaction theo ngày.

        Đây là bảng validation riêng, không tính vào 24 bảng chính.
        Không mặc định analytical sales bằng transaction aggregate.
        """
        cols = [
            "date", "sales_revenue", "sales_cogs", "sales_profit",
            "reconstructed_gross_sales", "reconstructed_discount_amount", "reconstructed_net_sales",
            "reconstructed_cogs", "reconstructed_profit",
            "revenue_gap_vs_sales", "cogs_gap_vs_sales", "profit_gap_vs_sales",
            "order_count", "item_count"
        ]

        sales_daily = self._empty_df(["date", "sales_revenue", "sales_cogs", "sales_profit"])
        tx_daily = self._empty_df([
            "date", "reconstructed_gross_sales", "reconstructed_discount_amount", "reconstructed_net_sales",
            "reconstructed_cogs", "reconstructed_profit", "order_count", "item_count"
        ])

        if not self._fact_sales_series.empty:
            sales_daily = self._fact_sales_series.groupby("date", as_index=False, sort=False).agg(
                sales_revenue=("revenue", "sum"),
                sales_cogs=("cogs", "sum"),
                sales_profit=("profit", "sum"),
            )

        if not self._fact_sales_item.empty:
            tx_daily = self._fact_sales_item.groupby("order_date", as_index=False, sort=False).agg(
                reconstructed_gross_sales=("gross_sales", "sum"),
                reconstructed_discount_amount=("discount_amount", "sum"),
                reconstructed_net_sales=("net_sales", "sum"),
                reconstructed_cogs=("line_cogs", "sum"),
                reconstructed_profit=("line_profit", "sum"),
                order_count=("order_id", "nunique"),
                item_count=("product_id", "size"),
            ).rename(columns={"order_date": "date"})

        recon = sales_daily.merge(tx_daily, on="date", how="outer").sort_values("date").reset_index(drop=True)
        recon["revenue_gap_vs_sales"] = recon["reconstructed_net_sales"] - recon["sales_revenue"]
        recon["cogs_gap_vs_sales"] = recon["reconstructed_cogs"] - recon["sales_cogs"]
        recon["profit_gap_vs_sales"] = recon["reconstructed_profit"] - recon["sales_profit"]
        return recon[cols]

    # =========================
    # Build marts
    # =========================
    def _build_mart_sales_overview(self) -> pd.DataFrame:
        """Mart tổng quan doanh số theo tháng × vùng × ngành hàng."""
        base = self._fact_sales_item.loc[:, [
            "month_key", "year", "month", "quarter", "region", "city", "category", "segment",
            "gross_sales", "discount_amount", "net_sales", "line_cogs", "line_profit", "quantity", "order_id"
        ]]
        if base.empty:
            return self._empty_df([
                "month_key", "year", "month", "quarter", "region", "city", "category", "segment",
                "orders", "units_sold", "gross_sales", "discount_amount", "net_sales", "cogs", "profit", "aov"
            ])
        out = base.groupby(["month_key", "year", "month", "quarter", "region", "city", "category", "segment"], as_index=False, sort=False).agg(
            orders=("order_id", "nunique"),
            units_sold=("quantity", "sum"),
            gross_sales=("gross_sales", "sum"),
            discount_amount=("discount_amount", "sum"),
            net_sales=("net_sales", "sum"),
            cogs=("line_cogs", "sum"),
            profit=("line_profit", "sum"),
        )
        out["aov"] = self._safe_divide(out["net_sales"], out["orders"])
        return out.sort_values(["month_key", "region", "category"]).reset_index(drop=True)

    def _build_mart_customer_insight(self) -> pd.DataFrame:
        """Mart khách hàng theo tháng × phân khúc khách hàng."""
        base = self._fact_orders.loc[:, ["month_key", "year", "month", "quarter", "customer_id", "region", "order_id", "net_sales"]]
        cust = self.dim_customer.loc[:, ["customer_id", "signup_date", "gender", "age_group", "acquisition_channel"]] if not self.dim_customer.empty else self._empty_df(["customer_id"])
        if base.empty:
            return self._empty_df([
                "month_key", "year", "month", "quarter", "age_group", "gender", "region", "acquisition_channel",
                "active_customers", "new_customers", "orders", "revenue", "revenue_per_customer", "orders_per_customer", "aov"
            ])
        cm = base.groupby(["month_key", "year", "month", "quarter", "customer_id", "region"], as_index=False, sort=False).agg(
            orders=("order_id", "nunique"),
            revenue=("net_sales", "sum"),
        )
        cm = cm.merge(cust, on="customer_id", how="left")
        current_period = pd.to_datetime(cm["month_key"].astype("Int64").astype(str) + "01", format="%Y%m%d", errors="coerce").dt.to_period("M")
        signup_period = pd.to_datetime(cm["signup_date"], errors="coerce").dt.to_period("M")
        cm["new_customer_flag"] = (current_period == signup_period).fillna(False).astype(int)
        grp = ["month_key", "year", "month", "quarter", "age_group", "gender", "region", "acquisition_channel"]
        out = cm.groupby(grp, as_index=False, sort=False).agg(
            active_customers=("customer_id", "nunique"),
            new_customers=("new_customer_flag", "sum"),
            orders=("orders", "sum"),
            revenue=("revenue", "sum"),
        )
        out["revenue_per_customer"] = self._safe_divide(out["revenue"], out["active_customers"])
        out["orders_per_customer"] = self._safe_divide(out["orders"], out["active_customers"])
        out["aov"] = self._safe_divide(out["revenue"], out["orders"])
        return out.sort_values(["month_key", "region"]).reset_index(drop=True)

    def _build_mart_product_performance(self) -> pd.DataFrame:
        """Mart hiệu quả sản phẩm theo tháng × sản phẩm × vùng."""
        sales = self._fact_sales_item.loc[:, [
            "month_key", "year", "month", "quarter", "product_id", "product_name", "category", "segment", "region",
            "quantity", "net_sales", "line_profit", "line_cogs", "discount_amount", "order_id"
        ]]
        if sales.empty:
            return self._empty_df([
                "month_key", "year", "month", "quarter", "product_id", "product_name", "category", "segment", "region",
                "orders", "units_sold", "revenue", "profit", "cogs", "discount_amount", "return_qty", "refund_amount", "avg_rating", "review_count"
            ])
        out = sales.groupby(["month_key", "year", "month", "quarter", "product_id", "product_name", "category", "segment", "region"], as_index=False, sort=False).agg(
            orders=("order_id", "nunique"),
            units_sold=("quantity", "sum"),
            revenue=("net_sales", "sum"),
            profit=("line_profit", "sum"),
            cogs=("line_cogs", "sum"),
            discount_amount=("discount_amount", "sum"),
        )
        if not self._fact_returns.empty:
            ret = self._fact_returns.groupby(["return_month_key", "product_id", "category", "segment", "region"], as_index=False, sort=False).agg(
                return_qty=("return_quantity", "sum"), refund_amount=("refund_amount", "sum")
            ).rename(columns={"return_month_key": "month_key"})
            out = out.merge(ret, on=["month_key", "product_id", "category", "segment", "region"], how="left")
        else:
            out["return_qty"] = np.nan
            out["refund_amount"] = np.nan
        if not self._fact_reviews.empty:
            rev = self._fact_reviews.groupby(["review_month_key", "product_id", "category", "segment", "region"], as_index=False, sort=False).agg(
                avg_rating=("rating", "mean"), review_count=("review_id", "nunique")
            ).rename(columns={"review_month_key": "month_key"})
            out = out.merge(rev, on=["month_key", "product_id", "category", "segment", "region"], how="left")
        else:
            out["avg_rating"] = np.nan
            out["review_count"] = np.nan
        return out.sort_values(["month_key", "product_id", "region"]).reset_index(drop=True)

    def _build_mart_operations(self) -> pd.DataFrame:
        """Mart vận hành theo tháng × vùng, tập trung giao hàng và hoàn trả."""
        orders = self._fact_orders.loc[:, ["month_key", "year", "month", "quarter", "region", "order_id", "net_sales"]]
        if orders.empty:
            return self._empty_df([
                "month_key", "year", "month", "quarter", "region", "orders", "revenue", "avg_delivery_days",
                "shipping_fee", "returns", "refund_amount", "avg_rating"
            ])
        out = orders.groupby(["month_key", "year", "month", "quarter", "region"], as_index=False, sort=False).agg(
            orders=("order_id", "nunique"), revenue=("net_sales", "sum")
        )
        if not self._fact_shipments.empty:
            shp = self._fact_shipments.groupby(["ship_month_key", "region"], as_index=False, sort=False).agg(
                avg_delivery_days=("delivery_days", "mean"), shipping_fee=("shipping_fee", "sum")
            ).rename(columns={"ship_month_key": "month_key"})
            out = out.merge(shp, on=["month_key", "region"], how="left")
        else:
            out["avg_delivery_days"] = np.nan
            out["shipping_fee"] = np.nan
        if not self._fact_returns.empty:
            ret = self._fact_returns.groupby(["return_month_key", "region"], as_index=False, sort=False).agg(
                returns=("return_id", "nunique"), refund_amount=("refund_amount", "sum")
            ).rename(columns={"return_month_key": "month_key"})
            out = out.merge(ret, on=["month_key", "region"], how="left")
        else:
            out["returns"] = np.nan
            out["refund_amount"] = np.nan
        if not self._fact_reviews.empty:
            rev = self._fact_reviews.groupby(["review_month_key", "region"], as_index=False, sort=False).agg(
                avg_rating=("rating", "mean")
            ).rename(columns={"review_month_key": "month_key"})
            out = out.merge(rev, on=["month_key", "region"], how="left")
        else:
            out["avg_rating"] = np.nan
        return out.sort_values(["month_key", "region"]).reset_index(drop=True)

    def _build_mart_promotion_effectiveness(self) -> pd.DataFrame:
        """Mart hiệu quả promotion theo tháng × promo × category."""
        base = self._fact_sales_item.loc[:, [
            "month_key", "year", "month", "quarter", "promo_id", "category", "segment", "order_id", "quantity", "discount_amount", "net_sales", "line_profit"
        ]]
        if base.empty:
            return self._empty_df([
                "month_key", "year", "month", "quarter", "promo_id", "promo_name", "promo_type", "category", "segment",
                "orders", "units_sold", "discount_amount", "revenue", "profit"
            ])
        out = base.groupby(["month_key", "year", "month", "quarter", "promo_id", "category", "segment"], as_index=False, sort=False).agg(
            orders=("order_id", "nunique"),
            units_sold=("quantity", "sum"),
            discount_amount=("discount_amount", "sum"),
            revenue=("net_sales", "sum"),
            profit=("line_profit", "sum"),
        )
        if not self.dim_promotion.empty:
            out = out.merge(self.dim_promotion[["promo_id", "promo_name", "promo_type"]], on="promo_id", how="left")
        else:
            out["promo_name"] = pd.NA
            out["promo_type"] = pd.NA
        return out.sort_values(["month_key", "promo_id", "category"]).reset_index(drop=True)

    def _build_mart_return_diagnostics(self) -> pd.DataFrame:
        """Mart chẩn đoán hoàn trả theo tháng × reason × category."""
        if self._fact_returns.empty:
            return self._empty_df([
                "month_key", "return_reason_key", "return_reason", "category", "segment", "region",
                "return_count", "return_qty", "refund_amount", "avg_rating"
            ])
        out = self._fact_returns.groupby(["return_month_key", "return_reason_key", "return_reason", "category", "segment", "region"], as_index=False, sort=False).agg(
            return_count=("return_id", "nunique"),
            return_qty=("return_quantity", "sum"),
            refund_amount=("refund_amount", "sum"),
        ).rename(columns={"return_month_key": "month_key"})
        if not self._fact_reviews.empty:
            rev = self._fact_reviews.groupby(["review_month_key", "category", "segment", "region"], as_index=False, sort=False).agg(
                avg_rating=("rating", "mean")
            ).rename(columns={"review_month_key": "month_key"})
            out = out.merge(rev, on=["month_key", "category", "segment", "region"], how="left")
        else:
            out["avg_rating"] = np.nan
        return out.sort_values(["month_key", "return_reason", "category"]).reset_index(drop=True)

    def _build_mart_shipping_service(self) -> pd.DataFrame:
        """Mart chất lượng giao hàng theo tháng × vùng × nguồn đơn."""
        if self._fact_shipments.empty:
            return self._empty_df([
                "month_key", "region", "order_source", "device_type", "shipments", "shipping_fee", "avg_delivery_days"
            ])
        out = self._fact_shipments.groupby(["ship_month_key", "region", "order_source", "device_type"], as_index=False, sort=False).agg(
            shipments=("order_id", "nunique"),
            shipping_fee=("shipping_fee", "sum"),
            avg_delivery_days=("delivery_days", "mean"),
        ).rename(columns={"ship_month_key": "month_key"})
        return out.sort_values(["month_key", "region", "order_source"]).reset_index(drop=True)

    def _build_mart_inventory_risk(self) -> pd.DataFrame:
        """Mart rủi ro tồn kho theo tháng × sản phẩm."""
        if self._fact_inventory_snapshot.empty:
            return self._empty_df([
                "month_key", "year", "month", "quarter", "product_id", "product_name", "category", "segment",
                "stock_on_hand", "units_received", "units_sold", "stockout_days", "days_of_supply", "fill_rate",
                "stockout_flag", "overstock_flag", "reorder_flag", "sell_through_rate"
            ])
        out = self._fact_inventory_snapshot.groupby([
            "month_key", "year", "month", "quarter", "product_id", "product_name", "category", "segment"
        ], as_index=False, sort=False).agg(
            stock_on_hand=("stock_on_hand", "last"),
            units_received=("units_received", "sum"),
            units_sold=("units_sold", "sum"),
            stockout_days=("stockout_days", "sum"),
            days_of_supply=("days_of_supply", "mean"),
            fill_rate=("fill_rate", "mean"),
            stockout_flag=("stockout_flag", "max"),
            overstock_flag=("overstock_flag", "max"),
            reorder_flag=("reorder_flag", "max"),
            sell_through_rate=("sell_through_rate", "mean"),
        )
        return out.sort_values(["month_key", "product_id"]).reset_index(drop=True)

    def _build_mart_sales_forecast_ready(self) -> pd.DataFrame:
        """Mart feature-ready cho forecast, build từ fact_sales_series độc lập."""
        base = self._fact_sales_series.copy()
        if base.empty:
            return self._empty_df([
                "date", "date_key", "month_key", "year", "month", "quarter", "revenue", "cogs", "profit",
                "lag_1", "lag_7", "lag_30", "rolling_mean_7", "rolling_mean_30", "rolling_std_30",
                "revenue_growth_7d", "profit_margin"
            ])
        base = base.sort_values("date").reset_index(drop=True)
        base["lag_1"] = base["revenue"].shift(1)
        base["lag_7"] = base["revenue"].shift(7)
        base["lag_30"] = base["revenue"].shift(30)
        base["rolling_mean_7"] = base["revenue"].rolling(7, min_periods=1).mean()
        base["rolling_mean_30"] = base["revenue"].rolling(30, min_periods=1).mean()
        base["rolling_std_30"] = base["revenue"].rolling(30, min_periods=2).std()
        base["revenue_growth_7d"] = self._safe_divide(base["revenue"] - base["lag_7"], base["lag_7"])
        base["profit_margin"] = self._safe_divide(base["profit"], base["revenue"])
        return base[[
            "date", "date_key", "month_key", "year", "month", "quarter", "revenue", "cogs", "profit",
            "lag_1", "lag_7", "lag_30", "rolling_mean_7", "rolling_mean_30", "rolling_std_30",
            "revenue_growth_7d", "profit_margin"
        ]]

    # =========================
    # Public API
    # =========================
    @property
    def dim_product(self) -> pd.DataFrame:
        return self._dim_product

    @property
    def dim_return_reason(self) -> pd.DataFrame:
        return self._dim_return_reason

    @property
    def dim_promotion(self) -> pd.DataFrame:
        return self._dim_promotion

    @property
    def dim_geography(self) -> pd.DataFrame:
        return self._dim_geography

    @property
    def dim_customer(self) -> pd.DataFrame:
        return self._dim_customer

    @property
    def dim_traffic_source(self) -> pd.DataFrame:
        return self._dim_traffic_source

    def transform(self) -> Dict[str, pd.DataFrame]:
        """
        Xây toàn bộ 24 bảng transform.

        Thứ tự build:
        1. Đọc và chuẩn hoá raw sources
        2. Build dimensions
        3. Build facts
        4. Build marts
        5. Đóng gói toàn bộ vào self.table
        """
        self._prepare_source_data()

        # dimensions (7)
        self._dim_date = self._build_dim_date()
        self._dim_product = self._build_dim_product()
        self._dim_customer = self._build_dim_customer()
        self._dim_geography = self._build_dim_geography()
        self._dim_promotion = self._build_dim_promotion()
        self._dim_return_reason = self._build_dim_return_reason()
        self._dim_traffic_source = self._build_dim_traffic_source()

        # facts (8)
        self._fact_sales_item = self._build_fact_sales_item()
        self._fact_orders = self._build_fact_orders()
        self._fact_returns = self._build_fact_returns()
        self._fact_shipments = self._build_fact_shipments()
        self._fact_reviews = self._build_fact_reviews()
        self._fact_inventory_snapshot = self._build_fact_inventory_snapshot()
        self._fact_sales_series = self._build_fact_sales_series()
        self._fact_web_traffic = self._build_fact_web_traffic()

        # validation (không tính vào 24 bảng chính)
        self._sales_reconciliation = self._build_sales_reconciliation()

        # marts (9)
        self._mart_sales_overview = self._build_mart_sales_overview()
        self._mart_customer_insight = self._build_mart_customer_insight()
        self._mart_product_performance = self._build_mart_product_performance()
        self._mart_operations = self._build_mart_operations()
        self._mart_promotion_effectiveness = self._build_mart_promotion_effectiveness()
        self._mart_return_diagnostics = self._build_mart_return_diagnostics()
        self._mart_shipping_service = self._build_mart_shipping_service()
        self._mart_inventory_risk = self._build_mart_inventory_risk()
        self._mart_sales_forecast_ready = self._build_mart_sales_forecast_ready()

        self.table = {
            # dimensions
            "dim_date": self._dim_date,
            "dim_product": self._dim_product,
            "dim_customer": self._dim_customer,
            "dim_geography": self._dim_geography,
            "dim_promotion": self._dim_promotion,
            "dim_return_reason": self._dim_return_reason,
            "dim_traffic_source": self._dim_traffic_source,
            # facts
            "fact_sales_item": self._fact_sales_item,
            "fact_orders": self._fact_orders,
            "fact_returns": self._fact_returns,
            "fact_shipments": self._fact_shipments,
            "fact_reviews": self._fact_reviews,
            "fact_inventory_snapshot": self._fact_inventory_snapshot,
            "fact_sales_series": self._fact_sales_series,
            "fact_web_traffic": self._fact_web_traffic,
            # marts
            "mart_sales_overview": self._mart_sales_overview,
            "mart_customer_insight": self._mart_customer_insight,
            "mart_product_performance": self._mart_product_performance,
            "mart_operations": self._mart_operations,
            "mart_promotion_effectiveness": self._mart_promotion_effectiveness,
            "mart_return_diagnostics": self._mart_return_diagnostics,
            "mart_shipping_service": self._mart_shipping_service,
            "mart_inventory_risk": self._mart_inventory_risk,
            "mart_sales_forecast_ready": self._mart_sales_forecast_ready,
        }

        self._log("payments được dùng để enrich fact_sales_item/fact_orders; không tách thành fact riêng để giữ tổng số bảng = 24.")
        self._log("web_traffic được giữ độc lập; không build relationship sang orders để tránh attribution giả.")
        self._log("sales được giữ độc lập; dùng cho forecast và feature engineering, không ép mặc định bằng transaction aggregate.")
        self._log("sales_reconciliation được build như bảng validation riêng để đối chiếu sales.csv với transaction theo ngày.")
        self._log("Không build mart web riêng để giữ đúng khung 24 bảng; Power BI có thể trực quan trực tiếp từ fact_web_traffic.")
        return self.table

    def export_tables(self, output_root: str = "output/transform_output") -> None:
        """
        Export toàn bộ bảng ra thư mục output_root.

        Cấu trúc thư mục:
        - output_root/dim
        - output_root/fact
        - output_root/mart
        - output_root/validation
        - output_root/build_notes.txt
        """
        root = Path(output_root)
        dim_dir = root / "dim"
        fact_dir = root / "fact"
        mart_dir = root / "mart"
        validation_dir = root / "validation"
        dim_dir.mkdir(parents=True, exist_ok=True)
        fact_dir.mkdir(parents=True, exist_ok=True)
        mart_dir.mkdir(parents=True, exist_ok=True)
        validation_dir.mkdir(parents=True, exist_ok=True)

        for name, df in self.table.items():
            if name.startswith("dim_"):
                out_path = dim_dir / f"{name}.csv"
            elif name.startswith("fact_"):
                out_path = fact_dir / f"{name}.csv"
            else:
                out_path = mart_dir / f"{name}.csv"
            df.to_csv(out_path, index=False)

        if not self._sales_reconciliation.empty:
            self._sales_reconciliation.to_csv(validation_dir / "sales_reconciliation.csv", index=False)

        (root / "build_notes.txt").write_text("\n".join(self.build_notes), encoding="utf-8")

    def get_build_notes(self) -> List[str]:
        """Trả về danh sách ghi chú build."""
        return self.build_notes

    def get_sales_reconciliation(self) -> pd.DataFrame:
        """Trả về bảng đối chiếu sales.csv và transaction aggregate theo ngày."""
        return self._sales_reconciliation.copy()



if __name__ == "__main__":
    transformer = Transform(raw_path=Transform.get_path("data"))
    transformer.transform()
    transformer.export_tables("output/transform_output")
    print("Build xong 24 bảng Transform.")
