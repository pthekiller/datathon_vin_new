[![Kaggle](https://img.shields.io/badge/Kaggle-Competition-blue.svg)](https://www.kaggle.com/competitions/datathon-2026-round-1)
[![Python](https://img.shields.io/badge/Python-3.9%2B-blue.svg)](https://www.python.org/)
[![LightGBM](https://img.shields.io/badge/LightGBM-Enabled-orange.svg)](https://lightgbm.readthedocs.io/)
[![VinUni](https://img.shields.io/badge/Hosted%20by-Vin%20Telligence%20(VinUni%20DS%26AI)-red.svg)]()

Dự án này là giải pháp tham gia cuộc thi **DATATHON 2026: THE GRIDBREAKER** (Biến Dữ liệu thành Giải pháp cho Doanh nghiệp) do Vin Telligence - VinUniversity Data Science & AI Club tổ chức.

Mục tiêu chính của dự án là xây dựng một hệ thống **Dự báo doanh thu (Sales Forecasting)** và **Giá vốn hàng bán (COGS)** cho một doanh nghiệp thương mại điện tử mảng thời trang tại Việt Nam trong giai đoạn 2023-2024, dựa trên dữ liệu lịch sử từ 2012-2022.

---

## 📊 Tổng quan Dữ liệu

Bộ dữ liệu được cung cấp mô phỏng hoạt động thực tế của doanh nghiệp, bao gồm 15 tệp CSV được chia thành 4 lớp dữ liệu chính:

1. **Master Data (Dữ liệu tham chiếu)**
   - `products.csv`: Danh mục và thông tin sản phẩm.
   - `customers.csv`: Thông tin khách hàng.
   - `promotions.csv`: Các chương trình khuyến mãi.
   - `geography.csv`: Bản đồ mã bưu chính.
2. **Transaction Data (Giao dịch)**
   - `orders.csv`, `order_items.csv`: Thông tin đơn hàng và chi tiết từng dòng sản phẩm.
   - `payments.csv`, `shipments.csv`, `returns.csv`: Thông tin thanh toán, vận chuyển và hoàn trả.
   - `reviews.csv`: Đánh giá của khách hàng.
3. **Analytical Data (Phân tích)**
   - `sales.csv`: Dữ liệu doanh thu và giá vốn hàng bán lịch sử (Train).
   - `sample_submission.csv`: Dữ liệu thời gian cần dự báo (Test).
4. **Operational Data (Vận hành)**
   - `inventory.csv`, `inventory_enhanced.csv`: Thông tin tồn kho hàng tháng.
   - `web_traffic.csv`: Lưu lượng truy cập website hàng ngày.

---

## 🧠 Phương pháp tiếp cận (Methodology)

Giải pháp của nhóm tập trung vào các bước cốt lõi sau:

### 1. Trực quan hóa & Phân tích Dữ liệu (EDA)
- Phân tích giỏ hàng (Market Basket Analysis) sử dụng thuật toán **Apriori** để tìm ra tập sản phẩm phổ biến và luật kết hợp (Association Rules).
- Trích xuất đặc trưng về hành vi khách hàng theo độ tuổi và thời gian.

### 2. Trích xuất đặc trưng (Feature Engineering)
- **Time-series Features:** Biến đổi chu kỳ thời gian (Seasonality) sử dụng sóng `Sin/Cos`. Bổ sung các cờ ngày lễ, cuối tuần.
- **Expected Demand Mapping:** Xây dựng bản đồ nhu cầu kỳ vọng dựa trên trung vị (Median) số lượng đơn và sản phẩm để chống rò rỉ dữ liệu (Data Leakage).
- **Promotion & Traffic:** Tích hợp số lượng khuyến mãi đang chạy, mức giảm giá tối đa và dữ liệu lịch sử traffic web (Sessions, Bounce Rate)
- **Inventory Health:** Tính toán độ lấp đầy (Fill Rate), tỷ lệ cháy hàng (Stockout Rate) và tốc độ bán (Sell-through Rate).

### 3. Mô hình lai (Hybrid Model: Ridge + LightGBM)
Sử dụng phương pháp học máy kết hợp giữa học thống kê tuyến tính và cây quyết định:
- **Bước 1 (Trend):** Sử dụng `HuberRegressor` / `Ridge Regression` để dự đoán **xu hướng (Trend)** dài hạn của doanh thu nhằm nắm bắt sự tăng trưởng tổng quát.
- **Bước 2 (Seasonality/Residuals):** Sử dụng `LightGBM Regressor` để dự đoán **phần dư (Residuals / Dao động)** bị bỏ lại bởi mô hình tuyến tính.
- **Bước 3 (Fusion):** Kết quả cuối cùng = `Trend` + `Predicted Residuals`.

Mô hình được đánh giá thông qua cơ chế **Time Series Cross-Validation** (5 Folds) tối ưu hóa cho dữ liệu chuỗi thời gian, đạt kết quả cao với các chỉ số **RMSE, MAE, và R2-Score**.

---

## 📂 Cấu trúc thư mục

```text
├── data/
│   ├── products.csv
│   ├── customers.csv
│   ├── orders.csv
│   ├── sales.csv
│   ├── sample_submission.csv
│   └── ... (Các file dữ liệu khác)
├── demo_before.ipynb    # Notebook chính chứa toàn bộ pipeline: Tiền xử lý, Trích xuất đặc trưng và Mô hình
├── Đề-thi-Vòng-1.pdf    # Đề thi gốc từ BTC Datathon 2026
├── README.md            # Tài liệu dự án
└── submission.csv       # File kết quả dự báo trên tập Test
