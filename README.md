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
### 1. Trực quan hóa & Phân tích Dữ liệu (EDA)

Ở giai đoạn EDA, nhóm xây dựng hệ thống dashboard trong Power BI nhằm mô tả tình hình kinh doanh, phát hiện các điểm bất thường và hỗ trợ định hướng cho bài toán dự báo doanh thu và giá vốn hàng bán. Thay vì chỉ dừng lại ở thống kê mô tả, phần trực quan hóa được thiết kế theo luồng phân tích từ **Descriptive → Diagnostic → Predictive Support**.

#### Dashboard 1: Business Overview – Tổng quan hoạt động kinh doanh

Dashboard đầu tiên tập trung mô tả bức tranh tổng thể của doanh nghiệp qua các chỉ số chính như **Revenue, Profit, Orders, Customers, COGS, Return Amount** theo thời gian. Mục tiêu của dashboard này là xác định xu hướng tăng trưởng, chu kỳ mùa vụ và các giai đoạn biến động mạnh trong hoạt động kinh doanh.

Thông qua dashboard tổng quan, nhóm nhận thấy doanh nghiệp có giai đoạn tăng trưởng rõ rệt trong các năm đầu, thể hiện qua sự gia tăng đồng thời của doanh thu, số lượng đơn hàng và lượng khách hàng. Tuy nhiên, lợi nhuận không luôn tăng tương ứng với doanh thu, cho thấy chi phí, hoàn trả, khuyến mãi hoặc vận chuyển có thể ảnh hưởng đáng kể đến hiệu quả kinh doanh thực tế. Đây là cơ sở để chuyển sang phân tích nguyên nhân ở các dashboard tiếp theo.

#### Dashboard 2: Profit Diagnostic – Phân tích nguyên nhân biến động lợi nhuận

Dashboard thứ hai được xây dựng nhằm phân rã các yếu tố tác động đến lợi nhuận. Các nhóm yếu tố chính được quan sát bao gồm **COGS, Discount, Shipping Cost, Return Amount, Promotion, Product Category và Region**.

Phân tích diagnostic cho thấy một số giai đoạn doanh thu vẫn duy trì ở mức cao nhưng lợi nhuận suy giảm, hàm ý rằng tăng trưởng doanh thu chưa chắc tạo ra tăng trưởng lợi nhuận bền vững. Các yếu tố như giá vốn hàng bán cao, chi phí vận chuyển lớn, tỷ lệ hoàn trả tăng hoặc mức giảm giá sâu có thể làm biên lợi nhuận bị thu hẹp. Việc phân rã này giúp nhóm nhận diện rõ hơn các nguyên nhân tiềm năng phía sau biến động lợi nhuận, thay vì chỉ quan sát doanh thu ở cấp tổng quan.

#### Dashboard 3: Customer, Product & Region Insight – Phân tích đối tượng và khu vực

Dashboard thứ ba tập trung vào việc xác định nhóm khách hàng, dòng sản phẩm và khu vực đóng góp nhiều nhất vào doanh thu, lợi nhuận cũng như rủi ro hoàn trả. Các biểu đồ được sử dụng để so sánh hiệu quả kinh doanh theo **nhóm tuổi khách hàng, danh mục sản phẩm, khu vực địa lý, rating, số lượng đơn hàng và tỷ lệ hoàn trả**.

Từ góc nhìn khách hàng và sản phẩm, nhóm có thể đánh giá nhóm đối tượng nào mang lại doanh thu cao nhưng lợi nhuận thấp, dòng sản phẩm nào có rating tốt nhưng tỷ lệ hoàn trả cao, hoặc khu vực nào phát sinh chi phí vận chuyển lớn. Những insight này hỗ trợ doanh nghiệp không chỉ hiểu “đang bán được bao nhiêu”, mà còn hiểu “bán cho ai, bán ở đâu, bán dòng hàng nào và hiệu quả thực sự ra sao”.

#### Vai trò của EDA đối với mô hình dự báo

Kết quả trực quan hóa không chỉ phục vụ phần mô tả dữ liệu mà còn được sử dụng trực tiếp để định hướng Feature Engineering. Các insight từ dashboard giúp nhóm xác định những nhóm biến quan trọng cần đưa vào mô hình, bao gồm:

- Biến thời gian và mùa vụ để nắm bắt chu kỳ kinh doanh.
- Biến khuyến mãi để phản ánh tác động của discount và campaign.
- Biến traffic để đo lường nhu cầu và hành vi truy cập website.
- Biến tồn kho để đánh giá khả năng đáp ứng đơn hàng và rủi ro stockout.
- Biến hoàn trả và chi phí để giải thích sự khác biệt giữa doanh thu và lợi nhuận.
- Biến theo khách hàng, sản phẩm và khu vực để phản ánh khác biệt về hành vi mua sắm.

Nhờ đó, phần EDA đóng vai trò cầu nối giữa phân tích kinh doanh và mô hình học máy, giúp mô hình dự báo không chỉ dựa trên xu hướng doanh thu quá khứ mà còn tận dụng các tín hiệu vận hành thực tế của doanh nghiệp.

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
