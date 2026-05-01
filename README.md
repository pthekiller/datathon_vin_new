[![Kaggle](https://img.shields.io/badge/Kaggle-Competition-blue.svg)](https://www.kaggle.com/competitions/datathon-2026-round-1)
[![Python](https://img.shields.io/badge/Python-3.9%2B-blue.svg)](https://www.python.org/)
[![LightGBM](https://img.shields.io/badge/LightGBM-Enabled-orange.svg)](https://lightgbm.readthedocs.io/)
[![VinUni](https://img.shields.io/badge/Hosted%20by-Vin%20Telligence%20(VinUni%20DS%26AI)-red.svg)]()

# DATATHON 2026: THE GRIDBREAKER

Dự án này là giải pháp tham gia cuộc thi **DATATHON 2026: THE GRIDBREAKER** (Biến Dữ liệu thành Giải pháp cho Doanh nghiệp) do Vin Telligence - VinUniversity Data Science & AI Club tổ chức.

Mục tiêu chính của dự án là xây dựng một hệ thống **Dự báo doanh thu (Sales Forecasting)** và **Giá vốn hàng bán (COGS)** cho một doanh nghiệp thương mại điện tử mảng thời trang tại Việt Nam trong giai đoạn 2023-2024, dựa trên dữ liệu lịch sử từ 2012-2022.

---

## 📊 Tổng quan Dữ liệu

Bộ dữ liệu được cung cấp mô phỏng hoạt động thực tế của doanh nghiệp, bao gồm 15 tệp CSV được chia thành 4 lớp dữ liệu chính:

### 1. Master Data (Dữ liệu tham chiếu)

- `products.csv`: Danh mục và thông tin sản phẩm.
- `customers.csv`: Thông tin khách hàng.
- `promotions.csv`: Các chương trình khuyến mãi.
- `geography.csv`: Bản đồ mã bưu chính.

### 2. Transaction Data (Dữ liệu giao dịch)

- `orders.csv`, `order_items.csv`: Thông tin đơn hàng và chi tiết từng dòng sản phẩm.
- `payments.csv`, `shipments.csv`, `returns.csv`: Thông tin thanh toán, vận chuyển và hoàn trả.
- `reviews.csv`: Đánh giá của khách hàng.

### 3. Analytical Data (Dữ liệu phân tích)

- `sales.csv`: Dữ liệu doanh thu và giá vốn hàng bán lịch sử.
- `sample_submission.csv`: Dữ liệu thời gian cần dự báo.

### 4. Operational Data (Dữ liệu vận hành)

- `inventory.csv`, `inventory_enhanced.csv`: Thông tin tồn kho hàng tháng.
- `web_traffic.csv`: Lưu lượng truy cập website hằng ngày.

---

## 🧠 Phương pháp tiếp cận (Methodology)

Giải pháp của nhóm được xây dựng theo hướng kết hợp giữa **Data Engineering**, **Business Analytics** và **Machine Learning Forecasting**. Thay vì chỉ sử dụng dữ liệu thô để trực quan hóa hoặc huấn luyện mô hình trực tiếp, nhóm thiết kế một quy trình xử lý dữ liệu có cấu trúc nhằm biến dữ liệu giao dịch ban đầu thành các bảng phân tích có thể sử dụng ổn định cho cả Power BI và mô hình dự báo.

---

### 1. Xây dựng Data Pipeline, Fact Table và Data Mart

Ở giai đoạn đầu, nhóm tiến hành chuẩn hóa dữ liệu từ các tệp CSV gốc trong thư mục `data/`, sau đó xử lý, làm sạch và liên kết các bảng dữ liệu theo logic nghiệp vụ. Các bảng như đơn hàng, chi tiết đơn hàng, thanh toán, vận chuyển, hoàn trả, sản phẩm, khách hàng, khuyến mãi, tồn kho và web traffic được tổ chức lại thành các lớp dữ liệu phục vụ phân tích.

Ý tưởng chính là xây dựng một lớp **Clean Data** và **Data Mart** trước khi đi vào EDA. Trong đó:

- **Fact tables** lưu các sự kiện kinh doanh chính như doanh số bán hàng, dòng sản phẩm trong đơn hàng, thanh toán, vận chuyển, hoàn trả, tồn kho và lưu lượng truy cập.
- **Dimension tables** lưu thông tin mô tả như ngày tháng, sản phẩm, khách hàng, khu vực địa lý và chương trình khuyến mãi.
- **Business mart** được tổng hợp từ các bảng fact/dimension để phục vụ trực tiếp cho dashboard Power BI và phân tích kinh doanh.

Cách tiếp cận này giúp dữ liệu có cấu trúc rõ ràng hơn, hạn chế việc tính toán lặp lại trong Power BI, đồng thời tạo ra một nguồn dữ liệu nhất quán giữa phần phân tích mô tả và phần mô hình dự báo. Nhờ đó, dashboard không chỉ là phần trực quan hóa độc lập mà còn phản ánh đúng logic dữ liệu đã được chuẩn hóa trong pipeline.

Quy trình tổng quát của lớp xử lý dữ liệu có thể mô tả như sau:

"Raw Data → Data Cleaning → Processed Data → Fact/Dimension Tables → Data Mart → Power BI Dashboard / Forecasting Model"

Trong đó, lớp **Fact/Mart** đóng vai trò trung gian quan trọng giữa dữ liệu gốc và các sản phẩm phân tích cuối cùng. Power BI sử dụng các bảng mart để tạo dashboard, còn notebook dự báo sử dụng dữ liệu đã được xử lý và đặc trưng hóa để huấn luyện mô hình.

---

### 2. Trực quan hóa & Phân tích Dữ liệu (EDA)

Sau khi xây dựng lớp dữ liệu fact/mart, nhóm sử dụng Power BI để phân tích tình hình kinh doanh của doanh nghiệp. Phần EDA được thiết kế theo luồng **Descriptive → Diagnostic → Predictive Support**, nhằm đi từ mô tả thực trạng, phân rã nguyên nhân đến hỗ trợ lựa chọn đặc trưng cho mô hình dự báo.

#### Dashboard 1: Business Overview – Tổng quan hoạt động kinh doanh

Dashboard đầu tiên tập trung mô tả bức tranh tổng thể của doanh nghiệp thông qua các chỉ số chính như **Revenue, Profit, Orders, Customers, COGS và Return Amount** theo thời gian. Mục tiêu của dashboard này là nhận diện xu hướng tăng trưởng, chu kỳ mùa vụ và các giai đoạn biến động mạnh trong hoạt động kinh doanh.

Thông qua dashboard tổng quan, nhóm nhận thấy doanh nghiệp có giai đoạn tăng trưởng rõ rệt trong các năm đầu, thể hiện qua sự gia tăng đồng thời của doanh thu, số lượng đơn hàng và lượng khách hàng. Tuy nhiên, lợi nhuận không luôn tăng tương ứng với doanh thu, cho thấy các yếu tố như giá vốn, hoàn trả, khuyến mãi hoặc chi phí vận chuyển có thể ảnh hưởng đáng kể đến hiệu quả kinh doanh thực tế.

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

---

### 3. Trích xuất đặc trưng (Feature Engineering)

Sau khi có lớp dữ liệu đã xử lý, nhóm tiến hành xây dựng các nhóm đặc trưng phục vụ bài toán dự báo:

- **Time-series Features:** Tạo các biến thời gian như ngày, tháng, quý, cuối tuần, mùa vụ và biến đổi chu kỳ bằng `Sin/Cos`.
- **Expected Demand Mapping:** Xây dựng bản đồ nhu cầu kỳ vọng dựa trên trung vị số lượng đơn hàng và sản phẩm nhằm hạn chế rò rỉ dữ liệu.
- **Promotion & Traffic Features:** Tích hợp số lượng khuyến mãi đang chạy, mức giảm giá, phiên truy cập website, bounce rate và các tín hiệu traffic liên quan.
- **Inventory Health Features:** Tính toán các biến về tình trạng tồn kho như fill rate, stockout rate và sell-through rate.
- **Business Diagnostic Features:** Bổ sung các tín hiệu từ hoàn trả, vận chuyển, chi phí và lợi nhuận để mô hình phản ánh tốt hơn bối cảnh vận hành thực tế.

Các đặc trưng này giúp mô hình không chỉ học từ chuỗi doanh thu lịch sử, mà còn tận dụng được các yếu tố vận hành có khả năng ảnh hưởng đến doanh thu và COGS trong tương lai.

---

### 4. Mô hình lai (Hybrid Model: Ridge + LightGBM)

Nhóm sử dụng mô hình lai nhằm kết hợp khả năng nắm bắt xu hướng tuyến tính dài hạn và khả năng học các dao động phi tuyến từ dữ liệu.

#### Bước 1 – Trend Modeling

Sử dụng `HuberRegressor` hoặc `Ridge Regression` để dự đoán xu hướng dài hạn của doanh thu và COGS. Mục tiêu của bước này là tách phần tăng trưởng tổng quát khỏi chuỗi dữ liệu.

#### Bước 2 – Residual Modeling

Sử dụng `LightGBM Regressor` để học phần dư còn lại sau khi đã tách xu hướng. Phần dư này thường chứa các dao động phi tuyến do mùa vụ, khuyến mãi, traffic, tồn kho hoặc các yếu tố vận hành khác gây ra.

#### Bước 3 – Fusion

Kết quả dự báo cuối cùng được tính bằng tổng của phần xu hướng và phần dư dự đoán:

"Final Forecast = Trend Prediction + Predicted Residual"

Mô hình được đánh giá bằng **Time Series Cross-Validation** nhằm đảm bảo quá trình kiểm định phù hợp với bản chất chuỗi thời gian. Các chỉ số như **RMSE, MAE và R2-Score** được sử dụng để đánh giá hiệu quả dự báo.

---

## 📂 Cấu trúc thư mục

`data/` là nơi lưu dữ liệu gốc của cuộc thi. `output/` là nơi lưu dữ liệu sau xử lý, bao gồm dữ liệu sạch, datamart và dữ liệu processed phục vụ phân tích. Phần 1 chứa notebook xử lý yêu cầu ban đầu, Phần 2 chứa pipeline xử lý dữ liệu và dashboard Power BI, còn Phần 3 chứa notebook dự báo.

DATATHON_VIN_NEW/
├── data/
├── output/
│   ├── Clean_Data/
│   │   ├── datamart/
│   │   └── processed/
│   └── processed_data/
├── Phần 1/
│   └── vinuni-part1.ipynb
├── Phần 2/
│   ├── processing/
│   │   ├── __pycache__/
│   │   ├── create_db_sqlserver.sql
│   │   ├── export_business.py
│   │   ├── load_to_db_sqlserver.py
│   │   ├── processing_data_aligned.py
│   │   ├── run_pipeline.py
│   │   └── Transform.py
│   └── Datathon2026_Round1_BusinessAnalytics_Dashboard.pbix
├── Phần 3/
│   └── Forecast.ipynb
├── .gitattributes
├── Đề-thi-Vòng-1.pdf
├── features.txt
├── README.md
└── submission.csv

---

## ⚙️ Quy trình chạy dự án

Để chạy pipeline xử lý dữ liệu, thực hiện file sau trong thư mục `Phần 2/processing/`:

"python run_pipeline.py"

Pipeline sẽ thực hiện các bước chính:

- Đọc dữ liệu gốc từ thư mục `data/`.
- Làm sạch và chuẩn hóa dữ liệu.
- Sinh dữ liệu processed.
- Xây dựng các bảng fact/dimension/datamart.
- Xuất dữ liệu phục vụ Power BI và mô hình dự báo.

Để load dữ liệu vào SQL Server, sử dụng file:

"python load_to_db_sqlserver.py"

File `create_db_sqlserver.sql` được sử dụng để tạo cấu trúc database, schema và các bảng cần thiết cho lớp datamart.

---

## 📈 Dashboard Power BI

Dashboard Power BI được đặt tại:

"Phần 2/Datathon2026_Round1_BusinessAnalytics_Dashboard.pbix"

Dashboard được xây dựng nhằm hỗ trợ phân tích kinh doanh theo ba hướng chính:

- **Business Overview:** Theo dõi tổng quan doanh thu, lợi nhuận, đơn hàng, khách hàng, COGS và hoàn trả.
- **Profit Diagnostic:** Phân tích nguyên nhân biến động lợi nhuận theo chi phí, hoàn trả, vận chuyển, khuyến mãi, sản phẩm và khu vực.
- **Customer, Product & Region Insight:** Phân tích nhóm khách hàng, dòng sản phẩm và khu vực có ảnh hưởng lớn đến hiệu quả kinh doanh.

Các dashboard này giúp kết nối giữa phân tích mô tả, phân tích nguyên nhân và định hướng xây dựng đặc trưng cho mô hình dự báo.

---

## 🤖 Mô hình dự báo

Notebook dự báo được đặt tại:

"Phần 3/Forecast.ipynb"

Mô hình dự báo tập trung vào hai biến mục tiêu chính:

- **Revenue:** Doanh thu.
- **COGS:** Giá vốn hàng bán.

Phương pháp chính là mô hình lai giữa hồi quy tuyến tính và LightGBM. Trong đó, hồi quy tuyến tính dùng để học xu hướng dài hạn, còn LightGBM dùng để học phần dư và các dao động phi tuyến.

Kết quả cuối cùng được xuất ra file:

"submission.csv"

---

## 🧩 Công nghệ sử dụng

- **Python 3.9+**
- **Pandas, NumPy** cho xử lý dữ liệu.
- **Scikit-learn** cho mô hình tuyến tính và đánh giá.
- **LightGBM** cho mô hình học máy.
- **SQL Server** cho lưu trữ và tổ chức datamart.
- **Power BI** cho trực quan hóa và phân tích kinh doanh.
- **Git/Git LFS** cho quản lý mã nguồn và tệp dung lượng lớn.

---

## 🎯 Ý nghĩa kinh doanh

Dự án không chỉ tập trung vào việc dự báo doanh thu và COGS, mà còn xây dựng một quy trình phân tích dữ liệu có khả năng hỗ trợ ra quyết định kinh doanh. Thông qua việc kết hợp pipeline xử lý dữ liệu, fact/mart, dashboard Power BI và mô hình học máy, doanh nghiệp có thể:

- Theo dõi tình hình kinh doanh theo thời gian.
- Phát hiện các giai đoạn tăng trưởng hoặc suy giảm bất thường.
- Phân tích nguyên nhân ảnh hưởng đến lợi nhuận.
- Hiểu rõ nhóm khách hàng, sản phẩm và khu vực quan trọng.
- Sử dụng các tín hiệu vận hành để cải thiện chất lượng dự báo.
- Hỗ trợ lập kế hoạch doanh thu, tồn kho, khuyến mãi và vận hành trong tương lai.

---

## 📌 Kết luận

Dự án DATATHON 2026 này được xây dựng theo hướng end-to-end, bao gồm xử lý dữ liệu, xây dựng datamart, trực quan hóa kinh doanh và mô hình dự báo. Cách tiếp cận này giúp nhóm không chỉ tạo ra kết quả dự báo, mà còn xây dựng được một hệ thống phân tích có khả năng giải thích, hỗ trợ insight và gắn với bối cảnh vận hành thực tế của doanh nghiệp.

