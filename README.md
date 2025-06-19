# Xử Lý Dữ Liệu Lớn Thời Gian Thực với PySpark: Phân tích Dữ liệu Chuyến đi Taxi NYC

![Python](https://img.shields.io/badge/Python-3.8+-blue?logo=python&logoColor=white)
![PySpark](https://img.shields.io/badge/PySpark-3.4.1-orange?logo=apache-spark&logoColor=white)
![Architecture](https://img.shields.io/badge/Architecture-Medallion-brightgreen)
![License](https://img.shields.io/badge/License-MIT-green.svg)

Dự án này là một pipeline xử lý dữ liệu lớn end-to-end, được xây dựng để tiếp nhận, làm sạch, biến đổi và phân tích bộ dữ liệu khổng lồ về các chuyến đi taxi tại New York City (NYC). Pipeline được thiết kế với khả năng mở rộng và chịu lỗi, áp dụng các phương pháp thực tiễn tốt nhất trong ngành Kỹ sư Dữ liệu (Data Engineering) như kiến trúc Medallion và tích hợp xử lý luồng.

## Mục Tiêu Đồ Án

-   **Thiết kế và triển khai một pipeline xử lý dữ liệu lớn** có khả năng mở rộng và chịu lỗi bằng Apache PySpark.
-   **Áp dụng kiến trúc Medallion (Bronze → Silver → Gold)** để xây dựng một quy trình biến đổi dữ liệu nhiều giai đoạn, đảm bảo chất lượng và tính nhất quán.
-   **Tích hợp Spark Structured Streaming** để xây dựng một lớp xử lý dữ liệu gần thời gian thực (near real-time).
-   **Xây dựng một bộ dữ liệu cuối cùng (Gold layer)** sạch, tổng hợp, và sẵn sàng cho việc phân tích (analysis-ready).
-   **Đảm bảo chất lượng dữ liệu** thông qua một framework kiểm tra tự động.
-   **Đóng gói ứng dụng** để có thể thực thi thông qua lệnh `spark-submit`, mô phỏng môi trường sản xuất.

## Kiến Trúc Pipeline

Pipeline được thiết kế theo **Kiến trúc Medallion**, một mô hình chuẩn giúp tổ chức dữ liệu thành các lớp với chất lượng tăng dần.

-   **Bronze Layer (Lớp Đồng):**
    -   **Mục tiêu:** Nạp dữ liệu thô từ nguồn.
    -   **Đặc điểm:** Dữ liệu được giữ nguyên trạng, không thay đổi, chỉ thêm các metadata như thời gian nạp (`ingestion_timestamp`). Dữ liệu được phân vùng (partition) theo ngày tháng để tối ưu hóa.

-   **Silver Layer (Lớp Bạc):**
    -   **Mục tiêu:** Làm sạch, chuẩn hóa và làm giàu dữ liệu.
    -   **Đặc điểm:** Áp dụng các quy tắc chất lượng dữ liệu (lọc các bản ghi không hợp lệ), chuẩn hóa kiểu dữ liệu, và tạo ra các cột mới thông qua kỹ thuật feature engineering (ví dụ: `trip_duration`, `speed_mph`).

-   **Gold Layer (Lớp Vàng):**
    -   **Mục tiêu:** Tạo ra các bảng dữ liệu tổng hợp theo nghiệp vụ (business aggregates).
    -   **Đặc điểm:** Dữ liệu được tổng hợp theo các chiều phân tích (ví dụ: theo giờ, theo địa điểm), sẵn sàng cho các công cụ BI, dashboard hoặc machine learning.

-   **Streaming Layer (Lớp Luồng):**
    -   **Mục tiêu:** Xử lý dữ liệu mới trong thời gian gần thực.
    -   **Đặc điểm:** Sử dụng Spark Structured Streaming để đọc dữ liệu mới, thực hiện các phép tổng hợp trên cửa sổ thời gian (sliding windows) và ghi kết quả ra ngoài.

![Medallion Architecture Diagram](https://user-images.githubusercontent.com/11261397/152882285-b8830870-1349-4f7f-9477-850a588889a7.png)
*(Lưu ý: Đây là ảnh minh họa)*

## Công Nghệ Sử Dụng

-   **Framework:** Apache PySpark 3.4+
-   **Ngôn ngữ:** Python 3.8+
-   **Kiến trúc:** Medallion (Bronze, Silver, Gold)
-   **Xử lý:** Batch Processing & Streaming Processing
-   **Định dạng dữ liệu:** Parquet
-   **Thư viện phụ trợ:** Pandas, Matplotlib, Seaborn, Plotly
-   **Môi trường:** Jupyter Notebook, Terminal/Shell

## Cấu Trúc Thư Mục Dự Án
```
nyc_taxi_pipeline/
├── data/
│ ├── raw/ # Dữ liệu Parquet thô từ nguồn
│ ├── bronze/ # Dữ liệu lớp Bronze
│ ├── silver/ # Dữ liệu lớp Silver
│ └── gold/ # Dữ liệu lớp Gold
│ └── streaming_input/ # Dữ liệu để mô phỏng luồng
│ └── streaming_output/ # Kết quả từ job streaming
├── src/
│ ├── bronze_layer.py # Logic cho pipeline Bronze
│ ├── silver_layer.py # Logic cho pipeline Silver
│ ├── gold_layer.py # Logic cho pipeline Gold
│ ├── streaming_pipeline.py # Logic cho pipeline Streaming
│ ├── data_quality.py # Framework kiểm tra chất lượng dữ liệu
│ ├── utils.py # Các hàm tiện ích
│ └── main_pipeline.py # Script chính để thực thi toàn bộ pipeline
├── notebooks/
│ ├── 01_data_exploration.ipynb
│ ├── 02_pipeline_development.ipynb
│ └── 03_analytics_dashboard.ipynb # Notebook để visualize kết quả
├── config/
│ ├── spark_config.py
│ └── pipeline_config.yaml
├── tests/
│ ├── test_bronze_layer.py
│ └── test_silver_layer.py
├── checkpoint/ # Checkpoints cho streaming
├── logs/ # Log của ứng dụng
├── requirements.txt # Các thư viện Python cần thiết
└── README.md # Tài liệu hướng dẫn này
```
## Hướng Dẫn Cài Đặt và Chạy Dự Án

### 1. Yêu cầu Cần có
-   Java 8+ (bắt buộc cho Spark)
-   Python 3.8+ và `pip`

### 2. Cài đặt

1.  **Clone repository về máy:**
    ```bash
    git clone <URL_repository_của_bạn>
    cd nyc_taxi_pipeline
    ```

2.  **Tạo và kích hoạt môi trường ảo (khuyến khích):**
    ```bash
    python3 -m venv venv
    source venv/bin/activate
    # Đối với Windows: venv\Scripts\activate
    ```

3.  **Cài đặt các thư viện cần thiết:**
    ```bash
    pip install -r requirements.txt
    ```

4.  **Tải dữ liệu mẫu:**
    ```bash
    # Tạo thư mục để chứa dữ liệu thô
    mkdir -p data/raw

    # Tải file parquet mẫu (tháng 1, 2024)
    wget https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-01.parquet -P data/raw/
    ```
    *Lưu ý: Để chạy với nhiều dữ liệu hơn, hãy tải thêm các file từ [NYC TLC website](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page) và đặt vào thư mục `data/raw/`.*

### 3. Chạy Pipeline

Bạn có thể chạy toàn bộ pipeline batch hoặc pipeline streaming thông qua `main_pipeline.py`.

-   **Chạy pipeline Batch (Bronze → Silver → Gold):**
    Mở terminal, đảm bảo môi trường ảo đã được kích hoạt và chạy lệnh sau:
    ```bash
    python src/main_pipeline.py
    ```
    Quá trình này sẽ đọc dữ liệu từ `data/raw`, xử lý qua các lớp Bronze, Silver và lưu kết quả vào `data/gold`.

-   **Chạy pipeline Streaming (Tùy chọn):**
    Để chạy pipeline streaming, bạn cần chuẩn bị dữ liệu trong thư mục `data/streaming_input/`. Sau đó, chạy với cờ `--streaming`:
    ```bash
    python src/main_pipeline.py --streaming
    ```
    Pipeline sẽ bắt đầu theo dõi thư mục đầu vào và xử lý các file mới xuất hiện.

### 4. Xem Kết quả Phân tích
Sau khi chạy pipeline batch, mở notebook `notebooks/03_analytics_dashboard.ipynb` bằng Jupyter để xem các phân tích và trực quan hóa từ dữ liệu ở lớp Gold.

## Thành viên Nhóm
-   **Đỗ Kiến Hưng(darktheDE)** (Team Leader/Data Engineer)
-   **Vũ Đặng Tuấn Anh** (Data Pipeline Engineer)
-   **Nguyễn Văn Quang Duy(QuangDuyReal)** (Streaming Engineer)
-   **Nguyễn Văn Hiền** (Analytics Engineer)

## License
This project is licensed under the MIT License. See the `LICENSE` file for details.
Gợi ý thêm:

Ảnh kiến trúc: Bạn nên vẽ một sơ đồ kiến trúc Medallion đơn giản (có thể dùng draw.io, Lucidchart, hoặc thậm chí PowerPoint) và lưu nó dưới dạng file ảnh (ví dụ: architecture.png). Sau đó, tải ảnh này lên repo GitHub và thay thế link ảnh minh họa trong file README để nó hiển thị đúng sơ đồ của bạn.

LICENSE file: Tạo một file mới tên là LICENSE trong thư mục gốc và dán nội dung của giấy phép MIT vào đó. Bạn có thể tìm thấy mẫu giấy phép MIT dễ dàng trên mạng.

Tùy chỉnh: Hãy thoải mái chỉnh sửa các mô tả để phù hợp hơn với những quyết định thiết kế cụ thể mà nhóm bạn đã thực hiện trong quá trình làm đồ án.
