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
│ └── 02_analytics_dashboard.ipynb # Notebook để visualize kết quả
├── config/
│ ├── spark_config.py
│ └── pipeline_config.yaml
├── checkpoint/ # Checkpoints cho streaming
├── requirements.txt # Các thư viện Python cần thiết
└── README.md # Tài liệu hướng dẫn này
```
## Hướng Dẫn Cài Đặt và Chạy Dự Án

### 1. Yêu cầu Cần có

-   **Java 8+** (bắt buộc cho Spark, nên dùng bản LTS)
-   **Apache Spark 3.4+** (cài đặt local hoặc sử dụng qua PySpark, đã test tốt với Spark 3.4.1)
-   **Hadoop** (chỉ cần cho môi trường Windows để Spark truy cập hệ thống file, không cần cài full cluster):
    -   Tải [winutils.exe](https://github.com/steveloughran/winutils) và đặt vào thư mục `hadoop/bin`
    -   Đảm bảo có file `hadoop.dll` trong thư mục `hadoop/bin` (nếu thiếu, Spark sẽ báo lỗi khi chạy trên Windows)
    -   Thiết lập biến môi trường:  
        -   `HADOOP_HOME` trỏ tới thư mục Hadoop  
        -   Thêm `hadoop/bin` vào `PATH`
-   **Python 3.8+** và `pip`
-   **Các thư viện Python**: cài qua `requirements.txt`
-   **(Khuyến nghị)**: Jupyter Notebook để xem kết quả phân tích

**Lưu ý cho Windows:**  
Nếu chạy Spark trên Windows, bạn *bắt buộc* phải có `winutils.exe` và `hadoop.dll` đúng vị trí, nếu không Spark sẽ không thể ghi/đọc file hệ thống.

### 2. Cài đặt

1.  **Clone repository về máy:**
    ```bash
    git clone https://github.com/QuangDuyReal/nyc-taxi-trip-analysis
    cd nyc-taxi-trip-analysis
    ```

2.  **Cài đặt Java, Spark, Hadoop (nếu chưa có):**
    -   [Tải Java](https://adoptium.net/) và cài đặt
    -   [Tải Spark](https://spark.apache.org/downloads.html) (chọn bản phù hợp với Hadoop 3.x)
    -   [Tải winutils.exe cho Hadoop](https://github.com/steveloughran/winutils) (Windows)
    -   Thiết lập biến môi trường:  
        -   `JAVA_HOME`  
        -   `SPARK_HOME`  
        -   `HADOOP_HOME`  
        -   Thêm `bin` của Spark và Hadoop vào `PATH`

3.  **Tạo và kích hoạt môi trường ảo (khuyến khích):**
    ```bash
    python -m venv venv
    # Linux/macOS:
    source venv/bin/activate
    # Windows:
    venv\Scripts\activate
    ```

4.  **Cài đặt các thư viện Python cần thiết:**
    ```bash
    pip install -r requirements.txt
    ```

5.  **Tải dữ liệu mẫu:**
    ```bash
    mkdir -p data/raw
    curl -L "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-01.parquet" -o data/raw/yellow_tripdata_2024-01.parquet
    ```
    *Hoặc tải thêm dữ liệu từ [NYC TLC website](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page) và đặt vào `data/raw/`.*


## Thành viên Nhóm
-   **Đỗ Kiến Hưng(darktheDE)** (Team Leader/Data Engineer, Data Pipeline Engineer)
-   **Nguyễn Văn Quang Duy(QuangDuyReal)** (Streaming Engineer, Analytics Engineer)

## License
This project is licensed under the MIT License. See the `LICENSE` file for details.
