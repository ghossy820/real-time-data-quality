# 🚀 IoT Real-Time Data Quality Pipeline



## 📌 Tổng quan dự án (Overview)
Dự án này là một hệ thống Data Pipeline thời gian thực (Real-time Streaming) được thiết kế để thu thập, kiểm định và lưu trữ dữ liệu đo lường (Telemetry) từ các thiết bị IoT. 

Hệ thống giải quyết bài toán cốt lõi trong Data Engineering: **Đảm bảo chất lượng dữ liệu (Data Quality) ngay tại thời điểm di chuyển (Data in-motion)**. Thay vì để dữ liệu "rác" đi vào kho lưu trữ, hệ thống áp dụng nguyên lý Fail-fast để phân luồng dữ liệu sạch vào Data Warehouse và đẩy dữ liệu lỗi vào Dead Letter Queue (DLQ) để phân tích sau.

## 🛠 Kiến trúc & Công nghệ (Tech Stack)
Hệ thống được thiết kế theo kiến trúc Microservices và được đóng gói hoàn toàn bằng Docker.
* **Message Broker:** Redpanda (Kafka-compatible) - Đảm bảo khả năng Ingestion với độ trễ thấp.
* **Stream Processing:** Bytewax (Python) - Xử lý luồng dữ liệu liên tục kết hợp State Management.
* **Data Validation:** Pydantic - Quản lý Data Contract và Schema Validation.
* **Data Warehouse:** ClickHouse - Cơ sở dữ liệu dạng cột (Columnar DB) tối ưu hóa cho truy vấn phân tích siêu tốc.
* **Observability:** Grafana - Giám sát Throughput và Error Rate theo thời gian thực.
* **Data Transformation & Testing:** dbt (Data Build Tool).
* **Orchestration:** Makefile & Docker Compose.

## ✨ Tính năng nổi bật (Key Features)
1. **Kiểm định dữ liệu chủ động:** Áp dụng Pydantic để bắt lỗi định dạng, kiểu dữ liệu và logic nghiệp vụ ngay khi dữ liệu rời khỏi Kafka.
2. **Cơ chế Dead Letter Queue (DLQ):** Tự động định tuyến (Routing) dữ liệu bị từ chối vào bảng `iot_telemetry_error` kèm theo nguyên nhân chi tiết.
3. **Batch Insertion tối ưu:** Bytewax gom lô dữ liệu linh hoạt (Time-based & Size-based windowing) trước khi insert vào ClickHouse để tối ưu hóa hiệu năng ghi.
4. **Giám sát trực quan (Observability):** Dashboard Grafana theo dõi lưu lượng sự kiện và top các thiết bị gửi dữ liệu lỗi cập nhật mỗi 5 giây.
5. **Everything-as-Code:** Toàn bộ vòng đời dự án được tự động hóa thông qua `Makefile`.