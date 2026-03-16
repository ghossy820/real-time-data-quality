.PHONY: help up down logs producer processor dbt-test dbt-run clean

# Mặc định hiển thị danh sách lệnh khi chỉ gõ `make`
help:
	@echo "Danh sách các lệnh vận hành IoT Data Pipeline:"
	@echo "---------------------------------------------------------"
	@echo "HẠ TẦNG:"
	@echo "  make up          - Khởi động Redpanda, ClickHouse, Grafana"
	@echo "  make down        - Tắt toàn bộ hệ thống Docker"
	@echo "  make logs        - Xem log theo thời gian thực của các container"
	@echo "  make clean       - [Nguy hiểm] Xóa toàn bộ hạ tầng và dữ liệu (Volumes)"
	@echo "---------------------------------------------------------"
	@echo "LUỒNG DỮ LIỆU:"
	@echo "  make producer    - Chạy giả lập thiết bị IoT (Python)"
	@echo "  make processor   - Chạy Bytewax phân luồng dữ liệu (Python)"
	@echo "---------------------------------------------------------"
	@echo "DBT & CHẤT LƯỢNG:"
	@echo "  make dbt-run     - Chạy các model tổng hợp (Marts Layer)"
	@echo "  make dbt-test    - Kiểm thử chất lượng dữ liệu (Fail-fast)"

# ==========================================
# 1. Quản lý Hạ tầng (Docker)
# ==========================================
up:
	cd docker && docker compose up -d

down:
	cd docker && docker compose down

logs:
	cd docker && docker compose logs -f

clean:
	cd docker && docker compose down -v

# ==========================================
# 2. Xử lý dữ liệu In-motion (Python/Bytewax)
# ==========================================
producer:
	PYTHONPATH=. python3 -m src.producer.iot_producer

processor:
	PYTHONPATH=. python3 -m src.processor.validation_pipeline
	
# ==========================================
# 3. Lưu trữ & Kiểm thử (dbt/ClickHouse)
# ==========================================
dbt-run:
	cd dbt_project && dbt run --profiles-dir .

dbt-test:
	cd dbt_project && dbt test --profiles-dir .