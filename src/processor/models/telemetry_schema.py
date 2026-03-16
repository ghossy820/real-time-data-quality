from pydantic import BaseModel, Field, field_validator
from datetime import datetime

class IotTelemetryRecord(BaseModel):
    # Các trường dữ liệu bắt buộc
    device_id: str = Field(..., description="Mã định danh duy nhất của thiết bị")
    timestamp: datetime = Field(..., description="Thời gian hệ thống ghi nhận sự kiện")
    temperature: float = Field(..., description="Nhiệt độ đo được (độ C)")
    humidity: float = Field(..., description="Độ ẩm đo được (%)")

    @field_validator('temperature')
    @classmethod
    def validate_temperature_threshold(cls, temperature_value: float) -> float:
        is_valid_temperature = -50.0 <= temperature_value <= 100.0
        if not is_valid_temperature:
            raise ValueError(f"Lỗi Logic: Nhiệt độ {temperature_value}°C nằm ngoài ngưỡng vật lý (-50 đến 100)")
        return temperature_value

    @field_validator('humidity')
    @classmethod
    def validate_humidity_threshold(cls, humidity_value: float) -> float:
        is_valid_humidity = 0.0 <= humidity_value <= 100.0
        if not is_valid_humidity:
            raise ValueError(f"Lỗi Logic: Độ ẩm {humidity_value}% không hợp lệ (phải từ 0 đến 100)")
        return humidity_value