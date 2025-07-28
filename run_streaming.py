import logging
from src.streaming_pipeline import run_streaming_pipeline

def main():
    # Logging cấu hình để hiển thị rõ thời gian và cấp độ
    logging.basicConfig(level=logging.INFO, format='%(asctime)s: %(levelname)s: %(message)s')
    logger = logging.getLogger("RunStreaming")

    try:
        duration_minutes = 1  # Bạn có thể sửa thời gian ở đây nếu muốn
        logger.info(f"Starting streaming pipeline for {duration_minutes} minutes...")
        run_streaming_pipeline(duration_minutes=duration_minutes)
        logger.info("Streaming pipeline completed successfully.")

    except Exception as e:
        logger.error("Streaming pipeline failed.", exc_info=True)

if __name__ == "__main__":
    main()
