import os
import sys
import logging
from datetime import datetime, timedelta

# Khai báo đường dẫn để python tìm thấy module customer_behavior trong dags/
DAGS_DIR = os.path.dirname(os.path.abspath(__file__))
if DAGS_DIR not in sys.path:
    sys.path.insert(0, DAGS_DIR)

from customer_behavior.extract import extract_all
from customer_behavior.Load_bronze import load_all_to_s3_bronze

import redshift_connector # Import thư viện kết nối Redshift của AWS

from airflow import DAG
from airflow.sdk import task, Param
from dotenv import load_dotenv

logger = logging.getLogger(__name__)

# Khai báo các thư mục
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR = os.path.join(BASE_DIR, "data", "ga4")
# Thư mục SQL nằm trong dags/sql/
SQL_DIR = os.path.join(DAGS_DIR, "sql")

# Load file .env nếu có
for env_candidate in [os.path.join(BASE_DIR, ".env"), "/opt/airflow/.env"]:
    if os.path.exists(env_candidate):
        load_dotenv(env_candidate)

default_args = {
    "owner": "data_platform",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# -------------------------------------------------------------------------
# KHAI BÁO AIRFLOW DAG (TASKFLOW API)
# -------------------------------------------------------------------------
with DAG(
    dag_id="customer_behavior_etl",
    default_args=default_args,
    description="ETL Customer Behavior GA4 events to S3 Bronze & Redshift Silver",
    schedule="0 6 * * *",  # Chạy lúc 06:00 sáng mỗi ngày
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["behavior", "ga4", "bronze", "silver", "redshift"],
    params={
        # Ô nhập ngày trên màn hình Trigger (Để trống = tự động lấy ngày hôm qua)
        "run_date": Param(
            default=None,
            type=["null", "string"],
            title="Ngày cần chạy",
            description="Nhập dạng YYYYMMDD (vd: 20261118). Để trống = tự động lấy ngày hôm trước.",
        ),
    },
) as dag:

    @task
    def resolve_run_date(params=None) -> str:
        """Xử lý ngày chạy dựa trên input từ màn hình Trigger"""
        user_date = ""
        if params and "run_date" in params and params["run_date"]:
            user_date = str(params["run_date"]).strip()

        if not user_date:
            yesterday = datetime.now() - timedelta(days=1)
            run_date = yesterday.strftime("%Y%m%d")
        else:
            run_date = user_date

        logger.info(f"Ngày sẽ chạy (run_date): {run_date}")
        return run_date


    @task
    def extract_ga4_events(run_date: str) -> dict:
        """Giai đoạn 1: Trích xuất dữ liệu thô (Extract)"""
        logger.info(f"=== GIAI DOAN 1: EXTRACT (LANDING TO PARQUET) CHO NGÀY: {run_date} ===")
        files = extract_all(DATA_DIR, run_date=run_date) 
        logger.info(f"Extract hoàn tất. Đã tạo {len(files)} files.")
        return {"run_date": run_date, "extracted_files": files}


    @task
    def load_ga4_to_bronze(extract_result: dict) -> dict:
        """Giai đoạn 2: Đẩy lên S3 (Bronze)"""
        run_date = extract_result.get("run_date", "")
        s3_bucket = os.environ.get("S3_BUCKET_NAME", "candycrush-raw")
        s3_prefix = os.environ.get("S3_PREFIX", "bronze-behavior/")
        
        logger.info(f"=== GIAI DOAN 2: LOAD BRONZE (AWS S3: s3://{s3_bucket}/{s3_prefix}) ===")
        loaded = load_all_to_s3_bronze(in_dir=DATA_DIR, run_date=run_date)
        logger.info(f"Load hoàn tất. Đã nạp {len(loaded)} files lên AWS S3.")
        return {"run_date": run_date, "loaded_files": loaded}


    @task
    def load_ga4_to_silver(bronze_result: dict):
        """Giai đoạn 3: Làm phẳng dữ liệu trên Redshift (Silver)"""
        run_date = bronze_result.get("run_date", "")
        logger.info(f"=== GIAI DOAN 3: LOAD SILVER (REDSHIFT PIVOT) CHO NGÀY {run_date} ===")
        
        sql_file_path = os.path.join(SQL_DIR, 'silver-behavior-flat-event.sql')
        if not os.path.exists(sql_file_path):
            alt_path = os.path.join(BASE_DIR, "dags", "sql", "silver-behavior-flat-event.sql")
            if os.path.exists(alt_path):
                sql_file_path = alt_path
        
        # Đọc file SQL
        with open(sql_file_path, 'r', encoding='utf-8') as file:
            sql_query = file.read()
            
        # Thay thế placeholder {{ run_date }} nếu có trong SQL (tránh replace mù quáng YYYYMMDD làm hỏng hàm TO_DATE)
        sql_query = sql_query.replace("{{ run_date }}", run_date).replace("{{run_date}}", run_date)

        host = os.environ.get("REDSHIFT_HOST")
        if not host:
            raise ValueError(
                "Chưa cấu hình biến môi trường REDSHIFT_HOST trong file .env! "
                "Vui lòng thêm thông tin kết nối Redshift vào file .env: "
                "REDSHIFT_HOST, REDSHIFT_USER, REDSHIFT_PASS, REDSHIFT_DB."
            )

        try:
            # Kết nối Redshift
            conn = redshift_connector.connect(
                host=host,
                port=int(os.environ.get("REDSHIFT_PORT", 5439)),
                database=os.environ.get("REDSHIFT_DB", "dev"),
                user=os.environ.get("REDSHIFT_USER"),
                password=os.environ.get("REDSHIFT_PASS")
            )
            cursor = conn.cursor()
            
            logger.info(f"Đang thực thi SQL làm phẳng trên Redshift cho ngày {run_date}...")
            
            # Cắt chuỗi SQL thành một danh sách các câu lệnh riêng lẻ dựa vào dấu chấm phẩy
            sql_commands = sql_query.split(';')
            
            # Chạy lần lượt từng lệnh
            for cmd in sql_commands:
                if cmd.strip():  # Bỏ qua các chuỗi khoảng trắng thừa ở cuối file
                    cursor.execute(cmd)
                    
            conn.commit()
            
            logger.info("Thành công! Đã xử lý xong dữ liệu Silver.")
            
        except Exception as e:
            logger.error(f"Lỗi khi truy vấn Redshift: {e}")
            raise e
        finally:
            if 'conn' in locals():
                conn.close()

    # -------------------------------------------------------------------------
    # LIÊN KẾT CÁC TASK (TUẦN TỰ 1 ĐƯỜNG THẲNG: 1 -> 2 -> 3 -> 4)
    # -------------------------------------------------------------------------
    run_date_val = resolve_run_date()
    step_1 = extract_ga4_events(run_date=run_date_val)
    step_2 = load_ga4_to_bronze(extract_result=step_1)
    step_3 = load_ga4_to_silver(bronze_result=step_2)