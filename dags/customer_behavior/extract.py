import logging
import os
import re
from datetime import datetime, timezone
from google.cloud import bigquery
import pyarrow.parquet as pq

logger = logging.getLogger(__name__)


def ensure_google_credentials():
    """
    Tự động định vị file service account Google Credentials để đọc dữ liệu từ BigQuery nguồn
    dù chạy ở máy host hay trong container Airflow.
    """
    current = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS", "")
    if current and os.path.isabs(current) and os.path.exists(current):
        return

    key_filename = os.path.basename(current or "jde_behavior_key.json")
    base_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    candidates = [
        current,
        os.path.join(base_dir, key_filename),
        os.path.join(base_dir, "config", key_filename),
        os.path.join("/opt/airflow", key_filename),
        os.path.join("/opt/airflow/config", key_filename),
    ]

    for path in candidates:
        if path and os.path.exists(path):
            os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = os.path.abspath(path)
            logger.info("Đã tìm thấy Google Credentials tại: %s", os.environ["GOOGLE_APPLICATION_CREDENTIALS"])
            return

    logger.warning("Không tìm thấy file credentials tại các vị trí mặc định: %s", candidates)


def get_client(project_id):
    """Khởi tạo BigQuery Client kết nối tới project nguồn."""
    ensure_google_credentials()
    return bigquery.Client(project=project_id)


def get_watermark():
    """
    Lấy watermark hiện tại: event_timestamp lớn nhất từ AWS S3 Bronze Data Lake.
    """
    from customer_behavior.Load_bronze import get_s3_watermark

    return get_s3_watermark()


def list_source_tables(src_client=None):
    """Lấy danh sách bảng events_intraday_YYYYMMDD ở dataset nguồn."""
    src_project = os.environ["SRC_PROJECT_ID"]
    if src_client is None:
        src_client = get_client(src_project)

    dataset = os.environ["SRC_DATASET_ID"]
    prefix = os.environ.get("SRC_TABLE_ID", "events_intraday_")

    tables = sorted(
        t.table_id
        for t in src_client.list_tables(f"{src_project}.{dataset}")
        if re.fullmatch(rf"{prefix}\d{{8}}", t.table_id)
    )
    return tables


def extract_table(table_id, out_dir, watermark=0, src_client=None, overwrite=False):
    """
    Đọc dữ liệu từ 1 bảng nguồn với điều kiện event_timestamp > watermark,
    sau đó lưu ra file Parquet nén SNAPPY tại Landing Zone.
    """
    src_project = os.environ["SRC_PROJECT_ID"]
    if src_client is None:
        src_client = get_client(src_project)

    os.makedirs(out_dir, exist_ok=True)
    path = os.path.join(out_dir, f"{table_id}.parquet")

    if os.path.exists(path) and not overwrite:
        logger.info("File %s đã tồn tại trên đĩa, bỏ qua extract.", path)
        return path

    src_dataset = os.environ["SRC_DATASET_ID"]
    ref = f"{src_project}.{src_dataset}.{table_id}"

    # Lọc dữ liệu tăng dần theo watermark
    query = f"""
        SELECT *
        FROM `{ref}`
        WHERE event_timestamp > {watermark}
    """

    # Query source tại location US
    query_job = src_client.query(query, location="US")

    # Tối ưu: Dùng Storage Read API qua client của SRC_PROJECT_ID để tải siêu tốc
    try:
        arrow_table = query_job.to_arrow(create_bqstorage_client=True)
    except Exception as e:
        logger.warning("Storage Read API không khả dụng (%s), fallback sang REST API...", e)
        arrow_table = query_job.to_arrow(create_bqstorage_client=False)

    num_rows = arrow_table.num_rows
    if num_rows == 0:
        logger.info("Bảng %s không có dữ liệu mới (> watermark %s).", table_id, watermark)
        return None

    # Ghi file Parquet ra đĩa (Landing Zone)
    pq.write_table(arrow_table, path, compression="SNAPPY")
    logger.info("Đã lưu %s: %s dòng vào %s", table_id, num_rows, path)
    return path


def extract_all(out_dir, run_date=None, watermark=None, max_tables=None, overwrite=False):
    """
    Tải các sự kiện mới từ BigQuery nguồn vào file Parquet:
    - Nếu run_date được truyền vào (vd: '20261118'): Chỉ trích xuất riêng ngày đó (chế độ chạy ngày / backfill).
    - Nếu run_date để trống: Tự động trích xuất tăng dần (Incremental) dựa vào Watermark từ AWS S3.
    """
    src_project = os.environ["SRC_PROJECT_ID"]
    src_client = get_client(src_project)
    prefix = os.environ.get("SRC_TABLE_ID", "events_intraday_")

    # Trường hợp 1: Chạy theo ngày cụ thể (do Airflow truyền vào hoặc chạy tay)
    if run_date:
        table_id = f"{prefix}{run_date}"
        logger.info("Trích xuất cho ngày cụ thể: %s (Bảng nguồn: %s)", run_date, table_id)
        # Khi chỉ định ngày cụ thể, trích xuất toàn bộ ngày đó
        file_path = extract_table(table_id, out_dir, watermark=0, src_client=src_client, overwrite=overwrite)
        return [file_path] if file_path else []

    # Trường hợp 2: Chạy Incremental theo Watermark trên AWS S3
    if watermark is None:
        watermark = get_watermark()

    tables = list_source_tables(src_client)
    logger.info("Tìm thấy %s bảng nguồn tại %s.%s", len(tables), src_project, os.environ["SRC_DATASET_ID"])

    candidate_tables = []
    if watermark > 0:
        wm_date = datetime.fromtimestamp(watermark / 1e6, tz=timezone.utc).strftime("%Y%m%d")
        for t in tables:
            match = re.search(r"\d{8}", t)
            if match:
                table_date = match.group(0)
                if table_date >= wm_date:
                    candidate_tables.append(t)
    else:
        candidate_tables = tables

    if max_tables:
        candidate_tables = candidate_tables[:max_tables]

    logger.info("Số bảng cần quét sau khi lọc watermark (%s): %s bảng", watermark, len(candidate_tables))

    extracted_files = []
    for table_id in candidate_tables:
        file_path = extract_table(table_id, out_dir, watermark=watermark, src_client=src_client, overwrite=overwrite)
        if file_path:
            extracted_files.append(file_path)

    logger.info("Hoàn tất extract: %s file Parquet sẵn sàng để load.", len(extracted_files))
    return extracted_files
