import os
import glob
import time
import shutil
import logging
from pyarrow import parquet as pq
import boto3

logger = logging.getLogger(__name__)

def load_all_to_s3_bronze(in_dir: str, run_date: str):
    """
    Thực thi đúng triết lý của anh Huy: Bơm (Append) dữ liệu thô vào tầng Bronze.
    Không bao giờ ghi đè file cũ. Lưu giữ 100% lịch sử gốc.
    """
    bucket = os.environ.get("S3_BUCKET_NAME", "candycrush-raw")
    prefix = os.environ.get("S3_PREFIX", "bronze-behavior/").rstrip("/") + "/"
    
    s3_client = boto3.client(
        "s3",
        aws_access_key_id=os.environ.get("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=os.environ.get("AWS_SECRET_ACCESS_KEY"),
        region_name=os.environ.get("AWS_DEFAULT_REGION", "ap-southeast-2")
    )

    # Tìm các file parquet vừa được extract ở local
    paths = sorted(glob.glob(f"{in_dir}/*.parquet"))
    if not paths:
        logger.info(f"Không có dữ liệu mới cho ngày {run_date} để nạp lên Bronze.")
        return []

    archive_dir = os.path.join(in_dir, "archive")
    os.makedirs(archive_dir, exist_ok=True)
    
    loaded_files = []
    
    # [QUAN TRỌNG NHẤT]: Lấy mốc thời gian chạy (Timestamp) để làm chìa khóa Append
    run_timestamp = int(time.time())

    for path in paths:
        filename = os.path.basename(path)
        name, ext = os.path.splitext(filename)
        
        # BÍ QUYẾT LÀM CODE THEO LỜI ANH HUY:
        # Nếu hôm nay là ngày T, file sẽ được gắn thêm timestamp lúc chạy.
        # Ví dụ: data_20261117_1731920000.parquet
        # Nếu ngày mai bạn chạy lại ngày T, nó sẽ sinh ra data_20261117_1731999999.parquet
        # => File cũ trên S3 (dù là của ngày T-31) KHÔNG BAO GIỜ bị ghi đè hay xóa mất!
        unique_append_filename = f"{name}_{run_timestamp}{ext}"
        s3_key = f"{prefix}{unique_append_filename}"
        
        try:
            logger.info(f"Đang nạp (APPEND) file vào Bronze Data Lake: s3://{bucket}/{s3_key}")
            
            # Đẩy thẳng lên S3 với tên file duy nhất
            s3_client.upload_file(path, bucket, s3_key)
            
            # Cất file ở local vào archive sau khi đẩy thành công để dọn dẹp thư mục làm việc
            dest_archive_path = os.path.join(archive_dir, filename)
            shutil.move(path, dest_archive_path)
            
            loaded_files.append(unique_append_filename)

        except Exception as e:
            logger.error(f"Lỗi khi đẩy file {filename} lên S3 Bronze: {e}")
            raise e

    logger.info(f"Hoàn tất! Đã append {len(loaded_files)} file an toàn vào tầng Bronze.")
    return loaded_files