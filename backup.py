import os
import datetime
import subprocess
import logging
import boto3
from botocore.exceptions import ClientError

logger = logging.getLogger(__name__)

def upload_to_s3_and_cleanup(local_path, filename, s3_bucket_name, s3_path_prefix, s3_region, s3_endpoint, s3_access_key, s3_secret_key):
    try:
        s3 = boto3.client(
            's3',
            region_name=s3_region,
            endpoint_url=s3_endpoint,
            aws_access_key_id=s3_access_key,
            aws_secret_access_key=s3_secret_key
        )
        remote_path = f"{s3_path_prefix}/{filename}"
        s3.upload_file(local_path, s3_bucket_name, remote_path)
        logger.info(f"Uploaded to S3: {remote_path}")

        # Delete local file
        os.remove(local_path)
        logger.info(f"Deleted local file: {local_path}")
        return True

    except ClientError as e:
        logger.exception(f"S3 upload error: {e}")
        return False
    except Exception as e:
        logger.exception(f"Unexpected error during upload/cleanup: {e}")
        return False


def backup_postgresql(db_name, db_user, db_password, db_host, db_port, backup_dir, **s3_kwargs):
    timestamp = datetime.datetime.now().strftime('%d-%m-%y_%H-%M')
    filename = f"{db_name}_pg_backup_{timestamp}.backup"
    full_path = os.path.join(backup_dir, filename)
    os.makedirs(backup_dir, exist_ok=True)

    env = os.environ.copy()
    env["PGPASSWORD"] = db_password

    cmd = [
        "/usr/bin/pg_dump",
        "-h", db_host,
        "-p", str(db_port),
        "-U", db_user,
        "-F", "c",
        "-b",
        "-v",
        "-f", full_path,
        f"--dbname=postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}?sslmode=require"
    ]

    return _run_and_upload(cmd, full_path, filename, env, **s3_kwargs)


def backup_mysql(db_name, db_user, db_password, db_host, db_port, backup_dir, **s3_kwargs):
    timestamp = datetime.datetime.now().strftime('%d-%m-%y_%H-%M')
    filename = f"{db_name}_mysql_backup_{timestamp}.sql"
    full_path = os.path.join(backup_dir, filename)
    os.makedirs(backup_dir, exist_ok=True)

    cmd = [
        "mysqldump",
        f"-h{db_host}",
        f"-P{db_port}",
        f"-u{db_user}",
        f"-p{db_password}",
        db_name
    ]

    try:
        with open(full_path, 'w') as f:
            result = subprocess.run(cmd, stdout=f, stderr=subprocess.PIPE, text=True)

        if result.returncode != 0:
            logger.error(f"MySQL backup failed: {result.stderr}")
            return False

        logger.info(f"MySQL backup created: {full_path}")
        return upload_to_s3_and_cleanup(full_path, filename, **s3_kwargs)

    except Exception as e:
        logger.exception(f"MySQL backup error: {e}")
        return False


def backup_mongodb(db_name, db_user, db_password, db_host, db_port, backup_dir, **s3_kwargs):
    timestamp = datetime.datetime.now().strftime('%d-%m-%y_%H-%M')
    filename = f"{db_name}_mongo_backup_{timestamp}.archive"
    full_path = os.path.join(backup_dir, filename)
    os.makedirs(backup_dir, exist_ok=True)

    cmd = [
        "mongodump",
        f"--uri=mongodb://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}",
        f"--archive={full_path}",
        "--gzip"
    ]

    return _run_and_upload(cmd, full_path, filename, os.environ.copy(), **s3_kwargs)


def _run_and_upload(cmd, full_path, filename, env, **s3_kwargs):
    try:
        result = subprocess.run(cmd, env=env, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)

        if result.returncode != 0:
            logger.error(f"Backup failed: {result.stderr}")
            return False

        logger.info(f"Backup created: {full_path}")
        return upload_to_s3_and_cleanup(full_path, filename, **s3_kwargs)

    except Exception as e:
        logger.exception(f"Backup or upload failed: {e}")
        return False
