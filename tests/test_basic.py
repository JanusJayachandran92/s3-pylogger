"""
Basic tests for s3_pylogger.

Requires `moto` (pip install "moto[s3]") to mock S3 without touching a
real bucket:

    pip install -e ".[dev]"
    pytest
"""

import gzip
import json
import logging
import re
import time
from datetime import datetime, timezone

import boto3
import pytest

try:
    from moto import mock_aws  # moto >= 5
except ImportError:  # pragma: no cover
    from moto import mock_s3 as mock_aws  # moto < 5 fallback

from s3_pylogger import S3StreamLogger, S3StreamHandler, JsonFormatter

BUCKET = "test-bucket"
REGION = "us-east-1"


@pytest.fixture
def s3_bucket():
    with mock_aws():
        client = boto3.client("s3", region_name=REGION)
        client.create_bucket(Bucket=BUCKET)
        yield client


def _list_bodies(client, bucket, prefix=""):
    resp = client.list_objects_v2(Bucket=bucket, Prefix=prefix)
    bodies = []
    for obj in resp.get("Contents", []):
        body = client.get_object(Bucket=bucket, Key=obj["Key"])["Body"].read()
        bodies.append((obj["Key"], body))
    return bodies


def test_write_and_flush_uploads_to_s3(s3_bucket):
    stream = S3StreamLogger(
        bucket=BUCKET,
        boto3_kwargs={"region_name": REGION},
        upload_every=999,  # don't let the background thread race the test
        buffer_size=10_000,
    )
    stream.write("hello S3\n")
    stream.flush()

    bodies = _list_bodies(s3_bucket, BUCKET)
    assert len(bodies) == 1
    key, body = bodies[0]
    assert body == b"hello S3\n"
    stream.close()


def test_folder_prefix(s3_bucket):
    stream = S3StreamLogger(
        bucket=BUCKET,
        folder="my/nested/subfolder",
        boto3_kwargs={"region_name": REGION},
        upload_every=999,
    )
    stream.write("nested\n")
    stream.flush()

    bodies = _list_bodies(s3_bucket, BUCKET, prefix="my/nested/subfolder/")
    assert len(bodies) == 1
    stream.close()


def test_compression(s3_bucket):
    stream = S3StreamLogger(
        bucket=BUCKET,
        compress=True,
        boto3_kwargs={"region_name": REGION},
        upload_every=999,
    )
    stream.write("compressed data\n")
    stream.flush()

    bodies = _list_bodies(s3_bucket, BUCKET)
    assert len(bodies) == 1
    key, body = bodies[0]
    assert key.endswith(".gz")
    assert gzip.decompress(body) == b"compressed data\n"
    stream.close()


def test_buffer_size_triggers_auto_flush(s3_bucket):
    stream = S3StreamLogger(
        bucket=BUCKET,
        boto3_kwargs={"region_name": REGION},
        upload_every=999,
        buffer_size=5,  # tiny, so any write over 5 bytes auto-flushes
    )
    stream.write("0123456789")  # 10 bytes > buffer_size
    time.sleep(0.1)

    bodies = _list_bodies(s3_bucket, BUCKET)
    assert len(bodies) == 1
    stream.close()


def test_logging_handler(s3_bucket):
    handler = S3StreamHandler(
        bucket=BUCKET,
        boto3_kwargs={"region_name": REGION},
        upload_every=999,
    )
    logger = logging.getLogger("s3_pylogger.test")
    logger.setLevel(logging.INFO)
    logger.addHandler(handler)

    logger.info("hello logging")
    handler.stream.flush()

    bodies = _list_bodies(s3_bucket, BUCKET)
    assert len(bodies) == 1
    assert b"hello logging" in bodies[0][1]

    logger.removeHandler(handler)
    handler.close()


def test_date_partition_default(s3_bucket):
    """Keys should contain year=YYYY/month=MM/day=DD segments by default."""
    stream = S3StreamLogger(
        bucket=BUCKET,
        boto3_kwargs={"region_name": REGION},
        upload_every=999,
    )
    stream.write("partitioned\n")
    stream.flush()

    bodies = _list_bodies(s3_bucket, BUCKET)
    assert len(bodies) == 1
    key, _ = bodies[0]

    now = datetime.now(timezone.utc)
    expected_prefix = (
        f"year={now.year:04d}/month={now.month:02d}/day={now.day:02d}/"
    )
    assert expected_prefix in key, (
        f"Expected Hive-style partition prefix '{expected_prefix}' in key '{key}'"
    )
    stream.close()


def test_date_partition_with_folder(s3_bucket):
    """Folder prefix should appear before the date partition segments."""
    stream = S3StreamLogger(
        bucket=BUCKET,
        folder="myapp/logs",
        boto3_kwargs={"region_name": REGION},
        upload_every=999,
    )
    stream.write("partitioned with folder\n")
    stream.flush()

    bodies = _list_bodies(s3_bucket, BUCKET, prefix="myapp/logs/")
    assert len(bodies) == 1
    key, _ = bodies[0]

    now = datetime.now(timezone.utc)
    expected_prefix = (
        f"myapp/logs/year={now.year:04d}/month={now.month:02d}/day={now.day:02d}/"
    )
    assert key.startswith(expected_prefix), (
        f"Expected key to start with '{expected_prefix}', got '{key}'"
    )
    stream.close()


def test_date_partition_disabled(s3_bucket):
    """partition_by_date=False should produce a flat (legacy) key layout."""
    stream = S3StreamLogger(
        bucket=BUCKET,
        folder="flat",
        boto3_kwargs={"region_name": REGION},
        upload_every=999,
        partition_by_date=False,
    )
    stream.write("flat layout\n")
    stream.flush()

    bodies = _list_bodies(s3_bucket, BUCKET, prefix="flat/")
    assert len(bodies) == 1
    key, _ = bodies[0]

    # Key must start directly with the folder prefix, no year= segments
    assert key.startswith("flat/"), f"Unexpected key: {key}"
    assert "year=" not in key, f"Unexpected partition in key: {key}"
    stream.close()


def test_on_error_callback_called_on_failure():
    errors = []
    stream = S3StreamLogger(
        bucket="this-bucket-does-not-exist-hopefully",
        boto3_kwargs={"region_name": REGION},
        upload_every=999,
        on_error=lambda exc: errors.append(exc),
    )
    with mock_aws():
        stream.write("this should fail\n")
        stream.flush()
    assert len(errors) == 1
    stream.close()


# ---------------------------------------------------------------------------
# JsonFormatter / log_format tests
# ---------------------------------------------------------------------------


def test_json_formatter_core_fields():
    """JsonFormatter emits the four mandatory fields as valid JSON."""
    formatter = JsonFormatter()
    record = logging.LogRecord(
        name="myapp", level=logging.INFO,
        pathname="", lineno=0, msg="hello json", args=(), exc_info=None,
    )
    line = formatter.format(record)
    data = json.loads(line)  # must be valid JSON
    assert data["level"] == "INFO"
    assert data["logger"] == "myapp"
    assert data["message"] == "hello json"
    assert "timestamp" in data


def test_json_formatter_extra_fields():
    """Extra fields passed via `extra=` are promoted to top-level JSON keys."""
    formatter = JsonFormatter()
    record = logging.LogRecord(
        name="myapp", level=logging.WARNING,
        pathname="", lineno=0, msg="user action", args=(), exc_info=None,
    )
    record.user_id = 42
    record.ip = "1.2.3.4"
    data = json.loads(formatter.format(record))
    assert data["user_id"] == 42
    assert data["ip"] == "1.2.3.4"


def test_json_formatter_exc_info():
    """JsonFormatter serialises exc_info to a string field."""
    formatter = JsonFormatter()
    try:
        raise ValueError("boom")
    except ValueError:
        import sys
        record = logging.LogRecord(
            name="myapp", level=logging.ERROR,
            pathname="", lineno=0, msg="error occurred",
            args=(), exc_info=sys.exc_info(),
        )
    data = json.loads(formatter.format(record))
    assert "exc_info" in data
    assert "ValueError" in data["exc_info"]




# ---------------------------------------------------------------------------
# content_type / S3 metadata tests
# ---------------------------------------------------------------------------


def test_content_type_default_is_text_plain(s3_bucket):
    """Default upload sets ContentType=text/plain; charset=utf-8."""
    stream = S3StreamLogger(
        bucket=BUCKET,
        boto3_kwargs={"region_name": REGION},
        upload_every=999,
    )
    stream.write("hello\n")
    stream.flush()

    resp = s3_bucket.list_objects_v2(Bucket=BUCKET)
    obj = resp["Contents"][0]
    head = s3_bucket.head_object(Bucket=BUCKET, Key=obj["Key"])
    assert head["ContentType"] == "text/plain; charset=utf-8"
    stream.close()


def test_content_type_explicit_override(s3_bucket):
    """Explicit content_type= is passed through to S3."""
    stream = S3StreamLogger(
        bucket=BUCKET,
        boto3_kwargs={"region_name": REGION},
        upload_every=999,
        content_type="application/x-ndjson",
    )
    stream.write("{}\n")
    stream.flush()

    resp = s3_bucket.list_objects_v2(Bucket=BUCKET)
    obj = resp["Contents"][0]
    head = s3_bucket.head_object(Bucket=BUCKET, Key=obj["Key"])
    assert head["ContentType"] == "application/x-ndjson"
    stream.close()


def test_content_type_json_key(s3_bucket):
    """Keys ending in .json get ContentType=application/json automatically."""
    stream = S3StreamLogger(
        bucket=BUCKET,
        boto3_kwargs={"region_name": REGION},
        upload_every=999,
        name_format="app-{unique}.json",
    )
    stream.write("{}\n")
    stream.flush()

    resp = s3_bucket.list_objects_v2(Bucket=BUCKET)
    obj = resp["Contents"][0]
    assert obj["Key"].endswith(".json")
    head = s3_bucket.head_object(Bucket=BUCKET, Key=obj["Key"])
    assert head["ContentType"] == "application/json"
    stream.close()


def test_content_encoding_gzip_set_when_compress(s3_bucket):
    """compress=True sets ContentEncoding=gzip alongside ContentType."""
    stream = S3StreamLogger(
        bucket=BUCKET,
        boto3_kwargs={"region_name": REGION},
        upload_every=999,
        compress=True,
    )
    stream.write("compressed\n")
    stream.flush()

    resp = s3_bucket.list_objects_v2(Bucket=BUCKET)
    obj = resp["Contents"][0]
    head = s3_bucket.head_object(Bucket=BUCKET, Key=obj["Key"])
    assert head["ContentType"] == "text/plain; charset=utf-8"
    assert head.get("ContentEncoding") == "gzip"
    stream.close()
