# s3-pylogger

A writable, file-like stream that uploads to S3 objects, periodically
rotating to a new object name. Usable as a Python `logging.Handler`.

Python port of the Node.js
[`s3-streamlogger`](https://github.com/Coggle/s3-streamlogger) package,
keeping the same options and behavior where Python conventions allow.

## Features

- 🗂 **Hive-style date partitioning** (`year=YYYY/month=MM/day=DD`) out of the box — ready for AWS Athena
- 📋 **JSON log format** — structured, field-queryable logs with a single option
- 🔄 **Automatic log rotation** — by time or file size
- 🗜 **gzip compression** — upload compressed logs to cut S3 costs
- 🧵 **Thread-safe** — safe to use from multi-threaded applications
- 🪣 **Zero extra dependencies** — only `boto3` required

## Installation

```bash
pip install s3-pylogger
```

AWS credentials are resolved the normal `boto3` way (environment
variables, shared credentials file, IAM role, etc.) — there's no
`access_key_id` / `secret_access_key` option; pass `boto3_kwargs` or your
own `s3_client` if you need to customize this.

---

## Basic usage

```python
from s3_pylogger import S3StreamLogger

s3stream = S3StreamLogger(bucket="mys3bucket")
s3stream.write("hello S3\n")
s3stream.close()  # flushes any remaining buffered data
```

## Use with the standard `logging` module

```python
import logging
from s3_pylogger import S3StreamHandler

handler = S3StreamHandler(bucket="mys3bucket")
logger = logging.getLogger("myapp")
logger.addHandler(handler)
logger.setLevel(logging.INFO)

logger.info("Hello logging!")
```

---

## Date partitioning (Athena-compatible)

By default every object is stored under a Hive-style date partition so
that analytics tools like **AWS Athena** can discover and prune partitions
automatically:

```
s3://mys3bucket/myapp/logs/year=2026/month=09/day=23/2026-09-23-17-30-00-abc12345-myhost.log
```

This is **on by default** — no configuration needed. To disable it and
use a flat layout, set `partition_by_date=False`:

```python
# Partitioned layout (default)
s3stream = S3StreamLogger(bucket="mys3bucket", folder="myapp/logs")
# → myapp/logs/year=2026/month=09/day=23/<filename>.log

# Flat layout (legacy)
s3stream = S3StreamLogger(bucket="mys3bucket", folder="myapp/logs", partition_by_date=False)
# → myapp/logs/<filename>.log
```

---

## JSON log format

Pass `log_format="json"` to `S3StreamHandler` and every log record is
written as a single-line JSON object — no extra packages required:

```python
import logging
from s3_pylogger import S3StreamHandler

handler = S3StreamHandler(
    bucket="mys3bucket",
    folder="myapp/logs",
    log_format="json",          # "json" or "text" (default)
)
logger = logging.getLogger("myapp")
logger.addHandler(handler)
logger.setLevel(logging.INFO)

logger.info("user signed in", extra={"user_id": 42, "ip": "1.2.3.4"})
```

Each line in S3 looks like:

```json
{"timestamp": "2026-09-23T17:30:01.234000Z", "level": "INFO", "logger": "myapp", "message": "user signed in", "user_id": 42, "ip": "1.2.3.4"}
```

Fields always present: `timestamp`, `level`, `logger`, `message`.
Any `extra={...}` keys are promoted to top-level JSON fields and are
directly queryable in Athena. `exc_info` and `stack_info` appear only
when an exception is logged.

You can also use `JsonFormatter` standalone with any handler:

```python
from s3_pylogger import JsonFormatter

handler.setFormatter(JsonFormatter())
```

---

## Querying with AWS Athena

### 1. Create the table

**JSON logs** (recommended — gives per-field filtering):

```sql
CREATE EXTERNAL TABLE IF NOT EXISTS app_logs (
    timestamp STRING,
    level     STRING,
    logger    STRING,
    message   STRING
    -- add extra columns here that match your `extra=` fields
)
PARTITIONED BY (year STRING, month STRING, day STRING)
ROW FORMAT SERDE 'org.openx.data.JsonSerDe'
STORED AS TEXTFILE
LOCATION 's3://mys3bucket/myapp/logs/'
TBLPROPERTIES (
    'projection.enabled'              = 'true',
    'projection.year.type'            = 'integer',
    'projection.year.range'           = '2024,2030',
    'projection.month.type'           = 'integer',
    'projection.month.range'          = '1,12',
    'projection.month.digits'         = '2',
    'projection.day.type'             = 'integer',
    'projection.day.range'            = '1,31',
    'projection.day.digits'           = '2',
    'storage.location.template'       =
        's3://mys3bucket/myapp/logs/year=${year}/month=${month}/day=${day}'
);
```

**Plain-text logs** (one raw line per column):

```sql
CREATE EXTERNAL TABLE IF NOT EXISTS app_logs_raw (
    log_line STRING
)
PARTITIONED BY (year STRING, month STRING, day STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\n'
STORED AS TEXTFILE
LOCATION 's3://mys3bucket/myapp/logs/'
TBLPROPERTIES (
    'projection.enabled'        = 'true',
    -- ... same projection properties as above
);
```

> **Tip:** Using `'projection.enabled' = 'true'` (Partition Projection)
> means Athena discovers new days automatically — you never need to run
> `MSCK REPAIR TABLE`.

### 2. Query examples

```sql
-- All logs for today
SELECT * FROM app_logs
WHERE year='2026' AND month='09' AND day='23';

-- Errors in a date range
SELECT timestamp, logger, message
FROM app_logs
WHERE year='2026' AND month='09' AND day BETWEEN '20' AND '23'
  AND level = 'ERROR'
ORDER BY timestamp DESC;

-- Count log lines per day
SELECT year, month, day, COUNT(*) AS line_count
FROM app_logs
WHERE year='2026' AND month='09'
GROUP BY year, month, day
ORDER BY day;

-- Filter by a custom extra field (JSON format only)
SELECT timestamp, message, user_id, ip
FROM app_logs
WHERE year='2026' AND month='09' AND day='23'
  AND level = 'WARNING'
  AND user_id = 42;
```

> **Always filter on `year`, `month`, `day`** — this prunes the S3 scan
> to only the relevant prefixes and avoids costly full-table scans.

---

## Define subfolder

```python
from s3_pylogger import S3StreamLogger

s3stream = S3StreamLogger(bucket="mys3bucket", folder="my/nested/subfolder")
s3stream.write("hello S3\n")
```

## Assign tags

```python
from s3_pylogger import S3StreamLogger

s3stream = S3StreamLogger(
    bucket="mys3bucket",
    folder="my/nested/subfolder",
    tags={"type": "myType", "project": "myProject"},
)
s3stream.write("hello S3\n")
```

## Handling upload errors

When an upload to S3 fails, the error is passed to the `on_error`
callback you provide, or printed to stderr if you don't provide one:

```python
def handle_error(exc):
    # log elsewhere - never write this back into the same stream,
    # or you risk infinite recursion
    print("s3 upload failed:", exc)

s3stream = S3StreamLogger(bucket="mys3bucket", on_error=handle_error)
```

---

## Options

### `S3StreamLogger`

| Option | Description | Default |
|---|---|---|
| `bucket` *(required)* | Name of the S3 bucket. Falls back to `s3_pylogger_BUCKET` env var. | — |
| `folder` | Key prefix ("subfolder"), e.g. `"myapp/logs"`. | `""` |
| `partition_by_date` | Prepend `year=YYYY/month=MM/day=DD/` to every object key for Athena compatibility. | `True` |
| `tags` | Dict of S3 object tags, e.g. `{"type": "myType"}`. | `{}` |
| `name_format` | `strftime` format for object names, plus `{unique}` and `{hostname}` placeholders. | `"%Y-%m-%d-%H-%M-%S-{unique}-{hostname}.log"` |
| `rotate_every` | Rotate to a new object after this many **seconds**. | `3600` (1 h) |
| `max_file_size` | Rotate once the buffer reaches this many bytes. | `200000` |
| `upload_every` | Upload the buffer at least this often, in **seconds**. | `20` |
| `buffer_size` | Flush immediately once unuploaded data exceeds this many bytes. | `10000` |
| `compress` | Gzip data before uploading (appends `.gz` to the key). | `False` |
| `server_side_encryption` | Use S3 `ServerSideEncryption="AES256"`. | `False` |
| `storage_class` | S3 storage class (`STANDARD`, `STANDARD_IA`, etc.). | S3 default |
| `acl` | Canned ACL for uploaded objects. | none |
| `s3_client` | A pre-configured `boto3` S3 client. | auto-created |
| `boto3_kwargs` | Extra kwargs passed to `boto3.client("s3", ...)`, e.g. `region_name`. | `{}` |
| `on_error` | `fn(exception)` callback invoked on upload failure. | prints to stderr |

> `rotate_every` and `upload_every` are in **seconds** here, unlike the
> Node.js version which uses milliseconds.

### `S3StreamHandler`

Accepts all `S3StreamLogger` options above, plus:

| Option | Description | Default |
|---|---|---|
| `log_format` | `"text"` — normal Python log format. `"json"` — one JSON object per line (Athena-friendly). | `"text"` |
| `stream_logger` | Supply a pre-built `S3StreamLogger` instance directly. | auto-created |

---

## Development

```bash
pip install -e ".[dev]"
pytest
```

## Building and publishing to PyPI

```bash
python -m pip install --upgrade build twine
python -m build                 # creates dist/*.whl and dist/*.tar.gz
python -m twine upload dist/*   # prompts for your PyPI API token
```

Register/reserve the package name on [pypi.org](https://pypi.org) first
(and consider testing against [TestPyPI](https://test.pypi.org) before a
real upload) — package names are first-come, first-served and cannot be
reused once claimed.

## Credits

This package is a Python port of the Node.js
**[s3-streamlogger](https://github.com/Coggle/s3-streamlogger)** library,
originally created and maintained by **[Coggle Ltd.](https://coggle.it)**

The original Node.js library defined the core design of this package:

- Writable-stream interface for S3 uploads
- Time-based and size-based log rotation
- Object naming via `strftime`-compatible format strings with `{unique}` and `{hostname}` placeholders
- `upload_every` / `buffer_size` flush scheduling
- `on_error` callback pattern

All credit for the original concept and architecture belongs to the Coggle
team and contributors. This Python port adds date partitioning,
JSON formatting, and Athena integration on top of that foundation.

## License

ISC — see [LICENSE](LICENSE) for the full text.

The original Node.js `s3-streamlogger` is copyright (c) Coggle Ltd. and
contributors, also distributed under the ISC License.
