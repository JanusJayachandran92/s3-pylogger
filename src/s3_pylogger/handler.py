"""
s3_pylogger.handler
====================

A Python port of the Node.js ``s3-streamlogger`` package
(https://github.com/Coggle/s3-streamlogger).

Provides ``S3StreamLogger``: a thread-safe, file-like / writable object
that buffers writes in memory and periodically uploads them to an S3
object, rotating to a new object name on a time or size schedule.

Also provides ``S3StreamHandler``, a ``logging.Handler`` subclass built
on top of ``S3StreamLogger`` so it can be dropped straight into the
standard library ``logging`` module (the Python analogue of wiring
``s3-streamlogger`` into a Winston transport).
"""

from __future__ import annotations

import gzip
import io
import json
import logging
import os
import socket
import threading
import time
import uuid
from datetime import datetime, timezone
from typing import Callable, Optional

try:
    import boto3
    from botocore.exceptions import BotoCoreError, ClientError
except ImportError as exc:  # pragma: no cover
    raise ImportError(
        "s3_pylogger requires boto3. Install it with `pip install boto3`."
    ) from exc


__all__ = ["S3StreamLogger", "S3StreamHandler", "JsonFormatter"]

DEFAULT_NAME_FORMAT = "%Y-%m-%d-%H-%M-%S-{unique}-{hostname}.log"
DEFAULT_PARTITION_BY_DATE = True
DEFAULT_ROTATE_EVERY = 3600  # seconds (matches upstream default of 60 minutes)
DEFAULT_MAX_FILE_SIZE = 200_000  # bytes
DEFAULT_UPLOAD_EVERY = 20  # seconds
DEFAULT_BUFFER_SIZE = 10_000  # bytes


class S3StreamLogger(io.RawIOBase):
    """A writable, file-like object that streams data to rotating S3 objects.

    Parameters mirror the options of the Node.js ``s3-streamlogger``
    package as closely as Python conventions allow. All time-based
    options are given in **seconds** (the Node version uses
    milliseconds).

    Parameters
    ----------
    bucket:
        Name of the S3 bucket to upload to. Required. Falls back to the
        ``S3_PYLOGGER_BUCKET`` environment variable if not given.
    folder:
        Optional key prefix ("subfolder") to store objects under.
    partition_by_date:
        If ``True`` (default), Hive-style date partition segments are
        inserted between the folder prefix and the object name::

            {folder}/year=YYYY/month=MM/day=DD/{name}

        This layout is directly compatible with AWS Athena partition
        projection and ``MSCK REPAIR TABLE``.  Set to ``False`` to
        use the legacy flat layout.
    tags:
        Optional dict of S3 object tags to apply to each uploaded object.
    name_format:
        ``strftime``-compatible format string for the object key, with two
        extra placeholders available: ``{unique}`` (a short random id used
        to avoid collisions) and ``{hostname}``. Defaults to
        ``"%Y-%m-%d-%H-%M-%S-{unique}-{hostname}.log"``. If ``compress``
        is true and the format has no extension hint, ``.gz`` is appended.
    rotate_every:
        Rotate to a new object after this many seconds. Default 3600 (1h).
    max_file_size:
        Rotate to a new object once the buffered file reaches this many
        bytes. Default 200,000.
    upload_every:
        Flush the current buffer to S3 at least this often, in seconds.
        Default 20.
    buffer_size:
        Flush immediately once unuploaded data exceeds this many bytes.
        Default 10,000.
    compress:
        If true, gzip the data before uploading. Default False.
    content_type:
        HTTP ``Content-Type`` header set on every uploaded S3 object.
        When ``None`` (default), the type is inferred automatically:
        ``"application/json"`` if the key ends with ``.json``, otherwise
        ``"text/plain; charset=utf-8"``.
        When ``compress=True``, ``ContentEncoding: gzip`` is also set so
        browsers and S3 clients decompress the file transparently.
    server_side_encryption:
        If true, use S3 ``ServerSideEncryption="AES256"``. Default False.
    storage_class:
        S3 storage class (e.g. ``"STANDARD_IA"``). Default: S3's default.
    acl:
        Canned ACL to apply to uploaded objects. Default: none.
    aws_access_key_id:
        AWS access key ID. If omitted, boto3's credential chain is used
        (env vars, ``~/.aws/credentials``, IAM role, etc.).
    aws_secret_access_key:
        AWS secret access key paired with ``aws_access_key_id``.
    region_name:
        AWS region, e.g. ``"us-east-1"``. If omitted, boto3 resolves it
        from the environment or config file.
    s3_client:
        Optional pre-configured ``boto3`` S3 client. When supplied, all
        credential / region arguments above are ignored.
    boto3_kwargs:
        Extra keyword arguments forwarded to ``boto3.client("s3", ...)``.
        Anything set here is overridden by the explicit credential params
        above when both are provided.
    on_error:
        Optional callback ``fn(exception)`` invoked when an upload fails.
        If not given, errors are printed to stderr. As with the Node
        version: do NOT route this back into the same stream/logger, or
        you risk infinite recursion.
    """

    def __init__(
        self,
        bucket: Optional[str] = None,
        *,
        folder: str = "",
        tags: Optional[dict] = None,
        name_format: str = DEFAULT_NAME_FORMAT,
        rotate_every: float = DEFAULT_ROTATE_EVERY,
        max_file_size: int = DEFAULT_MAX_FILE_SIZE,
        upload_every: float = DEFAULT_UPLOAD_EVERY,
        buffer_size: int = DEFAULT_BUFFER_SIZE,
        partition_by_date: bool = DEFAULT_PARTITION_BY_DATE,
        compress: bool = False,
        content_type: Optional[str] = None,
        server_side_encryption: bool = False,
        storage_class: Optional[str] = None,
        acl: Optional[str] = None,
        aws_access_key_id: Optional[str] = None,
        aws_secret_access_key: Optional[str] = None,
        region_name: Optional[str] = None,
        s3_client=None,
        boto3_kwargs: Optional[dict] = None,
        on_error: Optional[Callable[[Exception], None]] = None,
    ):
        super().__init__()

        self.bucket = bucket or os.environ.get("S3_PYLOGGER_BUCKET")
        if not self.bucket:
            raise ValueError(
                "bucket is required (pass bucket=... or set "
                "S3_PYLOGGER_BUCKET)"
            )

        self.folder = folder.strip("/")
        self.tags = tags or {}
        self.name_format = name_format
        self.rotate_every = float(rotate_every)
        self.max_file_size = int(max_file_size)
        self.upload_every = float(upload_every)
        self.buffer_size = int(buffer_size)
        self.partition_by_date = bool(partition_by_date)
        self.compress = bool(compress)
        self.content_type = content_type
        self.server_side_encryption = server_side_encryption
        self.storage_class = storage_class
        self.acl = acl
        self.on_error = on_error

        # Build boto3 client kwargs: start from boto3_kwargs then overlay
        # any explicitly provided credential / region arguments so that
        # direct params always take precedence over the dict.
        _client_kwargs: dict = dict(boto3_kwargs or {})
        if aws_access_key_id is not None:
            _client_kwargs["aws_access_key_id"] = aws_access_key_id
        if aws_secret_access_key is not None:
            _client_kwargs["aws_secret_access_key"] = aws_secret_access_key
        if region_name is not None:
            _client_kwargs["region_name"] = region_name
        self._s3 = s3_client or boto3.client("s3", **_client_kwargs)
        self._hostname = socket.gethostname()

        self._lock = threading.RLock()
        self._buffer = bytearray()
        self._current_key: Optional[str] = None
        self._object_started_at = 0.0
        self._closed = False

        self._stop_event = threading.Event()
        self._worker = threading.Thread(
            target=self._run, name="S3StreamLoggerUploader", daemon=True
        )
        self._worker.start()

    # -- file-like / stream interface ------------------------------------

    def writable(self) -> bool:
        return True

    def write(self, data) -> int:
        """Append data to the buffer. Accepts str or bytes, like a log stream."""
        if self._closed:
            raise ValueError("write to closed S3StreamLogger")
        if isinstance(data, str):
            data = data.encode("utf-8")
        with self._lock:
            if not self._current_key:
                self._start_new_object()
            self._buffer.extend(data)
            should_flush = len(self._buffer) >= self.buffer_size
        if should_flush:
            # Force a rotation so the next batch goes to a fresh S3 key.
            # Without this, successive buffer-size flushes all call put_object
            # on the *same* key, with each upload overwriting the previous one
            # (S3 has no append semantics), causing data loss.
            self._flush(rotate_if_needed=True, force_rotate=True)
        return len(data)

    def flush(self) -> None:
        """Force an upload of any buffered data right now."""
        self._flush(rotate_if_needed=True)

    def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        self._stop_event.set()
        self._worker.join(timeout=self.upload_every + 5)
        # final flush, synchronously, whatever is left
        self._flush(rotate_if_needed=False)
        super().close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        self.close()

    # -- internals ---------------------------------------------------------

    def _run(self):
        while not self._stop_event.wait(self.upload_every):
            self._flush(rotate_if_needed=True)

    def _start_new_object(self):
        unique = uuid.uuid4().hex[:8]
        now = datetime.now(timezone.utc).astimezone()
        name = now.strftime(self.name_format).format(
            unique=unique, hostname=self._hostname
        )
        if self.compress and not name.endswith((".gz", ".gzip")):
            name += ".gz"

        if self.partition_by_date:
            date_partition = (
                f"year={now.year:04d}/"
                f"month={now.month:02d}/"
                f"day={now.day:02d}"
            )
            prefix = f"{self.folder}/{date_partition}" if self.folder else date_partition
        else:
            prefix = self.folder

        key = f"{prefix}/{name}" if prefix else name
        self._current_key = key
        self._object_started_at = time.monotonic()

    def _should_rotate(self, pre_flush_size: int = 0) -> bool:
        if self._current_key is None:
            return False
        age = time.monotonic() - self._object_started_at
        # Use pre_flush_size (captured before buffer.clear()) so the
        # max_file_size check is not always False after clearing.
        return age >= self.rotate_every or pre_flush_size >= self.max_file_size

    def _flush(self, rotate_if_needed: bool, force_rotate: bool = False):
        with self._lock:
            if not self._buffer or self._current_key is None:
                return
            payload = bytes(self._buffer)
            key = self._current_key
            # Capture size *before* clearing so _should_rotate can compare
            # against max_file_size accurately.
            pre_flush_size = len(self._buffer)
            self._buffer.clear()
            if rotate_if_needed and (force_rotate or self._should_rotate(pre_flush_size)):
                self._current_key = None  # next write() starts a fresh object

        self._upload(key, payload)

    def _upload(self, key: str, payload: bytes):
        if self.compress:
            payload = gzip.compress(payload)

        # Determine Content-Type: explicit override > auto-detect from key
        if self.content_type is not None:
            ct = self.content_type
        elif key.endswith(('.json', '.json.gz')):
            ct = 'application/json'
        else:
            ct = 'text/plain; charset=utf-8'

        extra = {'ContentType': ct}
        if self.compress:
            extra['ContentEncoding'] = 'gzip'
        if self.server_side_encryption:
            extra["ServerSideEncryption"] = "AES256"
        if self.storage_class:
            extra["StorageClass"] = self.storage_class
        if self.acl:
            extra["ACL"] = self.acl
        if self.tags:
            extra["Tagging"] = "&".join(f"{k}={v}" for k, v in self.tags.items())

        try:
            self._s3.put_object(Bucket=self.bucket, Key=key, Body=payload, **extra)
        except (BotoCoreError, ClientError) as exc:
            self._handle_error(exc)

    def _handle_error(self, exc: Exception):
        if self.on_error:
            try:
                self.on_error(exc)
            except Exception:  # noqa: BLE001 - never let error handling crash
                pass
        else:
            # Never re-enter logging through this same stream (mirrors the
            # upstream warning about infinite recursion); print to stderr.
            import sys

            print(f"s3_pylogger: upload failed: {exc!r}", file=sys.stderr)


# Fields present on every LogRecord that we handle explicitly in JsonFormatter
# and therefore exclude from the "extra" fields pass-through.
_LOG_RECORD_BUILTIN_ATTRS: frozenset = frozenset({
    "args", "asctime", "created", "exc_info", "exc_text", "filename",
    "funcName", "levelname", "levelno", "lineno", "message", "module",
    "msecs", "msg", "name", "pathname", "process", "processName",
    "relativeCreated", "stack_info", "taskName", "thread", "threadName",
})


class JsonFormatter(logging.Formatter):
    """A ``logging.Formatter`` that serialises each ``LogRecord`` as a
    single-line JSON object — one record per line.

    Core fields emitted
    -------------------
    * ``timestamp`` – ISO-8601 UTC timestamp (``datefmt`` overrides format)
    * ``level``     – e.g. ``"INFO"``, ``"ERROR"``
    * ``logger``    – logger name
    * ``message``   – formatted log message
    * ``exc_info``  – exception traceback string (only when present)
    * ``stack_info``– stack string (only when present)

    Any *extra* keyword arguments passed to the logger call (e.g.
    ``logger.info("msg", extra={"user_id": 42})``) are automatically
    promoted to top-level JSON keys, making them directly queryable in
    Athena.

    The ``timestamp`` field is always formatted as
    ``YYYY-MM-DDTHH:MM:SS.ffffffZ`` (ISO-8601 UTC with microseconds)
    regardless of platform — note that ``%f`` in ``strftime`` is not
    supported on Windows, so the timestamp is built directly from
    :class:`datetime.datetime` instead.
    """

    def __init__(self):
        super().__init__()

    @staticmethod
    def _utc_timestamp(record: logging.LogRecord) -> str:
        """Return an ISO-8601 UTC timestamp with microseconds, cross-platform."""
        dt = datetime.fromtimestamp(record.created, tz=timezone.utc)
        return dt.strftime("%Y-%m-%dT%H:%M:%S.") + f"{dt.microsecond:06d}Z"

    def format(self, record: logging.LogRecord) -> str:  # type: ignore[override]
        record.message = record.getMessage()

        data: dict = {
            "timestamp": self._utc_timestamp(record),
            "level": record.levelname,
            "logger": record.name,
            "message": record.message,
        }

        if record.exc_info:
            data["exc_info"] = self.formatException(record.exc_info)
        if record.stack_info:
            data["stack_info"] = self.formatStack(record.stack_info)

        # Promote any caller-supplied `extra` fields to top-level keys.
        for key, val in record.__dict__.items():
            if key in _LOG_RECORD_BUILTIN_ATTRS or key.startswith("_"):
                continue
            try:
                json.dumps(val)  # guard against non-serialisable values
                data[key] = val
            except (TypeError, ValueError):
                data[key] = str(val)

        return json.dumps(data, ensure_ascii=False)


class S3StreamHandler(logging.Handler):
    """A ``logging.Handler`` that streams formatted records to S3 via
    ``S3StreamLogger``. This is the Python equivalent of wiring
    ``s3-streamlogger`` into a Winston ``Stream`` transport.

    Parameters
    ----------
    log_format:
        Controls the formatter applied to every log record before it is
        written to S3.

        ``"text"`` *(default)*
            Uses whatever formatter you attach with ``setFormatter()``;
            falls back to Python's default ``logging.Formatter``.

        ``"json"``
            Automatically attaches :class:`JsonFormatter` so every record
            is written as a single-line JSON object::

                {"timestamp": "2026-09-23T17:30:01.234000Z",
                 "level": "INFO",
                 "logger": "myapp",
                 "message": "hello S3"}

            This layout maps perfectly to an Athena table created with
            ``ROW FORMAT SERDE 'org.openx.data.JsonSerDe'`` and gives you
            per-field filtering without any regex parsing.
    stream_logger:
        Optional pre-built :class:`S3StreamLogger` instance. When given,
        all other positional / keyword arguments are ignored.

    All remaining ``*args`` and ``**kwargs`` are forwarded verbatim to
    :class:`S3StreamLogger` (``bucket``, ``folder``, ``compress``, etc.).

    Examples
    --------
    Plain-text (default)::

        import logging
        from s3_pylogger import S3StreamHandler

        handler = S3StreamHandler(bucket="my-bucket", folder="myapp/logs")
        logger = logging.getLogger("myapp")
        logger.addHandler(handler)
        logger.setLevel(logging.INFO)
        logger.info("hello S3")

    JSON (Athena-friendly)::

        handler = S3StreamHandler(
            bucket="my-bucket",
            folder="myapp/logs",
            log_format="json",
        )
        logger.addHandler(handler)
        logger.info("user signed in", extra={"user_id": 42, "ip": "1.2.3.4"})
        # -> {"timestamp": "...", "level": "INFO", "logger": "myapp",
        #     "message": "user signed in", "user_id": 42, "ip": "1.2.3.4"}
    """

    _VALID_FORMATS = frozenset({"text", "json"})

    def __init__(
        self,
        *args,
        stream_logger: Optional[S3StreamLogger] = None,
        log_format: str = "text",
        **kwargs,
    ):
        if log_format not in self._VALID_FORMATS:
            raise ValueError(
                f"log_format must be one of {sorted(self._VALID_FORMATS)!r}, "
                f"got {log_format!r}"
            )
        super().__init__()
        self.stream = stream_logger or S3StreamLogger(*args, **kwargs)
        if log_format == "json":
            self.setFormatter(JsonFormatter())

    def emit(self, record: logging.LogRecord) -> None:
        try:
            msg = self.format(record) + "\n"
            self.stream.write(msg)
        except Exception:
            self.handleError(record)

    def close(self) -> None:
        try:
            self.stream.close()
        finally:
            super().close()
