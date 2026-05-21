from __future__ import annotations

import hashlib
import os
from dataclasses import dataclass
from pathlib import Path, PurePosixPath
from typing import Any
from urllib.parse import quote


@dataclass(frozen=True)
class StoredFile:
    provider: str
    key: str
    original_filename: str
    content_type: str
    size_bytes: int
    checksum_sha256: str


class FileStorageError(RuntimeError):
    pass


class FileStorage:
    provider = "local"

    def save_bytes(self, key: str, data: bytes, *, original_filename: str = "", content_type: str = "") -> StoredFile:
        raise NotImplementedError

    def read_bytes(self, key: str) -> bytes:
        raise NotImplementedError

    def file_exists(self, key: str) -> bool:
        raise NotImplementedError

    def delete_file(self, key: str) -> None:
        raise NotImplementedError

    def get_file_url(
        self,
        key: str,
        *,
        filename: str = "",
        content_type: str = "",
        disposition: str = "inline",
    ) -> str | None:
        return None


def normalize_storage_key(key: str) -> str:
    cleaned = key.replace("\\", "/").strip().lstrip("/")
    path = PurePosixPath(cleaned)
    parts = [part for part in path.parts if part not in ("", ".", "..")]
    if not parts:
        raise FileStorageError("Empty storage key")
    return "/".join(parts)


def checksum_sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


class LocalFileStorage(FileStorage):
    provider = "local"

    def __init__(self, root: Path):
        self.root = root.expanduser().resolve()
        self.root.mkdir(parents=True, exist_ok=True)

    def _path_for_key(self, key: str) -> Path:
        normalized = normalize_storage_key(key)
        path = (self.root / normalized).resolve()
        if self.root not in path.parents and path != self.root:
            raise FileStorageError("Forbidden storage key")
        return path

    def save_bytes(self, key: str, data: bytes, *, original_filename: str = "", content_type: str = "") -> StoredFile:
        normalized = normalize_storage_key(key)
        path = self._path_for_key(normalized)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(data)
        return StoredFile(
            provider=self.provider,
            key=normalized,
            original_filename=original_filename,
            content_type=content_type,
            size_bytes=len(data),
            checksum_sha256=checksum_sha256(data),
        )

    def read_bytes(self, key: str) -> bytes:
        return self._path_for_key(key).read_bytes()

    def file_exists(self, key: str) -> bool:
        return self._path_for_key(key).is_file()

    def delete_file(self, key: str) -> None:
        path = self._path_for_key(key)
        if path.exists():
            path.unlink()

    def local_path(self, key: str) -> Path:
        return self._path_for_key(key)


class S3FileStorage(FileStorage):
    provider = "s3"

    def __init__(self) -> None:
        self.bucket = os.getenv("S3_BUCKET", "").strip()
        if not self.bucket:
            raise FileStorageError("S3_BUCKET is required for S3 file storage")
        self.key_prefix = normalize_storage_key(os.getenv("S3_KEY_PREFIX", "crm").strip() or "crm")
        self.presigned_ttl = int(os.getenv("S3_PRESIGNED_URL_TTL_SECONDS", "300") or "300")
        try:
            import boto3
            from botocore.config import Config
        except ImportError as exc:
            raise FileStorageError("boto3 is required for S3 file storage") from exc
        endpoint_url = os.getenv("S3_ENDPOINT_URL", "").strip() or None
        region_name = os.getenv("S3_REGION", "").strip() or None
        access_key = os.getenv("S3_ACCESS_KEY_ID", "").strip() or None
        secret_key = os.getenv("S3_SECRET_ACCESS_KEY", "").strip() or None
        addressing_style = os.getenv("S3_ADDRESSING_STYLE", "path").strip() or "path"
        self._client = boto3.client(
            "s3",
            endpoint_url=endpoint_url,
            region_name=region_name,
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
            config=Config(signature_version="s3v4", s3={"addressing_style": addressing_style}),
        )

    def _full_key(self, key: str) -> str:
        return normalize_storage_key(f"{self.key_prefix}/{normalize_storage_key(key)}")

    @staticmethod
    def _not_found(exc: Exception) -> bool:
        response = getattr(exc, "response", {})
        code = str(response.get("Error", {}).get("Code", ""))
        status = str(response.get("ResponseMetadata", {}).get("HTTPStatusCode", ""))
        return code in {"404", "NoSuchKey", "NotFound"} or status == "404"

    def save_bytes(self, key: str, data: bytes, *, original_filename: str = "", content_type: str = "") -> StoredFile:
        normalized = normalize_storage_key(key)
        full_key = self._full_key(normalized)
        metadata: dict[str, str] = {}
        if original_filename:
            metadata["original-filename"] = original_filename
        metadata["sha256"] = checksum_sha256(data)
        put_kwargs: dict[str, Any] = {
            "Bucket": self.bucket,
            "Key": full_key,
            "Body": data,
            "Metadata": metadata,
        }
        if content_type:
            put_kwargs["ContentType"] = content_type
        self._client.put_object(**put_kwargs)
        return StoredFile(
            provider=self.provider,
            key=normalized,
            original_filename=original_filename,
            content_type=content_type,
            size_bytes=len(data),
            checksum_sha256=metadata["sha256"],
        )

    def read_bytes(self, key: str) -> bytes:
        try:
            response = self._client.get_object(Bucket=self.bucket, Key=self._full_key(key))
        except Exception as exc:
            if self._not_found(exc):
                raise FileNotFoundError(key) from exc
            raise
        return response["Body"].read()

    def file_exists(self, key: str) -> bool:
        try:
            self._client.head_object(Bucket=self.bucket, Key=self._full_key(key))
            return True
        except Exception as exc:
            if self._not_found(exc):
                return False
            raise

    def delete_file(self, key: str) -> None:
        self._client.delete_object(Bucket=self.bucket, Key=self._full_key(key))

    def get_file_url(
        self,
        key: str,
        *,
        filename: str = "",
        content_type: str = "",
        disposition: str = "inline",
    ) -> str | None:
        params: dict[str, Any] = {"Bucket": self.bucket, "Key": self._full_key(key)}
        if content_type:
            params["ResponseContentType"] = content_type
        if filename:
            ascii_filename = "".join(ch if ord(ch) < 128 and ch not in {'"', "\\"} else "_" for ch in filename) or "file"
            params["ResponseContentDisposition"] = (
                f'{disposition}; filename="{ascii_filename}"; filename*=UTF-8\'\'{quote(filename, safe="")}'
            )
        return self._client.generate_presigned_url("get_object", Params=params, ExpiresIn=self.presigned_ttl)


def configured_storage_provider() -> str:
    provider = os.getenv("FILE_STORAGE_PROVIDER", "local").strip().lower()
    if provider == "yandex":
        return "s3"
    return provider or "local"


def create_file_storage(local_root: Path, provider: str | None = None) -> FileStorage:
    selected = (provider or configured_storage_provider()).strip().lower()
    if selected in {"", "local"}:
        return LocalFileStorage(local_root)
    if selected in {"s3", "yandex"}:
        return S3FileStorage()
    raise FileStorageError(f"Unsupported file storage provider: {selected}")
