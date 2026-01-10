"""Akamai Object Storage client wrapper (S3-compatible API).

Provides a clean interface for storing raw scraped data and snapshots
with proper path conventions and error handling.
"""

import logging
from datetime import datetime
from pathlib import Path
from typing import Optional, BinaryIO, Union
from io import BytesIO

import boto3
from botocore.exceptions import ClientError, BotoCoreError
from botocore.config import Config as BotoConfig

from config.config_loader import get_config

logger = logging.getLogger(__name__)


class StorageError(Exception):
    """Base exception for storage operations."""
    pass


class StorageConnectionError(StorageError):
    """Raised when connection to storage fails."""
    pass


class StorageUploadError(StorageError):
    """Raised when file upload fails."""
    pass


class StorageDownloadError(StorageError):
    """Raised when file download fails."""
    pass


class AkamaiStorageClient:
    """Client for interacting with Akamai Object Storage (S3-compatible).
    
    Handles path conventions:
    - raw/{source}/{YYYY-MM-DD}/
    - snapshots/{entity}/{YYYY-MM-DD}/
    """

    def __init__(self, config=None):
        """Initialize Akamai storage client.
        
        Args:
            config: Optional config instance. If None, loads from get_config().
        """
        if config is None:
            config = get_config()
        
        self.config = config
        self.dry_run = config.is_dry_run()
        
        if self.dry_run:
            logger.info("Storage client initialized in DRY-RUN mode - no actual uploads will occur")
            self.s3_client = None
        else:
            try:
                # Configure boto3 for S3-compatible endpoint
                boto_config = BotoConfig(
                    signature_version='s3v4',
                    retries={'max_attempts': 3, 'mode': 'standard'}
                )
                
                self.s3_client = boto3.client(
                    's3',
                    endpoint_url=config.akamai.endpoint,
                    aws_access_key_id=config.akamai.access_key,
                    aws_secret_access_key=config.akamai.secret_key,
                    region_name=config.akamai.region or 'us-east-1',
                    config=boto_config
                )
                
                # Test connection
                self._verify_connection()
                logger.info(
                    f"Storage client initialized successfully for bucket: {config.akamai.bucket_name}"
                )
            except Exception as e:
                logger.error(f"Failed to initialize storage client: {e}")
                raise StorageConnectionError(f"Failed to connect to Akamai storage: {e}") from e

    def _verify_connection(self) -> None:
        """Verify connection to storage bucket.
        
        Raises:
            StorageConnectionError: If connection verification fails.
        """
        try:
            self.s3_client.head_bucket(Bucket=self.config.akamai.bucket_name)
        except ClientError as e:
            error_code = e.response.get('Error', {}).get('Code', 'Unknown')
            raise StorageConnectionError(
                f"Failed to verify bucket access: {error_code}"
            ) from e
        except BotoCoreError as e:
            raise StorageConnectionError(
                f"Connection error: {e}"
            ) from e

    def _build_raw_path(self, source: str, date: Optional[datetime] = None) -> str:
        """Build path for raw data storage.
        
        Args:
            source: Data source name (e.g., 'controller', 'aircraftexchange', 'faa')
            date: Optional date. If None, uses current date.
            
        Returns:
            Path string in format: raw/{source}/{YYYY-MM-DD}/
        """
        if date is None:
            date = datetime.now()
        date_str = date.strftime("%Y-%m-%d")
        return f"raw/{source}/{date_str}/"

    def _build_snapshot_path(self, entity: str, date: Optional[datetime] = None) -> str:
        """Build path for snapshot storage.
        
        Args:
            entity: Entity name (e.g., 'aircraft', 'listing', 'valuation')
            date: Optional date. If None, uses current date.
            
        Returns:
            Path string in format: snapshots/{entity}/{YYYY-MM-DD}/
        """
        if date is None:
            date = datetime.now()
        date_str = date.strftime("%Y-%m-%d")
        return f"snapshots/{entity}/{date_str}/"

    def upload_raw_data(
        self,
        source: str,
        filename: str,
        data: Union[bytes, BinaryIO, BytesIO],
        date: Optional[datetime] = None,
        content_type: Optional[str] = None
    ) -> str:
        """Upload raw scraped data to storage.
        
        Args:
            source: Data source name (e.g., 'controller', 'aircraftexchange')
            filename: Name of the file to store
            data: File data (bytes, file-like object, or BytesIO)
            date: Optional date for path. If None, uses current date.
            content_type: Optional MIME type (e.g., 'application/json', 'text/html')
            
        Returns:
            Full S3 key path where file was stored.
            
        Raises:
            StorageUploadError: If upload fails.
        """
        base_path = self._build_raw_path(source, date)
        s3_key = f"{base_path}{filename}"
        
        if self.dry_run:
            logger.info(f"[DRY-RUN] Would upload to: s3://{self.config.akamai.bucket_name}/{s3_key}")
            return s3_key
        
        try:
            # Convert data to bytes if needed
            if isinstance(data, (BytesIO, BinaryIO)):
                data.seek(0)
                body = data.read()
            elif isinstance(data, bytes):
                body = data
            else:
                raise ValueError(f"Unsupported data type: {type(data)}")
            
            extra_args = {}
            if content_type:
                extra_args['ContentType'] = content_type
            
            self.s3_client.put_object(
                Bucket=self.config.akamai.bucket_name,
                Key=s3_key,
                Body=body,
                **extra_args
            )
            
            logger.info(f"Successfully uploaded raw data: {s3_key}")
            return s3_key
            
        except ClientError as e:
            error_code = e.response.get('Error', {}).get('Code', 'Unknown')
            error_msg = e.response.get('Error', {}).get('Message', str(e))
            logger.error(f"Failed to upload {s3_key}: {error_code} - {error_msg}")
            raise StorageUploadError(
                f"Upload failed for {s3_key}: {error_code} - {error_msg}"
            ) from e
        except BotoCoreError as e:
            logger.error(f"Storage error during upload of {s3_key}: {e}")
            raise StorageUploadError(f"Storage error: {e}") from e
        except Exception as e:
            logger.error(f"Unexpected error during upload of {s3_key}: {e}")
            raise StorageUploadError(f"Unexpected error: {e}") from e

    def upload_snapshot(
        self,
        entity: str,
        filename: str,
        data: Union[bytes, BinaryIO, BytesIO],
        date: Optional[datetime] = None,
        content_type: Optional[str] = None
    ) -> str:
        """Upload snapshot data to storage.
        
        Args:
            entity: Entity name (e.g., 'aircraft', 'listing')
            filename: Name of the file to store
            data: File data (bytes, file-like object, or BytesIO)
            date: Optional date for path. If None, uses current date.
            content_type: Optional MIME type (e.g., 'application/json')
            
        Returns:
            Full S3 key path where file was stored.
            
        Raises:
            StorageUploadError: If upload fails.
        """
        base_path = self._build_snapshot_path(entity, date)
        s3_key = f"{base_path}{filename}"
        
        if self.dry_run:
            logger.info(f"[DRY-RUN] Would upload snapshot to: s3://{self.config.akamai.bucket_name}/{s3_key}")
            return s3_key
        
        try:
            # Convert data to bytes if needed
            if isinstance(data, (BytesIO, BinaryIO)):
                data.seek(0)
                body = data.read()
            elif isinstance(data, bytes):
                body = data
            else:
                raise ValueError(f"Unsupported data type: {type(data)}")
            
            extra_args = {}
            if content_type:
                extra_args['ContentType'] = content_type
            
            self.s3_client.put_object(
                Bucket=self.config.akamai.bucket_name,
                Key=s3_key,
                Body=body,
                **extra_args
            )
            
            logger.info(f"Successfully uploaded snapshot: {s3_key}")
            return s3_key
            
        except ClientError as e:
            error_code = e.response.get('Error', {}).get('Code', 'Unknown')
            error_msg = e.response.get('Error', {}).get('Message', str(e))
            logger.error(f"Failed to upload snapshot {s3_key}: {error_code} - {error_msg}")
            raise StorageUploadError(
                f"Snapshot upload failed for {s3_key}: {error_code} - {error_msg}"
            ) from e
        except BotoCoreError as e:
            logger.error(f"Storage error during snapshot upload of {s3_key}: {e}")
            raise StorageUploadError(f"Storage error: {e}") from e
        except Exception as e:
            logger.error(f"Unexpected error during snapshot upload of {s3_key}: {e}")
            raise StorageUploadError(f"Unexpected error: {e}") from e

    def download_file(self, s3_key: str) -> bytes:
        """Download a file from storage.
        
        Args:
            s3_key: Full S3 key path of the file to download.
            
        Returns:
            File contents as bytes.
            
        Raises:
            StorageDownloadError: If download fails.
        """
        if self.dry_run:
            logger.info(f"[DRY-RUN] Would download: s3://{self.config.akamai.bucket_name}/{s3_key}")
            return b""
        
        try:
            response = self.s3_client.get_object(
                Bucket=self.config.akamai.bucket_name,
                Key=s3_key
            )
            data = response['Body'].read()
            logger.info(f"Successfully downloaded: {s3_key}")
            return data
            
        except ClientError as e:
            error_code = e.response.get('Error', {}).get('Code', 'Unknown')
            if error_code == 'NoSuchKey':
                raise StorageDownloadError(f"File not found: {s3_key}") from e
            error_msg = e.response.get('Error', {}).get('Message', str(e))
            logger.error(f"Failed to download {s3_key}: {error_code} - {error_msg}")
            raise StorageDownloadError(
                f"Download failed for {s3_key}: {error_code} - {error_msg}"
            ) from e
        except BotoCoreError as e:
            logger.error(f"Storage error during download of {s3_key}: {e}")
            raise StorageDownloadError(f"Storage error: {e}") from e
        except Exception as e:
            logger.error(f"Unexpected error during download of {s3_key}: {e}")
            raise StorageDownloadError(f"Unexpected error: {e}") from e

    def list_files(self, prefix: str) -> list[str]:
        """List files in storage with given prefix.
        
        Args:
            prefix: Path prefix to filter files.
            
        Returns:
            List of S3 keys matching the prefix.
            
        Raises:
            StorageError: If listing fails.
        """
        if self.dry_run:
            logger.info(f"[DRY-RUN] Would list files with prefix: {prefix}")
            return []
        
        try:
            response = self.s3_client.list_objects_v2(
                Bucket=self.config.akamai.bucket_name,
                Prefix=prefix
            )
            
            if 'Contents' not in response:
                return []
            
            keys = [obj['Key'] for obj in response['Contents']]
            logger.info(f"Found {len(keys)} files with prefix: {prefix}")
            return keys
            
        except ClientError as e:
            error_code = e.response.get('Error', {}).get('Code', 'Unknown')
            error_msg = e.response.get('Error', {}).get('Message', str(e))
            logger.error(f"Failed to list files with prefix {prefix}: {error_code} - {error_msg}")
            raise StorageError(f"List failed: {error_code} - {error_msg}") from e
        except BotoCoreError as e:
            logger.error(f"Storage error during list operation: {e}")
            raise StorageError(f"Storage error: {e}") from e
