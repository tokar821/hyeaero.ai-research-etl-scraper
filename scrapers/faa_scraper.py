"""FAA Aircraft Registration Database Scraper.

Downloads the FAA Releasable Aircraft Registration Database and documentation,
verifies file integrity, and counts records.
"""

import hashlib
import zipfile
from datetime import datetime
from pathlib import Path
from typing import Optional, Dict, Tuple
from urllib.parse import urljoin

import requests
from requests.adapters import HTTPAdapter
try:
    from urllib3.util.retry import Retry
except ImportError:
    from requests.packages.urllib3.util.retry import Retry

from utils.logger import get_logger

logger = get_logger(__name__)


class FAAScraperError(Exception):
    """Base exception for FAA scraper."""
    pass


class FAADownloadError(FAAScraperError):
    """Raised when download fails."""
    pass


class FAAVerificationError(FAAScraperError):
    """Raised when file verification fails."""
    pass


class FAAScraper:
    """Scraper for FAA Aircraft Registration Database."""
    
    BASE_URL = "https://www.faa.gov"
    DOWNLOAD_PAGE = "/licenses_certificates/aircraft_certification/aircraft_registry/releasable_aircraft_download"
    
    # Expected files in the ZIP archive
    EXPECTED_FILES = [
        "MASTER.txt",
        "DEALER.txt",
        "DOCINDEX.txt",
        "ACFTREF.txt",
        "DEREG.txt",
        "ENGINE.txt",
        "RESERVE.txt",
    ]
    
    def __init__(self, storage_base_path: Optional[Path] = None):
        """Initialize FAA scraper.
        
        Args:
            storage_base_path: Base path for local storage. If None, uses './store'.
        """
        if storage_base_path is None:
            storage_base_path = Path(__file__).parent.parent / "store"
        
        self.storage_base_path = Path(storage_base_path)
        self.raw_faa_path = self.storage_base_path / "raw" / "faa"
        
        # Create directories if they don't exist
        self.raw_faa_path.mkdir(parents=True, exist_ok=True)
        
        # Setup session with retries
        self.session = self._create_session()
        
    def _create_session(self) -> requests.Session:
        """Create requests session with retry strategy."""
        session = requests.Session()
        
        retry_strategy = Retry(
            total=3,
            backoff_factor=1,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["GET", "HEAD"]
        )
        
        adapter = HTTPAdapter(max_retries=retry_strategy)
        session.mount("http://", adapter)
        session.mount("https://", adapter)
        
        session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        })
        
        return session
    
    def _get_download_urls(self) -> Tuple[str, Optional[str]]:
        """Get download URLs for database and documentation.
        
        Returns:
            Tuple of (database_url, documentation_url).
            Documentation URL may be None if not found.
        """
        try:
            page_url = urljoin(self.BASE_URL, self.DOWNLOAD_PAGE)
            logger.info(f"Fetching download page: {page_url}")
            
            response = self.session.get(page_url, timeout=30)
            response.raise_for_status()
            
            # Parse HTML to find download links
            from bs4 import BeautifulSoup
            soup = BeautifulSoup(response.content, 'html.parser')
            
            database_url = None
            doc_url = None
            
            # Find links - look for text containing "Download the Aircraft Registration Database"
            for link in soup.find_all('a', href=True):
                href = link.get('href', '')
                link_text = link.get_text(strip=True)
                text_lower = link_text.lower()
                
                # Database download link - look for text like "Download the Aircraft Registration Database"
                if not database_url:
                    if ('download' in text_lower and 'aircraft registration database' in text_lower) or \
                       ('aircraft registration database' in text_lower and '60mb' in text_lower):
                        if href.endswith('.zip') or not href.startswith('#'):
                            database_url = urljoin(page_url, href) if not href.startswith('http') else href
                            logger.info(f"Found database download URL: {database_url}")
                
                # Documentation link
                if not doc_url and 'documentation' in text_lower:
                    if href.endswith('.pdf') or 'documentation' in href.lower():
                        doc_url = urljoin(page_url, href) if not href.startswith('http') else href
                        logger.info(f"Found documentation URL: {doc_url}")
            
            # Fallback: look for any ZIP file links on the page
            if not database_url:
                logger.warning("Could not find database URL by text, searching for ZIP links...")
                for link in soup.find_all('a', href=True):
                    href = link.get('href', '')
                    if href.endswith('.zip'):
                        database_url = urljoin(page_url, href) if not href.startswith('http') else href
                        logger.info(f"Found ZIP file URL: {database_url}")
                        break
            
            if not database_url:
                raise FAADownloadError(
                    "Could not find database download URL on the page. "
                    "The page structure may have changed."
                )
                
            return database_url, doc_url
            
        except Exception as e:
            logger.error(f"Error getting download URLs: {e}")
            raise FAADownloadError(f"Failed to get download URLs: {e}") from e
    
    def _download_file(
        self,
        url: str,
        filepath: Path,
        chunk_size: int = 8192
    ) -> Tuple[int, str]:
        """Download a file with progress tracking.
        
        Args:
            url: URL to download from.
            filepath: Local path to save file.
            chunk_size: Chunk size for streaming download.
            
        Returns:
            Tuple of (file_size_bytes, md5_hash).
            
        Raises:
            FAADownloadError: If download fails.
        """
        try:
            logger.info(f"Downloading: {url}")
            logger.info(f"Destination: {filepath}")
            
            response = self.session.get(url, stream=True, timeout=60)
            response.raise_for_status()
            
            total_size = int(response.headers.get('content-length', 0))
            downloaded = 0
            
            md5_hash = hashlib.md5()
            
            with open(filepath, 'wb') as f:
                for chunk in response.iter_content(chunk_size=chunk_size):
                    if chunk:
                        f.write(chunk)
                        md5_hash.update(chunk)
                        downloaded += len(chunk)
                        
                        if total_size > 0:
                            percent = (downloaded / total_size) * 100
                            if downloaded % (chunk_size * 100) == 0:  # Log every 100 chunks
                                logger.info(f"Progress: {downloaded:,} / {total_size:,} bytes ({percent:.1f}%)")
            
            file_hash = md5_hash.hexdigest()
            actual_size = filepath.stat().st_size
            
            logger.info(f"Download complete: {actual_size:,} bytes")
            logger.info(f"MD5 hash: {file_hash}")
            
            if total_size > 0 and actual_size != total_size:
                logger.warning(f"Size mismatch: expected {total_size:,}, got {actual_size:,}")
            
            return actual_size, file_hash
            
        except Exception as e:
            if filepath.exists():
                filepath.unlink()  # Clean up partial download
            logger.error(f"Error downloading {url}: {e}")
            raise FAADownloadError(f"Failed to download {url}: {e}") from e
    
    def _extract_zip(self, zip_path: Path, extract_to: Path) -> Dict[str, Path]:
        """Extract ZIP file and verify contents.
        
        Args:
            zip_path: Path to ZIP file.
            extract_to: Directory to extract to.
            
        Returns:
            Dictionary mapping filename to extracted file path.
            
        Raises:
            FAAVerificationError: If extraction or verification fails.
        """
        try:
            extract_to.mkdir(parents=True, exist_ok=True)
            
            logger.info(f"Extracting ZIP: {zip_path}")
            
            extracted_files = {}
            
            with zipfile.ZipFile(zip_path, 'r') as zip_ref:
                # Verify ZIP is valid
                if zip_ref.testzip():
                    raise FAAVerificationError("ZIP file is corrupted")
                
                # List contents
                file_list = zip_ref.namelist()
                logger.info(f"ZIP contains {len(file_list)} files")
                
                # Extract all files
                zip_ref.extractall(extract_to)
                
                # Map extracted files
                for filename in file_list:
                    extracted_path = extract_to / filename
                    if extracted_path.is_file():
                        extracted_files[filename] = extracted_path
                        file_size = extracted_path.stat().st_size
                        logger.info(f"  Extracted: {filename} ({file_size:,} bytes)")
            
            logger.info(f"Extraction complete: {len(extracted_files)} files")
            return extracted_files
            
        except zipfile.BadZipFile as e:
            raise FAAVerificationError(f"Invalid ZIP file: {e}") from e
        except Exception as e:
            logger.error(f"Error extracting ZIP: {e}")
            raise FAAVerificationError(f"Failed to extract ZIP: {e}") from e
    
    def _count_records(self, filepath: Path) -> int:
        """Count records in a file (number of lines).
        
        Args:
            filepath: Path to file.
            
        Returns:
            Number of records (lines) in file.
        """
        try:
            count = 0
            with open(filepath, 'r', encoding='utf-8', errors='ignore') as f:
                for line in f:
                    count += 1
            return count
        except Exception as e:
            logger.error(f"Error counting records in {filepath}: {e}")
            return 0
    
    def download_database(
        self,
        date: Optional[datetime] = None,
        download_docs: bool = True
    ) -> Dict:
        """Download FAA Aircraft Registration Database.
        
        Args:
            date: Date for storage path. If None, uses current date.
            download_docs: Whether to download documentation.
            
        Returns:
            Dictionary with download statistics:
            - date: Date string
            - database_file: Path to ZIP file
            - database_size: Size in bytes
            - database_hash: MD5 hash
            - extracted_files: Dict of extracted files
            - record_counts: Dict of record counts per file
            - documentation_file: Optional path to documentation
            - documentation_size: Optional size in bytes
        """
        if date is None:
            date = datetime.now()
        
        date_str = date.strftime("%Y-%m-%d")
        output_dir = self.raw_faa_path / date_str
        output_dir.mkdir(parents=True, exist_ok=True)
        
        logger.info("=" * 60)
        logger.info("FAA Aircraft Registration Database Scraper")
        logger.info(f"Date: {date_str}")
        logger.info(f"Output directory: {output_dir}")
        logger.info("=" * 60)
        
        result = {
            "date": date_str,
            "database_file": None,
            "database_size": 0,
            "database_hash": None,
            "extracted_files": {},
            "record_counts": {},
            "documentation_file": None,
            "documentation_size": 0,
        }
        
        try:
            # Get download URLs
            db_url, doc_url = self._get_download_urls()
            
            # Download database
            zip_filename = f"FAA_Aircraft_Registration_Database_{date_str}.zip"
            zip_path = output_dir / zip_filename
            
            logger.info("Starting database download...")
            db_size, db_hash = self._download_file(db_url, zip_path)
            
            result["database_file"] = str(zip_path)
            result["database_size"] = db_size
            result["database_hash"] = db_hash
            
            # Extract ZIP
            extract_dir = output_dir / "extracted"
            extracted_files = self._extract_zip(zip_path, extract_dir)
            result["extracted_files"] = {k: str(v) for k, v in extracted_files.items()}
            
            # Count records in each file
            logger.info("Counting records...")
            for filename, filepath in extracted_files.items():
                count = self._count_records(filepath)
                result["record_counts"][filename] = count
                logger.info(f"  {filename}: {count:,} records")
            
            # Download documentation if requested
            if download_docs and doc_url:
                logger.info("Downloading documentation...")
                doc_filename = f"FAA_Database_Documentation_{date_str}.pdf"
                doc_path = output_dir / doc_filename
                
                try:
                    doc_size, _ = self._download_file(doc_url, doc_path)
                    result["documentation_file"] = str(doc_path)
                    result["documentation_size"] = doc_size
                except Exception as e:
                    logger.warning(f"Could not download documentation: {e}")
            
            # Summary
            total_records = sum(result["record_counts"].values())
            logger.info("=" * 60)
            logger.info("Download Summary")
            logger.info(f"Database file: {zip_path.name}")
            logger.info(f"Database size: {db_size:,} bytes ({db_size / 1024 / 1024:.2f} MB)")
            logger.info(f"Database hash: {db_hash}")
            logger.info(f"Extracted files: {len(extracted_files)}")
            logger.info(f"Total records: {total_records:,}")
            logger.info("=" * 60)
            
            return result
            
        except Exception as e:
            logger.error(f"FAA scraper failed: {e}", exc_info=True)
            raise


def main():
    """Main entry point for FAA scraper."""
    from utils.logger import setup_logging
    
    setup_logging()
    logger = get_logger(__name__)
    
    try:
        scraper = FAAScraper()
        result = scraper.download_database(download_docs=True)
        
        logger.info("FAA scraper completed successfully")
        return result
        
    except Exception as e:
        logger.error(f"FAA scraper failed: {e}", exc_info=True)
        raise


if __name__ == "__main__":
    main()
