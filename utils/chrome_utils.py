"""Shared utilities for undetected-chromedriver scrapers.

- Chrome version detection (to match ChromeDriver)
- Safe driver cleanup (suppress harmless shutdown errors)
"""

import os
import re
import subprocess
from typing import Optional


def get_chrome_version() -> Optional[int]:
    """Detect installed Chrome major version to match ChromeDriver.

    Returns:
        Chrome major version (e.g. 143) or None if detection fails.
    """
    try:
        try:
            import sys
            if sys.platform == "win32":
                import winreg
                key = winreg.OpenKey(
                    winreg.HKEY_CURRENT_USER,
                    r"Software\Google\Chrome\BLBeacon"
                )
                version = winreg.QueryValueEx(key, "version")[0]
                winreg.CloseKey(key)
                m = re.search(r"(\d+)\.", version)
                if m:
                    return int(m.group(1))
        except (OSError, Exception):
            pass

        paths = [
            r"C:\Program Files\Google\Chrome\Application\chrome.exe",
            r"C:\Program Files (x86)\Google\Chrome\Application\chrome.exe",
            r"%LOCALAPPDATA%\Google\Chrome\Application\chrome.exe",
        ]
        for p in paths:
            try:
                exe = os.path.expandvars(p)
                if os.path.exists(exe):
                    r = subprocess.run(
                        [exe, "--version"],
                        capture_output=True,
                        text=True,
                        timeout=5
                    )
                    if r.returncode == 0:
                        m = re.search(r"(\d+)\.", r.stdout)
                        if m:
                            return int(m.group(1))
            except (subprocess.TimeoutExpired, FileNotFoundError, OSError):
                continue
        return None
    except Exception:
        return None


def safe_driver_quit(driver) -> None:
    """Quit Chrome driver and suppress cleanup errors.

    Avoids 'Exception ignored' / 'handle is invalid' messages during GC.
    """
    if driver is None:
        return
    try:
        driver.quit()
    except Exception:
        pass
