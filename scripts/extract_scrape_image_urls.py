"""
Walk store/raw HTML per marketplace platform and emit one JSON file per platform
with image URLs extracted from each saved page.
"""
from __future__ import annotations

import argparse
import html
import json
import re
from collections import OrderedDict
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable
from urllib.parse import unquote, urlparse

from bs4 import BeautifulSoup

Extractor = Callable[[str], dict[str, list[str] | str]]

SKIP_SUBSTR = (
    "gstatic.com",
    "googletagmanager.com",
    "google-analytics.com",
    "facebook.com",
    "doubleclick.net",
    "aircraftexchange.com/icns/",
    "aircraftexchange.com/img/aircraft_exchange_logo",
    "aircraftexchange.com/img/hamburger",
    "aircraftexchange.com/img/iada-logo",
    "recaptcha/",
    "google.com/recaptcha",
)

HTTP_IMG_RE = re.compile(
    r"https?://[^\s\"'<>]+?\.(?:jpg|jpeg|png|webp|gif)(?:\?[^\s\"'<>]*)?",
    re.IGNORECASE,
)

CONTROLLER_JSON_URL_RE = re.compile(
    r'"(?:MediaUrl|FullScreenUrl)"\s*:\s*"(https://media\.sandhills\.com/img\.axd[^"]*)"',
)


def normalize_url(u: str) -> str:
    u = html.unescape((u or "").strip())
    u = u.replace("\\u0026", "&").replace("\\u002f", "/")
    return u


def should_skip_url(u: str) -> bool:
    if not u or not u.startswith("http"):
        return True
    low = u.lower()
    if any(s in low for s in SKIP_SUBSTR):
        return True
    try:
        parsed = urlparse(u)
        host = (parsed.netloc or "").lower()
        path = unquote(parsed.path or "").lower()
    except Exception:
        return False

    # Site default / marketing previews (not listing photos).
    if host.endswith("aircraftexchange.com") and "open_graph_default" in path:
        return True

    # AircraftExchange marketing CDN: banners and IADA promo strips.
    if host.startswith("aircraft-exchange.s3."):
        if "/banners/" in path or "iada_banners" in path:
            return True

    # IADA-hosted logos (not aircraft listing imagery).
    if host == "iada.aero" or host.endswith(".iada.aero"):
        if "/logos/" in path or "/storage/logos/" in path:
            return True

    return False


def dedupe_preserve(urls: list[str]) -> list[str]:
    seen: set[str] = set()
    out: list[str] = []
    for u in urls:
        if not u or u in seen:
            continue
        seen.add(u)
        out.append(u)
    return out


def extract_aircraftexchange(html_text: str) -> dict[str, list[str] | str]:
    soup = BeautifulSoup(html_text, "html.parser")
    og: list[str] = []
    tw: list[str] = []
    gallery: list[str] = []

    page_url = ""
    ou = soup.find("meta", property="og:url")
    if ou and ou.get("content"):
        page_url = normalize_url(ou["content"])

    m = soup.find("meta", property="og:image")
    if m and m.get("content"):
        og.append(normalize_url(m["content"]))
    m = soup.find("meta", attrs={"name": "twitter:image"})
    if m and m.get("content"):
        tw.append(normalize_url(m["content"]))

    for a in soup.select('a[data-fancybox="gallery"]'):
        h = a.get("href")
        if h:
            gallery.append(normalize_url(h))

    og = [u for u in og if not should_skip_url(u)]
    tw = [u for u in tw if not should_skip_url(u)]
    gallery = [u for u in gallery if not should_skip_url(u)]

    all_urls = dedupe_preserve(og + tw + gallery)
    return {
        "og": og,
        "twitter": tw,
        "gallery": gallery,
        "all": all_urls,
        "listing_page_url": page_url,
    }


def extract_controller(html_text: str) -> dict[str, list[str] | str]:
    soup = BeautifulSoup(html_text, "html.parser")
    meta_urls: list[str] = []
    preload: list[str] = []
    embedded: list[str] = []

    page_url = ""
    ou = soup.find("meta", property="og:url")
    if ou and ou.get("content"):
        page_url = normalize_url(ou["content"])

    m = soup.find("meta", property="og:image")
    if m and m.get("content"):
        meta_urls.append(normalize_url(m["content"]))
    m = soup.find("meta", attrs={"name": "twitter:image"})
    if m and m.get("content"):
        meta_urls.append(normalize_url(m["content"]))

    for link in soup.find_all("link", rel="preload"):
        if (link.get("as") or "").lower() == "image" and link.get("href"):
            preload.append(normalize_url(link["href"]))

    for match in CONTROLLER_JSON_URL_RE.finditer(html_text):
        embedded.append(normalize_url(match.group(1)))

    meta_urls = [u for u in meta_urls if not should_skip_url(u)]
    preload = [u for u in preload if "sandhills.com/img.axd" in u]
    embedded = [u for u in embedded if not should_skip_url(u)]

    all_urls = dedupe_preserve(meta_urls + preload + embedded)
    return {
        "meta": meta_urls,
        "preload": preload,
        "embedded_json": embedded,
        "all": all_urls,
        "listing_page_url": page_url,
    }


def extract_aircraftpost(html_text: str) -> dict[str, list[str] | str]:
    soup = BeautifulSoup(html_text, "html.parser")
    meta_urls: list[str] = []
    imgs: list[str] = []

    page_url = ""
    ou = soup.find("meta", property="og:url")
    if ou and ou.get("content"):
        page_url = normalize_url(ou["content"])

    m = soup.find("meta", property="og:image")
    if m and m.get("content"):
        meta_urls.append(normalize_url(m["content"]))

    for img in soup.find_all("img"):
        src = img.get("src")
        if src:
            u = normalize_url(src)
            if u.startswith("http") and not should_skip_url(u):
                imgs.append(u)
        srcset = img.get("srcset")
        if srcset:
            for part in srcset.split(","):
                piece = part.strip().split()[0] if part.strip() else ""
                if piece.startswith("http"):
                    u = normalize_url(piece)
                    if not should_skip_url(u):
                        imgs.append(u)

    for m in HTTP_IMG_RE.finditer(html_text):
        u = normalize_url(m.group(0).rstrip('",)'))
        if not should_skip_url(u):
            imgs.append(u)

    meta_urls = [u for u in meta_urls if not should_skip_url(u)]
    all_urls = dedupe_preserve(meta_urls + imgs)
    return {
        "meta": meta_urls,
        "img_tags": imgs,
        "all": all_urls,
        "listing_page_url": page_url,
    }


def extract_generic(html_text: str) -> dict[str, list[str] | str]:
    soup = BeautifulSoup(html_text, "html.parser")
    found: list[str] = []

    page_url = ""
    ou = soup.find("meta", property="og:url")
    if ou and ou.get("content"):
        page_url = normalize_url(ou["content"])

    m = soup.find("meta", property="og:image")
    if m and m.get("content"):
        found.append(normalize_url(m["content"]))
    m = soup.find("meta", attrs={"name": "twitter:image"})
    if m and m.get("content"):
        found.append(normalize_url(m["content"]))
    for img in soup.find_all("img"):
        src = img.get("src")
        if src and src.startswith("http"):
            found.append(normalize_url(src))
    found = [u for u in found if not should_skip_url(u)]
    return {
        "all": dedupe_preserve(found),
        "listing_page_url": page_url,
    }


def merge_by_lookup_key(entries: list[dict]) -> dict[str, dict]:
    """One entry per lookup_key; merge image URLs if the same listing was scraped on multiple dates."""
    out: dict[str, dict] = {}
    for e in entries:
        lk = (e.get("lookup_key") or "").strip()
        if not lk:
            continue
        urls = e.get("image_urls") or []
        if not isinstance(urls, list):
            urls = []
        if lk not in out:
            out[lk] = {
                "image_urls": [str(u) for u in urls if u],
                "source_file": (e.get("source_file") or ""),
                "listing_page_url": (e.get("listing_page_url") or ""),
            }
            continue
        o = out[lk]
        o["image_urls"] = dedupe_preserve(o["image_urls"] + [str(u) for u in urls if u])
        if not (o.get("listing_page_url") or "").strip() and (e.get("listing_page_url") or "").strip():
            o["listing_page_url"] = e["listing_page_url"]
    return out


def page_key_and_extractor(platform: str, name: str) -> tuple[str, str, Extractor]:
    if name.startswith("listing_") and name.endswith(".html"):
        lid = name[len("listing_") : -len(".html")]
        if platform == "aircraftexchange":
            return lid, "listing", extract_aircraftexchange
        if platform == "controller":
            return lid, "listing", extract_controller
    if name.startswith("make_model_") and name.endswith(".html"):
        mid = name[len("make_model_") : -len(".html")]
        if platform == "aircraftpost":
            return mid, "make_model", extract_aircraftpost
    stem = Path(name).stem
    return stem, "page", extract_generic


def collect_platform(raw_root: Path, platform: str) -> list[dict]:
    platform_dir = raw_root / platform
    if not platform_dir.is_dir():
        return []

    entries: list[dict] = []
    for path in sorted(platform_dir.rglob("*.html")):
        rel = path.relative_to(raw_root).as_posix()
        name = path.name
        page_id, page_kind, extractor = page_key_and_extractor(platform, name)
        try:
            text = path.read_text(encoding="utf-8", errors="replace")
        except OSError:
            continue
        try:
            buckets = extractor(text)
        except Exception:
            buckets = {"all": [], "listing_page_url": ""}

        listing_page_url = str(buckets.pop("listing_page_url", "") or "")

        lookup_key = ""
        if platform == "controller" and page_kind == "listing":
            lookup_key = f"controller:{page_id}"
        elif platform == "aircraftexchange" and page_kind == "listing":
            lookup_key = f"aircraftexchange:{page_id}"
        elif platform == "aircraftpost" and page_kind == "make_model":
            lookup_key = f"aircraftpost:make_model:{page_id}"

        by_source = {k: v for k, v in buckets.items() if k != "all"}

        entries.append(
            {
                "lookup_key": lookup_key,
                "listing_page_url": listing_page_url,
                "source_file": rel,
                "page_id": page_id,
                "page_kind": page_kind,
                "image_urls": buckets.get("all", []),
                "by_source": by_source,
            }
        )
    return entries


def main() -> None:
    parser = argparse.ArgumentParser(description="Extract image URLs from raw HTML by platform.")
    parser.add_argument(
        "--store-root",
        type=Path,
        default=Path(__file__).resolve().parent.parent / "store" / "raw",
        help="Path to store/raw",
    )
    parser.add_argument(
        "--out-dir",
        type=Path,
        default=Path(__file__).resolve().parent.parent
        / "store"
        / "derived"
        / "scrape_image_urls",
        help="Directory for per-platform JSON output",
    )
    args = parser.parse_args()
    raw_root: Path = args.store_root.resolve()
    out_dir: Path = args.out_dir.resolve()
    out_dir.mkdir(parents=True, exist_ok=True)

    platforms = sorted(
        p.name for p in raw_root.iterdir() if p.is_dir() and not p.name.startswith(".")
    )
    generated = datetime.now(timezone.utc).isoformat()

    for platform in platforms:
        files = collect_platform(raw_root, platform)
        by_lk = merge_by_lookup_key(files)
        payload = OrderedDict(
            [
                ("platform", platform),
                (
                    "consultant_listing_lookup",
                    {
                        "how_to_match": (
                            "Ask Consultant resolves scraped galleries to internal listing rows using "
                            "lookup_key = '{platform}:{marketplace_listing_id}' for Controller and AircraftExchange "
                            "(same as listing_image_lookup_key in backend.services.scrape_listing_image_lookup). "
                            "Postgres: aircraft_listings.source_platform + source_listing_id, or parse id from listing_url."
                        ),
                        "keys_used_for_listings": ["controller:*", "aircraftexchange:*"],
                        "aircraftpost_keys": (
                            "make_model pages use lookup_key aircraftpost:make_model:{id} for traceability only; "
                            "they are not joined to aircraft_listings the same way."
                        ),
                    },
                ),
                ("generated_at", generated),
                ("source_root", raw_root.as_posix()),
                ("by_lookup_key", by_lk),
                ("file_count", len(files)),
                ("files", files),
            ]
        )
        out_path = out_dir / f"{platform}_image_urls.json"
        out_path.write_text(
            json.dumps(payload, indent=2, ensure_ascii=False) + "\n",
            encoding="utf-8",
        )
        print(f"Wrote {out_path} ({len(files)} HTML files)")


if __name__ == "__main__":
    main()
