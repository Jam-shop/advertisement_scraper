#!/usr/bin/env python3
"""
Anandabazar Patrika e-paper advertisement scraper -> straight to Google Drive / OneDrive.

Nothing scraped touches your disk: page scans are held in RAM, ads are cropped in RAM,
and every image / CSV is streamed to the cloud with `rclone rcat`.
(Only the browser login profile and editions.json live locally.)

How the site works (from its network traffic):
  * GET /api/v1/next-page/{slug}/{edition_id}/{YYYY-MM-DD}/page-{n}.html
    -> JSON; data.epaper_page_data holds <img class="panzoom-image" data-full=...>
       plus <div id=... class="article-layer" style="top:%;left:%;width:%;height:%">
       per clickable box. Ads have ids like "ad236522"; articles are numeric.
  * Each ad is cropped from the full-resolution page scan using those %s.

One-time setup:
  pip install playwright pillow && playwright install chromium
  install rclone (https://rclone.org/install/), then `rclone config`:
      n -> name: gdrive   -> storage: drive      (Google Drive)   -> accept defaults, log in
   or n -> name: onedrive -> storage: onedrive   (OneDrive)       -> accept defaults, log in

Usage:
  python abp_ads.py login
  python abp_ads.py discover
  python abp_ads.py scrape --dest gdrive:ABP_ads --start 2026-04-15 --end 2026-04-26
      (or --dest onedrive:ABP_ads)

Cloud layout:
  ABP_ads/<date>/<edition-slug>/p03_ad236522.jpg
  ABP_ads/<date>/metadata.csv
  ABP_ads/all_ads.csv
  ABP_ads/_done.txt            (resume log: re-run the same command to continue)
"""
import argparse, csv, io, json, random, re, shutil, subprocess, sys, time
from collections import OrderedDict
from concurrent.futures import ThreadPoolExecutor, wait
from datetime import date, timedelta
from html.parser import HTMLParser
from pathlib import Path

BASE = "https://epaper.anandabazar.com"
API = BASE + "/api/v1/next-page/{slug}/{eid}/{d}/page-{n}.html"
PAGE_URL = BASE + "/{slug}/{eid}/{d}/page-{n}.html"
AD_URL = BASE + "/{slug}/{eid}/{d}/page-{n}/{ad}.html"
PROFILE = Path("abp_browser_profile")
MAX_PAGES = 80
PAGE_CACHE = 48  # page scans kept in RAM (~1-2 MB each); shared pages across districts download once
FIELDS = ["date", "edition_slug", "edition_id", "edition_name", "page", "section", "ad_id",
          "top_pct", "left_pct", "width_pct", "height_pct", "x_px", "y_px", "w_px", "h_px",
          "page_w_px", "page_h_px", "area_pct_of_page", "image_file", "duplicate_of",
          "ad_page_url", "page_image_url", "page_image_quality"]


# ---------------------------------------------------------------- cloud storage (rclone)
class Cloud:
    def __init__(self, dest, workers):
        if not shutil.which("rclone"):
            sys.exit("rclone not found. Install it from https://rclone.org/install/ and run `rclone config`.")
        self.dest = dest.rstrip("/")
        self.pool = ThreadPoolExecutor(max_workers=workers)
        self._dirs = set()
        self.mkdir("")  # fails loudly if the remote isn't configured / logged in
        r = self._run("lsf", self.dest, "--max-depth", "1")
        if r.returncode:
            sys.exit(f"Can't reach {self.dest}: {r.stderr.decode(errors='ignore')}")

    def _run(self, *args, data=None):
        return subprocess.run(["rclone", *args], input=data, capture_output=True)

    def _p(self, rel):
        return f"{self.dest}/{rel}" if rel else self.dest

    def mkdir(self, rel):
        # created synchronously before parallel uploads: Google Drive allows duplicate
        # folder names, so racing uploads into a new folder would create several copies
        if rel in self._dirs:
            return
        r = self._run("mkdir", self._p(rel))
        if r.returncode:
            sys.exit(f"rclone mkdir {self._p(rel)} failed: {r.stderr.decode(errors='ignore')}")
        self._dirs.add(rel)

    def put(self, rel, data, retries=4):
        for i in range(retries):
            r = self._run("rcat", self._p(rel), data=data)
            if r.returncode == 0:
                return
            time.sleep(2 ** i * 3)
        raise RuntimeError(f"upload failed {rel}: {r.stderr.decode(errors='ignore')[:300]}")

    def put_async(self, rel, data):
        return self.pool.submit(self.put, rel, data)

    def get_text(self, rel):
        r = self._run("cat", self._p(rel))
        return r.stdout.decode("utf-8-sig") if r.returncode == 0 else None


def csv_bytes(rows):
    buf = io.StringIO()
    w = csv.DictWriter(buf, fieldnames=FIELDS, extrasaction="ignore")
    w.writeheader()
    w.writerows(rows)
    return buf.getvalue().encode("utf-8-sig")  # BOM so Excel shows Bengali correctly


def csv_rows(text):
    return list(csv.DictReader(io.StringIO(text))) if text else []


# ---------------------------------------------------------------- parsing
class _BoxParser(HTMLParser):
    def __init__(self):
        super().__init__()
        self.img, self.boxes = {}, []

    def handle_starttag(self, tag, attrs):
        a = dict(attrs)
        if tag == "img" and "panzoom-image" in (a.get("class") or ""):
            self.img = a
        elif tag == "div" and a.get("id") and "style" in a:
            st = {k.strip(): v for k, v in re.findall(r"([\w-]+)\s*:\s*([\d.]+)%", a["style"])}
            if all(k in st for k in ("top", "left", "width", "height")):
                self.boxes.append({"id": a["id"], "class": a.get("class") or "",
                                   **{k: float(st[k]) for k in ("top", "left", "width", "height")}})


def is_ad(box):
    return box["id"].lower().startswith("ad") or "ad" in re.split(r"[\s_-]+", box["class"].lower())


def parse_page(page_html):
    p = _BoxParser()
    p.feed(page_html)
    full, hq = p.img.get("data-full"), p.img.get("data-hq")
    # Original JPEG sits behind Cloudflare's resizer: .../cdn-cgi/image/<opts>/https://assets-abp...
    orig = None
    for u in (full, hq, p.img.get("src")):
        m = re.search(r"(https://assets-abp\.ttef\.in/\S+?\.(?:jpe?g|png|webp))", u or "")
        if m:
            orig = m.group(1)
            break
    candidates = [(u, q) for u, q in ((orig, "original"), (full, "full"), (hq, "hq")) if u]
    return candidates, [b for b in p.boxes if is_ad(b)]


def parse_meta_title(t):
    # "Anandabazar Patrika Epaper - কলকাতা, Friday, 10 Apr, 2026 | প্রথম পাতা -  page 1"
    m = re.search(r"Epaper\s*-\s*([^,|]+)", t or "")
    s = re.search(r"\|\s*(.*?)\s*-\s*page\s*\d+", t or "", re.I)
    return (m.group(1).strip() if m else ""), (s.group(1).strip() if s else "")


def crop_ad(img, box):
    W, H = img.size
    x, y = round(box["left"] / 100 * W), round(box["top"] / 100 * H)
    w, h = round(box["width"] / 100 * W), round(box["height"] / 100 * H)
    x2, y2 = min(W, x + w), min(H, y + h)
    x, y = max(0, x), max(0, y)
    return img.crop((x, y, x2, y2)), (x, y, x2 - x, y2 - y)


# ---------------------------------------------------------------- browser / http
def open_ctx(pw, headed=False):
    return pw.chromium.launch_persistent_context(
        str(PROFILE), headless=not headed, viewport={"width": 1400, "height": 1000},
        user_agent=("Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                    "(KHTML, like Gecko) Chrome/146.0.0.0 Safari/537.36"))


def nap(a):
    time.sleep(a.delay * random.uniform(0.7, 1.4))


def fetch_page_api(ctx, slug, eid, d, n):
    """Return parsed JSON data dict, or None if page doesn't exist / isn't served."""
    r = ctx.request.get(API.format(slug=slug, eid=eid, d=d, n=n), headers={
        "X-Requested-With": "XMLHttpRequest", "Accept": "*/*",
        "Referer": PAGE_URL.format(slug=slug, eid=eid, d=d, n=max(1, n - 1))}, timeout=60000)
    if r.status != 200:
        return None
    try:
        data = (r.json() or {}).get("data") or {}
    except Exception:
        return None
    if not data.get("epaper_page_data") or int(data.get("current_page") or n) != n:
        return None
    return data


def fetch_page_browser(ctx, slug, eid, d, n):
    """Fallback: render the page and read the same HTML out of the DOM."""
    pg = ctx.new_page()
    try:
        pg.goto(PAGE_URL.format(slug=slug, eid=eid, d=d, n=n), wait_until="networkidle", timeout=90000)
        html = pg.evaluate("""(n) => {
            const img = document.querySelector('#zoom_' + n) || document.querySelector('img.panzoom-image');
            if (!img) return null;
            const area = img.parentElement.querySelector('.hover-selected-area');
            return img.outerHTML + (area ? area.outerHTML : '');
        }""", n)
        return {"epaper_page_data": html, "meta_title": pg.title()} if html else None
    finally:
        pg.close()


def download_image(ctx, candidates, cache):
    """Fetch the page scan into RAM (LRU cache keyed by URL). Returns (PIL image, url, quality)."""
    from PIL import Image
    for url, quality in candidates:
        body = cache.get(url)
        if body is None:
            try:
                r = ctx.request.get(url, headers={"Referer": BASE + "/"}, timeout=120000)
                body = r.body() if r.status == 200 else b""
            except Exception:
                body = b""
            if len(body) < 5000:
                continue
        try:
            img = Image.open(io.BytesIO(body))
            img.load()
        except Exception:
            continue
        cache[url] = body
        cache.move_to_end(url)
        while len(cache) > PAGE_CACHE:
            cache.popitem(last=False)
        return img.convert("RGB"), url, quality
    return None, None, None


# ---------------------------------------------------------------- commands
def cmd_login(a):
    from playwright.sync_api import sync_playwright
    with sync_playwright() as pw:
        ctx = open_ctx(pw, headed=True)
        pg = ctx.pages[0] if ctx.pages else ctx.new_page()
        pg.goto(BASE + "/")
        input("\nLog in to your subscription in the browser window, open any paper page "
              "to confirm it's readable, then press Enter here... ")
        ok = fetch_page_api(ctx, "calcutta-north", "15", a.test_date, 1)
        print("Session saved." if ok else
              "Saved, but the test page didn't load via API. scrape will fall back to the browser.")
        ctx.close()


def cmd_discover(a):
    from playwright.sync_api import sync_playwright
    found, bodies = {}, []
    pat = re.compile(r"(?:epaper\.anandabazar\.com)?/([a-z][a-z0-9-]+)/(\d{1,5})/(?:\d{4}-\d{2}-\d{2}|page|\")")
    skip = {"api", "assets", "static", "cdn-cgi", "images", "js", "css"}
    with sync_playwright() as pw:
        ctx = open_ctx(pw, headed=a.headed)
        pg = ctx.new_page()

        def grab(resp):
            try:
                if "anandabazar" in resp.url and any(t in resp.headers.get("content-type", "") for t in ("json", "html", "javascript")):
                    bodies.append(resp.text())
            except Exception:
                pass
        pg.on("response", grab)
        for url in (BASE + "/", PAGE_URL.format(slug="calcutta-north", eid="15", d=a.test_date, n=1)):
            pg.goto(url, wait_until="networkidle", timeout=90000)
            for sel in ("text=Edition", "text=সংস্করণ", ".edition", "#edition", "[class*=edition]"):
                try:
                    pg.locator(sel).first.click(timeout=1500)
                    pg.wait_for_timeout(800)
                except Exception:
                    pass
            for href, text in pg.evaluate("Array.from(document.querySelectorAll('a[href]')).map(e=>[e.href,e.innerText.trim()])"):
                m = pat.search(href + '"')
                if m and m.group(1) not in skip:
                    found.setdefault((m.group(1), m.group(2)), text[:60])
            bodies.append(pg.content())
        for b in bodies:
            for m in pat.finditer(b.replace("\\/", "/")):
                if m.group(1) not in skip:
                    found.setdefault((m.group(1), m.group(2)), "")
        ctx.close()
    eds = [{"slug": s, "id": i, "label": t} for (s, i), t in sorted(found.items(), key=lambda kv: int(kv[0][1]))]
    Path(a.editions_file).write_text(json.dumps(eds, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"Found {len(eds)} editions -> {a.editions_file}")
    for e in eds:
        print(f"  {e['id']:>5}  {e['slug']:<30} {e['label']}")
    if not eds:
        print("Nothing found. Fill editions.json by hand: [{\"slug\": \"calcutta-north\", \"id\": \"15\"}, ...]")


def cmd_scrape(a):
    from playwright.sync_api import sync_playwright
    eds = json.loads(Path(a.editions_file).read_text(encoding="utf-8"))
    if a.editions:
        keep = set(a.editions.split(","))
        eds = [e for e in eds if e["slug"] in keep or str(e["id"]) in keep]
    cloud = Cloud(a.dest, a.upload_workers)
    done = set((cloud.get_text("_done.txt") or "").split())
    all_rows = csv_rows(cloud.get_text("all_ads.csv"))
    d0, d1 = date.fromisoformat(a.start), date.fromisoformat(a.end)
    dates = [(d0 + timedelta(i)).isoformat() for i in range((d1 - d0).days + 1)]
    cache = OrderedDict()
    print(f"Uploading to {cloud.dest}  ({len(eds)} editions x {len(dates)} dates)")

    with sync_playwright() as pw:
        ctx = open_ctx(pw, headed=a.headed)
        for d in dates:
            if all(f"{d}|{e['slug']}" in done for e in eds):
                continue
            day_rows = csv_rows(cloud.get_text(f"{d}/metadata.csv"))
            seen = {}  # ad_id -> first file (same ad usually runs in many district editions)
            for r in day_rows:
                seen.setdefault(r["ad_id"], r["image_file"])
            for e in eds:
                slug, eid, key = e["slug"], str(e["id"]), f"{d}|{e['slug']}"
                if key in done:
                    continue
                mode, n, rows, futs = "api", 1, [], []
                while n <= MAX_PAGES:
                    data = fetch_page_api(ctx, slug, eid, d, n) if mode == "api" else None
                    if data is None and n == 1 and mode == "api":
                        data = fetch_page_browser(ctx, slug, eid, d, 1)
                        if data:
                            mode = "browser"
                            print(f"  [{slug}] API not serving pages; using browser mode")
                    elif data is None and mode == "browser":
                        data = fetch_page_browser(ctx, slug, eid, d, n)
                    nap(a)
                    if data is None:
                        break
                    ed_name, section = parse_meta_title(data.get("meta_title", ""))
                    candidates, ads = parse_page(data["epaper_page_data"])
                    if ads:
                        img, img_url, quality = download_image(ctx, candidates, cache)
                        nap(a)
                        if img is None:
                            print(f"  !! {d} {slug} p{n}: page image failed, {len(ads)} ads skipped")
                        else:
                            cloud.mkdir(d)
                            cloud.mkdir(f"{d}/{slug}")
                            for b in ads:
                                crop, (x, y, w, h) = crop_ad(img, b)
                                buf = io.BytesIO()
                                crop.save(buf, format="JPEG", quality=95)
                                rel = f"{d}/{slug}/p{n:02d}_{b['id']}.jpg"
                                futs.append(cloud.put_async(rel, buf.getvalue()))
                                dup = seen.get(b["id"], "")
                                seen.setdefault(b["id"], rel)
                                rows.append({
                                    "date": d, "edition_slug": slug, "edition_id": eid, "edition_name": ed_name,
                                    "page": n, "section": section, "ad_id": b["id"],
                                    "top_pct": b["top"], "left_pct": b["left"], "width_pct": b["width"], "height_pct": b["height"],
                                    "x_px": x, "y_px": y, "w_px": w, "h_px": h,
                                    "page_w_px": img.width, "page_h_px": img.height,
                                    "area_pct_of_page": round(b["width"] * b["height"] / 100, 2),
                                    "image_file": rel, "duplicate_of": dup,
                                    "ad_page_url": AD_URL.format(slug=slug, eid=eid, d=d, n=n, ad=b["id"]),
                                    "page_image_url": img_url, "page_image_quality": quality})
                    n += 1
                for f in wait(futs).done:
                    f.result()  # raises if an image upload ultimately failed -> edition not marked done
                if rows:
                    day_rows = [r for r in day_rows if r["edition_slug"] != slug] + rows
                    cloud.mkdir(d)
                    cloud.put(f"{d}/metadata.csv", csv_bytes(day_rows))
                done.add(key)
                cloud.put("_done.txt", ("\n".join(sorted(done)) + "\n").encode())
                print(f"{d} {slug:<28} pages={n - 1:<3} ads={len(rows)}")
            all_rows = [r for r in all_rows if r["date"] != d] + day_rows
            cloud.put("all_ads.csv", csv_bytes(all_rows))
        ctx.close()
    cloud.pool.shutdown()
    print(f"Done -> {cloud.dest}")


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--editions-file", default="editions.json")
    ap.add_argument("--test-date", default="2026-04-15", help="date used for login check / discovery")
    ap.add_argument("--headed", action="store_true", help="show the browser while working")
    sub = ap.add_subparsers(dest="cmd", required=True)
    sub.add_parser("login")
    sub.add_parser("discover")
    s = sub.add_parser("scrape")
    s.add_argument("--dest", required=True, help="rclone remote path, e.g. gdrive:ABP_ads or onedrive:ABP_ads")
    s.add_argument("--start", required=True)
    s.add_argument("--end", required=True)
    s.add_argument("--editions", help="comma-separated slugs or ids to restrict to")
    s.add_argument("--delay", type=float, default=1.5, help="avg seconds between site requests")
    s.add_argument("--upload-workers", type=int, default=4, help="parallel uploads")
    a = ap.parse_args()
    {"login": cmd_login, "discover": cmd_discover, "scrape": cmd_scrape}[a.cmd](a)


if __name__ == "__main__":
    sys.exit(main())