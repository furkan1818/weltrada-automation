from fastapi import FastAPI, UploadFile, File
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles

import os
import shutil
import zipfile
from datetime import datetime
from io import BytesIO

import logging
import pandas as pd
import requests
from PIL import Image

# ------------------------------------------------------
# LOGGING
# ------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

# ------------------------------------------------------
# FASTAPI SETUP
# ------------------------------------------------------
app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

BASE_DIR = "/opt/render/project/src"

# ------------------------------------------------------
# RAPIDAPI CONFIG
# ------------------------------------------------------
RAPID_KEY = os.getenv("RAPIDAPI_KEY", "")
RAPID_HOST = "bing-web-search1.p.rapidapi.com"
RAPID_BASE = "https://bing-web-search1.p.rapidapi.com"

if not RAPID_KEY:
    logger.warning("⚠️ RAPIDAPI_KEY env variable eksik! Arama ÇALIŞMAZ.")

# ------------------------------------------------------
# TEST
# ------------------------------------------------------
@app.get("/")
def root():
    return {"status": "ok", "msg": "RapidAPI Bing Search aktif!"}

# ------------------------------------------------------
# HELPERS
# ------------------------------------------------------
def clean_filename(s: str) -> str:
    if not s:
        return ""
    return "".join(c.lower() for c in s.replace(" ", "-") if c.isalnum() or c in ["-", "_"])

def rapid_web_search(query: str):
    url = f"{RAPID_BASE}/search"
    headers = {
        "X-RapidAPI-Key": RAPID_KEY,
        "X-RapidAPI-Host": RAPID_HOST,
    }
    params = {
        "q": query,
        "mkt": "en-US",
        "textFormat": "Raw",
        "safeSearch": "Off",
        "count": 10
    }
    try:
        logger.info(f"[WEB] {query}")
        r = requests.get(url, headers=headers, params=params, timeout=20)
        return r.json() if r.status_code == 200 else {}
    except Exception as e:
        logger.error(f"[WEB ERROR] {e}")
        return {}

def rapid_image_search(query: str):
    url = f"{RAPID_BASE}/images/search"
    headers = {
        "X-RapidAPI-Key": RAPID_KEY,
        "X-RapidAPI-Host": RAPID_HOST,
    }
    params = {"q": query, "count": 5}
    try:
        logger.info(f"[IMG] {query}")
        r = requests.get(url, headers=headers, params=params, timeout=20)
        js = r.json() if r.status_code == 200 else {}
        return [i.get("contentUrl") for i in js.get("value", [])]
    except Exception as e:
        logger.error(f"[IMG ERROR] {e}")
        return []

# ------------------------------------------------------
# SEARCH PRODUCT
# ------------------------------------------------------
def search_product(brand: str, code: str):
    result = {
        "product_name": "",
        "product_page_url": "",
        "datasheet_url": "",
        "image_urls": [],
        "status": "NOT_FOUND",
    }

    query = f'{brand} "{code}"'

    # --- WEB ---
    js = rapid_web_search(query + " datasheet")
    web_items = js.get("webPages", {}).get("value", []) if js else []

    if web_items:
        first = web_items[0]
        result["product_name"] = first.get("name", "")
        result["product_page_url"] = first.get("url", "")

        for w in web_items:
            url = w.get("url", "").lower()
            if ".pdf" in url:
                result["datasheet_url"] = url
                break

    # --- IMAGE ---
    result["image_urls"] = rapid_image_search(query + " product image")

    # Status
    if result["datasheet_url"] or result["image_urls"]:
        result["status"] = "OK"
    elif result["product_page_url"]:
        result["status"] = "PARTIAL"

    return result

# ------------------------------------------------------
# DOWNLOAD HELPERS
# ------------------------------------------------------
def download_image_to_webp(url: str, save_path: str) -> bool:
    try:
        r = requests.get(url, timeout=20)
        if r.status_code != 200:
            return False
        img = Image.open(BytesIO(r.content)).convert("RGB")
        img.save(save_path, "webp")
        return True
    except:
        return False

def download_file(url: str, save_path: str) -> bool:
    try:
        r = requests.get(url, timeout=20)
        if r.status_code != 200:
            return False
        with open(save_path, "wb") as f:
            f.write(r.content)
        return True
    except:
        return False

# ------------------------------------------------------
# PROCESS PRODUCTS
# ------------------------------------------------------
@app.post("/process-products")
async def process_products(file: UploadFile = File(...)):
    logger.info("[API] Excel işlendi")

    folder = f"Research-{datetime.now().strftime('%d-%m-%Y-at-%H-%M')}"
    root = os.path.join(BASE_DIR, folder)
    images_root = os.path.join(root, "Images")
    datasheets_root = os.path.join(root, "Datasheets")

    os.makedirs(images_root, exist_ok=True)
    os.makedirs(datasheets_root, exist_ok=True)

    # Excel kaydet
    excel_path = os.path.join(root, "uploaded.xlsx")
    with open(excel_path, "wb") as b:
        shutil.copyfileobj(file.file, b)

    df = pd.read_excel(excel_path)

    output = []

    for idx, row in df.iterrows():
        brand = str(row["brand"]).strip()
        code = str(row["product_code"]).strip()

        logger.info(f"[ROW] {brand} - {code}")

        res = search_product(brand, code)

        # IMAGES
        saved_imgs = []
        if res["image_urls"]:
            pdir = os.path.join(images_root, code)
            os.makedirs(pdir, exist_ok=True)
            c = 1
            for url in res["image_urls"]:
                fname = f"{code.lower()}-{c:02d}.webp"
                dest = os.path.join(pdir, fname)
                if download_image_to_webp(url, dest):
                    saved_imgs.append(os.path.relpath(dest, root))
                    c += 1

        # DATASHEET
        if res["datasheet_url"]:
            ds_path = os.path.join(datasheets_root, f"{code}-datasheet.pdf")
            if download_file(res["datasheet_url"], ds_path):
                ds_local = os.path.relpath(ds_path, root)
            else:
                ds_local = ""
        else:
            ds_local = ""

        output.append({
            "brand": brand,
            "product_code": code,
            "product_name": res["product_name"],
            "product_page_url": res["product_page_url"],
            "datasheet_url": res["datasheet_url"],
            "datasheet_file": ds_local,
            "image_urls": ";".join(res["image_urls"]),
            "saved_image_files": ";".join(saved_imgs),
            "status": res["status"],
        })

    out_excel = os.path.join(root, "products_output.xlsx")
    pd.DataFrame(output).to_excel(out_excel, index=False)

    # ZIP
    zip_name = f"{folder}.zip"
    zip_path = os.path.join(BASE_DIR, zip_name)

    with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as z:
        for base, _, files in os.walk(root):
            for f in files:
                fp = os.path.join(base, f)
                z.write(fp, os.path.relpath(fp, root))

    logger.info(f"[OK] ZIP hazır: {zip_name}")

    return {
        "status": "success",
        "zip_file": zip_name,
        "download_url": f"https://weltrada-automation.onrender.com/static/{zip_name}"
    }

# ------------------------------------------------------
# STATIC
# ------------------------------------------------------
app.mount("/static", StaticFiles(directory=BASE_DIR), name="static")