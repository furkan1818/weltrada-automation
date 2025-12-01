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
# FASTAPI & CONFIG
# ------------------------------------------------------
app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

BASE_DIR = "/opt/render/project/src"

# Azure Bing Search config (ENV'DEN OKUNUYOR)
AZURE_BING_ENDPOINT = os.getenv("AZURE_BING_ENDPOINT", "").rstrip("/")
AZURE_BING_KEY = os.getenv("AZURE_BING_KEY", "")

if not AZURE_BING_ENDPOINT or not AZURE_BING_KEY:
    logger.warning(
        "AZURE_BING_ENDPOINT veya AZURE_BING_KEY environment variable tanımlı değil! "
        "Bing aramaları çalışmayacak."
    )


# ------------------------------------------------------
# ROOT (TEST)
# ------------------------------------------------------
@app.get("/")
def root():
    return {
        "status": "ok",
        "message": "Weltrada Bing Research API. POST /process-products ile Excel yükle.",
    }


# ------------------------------------------------------
# UTILS
# ------------------------------------------------------
def clean_filename(s: str) -> str:
    """
    Boşlukları - yapar, küçük harfe çevirir, sadece harf/rakam/-/_ bırakır.
    """
    if not s:
        return ""
    return "".join(
        c.lower()
        for c in s.replace(" ", "-")
        if c.isalnum() or c in ["-", "_"]
    )


def download_image_to_webp(url: str, save_path: str) -> bool:
    """
    Verilen URL'den görsel indir, RGB'ye çevirip .webp kaydet.
    """
    try:
        logger.info(f"[IMG] {url}")
        headers = {
            "User-Agent": "Mozilla/5.0 (compatible; WeltradaBot/1.0; +https://weltrada.com)"
        }
        r = requests.get(url, timeout=25, headers=headers)
        if r.status_code != 200:
            logger.warning(f"[IMG ERROR] {url} -> Status {r.status_code}")
            return False

        img = Image.open(BytesIO(r.content))
        img = img.convert("RGB")
        img.save(save_path, "webp")
        return True

    except Exception as e:
        logger.error(f"[IMG ERROR] {url} -> {e}")
        return False


def download_file(url: str, save_path: str) -> bool:
    """
    PDF gibi dosyaları indirir.
    """
    try:
        logger.info(f"[FILE] {url}")
        headers = {
            "User-Agent": "Mozilla/5.0 (compatible; WeltradaBot/1.0; +https://weltrada.com)"
        }
        r = requests.get(url, timeout=25, headers=headers)
        if r.status_code != 200:
            logger.warning(f"[FILE ERROR] {url} -> Status {r.status_code}")
            return False

        with open(save_path, "wb") as f:
            f.write(r.content)
        return True

    except Exception as e:
        logger.error(f"[FILE ERROR] {url} -> {e}")
        return False


# ------------------------------------------------------
# BING SEARCH HELPERS
# ------------------------------------------------------
def bing_web_search(query: str, count: int = 10) -> dict:
    """
    Bing Web Search (web sayfaları) – JSON döner.
    """
    if not AZURE_BING_ENDPOINT or not AZURE_BING_KEY:
        logger.error("Bing config eksik, web search çağrısı yapılamıyor.")
        return {}

    url = f"{AZURE_BING_ENDPOINT}/bing/v7.0/search"
    headers = {"Ocp-Apim-Subscription-Key": AZURE_BING_KEY}
    params = {
        "q": query,
        "mkt": "en-us",
        "responseFilter": "Webpages",
        "count": count,
    }

    try:
        logger.info(f"[BING WEB] {query}")
        r = requests.get(url, headers=headers, params=params, timeout=15)
        if r.status_code != 200:
            logger.warning(f"[BING WEB ERROR] {r.status_code} -> {r.text[:200]}")
            return {}
        return r.json()
    except Exception as e:
        logger.error(f"[BING WEB EXC] {e}")
        return {}


def bing_image_search(query: str, count: int = 3) -> dict:
    """
    Bing Image Search – JSON döner.
    """
    if not AZURE_BING_ENDPOINT or not AZURE_BING_KEY:
        logger.error("Bing config eksik, image search çağrısı yapılamıyor.")
        return {}

    url = f"{AZURE_BING_ENDPOINT}/bing/v7.0/images/search"
    headers = {"Ocp-Apim-Subscription-Key": AZURE_BING_KEY}
    params = {
        "q": query,
        "mkt": "en-us",
        "safeSearch": "Strict",
        "count": count,
    }

    try:
        logger.info(f"[BING IMG] {query}")
        r = requests.get(url, headers=headers, params=params, timeout=15)
        if r.status_code != 200:
            logger.warning(f"[BING IMG ERROR] {r.status_code} -> {r.text[:200]}")
            return {}
        return r.json()
    except Exception as e:
        logger.error(f"[BING IMG EXC] {e}")
        return {}


def search_product_with_bing(brand: str, code: str) -> dict:
    """
    Verilen marka + ürün kodu için:
      - ürün adı (web sonuçlarından)
      - datasheet pdf linki (web sonuçlarından)
      - ürün sayfası linki
      - image URL listesi (image search sonuçlarından)
    """
    result = {
        "product_name": "",
        "product_page_url": "",
        "datasheet_url": "",
        "image_urls": [],
        "status": "NOT_FOUND",
    }

    query_base = f'{brand} "{code}"'
    # --- WEB SEARCH ---
    web_json = bing_web_search(query_base + " datasheet")
    web_pages = web_json.get("webPages", {}).get("value", []) if web_json else []

    if web_pages:
        # İlk sonucu ürün sayfası gibi kabul ediyoruz
        first = web_pages[0]
        result["product_name"] = first.get("name", "") or ""
        result["product_page_url"] = first.get("url", "") or ""

        # İlk .pdf olan linki datasheet kabul et
        datasheet_url = ""
        for wp in web_pages:
            url = wp.get("url", "") or ""
            snippet = (wp.get("snippet", "") or "").lower()
            if ".pdf" in url.lower():
                datasheet_url = url
                break
            if "datasheet" in snippet and ".pdf" in url.lower():
                datasheet_url = url
                break

        result["datasheet_url"] = datasheet_url

    # --- IMAGE SEARCH ---
    img_json = bing_image_search(query_base + " product image")
    images = img_json.get("value", []) if img_json else []
    image_urls = []
    for img in images:
        url = img.get("contentUrl")
        if url and url not in image_urls:
            image_urls.append(url)

    result["image_urls"] = image_urls

    # Status belirle
    if web_pages or image_urls:
        if result["datasheet_url"] and image_urls:
            result["status"] = "OK"
        elif result["datasheet_url"] or image_urls or result["product_page_url"]:
            result["status"] = "PARTIAL"
        else:
            result["status"] = "NOT_FOUND"
    else:
        result["status"] = "NOT_FOUND"

    return result


# ------------------------------------------------------
# API: PROCESS PRODUCTS
# ------------------------------------------------------
@app.post("/process-products")
async def process_products(file: UploadFile = File(...)):
    logger.info("[API] /process-products çağrıldı")

    time_str = datetime.now().strftime("%d-%m-%Y-at-%H-%M")
    root_folder = f"Research-{time_str}"
    root_path = os.path.join(BASE_DIR, root_folder)

    images_root = os.path.join(root_path, "Images")
    datasheets_root = os.path.join(root_path, "Datasheets")
    os.makedirs(images_root, exist_ok=True)
    os.makedirs(datasheets_root, exist_ok=True)

    # Excel kaydet
    excel_path = os.path.join(root_path, "uploaded.xlsx")
    os.makedirs(root_path, exist_ok=True)
    with open(excel_path, "wb") as buffer:
        shutil.copyfileobj(file.file, buffer)

    df = pd.read_excel(excel_path)
    logger.info(f"[API] Excel rows: {len(df)}")

    output_rows = []

    for idx, row in df.iterrows():
        brand = str(row.get("brand", "")).strip()
        code = str(row.get("product_code", "")).strip()

        if not brand or not code:
            logger.warning(f"[ROW {idx}] brand veya product_code boş, atlandı.")
            continue

        logger.info(f"[ROW {idx}] {brand} - {code}")

        search_result = search_product_with_bing(brand, code)

        product_name = search_result["product_name"]
        product_page_url = search_result["product_page_url"]
        datasheet_url = search_result["datasheet_url"]
        image_urls = search_result["image_urls"]
        status = search_result["status"]

        saved_image_files = []

        # Görselleri indir
        if image_urls:
            img_dir = os.path.join(images_root, code)
            os.makedirs(img_dir, exist_ok=True)
            clean_brand = clean_filename(brand)
            count = 1
            for img_url in image_urls:
                filename = f"{code.lower()}-{clean_brand}-{count:02d}.webp"
                save_path = os.path.join(img_dir, filename)
                if download_image_to_webp(img_url, save_path):
                    saved_image_files.append(
                        os.path.relpath(save_path, root_path)
                    )
                    count += 1

        # Datasheet indir
        if datasheet_url:
            ds_filename = f"{code}-datasheet.pdf"
            ds_path = os.path.join(datasheets_root, ds_filename)
            if download_file(datasheet_url, ds_path):
                datasheet_local = os.path.relpath(ds_path, root_path)
            else:
                datasheet_local = ""
        else:
            datasheet_local = ""

        output_rows.append(
            {
                "brand": brand,
                "product_code": code,
                "product_name": product_name,
                "product_page_url": product_page_url,
                "datasheet_url": datasheet_url,
                "datasheet_file": datasheet_local,
                "image_urls": ";".join(image_urls),
                "saved_image_files": ";".join(saved_image_files),
                "status": status,
            }
        )

    # Excel output
    output_excel_path = os.path.join(root_path, "products_output.xlsx")
    if output_rows:
        pd.DataFrame(output_rows).to_excel(output_excel_path, index=False)
        logger.info(f"[API] Excel çıktı: {output_excel_path}")
    else:
        # Yine de boş bir Excel oluşturalım
        pd.DataFrame(
            columns=[
                "brand",
                "product_code",
                "product_name",
                "product_page_url",
                "datasheet_url",
                "datasheet_file",
                "image_urls",
                "saved_image_files",
                "status",
            ]
        ).to_excel(output_excel_path, index=False)
        logger.info("[API] Hiç sonuç bulunamadı, boş Excel oluşturuldu.")

    # ZIP
    zip_name = f"{root_folder}.zip"
    zip_path = os.path.join(BASE_DIR, zip_name)

    with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as z:
        for root_dir, _, files in os.walk(root_path):
            for f in files:
                fp = os.path.join(root_dir, f)
                rel = os.path.relpath(fp, root_path)
                z.write(fp, rel)

    logger.info(f"[API] DONE ✓ ZIP: {zip_name}")

    download_url = f"https://weltrada-automation.onrender.com/static/{zip_name}"

    return {
        "status": "success",
        "zip_file": zip_name,
        "download_url": download_url,
    }


# ------------------------------------------------------
# STATIC FILES (ZIP İNDİRME)
# ------------------------------------------------------
app.mount("/static", StaticFiles(directory=BASE_DIR), name="static")