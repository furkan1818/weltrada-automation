from fastapi import FastAPI, UploadFile, File
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles

import os
import shutil
import zipfile
from datetime import datetime
from io import BytesIO
import time
from urllib.parse import urljoin

import logging
import pandas as pd
import requests
from bs4 import BeautifulSoup
from PIL import Image
from googlesearch import search


# ------------------------------------------------------
# LOGGING
# ------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


# ------------------------------------------------------
# FASTAPI CONFIG
# ------------------------------------------------------
app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# Render.com çalışma dizini
BASE_DIR = "/opt/render/project/src"


# ------------------------------------------------------
# ROOT (TEST İÇİN)
# ------------------------------------------------------
@app.get("/")
def root():
    return {
        "status": "ok",
        "message": "Weltrada Research Automation API. Use POST /process-products with an Excel file."
    }


# ------------------------------------------------------
# UTILS
# ------------------------------------------------------
def clean_filename(s: str) -> str:
    """
    Boşlukları - yapar, küçük harfe çevirir,
    sadece harf/rakam/-/_ bırakır.
    """
    return "".join(
        c.lower()
        for c in s.replace(" ", "-")
        if c.isalnum() or c in ["-", "_"]
    )


def safe_request(url: str, timeout: int = 15):
    """
    Basit GET isteği, hata ya da 200 dışı durumda None döner.
    """
    try:
        headers = {
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/130.0 Safari/537.36 WeltradaBot/1.0"
            )
        }
        resp = requests.get(url, headers=headers, timeout=timeout)
        if resp.status_code == 200:
            return resp
        logger.warning(f"[REQUEST] {url} -> {resp.status_code}")
        return None
    except Exception as e:
        logger.error(f"[REQUEST ERROR] {url} -> {e}")
        return None


def normalize_url(src: str, base_url: str) -> str | None:
    """
    /image.jpg, //cdn.site.com/x.png gibi linkleri normalize eder.
    """
    if not src:
        return None

    src = src.strip()
    if src.startswith("data:"):
        return None

    if src.startswith("//"):
        return "https:" + src

    if src.startswith("http://") or src.startswith("https://"):
        return src

    return urljoin(base_url, src)


def google_search_product(brand: str, product_code: str, max_results: int = 3):
    """
    Google'da 'brand product_code datasheet' araması yapar.
    """
    query = f'"{brand}" "{product_code}" datasheet'
    logger.info(f"[GOOGLE] {query}")
    try:
        results = list(search(query, num_results=max_results))
        logger.info(f"[GOOGLE] {len(results)} sonuç")
        return results
    except Exception as e:
        logger.error(f"[GOOGLE ERROR] {e}")
        return []


def extract_product_info_from_page(url: str, brand: str, product_code: str) -> dict | None:
    """
    Tek bir sayfadan ürün adı, görseller ve datasheet linkini almaya çalışır.
    """
    resp = safe_request(url)
    if not resp:
        return None

    soup = BeautifulSoup(resp.text, "html.parser")

    # Ürün adı (önce h1, sonra title)
    product_name = ""
    h1 = soup.find("h1")
    if h1:
        product_name = h1.get_text(strip=True)
    elif soup.title and soup.title.string:
        product_name = soup.title.string.strip()

    # Basit bir doğrulama: isim içinde marka ya da ürün kodu geçsin
    lower_name = product_name.lower()
    if product_code.lower() not in lower_name and brand.lower().split()[0] not in lower_name:
        logger.info(f"[PAGE] {url} -> İsim zayıf eşleşme: '{product_name}'")

    # Datasheet PDF linki
    datasheet_url = ""
    for a in soup.find_all("a", href=True):
        href = a["href"]
        href_lower = href.lower()
        text = a.get_text(strip=True).lower()

        if ".pdf" in href_lower and any(
            kw in text for kw in ["datasheet", "data sheet", "technical", "spec", "catalog", "manual"]
        ):
            full = normalize_url(href, url)
            if full:
                datasheet_url = full
                break

    # Görseller
    image_urls: list[str] = []
    for img in soup.find_all("img"):
        src = img.get("src")
        if not src:
            continue

        src_lower = src.lower()
        if any(ext in src_lower for ext in [".jpg", ".jpeg", ".png", ".webp"]):
            full = normalize_url(src, url)
            if not full:
                continue
            # Çok küçük ikonları elemek için basit bir width/height filtresi denenebilir
            if full not in image_urls:
                image_urls.append(full)

    logger.info(f"[PAGE] {url} -> name='{product_name}', images={len(image_urls)}, pdf={'yes' if datasheet_url else 'no'}")

    return {
        "product_name": product_name,
        "image_urls": image_urls,
        "datasheet_url": datasheet_url,
        "page_url": url,
    }


def download_image_to_webp(url: str, save_path: str) -> bool:
    """
    Verilen URL'den görsel indir, RGB'ye çevirip .webp kaydet.
    """
    try:
        logger.info(f"[IMG] {url}")
        r = safe_request(url, timeout=25)
        if not r:
            return False

        img = Image.open(BytesIO(r.content)).convert("RGB")
        img.save(save_path, "WEBP", quality=85)
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
        r = safe_request(url, timeout=30)
        if not r:
            return False

        with open(save_path, "wb") as f:
            f.write(r.content)
        return True

    except Exception as e:
        logger.error(f"[FILE ERROR] {url} -> {e}")
        return False


# ------------------------------------------------------
# API: PROCESS PRODUCTS
# ------------------------------------------------------
@app.post("/process-products")
async def process_products(file: UploadFile = File(...)):
    logger.info("[API] /process-products çağrıldı")

    # Ana klasör
    time_str = datetime.now().strftime("%d-%m-%Y-at-%H-%M")
    root_folder = f"Research-{time_str}"
    root_path = os.path.join(BASE_DIR, root_folder)

    images_root = os.path.join(root_path, "Images")
    datasheets_root = os.path.join(root_path, "Datasheets")
    os.makedirs(images_root, exist_ok=True)
    os.makedirs(datasheets_root, exist_ok=True)

    # Excel'i kaydet
    excel_path = os.path.join(root_path, "uploaded.xlsx")
    os.makedirs(root_path, exist_ok=True)
    with open(excel_path, "wb") as buffer:
        shutil.copyfileobj(file.file, buffer)

    # Excel oku
    df = pd.read_excel(excel_path)
    logger.info(f"[API] Excel rows: {len(df)}")

    # Gerekli sütunlar: brand, product_code
    for col in ["brand", "product_code"]:
        if col not in df.columns:
            return {
                "status": "error",
                "message": f"Excel içinde '{col}' adlı sütun bulunamadı."
            }

    output_rows = []

    for idx, row in df.iterrows():
        brand = str(row["brand"]).strip()
        product_code = str(row["product_code"]).strip()

        if not brand or not product_code:
            logger.warning(f"[ROW {idx}] Boş brand/product_code, atlandı.")
            continue

        logger.info(f"[ROW {idx}] {brand} - {product_code}")

        product_name = ""
        datasheet_url = ""
        image_urls: list[str] = []
        saved_image_files: list[str] = []
        status = "NOT_FOUND"

        # 1) Google araması
        urls = google_search_product(brand, product_code, max_results=3)

        # 2) Sonuç sayfalarını gez
        for url in urls:
            info = extract_product_info_from_page(url, brand, product_code)
            if not info:
                continue

            name = info["product_name"] or ""
            # Ürün kodu veya marka ismi geçiyorsa güvenilir sayalım
            if product_code.lower() in name.lower() or brand.lower().split()[0] in name.lower():
                product_name = info["product_name"]
                datasheet_url = info["datasheet_url"]
                image_urls = info["image_urls"]
                status = "OK"
                break

        # Eğer güçlü eşleşme yoksa ama en az bir sonuç varsa ilk sayfayı yine de kullanabiliriz
        if status != "OK" and urls:
            fallback_info = extract_product_info_from_page(urls[0], brand, product_code)
            if fallback_info:
                product_name = fallback_info["product_name"]
                datasheet_url = fallback_info["datasheet_url"]
                image_urls = fallback_info["image_urls"]
                status = "PARTIAL"

        # 3) Görselleri indir + webp
        if image_urls:
            product_dir = os.path.join(images_root, product_code)
            os.makedirs(product_dir, exist_ok=True)

            clean_brand = clean_filename(brand)
            base_name = f"{product_code.lower()}-{clean_brand}"

            max_images = 3
            count = 0
            for i, img_url in enumerate(image_urls):
                if count >= max_images:
                    break
                filename = f"{base_name}-{count+1}.webp"
                save_path = os.path.join(product_dir, filename)
                if download_image_to_webp(img_url, save_path):
                    saved_image_files.append(filename)
                    count += 1

        # 4) Datasheet indir
        if datasheet_url:
            ds_filename = f"{product_code}-datasheet.pdf"
            ds_path = os.path.join(datasheets_root, ds_filename)
            if not download_file(datasheet_url, ds_path):
                # İndiremezsek sadece URL kaydedilir
                logger.warning(f"[ROW {idx}] Datasheet indirilemedi, sadece URL tutulacak.")

        # 5) Çıktı satırı
        output_rows.append({
            "brand": brand,
            "product_code": product_code,
            "product_name": product_name,
            "datasheet_url": datasheet_url,
            "image_urls": "|".join(image_urls),
            "saved_image_files": "|".join(saved_image_files),
            "status": status
        })

        # Google'a çok yüklenmemek için
        time.sleep(3)

    # Çıkış Excel
    output_df = pd.DataFrame(output_rows)
    output_excel_path = os.path.join(root_path, "products_output.xlsx")
    output_df.to_excel(output_excel_path, index=False)
    logger.info(f"[API] Excel çıktı: {output_excel_path}")

    # ZIP DOSYASI
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
        "download_url": download_url
    }


# ------------------------------------------------------
# STATIC FILES (ZIP İNDİRME)
# ------------------------------------------------------
app.mount("/static", StaticFiles(directory=BASE_DIR), name="static")