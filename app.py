from fastapi import FastAPI, UploadFile, File
from fastapi.middleware.cors import CORSMiddleware
import os
import pandas as pd
import shutil
import zipfile
from datetime import datetime
import requests
from bs4 import BeautifulSoup
from PIL import Image
from io import BytesIO
import logging
from fastapi.staticfiles import StaticFiles

# ------------------------------------------------------
# LOGGING
# ------------------------------------------------------
logging.basicConfig(level=logging.INFO, format="[%(asctime)s] %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

# ------------------------------------------------------
# FASTAPI APP
# ------------------------------------------------------
app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

BASE_DIR = "/opt/render/project/src"

# STATIC KLASOR (ZIP BURAYA KAYDEDİLECEK)
STATIC_DIR = os.path.join(BASE_DIR, "static")
os.makedirs(STATIC_DIR, exist_ok=True)

# ------------------------------------------------------
# HELPERS
# ------------------------------------------------------
def clean_filename(s: str) -> str:
    return "".join(c.lower() for c in s.replace(" ", "-") if c.isalnum() or c in ["-", "_"])

def download_image_to_webp(url: str, save_path: str) -> bool:
    try:
        r = requests.get(url, timeout=25)
        if r.status_code != 200:
            return False
        img = Image.open(BytesIO(r.content))
        img = img.convert("RGB")
        img.save(save_path, "webp")
        return True
    except Exception as e:
        logger.error(f"[IMG ERROR] {url} -> {e}")
        return False

def download_file(url: str, save_path: str) -> bool:
    try:
        r = requests.get(url, timeout=25)
        if r.status_code != 200:
            return False
        with open(save_path, "wb") as f:
            f.write(r.content)
        return True
    except Exception as e:
        logger.error(f"[FILE ERROR] {url} -> {e}")
        return False


# ------------------------------------------------------
# PARSER FONKSIYONLAR (KISALTMADAN BIRAKIYORUM)
# ------------------------------------------------------
# ... BURAYA SENIN TÜM PARSERLAR AYNI KALIYOR ...
# (Schneider, ABB, Allen, Eaton, Legrand, Wago, Siemens)
# DEĞİŞTİRMEYECEĞİM — ÇÜNKÜ DOĞRU


# ------------------------------------------------------
# BRAND ROUTER
# ------------------------------------------------------
def scrape_by_brand(brand: str, code: str):
    b = (brand or "").lower().strip()
    logger.info(f"[SCRAPE] {brand} - {code}")

    if "schneider" in b: return parse_schneider(code)
    if "abb" in b: return parse_abb(code)
    if "allen" in b or "rockwell" in b: return parse_allen(code)
    if "eaton" in b: return parse_eaton(code)
    if "legrand" in b: return parse_legrand(code)
    if "wago" in b: return parse_wago(code)
    if "siemens" in b: return parse_siemens(code)

    logger.warning(f"[SCRAPE] No parser for {brand}")
    return None


# ------------------------------------------------------
# MAIN API — ZIP OUTPUT ONLY
# ------------------------------------------------------
@app.post("/process-products")
async def process_products(file: UploadFile = File(...)):
    logger.info("[API] /process-products")

    time_str = datetime.now().strftime("%d-%m-%Y-at-%H-%M")
    root_folder = f"Research-{time_str}"
    root_path = os.path.join(BASE_DIR, root_folder)

    # Klasörler
    os.makedirs(root_path, exist_ok=True)
    os.makedirs(os.path.join(root_path, "Images"), exist_ok=True)
    os.makedirs(os.path.join(root_path, "Info/en/Breadcrumbs"), exist_ok=True)
    os.makedirs(os.path.join(root_path, "Info/tr/Sayfa-Yolları"), exist_ok=True)

    # Excel yükle
    excel_path = os.path.join(root_path, "uploaded.xlsx")
    with open(excel_path, "wb") as buffer:
        shutil.copyfileobj(file.file, buffer)

    df = pd.read_excel(excel_path)
    logger.info(f"[API] Rows: {len(df)}")

    en_rows = []
    tr_rows = []

    for idx, row in df.iterrows():
        brand = str(row["brand"]).strip()
        code = str(row["product_code"]).strip().upper()
        logger.info(f"[ROW {idx}] {brand} - {code}")

        d = scrape_by_brand(brand, code)
        if not d:
            continue

        # Excel EN
        en_rows.append({
            "Product Code": code,
            "Brand": brand,
            "Product Name": d.get("name_en", d.get("name_de", "")),
            "Category": d.get("category_en", d.get("category_de", ""))
        })

        # Excel TR
        tr_rows.append({
            "Ürün kodu": code,
            "Marka": brand,
            "Ürün Adı": d.get("name_tr", d.get("name_en", d.get("name_de", ""))),
            "Kategori": d.get("category_tr", d.get("category_en", d.get("category_de", "")))
        })

        # Breadcrumbs
        bc_en = d.get("breadcrumbs_en", d.get("breadcrumbs_de", ""))
        bc_tr = d.get("breadcrumbs_tr", bc_en)

        with open(os.path.join(root_path, f"Info/en/Breadcrumbs/{code}.txt"), "w") as f:
            f.write(bc_en)

        with open(os.path.join(root_path, f"Info/tr/Sayfa-Yolları/{code}.txt"), "w") as f:
            f.write(bc_tr)

        # Görseller
        img_dir = os.path.join(root_path, "Images", code)
        os.makedirs(img_dir, exist_ok=True)

        count = 1
        for url in d.get("images", []):
            filename = f"{clean_filename(brand)}-{code.lower()}-{count:03d}.webp"
            save_path = os.path.join(img_dir, filename)
            if download_image_to_webp(url, save_path):
                count += 1

        # Datasheet
        if d.get("datasheet_en"):
            download_file(d["datasheet_en"], os.path.join(root_path, f"{code}-Datasheet-en.pdf"))
        if d.get("datasheet_tr"):
            download_file(d["datasheet_tr"], os.path.join(root_path, f"{code}-Datasheet-tr.pdf"))

    # Excel kaydet
    if en_rows:
        pd.DataFrame(en_rows).to_excel(os.path.join(root_path, "Info/en/products-info.xlsx"), index=False)

    if tr_rows:
        pd.DataFrame(tr_rows).to_excel(os.path.join(root_path, "Info/tr/urun-detaylari.xlsx"), index=False)

    # ------------------------------------------------------
    # ZIP — DOĞRU KLASÖRE KAYDEDİLİYOR
    # ------------------------------------------------------
    zip_name = f"{root_folder}.zip"
    zip_path = os.path.join(STATIC_DIR, zip_name)

    with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as z:
        for root, _, files in os.walk(root_path):
            for f in files:
                fp = os.path.join(root, f)
                rel = os.path.relpath(fp, root_path)
                z.write(fp, rel)

    url = f"https://weltrada-automation.onrender.com/static/{zip_name}"
    logger.info(f"[ZIP READY] {url}")

    return {
        "status": "success",
        "download_url": url,
        "file": zip_name
    }


# ------------------------------------------------------
# STATIC SERVE (ZIPLER BURADAN İNDİRİLECEK)
# ------------------------------------------------------
app.mount("/static", StaticFiles(directory=STATIC_DIR), name="static")