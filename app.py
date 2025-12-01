# ------------------------------------------------------
# IMPORTS
# ------------------------------------------------------
from fastapi import FastAPI, UploadFile, File
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
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


# ------------------------------------------------------
# LOGGING
# ------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


# ------------------------------------------------------
# FASTAPI
# ------------------------------------------------------
app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# Render içindeki proje kökü
BASE_DIR = "/opt/render/project/src"


# ------------------------------------------------------
# UTILS
# ------------------------------------------------------
def clean_filename(s: str) -> str:
    """Dosya adı için basit temizleyici."""
    return "".join(
        c.lower()
        for c in s.replace(" ", "-")
        if c.isalnum() or c in ["-", "_"]
    )


def download_image_to_webp(url: str, save_path: str) -> bool:
    """URL'den görsel indirip .webp kaydeder."""
    try:
        logger.info(f"[IMG] {url}")
        r = requests.get(url, timeout=25)
        if r.status_code != 200:
            logger.warning(f"[IMG] {url} -> Status {r.status_code}")
            return False

        img = Image.open(BytesIO(r.content))
        img = img.convert("RGB")
        img.save(save_path, "webp")
        return True

    except Exception as e:
        logger.error(f"[IMG ERROR] {url} -> {e}")
        return False


def download_file(url: str, save_path: str) -> bool:
    """PDF vb. dosyayı indirip kaydeder."""
    try:
        logger.info(f"[FILE] {url}")
        r = requests.get(url, timeout=25)
        if r.status_code != 200:
            logger.warning(f"[FILE] {url} -> Status {r.status_code}")
            return False

        with open(save_path, "wb") as f:
            f.write(r.content)
        return True

    except Exception as e:
        logger.error(f"[FILE ERROR] {url} -> {e}")
        return False


# ------------------------------------------------------
# PARSERS (7 MARKA)
# ------------------------------------------------------

# ================= SCHNEIDER =================
def parse_schneider(code: str) -> dict:
    url_en = f"https://www.se.com/uk/en/product/{code}/"
    url_tr = f"https://www.se.com/tr/tr/product/{code}/"

    data = {
        "brand": "Schneider Electric",
        "code": code,
        "name_en": "",
        "name_tr": "",
        "breadcrumbs_en": "",
        "breadcrumbs_tr": "",
        "category_en": "",
        "category_tr": "",
        "datasheet_en": "",
        "datasheet_tr": "",
        "ean": "",
        "images": []
    }

    # EN
    try:
        r = requests.get(url_en, timeout=25)
        soup = BeautifulSoup(r.text, "html.parser")

        h = soup.find("h1")
        if h:
            data["name_en"] = h.text.strip()

        bc = soup.select("li[itemprop=itemListElement]")
        if bc:
            data["breadcrumbs_en"] = " > ".join(i.text.strip() for i in bc)
            if len(bc) >= 2:
                data["category_en"] = bc[-2].text.strip()

        imgs = soup.find_all("img")
        for img in imgs:
            src = img.get("src")
            if not src:
                continue
            if "/product/" in src:
                if src.startswith("//"):
                    src = "https:" + src
                if src.startswith("/"):
                    src = "https://www.se.com" + src
                data["images"].append(src)

        pdf = soup.find("a", href=lambda x: x and x.endswith(".pdf"))
        if pdf:
            link = pdf["href"]
            if link.startswith("/"):
                link = "https://www.se.com" + link
            data["datasheet_en"] = link

        gtin = soup.find("span", {"itemprop": "gtin13"})
        if gtin:
            data["ean"] = gtin.text.strip()

    except Exception as e:
        logger.error(f"[SCHNEIDER EN ERROR] {code} -> {e}")

    # TR
    try:
        r = requests.get(url_tr, timeout=25)
        soup = BeautifulSoup(r.text, "html.parser")

        h = soup.find("h1")
        if h:
            data["name_tr"] = h.text.strip()

        bc = soup.select("li[itemprop=itemListElement]")
        if bc:
            data["breadcrumbs_tr"] = " > ".join(i.text.strip() for i in bc)
            if len(bc) >= 2:
                data["category_tr"] = bc[-2].text.strip()

        pdf = soup.find("a", href=lambda x: x and x.endswith(".pdf"))
        if pdf:
            link = pdf["href"]
            if link.startswith("/"):
                link = "https://www.se.com" + link
            data["datasheet_tr"] = link

    except Exception as e:
        logger.error(f"[SCHNEIDER TR ERROR] {code} -> {e}")

    return data


# ================= ABB =================
def parse_abb(code: str) -> dict:
    url_en = f"https://new.abb.com/products/{code}"
    url_tr = f"https://new.abb.com/products/tr/{code}"

    data = {
        "brand": "ABB Group",
        "code": code,
        "name_en": "",
        "name_tr": "",
        "breadcrumbs_en": "",
        "breadcrumbs_tr": "",
        "category_en": "",
        "category_tr": "",
        "datasheet_en": "",
        "datasheet_tr": "",
        "images": [],
        "ean": ""
    }

    # EN
    try:
        r = requests.get(url_en, timeout=25)
        soup = BeautifulSoup(r.text, "html.parser")

        h = soup.find("h1")
        if h:
            data["name_en"] = h.text.strip()

        imgs = soup.find_all("img")
        for i in imgs:
            src = i.get("src")
            if src and code.lower() in src.lower():
                if src.startswith("/"):
                    src = "https://new.abb.com" + src
                data["images"].append(src)

        pdf = soup.find("a", href=lambda x: x and ".pdf" in x)
        if pdf:
            data["datasheet_en"] = pdf["href"]

    except Exception as e:
        logger.error(f"[ABB EN ERROR] {code} -> {e}")

    # TR
    try:
        r = requests.get(url_tr, timeout=25)
        soup = BeautifulSoup(r.text, "html.parser")

        h = soup.find("h1")
        if h:
            data["name_tr"] = h.text.strip()

        pdf = soup.find("a", href=lambda x: x and ".pdf" in x)
        if pdf:
            data["datasheet_tr"] = pdf["href"]

    except Exception as e:
        logger.error(f"[ABB TR ERROR] {code} -> {e}")

    return data


# ================= ALLEN BRADLEY =================
def parse_allen(code: str) -> dict:
    url = f"https://www.rockwellautomation.com/en-dk/products/details.{code}.html"

    data = {
        "brand": "Allen Bradley (Rockwell Automation)",
        "code": code,
        "name_en": "",
        "breadcrumbs_en": "",
        "category_en": "",
        "datasheet_en": "",
        "images": []
    }

    try:
        r = requests.get(url, timeout=25)
        soup = BeautifulSoup(r.text, "html.parser")

        h = soup.find("h1")
        if h:
            data["name_en"] = h.text.strip()

        crumbs = soup.select("li.breadcrumb-item")
        if crumbs:
            data["breadcrumbs_en"] = " > ".join(i.text.strip() for i in crumbs)
            if len(crumbs) >= 2:
                data["category_en"] = crumbs[-2].text.strip()

        imgs = soup.find_all("img")
        for i in imgs:
            src = i.get("src")
            if src and code in src:
                if src.startswith("/"):
                    src = "https://www.rockwellautomation.com" + src
                data["images"].append(src)

        pdf = soup.find("a", href=lambda x: x and x.endswith(".pdf"))
        if pdf:
            link = pdf["href"]
            if link.startswith("/"):
                link = "https://www.rockwellautomation.com" + link
            data["datasheet_en"] = link

    except Exception as e:
        logger.error(f"[ALLEN ERROR] {code} -> {e}")

    return data


# ================= EATON =================
def parse_eaton(code: str) -> dict:
    url_en = f"https://www.eaton.com/gb/en-gb/skuPage.{code}.html#tab-2"

    data = {
        "brand": "Eaton",
        "code": code,
        "name_en": "",
        "breadcrumbs_en": "",
        "category_en": "",
        "datasheet_en": "",
        "images": []
    }

    try:
        r = requests.get(url_en, timeout=25)
        soup = BeautifulSoup(r.text, "html.parser")

        h = soup.find("h1")
        if h:
            data["name_en"] = h.text.strip()

        imgs = soup.find_all("img")
        for i in imgs:
            src = i.get("src")
            if src and code.lower() in src.lower():
                if src.startswith("/"):
                    src = "https://www.eaton.com" + src
                data["images"].append(src)

    except Exception as e:
        logger.error(f"[EATON ERROR] {code} -> {e}")

    return data


# ================= LEGRAND =================
def parse_legrand(code: str) -> dict:
    # Örnek: sabit ürün sayfası (sen sadece kodu kullanıyorsun zaten)
    url = "https://www.legrand.at/de/katalog/produkte/innen-aussenwinkel-16x16-weiss-030191"

    data = {
        "brand": "Legrand",
        "code": code,
        "name_de": "",
        "breadcrumbs_de": "",
        "category_de": "",
        "images": [],
        "datasheet_en": "",
        "datasheet_tr": ""
    }

    try:
        r = requests.get(url, timeout=25)
        soup = BeautifulSoup(r.text, "html.parser")

        h = soup.find("h1")
        if h:
            data["name_de"] = h.text.strip()

        crumbs = soup.select("ul.breadcrumb li")
        if crumbs:
            data["breadcrumbs_de"] = " > ".join(i.text.strip() for i in crumbs)
            if len(crumbs) >= 2:
                data["category_de"] = crumbs[-2].text.strip()

        imgs = soup.find_all("img")
        for i in imgs:
            src = i.get("src")
            if src and "030191" in src:
                if src.startswith("/"):
                    src = "https://www.legrand.at" + src
                data["images"].append(src)

    except Exception as e:
        logger.error(f"[LEGRAND ERROR] {code} -> {e}")

    return data


# ================= WAGO =================
def parse_wago(code: str) -> dict:
    url_en = f"https://www.wago.com/global/marking/roller/p/{code}"

    data = {
        "brand": "Wago",
        "code": code,
        "name_en": "",
        "category_en": "",
        "breadcrumbs_en": "",
        "images": []
    }

    try:
        r = requests.get(url_en, timeout=25)
        soup = BeautifulSoup(r.text, "html.parser")

        h = soup.find("h1")
        if h:
            data["name_en"] = h.text.strip()

        imgs = soup.find_all("img")
        for i in imgs:
            src = i.get("src")
            if src and code in src:
                if src.startswith("/"):
                    src = "https://www.wago.com" + src
                data["images"].append(src)

    except Exception as e:
        logger.error(f"[WAGO ERROR] {code} -> {e}")

    return data


# ================= SIEMENS =================
def parse_siemens(code: str) -> dict:
    url = f"https://mall.industry.siemens.com/mall/en/oeii/Catalog/Product/{code}"

    data = {
        "brand": "Siemens",
        "code": code,
        "name_en": "",
        "category_en": "",
        "breadcrumbs_en": "",
        "images": []
    }

    try:
        r = requests.get(url, timeout=25)
        soup = BeautifulSoup(r.text, "html.parser")

        h = soup.find("h1")
        if h:
            data["name_en"] = h.text.strip()

        crumbs = soup.select("ul.breadcrumb li")
        if crumbs:
            data["breadcrumbs_en"] = " > ".join(i.text.strip() for i in crumbs)
            if len(crumbs) >= 2:
                data["category_en"] = crumbs[-2].text.strip()

        imgs = soup.find_all("img")
        for i in imgs:
            src = i.get("src")
            if src and code.lower() in src.lower():
                if src.startswith("/"):
                    src = "https://mall.industry.siemens.com" + src
                data["images"].append(src)

    except Exception as e:
        logger.error(f"[SIEMENS ERROR] {code} -> {e}")

    return data


# ------------------------------------------------------
# BRAND ROUTER
# ------------------------------------------------------
def scrape_by_brand(brand: str, code: str):
    b = (brand or "").lower().strip()
    logger.info(f"[SCRAPE] brand={brand}, code={code}")

    if "schneider" in b:
        return parse_schneider(code)
    if "abb" in b:
        return parse_abb(code)
    if "allen" in b or "rockwell" in b:
        return parse_allen(code)
    if "eaton" in b:
        return parse_eaton(code)
    if "legrand" in b:
        return parse_legrand(code)
    if "wago" in b:
        return parse_wago(code)
    if "siemens" in b:
        return parse_siemens(code)

    logger.warning(f"[SCRAPE] No parser found for brand {brand}")
    return None


# ------------------------------------------------------
# API: ROOT (opsiyonel, 404 yerine küçük mesaj)
# ------------------------------------------------------
@app.get("/")
def root():
    return {
        "message": "Weltrada Automation API is running.",
        "usage": "POST /process-products with form-data: file=<Excel>"
    }


# ------------------------------------------------------
# API: PROCESS PRODUCTS (NO MAIL)
# ------------------------------------------------------
@app.post("/process-products")
async def process_products(file: UploadFile = File(...)):
    logger.info("[API] /process-products")

    # Ana klasör
    time_str = datetime.now().strftime("%d-%m-%Y-at-%H-%M")
    root_folder = f"Research-{time_str}"
    root_path = os.path.join(BASE_DIR, root_folder)

    os.makedirs(root_path, exist_ok=True)
    os.makedirs(os.path.join(root_path, "Images"), exist_ok=True)
    os.makedirs(os.path.join(root_path, "Info/en/Breadcrumbs"), exist_ok=True)
    os.makedirs(os.path.join(root_path, "Info/tr/Sayfa-Yollari"), exist_ok=True)

    # Excel kaydet
    excel_path = os.path.join(root_path, "uploaded.xlsx")
    with open(excel_path, "wb") as buffer:
        shutil.copyfileobj(file.file, buffer)

    df = pd.read_excel(excel_path)
    logger.info(f"[API] Rows: {len(df)}")

    en_rows = []
    tr_rows = []

    for idx, row in df.iterrows():
        brand = str(row.get("brand", "")).strip()
        code = str(row.get("product_code", "")).strip().upper()

        if not brand or not code:
            logger.warning(f"[ROW {idx}] EMPTY brand/code")
            continue

        logger.info(f"[ROW {idx}] {brand} - {code}")

        d = scrape_by_brand(brand, code)
        if not d:
            logger.warning(f"[ROW {idx}] NO DATA - {brand} {code}")
            continue

        # EN Excel
        en_rows.append({
            "Product Code": code,
            "Brand": brand,
            "Product Name": d.get("name_en", d.get("name_de", "")),
            "Category": d.get("category_en", d.get("category_de", ""))
        })

        # TR Excel
        tr_rows.append({
            "Ürün kodu": code,
            "Marka": brand,
            "Ürün Adı": d.get("name_tr", d.get("name_en", d.get("name_de", ""))),
            "Kategori": d.get("category_tr", d.get("category_en", d.get("category_de", "")))
        })

        # Breadcrumb EN / TR
        bc_en = d.get("breadcrumbs_en") or d.get("breadcrumbs_de") or ""
        bc_tr = d.get("breadcrumbs_tr") or bc_en

        with open(os.path.join(root_path, f"Info/en/Breadcrumbs/{code}-breadcrumbs.txt"), "w") as f:
            f.write(bc_en)

        with open(os.path.join(root_path, f"Info/tr/Sayfa-Yollari/{code}-sayfa-yolu.txt"), "w") as f:
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

        # Datasheet EN
        if d.get("datasheet_en"):
            download_file(
                d["datasheet_en"],
                os.path.join(root_path, f"{code}-Datasheet-en.pdf")
            )

        # Datasheet TR
        if d.get("datasheet_tr"):
            download_file(
                d["datasheet_tr"],
                os.path.join(root_path, f"{code}-Datasheet-tr.pdf")
            )

    # Excel yaz
    if en_rows:
        pd.DataFrame(en_rows).to_excel(
            os.path.join(root_path, "Info/en/products-info.xlsx"),
            index=False
        )

    if tr_rows:
        pd.DataFrame(tr_rows).to_excel(
            os.path.join(root_path, "Info/tr/urun-detaylari.xlsx"),
            index=False
        )

    # ZIP DOSYASI
    zip_name = f"{root_folder}.zip"
    zip_path = os.path.join(BASE_DIR, zip_name)

    with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as z:
        for r, _, files in os.walk(root_path):
            for f in files:
                fp = os.path.join(r, f)
                rel = os.path.relpath(fp, root_path)
                z.write(fp, rel)

    logger.info(f"[API] DONE ✓ ZIP: {zip_name}")

    # Render'da /static altında serve ediyoruz
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