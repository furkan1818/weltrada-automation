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
from bs4 import BeautifulSoup
from PIL import Image


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

BASE_DIR = "/opt/render/project/src"


# ------------------------------------------------------
# ROOT (TEST İÇİN)
# ------------------------------------------------------
@app.get("/")
def root():
    return {
        "status": "ok",
        "message": "Weltrada Automation API. Use POST /process-products with an Excel file."
    }


# ------------------------------------------------------
# UTILS
# ------------------------------------------------------
def clean_filename(s: str) -> str:
    """
    Boşlukları - yapar, küçük harfe çevirir, sadece harf/rakam/-/_ bırakır.
    """
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
# PARSERS (TÜM MARKALAR)
# ------------------------------------------------------
# ------- SCHNEIDER -------
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

    headers = {
        "User-Agent": "Mozilla/5.0 (compatible; WeltradaBot/1.0; +https://weltrada.com)"
    }

    # EN ----------------------------------------------------------
    try:
        r = requests.get(url_en, timeout=25, headers=headers)
        soup = BeautifulSoup(r.text, "html.parser")

        # Ürün adı
        h = soup.find("h1")
        if h:
            data["name_en"] = h.get_text(strip=True)

        # Breadcrumbs + Category
        bc = soup.select("li[itemprop=itemListElement]")
        if bc:
            crumbs = [i.get_text(strip=True) for i in bc]
            data["breadcrumbs_en"] = " > ".join(crumbs)
            if len(crumbs) >= 2:
                data["category_en"] = crumbs[-2]

        # Görseller (product içeren img src)
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
                if src not in data["images"]:
                    data["images"].append(src)

        # Datasheet PDF
        pdf = soup.find("a", href=lambda x: x and x.endswith(".pdf"))
        if pdf:
            link = pdf["href"]
            if link.startswith("/"):
                link = "https://www.se.com" + link
            data["datasheet_en"] = link

        # EAN
        gtin = soup.find("span", {"itemprop": "gtin13"})
        if gtin:
            data["ean"] = gtin.get_text(strip=True)

    except Exception as e:
        logger.error(f"[SCHNEIDER EN ERROR] {code} -> {e}")

    # TR ----------------------------------------------------------
    try:
        r = requests.get(url_tr, timeout=25, headers=headers)
        soup = BeautifulSoup(r.text, "html.parser")

        # Ürün adı TR
        h = soup.find("h1")
        if h:
            data["name_tr"] = h.get_text(strip=True)

        # Breadcrumbs + Category TR
        bc = soup.select("li[itemprop=itemListElement]")
        if bc:
            crumbs = [i.get_text(strip=True) for i in bc]
            data["breadcrumbs_tr"] = " > ".join(crumbs)
            if len(crumbs) >= 2:
                data["category_tr"] = crumbs[-2]

        # Datasheet TR
        pdf = soup.find("a", href=lambda x: x and x.endswith(".pdf"))
        if pdf:
            link = pdf["href"]
            if link.startswith("/"):
                link = "https://www.se.com" + link
            data["datasheet_tr"] = link

    except Exception as e:
        logger.error(f"[SCHNEIDER TR ERROR] {code} -> {e}")

    return data


# ------- ABB -------
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

    headers = {
        "User-Agent": "Mozilla/5.0 (compatible; WeltradaBot/1.0; +https://weltrada.com)"
    }

    # EN ----------------------------------------------------------
    try:
        r = requests.get(url_en, timeout=25, headers=headers)
        soup = BeautifulSoup(r.text, "html.parser")

        h = soup.find("h1")
        if h:
            data["name_en"] = h.get_text(strip=True)

        # Breadcrumbs / kategori – site yapısına göre zayıf, gerekirse güçlendiririz
        bc = soup.select("nav.breadcrumb li, nav.breadcrumb a")
        if bc:
            crumbs = [i.get_text(strip=True) for i in bc]
            data["breadcrumbs_en"] = " > ".join(crumbs)
            if len(crumbs) >= 2:
                data["category_en"] = crumbs[-2]

        # Ürüne ait gövsel
        imgs = soup.find_all("img")
        for i in imgs:
            src = i.get("src")
            if not src:
                continue
            if code.lower() in src.lower():
                if src.startswith("/"):
                    src = "https://new.abb.com" + src
                if src not in data["images"]:
                    data["images"].append(src)

        # Datasheet PDF (href içinde .pdf)
        pdf = soup.find("a", href=lambda x: x and ".pdf" in x)
        if pdf:
            link = pdf["href"]
            if link.startswith("/"):
                link = "https://new.abb.com" + link
            data["datasheet_en"] = link

    except Exception as e:
        logger.error(f"[ABB EN ERROR] {code} -> {e}")

    # TR ----------------------------------------------------------
    try:
        r = requests.get(url_tr, timeout=25, headers=headers)
        soup = BeautifulSoup(r.text, "html.parser")

        h = soup.find("h1")
        if h:
            data["name_tr"] = h.get_text(strip=True)

        bc = soup.select("nav.breadcrumb li, nav.breadcrumb a")
        if bc:
            crumbs = [i.get_text(strip=True) for i in bc]
            data["breadcrumbs_tr"] = " > ".join(crumbs)
            if len(crumbs) >= 2:
                data["category_tr"] = crumbs[-2]

        pdf = soup.find("a", href=lambda x: x and ".pdf" in x)
        if pdf:
            link = pdf["href"]
            if link.startswith("/"):
                link = "https://new.abb.com" + link
            data["datasheet_tr"] = link

    except Exception as e:
        logger.error(f"[ABB TR ERROR] {code} -> {e}")

    return data


# ------- ALLEN BRADLEY -------
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

    headers = {
        "User-Agent": "Mozilla/5.0 (compatible; WeltradaBot/1.0; +https://weltrada.com)"
    }

    try:
        r = requests.get(url, timeout=25, headers=headers)
        soup = BeautifulSoup(r.text, "html.parser")

        h = soup.find("h1")
        if h:
            data["name_en"] = h.get_text(strip=True)

        crumbs = soup.select("li.breadcrumb-item")
        if crumbs:
            crumb_texts = [i.get_text(strip=True) for i in crumbs]
            data["breadcrumbs_en"] = " > ".join(crumb_texts)
            if len(crumb_texts) >= 2:
                data["category_en"] = crumb_texts[-2]

        imgs = soup.find_all("img")
        for i in imgs:
            src = i.get("src")
            if not src:
                continue
            if code in src:
                if src.startswith("/"):
                    src = "https://www.rockwellautomation.com" + src
                if src not in data["images"]:
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


# ------- EATON -------
def parse_eaton(code: str) -> dict:
    url_en = f"https://www.eaton.com/gb/en-gb/skuPage.{code}.html#tab-2"

    data = {
        "brand": "Eaton",
        "code": code,
        "name_en": "",
        "name_tr": "",
        "breadcrumbs_en": "",
        "breadcrumbs_tr": "",
        "category_en": "",
        "category_tr": "",
        "datasheet_en": "",
        "datasheet_tr": "",
        "images": []
    }

    headers = {
        "User-Agent": "Mozilla/5.0 (compatible; WeltradaBot/1.0; +https://weltrada.com)"
    }

    try:
        r = requests.get(url_en, timeout=25, headers=headers)
        soup = BeautifulSoup(r.text, "html.parser")

        h = soup.find("h1")
        if h:
            data["name_en"] = h.get_text(strip=True)

        # (Breadcrumb yapısı siteye göre değişebilir; şu an sadece görsel + isim)
        imgs = soup.find_all("img")
        for i in imgs:
            src = i.get("src")
            if not src:
                continue
            if code.lower() in src.lower():
                if src.startswith("/"):
                    src = "https://www.eaton.com" + src
                if src not in data["images"]:
                    data["images"].append(src)

    except Exception as e:
        logger.error(f"[EATON ERROR] {code} -> {e}")

    return data


# ------- LEGRAND -------
def parse_legrand(code: str) -> dict:
    # Şimdilik tek örnek ürün üzerinden gidiyoruz (030191).
    # Legrand sitesi product code ile URL almaya izin veriyorsa sonra güncelleriz.
    url = "https://www.legrand.at/de/katalog/produkte/innen-aussenwinkel-16x16-weiss-030191"

    data = {
        "brand": "Legrand",
        "code": code,
        "name_de": "",
        "breadcrumbs_de": "",
        "category_de": "",
        "datasheet_en": "",
        "datasheet_tr": "",
        "images": []
    }

    headers = {
        "User-Agent": "Mozilla/5.0 (compatible; WeltradaBot/1.0; +https://weltrada.com)"
    }

    try:
        r = requests.get(url, timeout=25, headers=headers)
        soup = BeautifulSoup(r.text, "html.parser")

        h = soup.find("h1")
        if h:
            data["name_de"] = h.get_text(strip=True)

        crumbs = soup.select("ul.breadcrumb li")
        if crumbs:
            crumb_texts = [i.get_text(strip=True) for i in crumbs]
            data["breadcrumbs_de"] = " > ".join(crumb_texts)
            if len(crumb_texts) >= 2:
                data["category_de"] = crumb_texts[-2]

        imgs = soup.find_all("img")
        for i in imgs:
            src = i.get("src")
            if not src:
                continue
            if "030191" in src:
                if src.startswith("/"):
                    src = "https://www.legrand.at" + src
                if src not in data["images"]:
                    data["images"].append(src)

    except Exception as e:
        logger.error(f"[LEGRAND ERROR] {code} -> {e}")

    return data


# ------- WAGO -------
def parse_wago(code: str) -> dict:
    url_en = f"https://www.wago.com/global/marking/roller/p/{code}"

    data = {
        "brand": "Wago",
        "code": code,
        "name_en": "",
        "name_tr": "",
        "breadcrumbs_en": "",
        "breadcrumbs_tr": "",
        "category_en": "",
        "category_tr": "",
        "datasheet_en": "",
        "datasheet_tr": "",
        "images": []
    }

    headers = {
        "User-Agent": "Mozilla/5.0 (compatible; WeltradaBot/1.0; +https://weltrada.com)"
    }

    try:
        r = requests.get(url_en, timeout=25, headers=headers)
        soup = BeautifulSoup(r.text, "html.parser")

        h = soup.find("h1")
        if h:
            data["name_en"] = h.get_text(strip=True)

        imgs = soup.find_all("img")
        for i in imgs:
            src = i.get("src")
            if not src:
                continue
            if code in src:
                if src.startswith("/"):
                    src = "https://www.wago.com" + src
                if src not in data["images"]:
                    data["images"].append(src)

    except Exception as e:
        logger.error(f"[WAGO ERROR] {code} -> {e}")

    return data


# ------- SIEMENS -------
def parse_siemens(code: str) -> dict:
    url = f"https://mall.industry.siemens.com/mall/en/oeii/Catalog/Product/{code}"

    data = {
        "brand": "Siemens",
        "code": code,
        "name_en": "",
        "breadcrumbs_en": "",
        "category_en": "",
        "datasheet_en": "",
        "datasheet_tr": "",
        "images": []
    }

    headers = {
        "User-Agent": "Mozilla/5.0 (compatible; WeltradaBot/1.0; +https://weltrada.com)"
    }

    try:
        r = requests.get(url, timeout=25, headers=headers)
        soup = BeautifulSoup(r.text, "html.parser")

        h = soup.find("h1")
        if h:
            data["name_en"] = h.get_text(strip=True)

        crumbs = soup.select("ul.breadcrumb li")
        if crumbs:
            crumb_texts = [i.get_text(strip=True) for i in crumbs]
            data["breadcrumbs_en"] = " > ".join(crumb_texts)
            if len(crumb_texts) >= 2:
                data["category_en"] = crumb_texts[-2]

        imgs = soup.find_all("img")
        for i in imgs:
            src = i.get("src")
            if not src:
                continue
            if code.lower() in src.lower():
                if src.startswith("/"):
                    src = "https://mall.industry.siemens.com" + src
                if src not in data["images"]:
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
# API: PROCESS PRODUCTS (NO MAIL)
# ------------------------------------------------------
@app.post("/process-products")
async def process_products(file: UploadFile = File(...)):
    logger.info("[API] /process-products")

    # Ana klasör
    time_str = datetime.now().strftime("%d-%m-%Y-at-%H-%M")
    root_folder = f"Research-{time_str}"
    root_path = os.path.join(BASE_DIR, root_folder)

    # Klasörler
    images_root = os.path.join(root_path, "Images")
    info_en_breadcrumbs = os.path.join(root_path, "Info", "en", "Breadcrumbs")
    info_tr_breadcrumbs = os.path.join(root_path, "Info", "tr", "Sayfa-Yolları")
    datasheets_root = os.path.join(root_path, "Datasheets")

    os.makedirs(images_root, exist_ok=True)
    os.makedirs(info_en_breadcrumbs, exist_ok=True)
    os.makedirs(info_tr_breadcrumbs, exist_ok=True)
    os.makedirs(datasheets_root, exist_ok=True)

    # Excel kaydet
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

        # EN Excel satırı
        en_rows.append({
            "Product Code": code,
            "Brand": brand,
            "Product Name": d.get("name_en", d.get("name_de", "")),
            "Category": d.get("category_en", d.get("category_de", ""))
        })

        # TR Excel satırı
        tr_rows.append({
            "Ürün kodu": code,
            "Marka": brand,
            "Ürün Adı": d.get("name_tr", d.get("name_en", d.get("name_de", ""))),
            "Kategori": d.get("category_tr", d.get("category_en", d.get("category_de", "")))
        })

        # Breadcrumb EN / TR
        bc_en = d.get("breadcrumbs_en", d.get("breadcrumbs_de", ""))
        bc_tr = d.get("breadcrumbs_tr", bc_en)

        with open(os.path.join(info_en_breadcrumbs, f"{code}-breadcrumbs.txt"), "w", encoding="utf-8") as f_en:
            f_en.write(bc_en or "")

        with open(os.path.join(info_tr_breadcrumbs, f"{code}-sayfa-yolu.txt"), "w", encoding="utf-8") as f_tr:
            f_tr.write(bc_tr or "")

        # Görseller
        img_dir = os.path.join(images_root, code)
        os.makedirs(img_dir, exist_ok=True)

        count = 1
        for url in d.get("images", []):
            filename = f"{clean_filename(brand)}-{code.lower()}-{count:03d}.webp"
            save_path = os.path.join(img_dir, filename)
            if download_image_to_webp(url, save_path):
                count += 1

        # Datasheet EN
        if d.get("datasheet_en"):
            ds_en_path = os.path.join(datasheets_root, f"{code}-datasheet-en.pdf")
            download_file(d["datasheet_en"], ds_en_path)

        # Datasheet TR
        if d.get("datasheet_tr"):
            ds_tr_path = os.path.join(datasheets_root, f"{code}-datasheet-tr.pdf")
            download_file(d["datasheet_tr"], ds_tr_path)

    # Excel dosyaları
    info_en_dir = os.path.join(root_path, "Info", "en")
    info_tr_dir = os.path.join(root_path, "Info", "tr")
    os.makedirs(info_en_dir, exist_ok=True)
    os.makedirs(info_tr_dir, exist_ok=True)

    if en_rows:
        pd.DataFrame(en_rows).to_excel(
            os.path.join(info_en_dir, "products-info.xlsx"),
            index=False
        )

    if tr_rows:
        pd.DataFrame(tr_rows).to_excel(
            os.path.join(info_tr_dir, "urun-detaylari.xlsx"),
            index=False
        )

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

    # Render static URL
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