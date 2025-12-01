from fastapi import FastAPI, UploadFile, File
from fastapi.middleware.cors import CORSMiddleware
import uvicorn
import os
import pandas as pd
import shutil
import zipfile
from datetime import datetime
import requests
from bs4 import BeautifulSoup
from PIL import Image
from io import BytesIO
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email import encoders
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
# FASTAPI CONFIG
# ------------------------------------------------------
app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# Render içinde proje kökü
BASE_DIR = "/opt/render/project/src"


# ------------------------------------------------------
# UTILS
# ------------------------------------------------------
def clean_filename(s: str) -> str:
    return "".join(
        c.lower()
        for c in s.replace(" ", "-")
        if c.isalnum() or c in ["-", "_"]
    )


def download_image_to_webp(url: str, save_path: str) -> bool:
    try:
        logger.info(f"[IMG] Download: {url}")
        r = requests.get(url, timeout=20)
        if r.status_code != 200:
            logger.warning(f"[IMG] Status {r.status_code} for {url}")
            return False

        img = Image.open(BytesIO(r.content))
        img = img.convert("RGB")
        img.save(save_path, "webp")
        return True
    except Exception as e:
        logger.error(f"[IMG] Error {url} -> {e}")
        return False


def download_file(url: str, save_path: str) -> bool:
    try:
        logger.info(f"[FILE] Download: {url}")
        r = requests.get(url, timeout=30)
        if r.status_code != 200:
            logger.warning(f"[FILE] Status {r.status_code} for {url}")
            return False

        with open(save_path, "wb") as f:
            f.write(r.content)
        return True
    except Exception as e:
        logger.error(f"[FILE] Error {url} -> {e}")
        return False


# ------------------------------------------------------
# SCHNEIDER
# ------------------------------------------------------
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
        "ean": "",
        "datasheet_en": "",
        "datasheet_tr": "",
        "images": []
    }

    # EN
    try:
        r = requests.get(url_en, timeout=25)
        soup = BeautifulSoup(r.text, "html.parser")

        h1 = soup.find("h1")
        if h1:
            data["name_en"] = h1.text.strip()

        breadcrumb = soup.select("li[itemprop=itemListElement]")
        if breadcrumb:
            bc_text = " > ".join(b.text.strip() for b in breadcrumb)
            data["breadcrumbs_en"] = bc_text
            if len(breadcrumb) >= 2:
                data["category_en"] = breadcrumb[-2].text.strip()

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
            link = pdf.get("href")
            if link.startswith("/"):
                link = "https://www.se.com" + link
            data["datasheet_en"] = link

        gtin = soup.find("span", {"itemprop": "gtin13"})
        if gtin:
            data["ean"] = gtin.text.strip()
    except Exception as e:
        logger.error(f"[SCHNEIDER EN] {code}: {e}")

    # TR
    try:
        r = requests.get(url_tr, timeout=25)
        soup = BeautifulSoup(r.text, "html.parser")

        h1 = soup.find("h1")
        if h1:
            data["name_tr"] = h1.text.strip()

        breadcrumb = soup.select("li[itemprop=itemListElement]")
        if breadcrumb:
            bc_text = " > ".join(b.text.strip() for b in breadcrumb)
            data["breadcrumbs_tr"] = bc_text
            if len(breadcrumb) >= 2:
                data["category_tr"] = breadcrumb[-2].text.strip()

        pdf = soup.find("a", href=lambda x: x and x.endswith(".pdf"))
        if pdf:
            link = pdf.get("href")
            if link.startswith("/"):
                link = "https://www.se.com" + link
            data["datasheet_tr"] = link
    except Exception as e:
        logger.error(f"[SCHNEIDER TR] {code}: {e}")

    return data


# ------------------------------------------------------
# ABB
# ------------------------------------------------------
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
        "ean": "",
        "datasheet_en": "",
        "datasheet_tr": "",
        "images": []
    }

    # EN
    try:
        r = requests.get(url_en, timeout=25)
        soup = BeautifulSoup(r.text, "html.parser")

        h1 = soup.find("h1")
        if h1:
            data["name_en"] = h1.text.strip()

        imgs = soup.find_all("img")
        for img in imgs:
            src = img.get("src")
            if not src:
                continue
            if code.lower() in src.lower():
                if src.startswith("/"):
                    src = "https://new.abb.com" + src
                data["images"].append(src)

        pdf = soup.find("a", href=lambda x: x and ".pdf" in x)
        if pdf:
            data["datasheet_en"] = pdf["href"]
    except Exception as e:
        logger.error(f"[ABB EN] {code}: {e}")

    # TR
    try:
        r = requests.get(url_tr, timeout=25)
        soup = BeautifulSoup(r.text, "html.parser")

        h1 = soup.find("h1")
        if h1:
            data["name_tr"] = h1.text.strip()

        pdf = soup.find("a", href=lambda x: x and ".pdf" in x)
        if pdf:
            data["datasheet_tr"] = pdf["href"]
    except Exception as e:
        logger.error(f"[ABB TR] {code}: {e}")

    return data


# ------------------------------------------------------
# ALLEN BRADLEY (ROCKWELL)
# ------------------------------------------------------
def parse_allen(code: str) -> dict:
    url = f"https://www.rockwellautomation.com/en-dk/products/details.{code}.html"

    data = {
        "brand": "Allen Bradley (Rockwell Automation)",
        "code": code,
        "name_en": "",
        "breadcrumbs_en": "",
        "category_en": "",
        "ean": "",
        "datasheet_en": "",
        "datasheet_tr": "",
        "images": []
    }

    try:
        r = requests.get(url, timeout=25)
        soup = BeautifulSoup(r.text, "html.parser")

        h1 = soup.find("h1")
        if h1:
            data["name_en"] = h1.text.strip()

        crumbs = soup.select("li.breadcrumb-item")
        if crumbs:
            bc_text = " > ".join(c.text.strip() for c in crumbs)
            data["breadcrumbs_en"] = bc_text
            if len(crumbs) >= 2:
                data["category_en"] = crumbs[-2].text.strip()

        imgs = soup.find_all("img")
        for img in imgs:
            src = img.get("src")
            if not src:
                continue
            if code in src:
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
        logger.error(f"[ALLEN] {code}: {e}")

    return data


# ------------------------------------------------------
# EATON
# ------------------------------------------------------
def parse_eaton(code: str) -> dict:
    url_en = f"https://www.eaton.com/gb/en-gb/skuPage.{code}.html#tab-2"
    url_tr = f"https://www.eaton.com/tr/tr-tr/skuPage.{code}.html#tab-2"

    data = {
        "brand": "Eaton",
        "code": code,
        "name_en": "",
        "name_tr": "",
        "breadcrumbs_en": "",
        "breadcrumbs_tr": "",
        "category_en": "",
        "category_tr": "",
        "ean": "",
        "datasheet_en": "",
        "datasheet_tr": "",
        "images": []
    }

    # EN
    try:
        r = requests.get(url_en, timeout=25)
        soup = BeautifulSoup(r.text, "html.parser")

        h1 = soup.find("h1")
        if h1:
            data["name_en"] = h1.text.strip()

        imgs = soup.find_all("img")
        for img in imgs:
            src = img.get("src")
            if not src:
                continue
            if code.lower() in src.lower():
                if src.startswith("/"):
                    src = "https://www.eaton.com" + src
                data["images"].append(src)
    except Exception as e:
        logger.error(f"[EATON EN] {code}: {e}")

    # TR (varsa)
    try:
        r = requests.get(url_tr, timeout=25)
        soup = BeautifulSoup(r.text, "html.parser")

        h1 = soup.find("h1")
        if h1:
            data["name_tr"] = h1.text.strip()
    except Exception as e:
        logger.error(f"[EATON TR] {code}: {e}")

    return data


# ------------------------------------------------------
# LEGRAND (DE)
# ------------------------------------------------------
def parse_legrand(code: str) -> dict:
    url = "https://www.legrand.at/de/katalog/produkte/innen-aussenwinkel-16x16-weiss-030191"

    data = {
        "brand": "Legrand",
        "code": code,
        "name_en": "",
        "name_tr": "",
        "name_de": "",
        "breadcrumbs_de": "",
        "category_de": "",
        "ean": "",
        "datasheet_en": "",
        "datasheet_tr": "",
        "images": []
    }

    try:
        r = requests.get(url, timeout=25)
        soup = BeautifulSoup(r.text, "html.parser")

        h1 = soup.find("h1")
        if h1:
            data["name_de"] = h1.text.strip()

        crumbs = soup.select("ul.breadcrumb li")
        if crumbs:
            bc_text = " > ".join(c.text.strip() for c in crumbs)
            data["breadcrumbs_de"] = bc_text
            if len(crumbs) >= 2:
                data["category_de"] = crumbs[-2].text.strip()

        imgs = soup.find_all("img")
        for img in imgs:
            src = img.get("src")
            if not src:
                continue
            if "030191" in src:
                if src.startswith("/"):
                    src = "https://www.legrand.at" + src
                data["images"].append(src)
    except Exception as e:
        logger.error(f"[LEGRAND] {code}: {e}")

    return data


# ------------------------------------------------------
# WAGO
# ------------------------------------------------------
def parse_wago(code: str) -> dict:
    url_en = f"https://www.wago.com/global/marking/roller/p/{code}"
    url_tr = f"https://www.wago.com/tr/etiketleme/rulo/p/{code}"

    data = {
        "brand": "Wago",
        "code": code,
        "name_en": "",
        "name_tr": "",
        "breadcrumbs_en": "",
        "breadcrumbs_tr": "",
        "category_en": "",
        "category_tr": "",
        "ean": "",
        "datasheet_en": "",
        "datasheet_tr": "",
        "images": []
    }

    # EN
    try:
        r = requests.get(url_en, timeout=25)
        soup = BeautifulSoup(r.text, "html.parser")

        h1 = soup.find("h1")
        if h1:
            data["name_en"] = h1.text.strip()

        imgs = soup.find_all("img")
        for img in imgs:
            src = img.get("src")
            if not src:
                continue
            if code in src:
                if src.startswith("/"):
                    src = "https://www.wago.com" + src
                data["images"].append(src)
    except Exception as e:
        logger.error(f"[WAGO EN] {code}: {e}")

    # TR
    try:
        r = requests.get(url_tr, timeout=25)
        soup = BeautifulSoup(r.text, "html.parser")

        h1 = soup.find("h1")
        if h1:
            data["name_tr"] = h1.text.strip()
    except Exception as e:
        logger.error(f"[WAGO TR] {code}: {e}")

    return data


# ------------------------------------------------------
# SIEMENS
# ------------------------------------------------------
def parse_siemens(code: str) -> dict:
    url = f"https://mall.industry.siemens.com/mall/en/oeii/Catalog/Product/{code}"

    data = {
        "brand": "Siemens",
        "code": code,
        "name_en": "",
        "breadcrumbs_en": "",
        "category_en": "",
        "ean": "",
        "datasheet_en": "",
        "datasheet_tr": "",
        "images": []
    }

    try:
        r = requests.get(url, timeout=25)
        soup = BeautifulSoup(r.text, "html.parser")

        h1 = soup.find("h1")
        if h1:
            data["name_en"] = h1.text.strip()

        crumbs = soup.select("ul.breadcrumb li")
        if crumbs:
            bc_text = " > ".join(c.text.strip() for c in crumbs)
            data["breadcrumbs_en"] = bc_text
            if len(crumbs) >= 2:
                data["category_en"] = crumbs[-2].text.strip()

        imgs = soup.find_all("img")
        for img in imgs:
            src = img.get("src")
            if not src:
                continue
            if code.lower() in src.lower():
                if src.startswith("/"):
                    src = "https://mall.industry.siemens.com" + src
                data["images"].append(src)
    except Exception as e:
        logger.error(f"[SIEMENS] {code}: {e}")

    return data


# ------------------------------------------------------
# SCRAPER ROUTER
# ------------------------------------------------------
def scrape_by_brand(brand: str, code: str):
    b = (brand or "").lower()
    logger.info(f"[SCRAPE] brand={brand} code={code}")

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

    logger.warning(f"[SCRAPE] No parser for brand={brand}")
    return None


# ------------------------------------------------------
# MAIL
# ------------------------------------------------------
def send_mail(zip_path: str, title: str):
    try:
        logger.info("[MAIL] Preparing email...")
        msg = MIMEMultipart()
        msg["From"] = "automations@weltrada.com"
        msg["To"] = "automations@weltrada.com"
        msg["Subject"] = title

        part = MIMEBase("application", "zip")
        with open(zip_path, "rb") as f:
            part.set_payload(f.read())
        encoders.encode_base64(part)
        part.add_header("Content-Disposition", f"attachment; filename={title}.zip")
        msg.attach(part)

        server = smtplib.SMTP("mail.weltrada.com", 587)
        server.starttls()
        mail_pass = os.getenv("MAIL_PASS")
        server.login("automations@weltrada.com", mail_pass)
        server.send_message(msg)
        server.quit()
        logger.info("[MAIL] Sent successfully.")
    except Exception as e:
        logger.error(f"[MAIL] Error: {e}")


# ------------------------------------------------------
# MAIN API
# ------------------------------------------------------
@app.post("/process-products")
async def process_products(file: UploadFile = File(...)):
    logger.info("[API] /process-products called")

    # Ana klasörü oluştur
    time_str = datetime.now().strftime("%d-%m-%Y-at-%H-%M")
    root_folder = f"Research-{time_str}"
    root_path = os.path.join(BASE_DIR, root_folder)

    os.makedirs(root_path, exist_ok=True)
    os.makedirs(os.path.join(root_path, "Images"), exist_ok=True)
    os.makedirs(os.path.join(root_path, "Info/en/Breadcrumbs"), exist_ok=True)
    os.makedirs(os.path.join(root_path, "Info/tr/Sayfa-Yolları"), exist_ok=True)

    # Excel kaydet
    excel_path = os.path.join(root_path, "uploaded.xlsx")
    with open(excel_path, "wb") as buffer:
        shutil.copyfileobj(file.file, buffer)

    df = pd.read_excel(excel_path)
    logger.info(f"[API] Excel rows: {len(df)}")

    en_rows = []
    tr_rows = []

    for idx, row in df.iterrows():
        brand = str(row["brand"]).strip()
        code = str(row["product_code"]).strip().upper()
        logger.info(f"[ROW {idx}] brand={brand}, code={code}")

        data = scrape_by_brand(brand, code)
        if not data:
            logger.warning(f"[ROW {idx}] No data for {brand} {code}")
            continue

        # EN satırı
        en_rows.append({
            "Product Code": code,
            "Brand": brand,
            "Product Name": data.get("name_en", data.get("name_de", "")),
            "Category": data.get("category_en", data.get("category_de", ""))
        })

        # TR satırı (yoksa EN kullan)
        tr_rows.append({
            "Ürün kodu": code,
            "Marka": brand,
            "Ürün Adı": data.get("name_tr", data.get("name_en", data.get("name_de", ""))),
            "Kategori": data.get("category_tr", data.get("category_en", data.get("category_de", "")))
        })

        # Breadcrumb / Sayfa yolu
        bc_en = data.get("breadcrumbs_en", data.get("breadcrumbs_de", ""))
        bc_tr = data.get("breadcrumbs_tr", bc_en)

        with open(os.path.join(root_path, f"Info/en/Breadcrumbs/{code}-breadcrumbs.txt"), "w") as f:
            f.write(bc_en or "")

        with open(os.path.join(root_path, f"Info/tr/Sayfa-Yolları/{code}-sayfa-yolu.txt"), "w") as f:
            f.write(bc_tr or "")

        # Görseller
        img_folder = os.path.join(root_path, f"Images/{code}")
        os.makedirs(img_folder, exist_ok=True)

        count = 1
        for img_url in data.get("images", []):
            filename = f"{clean_filename(brand)}-{code.lower()}-{count:03d}.webp"
            save_path = os.path.join(img_folder, filename)
            ok = download_image_to_webp(img_url, save_path)
            if ok:
                count += 1

        # Datasheet EN
        if data.get("datasheet_en"):
            ds_path = os.path.join(root_path, f"{code}-Datasheet-en.pdf")
            download_file(data["datasheet_en"], ds_path)

        # Datasheet TR
        if data.get("datasheet_tr"):
            ds_path = os.path.join(root_path, f"{code}-Datasheet-tr.pdf")
            download_file(data["datasheet_tr"], ds_path)

    # Excel dosyaları
    if en_rows:
        pd.DataFrame(en_rows).to_excel(
            os.path.join(root_path, "Info/en/products-info.xlsx"), index=False
        )
    if tr_rows:
        pd.DataFrame(tr_rows).to_excel(
            os.path.join(root_path, "Info/tr/ürünleri-detay.xlsx"), index=False
        )

    # ZIP
    zip_name = f"{root_folder}.zip"
    zip_path = os.path.join(BASE_DIR, zip_name)

    with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as zipf:
        for root, _, files in os.walk(root_path):
            for f in files:
                full_path = os.path.join(root, f)
                rel = os.path.relpath(full_path, root_path)
                zipf.write(full_path, rel)

    # MAIL
    send_mail(zip_path, root_folder)

    logger.info("[API] Done")
    return {"status": "success", "zip": zip_name}


if __name__ == "__main__":
    uvicorn.run("app:app", host="0.0.0.0", port=10000)