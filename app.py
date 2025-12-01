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

# -------------------------------------------
# FASTAPI CONFIG
# -------------------------------------------
app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

BASE_DIR = "/opt/render/project/src"


# -------------------------------------------
# GLOBAL UTILS
# -------------------------------------------
def clean_filename(s: str):
    return "".join(c.lower() for c in s.replace(" ", "-") if c.isalnum() or c in ["-", "_"])


def download_image_to_webp(url, save_path):
    try:
        r = requests.get(url, timeout=12)
        img = Image.open(BytesIO(r.content))
        img = img.convert("RGB")
        img.save(save_path, "webp")
        return True
    except:
        return False


def download_file(url, save_path):
    try:
        r = requests.get(url, timeout=12)
        with open(save_path, "wb") as f:
            f.write(r.content)
        return True
    except:
        return False


# -------------------------------------------
# SCRAPER — SCHNEIDER ELECTRIC
# -------------------------------------------
def parse_schneider(product_code):
    url_en = f"https://www.se.com/uk/en/product/{product_code}/"
    url_tr = f"https://www.se.com/tr/tr/product/{product_code}/"

    data = {
        "brand": "Schneider Electric",
        "code": product_code,
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
        r = requests.get(url_en, timeout=20)
        soup = BeautifulSoup(r.text, "html.parser")

        title = soup.find("h1")
        if title:
            data["name_en"] = title.text.strip()

        bc = soup.find_all("li", {"itemprop": "itemListElement"})
        if bc:
            bc_text = " > ".join(i.text.strip() for i in bc)
            data["breadcrumbs_en"] = bc_text
            if len(bc) > 2:
                data["category_en"] = bc[1].text.strip()

        # EAN
        ean = soup.find("span", {"itemprop": "gtin13"})
        if ean:
            data["ean"] = ean.text.strip()

        # Images
        imgs = soup.find_all("img")
        for img in imgs:
            src = img.get("src")
            if src and "product" in src:
                if src.startswith("//"): src = "https:" + src
                if src.startswith("/"): src = "https://www.se.com" + src
                data["images"].append(src)

        # Datasheet
        pdf = soup.find("a", href=lambda x: x and x.endswith(".pdf"))
        if pdf:
            data["datasheet_en"] = pdf["href"]
    except:
        pass

    # TR
    try:
        r = requests.get(url_tr, timeout=20)
        soup = BeautifulSoup(r.text, "html.parser")

        title = soup.find("h1")
        if title:
            data["name_tr"] = title.text.strip()

        bc = soup.find_all("li", {"itemprop": "itemListElement"})
        if bc:
            bc_text = " > ".join(i.text.strip() for i in bc)
            data["breadcrumbs_tr"] = bc_text
            if len(bc) > 2:
                data["category_tr"] = bc[1].text.strip()

        # Datasheet TR
        pdf = soup.find("a", href=lambda x: x and x.endswith(".pdf"))
        if pdf:
            data["datasheet_tr"] = pdf["href"]
    except:
        pass

    return data


# -------------------------------------------
# SCRAPER — ABB
# -------------------------------------------
def parse_abb(product_code):
    url_en = f"https://new.abb.com/products/{product_code}"
    url_tr = f"https://new.abb.com/products/tr/{product_code}"

    data = {
        "brand": "ABB Group",
        "code": product_code,
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

    for lang, url in [("en", url_en), ("tr", url_tr)]:
        try:
            r = requests.get(url, timeout=20)
            soup = BeautifulSoup(r.text, "html.parser")

            title = soup.find("h1")
            if title:
                data[f"name_{lang}"] = title.text.strip()

            imgs = soup.find_all("img")
            for img in imgs:
                src = img.get("src")
                if src and product_code.lower() in src.lower():
                    if src.startswith("/"):
                        src = "https://new.abb.com" + src
                    data["images"].append(src)

            pdf = soup.find("a", href=lambda x: x and ".pdf" in x)
            if pdf:
                data[f"datasheet_{lang}"] = pdf["href"]

        except:
            pass

    return data


# -------------------------------------------
# SCRAPER — ALLEN BRADLEY (ROCKWELL)
# -------------------------------------------
def parse_allen(product_code):
    url = f"https://www.rockwellautomation.com/en-dk/products/details.{product_code}.html"

    data = {
        "brand": "Allen Bradley (Rockwell Automation)",
        "code": product_code,
        "name_en": "",
        "breadcrumbs_en": "",
        "category_en": "",
        "ean": "",
        "datasheet_en": "",
        "images": []
    }

    try:
        r = requests.get(url, timeout=20)
        soup = BeautifulSoup(r.text, "html.parser")

        title = soup.find("h1")
        if title:
            data["name_en"] = title.text.strip()

        imgs = soup.find_all("img")
        for img in imgs:
            src = img.get("src")
            if src and product_code in src:
                if src.startswith("/"):
                    src = "https://www.rockwellautomation.com" + src
                data["images"].append(src)

        pdf = soup.find("a", href=lambda x: x and x.endswith(".pdf"))
        if pdf:
            data["datasheet_en"] = pdf["href"]

    except:
        pass

    return data


# -------------------------------------------
# SCRAPER — EATON
# -------------------------------------------
def parse_eaton(product_code):
    url_en = f"https://www.eaton.com/gb/en-gb/skuPage.{product_code}.html#tab-2"

    data = {
        "brand": "Eaton",
        "code": product_code,
        "name_en": "",
        "name_tr": "",
        "breadcrumbs_en": "",
        "category_en": "",
        "images": [],
        "datasheet_en": "",
        "datasheet_tr": "",
        "ean": ""
    }

    try:
        r = requests.get(url_en, timeout=20)
        soup = BeautifulSoup(r.text, "html.parser")

        title = soup.find("h1")
        if title:
            data["name_en"] = title.text.strip()

        imgs = soup.find_all("img")
        for img in imgs:
            src = img.get("src")
            if src and product_code.lower() in src.lower():
                if src.startswith("/"):
                    src = "https://www.eaton.com" + src
                data["images"].append(src)
    except:
        pass

    return data


# -------------------------------------------
# SCRAPER — LEGRAND (ALMANCA)
# -------------------------------------------
def parse_legrand(product_code):
    url = "https://www.legrand.at/de/katalog/produkte/innen-aussenwinkel-16x16-weiss-030191"

    data = {
        "brand": "Legrand",
        "code": product_code,
        "name_de": "",
        "breadcrumbs_de": "",
        "category_de": "",
        "images": [],
        "datasheet_de": ""
    }

    try:
        r = requests.get(url, timeout=20)
        soup = BeautifulSoup(r.text, "html.parser")

        title = soup.find("h1")
        if title:
            data["name_de"] = title.text.strip()

        imgs = soup.find_all("img")
        for img in imgs:
            src = img.get("src")
            if src and "030191" in src:
                if src.startswith("/"):
                    src = "https://www.legrand.at" + src
                data["images"].append(src)

    except:
        pass

    return data


# -------------------------------------------
# SCRAPER — WAGO
# -------------------------------------------
def parse_wago(product_code):
    url = f"https://www.wago.com/global/marking/roller/p/{product_code}"

    data = {
        "brand": "Wago",
        "code": product_code,
        "name_en": "",
        "breadcrumbs_en": "",
        "category_en": "",
        "images": [],
        "datasheet_en": "",
        "ean": ""
    }

    try:
        r = requests.get(url, timeout=20)
        soup = BeautifulSoup(r.text, "html.parser")

        title = soup.find("h1")
        if title:
            data["name_en"] = title.text.strip()

        imgs = soup.find_all("img")
        for img in imgs:
            src = img.get("src")
            if src and product_code in src:
                if src.startswith("/"):
                    src = "https://www.wago.com" + src
                data["images"].append(src)

    except:
        pass

    return data


# -------------------------------------------
# SCRAPER — SIEMENS
# -------------------------------------------
def parse_siemens(product_code):
    url = f"https://mall.industry.siemens.com/mall/en/oeii/Catalog/Product/{product_code}"

    data = {
        "brand": "Siemens",
        "code": product_code,
        "name_en": "",
        "breadcrumbs_en": "",
        "category_en": "",
        "images": [],
        "datasheet_en": "",
        "ean": ""
    }

    try:
        r = requests.get(url, timeout=20)
        soup = BeautifulSoup(r.text, "html.parser")

        title = soup.find("h1")
        if title:
            data["name_en"] = title.text.strip()

        imgs = soup.find_all("img")
        for img in imgs:
            src = img.get("src")
            if src and product_code.lower() in src.lower():
                if src.startswith("/"):
                    src = "https://mall.industry.siemens.com" + src
                data["images"].append(src)

    except:
        pass

    return data


# -------------------------------------------
# SCRAPER ROUTER
# -------------------------------------------
def scrape_by_brand(brand, code):
    brand = brand.lower()

    if "schneider" in brand:
        return parse_schneider(code)

    if "abb" in brand:
        return parse_abb(code)

    if "allen" in brand:
        return parse_allen(code)

    if "eaton" in brand:
        return parse_eaton(code)

    if "legrand" in brand:
        return parse_legrand(code)

    if "wago" in brand:
        return parse_wago(code)

    if "siemens" in brand:
        return parse_siemens(code)

    return None


# -------------------------------------------
# SEND EMAIL
# -------------------------------------------
def send_mail(zip_path, name):
    msg = MIMEMultipart()
    msg["From"] = "automations@weltrada.com"
    msg["To"] = "automations@weltrada.com"
    msg["Subject"] = name

    part = MIMEBase("application", "zip")
    with open(zip_path, "rb") as f:
        part.set_payload(f.read())
    encoders.encode_base64(part)
    part.add_header("Content-Disposition", f"attachment; filename={name}.zip")

    msg.attach(part)

    server = smtplib.SMTP("mail.weltrada.com", 587)
    server.starttls()
    server.login("automations@weltrada.com", "MAIL_SIFREN")
    server.send_message(msg)
    server.quit()


# -------------------------------------------
# MAIN API
# -------------------------------------------
@app.post("/process-products")
async def process_products(file: UploadFile = File(...)):

    # ---- Create main folder
    time_str = datetime.now().strftime("%d-%m-%Y-at-%H-%M")
    root_folder = f"Research-{time_str}"
    root_path = os.path.join(BASE_DIR, root_folder)

    os.makedirs(root_path, exist_ok=True)
    os.makedirs(os.path.join(root_path, "Images"), exist_ok=True)
    os.makedirs(os.path.join(root_path, "Info/en/Breadcrumbs"), exist_ok=True)
    os.makedirs(os.path.join(root_path, "Info/tr/Sayfa-Yolları"), exist_ok=True)

    # ---- Save Excel
    excel_path = os.path.join(root_path, "uploaded.xlsx")
    with open(excel_path, "wb") as buffer:
        shutil.copyfileobj(file.file, buffer)

    df = pd.read_excel(excel_path)

    en_rows = []
    tr_rows = []

    # ---- Loop products
    for _, row in df.iterrows():
        brand = row["brand"]
        code = str(row["product_code"]).strip().upper()

        data = scrape_by_brand(brand, code)
        if not data:
            continue

        # ---- Save EN row
        en_rows.append({
            "Product Code": code,
            "Brand": brand,
            "Product Name": data.get("name_en", ""),
            "Category": data.get("category_en", "")
        })

        # ---- Save TR row (mock)
        tr_rows.append({
            "Ürün kodu": code,
            "Marka": brand,
            "Ürün Adı": data.get("name_tr", data.get("name_en", "")),
            "Kategori": data.get("category_tr", data.get("category_en", ""))
        })

        # ---- Breadcrumbs
        with open(os.path.join(root_path, f"Info/en/Breadcrumbs/{code}-breadcrumbs.txt"), "w") as f:
            f.write(data.get("breadcrumbs_en", ""))

        with open(os.path.join(root_path, f"Info/tr/Sayfa-Yolları/{code}-sayfa-yolu.txt"), "w") as f:
            f.write(data.get("breadcrumbs_tr", data.get("breadcrumbs_en", "")))

        # ---- Images
        img_folder = os.path.join(root_path, f"Images/{code}")
        os.makedirs(img_folder, exist_ok=True)

        count = 1
        for img in data["images"]:
            filename = f"{clean_filename(brand)}-{code.lower()}-{count:03d}.webp"
            save_path = os.path.join(img_folder, filename)
            download_image_to_webp(img, save_path)
            count += 1

        # ---- Datasheet EN
        if data.get("datasheet_en"):
            ds_path = os.path.join(root_path, f"{code}-Datasheet-en.pdf")
            download_file(data["datasheet_en"], ds_path)

        # ---- Datasheet TR
        if data.get("datasheet_tr"):
            ds_path = os.path.join(root_path, f"{code}-Datasheet-tr.pdf")
            download_file(data["datasheet_tr"], ds_path)

    # ---- Write Excel files
    pd.DataFrame(en_rows).to_excel(os.path.join(root_path, "Info/en/products-info.xlsx"), index=False)
    pd.DataFrame(tr_rows).to_excel(os.path.join(root_path, "Info/tr/ürünleri-detay.xlsx"), index=False)

    # ---- ZIP
    zip_name = f"{root_folder}.zip"
    zip_path = os.path.join(BASE_DIR, zip_name)

    with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as zipf:
        for root, _, files in os.walk(root_path):
            for file in files:
                full_path = os.path.join(root, file)
                zipf.write(full_path, os.path.relpath(full_path, root_path))

    # ---- Send Email
    send_mail(zip_path, root_folder)

    return {"status": "success", "zip": zip_name}


if __name__ == "__main__":
    uvicorn.run("app:app", host="0.0.0.0", port=10000)