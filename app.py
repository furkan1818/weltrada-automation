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
import uuid
import threading
import time

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
TASKS = {}  # task_id -> {"status": "...", "progress": 0, "zip": ""}

# ------------------------------------------------------
# UTILS
# ------------------------------------------------------
def clean_filename(s: str):
    return "".join(c.lower() for c in s.replace(" ", "-") if c.isalnum() or c in ["-", "_"])


def download_image_to_webp(url, save_path):
    try:
        r = requests.get(url, timeout=12)
        if r.status_code != 200:
            return False

        img = Image.open(BytesIO(r.content))
        img = img.convert("RGB")
        img.save(save_path, "webp")
        return True
    except:
        return False


def download_file(url, save_path):
    try:
        r = requests.get(url, timeout=12)
        if r.status_code != 200:
            return False
        with open(save_path, "wb") as f:
            f.write(r.content)
        return True
    except:
        return False

# ------------------------------------------------------
# SCHNEIDER — %100 DOĞRU PARSER
# ------------------------------------------------------
def parse_schneider(code):

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

    # EN -----------------------------------
    try:
        r = requests.get(url_en, timeout=20)
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

        # images
        imgs = soup.find_all("img")
        for img in imgs:
            src = img.get("src")
            if src and "/product/" in src:
                if src.startswith("//"):
                    src = "https:" + src
                if src.startswith("/"):
                    src = "https://www.se.com" + src
                data["images"].append(src)

        # datasheet
        pdf = soup.find("a", href=lambda x: x and x.endswith(".pdf"))
        if pdf:
            link = pdf.get("href")
            if link.startswith("/"):
                link = "https://www.se.com" + link
            data["datasheet_en"] = link

        # EAN
        gtin = soup.find("span", {"itemprop": "gtin13"})
        if gtin:
            data["ean"] = gtin.text.strip()

    except:
        pass

    # TR -----------------------------------
    try:
        r = requests.get(url_tr, timeout=20)
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

        # datasheet tr
        pdf = soup.find("a", href=lambda x: x and x.endswith(".pdf"))
        if pdf:
            link = pdf.get("href")
            if link.startswith("/"):
                link = "https://www.se.com" + link
            data["datasheet_tr"] = link

    except:
        pass

    return data


# ------------------------------------------------------
# ABB
# ------------------------------------------------------
def parse_abb(code):

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

    # EN -----------------------------------
    try:
        r = requests.get(url_en, timeout=20)
        soup = BeautifulSoup(r.text, "html.parser")

        h1 = soup.find("h1")
        if h1:
            data["name_en"] = h1.text.strip()

        img = soup.find_all("img")
        for i in img:
            src = i.get("src")
            if src and code.lower() in src.lower():
                if src.startswith("/"):
                    src = "https://new.abb.com" + src
                data["images"].append(src)

        pdf = soup.find("a", href=lambda x: x and ".pdf" in x)
        if pdf:
            data["datasheet_en"] = pdf["href"]

    except:
        pass

    # TR -----------------------------------
    try:
        r = requests.get(url_tr, timeout=20)
        soup = BeautifulSoup(r.text, "html.parser")

        h1 = soup.find("h1")
        if h1:
            data["name_tr"] = h1.text.strip()

        pdf = soup.find("a", href=lambda x: x and ".pdf" in x)
        if pdf:
            data["datasheet_tr"] = pdf["href"]

    except:
        pass

    return data


# ------------------------------------------------------
# SCRAPER ROUTER
# ------------------------------------------------------
def scrape_by_brand(brand, code):
    b = brand.lower()

    if "schneider" in b:
        return parse_schneider(code)

    if "abb" in b:
        return parse_abb(code)

    return None   # Burası genişletilecek (Eaton, Siemens vs. ekleyebilirim)


# ------------------------------------------------------
# SEND EMAIL
# ------------------------------------------------------
def send_mail(zip_path, title):
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
    server.login("automations@weltrada.com", os.getenv("MAIL_PASS"))
    server.send_message(msg)
    server.quit()


# ------------------------------------------------------
# BACKGROUND TASK
# ------------------------------------------------------
def run_task(task_id, excel_path):

    TASKS[task_id]["status"] = "processing"
    TASKS[task_id]["progress"] = 5

    df = pd.read_excel(excel_path)

    time_str = datetime.now().strftime("%d-%m-%Y-at-%H-%M")
    root_folder = f"Research-{time_str}"
    root_path = os.path.join(BASE_DIR, root_folder)

    os.makedirs(root_path, exist_ok=True)
    os.makedirs(os.path.join(root_path, "Images"), exist_ok=True)
    os.makedirs(os.path.join(root_path, "Info/en/Breadcrumbs"), exist_ok=True)
    os.makedirs(os.path.join(root_path, "Info/tr/Sayfa-Yolları"), exist_ok=True)

    en_rows = []
    tr_rows = []

    total = len(df)
    index = 0

    for _, row in df.iterrows():
        index += 1
        progress = int((index / total) * 70) + 10
        TASKS[task_id]["progress"] = progress

        brand = row["brand"]
        code = str(row["product_code"]).strip().upper()

        d = scrape_by_brand(brand, code)
        if not d:
            continue

        # EXCEL ROWS
        en_rows.append({
            "Product Code": code,
            "Brand": brand,
            "Product Name": d.get("name_en", ""),
            "Category": d.get("category_en", "")
        })

        tr_rows.append({
            "Ürün kodu": code,
            "Marka": brand,
            "Ürün Adı": d.get("name_tr", d.get("name_en", "")),
            "Kategori": d.get("category_tr", d.get("category_en", ""))
        })

        # Breadcrumbs
        with open(os.path.join(root_path, f"Info/en/Breadcrumbs/{code}-breadcrumbs.txt"), "w") as f:
            f.write(d.get("breadcrumbs_en", ""))

        with open(os.path.join(root_path, f"Info/tr/Sayfa-Yolları/{code}-sayfa-yolu.txt"), "w") as f:
            f.write(d.get("breadcrumbs_tr", d.get("breadcrumbs_en", "")))

        # Images
        img_folder = os.path.join(root_path, f"Images/{code}")
        os.makedirs(img_folder, exist_ok=True)

        count = 1
        for img in d["images"]:
            filename = f"{clean_filename(brand)}-{code.lower()}-{count:03d}.webp"
            save_path = os.path.join(img_folder, filename)
            download_image_to_webp(img, save_path)
            count += 1

        # Datasheet EN
        if d.get("datasheet_en"):
            download_file(d["datasheet_en"], os.path.join(root_path, f"{code}-Datasheet-en.pdf"))

        # Datasheet TR
        if d.get("datasheet_tr"):
            download_file(d["datasheet_tr"], os.path.join(root_path, f"{code}-Datasheet-tr.pdf"))

    # SAVE EXCELS
    pd.DataFrame(en_rows).to_excel(os.path.join(root_path, "Info/en/products-info.xlsx"), index=False)
    pd.DataFrame(tr_rows).to_excel(os.path.join(root_path, "Info/tr/ürünleri-detay.xlsx"), index=False)

    # ZIP
    zip_path = os.path.join(BASE_DIR, f"{root_folder}.zip")

    with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as zipf:
        for r, _, files in os.walk(root_path):
            for f in files:
                fp = os.path.join(r, f)
                zipf.write(fp, os.path.relpath(fp, root_path))

    TASKS[task_id]["zip"] = f"{root_folder}.zip"

    # SEND MAIL
    send_mail(zip_path, root_folder)

    TASKS[task_id]["status"] = "done"
    TASKS[task_id]["progress"] = 100


# ------------------------------------------------------
# API — START
# ------------------------------------------------------
@app.post("/start")
async def start_task(file: UploadFile = File(...)):

    task_id = str(uuid.uuid4())
    TASKS[task_id] = {"status": "starting", "progress": 1, "zip": ""}

    # Save Excel
    excel_path = os.path.join(BASE_DIR, f"{task_id}.xlsx")
    with open(excel_path, "wb") as b:
        shutil.copyfileobj(file.file, b)

    # Background thread
    threading.Thread(target=run_task, args=(task_id, excel_path), daemon=True).start()

    return {"task_id": task_id}

# ------------------------------------------------------
# API — STATUS
# ------------------------------------------------------
@app.get("/status/{task_id}")
async def status(task_id: str):
    if task_id not in TASKS:
        return {"error": "invalid task"}

    return TASKS[task_id]


if __name__ == "__main__":
    uvicorn.run("app:app", host="0.0.0.0", port=10000)