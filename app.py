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
TASKS = {}

# ------------------------------------------------------
# UTILS
# ------------------------------------------------------
def log(msg):
    print(f"[LOG] {msg}", flush=True)

def clean_filename(s: str):
    return "".join(c.lower() for c in s.replace(" ", "-") if c.isalnum() or c in ["-", "_"])


def download_image_to_webp(url, save_path):
    log(f"Downloading image: {url}")
    try:
        r = requests.get(url, timeout=12)
        if r.status_code != 200:
            log("Image download failed (status)")
            return False

        img = Image.open(BytesIO(r.content))
        img = img.convert("RGB")
        img.save(save_path, "webp")
        log("Saved image as WEBP")
        return True
    except Exception as e:
        log(f"Image download ERROR: {e}")
        return False


def download_file(url, save_path):
    log(f"Downloading file: {url}")
    try:
        r = requests.get(url, timeout=12)
        if r.status_code != 200:
            log("File download failed (status)")
            return False

        with open(save_path, "wb") as f:
            f.write(r.content)
        log("File saved")
        return True
    except Exception as e:
        log(f"File download ERROR: {e}")
        return False


# ------------------------------------------------------
# SCHNEIDER PARSER
# ------------------------------------------------------
def parse_schneider(code):
    log(f"Parsing SCHNEIDER → {code}")
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
        "images": [],
        "datasheet_en": "",
        "datasheet_tr": "",
        "ean": "",
    }

    try:
        r = requests.get(url_en, timeout=20)
        soup = BeautifulSoup(r.text, "html.parser")
        h1 = soup.find("h1")
        if h1:
            data["name_en"] = h1.text.strip()

        breadcrumb = soup.select("li[itemprop=itemListElement]")
        if breadcrumb:
            bc = " > ".join(b.text.strip() for b in breadcrumb)
            data["breadcrumbs_en"] = bc
            if len(breadcrumb) >= 2:
                data["category_en"] = breadcrumb[-2].text.strip()

        imgs = soup.find_all("img")
        for img in imgs:
            src = img.get("src")
            if src and "/product/" in src:
                if src.startswith("//"):
                    src = "https:" + src
                if src.startswith("/"):
                    src = "https://www.se.com" + src
                data["images"].append(src)
    except Exception as e:
        log(f"[SCHNEIDER EN ERROR] {e}")

    try:
        r = requests.get(url_tr, timeout=20)
        soup = BeautifulSoup(r.text, "html.parser")
        h1 = soup.find("h1")
        if h1:
            data["name_tr"] = h1.text.strip()
    except Exception as e:
        log(f"[SCHNEIDER TR ERROR] {e}")

    return data


# ------------------------------------------------------
# ABB
# ------------------------------------------------------
def parse_abb(code):
    log(f"Parsing ABB → {code}")
    url = f"https://new.abb.com/products/{code}"

    data = {
        "brand": "ABB Group",
        "code": code,
        "name_en": "",
        "images": [],
        "datasheet_en": "",
    }

    try:
        r = requests.get(url, timeout=20)
        soup = BeautifulSoup(r.text, "html.parser")
        h1 = soup.find("h1")
        if h1:
            data["name_en"] = h1.text.strip()

        imgs = soup.find_all("img")
        for img in imgs:
            src = img.get("src")
            if src:
                if src.startswith("/"):
                    src = "https://new.abb.com" + src
                data["images"].append(src)
    except Exception as e:
        log(f"[ABB ERROR] {e}")

    return data


# ------------------------------------------------------
# SCRAPER ROUTER
# ------------------------------------------------------
def scrape_by_brand(brand, code):
    b = brand.lower()
    log(f"SCRAPER ROUTER → {brand} {code}")

    if "schneider" in b:
        return parse_schneider(code)

    if "abb" in b:
        return parse_abb(code)

    log("NO MATCHING SCRAPER FOUND")
    return None


# ------------------------------------------------------
# SEND MAIL
# ------------------------------------------------------
def send_mail(zip_path, title):
    log("Sending email...")
    try:
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
        log("EMAIL SENT OK")
    except Exception as e:
        log(f"[MAIL ERROR] {e}")


# ------------------------------------------------------
# BACKGROUND TASK
# ------------------------------------------------------
def run_task(task_id, excel_path):
    log("TASK STARTED")
    TASKS[task_id]["status"] = "processing"
    TASKS[task_id]["progress"] = 5

    df = pd.read_excel(excel_path)

    root_name = f"Research-{datetime.now().strftime('%d-%m-%Y-at-%H-%M')}"
    root_path = os.path.join(BASE_DIR, root_name)

    os.makedirs(root_path, exist_ok=True)
    os.makedirs(os.path.join(root_path, "Images"), exist_ok=True)

    total = len(df)
    index = 0

    for _, row in df.iterrows():
        brand = row["brand"]
        code = str(row["product_code"]).strip().upper()

        log(f"> Processing {brand} {code}")

        index += 1
        TASKS[task_id]["progress"] = int((index / total) * 70) + 10

        data = scrape_by_brand(brand, code)
        if not data:
            log("❗ SCRAPER RETURNED EMPTY")
            continue

        img_dir = os.path.join(root_path, f"Images/{code}")
        os.makedirs(img_dir, exist_ok=True)

        count = 1
        for img_url in data["images"]:
            filename = f"{clean_filename(brand)}-{code}-{count:03d}.webp"
            save_path = os.path.join(img_dir, filename)
            download_image_to_webp(img_url, save_path)
            count += 1

    zip_path = os.path.join(BASE_DIR, f"{root_name}.zip")
    with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as zipf:
        for r, _, files in os.walk(root_path):
            for f in files:
                fp = os.path.join(r, f)
                zipf.write(fp, os.path.relpath(fp, root_path))

    TASKS[task_id]["zip"] = zip_path
    send_mail(zip_path, root_name)

    TASKS[task_id]["status"] = "done"
    TASKS[task_id]["progress"] = 100
    log("TASK COMPLETED")


# ------------------------------------------------------
# START
# ------------------------------------------------------
@app.post("/start")
async def start_task(file: UploadFile = File(...)):
    task_id = str(uuid.uuid4())
    TASKS[task_id] = {"status": "starting", "progress": 1, "zip": ""}

    excel_path = os.path.join(BASE_DIR, f"{task_id}.xlsx")
    with open(excel_path, "wb") as b:
        shutil.copyfileobj(file.file, b)

    threading.Thread(target=run_task, args=(task_id, excel_path), daemon=True).start()
    return {"task_id": task_id}


# ------------------------------------------------------
# STATUS
# ------------------------------------------------------
@app.get("/status/{task_id}")
async def status(task_id: str):
    return TASKS.get(task_id, {"error": "invalid task"})


if __name__ == "__main__":
    uvicorn.run("app:app", host="0.0.0.0", port=10000)