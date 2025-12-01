from fastapi import FastAPI

app = FastAPI()

@app.get("/")
def root():
    return {"message": "Weltrada Automation API is running"}