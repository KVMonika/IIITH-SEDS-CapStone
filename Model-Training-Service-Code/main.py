import uvicorn
import os
import requests
from fastapi import FastAPI
from pydantic import BaseModel
from typing import List
from retrain import retrain_model

# defining the main app
app = FastAPI(title="retrain", docs_url="/")

# Route definitions
@app.get("/ping")
# Healthcheck route to ensure that the API is up and running
def ping():
    return {"ping": "pong"}

@app.post("/retrain", status_code=200)
def retrain():
    response = retrain_model()
    return {"detail": "Training successful"}


# Main function to start the app when main.py is called
if __name__ == "__main__":
    # Uvicorn is used to run the server and listen for incoming API requests on 0.0.0.0:8888
    uvicorn.run("main:app", host="0.0.0.0", port=7777, reload=True)
