# API

This repository contains the code for the API, which is built using FastAPI and is callable at [https://api.codefordemocracy.org](https://api.codefordemocracy.org). This app is set up to run on GCP App Engine using Python 3.7.

## Running Locally

```
pip install -r requirements.txt
export GOOGLE_APPLICATION_CREDENTIALS="XXXXXXXXXXXXXXXXX.json"
uvicorn main:app --reload
```
