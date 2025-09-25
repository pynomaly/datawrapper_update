import json
import os
import time

import pandas as pd
import requests
from dotenv import load_dotenv

load_dotenv()

API_PATH = "https://api.minka-sdg.org/v1"


def get_access_token():
    url = "https://www.minka-sdg.org/oauth/token"

    payload = {
        "client_id": os.getenv("MINKA_CLIENT_ID"),
        "client_secret": os.getenv("MINKA_CLIENT_SECRET"),
        "grant_type": "password",
        "username": os.getenv("MINKA_USER_EMAIL"),
        "password": os.getenv("MINKA_USER_PASSWORD"),
    }

    max_retries = 3
    timeout = 30

    for attempt in range(max_retries):
        try:
            response = requests.post(url, data=payload, timeout=timeout)

            if response.ok:
                token = response.json().get("access_token")
                print("Access token obteined")
                return token
            else:
                print("Error:", response.status_code, response.text)
                return None

        except requests.exceptions.ConnectTimeout:
            print(f"Connection timeout (attempt {attempt + 1}/{max_retries})")
            if attempt < max_retries - 1:
                time.sleep(5)
                continue
            else:
                print("Max retries exceeded. Unable to get access token.")
                return None
        except requests.exceptions.RequestException as e:
            print(f"Request error: {e}")
            return None


def get_metrics_proj(proj_ids, access_token=None):
    headers = {"Authorization": f"Bearer {access_token}"} if access_token else {}
    session = requests.Session()
    total_results = []

    observations = f"{API_PATH}/observations?"
    species = f"{API_PATH}/observations/species_counts?"
    observers = f"{API_PATH}/observations/observers?"

    params = {
        "project_id": proj_ids,
        "order": "desc",
        "order_by": "created_at",
    }

    try:
        total_species = session.get(species, headers=headers, params=params).json()[
            "total_results"
        ]
        total_participants = session.get(
            observers, headers=headers, params=params
        ).json()["total_results"]
        total_obs = session.get(observations, headers=headers, params=params).json()[
            "total_results"
        ]

        result = {
            "observations": total_obs,
            "species": total_species,
            "participants": total_participants,
        }
        total_results.append(result)

        df_total = pd.DataFrame(total_results)
        return df_total

    except Exception as e:
        print(f"Error fetching metrics: {e}")
        return pd.DataFrame()


if __name__ == "__main__":

    access_token = get_access_token()

    if access_token is None:
        print("Continuing without authentication token...")

    proj_ids = "285, 283, 124, 20, 367, 417"

    # 285, biomaratona-norte-2024
    # 283, BioMARató 2024 (Catalunya)
    # 124, biomarato-2023-catalunya
    # 20,  biomarato-2022-catalunya
    # 367, BioMARató 2021 (Catalunya)
    # 417, biomarato-2025-catalunya

    df_total = get_metrics_proj(proj_ids, access_token)

    if not df_total.empty:
        downloaded_data = pd.read_csv("data/biomarato_global_counter.csv")
        if downloaded_data["observations"].iloc[0] != df_total["observations"].iloc[0]:
            df_total.to_csv("data/biomarato_global_counter.csv", index=False)
        else:
            print("No changes in data.")
    else:
        print("No data retrieved, skipping CSV update.")
