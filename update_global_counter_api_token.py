import json
import os

import pandas as pd
import requests
from dotenv import load_dotenv
from playwright.sync_api import sync_playwright

API_PATH = "https://api.minka-sdg.org/v1"

load_dotenv()


def get_admin_token():
    # Inicia Playwright
    with sync_playwright() as p:
        # Lanza el navegador Firefox
        browser = p.firefox.launch(
            headless=True
        )  # headless=False abre el navegador en modo visual
        page = browser.new_page()

        # Navega a la URL de login de Minka SDG
        page.goto("https://minka-sdg.org/login")

        # Introduce el nombre de usuario
        page.fill('//*[@id="user_email"]', os.getenv("MINKA_USER_EMAIL"))

        # Introduce la contraseña
        page.fill('//*[@id="user_password"]', os.getenv("MINKA_USER_PASSWORD"))

        # Haz clic en el botón de login usando el prefijo "xpath="
        page.locator(
            "xpath=/html/body/div[1]/div[2]/div/div[2]/div/form/div[4]/input"
        ).click()

        # Navega a la URL del api_token
        page.goto("https://minka-sdg.org/users/api_token")

        # Extrae el json de esa página
        # Espera a que la página cargue completamente
        page.wait_for_load_state("networkidle")

        # Extrae el JSON de la página
        page_text = page.evaluate("document.body.innerText")
        json_data = json.loads(page_text.strip())

        # Extrae específicamente el api_token
        api_token = json_data.get("api_token")

        # Cierra el navegador
        browser.close()

        return api_token


def get_metrics_proj(proj_ids):
    headers = {"Authorization": api_token}
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

    total_species = session.get(species, headers=headers, params=params).json()[
        "total_results"
    ]
    total_participants = session.get(observers, headers=headers, params=params).json()[
        "total_results"
    ]
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
    # df_total_sum = df_total.sum(axis=0)

    return df_total


if __name__ == "__main__":

    api_token = get_admin_token()

    proj_ids = "285, 283, 124, 20, 367, 417"

    # 285, biomaratona-norte-2024
    # 283, BioMARató 2024 (Catalunya)
    # 124, biomarato-2023-catalunya
    # 20,  biomarato-2022-catalunya
    # 367, BioMARató 2021 (Catalunya)
    # 417, biomarato-2025-catalunya

    df_total = get_metrics_proj(proj_ids)
    downloaded_data = pd.read_csv("data/biomarato_global_counter.csv")
    if downloaded_data["observations"].iloc[0] != df_total["observations"].iloc[0]:
        df_total.to_csv("data/biomarato_global_counter.csv", index=False)
    else:
        print("No changes in data.")
