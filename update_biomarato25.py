import datetime
import json
import math
import os
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

import pandas as pd
import requests
from dotenv import load_dotenv
from mecoda_minka import get_dfs, get_obs
from playwright.sync_api import sync_playwright

load_dotenv()

API_PATH = "https://api.minka-sdg.org/v1"

# Global session for all requests
SESSION = requests.Session()

# Global API token
api_token = None

main_project_bmt = 417

projects_bmt = {
    418: "Girona",
    419: "Tarragona",
    420: "Barcelona",
}

exclude_users = [
    "xasalva",
    "bertinhaco",
    "andrea",
    "laurabiomar",
    "guillermoalvarez_fecdas",
    "mediambient_ajelprat",
    "fecdas_mediambient",
    "planctondiving",
    "marinagm",
    "CEM",
    "jaume-piera",
    "sonialinan",
    "adrisoacha",
    "anellides",
    "irodero",
    "manelsalvador",
    "sara_riera",
    "anomalia",
    "amaliacardenas",
    "aluna",
    "carlosrodero",
    "lydia",
    "elibonfill",
    "marinatorresgi",
    "meri",
    "monyant",
    "ura4dive",
    "lauracoro",
    "pirotte_",
    "oceanicos",
    "abril",
    "alba_barrera",
    "amb_platges",
    "daniel_palacios",
    "davidpiquer",
    "laiamanyer",
    "rogerpuig",
    "guillemdavila",
    # vanessa,
    # teresa,
]


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


def get_main_metrics(proj_id):
    headers = {"Authorization": api_token}

    def make_api_request(url, max_retries=3):
        for attempt in range(max_retries):
            try:
                response = SESSION.get(url, headers=headers)
                response.raise_for_status()
                json_data = response.json()

                if "total_results" not in json_data:
                    print(
                        f"Warning: API response missing 'total_results', retrying... (attempt {attempt + 1})"
                    )
                    if attempt < max_retries - 1:
                        time.sleep(2**attempt)
                        continue
                    else:
                        raise ValueError(
                            "API response missing 'total_results' after retries"
                        )

                return json_data["total_results"]
            except Exception as e:
                if attempt < max_retries - 1:
                    print(
                        f"API request failed (attempt {attempt + 1}): {e}, retrying after delay..."
                    )
                    time.sleep(2**attempt)
                else:
                    print(f"API request failed after {max_retries} attempts: {e}")
                    raise

    species = f"{API_PATH}/observations/species_counts?"
    url1 = f"{species}project_id={proj_id}"
    total_species = make_api_request(url1)

    observers = f"{API_PATH}/observations/observers?"
    url2 = f"{observers}project_id={proj_id}"
    total_participants = make_api_request(url2)

    observations = f"{API_PATH}/observations?"
    url3 = f"{observations}project_id={proj_id}"
    total_obs = make_api_request(url3)

    return total_species, total_participants, total_obs


def fetch_day_metrics(proj_id, day_str):
    """Fetch metrics for a single day"""
    headers = {"Authorization": api_token}

    def make_api_request(url, max_retries=3):
        for attempt in range(max_retries):
            try:
                response = SESSION.get(url, headers=headers)
                response.raise_for_status()
                json_data = response.json()

                if "total_results" not in json_data:
                    print(
                        f"Warning: API response missing 'total_results', retrying... (attempt {attempt + 1})"
                    )
                    if attempt < max_retries - 1:
                        time.sleep(2**attempt)
                        continue
                    else:
                        raise ValueError(
                            "API response missing 'total_results' after retries"
                        )

                return json_data["total_results"]
            except Exception as e:
                if attempt < max_retries - 1:
                    print(
                        f"API request failed (attempt {attempt + 1}): {e}, retrying after delay..."
                    )
                    time.sleep(2**attempt)
                else:
                    print(f"API request failed after {max_retries} attempts: {e}")
                    raise

    observations = f"{API_PATH}/observations?"
    species = f"{API_PATH}/observations/species_counts?"
    observers = f"{API_PATH}/observations/observers?"

    params = {
        "project_id": proj_id,
        "created_d2": day_str,
        "order": "desc",
        "order_by": "created_at",
    }

    try:
        # Sequential requests with delay to avoid API rate limiting
        total_species = make_api_request(
            f"{species}project_id={proj_id}&created_d2={day_str}&order=desc&order_by=created_at"
        )
        total_participants = make_api_request(
            f"{observers}project_id={proj_id}&created_d2={day_str}&order=desc&order_by=created_at"
        )
        total_obs = make_api_request(
            f"{observations}project_id={proj_id}&created_d2={day_str}&order=desc&order_by=created_at"
        )

    except Exception as e:
        print(f"Error fetching data for {day_str}: {e}")
        total_species = total_participants = total_obs = 0

    return {
        "date": day_str,
        "observations": total_obs,
        "species": total_species,
        "participants": total_participants,
    }


def update_main_metrics_by_day(proj_id):
    # Rango de días de BioMARato
    start_day = datetime.date(year=2025, month=5, day=3)
    rango_temporal = (datetime.datetime.today().date() - start_day).days
    print("Número de días: ", rango_temporal)

    if rango_temporal >= 0:
        # Generate all days to process
        days_to_process = []
        day = start_day
        for i in range(rango_temporal):
            if datetime.datetime.today().date() >= day:
                days_to_process.append(day.strftime("%Y-%m-%d"))
                day = day + datetime.timedelta(days=1)

        print(f"Processing {len(days_to_process)} days in parallel...")

        # Process days in smaller batches with reduced concurrency
        results = []
        batch_size = 5  # Reduced batch size to avoid API rate limiting

        for i in range(0, len(days_to_process), batch_size):
            batch = days_to_process[i : i + batch_size]
            print(
                f"Processing batch {i//batch_size + 1}/{(len(days_to_process) + batch_size - 1)//batch_size}"
            )

            with ThreadPoolExecutor(max_workers=3) as executor:  # Reduced max_workers
                futures = [
                    executor.submit(fetch_day_metrics, proj_id, day_str)
                    for day_str in batch
                ]
                for future in as_completed(futures):
                    try:
                        result = future.result()
                        results.append(result)
                    except Exception as e:
                        print(f"Error processing day: {e}")

            # Add delay between batches
            import time

            time.sleep(0.5)

        # Sort results by date
        results.sort(key=lambda x: x["date"])
        result_df = pd.DataFrame(results)
        print("Updated main metrics")
        return result_df


def get_metrics_proj(proj_id, proj_city):
    headers = {"Authorization": api_token}
    observations = f"{API_PATH}/observations?"
    species = f"{API_PATH}/observations/species_counts?"
    observers = f"{API_PATH}/observations/observers?"

    params = {
        "project_id": proj_id,
        "order": "desc",
        "order_by": "created_at",
    }

    def make_api_request(url, max_retries=3):
        for attempt in range(max_retries):
            try:
                response = SESSION.get(url, headers=headers)
                response.raise_for_status()
                json_data = response.json()

                if "total_results" not in json_data:
                    print(
                        f"Warning: API response missing 'total_results', retrying... (attempt {attempt + 1})"
                    )
                    if attempt < max_retries - 1:
                        time.sleep(2**attempt)
                        continue
                    else:
                        raise ValueError(
                            "API response missing 'total_results' after retries"
                        )

                return json_data["total_results"]
            except Exception as e:
                if attempt < max_retries - 1:
                    print(
                        f"API request failed (attempt {attempt + 1}): {e}, retrying after delay..."
                    )
                    time.sleep(2**attempt)
                else:
                    print(f"API request failed after {max_retries} attempts: {e}")
                    raise

    # Parallelize the 3 API calls
    with ThreadPoolExecutor(max_workers=3) as executor:
        future_species = executor.submit(
            make_api_request,
            f"{species}project_id={proj_id}&order=desc&order_by=created_at",
        )
        future_observers = executor.submit(
            make_api_request,
            f"{observers}project_id={proj_id}&order=desc&order_by=created_at",
        )
        future_observations = executor.submit(
            make_api_request,
            f"{observations}project_id={proj_id}&order=desc&order_by=created_at",
        )

        total_species = future_species.result()
        total_participants = future_observers.result()
        total_obs = future_observations.result()

    result = {
        "project": proj_id,
        "city": proj_city,
        "observations": total_obs,
        "species": total_species,
        "participants": total_participants,
    }
    return result


def create_df_projs(projects):
    proj_metrics = []

    for k, v in projects.items():
        results = get_metrics_proj(k, v)
        proj_metrics.append(results)

    df_projs = pd.DataFrame(proj_metrics)

    return df_projs


def get_missing_taxon(taxon_id, rank):
    headers = {"Authorization": api_token}
    url = f"https://api.minka-sdg.org/v1/taxa/{taxon_id}"
    ancestors = SESSION.get(url, headers=headers).json()["results"][0]["ancestors"]
    for anc in ancestors:
        if anc["rank"] == rank:
            return anc["name"]


def _get_species(user_name, proj_id):
    headers = {"Authorization": api_token}
    species = f"{API_PATH}/observations/species_counts"
    params = {"project_id": proj_id, "user_login": user_name, "rank": "species"}
    return SESSION.get(species, params=params, headers=headers).json()["total_results"]


def get_list_users(id_project):
    headers = {"Authorization": api_token}
    users = []
    url1 = f"https://api.minka-sdg.org/v1/observations/observers?project_id={id_project}&quality_grade=research"
    results = SESSION.get(url1, headers=headers).json()["results"]
    for result in results:
        datos = {}
        datos["user_id"] = result["user_id"]
        datos["participant"] = result["user"]["login"]
        datos["observacions"] = result["observation_count"]
        datos["espècies"] = result["species_count"]
        users.append(datos)
    df_users = pd.DataFrame(users)

    identifiers = []
    url = f"https://api.minka-sdg.org/v1/observations/identifiers?project_id={id_project}&quality_grade=research"
    results = SESSION.get(url, headers=headers).json()["results"]
    for result in results:
        datos = {}
        datos["user_id"] = result["user_id"]
        datos["identificacions"] = result["count"]
        identifiers.append(datos)
    df_identifiers = pd.DataFrame(identifiers)

    df_users = pd.merge(df_users, df_identifiers, how="left", on="user_id")
    df_users.fillna(0, inplace=True)

    return df_users[["participant", "observacions", "espècies", "identificacions"]]


def get_participation_df(main_project):
    pt_users = get_list_users(main_project)
    pt_users_clean = pt_users[-pt_users["participant"].isin(exclude_users)]
    # convertimos nombres de columnas a mayúsculas
    pt_users_clean.columns = pt_users_clean.columns.str.upper()
    return pt_users_clean


def get_marine(taxon_name):
    name_clean = taxon_name.replace(" ", "+")
    status = SESSION.get(
        f"https://www.marinespecies.org/rest/AphiaIDByName/{name_clean}?marine_only=true"
    ).status_code
    if (status == 200) or (status == 206):
        result = True
    else:
        result = False
    return result


# Global cache for taxon_tree
_taxon_tree_cache = None


def get_cached_taxon_tree():
    global _taxon_tree_cache
    if _taxon_tree_cache is None:
        cache_file = "data/taxon_tree_with_marines.csv"
        if os.path.exists(cache_file):
            print("Loading taxon_tree from cache")
            _taxon_tree_cache = pd.read_csv(cache_file)
        else:
            print("Downloading taxon_tree")
            taxon_url = "https://raw.githubusercontent.com/eosc-cos4cloud/mecoda-orange/master/mecoda_orange/data/taxon_tree_with_marines.csv"
            _taxon_tree_cache = pd.read_csv(taxon_url)
            os.makedirs("data", exist_ok=True)
            _taxon_tree_cache.to_csv(cache_file, index=False)
    return _taxon_tree_cache


def get_marine_species(proj_id):
    headers = {"Authorization": api_token}
    total_sp = []

    species = f"{API_PATH}/observations/species_counts?"
    url1 = f"{species}project_id={proj_id}"

    def make_api_request(url, max_retries=3):
        for attempt in range(max_retries):
            try:
                response = SESSION.get(url, headers=headers)
                response.raise_for_status()
                json_data = response.json()

                if "total_results" not in json_data:
                    print(
                        f"Warning: API response missing 'total_results', retrying... (attempt {attempt + 1})"
                    )
                    if attempt < max_retries - 1:
                        time.sleep(2**attempt)
                        continue
                    else:
                        raise ValueError(
                            "API response missing 'total_results' after retries"
                        )

                return json_data["total_results"]
            except Exception as e:
                if attempt < max_retries - 1:
                    print(
                        f"API request failed (attempt {attempt + 1}): {e}, retrying after delay..."
                    )
                    time.sleep(2**attempt)
                else:
                    print(f"API request failed after {max_retries} attempts: {e}")
                    raise

    total_num = make_api_request(url1)

    pages = math.ceil(total_num / 500)

    for i in range(pages):
        especie = {}
        page = i + 1
        url = f"{species}project_id={proj_id}&page={page}"
        response = SESSION.get(url, headers=headers)
        json_data = response.json()
        if "results" not in json_data:
            print(
                f"Warning: API response missing 'results' for page {page}, retrying..."
            )
            time.sleep(1)
            response = SESSION.get(url, headers=headers)
            json_data = response.json()
        results = json_data["results"]
        for result in results:
            especie = {}
            especie["taxon_id"] = result["taxon"]["id"]
            especie["taxon_name"] = result["taxon"]["name"]
            especie["rank"] = result["taxon"]["rank"]
            especie["ancestry"] = result["taxon"]["ancestry"]
            total_sp.append(especie)

    df_species = pd.DataFrame(total_sp)
    taxon_tree = get_cached_taxon_tree()

    df_species = pd.merge(
        df_species,
        taxon_tree[["taxon_id", "marine"]],
        on="taxon_id",
        how="left",
    )
    return df_species


def get_marine_count(df_obs) -> pd.DataFrame:
    df_marines = (
        df_obs.groupby("marine")
        .size()
        .reset_index()
        .rename(columns={"marine": "entorn", 0: "observacions"})
    )

    df_spe = df_obs.groupby("marine")["taxon_name"].nunique().reset_index()
    especies_terrestres = df_spe.loc[df_spe.marine == False, "taxon_name"].item()
    especies_marinas = df_spe.loc[df_spe.marine == True, "taxon_name"].item()

    df_marines["entorn"] = df_marines["entorn"].map({False: "terrestre", True: "marí"})
    df_marines.loc[df_marines.entorn == "marí", "espècies"] = especies_marinas
    df_marines.loc[df_marines.entorn == "terrestre", "espècies"] = especies_terrestres

    df_marines = df_marines.sort_values(by="observacions", ascending=False).reset_index(
        drop=True
    )
    return df_marines


def get_number_identifications(user_name, df_identifiers):
    try:
        number_id = df_identifiers.loc[
            df_identifiers.user_login == user_name, "number"
        ].item()
    except:
        number_id = 0
    return number_id


# update obs for projects
def get_new_data(project):
    df_obs = pd.read_csv(f"data/biomarato25/{project}_obs.csv")
    df_photos = pd.read_csv(f"data/biomarato25/{project}_photos.csv")
    max_id = df_obs["id"].max()

    # Comprueba si hay observaciones nuevas
    obs = get_obs(id_project=project, id_above=max_id)
    if len(obs) > 0:
        print(f"Add {len(obs)} obs in project {project}")
        df_obs2, df_photos2 = get_dfs(obs)
        df_obs = pd.concat([df_obs, df_obs2], ignore_index=True)
        df_obs.to_csv(f"data/biomarato25/{project}_obs.csv", index=False)
        df_photos = pd.concat([df_photos, df_photos2], ignore_index=True)
        df_photos.to_csv(f"data/biomarato25/{project}_photos.csv", index=False)


def update_dfs_projects(
    project,
    day=(datetime.date.today() - datetime.timedelta(days=1)).strftime("%Y-%m-%d"),
):

    # updated today
    obs_nuevas = get_obs(id_project=project, updated_since=day)
    if len(obs_nuevas) > 0:
        df_obs_new, df_photos_new = get_dfs(obs_nuevas)
        df_photos_new["photos_id"] = df_photos_new["photos_id"].astype(int)

        # get downloaded
        df_obs = pd.read_csv(f"data/biomarato25/{project}_obs.csv")
        df_photos = pd.read_csv(f"data/biomarato25/{project}_photos.csv")
        old_obs = df_obs[-df_obs["id"].isin(df_obs_new["id"].to_list())]
        old_photos = df_photos[
            -df_photos["photos_id"].isin(df_photos_new["photos_id"].to_list())
        ]

        # join old and updated
        df_obs_updated = pd.concat(
            [old_obs, df_obs_new], ignore_index=True
        ).sort_values(by="id", ascending=False)
        df_photo_updated = pd.concat(
            [old_photos, df_photos_new], ignore_index=True
        ).sort_values(by="photos_id", ascending=False)
    else:
        df_obs_updated = None
        df_photo_updated = None

    # remove casuals
    obs_casual = get_obs(grade="casual", updated_since=day)
    if len(obs_casual) > 0:
        casual_ids = [ob_casual.id for ob_casual in obs_casual]
        df_obs_updated = df_obs_updated[-df_obs_updated["id"].isin(casual_ids)]
        df_photo_updated = df_photo_updated[-df_photo_updated["id"].isin(casual_ids)]

    df_obs_updated.to_csv(f"data/biomarato25/{project}_obs.csv", index=False)
    df_photo_updated.to_csv(f"data/biomarato25/{project}_photos.csv", index=False)

    print(f"Updated obs and photos for project {project}: {len(df_obs_updated)}")


if __name__ == "__main__":

    # BioMARató 2024
    start_time = time.time()

    # Obtener api_token de admin
    # api_token = get_admin_token()
    api_token = None

    # Actualiza main metrics
    main_metrics_df = update_main_metrics_by_day(main_project_bmt)
    if main_metrics_df is not None:
        main_metrics_df.to_csv(
            f"data/biomarato25/{main_project_bmt}_main_metrics_per_day.csv", index=False
        )
        print("Main metrics actualizada")

    # Actualiza métricas de los proyectos
    df_projs = create_df_projs(projects_bmt)
    df_projs.to_csv(
        f"data/biomarato25/{main_project_bmt}_main_metrics_projects.csv", index=False
    )
    print("Main metrics of city projects actualizado")

    # Actualiza df_obs y df_photos totales
    for id_proj in [417, 418, 419, 420]:
        # Update df_proj
        obs = get_obs(id_project=id_proj, grade="research")
        downloaded_obs = pd.read_csv(f"data/biomarato25/{id_proj}_obs.csv")

        if (len(obs) > 0) & (len(obs) != len(downloaded_obs)):
            df_obs, df_photos = get_dfs(obs)
            df_obs.to_csv(f"data/biomarato25/{id_proj}_obs.csv", index=False)
            df_photos.to_csv(f"data/biomarato25/{id_proj}_photos.csv", index=False)

            # Sacar columna marino
            print("Sacando columna marine")
            df_obs["taxon_id"] = df_obs["taxon_id"].replace("nan", None)
            df_filtered = df_obs[df_obs["taxon_id"].notnull()].copy()
            df_filtered["taxon_id"] = df_filtered["taxon_id"].astype(int)

            # sacamos listado de especies incluidas en el proyecto con col marina
            print("Sacando listado de especies")
            df_species = get_marine_species(id_proj)

            df_filtered = pd.merge(
                df_filtered,
                df_species[["taxon_id", "marine"]],
                on="taxon_id",
                how="left",
            )

            # Dataframe de participantes
            print("Dataframe de participantes")
            df_users = get_participation_df(id_proj)
            df_users.to_csv(f"data/biomarato25/{id_proj}_users.csv", index=False)

            # Dataframe de marino/terrestre
            print("Cuenta de marinos/terrestres")
            try:
                df_marine = get_marine_count(df_filtered)
                df_marine.to_csv(f"data/biomarato25/{id_proj}_marines.csv", index=False)
            except:
                pass
        else:
            print("Ninguna observación en proyecto:", id_proj)

    # Dataframe métricas totales
    total_species, total_participants, total_obs = get_main_metrics(main_project_bmt)
    df = pd.DataFrame(
        {
            "metrics": ["observacions", "espècies", "participants"],
            "values": [total_obs, total_species, total_participants],
        }
    )
    df.to_csv(
        f"data/biomarato25/{main_project_bmt}_metrics_tiempo_real.csv", index=False
    )

    end_time = time.time()

    execution_time = end_time - start_time

    print(f"Tiempo de ejecución {(execution_time / 60):.2f} minutos")
