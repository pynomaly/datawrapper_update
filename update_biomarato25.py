import datetime
import math
import time
import os
from concurrent.futures import ThreadPoolExecutor, as_completed

import pandas as pd
import requests
from mecoda_minka import get_dfs, get_obs

API_PATH = "https://api.minka-sdg.org/v1"

# Global session for all requests
SESSION = requests.Session()

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


def get_main_metrics(proj_id):
    species = f"{API_PATH}/observations/species_counts?"
    url1 = f"{species}&project_id={proj_id}"
    total_species = SESSION.get(url1).json()["total_results"]

    observers = f"{API_PATH}/observations/observers?"
    url2 = f"{observers}&project_id={proj_id}"
    total_participants = SESSION.get(url2).json()["total_results"]

    observations = f"{API_PATH}/observations?"
    url3 = f"{observations}&project_id={proj_id}"
    total_obs = SESSION.get(url3).json()["total_results"]

    return total_species, total_participants, total_obs


def fetch_day_metrics(proj_id, day_str):
    """Fetch metrics for a single day"""
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
        import time
        
        species_resp = SESSION.get(species, params=params)
        time.sleep(0.1)  # Small delay between requests
        observers_resp = SESSION.get(observers, params=params)
        time.sleep(0.1)
        observations_resp = SESSION.get(observations, params=params)
        
        # Check if responses are valid
        species_json = species_resp.json()
        observers_json = observers_resp.json()
        observations_json = observations_resp.json()
        
        total_species = species_json.get("total_results", 0)
        total_participants = observers_json.get("total_results", 0) 
        total_obs = observations_json.get("total_results", 0)
        
        # Log if any response is missing total_results
        if "total_results" not in species_json:
            print(f"Species API response for {day_str}: {list(species_json.keys())}")
        if "total_results" not in observers_json:
            print(f"Observers API response for {day_str}: {list(observers_json.keys())}")
        if "total_results" not in observations_json:
            print(f"Observations API response for {day_str}: {list(observations_json.keys())}")
        
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
    results = []
    observations = f"{API_PATH}/observations?"
    species = f"{API_PATH}/observations/species_counts?"
    observers = f"{API_PATH}/observations/observers?"

    # Rango de días de BioMARato
    day = datetime.date(year=2025, month=5, day=3)
    rango_temporal = (datetime.datetime.today().date() - day).days
    print("Número de días: ", rango_temporal)

    if rango_temporal >= 0:
        for i in range(rango_temporal + 1):
            print(i)
            if datetime.datetime.today().date() >= day:
                st_day = day.strftime("%Y-%m-%d")

                params = {
                    "project_id": proj_id,
                    "created_d2": st_day,
                    "order": "desc",
                    "order_by": "created_at",
                }

                # Utilizar la sesión global para realizar las solicitudes
                total_species = SESSION.get(species, params=params).json()[
                    "total_results"
                ]
                total_participants = SESSION.get(observers, params=params).json()[
                    "total_results"
                ]
                total_obs = SESSION.get(observations, params=params).json()[
                    "total_results"
                ]

                result = {
                    "date": st_day,
                    "observations": total_obs,
                    "species": total_species,
                    "participants": total_participants,
                }

                results.append(result)
                day = day + datetime.timedelta(days=1)

        result_df = pd.DataFrame(results)
        print("Updated main metrics")
        return result_df


def get_metrics_proj(proj_id, proj_city):
    observations = f"{API_PATH}/observations?"
    species = f"{API_PATH}/observations/species_counts?"
    observers = f"{API_PATH}/observations/observers?"

    params = {
        "project_id": proj_id,
        "order": "desc",
        "order_by": "created_at",
    }
    
    # Parallelize the 3 API calls
    with ThreadPoolExecutor(max_workers=3) as executor:
        future_species = executor.submit(SESSION.get, species, params=params)
        future_observers = executor.submit(SESSION.get, observers, params=params)
        future_observations = executor.submit(SESSION.get, observations, params=params)
        
        total_species = future_species.result().json()["total_results"]
        total_participants = future_observers.result().json()["total_results"]
        total_obs = future_observations.result().json()["total_results"]

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
    url = f"https://api.minka-sdg.org/v1/taxa/{taxon_id}"
    ancestors = SESSION.get(url).json()["results"][0]["ancestors"]
    for anc in ancestors:
        if anc["rank"] == rank:
            return anc["name"]


def _get_species(user_name, proj_id):
    species = f"{API_PATH}/observations/species_counts"
    params = {"project_id": proj_id, "user_login": user_name, "rank": "species"}
    return SESSION.get(species, params=params).json()["total_results"]


def get_list_users(id_project):
    users = []
    url1 = f"https://api.minka-sdg.org/v1/observations/observers?project_id={id_project}&quality_grade=research"
    results = SESSION.get(url1).json()["results"]
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
    results = SESSION.get(url).json()["results"]
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
    total_sp = []

    species = f"{API_PATH}/observations/species_counts?"
    url1 = f"{species}&project_id={proj_id}"

    total_num = SESSION.get(url1).json()["total_results"]

    pages = math.ceil(total_num / 500)

    for i in range(pages):
        especie = {}
        page = i + 1
        url = f"{species}&project_id={proj_id}&page={page}"
        
        # Rate limiting - pausa entre requests
        time.sleep(0.5)
        
        # Reintentos con manejo de errores
        max_retries = 3
        for retry in range(max_retries):
            try:
                response = SESSION.get(url)
                response.raise_for_status()  # Lanza excepción si status code es error
                
                json_data = response.json()
                if "results" not in json_data:
                    print(f"Warning: 'results' key missing in API response for page {page}")
                    print(f"Response keys: {list(json_data.keys())}")
                    if retry == max_retries - 1:
                        print(f"Skipping page {page} after {max_retries} attempts")
                        break
                    time.sleep(2)  # Pausa más larga antes de reintentar
                    continue
                
                results = json_data["results"]
                break
                
            except requests.exceptions.RequestException as e:
                print(f"Error en request para página {page}, intento {retry + 1}: {e}")
                if retry == max_retries - 1:
                    print(f"Fallaron todos los intentos para página {page}")
                    continue
                time.sleep(2)
            except KeyError as e:
                print(f"Error en estructura de respuesta para página {page}: {e}")
                if retry == max_retries - 1:
                    continue
                time.sleep(2)
        else:
            continue  # Si no se pudo obtener results, continúa con la siguiente página
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
