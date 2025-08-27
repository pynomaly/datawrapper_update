import datetime
import math
import os
import time
import json
import hashlib
from concurrent.futures import ThreadPoolExecutor, as_completed
from functools import wraps

import pandas as pd
import requests
from mecoda_minka import get_dfs, get_obs

API_PATH = "https://api.minka-sdg.org/v1"

# Global session for all requests
SESSION = requests.Session()

# Rate limiting configuration - optimizado para velocidad pero seguro
API_RATE_LIMIT = float(os.environ.get('API_RATE_LIMIT', 1.2))  # Reducido de 3s a 1.2s
API_RETRY_MAX = int(os.environ.get('API_RETRY_MAX', 3))
API_BACKOFF_FACTOR = int(os.environ.get('API_BACKOFF_FACTOR', 2))
BATCH_SIZE = int(os.environ.get('BATCH_SIZE', 4))  # Procesar en lotes de 4

# Global rate limiter
last_request_time = 0

# Cache configuration
CACHE_DIR = "data/api_cache"
CACHE_DURATION = int(os.environ.get('CACHE_DURATION', 1800))  # 30 minutes default for CI

def get_cache_path(url, params=None):
    """Genera path del cache basado en URL y parámetros"""
    cache_key = f"{url}_{params}"
    cache_hash = hashlib.md5(cache_key.encode()).hexdigest()
    return os.path.join(CACHE_DIR, f"{cache_hash}.json")

def is_cache_valid(cache_file):
    """Verifica si el cache es válido (no expirado)"""
    if not os.path.exists(cache_file):
        return False
    
    cache_age = time.time() - os.path.getmtime(cache_file)
    return cache_age < CACHE_DURATION

def load_from_cache(cache_file):
    """Carga datos del cache"""
    try:
        with open(cache_file, 'r') as f:
            return json.load(f)
    except:
        return None

def save_to_cache(cache_file, data):
    """Guarda datos al cache"""
    try:
        os.makedirs(CACHE_DIR, exist_ok=True)
        with open(cache_file, 'w') as f:
            json.dump(data, f)
    except Exception as e:
        print(f"Error saving to cache: {e}")

def rate_limited_request(func):
    """Decorator para limitar la tasa de peticiones a la API"""
    @wraps(func)
    def wrapper(*args, **kwargs):
        global last_request_time
        current_time = time.time()
        time_since_last = current_time - last_request_time
        
        if time_since_last < API_RATE_LIMIT:
            sleep_time = API_RATE_LIMIT - time_since_last
            print(f"Rate limiting: sleeping for {sleep_time:.2f}s")
            time.sleep(sleep_time)
        
        last_request_time = time.time()
        return func(*args, **kwargs)
    return wrapper

@rate_limited_request
def safe_api_request(url, params=None, max_retries=None, use_cache=True):
    """Hace petición a la API con cache, rate limiting y manejo de errores"""
    if max_retries is None:
        max_retries = API_RETRY_MAX
    
    # Verificar cache primero
    cache_file = None
    if use_cache:
        cache_file = get_cache_path(url, params)
        if is_cache_valid(cache_file):
            cached_data = load_from_cache(cache_file)
            if cached_data:
                print(f"Using cached data for {url}")
                return cached_data
        
    for attempt in range(max_retries):
        try:
            response = SESSION.get(url, params=params, timeout=30)
            
            if response.status_code == 429:  # Too Many Requests
                wait_time = API_BACKOFF_FACTOR ** attempt
                print(f"Rate limited (429), waiting {wait_time}s before retry {attempt+1}/{max_retries}")
                time.sleep(wait_time)
                continue
                
            response.raise_for_status()
            data = response.json()
            
            # Guardar en cache
            if use_cache and cache_file and data:
                save_to_cache(cache_file, data)
                
            return data
            
        except requests.exceptions.RequestException as e:
            wait_time = API_BACKOFF_FACTOR ** attempt
            print(f"Request error (attempt {attempt+1}/{max_retries}): {e}")
            if attempt < max_retries - 1:
                print(f"Retrying in {wait_time}s...")
                time.sleep(wait_time)
            else:
                print(f"Max retries reached for {url}")
                return None
                
    return None

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
    species_data = safe_api_request(url1)
    total_species = species_data.get("total_results", 0) if species_data else 0

    observers = f"{API_PATH}/observations/observers?"
    url2 = f"{observers}&project_id={proj_id}"
    observers_data = safe_api_request(url2)
    total_participants = observers_data.get("total_results", 0) if observers_data else 0

    observations = f"{API_PATH}/observations?"
    url3 = f"{observations}&project_id={proj_id}"
    obs_data = safe_api_request(url3)
    total_obs = obs_data.get("total_results", 0) if obs_data else 0

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
            print(
                f"Observers API response for {day_str}: {list(observers_json.keys())}"
            )
        if "total_results" not in observations_json:
            print(
                f"Observations API response for {day_str}: {list(observations_json.keys())}"
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


def fetch_day_batch_parallel(proj_id, days_batch):
    """Procesa un lote de días usando ThreadPoolExecutor muy limitado"""
    
    def fetch_single_day_metrics(day_str):
        observations = f"{API_PATH}/observations?"
        species = f"{API_PATH}/observations/species_counts?"
        observers = f"{API_PATH}/observations/observers?"
        
        params = {
            "project_id": proj_id,
            "created_d2": day_str,
            "order": "desc",
            "order_by": "created_at",
        }
        
        # Hacer las 3 peticiones secuenciales para este día
        species_data = safe_api_request(species, params, use_cache=True)
        observers_data = safe_api_request(observers, params, use_cache=True) 
        obs_data = safe_api_request(observations, params, use_cache=True)
        
        return {
            "date": day_str,
            "observations": obs_data.get("total_results", 0) if obs_data else 0,
            "species": species_data.get("total_results", 0) if species_data else 0,
            "participants": observers_data.get("total_results", 0) if observers_data else 0,
        }
    
    results = []
    # Solo 2 workers para evitar sobrecargar la API
    with ThreadPoolExecutor(max_workers=2) as executor:
        future_to_day = {executor.submit(fetch_single_day_metrics, day): day for day in days_batch}
        
        for future in as_completed(future_to_day):
            try:
                result = future.result()
                results.append(result)
                print(f"✓ {result['date']}: {result['observations']} obs, {result['species']} spp")
            except Exception as e:
                day = future_to_day[future]
                print(f"✗ Error en {day}: {e}")
                results.append({"date": day, "observations": 0, "species": 0, "participants": 0})
    
    return sorted(results, key=lambda x: x['date'])

def update_main_metrics_by_day(proj_id):
    results = []

    # Rango de días de BioMARato
    day = datetime.date(year=2025, month=5, day=3)
    rango_temporal = (datetime.datetime.today().date() - day).days
    print(f"Procesando {rango_temporal + 1} días en lotes de {BATCH_SIZE}")

    def get_total_results_with_retry_deprecated(url, params, max_retries=5, initial_wait=1):
        """
        Hace petición a la API con reintentos y manejo de límites de API
        """
        for attempt in range(max_retries):
            try:
                response = SESSION.get(url, params=params)
                response.raise_for_status()

                json_data = response.json()

                if "total_results" in json_data:
                    return json_data["total_results"]
                else:
                    print(f"Warning: 'total_results' not found in response for {url}")
                    print(f"Response keys: {list(json_data.keys())}")

                    if attempt < max_retries - 1:
                        wait_time = initial_wait * (2**attempt)  # Exponential backoff
                        print(
                            f"Retrying in {wait_time} seconds... (attempt {attempt + 1}/{max_retries})"
                        )
                        time.sleep(wait_time)
                        continue
                    else:
                        print(
                            f"Failed to get total_results after {max_retries} attempts"
                        )
                        return 0

            except requests.exceptions.RequestException as e:
                print(f"Request error on attempt {attempt + 1}: {e}")
                if attempt < max_retries - 1:
                    wait_time = initial_wait * (2**attempt)
                    print(f"Retrying in {wait_time} seconds...")
                    time.sleep(wait_time)
                    continue
                else:
                    print(f"Request failed after {max_retries} attempts")
                    return 0
            except (KeyError, ValueError) as e:
                print(f"JSON parsing error on attempt {attempt + 1}: {e}")
                if attempt < max_retries - 1:
                    wait_time = initial_wait * (2**attempt)
                    print(f"Retrying in {wait_time} seconds...")
                    time.sleep(wait_time)
                    continue
                else:
                    return 0

        return 0

    if rango_temporal >= 0:
        days_batch = []
        
        for i in range(rango_temporal + 1):
            if datetime.datetime.today().date() >= day:
                st_day = day.strftime("%Y-%m-%d")
                days_batch.append(st_day)
                
                # Procesar lote cuando esté lleno o sea el último día
                if len(days_batch) >= BATCH_SIZE or i == rango_temporal:
                    print(f"\nProcesando lote {len(days_batch)} días: {days_batch[0]} - {days_batch[-1]}")
                    batch_results = fetch_day_batch_parallel(proj_id, days_batch)
                    results.extend(batch_results)
                    days_batch = []  # Limpiar lote
                    
                    # Breve pausa entre lotes
                    if i < rango_temporal:
                        print("Pausa entre lotes...")
                        time.sleep(1)
                
                day = day + datetime.timedelta(days=1)

        result_df = pd.DataFrame(results)
        if not result_df.empty:
            result_df = result_df.sort_values('date').reset_index(drop=True)
        print(f"\n✓ Main metrics actualizadas: {len(result_df)} días")
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

    # Sequential API calls with rate limiting (avoiding parallel overload)
    species_data = safe_api_request(species, params=params)
    total_species = species_data.get("total_results", 0) if species_data else 0
    
    observers_data = safe_api_request(observers, params=params)
    total_participants = observers_data.get("total_results", 0) if observers_data else 0
    
    obs_data = safe_api_request(observations, params=params)
    total_obs = obs_data.get("total_results", 0) if obs_data else 0

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
    data = safe_api_request(species, params=params)
    return data.get("total_results", 0) if data else 0


def get_list_users(id_project):
    users = []
    url1 = f"https://api.minka-sdg.org/v1/observations/observers?project_id={id_project}&quality_grade=research"
    observers_data = safe_api_request(url1)
    
    if observers_data and "results" in observers_data:
        for result in observers_data["results"]:
            datos = {}
            datos["user_id"] = result["user_id"]
            datos["participant"] = result["user"]["login"]
            datos["observacions"] = result["observation_count"]
            datos["espècies"] = result["species_count"]
            users.append(datos)
    df_users = pd.DataFrame(users)

    identifiers = []
    url = f"https://api.minka-sdg.org/v1/observations/identifiers?project_id={id_project}&quality_grade=research"
    identifiers_data = safe_api_request(url)
    
    if identifiers_data and "results" in identifiers_data:
        for result in identifiers_data["results"]:
            datos = {}
            datos["user_id"] = result["user_id"]
            datos["identificacions"] = result["count"]
            identifiers.append(datos)
    df_identifiers = pd.DataFrame(identifiers)

    if not df_users.empty and not df_identifiers.empty:
        df_users = pd.merge(df_users, df_identifiers, how="left", on="user_id")
        df_users.fillna(0, inplace=True)
        return df_users[["participant", "observacions", "espècies", "identificacions"]]
    elif not df_users.empty:
        df_users["identificacions"] = 0
        return df_users[["participant", "observacions", "espècies", "identificacions"]]
    else:
        return pd.DataFrame(columns=["participant", "observacions", "espècies", "identificacions"])


def get_participation_df(main_project):
    pt_users = get_list_users(main_project)
    pt_users_clean = pt_users[-pt_users["participant"].isin(exclude_users)]
    # convertimos nombres de columnas a mayúsculas
    pt_users_clean.columns = pt_users_clean.columns.str.upper()
    return pt_users_clean


def get_marine(taxon_name):
    name_clean = taxon_name.replace(" ", "+")
    url = f"https://www.marinespecies.org/rest/AphiaIDByName/{name_clean}?marine_only=true"
    
    try:
        # MarineSpecies API no necesita tanto rate limiting, pero aún aplicamos prudencia
        time.sleep(0.1)
        response = SESSION.get(url, timeout=10)
        status = response.status_code
        if (status == 200) or (status == 206):
            result = True
        else:
            result = False
    except:
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


def fetch_species_page_batch(proj_id, page_numbers):
    """Procesa un lote de páginas de especies en paralelo"""
    species = f"{API_PATH}/observations/species_counts?"
    
    def fetch_single_page(page):
        url = f"{species}&project_id={proj_id}&page={page}"
        json_data = safe_api_request(url)
        
        if not json_data or "results" not in json_data:
            return []
            
        page_species = []
        for result in json_data["results"]:
            especie = {
                "taxon_id": result["taxon"]["id"],
                "taxon_name": result["taxon"]["name"],
                "rank": result["taxon"]["rank"],
                "ancestry": result["taxon"]["ancestry"]
            }
            page_species.append(especie)
        return page_species
    
    all_species = []
    # Solo 2 workers para evitar sobrecargar
    with ThreadPoolExecutor(max_workers=2) as executor:
        future_to_page = {executor.submit(fetch_single_page, page): page for page in page_numbers}
        
        for future in as_completed(future_to_page):
            try:
                page_species = future.result()
                all_species.extend(page_species)
                page = future_to_page[future]
                print(f"✓ Página {page}: {len(page_species)} especies")
            except Exception as e:
                page = future_to_page[future]
                print(f"✗ Error en página {page}: {e}")
    
    return all_species

def get_marine_species(proj_id):
    total_sp = []

    species = f"{API_PATH}/observations/species_counts?"
    url1 = f"{species}&project_id={proj_id}"

    species_count_data = safe_api_request(url1)
    if not species_count_data:
        print(f"No se pudo obtener el conteo de especies para el proyecto {proj_id}")
        return pd.DataFrame()
    
    total_num = species_count_data.get("total_results", 0)
    pages = math.ceil(total_num / 500)
    print(f"Procesando {pages} páginas de especies en lotes de {BATCH_SIZE}")

    # Procesar en lotes
    page_batch = []
    for page_idx in range(pages):
        page = page_idx + 1
        page_batch.append(page)
        
        # Procesar lote cuando esté lleno o sea la última página
        if len(page_batch) >= BATCH_SIZE or page_idx == pages - 1:
            print(f"Procesando lote páginas {page_batch[0]}-{page_batch[-1]}")
            batch_species = fetch_species_page_batch(proj_id, page_batch)
            total_sp.extend(batch_species)
            page_batch = []
            
            # Pausa entre lotes
            if page_idx < pages - 1:
                time.sleep(1)

    df_species = pd.DataFrame(total_sp)
    if df_species.empty:
        return df_species
        
    print(f"Obtenidas {len(df_species)} especies, aplicando datos marinos...")
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

    # BioMARató 2025
    start_time = time.time()
    
    print(f"Starting with rate limit: {API_RATE_LIMIT}s, max retries: {API_RETRY_MAX}")
    print(f"Cache duration: {CACHE_DURATION}s")
    
    # Crear directorio de datos si no existe
    os.makedirs("data/biomarato25", exist_ok=True)

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
