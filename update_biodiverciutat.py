import datetime
import math

import pandas as pd
import requests
from mecoda_minka import get_dfs, get_obs

API_PATH = "https://api.minka-sdg.org/v1"

main_project_bdc = 233  # Area metropolitana de Barcelona, proyecto paraguas

projects_bdc_sorted = {
    79: "Begues",
    80: "Viladecans",
    81: "Sant Climent de Llobregat",
    83: "Cervelló",
    85: "Sant Boi de Llobregat",
    86: "Santa Coloma de Cervelló",
    87: "Sant Vicenç dels Horts",
    88: "la Palma de Cervelló",
    89: "Corbera de Llobregat",
    91: "Sant Andreu de la Barca",
    92: "Castellbisbal",
    93: "El Papiol",
    94: "Molins de Rei",
    95: "Sant Feliu de Llobregat",
    97: "Cornellà de Llobregat",
    98: "l'Hospitalet de Llobregat",
    99: "Esplugues de Llobregat",
    100: "Sant Just Desvern",
    101: "Sant Cugat del Vallès",
    102: "Barberà del Vallès",
    103: "Ripollet",
    104: "Montcada i Reixac",
    106: "Sant Adrià del Besós",
    107: "Badalona",
    108: "Tiana",
    109: "Montgat",
    224: "Barcelona",
    225: "el Prat de Llobregat",
    226: "Pallejà",
    227: "Torrelles de Llobregat",
    228: "Castelldefels",
    229: "Gavà",
    230: "Sant Joan Despí",
    231: "Santa Coloma de Gramenet",
    232: "Àrea marina Barcelona",
}

places_bdc = {
    279: "Begues",
    (281, 354): "Viladecans",
    280: "Sant Climent de Llobregat",
    286: "Cervelló",
    283: "Sant Boi de Llobregat",
    284: "Santa Coloma de Cervelló",
    291: "Sant Vicenç dels Horts",
    285: "la Palma de Cervelló",
    287: "Corbera de Llobregat",
    288: "Sant Andreu de la Barca",
    289: "Castellbisbal",
    293: "el Papiol",
    294: "Molins de Rei",
    295: "Sant Feliu de Llobregat",
    297: "Cornellà de Llobregat",
    298: "l'Hospitalet de Llobregat",
    310: "Esplugues de Llobregat",
    309: "Sant Just Desvern",
    292: "Sant Cugat del Vallès",
    300: "Barberà del Vallès",
    302: "Ripollet",
    303: "Montcada i Reixac",
    (305, 252): "Sant Adrià de Besòs",
    (306, 251): "Badalona",
    308: "Tiana",
    (307, 366, 357): "Montgat",
    (311, 247): "Barcelona",
    (282, 351): "el Prat de Llobregat",
    290: "Pallejà",
    243: "Torrelles de Llobregat",
    (277, 349): "Castelldefels",
    (278, 350): "Gavà",
    296: "Sant Joan Despí",
    304: "Santa Coloma de Gramenet",
}

session = requests.Session()
observations = f"{API_PATH}/observations?"
species = f"{API_PATH}/observations/species_counts?"
observers = f"{API_PATH}/observations/observers?"


def update_main_metrics(proj_id, session=session):
    results = []
    observations = f"{API_PATH}/observations?"
    species = f"{API_PATH}/observations/species_counts?"
    observers = f"{API_PATH}/observations/observers?"

    # Rango de días de BioDiverCiutat 2025
    day = datetime.date(year=2025, month=4, day=25)
    rango_temporal = (datetime.date(year=2025, month=4, day=29) - day).days

    if rango_temporal > 0:
        for i in range(rango_temporal):
            print(i)
            st_day = day.strftime("%Y-%m-%d")

            params = {
                "project_id": proj_id,
                "created_d2": st_day,
                "order": "desc",
                "order_by": "created_at",
            }

            # Utilizar la sesión para realizar las solicitudes
            total_species = session.get(species, params=params).json()["total_results"]
            total_participants = session.get(observers, params=params).json()[
                "total_results"
            ]
            total_obs = session.get(observations, params=params).json()["total_results"]

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


def get_metrics_proj(places_bdc, main_project_bdc, session=session):

    results = []

    for place_ids, place_name in places_bdc.items():
        total_species = 0
        total_participants = 0
        total_obs = 0

        if not isinstance(place_ids, tuple):
            place_ids = (place_ids,)

        for place_id in place_ids:
            params = {
                "project_id": main_project_bdc,
                "place_id": place_id,
                "order": "desc",
                "order_by": "created_at",
            }
            total_species += (
                session.get(species, params=params).json().get("total_results", 0)
            )
            total_participants += (
                session.get(observers, params=params).json().get("total_results", 0)
            )
            total_obs += (
                session.get(observations, params=params).json().get("total_results", 0)
            )

        results.append(
            {
                "city": place_name,
                "observations": total_obs,
                "species": total_species,
                "participants": total_participants,
            }
        )

    # Imprime o usa los resultados
    df_results = pd.DataFrame(results)
    return df_results


def get_missing_taxon(taxon_id, rank):
    url = f"https://api.minka-sdg.org/v1/taxa/{taxon_id}"
    ancestors = requests.get(url).json()["results"][0]["ancestors"]
    for anc in ancestors:
        if anc["rank"] == rank:
            return anc["name"]


def _get_species(user_name, proj_id, session=session):
    species = f"{API_PATH}/observations/species_counts"
    params = {"project_id": proj_id, "user_login": user_name}
    return session.get(species, params=params).json()["total_results"]


def _get_identifiers(proj_id: int, session=session) -> pd.DataFrame:
    url = f"https://api.minka-sdg.org/v1/observations/identifiers?project_id={proj_id}"
    results = session.get(url).json()["results"]
    identifiers = []
    for result in results:
        identifier = {}
        identifier["user_id"] = result["user_id"]
        identifier["user_login"] = result["user"]["login"]
        identifier["number"] = result["count"]
        identifiers.append(identifier)
    return pd.DataFrame(identifiers)


def get_number_identifications(user_name, df_identifiers):
    try:
        number_id = df_identifiers.loc[
            df_identifiers.user_login == user_name, "number"
        ].item()
    except:
        number_id = 0
    return number_id


def get_participation_df(main_project):
    df_obs = pd.read_csv(f"data/biodiverciutat25/{main_project}_obs.csv")
    pt_users = (
        df_obs["user_login"]
        .value_counts()
        .to_frame()
        .reset_index(drop=False)
        .rename(columns={"user_login": "participant", "count": "observacions"})
    )
    df_identifiers = _get_identifiers(main_project)

    pt_users["identificacions"] = pt_users["participant"].apply(
        lambda x: get_number_identifications(x, df_identifiers)
    )
    pt_users["espècies"] = pt_users["participant"].apply(
        lambda x: _get_species(x, main_project)
    )
    # convertimos nombres de columnas a mayúsculas
    pt_users.columns = pt_users.columns.str.upper()
    return pt_users


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


def get_main_metrics(proj_id, session=session):
    species = f"{API_PATH}/observations/species_counts?"
    url1 = f"{species}&project_id={proj_id}"
    total_species = session.get(url1).json()["total_results"]

    observers = f"{API_PATH}/observations/observers?"
    url2 = f"{observers}&project_id={proj_id}"
    total_participants = session.get(url2).json()["total_results"]

    observations = f"{API_PATH}/observations?"
    url3 = f"{observations}&project_id={proj_id}"
    total_obs = session.get(url3).json()["total_results"]

    return total_species, total_participants, total_obs


def get_marine(taxon_name: str, session=session) -> bool:
    """
    Devuelve True/False en base a un taxon_name
    """
    name_clean = taxon_name.replace(" ", "+")
    status = session.get(
        f"https://www.marinespecies.org/rest/AphiaIDByName/{name_clean}?marine_only=true"
    ).status_code
    if (status == 200) or (status == 206):
        result = True
    else:
        result = False
    return result


def get_species_df(proj_id, session=session):
    total_sp = []

    species = f"{API_PATH}/observations/species_counts?"
    url1 = f"{species}&project_id={proj_id}"

    total_num = session.get(url1).json()["total_results"]

    pages = math.ceil(total_num / 500)

    for i in range(pages):
        especie = {}
        page = i + 1
        url = f"{species}&project_id={proj_id}&page={page}"
        results = session.get(url).json()["results"]
        for result in results:
            especie = {}
            especie["taxon_id"] = result["taxon"]["id"]
            especie["taxon_name"] = result["taxon"]["name"]
            especie["rank"] = result["taxon"]["rank"]
            especie["ancestry"] = result["taxon"]["ancestry"]
            total_sp.append(especie)

    df_species = pd.DataFrame(total_sp)

    # Añadimos columna de marine
    taxon_url = "https://raw.githubusercontent.com/eosc-cos4cloud/mecoda-minka/refs/heads/master/src/mecoda_minka/data/taxon_tree.csv"
    taxon_tree = pd.read_csv(taxon_url)

    df_species = pd.merge(
        df_species,
        taxon_tree[["taxon_id", "marine"]],
        on="taxon_id",
        how="left",
    )
    return df_species


def get_marine_species(proj_id, session=session):
    total_sp = []

    species = f"{API_PATH}/observations/species_counts?"
    url1 = f"{species}&project_id={proj_id}"

    total_num = session.get(url1).json()["total_results"]

    pages = math.ceil(total_num / 500)

    for i in range(pages):
        especie = {}
        page = i + 1
        url = f"{species}&project_id={proj_id}&page={page}"
        results = session.get(url).json()["results"]
        for result in results:
            especie = {}
            especie["taxon_id"] = result["taxon"]["id"]
            especie["taxon_name"] = result["taxon"]["name"]
            especie["rank"] = result["taxon"]["rank"]
            especie["ancestry"] = result["taxon"]["ancestry"]
            total_sp.append(especie)

    df_species = pd.DataFrame(total_sp)
    taxon_url = "https://raw.githubusercontent.com/eosc-cos4cloud/mecoda-orange/master/mecoda_orange/data/taxon_tree_with_marines.csv"
    taxon_tree = pd.read_csv(taxon_url)

    df_species = pd.merge(
        df_species,
        taxon_tree[["taxon_id", "marine"]],
        on="taxon_id",
        how="left",
    )
    return df_species


if __name__ == "__main__":

    # Datos de BioDiverCiutat

    # Actualiza main metrics
    main_metrics_df = update_main_metrics(main_project_bdc)
    main_metrics_df.to_csv(
        f"data/biodiverciutat25/{main_project_bdc}_main_metrics.csv", index=False
    )
    print("Main metrics actualizada")

    # Actualiza métricas de los proyectos
    df_projs = get_metrics_proj(places_bdc, main_project_bdc)
    df_projs.to_csv(
        f"data/biodiverciutat25/{main_project_bdc}_main_metrics_projects.csv",
        index=False,
    )
    print("Main metrics of city projects actualizado")

    # Actualiza df_obs y df_photos totales
    obs = get_obs(id_project=main_project_bdc)

    # solo si hay observaciones
    if len(obs) > 0:
        print("Sacando df de observaciones totales")
        df_obs, df_photos = get_dfs(obs)
        df_obs.to_csv(f"data/biodiverciutat25/{main_project_bdc}_obs.csv", index=False)
        df_photos.to_csv(
            f"data/biodiverciutat25/{main_project_bdc}_photos.csv", index=False
        )

        print("Sacando columna marine")
        df_obs["taxon_id"] = df_obs["taxon_id"].replace("nan", None)
        df_obs["taxon_id"] = df_obs["taxon_id"].replace("", None)
        df_filtered = df_obs[df_obs["taxon_id"].notnull()].copy()
        df_filtered["taxon_id"] = df_filtered["taxon_id"].astype(float).astype(int)

        # sacamos listado de especies incluidas en el proyecto con col marina
        df_species = get_marine_species(main_project_bdc)

        df_filtered = pd.merge(
            df_filtered,
            df_species[["taxon_id", "marine"]],
            on="taxon_id",
            how="left",
        )

        # Dataframe de participantes
        print("Dataframe de participantes")
        df_users = get_participation_df(main_project_bdc)
        df_users.to_csv(
            f"data/biodiverciutat25/{main_project_bdc}_users.csv", index=False
        )

        # Dataframe de marino/terrestre
        print("Cuenta de marinos/terrestres")
        df_marine = get_marine_count(df_filtered)
        df_marine.to_csv(
            f"data/biodiverciutat25/{main_project_bdc}_marines.csv", index=False
        )

    # Dataframe métricas totales
    print("Dataframe métricas tiempo real")
    total_species, total_participants, total_obs = get_main_metrics(main_project_bdc)
    df = pd.DataFrame(
        {
            "metrics": ["observacions", "espècies", "participants"],
            "values": [total_obs, total_species, total_participants],
        }
    )
    df.to_csv(
        f"data/biodiverciutat25/{main_project_bdc}_metrics_tiempo_real.csv", index=False
    )
