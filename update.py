import datetime

import pandas as pd
import requests
from mecoda_minka import get_dfs, get_obs

API_PATH = "https://api.minka-sdg.org/v1"

main_project_bdc = 233
main_project_bmt = 283

projects_bdc = {
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
    93: "el Papiol",
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
    106: "Sant Adrià de Besòs",
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

projects_bmt = {
    281: "Girona",
    280: "Tarragona",
    282: "Barcelona",
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
    "uri_domingo",
    "mimo_fecdas",
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
    # meri,
    # marina,
    # vanessa,
    # teresa
]


def update_main_metrics(proj_id):
    results = []
    observations = f"{API_PATH}/observations?"
    species = f"{API_PATH}/observations/species_counts?"
    observers = f"{API_PATH}/observations/observers?"

    # Crear una sesión de requests
    session = requests.Session()

    # Rango de días de BioDiverCiutat
    if proj_id == 233:
        day = datetime.date(year=2024, month=4, day=26)
        rango_temporal = (datetime.date(year=2024, month=4, day=30) - day).days
    # Rango de días de BioMARato
    elif proj_id == 283:
        day = datetime.date(year=2024, month=5, day=6)
        rango_temporal = (datetime.datetime.today().date() - day).days

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


def get_metrics_proj(proj_id, proj_city):
    observations = f"{API_PATH}/observations?"
    species = f"{API_PATH}/observations/species_counts?"
    observers = f"{API_PATH}/observations/observers?"

    params = {
        "project_id": proj_id,
        "order": "desc",
        "order_by": "created_at",
    }
    # Crear una sesión de requests
    session = requests.Session()
    total_species = session.get(species, params=params).json()["total_results"]
    total_participants = session.get(observers, params=params).json()["total_results"]
    total_obs = session.get(observations, params=params).json()["total_results"]

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
    ancestors = requests.get(url).json()["results"][0]["ancestors"]
    for anc in ancestors:
        if anc["rank"] == rank:
            return anc["name"]


def _get_species(user_name, proj_id):
    species = f"{API_PATH}/observations/species_counts"
    params = {"project_id": proj_id, "user_login": user_name}
    return requests.get(species, params=params).json()["total_results"]


def _get_identifiers(user_name, proj_id):
    identifiers = f"{API_PATH}/observations/identifiers"
    params = {"project_id": proj_id, "user_login": user_name}
    return requests.get(identifiers, params=params).json()["total_results"]


def get_participation_df(main_project):
    df_obs = pd.read_csv(f"data/{main_project}_obs.csv")
    pt_users = (
        df_obs["user_login"]
        .value_counts()
        .to_frame()
        .reset_index(drop=False)
        .rename(columns={"user_login": "participant", "count": "observacions"})
    )
    if main_project == 283:
        pt_users = pt_users[-pt_users["user_login"].isin(exclude_users)].reset_index(
            drop=True
        )
    pt_users["identificacions"] = pt_users["participant"].apply(
        lambda x: _get_identifiers(x, main_project)
    )
    pt_users["espècies"] = pt_users["participant"].apply(
        lambda x: _get_species(x, main_project)
    )
    # convertimos nombres de columnas a mayúsculas
    pt_users.columns = pt_users.columns.str.upper()
    return pt_users


def get_marine_df(df_obs) -> pd.DataFrame:
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


def get_main_metrics(proj_id):
    species = f"{API_PATH}/observations/species_counts?"
    url1 = f"{species}&project_id={proj_id}"
    total_species = requests.get(url1).json()["total_results"]

    observers = f"{API_PATH}/observations/observers?"
    url2 = f"{observers}&project_id={proj_id}"
    total_participants = requests.get(url2).json()["total_results"]

    observations = f"{API_PATH}/observations?"
    url3 = f"{observations}&project_id={proj_id}"
    total_obs = requests.get(url3).json()["total_results"]

    return total_species, total_participants, total_obs


if __name__ == "__main__":

    # BioDiverCiutat

    # Actualiza main metrics
    main_metrics_df = update_main_metrics(main_project_bdc)
    main_metrics_df.to_csv(f"data/{main_project_bdc}_main_metrics.csv", index=False)
    print("Main metrics actualizada")

    # Actualiza métricas de los proyectos
    df_projs = create_df_projs(projects_bdc)
    df_projs.to_csv(f"data/{main_project_bdc}_main_metrics_projects.csv", index=False)
    print("Main metrics of city projects actualizado")

    # Actualiza df_obs y df_photos totales
    obs = get_obs(id_project=main_project_bdc)
    if len(obs) > 0:
        df_obs, df_photos = get_dfs(obs)
        df_obs["taxon_id"] = df_obs["taxon_id"].astype(int)

        # Completar campos de taxonomías
        cols = ["class", "order", "family", "genus"]
        for col in cols:
            df_obs.loc[df_obs[col].isnull(), col] = df_obs[df_obs[col].isnull()][
                "taxon_id"
            ].apply(lambda x: get_missing_taxon(x, col))

        # Sacar columna marino
        taxon_url = "https://raw.githubusercontent.com/eosc-cos4cloud/mecoda-orange/master/mecoda_orange/data/taxon_tree_with_marines.csv"
        taxon_tree = pd.read_csv(taxon_url)

        df_obs = pd.merge(
            df_obs, taxon_tree[["taxon_id", "marine"]], on="taxon_id", how="left"
        )

        df_obs.to_csv(f"data/{main_project_bdc}_obs.csv", index=False)
        df_photos.to_csv(f"data/{main_project_bdc}_photos.csv", index=False)

        # Dataframe de participantes
        df_users = get_participation_df(main_project_bdc)
        df_users.to_csv(f"data/{main_project_bdc}_users.csv", index=False)

        # Dataframe de marino/terrestre
        df_marine = get_marine_df(df_obs)
        df_marine.to_csv(f"data/{main_project_bdc}_marines.csv", index=False)

    # Dataframe métricas totales
    total_species, total_participants, total_obs = get_main_metrics(main_project_bdc)
    df = pd.DataFrame(
        {
            "metrics": ["observacions", "espècies", "participants"],
            "values": [total_obs, total_species, total_participants],
        }
    )
    df.to_csv(f"data/{main_project_bdc}_metrics_tiempo_real.csv", index=False)

    # BioMARató 2024

    # Actualiza main metrics
    main_metrics_df = update_main_metrics(main_project_bmt)
    if main_metrics_df is not None:
        main_metrics_df.to_csv(
            f"data/{main_project_bmt}_main_metrics_per_day.csv", index=False
        )
        print("Main metrics actualizada")

    # Actualiza métricas de los proyectos
    df_projs = create_df_projs(projects_bmt)
    df_projs.to_csv(f"data/{main_project_bmt}_main_metrics_projects.csv", index=False)
    print("Main metrics of city projects actualizado")

    # Actualiza df_obs y df_photos totales
    obs = get_obs(id_project=main_project_bmt)
    if len(obs) > 0:
        df_obs, df_photos = get_dfs(obs)
        df_obs["taxon_id"] = df_obs["taxon_id"].astype(int)

        # Completar campos de taxonomías
        cols = ["class", "order", "family", "genus"]
        for col in cols:
            df_obs.loc[df_obs[col].isnull(), col] = df_obs[df_obs[col].isnull()][
                "taxon_id"
            ].apply(lambda x: get_missing_taxon(x, col))

        # Sacar columna marino
        taxon_url = "https://raw.githubusercontent.com/eosc-cos4cloud/mecoda-orange/master/mecoda_orange/data/taxon_tree_with_marines.csv"
        taxon_tree = pd.read_csv(taxon_url)

        df_obs = pd.merge(
            df_obs, taxon_tree[["taxon_id", "marine"]], on="taxon_id", how="left"
        )

        df_obs.to_csv(f"data/{main_project_bmt}_obs.csv", index=False)
        df_photos.to_csv(f"data/{main_project_bmt}_photos.csv", index=False)

        # Dataframe de participantes
        df_users = get_participation_df(main_project_bmt)
        df_users.to_csv(f"data/{main_project_bmt}_users.csv", index=False)

        # Dataframe de marino/terrestre
        df_marine = get_marine_df(df_obs)
        df_marine.to_csv(f"data/{main_project_bmt}_marines.csv", index=False)

    # Actualiza df_obs y df_photos de las provincias
    for proj in projects_bmt.keys():
        obs = get_obs(id_project=proj)
        if len(obs) > 0:
            df_obs, df_photos = get_dfs(obs)
            df_obs["taxon_id"] = df_obs["taxon_id"].astype(int)

            # Completar campos de taxonomías
            cols = ["class", "order", "family", "genus"]
            for col in cols:
                df_obs.loc[df_obs[col].isnull(), col] = df_obs[df_obs[col].isnull()][
                    "taxon_id"
                ].apply(lambda x: get_missing_taxon(x, col))

            # Sacar columna marino
            taxon_url = "https://raw.githubusercontent.com/eosc-cos4cloud/mecoda-orange/master/mecoda_orange/data/taxon_tree_with_marines.csv"
            taxon_tree = pd.read_csv(taxon_url)

            df_obs = pd.merge(
                df_obs, taxon_tree[["taxon_id", "marine"]], on="taxon_id", how="left"
            )

            df_obs.to_csv(f"data/{proj}_obs.csv", index=False)
            df_photos.to_csv(f"data/{proj}_photos.csv", index=False)

            # Dataframe de participantes
            df_users = get_participation_df(proj)
            df_users.to_csv(f"data/{proj}_users.csv", index=False)

            # Dataframe de marino/terrestre
            df_marine = get_marine_df(df_obs)
            df_marine.to_csv(f"data/{proj}_marines.csv", index=False)

    # Dataframe métricas totales
    total_species, total_participants, total_obs = get_main_metrics(main_project_bmt)
    df = pd.DataFrame(
        {
            "metrics": ["observacions", "espècies", "participants"],
            "values": [total_obs, total_species, total_participants],
        }
    )
    df.to_csv(f"data/{main_project_bmt}_metrics_tiempo_real.csv", index=False)
