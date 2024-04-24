import datetime

import pandas as pd
import requests
from mecoda_minka import get_dfs, get_obs

API_PATH = "https://api.minka-sdg.org/v1"

main_project = 283

projects = {
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
]


def update_main_metrics(proj_id):
    results = []
    observations = f"{API_PATH}/observations?"
    species = f"{API_PATH}/observations/species_counts?"
    observers = f"{API_PATH}/observations/observers?"

    # Crear una sesión de requests
    session = requests.Session()

    # Fecha de inicio de BioDiverCiutat
    day = datetime.date(year=2024, month=5, day=6)
    # rango_temporal = (datetime.date(year=2024, month=10, day=15) - day).days
    rango_temporal = (datetime.datetime.today().date() - day).days

    if i > 0:
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
    pt_users = pt_users[-pt_users["user_login"].isin(exclude_users)].reset_index(
        drop=True
    )
    pt_users["identificacions"] = pt_users["participant"].apply(
        lambda x: _get_identifiers(x, main_project)
    )
    pt_users["espècies"] = pt_users["participant"].apply(
        lambda x: _get_species(x, main_project)
    )
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

    # Actualiza main metrics
    main_metrics_df = update_main_metrics(main_project)
    main_metrics_df.to_csv(f"data/{main_project}_main_metrics_per_day.csv", index=False)
    print("Main metrics actualizada")

    # Actualiza métricas de los proyectos
    df_projs = create_df_projs(projects)
    df_projs.to_csv(f"data/{main_project}_main_metrics_projects.csv", index=False)
    print("Main metrics of city projects actualizado")

    # Actualiza df_obs y df_photos totales
    obs = get_obs(id_project=main_project)
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

        df_obs.to_csv(f"data/{main_project}_obs.csv", index=False)
        df_photos.to_csv(f"data/{main_project}_photos.csv", index=False)

        # Dataframe de participantes
        df_users = get_participation_df(main_project)
        df_users.to_csv(f"data/{main_project}_users.csv", index=False)

        # Dataframe de marino/terrestre
        df_marine = get_marine_df(df_obs)
        df_marine.to_csv(f"data/{main_project}_marines.csv", index=False)

    # Actualiza df_obs y df_photos de las provincias
    for proj in projects.keys():
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
    total_species, total_participants, total_obs = get_main_metrics(main_project)
    df = pd.DataFrame(
        {
            "metrics": ["observacions", "espècies", "participants"],
            "values": [total_obs, total_species, total_participants],
        }
    )
    df.to_csv(f"data/{main_project}_metrics_tiempo_real.csv", index=False)
