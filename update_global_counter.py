import pandas as pd
import requests

API_PATH = "https://api.minka-sdg.org/v1"


def get_metrics_proj(proj_ids):
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

    total_species = session.get(species, params=params).json()["total_results"]
    total_participants = session.get(observers, params=params).json()["total_results"]
    total_obs = session.get(observations, params=params).json()["total_results"]

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

    proj_ids = "285, 283, 124, 20, 367, 417"

    # 285, biomaratona-norte-2024
    # 283, BioMARató 2024 (Catalunya)
    # 124, biomarato-2023-catalunya
    # 20,  biomarato-2022-catalunya
    # 367, BioMARató 2021 (Catalunya)
    # 417, biomarato-2025-catalunya

    df_total = get_metrics_proj(proj_ids)
    print(df_total)

    df_total.to_csv("data/biomarato_global_counter.csv", index=False)
