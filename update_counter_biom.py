import pandas as pd
import requests

API_PATH = "https://api.minka-sdg.org/v1"


def get_metrics_proj(proj_ids):
    observations = f"{API_PATH}/observations?"
    species = f"{API_PATH}/observations/species_counts?"
    observers = f"{API_PATH}/observations/observers?"

    params = {
        "project_id": proj_ids,
        "order": "desc",
        "order_by": "created_at",
    }
    # Crear una sesión de requests
    session = requests.Session()
    total_species = session.get(species, params=params).json()["total_results"]
    total_participants = session.get(observers, params=params).json()["total_results"]
    total_obs = session.get(observations, params=params).json()["total_results"]

    result = {
        "observations": total_obs,
        "species": total_species,
        "participants": total_participants,
    }

    df_total = pd.DataFrame([result])
    return df_total


if __name__ == "__main__":

    proj_ids = "285, 283, 124, 20, 367"

    df_total = get_metrics_proj(proj_ids)

    df_total.to_csv("data/biomarato_counter.csv", index=False)
