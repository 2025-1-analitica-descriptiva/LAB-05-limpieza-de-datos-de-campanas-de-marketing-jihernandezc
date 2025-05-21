"""
Escriba el codigo que ejecute la accion solicitada.
"""

# pylint: disable=import-outside-toplevel

import os
import zipfile
import pandas as pd
from glob import glob

def clean_campaign_data():
    """
    En esta tarea se le pide que limpie los datos de una campaña de
    marketing realizada por un banco, la cual tiene como fin la
    recolección de datos de clientes para ofrecerls un préstamo.

    La información recolectada se encuentra en la carpeta
    files/input/ en varios archivos csv.zip comprimidos para ahorrar
    espacio en disco.

    Usted debe procesar directamente los archivos comprimidos (sin
    descomprimirlos). Se desea partir la data en tres archivos csv
    (sin comprimir): client.csv, campaign.csv y economics.csv.
    Cada archivo debe tener las columnas indicadas.

    Los tres archivos generados se almacenarán en la carpeta files/output/.

    client.csv:
    - client_id
    - age
    - job: se debe cambiar el "." por "" y el "-" por "_"
    - marital
    - education: se debe cambiar "." por "_" y "unknown" por pd.NA
    - credit_default: convertir a "yes" a 1 y cualquier otro valor a 0
    - mortage: convertir a "yes" a 1 y cualquier otro valor a 0

    campaign.csv:
    - client_id
    - number_contacts
    - contact_duration
    - previous_campaing_contacts
    - previous_outcome: cmabiar "success" por 1, y cualquier otro valor a 0
    - campaign_outcome: cambiar "yes" por 1 y cualquier otro valor a 0
    - last_contact_day: crear un valor con el formato "YYYY-MM-DD",
        combinando los campos "day" y "month" con el año 2022.

    economics.csv:
    - client_id
    - const_price_idx
    - eurobor_three_months



    """

    input_path = "files/input/"
    output_path = "files/output/"

    os.makedirs(output_path, exist_ok=True)

    zip_files = glob(os.path.join(input_path, "*.zip"))

    dataframes = []
    for zip_file in zip_files:
        with zipfile.ZipFile(zip_file) as archive:
            for name in archive.namelist():
                if name.endswith(".csv"):
                    with archive.open(name) as f:
                        df = pd.read_csv(f)
                        dataframes.append(df)

    df_all = pd.concat(dataframes, ignore_index=True)

    # CLIENT
    df_client = df_all[[
        "client_id", "age", "job", "marital", "education",
        "credit_default", "mortgage"
    ]].copy()

    df_client["job"] = df_client["job"].str.replace(".", "", regex=False).str.replace("-", "_", regex=False)
    df_client["education"] = df_client["education"].str.replace(".", "_", regex=False).replace("unknown", pd.NA)
    df_client["credit_default"] = (df_client["credit_default"].astype(str).str.lower() == "yes").astype(int)
    df_client["mortgage"] = (df_client["mortgage"].astype(str).str.lower() == "yes").astype(int)

    df_client.to_csv(os.path.join(output_path, "client.csv"), index=False)

    # CAMPAIGN
    df_campaign = df_all[[
        "client_id", "number_contacts", "contact_duration",
        "previous_campaign_contacts", "previous_outcome",
        "campaign_outcome", "day", "month"
    ]].copy()

    df_campaign["previous_outcome"] = (df_campaign["previous_outcome"].astype(str).str.lower() == "success").astype(int)
    df_campaign["campaign_outcome"] = (df_campaign["campaign_outcome"].astype(str).str.lower() == "yes").astype(int)

    month_map = {
        "jan": "01", "feb": "02", "mar": "03", "apr": "04",
        "may": "05", "jun": "06", "jul": "07", "aug": "08",
        "sep": "09", "oct": "10", "nov": "11", "dec": "12"
    }

    df_campaign["last_contact_date"] = pd.to_datetime(
        "2022-" + df_campaign["month"].str.lower().map(month_map) + "-" + df_campaign["day"].astype(str),
        errors="coerce"
    ).dt.strftime("%Y-%m-%d")

    df_campaign = df_campaign[[
        "client_id", "number_contacts", "contact_duration",
        "previous_campaign_contacts", "previous_outcome",
        "campaign_outcome", "last_contact_date"
    ]]

    df_campaign.to_csv(os.path.join(output_path, "campaign.csv"), index=False)

    # ECONOMICS
    df_economics = df_all[[
        "client_id", "cons_price_idx", "euribor_three_months"
    ]].copy()

    df_economics.to_csv(os.path.join(output_path, "economics.csv"), index=False)

    return

if __name__ == "__main__":
    clean_campaign_data()