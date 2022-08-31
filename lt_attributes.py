## IMPORTS
import requests
from requests.structures import CaseInsensitiveDict
import pandas as pd
import json
from datetime import datetime
import os
from app import app

today_dt = str(datetime.now()).replace(" ", "T")
today = datetime.now().strftime("%m-%d-%y")
month = datetime.now().month
month_name = datetime.now().strftime("%B")
year = datetime.now().year

ROOT_CUSTOMER_ORDERS_FOLDER = r"FILE SHARE"

LEAFTRADE_KEY = app.config.get("LEAFTRADE_KEY")

######### get batches
##  get current list of batches and THC/CBD % to update
def get_lt_batches():
    BASE_URL = "https://app.leaf.trade/"
    LT_ORDERS_ENDPOINT = f"/api/v3/vendor/batches/?page_size=10000&ordering=batch_ref"

    headers = CaseInsensitiveDict()
    headers["Accept"] = "application/json"
    headers["Authorization"] = f"Token {LEAFTRADE_KEY}"
    headers["Accept-Encoding"] = "gzip, deflate, br"
    headers["scheme"] = "https"
    headers[
        "User-Agent"
    ] = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/100.0.4896.88 Safari/537.36"

    url = BASE_URL + LT_ORDERS_ENDPOINT
    resp = requests.get(url, headers=headers)

    if resp.status_code == 200:
        print_gui("Successful Leaf Trade Web Request")
        resp_json = json.loads(resp.text)

        batches_arr = resp_json["results"]

        ######### for each batch, get proptiers
        batches_dict = {}
        for batch in batches_arr:
            enabled = batch["enabled"]
            if enabled:
                batch_id = batch["id"]
                batch_ref = batch["batch_ref"]
                thc = batch["attributes_verbose"]["thc"]
                thca = batch["attributes_verbose"]["thca"]
                cbd = batch["attributes_verbose"]["cbd"]
                enabled = batch["enabled"]

                batches_dict[batch_id] = [batch_ref, thc, thca, cbd, enabled]

        df_batch = pd.DataFrame.from_dict(
            batches_dict,
            orient="index",
            columns=["batch_ref", "thc", "thca", "cbd", "enabled"],
        )
        df_batch.index.name = "batch_id"
        df_batch.to_csv(f"{ROOT_CUSTOMER_ORDERS_FOLDER}\\batches_{today}.csv")

        output = (
            "Number of LeafTrade batches grabbed: " + str(len(df_batch.index)) + "\n"
        )
        print_gui(output)
        return df_batch


def patch_batch(batch_id, batch_ref, thc, thca, cbd):
    BASE_URL = "https://app.leaf.trade/"
    LT_ORDERS_ENDPOINT = f"/api/v3/vendor/batches/{batch_id}/"

    headers = CaseInsensitiveDict()
    headers["Accept"] = "application/json"
    headers["Authorization"] = f"Token {LEAFTRADE_KEY}"
    headers["Accept-Encoding"] = "gzip, deflate, br"
    headers["scheme"] = "https"
    headers[
        "User-Agent"
    ] = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/100.0.4896.88 Safari/537.36"
    headers["Content-type"] = "application/json"

    url = BASE_URL + LT_ORDERS_ENDPOINT
    data = """
    {
        "batch_ref" : "%s",
        "attributes" : {
            "thc": "%s",
            "thca": "%s",
            "cbd": "%s"
        }
    }
    """ % (
        batch_ref,
        thc,
        thca,
        cbd,
    )
    try:
        requests.patch(url, headers=headers, data=data)
        print_gui(f"{batch_id} - {batch_ref} attributes updated.")
    except:
        print_gui(f"Failed to update {batch_id} - {batch_ref}.")


def get_lt_inventory():
    BASE_URL = "https://app.leaf.trade/"
    LT_ORDERS_ENDPOINT = (
        f"/api/v3/vendor/inventory/?page_size=10000&status=enabled&ordering=batch_ref"
    )

    headers = CaseInsensitiveDict()
    headers["Accept"] = "application/json"
    headers["Authorization"] = f"Token {LEAFTRADE_KEY}"
    headers["Accept-Encoding"] = "gzip, deflate, br"
    headers["scheme"] = "https"
    headers[
        "User-Agent"
    ] = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/100.0.4896.88 Safari/537.36"

    url = BASE_URL + LT_ORDERS_ENDPOINT
    resp = requests.get(url, headers=headers)

    if resp.status_code == 200:
        print_gui("Successful Leaf Trade Web Request")
        resp_json = json.loads(resp.text)
        inv_arr = resp_json["results"]
        ######### for each batch, get proptiers
        inv_dict = {}
        for inv in inv_arr:
            inv_id = inv["id"]
            batch_ref = inv["stock"][0]["batch_ref"]
            pack_size = inv["package_size"]
            pack_uom = inv["package_unit_of_measure"]
            price = inv["price"]
            quantity = inv["stock"][0]["quantity"]
            storefront_name = inv["name"]

            inv_dict[inv_id] = [
                batch_ref,
                pack_size,
                pack_uom,
                price,
                quantity,
                storefront_name,
            ]

        df_inv = pd.DataFrame.from_dict(
            inv_dict,
            orient="index",
            columns=[
                "batch_ref",
                "pack_size",
                "pack_uom",
                "price",
                "quantity",
                "storefront_name",
            ],
        )
        df_inv.index.name = "inv_id"
        df_inv.to_csv(f"{ROOT_CUSTOMER_ORDERS_FOLDER}\\inventory_{today}.csv")

        output = "Number of LeafTrade batches grabbed: " + str(len(df_inv.index)) + "\n"
        print_gui(output)
        return df_inv


def patch_inventory(
    inv_id, batch_ref, pack_size, pack_uom, price, quantity, storefront_name
):
    BASE_URL = "https://app.leaf.trade/"
    LT_ORDERS_ENDPOINT = f"/api/v3/vendor/inventory/{inv_id}/"

    headers = CaseInsensitiveDict()
    headers["Accept"] = "application/json"
    headers["Authorization"] = f"Token {LEAFTRADE_KEY}"
    headers["Accept-Encoding"] = "gzip, deflate, br"
    headers["scheme"] = "https"
    headers[
        "User-Agent"
    ] = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/100.0.4896.88 Safari/537.36"
    headers["Content-type"] = "application/json"

    url = BASE_URL + LT_ORDERS_ENDPOINT
    data = """
    {
        "batch_ref" : "%s",
        "package_size" : "%s",
        "package_unit_of_measure" : "%s",
        "price" : "%s",
        "quantity" : "%s",
        "storefront_name" : "%s",
    }
    """ % (
        batch_ref,
        pack_size,
        pack_uom,
        price,
        quantity,
        storefront_name,
    )
    try:
        resp = requests.patch(url, headers=headers, data=data)
        print_gui(f"{inv_id} - {batch_ref} attributes updated.")
    except:
        print_gui(f"Failed to update {inv_id} - {batch_ref}.")


####### GUI FUNCTIONS
def print_gui(whatever):
    ##    with open('readme.txt', 'a') as f:
    ##        f.write(whatever)
    ##        f.close()
    # print(whatever)
    return


####### PROGRAM RUN

#### BATCH FUNCTIONS - UPDATE THC, THCA, CBD
def fetch_leaftrade_batches():
    start = datetime.now()
    output = "Program Start: " + str(start)
    print_gui(output)

    get_lt_batches()

    end = datetime.now()
    output = "\nProgram End: " + str(end)
    print_gui(output)


def patch_batch_list(FILE_PATH):
    start = datetime.now()
    output = "Program Start: " + str(start)
    print_gui(output)

    df_batches = pd.read_csv(FILE_PATH)

    for i, batch in df_batches.iterrows():
        batch_id = batch["batch_id"]
        batch_ref = batch["batch_ref"]
        batch_thc = batch["thc"]
        batch_thca = batch["thca"]
        batch_cbd = batch["cbd"]

        try:
            patch_batch(batch_id, batch_ref, batch_thc, batch_thca, batch_cbd)
            print_gui("Succesful batch update.")
        except:
            print_gui(f"Failed batch update.")

    end = datetime.now()
    output = "\nProgram End: " + str(end)
    print_gui(output)


#### INVENTORY FUNCTIONS - UPDATE PRICE, PACKAGE SIZE, UOM, QUANTITY, STOREFRONT NAME
def fetch_leaftrade_inventory():
    start = datetime.now()
    output = "Program Start: " + str(start)
    print_gui(output)

    get_lt_inventory()

    end = datetime.now()
    output = "\nProgram End: " + str(end)
    print_gui(output)


def patch_inv_list(FILE_PATH):
    start = datetime.now()
    output = "Program Start: " + str(start)
    print_gui(output)

    df_inv = pd.read_csv(FILE_PATH)

    for i, inv in df_inv.iterrows():
        inv_id = inv["inv_id"]
        batch_ref = inv["batch_ref"]
        pack_size = inv["pack_size"]
        pack_uom = inv["pack_uom"]
        price = inv["price"]
        quantity = inv["quantity"]
        storefront_name = inv["storefront_name"]

        try:
            patch_inventory(
                inv_id, batch_ref, pack_size, pack_uom, price, quantity, storefront_name
            )
            print_gui("Succesful batch update.")
        except:
            print_gui(f"Failed batch update.")

    end = datetime.now()
    output = "\nProgram End: " + str(end)
    print_gui(output)
