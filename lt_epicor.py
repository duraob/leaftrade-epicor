## IMPORTS
import pyodbc
import textwrap
import requests
from requests.structures import CaseInsensitiveDict
import pandas as pd
import json
from datetime import datetime
import os
from app import app
from concurrent.futures import ThreadPoolExecutor

today_dt = str(datetime.now()).replace(" ", "T")
today = datetime.now().strftime("%m-%d-%y")
month = datetime.now().month
month_name = datetime.now().strftime("%B")
year = datetime.now().year

ROOT_CUSTOMER_ORDERS_FOLDER = r"FILESHARE"
CURRENT_YEAR_FOLDER = os.path.join(
    ROOT_CUSTOMER_ORDERS_FOLDER, str(datetime.now().year)
)
CURRENT_MONTH_ORDERS_FOLDER = os.path.join(
    CURRENT_YEAR_FOLDER, (str(month) + " " + month_name + " " + str(year))
)
CURRENT_DAY_ORDERS_FOLDER = os.path.join(CURRENT_MONTH_ORDERS_FOLDER, str(today))

EPI_USER = app.config.get("EPICOR_USER")
EPI_PWD = app.config.get("EPICOR_PWD")
LEAFTRADE_KEY = app.config.get("LEAFTRADE_KEY")

######### get order(s) from leaf trade
##  only orders marked as APPROVED: /api/v3/vendor/orders/?status=approved&include_order_items=true
def get_lt_orders():
    BASE_URL = "https://app.leaf.trade/"
    STATUS = "approved"  # 'approved' 'cancelled'
    LT_ORDERS_ENDPOINT = (
        f"/api/v3/vendor/orders/?status={STATUS}&include_order_items=true"
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
        ##        print_gui('Succesful Leaf Trade Web Request')
        resp_json = json.loads(resp.text)
        orders_arr = resp_json["results"]
        ######### for each order, get product info, qty, unit price - split into objects in dict
        df_arr = []
        for order in orders_arr:
            orders_dict = {}
            ndc_arr = []
            pname_arr = []
            unit_price_arr = []
            quantity_arr = []
            order_id_arr = []
            order_status_arr = []
            created_at_arr = []
            user_email_arr = []
            order_total_arr = []
            dispensary_arr = []
            lic_arr = []
            location_arr = []

            order_id = order["id"]
            created_at = order["created_at"]
            order_status = order["status"]
            user_email = order["user_email"]
            order_total_net = order["total_net"]
            dispensary = order["dispensary_location"]["dispensary"]["name"]
            lic = order["dispensary_location"]["license_number"]
            location = order["dispensary_location"]["dispensary"]["address"]["city"]

            for item in order["items"]:
                product_name = item["product_name"]
                ndc = item["pull_number"]
                unit_price = float(item["unit_price_net"])
                quantity = float(item["quantity"])

                ndc_arr.append(ndc)
                pname_arr.append(product_name)
                unit_price_arr.append(unit_price)
                quantity_arr.append(quantity)
                order_id_arr.append(order_id)
                order_status_arr.append(order_status)
                created_at_arr.append(created_at)
                user_email_arr.append(user_email)
                order_total_arr.append(order_total_net)
                dispensary_arr.append(dispensary)
                lic_arr.append(str(lic))
                location_arr.append(location)

            for i, ndc in enumerate(ndc_arr):
                orders_dict[ndc] = [
                    pname_arr[i],
                    unit_price_arr[i],
                    quantity_arr[i],
                    order_id_arr[i],
                    order_status_arr[i],
                    created_at_arr[i],
                    user_email_arr[i],
                    order_total_arr[i],
                    dispensary_arr[i],
                    lic_arr[i],
                    location_arr[i],
                ]

            df_order = pd.DataFrame.from_dict(
                orders_dict,
                orient="index",
                columns=[
                    "product_name",
                    "UNIT $",
                    "QTY",
                    "order_id",
                    "order_status",
                    "created_at",
                    "user_email",
                    "order_total",
                    "dispensary",
                    "lic",
                    "location",
                ],
            )
            df_order.index.name = "NDC"
            df_arr.append(df_order)

        ##        output = 'Number of approved LeafTrade orders grabbed: ' + str(len(df_arr)) + '\n'
        ##        print_gui(output)
        return df_arr


def get_lt_order(orderNum):
    BASE_URL = "https://app.leaf.trade/"
    LT_ORDERS_ENDPOINT = (
        f"/api/v3/vendor/orders/?invoice_id={orderNum}&include_order_items=true"
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
    try:
        resp = requests.get(url, headers=headers)
    except:
        print_gui(
            f"{resp.status_code} - Error requesting LeafTrade Order {orderNum}", "\n"
        )

    df_arr = []
    if resp.status_code == 200:
        ##        print_gui('Succesful Leaf Trade Web Request')
        resp_json = json.loads(resp.text)
        orders_arr = resp_json["results"]
        ######### for each order, get product info, qty, unit price - split into objects in dict

        for order in orders_arr:
            orders_dict = {}
            ndc_arr = []
            pname_arr = []
            unit_price_arr = []
            quantity_arr = []
            order_id_arr = []
            order_status_arr = []
            created_at_arr = []
            user_email_arr = []
            order_total_arr = []
            dispensary_arr = []
            lic_arr = []
            location_arr = []
            order_id = order["id"]
            created_at = order["created_at"]
            order_status = order["status"]
            user_email = order["user_email"]
            order_total_net = order["total_net"]
            dispensary = order["dispensary_location"]["dispensary"]["name"]
            lic = order["dispensary_location"]["license_number"]
            location = order["dispensary_location"]["dispensary"]["address"]["city"]

            for item in order["items"]:
                product_name = item["product_name"]
                ndc = item["pull_number"]
                unit_price = float(item["unit_price_net"])
                quantity = float(item["quantity"])

                ndc_arr.append(ndc)
                pname_arr.append(product_name)
                unit_price_arr.append(unit_price)
                quantity_arr.append(quantity)
                order_id_arr.append(order_id)
                order_status_arr.append(order_status)
                created_at_arr.append(created_at)
                user_email_arr.append(user_email)
                order_total_arr.append(order_total_net)
                dispensary_arr.append(dispensary)
                lic_arr.append(str(lic))
                location_arr.append(location)

            for i, ndc in enumerate(ndc_arr):
                orders_dict[ndc] = [
                    pname_arr[i],
                    unit_price_arr[i],
                    quantity_arr[i],
                    order_id_arr[i],
                    order_status_arr[i],
                    created_at_arr[i],
                    user_email_arr[i],
                    order_total_arr[i],
                    dispensary_arr[i],
                    lic_arr[i],
                    location_arr[i],
                ]

            df_order = pd.DataFrame.from_dict(
                orders_dict,
                orient="index",
                columns=[
                    "product_name",
                    "UNIT $",
                    "QTY",
                    "order_id",
                    "order_status",
                    "created_at",
                    "user_email",
                    "order_total",
                    "dispensary",
                    "lic",
                    "location",
                ],
            )
            df_order.index.name = "NDC"
            df_arr.append(df_order)

        ##        if len(df_arr) > 0:
        ##            print_gui(f'LeafTrade order {orderNum} grabbed.'+ '\n')
        ##        else:
        ##            print_gui(f'{resp.status_code} - Error requesting LeafTrade Order {orderNum}'+ '\n')

        if len(df_arr) == 0:
            ##            print_gui('\nERROR: No LeafTrade Order Retrieved.\n')
            exit()
        return df_arr


def get_EPICOR_skus():
    ###### QUERY - Epicor DB XLOOKUP Table
    conn_str = (
        r"DRIVER={SQL Server};"
        r"SERVER=SERVERNAME;"
        r"DATABASE=DBINSTANCE;"
        r"Trusted_Connection=yes;"
    )

    ##    print_gui(f'Attempting to connect to: EPICOR10\n')
    try:
        cnxn = pyodbc.connect(conn_str)
    ##        print_gui(f'Successful connection to DB.\n')
    except:
        ##        print_gui(f'Error connecting to DB.\n')
        exit()
    ##    print_gui(f'Fetching SKUs from Epicor.\n')
    cursor = cnxn.cursor()
    sql = textwrap.dedent(
        """
    SELECT 
    P.PartNum, P.PartDescription, P.CommercialCategory AS 'TYPE',
    PL.LotNum, PL.PartLotDescription, PL.Batch, PL.MfgLot AS 'NDC', PL.ExpirationDate AS 'EXP DATE', PL.MfgBatch AS 'REG #', PL.HeatNum AS 'THC',
    PLU.BrandName_c AS 'BRAND NAME', PLU.Genetics_c as 'GENETICS', PLU.WeightForm_c as 'WEIGHT / FORM'
    FROM ERP.Part AS P
    INNER JOIN ERP.PartLot AS PL ON P.PartNum = PL.PartNum
    INNER JOIN ERP.PartLot_UD AS PLU ON PL.SysRowID = PLU.ForeignSysRowID
    WHERE P.InActive = 0 AND P.ClassID = ''
    """
    )
    cursor.execute(sql)
    rslt = cursor.fetchall()
    sku_list = [list(elem) for elem in rslt]
    df_sku = pd.DataFrame(
        sku_list,
        columns=[
            "PRODUCT NAME",
            "PartDesc",
            "TYPE",
            "LOT #",
            "LotDesc",
            "Batch",
            "NDC",
            "EXP DATE",
            "REG #",
            "THC",
            "BRAND NAME",
            "GENETICS",
            "WEIGHT / FORM",
        ],
    )
    df_sku.set_index("NDC", inplace=True)
    ##    print_gui('Epicor SKUs Returned\n')
    return df_sku


## GET CUSTOMER TABLE AND DO A LOOK UP FOR CUST NUM AND CUST ID
def get_EPICOR_cust():
    ###### QUERY - Epicor DB XLOOKUP Table
    conn_str = (
        r"DRIVER={SQL Server};"
        r"SERVER=SERVERNAME;"
        r"DATABASE=DBINSTANCE;"
        r"Trusted_Connection=yes;"
    )
    ##    print_gui(f'Attempting to connect to: EPICOR10\n')
    try:
        cnxn = pyodbc.connect(conn_str)
    ##        print_gui(f'Successful connection to DB.\n')
    except:
        ##        print_gui(f'Error connecting to DB.\n')
        exit()

    ##    print_gui(f'Fetching Customers from Epicor.\n')
    cursor = cnxn.cursor()
    sql = textwrap.dedent(
        """
    SELECT C.CustID, C.CustNum, C.Name, C.ResaleID
    FROM ERP.Customer AS C
    """
    )
    cursor.execute(sql)
    rslt = cursor.fetchall()
    cust_list = [list(elem) for elem in rslt]
    df_cust = pd.DataFrame(cust_list, columns=["CustID", "CustNum", "CustName", "Lic"])
    ##    print_gui('Epicor Customer Information Returned\n')
    return df_cust


def compare_LT_EPICOR(LT_ORDERS_LIST, DF_EPICOR_SKU, DF_CUSTOMERS):
    if not os.path.isdir(CURRENT_YEAR_FOLDER):
        os.makedirs(CURRENT_YEAR_FOLDER)
    if not os.path.isdir(CURRENT_MONTH_ORDERS_FOLDER):
        os.makedirs(CURRENT_MONTH_ORDERS_FOLDER)
    if not os.path.isdir(CURRENT_DAY_ORDERS_FOLDER):
        os.makedirs(CURRENT_DAY_ORDERS_FOLDER)

    df_final_arr = []

    for i, df in enumerate(LT_ORDERS_LIST):
        ####### compare product info lines against query in epicor to get specific lot and part num
        df_merge = df.merge(DF_EPICOR_SKU, how="inner", left_on="NDC", right_on="NDC")
        df_final = df_merge[
            [
                "TYPE",
                "EXP DATE",
                "GENETICS",
                "BRAND NAME",
                "REG #",
                "WEIGHT / FORM",
                "QTY",
                "UNIT $",
                "THC",
                "PRODUCT NAME",
                "LOT #",
                "dispensary",
                "lic",
                "location",
                "PartDesc",
                "order_id",
            ]
        ]
        df_final.set_index("TYPE", inplace=True)
        df_final["TOTAL"] = df_final["QTY"] * df_final["UNIT $"]
        total_col = df_final.pop("TOTAL")
        ####### fill out the rest of the info required to make a sales order
        df_final.insert(7, "TOTAL", total_col)
        df_final.insert(8, "RESERVE STOCK", "")
        df_final.insert(9, "", "")
        disp = df_final.iloc[0]["dispensary"]
        lic = df_final.iloc[0]["lic"]
        df_final["ID"] = df_final.lic.map(DF_CUSTOMERS.set_index("Lic").CustID)
        df_final["CustNum"] = df_final.lic.map(DF_CUSTOMERS.set_index("Lic").CustNum)
        loc_ID = df_final.iloc[0]["ID"]
        ####### *solution 1 end - create the csv files for sales team to upload to epicor  - CURRENT_MONTH_ORDERS_FOLDER
        df_final_arr.append(df_final)

    print_gui(
        f"Epicor and Leaf Trade Orders Matched Up. Files saved in {CURRENT_DAY_ORDERS_FOLDER}\n"
    )
    return df_final_arr


def create_epicor_order(custNum, poNum):
    EPI_REST_ORDERS_ENDPOINT = (
        "https://SERVERNAME/DBINSTANCE/api/v1/Erp.BO.SalesOrderSvc/SalesOrders"
    )

    headers = CaseInsensitiveDict()
    headers["Accept"] = "application/json"
    headers["Content-Type"] = "application/json"

    data = {}
    data["OrderNum"] = 0
    data["Company"] = "CP"
    data["CustNum"] = int(custNum)
    data["RequestDate"] = today_dt
    data["NeedByDate"] = today_dt
    data["FOB"] = "Factory"
    data["PONum"] = poNum
    data["ProFormaDate_c"] = today_dt

    resp = requests.post(
        EPI_REST_ORDERS_ENDPOINT,
        json=data,
        headers=headers,
        verify=False,
        auth=(EPI_USER, EPI_PWD),
    )

    if resp.status_code == 201:
        resp_json = json.loads(resp.text)
        orderNum = resp_json["OrderNum"]
        ##        print_gui(f'\nSuccessful EPICOR REST Request - Order {orderNum} Created')
        return orderNum


def create_epicor_order_detail(orderNum, orderLine, part, partDesc, unitPrice, qty):
    EPI_REST_ORDERS_ENDPOINT = (
        "https://SERVERNAME/DBINSTANCE/api/v1/Erp.BO.SalesOrderSvc/OrderDtls"
    )

    headers = CaseInsensitiveDict()
    headers["Accept"] = "application/json"
    headers["Content-Type"] = "application/json"

    data = {}
    data["Company"] = "CP"
    data["OrderNum"] = int(orderNum)  ## get from order creation
    data["OrderLine"] = int(orderLine)  ## increment
    data["PartNum"] = part  ## get from order line
    data["LineDesc"] = partDesc
    data["DocUnitPrice"] = str(unitPrice)  ## get from order line
    data["SellingQuantity"] = str(qty)  ## get from order line

    resp = requests.post(
        EPI_REST_ORDERS_ENDPOINT,
        json=data,
        headers=headers,
        verify=False,
        auth=(EPI_USER, EPI_PWD),
    )

    if resp.status_code == 201:
        resp_json = json.loads(resp.text)
        ##print_gui('Successful EPICOR REST Request - Order Line Created')


def create_epicor_order_rels(orderNum, orderLine, lot):
    EPI_REST_ORDERS_ENDPOINT = f"https://SERVERNAME/DBINSTANCE/api/v1/Erp.BO.SalesOrderSvc/OrderRels(CP,{orderNum},{orderLine},1)"

    headers = CaseInsensitiveDict()
    headers["Accept"] = "application/json"
    headers["Content-Type"] = "application/json"

    data = {}
    data["NeedByDate"] = today_dt  ## get from order line
    data["ReqDate"] = today_dt  ## get from order line
    data["LotNumber_c"] = lot  ## get from order line

    requests.patch(
        EPI_REST_ORDERS_ENDPOINT,
        json=data,
        headers=headers,
        verify=False,
        auth=(EPI_USER, EPI_PWD),
    )


##    if(resp.status_code == 204):
##        #resp_json = json.loads(resp.text)
##        print_gui('Successful EPICOR REST Request - Order Rel Updated')


def process_epicor_orders(order):
    dict_order = {}
    custNum = order.iloc[0]["CustNum"]
    poNum = str(order.iloc[0]["order_id"])
    disp = order.iloc[0]["dispensary"]
    loc = order.iloc[0]["ID"]
    orderNum = create_epicor_order(custNum, poNum)
    # print_gui(f'Epicor Order {orderNum} created for LT Order {poNum}.')
    orderLine = 1
    for i, ln in enumerate(order.values):
        part = ln[11]
        unitPrice = ln[6]
        qty = ln[5]
        lineDesc = ln[16]
        lotNum = ln[12]
        create_epicor_order_detail(orderNum, orderLine, part, lineDesc, unitPrice, qty)
        create_epicor_order_rels(orderNum, orderLine, lotNum)
        # print_gui(f'Order {orderNum} Ln {orderLine} done.')
        orderLine += 1

    dict_order[poNum] = [poNum, str(orderNum), disp, loc, today]

    df = pd.DataFrame.from_dict(
        dict_order, orient="index", columns=["poNum", "orderNum", "disp", "loc", "date"]
    )
    df_trans = pd.read_csv("./transactions_history.csv")
    df_history = pd.concat([df_history, df], ignore_index=True)
    df_history.to_csv("./transactions_history.csv")
    return df


####### GUI FUNCTIONS
def print_gui(whatever):
    ##    with open('readme.txt', 'a') as f:
    ##        f.write(whatever)
    ##        f.close()
    return


####### PROGRAM RUN
def fullRun():
    start = datetime.now()
    output = "Program Start: " + str(start)
    print(output)
    lt_orders_arr = get_lt_orders()
    df_epicor_skus = get_EPICOR_skus()
    df_epicor_customers = get_EPICOR_cust()
    parsed_orders_arr = compare_LT_EPICOR(
        lt_orders_arr, df_epicor_skus, df_epicor_customers
    )
    with ThreadPoolExecutor(max_workers=36) as executor:
        results = executor.map(process_epicor_orders, parsed_orders_arr)
    end = datetime.now()
    output = "\nProgram End: " + str(end)
    print(output)


##    return dict_orders


def oneRun(orderNo):
    orderNum = orderNo
    if not orderNum.isnumeric():
        print_gui(
            "ERROR: LeafTrade Orders are numeric, please give an appropriate entry.\n"
        )
        return

    ##    start = datetime.now()
    ##    output = 'Program Start: '+ str(start)
    ##    print_gui(output)
    lt_orders_arr = get_lt_order(orderNum)
    df_epicor_skus = get_EPICOR_skus()
    df_epicor_customers = get_EPICOR_cust()
    parsed_orders_arr = compare_LT_EPICOR(
        lt_orders_arr, df_epicor_skus, df_epicor_customers
    )
    process_epicor_orders(parsed_orders_arr)


##    end = datetime.now()
##    output = '\nProgram End: '+ str(end)
##    print_gui(output)
