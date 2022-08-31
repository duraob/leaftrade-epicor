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

today_dt = str(datetime.now()).replace(' ', 'T')
today = datetime.now().strftime("%m-%d-%y")
month = datetime.now().month
month_name = datetime.now().strftime('%B')
year = datetime.now().year

ROOT_CUSTOMER_ORDERS_FOLDER = r'\\svpo0fs01\Departmental\Order Forms'
CURRENT_YEAR_FOLDER = os.path.join(ROOT_CUSTOMER_ORDERS_FOLDER, (str(year) + ' ' + 'Order Forms'))
CURRENT_MONTH_ORDERS_FOLDER = os.path.join(CURRENT_YEAR_FOLDER, (str(month) + ' ' + month_name + ' ' + str(year)))
CURRENT_DAY_ORDERS_FOLDER = os.path.join(CURRENT_MONTH_ORDERS_FOLDER, str(today))

LEAFTRADE_KEY = app.config.get('LEAFTRADE_KEY')

######### get issue(s) from leaf trade
##  only issues marked as APPROVED: /api/v3/vendor/orders/?status=approved&include_order_items=true
def get_lt_inventory():
    BASE_URL = 'https://app.leaf.trade/'
    LT_PRODUCTS_ENDPOINT = f'/api/v3/vendor/inventory/?page_size=10000'

    headers = CaseInsensitiveDict()
    headers["Accept"] = "application/json"
    headers["Authorization"] = f'Token {LEAFTRADE_KEY}'
    headers['Accept-Encoding'] = 'gzip, deflate, br'
    headers['scheme'] = 'https'
    headers['User-Agent'] = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/100.0.4896.88 Safari/537.36'

    url = BASE_URL + LT_PRODUCTS_ENDPOINT
    resp = requests.get(url, headers=headers)

    if(resp.status_code == 200):
        ##print_gui('Succesful Leaf Trade Web Request')
        resp_json = json.loads(resp.text)
        products_arr = resp_json['results']
        
        product_dict = {}
        for product in products_arr:
                price = product['price']
                product_id = product['product_id']

                for stock in product['stock']:
                    pro_lot = stock['batch_ref']
                    pull_num = stock['pull_number']
                    qty = stock['quantity']

                    if qty > 0:
                        product_dict[pull_num] = [
                            pro_lot,
                            qty,
                            price,
                            product_id,
                            pull_num
                            ]

        df_pros = pd.DataFrame.from_dict(product_dict, orient='index', columns=['Lot', 'Qty - LeafTrade', 'Price', 'product_id', 'pull_num'])
        df_pros.index.name = 'NDC'

        ##output = 'Number of LeafTrade inventory items grabbed: ' + str(len(df_pros.index)) + '\n'
        ##print_gui(output)
        return df_pros
        
def get_lt_active_batches(df_products):
    BASE_URL = 'https://app.leaf.trade/'

    headers = CaseInsensitiveDict()
    headers["Accept"] = "application/json"
    headers["Authorization"] = f'Token {LEAFTRADE_KEY}'
    headers['Accept-Encoding'] = 'gzip, deflate, br'
    headers['scheme'] = 'https'
    headers['User-Agent'] = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/100.0.4896.88 Safari/537.36'

    active_products_dict = {}

    for i, product in df_products.iterrows():
        pro_id = product['product_id']
        LT_VARIANTS_ENDPOINT = f'/api/v3/vendor/product-variants/?status=enabled&product_id={pro_id}'

        url = BASE_URL + LT_VARIANTS_ENDPOINT
        resp = requests.get(url, headers=headers)

        if(resp.status_code == 200):
            resp_json = json.loads(resp.text)
            variant_arr = resp_json['results']

            for pro in variant_arr:
                if pro['status_display'] == 'Visible' and product['pull_num'] == pro['stock'][0]['pull_number']:
                    active_products_dict[i] = [
                        product['Lot'],
                        product['Qty - LeafTrade'],
                        product['Price']
                    ]
        else:
            print_gui('Error retrieving product variants from LeafTrade')
            
    df_active_pros = pd.DataFrame.from_dict(active_products_dict, orient='index', columns=['Lot', 'Qty - LeafTrade', 'Price'])
    df_active_pros.index.name = 'NDC'

    ##output = 'Number of LeafTrade inventory items grabbed: ' + str(len(df_active_pros.index)) + '\n'
    ##print_gui(output)
    return df_active_pros 

def get_EPICOR_skus():
    ###### QUERY - Epicor DB XLOOKUP Table
    conn_str = (
        r'DRIVER={SQL Server};' 
        r'SERVER=EPICOR10;'
        r'DATABASE=ERP10Live;'
        r'Trusted_Connection=yes;'
    )

##    print_gui(f'Attempting to connect to: EPICOR10\n')
    try:
        cnxn = pyodbc.connect(conn_str)
##        print_gui(f'Successful connection to DB.\n')
    except:
##        print_gui(f'Error connecting to DB.\n')
        exit()
##   print_gui(f'Fetching SKUs from Epicor.\n')
    cursor = cnxn.cursor()
    sql = textwrap.dedent("""
    SELECT 
    P.PartNum, P.PartDescription, P.CommercialCategory,
    PL.LotNum, PL.PartLotDescription, PL.Batch, PL.MfgLot AS 'NDC', PLU.Genetics_c AS 'Genetics', PL.ExpirationDate AS 'Exp Date',
    PLU.BrandName_c AS 'BRAND NAME', PG.Description
    FROM ERP.Part AS P
    INNER JOIN ERP.PartLot AS PL ON P.PartNum = PL.PartNum
    INNER JOIN ERP.PartLot_UD AS PLU ON PL.SysRowID = PLU.ForeignSysRowID
    INNER JOIN ERP.ProdGrup AS PG ON P.ProdCode = PG.ProdCode
    WHERE P.InActive = 0 AND P.ClassID = ''
    ORDER BY PG.Description
    """)
    cursor.execute(sql)
    rslt = cursor.fetchall()
    sku_list = [list(elem) for elem in rslt]
    df_sku = pd.DataFrame(sku_list, columns=['Part', 'Desc', 'Type', 'LOT', 'Lot Desc', 'Batch', 'NDC', 'Genetics', 'Exp Date', 'Brand Name - Epicor', 'Sale Group'])
    df_sku.set_index('NDC', inplace=True)
##    print_gui('Epicor SKUs Returned\n')
    return df_sku

def compare_LT_EPICOR(DF_INVENTORY, DF_EPICOR_SKU):
    df_merge = DF_INVENTORY.merge(DF_EPICOR_SKU, how='inner', left_on='NDC', right_on='NDC')
    df_final = df_merge[['Sale Group', 'Type', 'Exp Date', 'Genetics', 'Brand Name - Epicor', 'Part', 'Desc', 'Lot', 'Lot Desc', 'Batch', 'Qty - LeafTrade', 'Price']]
    df_final.sort_values(by=['Sale Group', 'Part'], inplace=True)

    if not os.path.isdir(CURRENT_YEAR_FOLDER):
        os.makedirs(CURRENT_YEAR_FOLDER)
    if not os.path.isdir(CURRENT_MONTH_ORDERS_FOLDER):
        os.makedirs(CURRENT_MONTH_ORDERS_FOLDER)

    writer = pd.ExcelWriter(f'{CURRENT_MONTH_ORDERS_FOLDER}\storefront_{today}.xlsx') 
    df_final.to_excel(writer, sheet_name='Order Form', index=False, na_rep='NaN')

    for column in df_final:
        column_length = max(df_final[column].astype(str).map(len).max(), len(column))
        col_idx = df_final.columns.get_loc(column)
        writer.sheets['Order Form'].set_column(col_idx, col_idx, column_length)

    writer.save() 
##    print_gui(f'Epicor and Leaf Trade Inventory Matched Up. Files saved in {CURRENT_MONTH_ORDERS_FOLDER}\n')

####### GUI FUNCTIONS
def print_gui(whatever):
##   with open('readme.txt', 'a') as f:
##        f.write(whatever)
##        f.close()
    return
    
####### PROGRAM RUN
def fullRun():
##    start = datetime.now()
##    output = 'Program Start: '+ str(start)
##    print_gui(output)
    df_inventory = get_lt_inventory()
    df_storefront = get_lt_active_batches(df_inventory)
    df_epicor_skus = get_EPICOR_skus()
    compare_LT_EPICOR(df_storefront, df_epicor_skus)
##    end = datetime.now()
##   output = '\nProgram End: '+ str(end)
##    print_gui(output)