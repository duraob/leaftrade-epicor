## IMPORTS
import pyodbc
import textwrap
import requests
from requests.structures import CaseInsensitiveDict
import pandas as pd
import json
from datetime import datetime
import os
import numpy as np
from app import app

today_dt = str(datetime.now()).replace(' ', 'T')
today = datetime.now().strftime("%m-%d-%y")
month = datetime.now().month
month_name = datetime.now().strftime('%B')
year = datetime.now().year

ROOT_CUSTOMER_ORDERS_FOLDER = r'\\svpo0fs01\Departmental\Customer Orders\LeafTrade Files\Inventory'

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
        print('Succesful Leaf Trade Web Request')
        resp_json = json.loads(resp.text)
        products_arr = resp_json['results']
        
        product_dict = {}
        for product in products_arr:
                pro_id = product['id']
                pro_name = product['name']
                price = product['price']

                for stock in product['stock']:
                    pro_lot = stock['batch_ref']
                    pull_num = stock['pull_number']
                    qty = stock['quantity']
                    qty_alloc = stock['quantity_allocated']

                    if qty > 0 or qty_alloc > 0:
                        product_dict[pull_num] = [
                            pro_id,
                            pro_name,
                            pro_lot,
                            qty,
                            qty_alloc,
                            price
                            ]

        df_pros = pd.DataFrame.from_dict(product_dict, orient='index', columns=['product_id', 'Brand Name', 'Lot', 'Qty - LeafTrade', 'Qty Allocated - LeafTrade', 'Price'])
        df_pros.index.name = 'NDC'

        output = 'Number of LeafTrade inventory items grabbed: ' + str(len(df_pros.index)) + '\n'
        print(output)
        return df_pros

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
##    print_gui(f'Fetching SKUs from Epicor.\n')
    cursor = cnxn.cursor()
    sql = textwrap.dedent("""
    SELECT 
    P.PartNum, P.PartDescription,
    PL.LotNum, PL.PartLotDescription, PL.Batch, PL.MfgLot AS 'NDC',
    PLU.BrandName_c AS 'BRAND NAME', SUM(PB.OnhandQty) AS 'QOH', PG.Description
    FROM ERP.Part AS P
    INNER JOIN ERP.PartLot AS PL ON P.PartNum = PL.PartNum
    INNER JOIN ERP.PartLot_UD AS PLU ON PL.SysRowID = PLU.ForeignSysRowID
    INNER JOIN ERP.PartBin AS PB ON PB.PartNum = P.PartNum AND PB.LotNum = PL.LotNum
    INNER JOIN ERP.WhseBin AS WB ON WB.BinNum = PB.BinNum AND WB.WarehouseCode = PB.WarehouseCode
    INNER JOIN ERP.ProdGrup AS PG ON P.ProdCode = PG.ProdCode
    WHERE P.InActive = 0 AND P.ClassID = '' AND PB.WarehouseCode = 'VAULT' AND WB.NonNettable = 0 AND NOT (WB.BinNum IN ('RHWIP', 'WIP'))
	GROUP BY P.PartNum, P.PartDescription, PL.LotNum, PL.PartLotDescription, PL.Batch, PL.MfgLot, PLU.BrandName_c, PG.Description
	ORDER BY PG.Description
    """)
    cursor.execute(sql)
    rslt = cursor.fetchall()
    sku_list = [list(elem) for elem in rslt]
    df_sku = pd.DataFrame(sku_list, columns=['Epicor Part', 'Epicor Part Desc', 'LOT', 'Lot Desc', 'Batch', 'NDC', 'Brand Name - Epicor', 'Qty - Epicor', 'Sale Group - Epicor'])
    df_sku.set_index('NDC', inplace=True)
##    print_gui('Epicor SKUs Returned\n')
    return df_sku

def compare_LT_EPICOR(DF_INVENTORY, DF_EPICOR_SKU):
    df_merge = DF_INVENTORY.merge(DF_EPICOR_SKU, how='left', left_on='NDC', right_on='NDC')
    df_final = df_merge[['product_id', 'Brand Name', 'Lot', 'Qty - LeafTrade', 'Qty Allocated - LeafTrade', 'Epicor Part', 'Epicor Part Desc', 'LOT', 'Lot Desc', 'Batch', 'Price', 'Brand Name - Epicor', 'Qty - Epicor', 'Sale Group - Epicor']]
    df_final.replace(np.NaN, 0, inplace=True)
    df_final = df_final.astype({'Qty - LeafTrade': 'int', 'Qty Allocated - LeafTrade' : 'int', 'Qty - Epicor' : 'int'})
    df_final['Ending Balance'] = df_final['Qty - Epicor'] - df_final['Qty Allocated - LeafTrade']
    df_final.sort_values(by=['Qty Allocated - LeafTrade', 'Sale Group - Epicor'], ascending=False, inplace=True)

    writer = pd.ExcelWriter(f'{ROOT_CUSTOMER_ORDERS_FOLDER}\inventory_{today}.xlsx') 
    df_final.to_excel(writer, sheet_name='Allocations Form', index=False, na_rep='NaN')

    for column in df_final:
        column_length = max(df_final[column].astype(str).map(len).max(), len(column))
        col_idx = df_final.columns.get_loc(column)
        writer.sheets['Allocations Form'].set_column(col_idx, col_idx, column_length)

    writer.save()    

    print_gui(f'Epicor and Leaf Trade Inventory Matched Up. Files saved in {ROOT_CUSTOMER_ORDERS_FOLDER}\n')

####### GUI FUNCTIONS
def print_gui(whatever):
##   with open('readme.txt', 'a') as f:
##       f.write(whatever)
##        f.close()
    return
    
####### PROGRAM RUN
def fullRun():
    start = datetime.now()
    output = 'Program Start: '+ str(start)
    print_gui(output + '\n')
    print_gui(f'env= {os.environ}\n')
    print_gui(f'app.config= {app.config}')
    df_inventory = get_lt_inventory()
    df_epicor_skus = get_EPICOR_skus()
    compare_LT_EPICOR(df_inventory, df_epicor_skus)
    end = datetime.now()
    output = '\nProgram End: '+ str(end)
    print_gui(output)