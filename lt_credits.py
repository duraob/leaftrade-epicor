## IMPORTS
import pyodbc
import textwrap
import requests
from requests.structures import CaseInsensitiveDict
import pandas as pd
import json
from datetime import datetime
from app import app
from concurrent.futures import ThreadPoolExecutor

today_dt = str(datetime.now()).replace(' ', 'T')
today = datetime.now().strftime("%m-%d-%y")
month = datetime.now().month
month_name = datetime.now().strftime('%B')
year = datetime.now().year

ROOT_CUSTOMER_ORDERS_FOLDER = r'\\svpo0fs01\Departmental\Customer Orders\LeafTrade Files\Credits'

EPI_USER=app.config.get('EPICOR_USER')
EPI_PWD=app.config.get('EPICOR_PWD')

LEAFTRADE_KEY = app.config.get('LEAFTRADE_KEY')

######### get issue(s) from leaf trade
##  only issues marked as APPROVED: /api/v3/vendor/orders/?status=approved&include_order_items=true
def get_lt_credits(df_rmas):
    BASE_URL = 'https://app.leaf.trade/'
    STATUS = 'approved' # 'approved' 'redeemed'
    LT_CREDITS_ENDPOINT = f'/api/v3/vendor/issues/'

    headers = CaseInsensitiveDict()
    headers["Accept"] = "application/json"
    headers["Authorization"] = f'Token {LEAFTRADE_KEY}'
    headers['Accept-Encoding'] = 'gzip, deflate, br'
    headers['scheme'] = 'https'
    headers['User-Agent'] = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/100.0.4896.88 Safari/537.36'

    url = BASE_URL + LT_CREDITS_ENDPOINT
    resp = requests.get(url, headers=headers)

    if(resp.status_code == 200):
        print_gui('Succesful Leaf Trade Web Request')
        resp_json = json.loads(resp.text)
        credits_arr = resp_json['results']
        
        credit_dict = {}
        for credit in credits_arr:
            status = credit['status']
            if status == STATUS: # 'redeemed' 'approved'
                credit_id = credit['id']
                if not (str(credit_id) in df_rmas['credit_id'].values):
                    product_variant_id = credit['product_variant']['id']
                    dispensary = credit['dispensary_location']['name']
                    lic = credit['dispensary_location']['license_number']
                    credit_qty = credit['quantity']
                    approved_unit_price = credit['approved_unit_price']
                    reason = credit['reason']
                    notes = credit['notes']
                    credit_date_created = credit['date_created']

                    credit_dict[credit_id] = [
                        product_variant_id,
                        dispensary,
                        lic,
                        credit_qty,
                        approved_unit_price,
                        reason,
                        notes,
                        status,
                        credit_date_created
                        ]

        df_credits = pd.DataFrame.from_dict(credit_dict, orient='index', columns=['product_variant_id', 'dispensary', 'lic', 'credit_qty', 'approved_unit_price', 'reason', 'notes', 'status', 'credit_date_created'])
        df_credits.index.name = 'credit_id'

        output = 'Number of approved LeafTrade credits grabbed: ' + str(len(df_credits.index)) + '\n'
        print_gui(output)

        if len(df_credits.index) == 0:
            exit()
        
        return df_credits

def get_product_variants(df_credits):
    BASE_URL = 'https://app.leaf.trade/'

    headers = CaseInsensitiveDict()
    headers["Accept"] = "application/json"
    headers["Authorization"] = f'Token {LEAFTRADE_KEY}'
    headers['Accept-Encoding'] = 'gzip, deflate, br'
    headers['scheme'] = 'https'
    headers['User-Agent'] = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/100.0.4896.88 Safari/537.36'

    variant_dict = {}
    
    for variant in enumerate(df_credits.values):
        v_id = variant[1][0]
        LT_VARIANTS_ENDPOINT = f'/api/v3/vendor/product-variants/{v_id}/'
        url = BASE_URL + LT_VARIANTS_ENDPOINT
        
        try:
            resp = requests.get(url, headers=headers)
        except:
            print_gui(f'{resp.status_code} - Error requesting LeafTrade Credit for Variant {v_id}.', '\n')

        if(resp.status_code == 200):
            print_gui(f'Successful Leaf Trade Web Request - Product Variant {v_id}')
            resp_json = json.loads(resp.text)
  
            variant_id = resp_json['id']
            variant_ndc = resp_json['stock'][0]['pull_number']
            
            variant_dict[variant_id] = [
                variant_ndc
                ]

    df_variant = pd.DataFrame.from_dict(variant_dict, orient='index', columns=['variant_ndc'])
    df_variant.index.name = 'variant_id'

    if len(df_variant.index) == 0:
        print_gui('\nERROR: No LeafTrade Variants Retrieved.\n')
        exit()

    df_credits['NDC'] = df_credits.product_variant_id.map(df_variant.variant_ndc)
    df_credits.to_csv(f'{ROOT_CUSTOMER_ORDERS_FOLDER}\\{today}_LeafTradeInfo.csv')
    print_gui(f'Saved to {ROOT_CUSTOMER_ORDERS_FOLDER}\\{today}_LeafTradeInfo.csv')

    return df_credits

def get_EPICOR_skus():
    ###### QUERY - Epicor DB XLOOKUP Table
    conn_str = (
        r'DRIVER={SQL Server};' 
        r'SERVER=EPICOR10;'
        r'DATABASE=ERP10Live;'
        r'Trusted_Connection=yes;'
    )

    print_gui(f'Attempting to connect to: EPICOR10\n')
    try:
        cnxn = pyodbc.connect(conn_str)
        print_gui(f'Successful connection to DB.\n')
    except:
        print_gui(f'Error connecting to DB.\n')
        exit()
    print_gui(f'Fetching SKUs from Epicor.\n')
    cursor = cnxn.cursor()
    sql = textwrap.dedent("""
    SELECT 
    P.PartNum, P.PartDescription,
    PL.LotNum, PL.PartLotDescription, PL.Batch, PL.MfgLot AS 'NDC'
    FROM ERP.Part AS P
    INNER JOIN ERP.PartLot AS PL ON P.PartNum = PL.PartNum
    INNER JOIN ERP.PartLot_UD AS PLU ON PL.SysRowID = PLU.ForeignSysRowID
    WHERE P.InActive = 0 AND P.ClassID = ''
    """)
    cursor.execute(sql)
    rslt = cursor.fetchall()
    sku_list = [list(elem) for elem in rslt]
    df_sku = pd.DataFrame(sku_list, columns=['PRODUCT NAME', 'PartDesc', 'LOT #', 'LotDesc', 'Batch', 'NDC'])
    df_sku.set_index('NDC', inplace=True)
    print_gui('Epicor SKUs Returned\n')
    return df_sku

## GET CUSTOMER TABLE AND DO A LOOK UP FOR CUST NUM AND CUST ID
def get_EPICOR_cust():
    ###### QUERY - Epicor DB XLOOKUP Table
    conn_str = (
        r'DRIVER={SQL Server};' 
        r'SERVER=EPICOR10;'
        r'DATABASE=ERP10Live;'
        r'Trusted_Connection=yes;'
    )
    print_gui(f'Attempting to connect to: EPICOR10\n')
    try:
        cnxn = pyodbc.connect(conn_str)
        print_gui(f'Successful connection to DB.\n')
    except:
        print_gui(f'Error connecting to DB.\n')
        exit()

    print_gui(f'Fetching Customers from Epicor.\n')
    cursor = cnxn.cursor()
    sql = textwrap.dedent("""
    SELECT C.CustID, C.CustNum, C.Name, C.ResaleID
    FROM ERP.Customer AS C
    """)
    cursor.execute(sql)
    rslt = cursor.fetchall()
    cust_list = [list(elem) for elem in rslt]
    df_cust = pd.DataFrame(cust_list, columns=['CustID', 'CustNum', 'CustName', 'Lic'])
    print_gui('Epicor Customer Information Returned\n')
    return df_cust

def get_epicor_rmas():
    ###### QUERY - Epicor DB XLOOKUP Table
    conn_str = (
        r'DRIVER={SQL Server};' 
        r'SERVER=EPICOR10;'
        r'DATABASE=ERP10Live;'
        r'Trusted_Connection=yes;'
    )

    print_gui(f'Attempting to connect to: EPICOR10\n')
    try:
        cnxn = pyodbc.connect(conn_str)
        print_gui(f'Successful connection to DB.\n')
    except:
        print_gui(f'Error connecting to DB.\n')
        exit()
    print_gui(f'Fetching RMAs from Epicor.\n')
    cursor = cnxn.cursor()
    sql = textwrap.dedent("""
    SELECT RMANum, ECCComment as 'credit_id'
    FROM ERP.RMADtl AS RMAD
    WHERE ECCComment != ''
    """)
    cursor.execute(sql)
    rslt = cursor.fetchall()
    rma_list = [list(elem) for elem in rslt]
    df_rma = pd.DataFrame(rma_list, columns=['RMANum', 'credit_id'])
    print_gui('Epicor RMAs Returned\n')
    return df_rma

def compare_LT_EPICOR(DF_LT_CREDITS, DF_EPICOR_SKU, DF_CUSTOMERS):
    unique_customers = DF_LT_CREDITS.lic.unique()
    rma_arr = []

    for cust in unique_customers:
        df = DF_LT_CREDITS.loc[DF_LT_CREDITS['lic'] == cust]
        df.reset_index(inplace=True)
        df_merge = df.merge(DF_EPICOR_SKU, how='inner', left_on='NDC', right_on='NDC') 
        df_final = df_merge[['PRODUCT NAME', 'PartDesc', 'LOT #', 'LotDesc', 'Batch', 'NDC', 'credit_id', 'dispensary', 'lic', 'credit_qty', 'approved_unit_price', 'reason', 'notes', ]]
        df_final.set_index('credit_id', inplace=True)
        disp = df_final.iloc[0]['dispensary']
        lic = df_final.iloc[0]['lic']
        df_final['CustID'] = df_final.lic.map(DF_CUSTOMERS.set_index('Lic').CustID)
        df_final['CustNum'] = df_final.lic.map(DF_CUSTOMERS.set_index('Lic').CustNum)
        loc_ID = df_final.iloc[0]['CustID']
        df_final.to_csv(f'{ROOT_CUSTOMER_ORDERS_FOLDER}\\{today} CTPharma Credit Form -{disp}_{loc_ID}.csv')
        rma_arr.append(df_final)

    print_gui(f'Epicor and Leaf Trade Credits Matched Up. Files saved in {ROOT_CUSTOMER_ORDERS_FOLDER}\n')
    return rma_arr

def create_epicor_rma(custNum):
    EPI_REST_RMA_ENDPOINT = 'https://epicor10/ERP10Live/api/v1/Erp.BO.RMAProcSvc/RMAProcs'

    headers = CaseInsensitiveDict()
    headers["Accept"] = "application/json"
    headers['Content-Type'] = 'application/json'

    data = {}
    data['OpenRMA'] = True
    data['Company'] = 'CP'
    data['RMANum'] = 0
    data['CustNum'] = int(custNum)
    data['BTCustNum'] = int(custNum)
    data['RMADate'] = today_dt

    resp = requests.post(EPI_REST_RMA_ENDPOINT, json=data, headers=headers, verify=False, auth=(EPI_USER,EPI_PWD)) #'O:\ERP\Projects\BDurao\LeafTrade APi\E10REST.cer'

    if(resp.status_code == 201):
        resp_json = json.loads(resp.text)
        rmaNum = resp_json['RMANum']
        print_gui(f'\nSuccessful EPICOR REST Request - RMA {rmaNum} Created')
        return rmaNum
        

def create_epicor_rma_detail(rmaNum, rmaLine, part, partDesc, unitPrice, qty, lotNum, notes, reason, credit_id):
    EPI_REST_RMADTL_ENDPOINT = 'https://epicor10/ERP10Live/api/v1/Erp.BO.RMAProcSvc/RMADtls'

    headers = CaseInsensitiveDict()
    headers["Accept"] = "application/json"
    headers['Content-Type'] = 'application/json'

    data = {}
    data['Company'] = 'CP'
    data['RMANum'] = rmaNum ## get from order creation
    data['RMALine'] = int(rmaLine) ## increment
    data['PartNum'] = part ## get from order line
    data['LineDesc'] = partDesc
    data['ReturnQty'] = str(qty) ## get from order line
    data['ReturnQtyUOM'] = 'EA'
    data['LotNum_c'] = str(lotNum)
    data['Note'] = notes
    data['ECCComment'] = str(credit_id)


    if reason == 'customer mistake':
        data['ReturnReasonCode'] = 'CUSTMIS'
    elif reason == 'defective':
        data['ReturnReasonCode'] = 'DEFECTIV'
    elif reason == 'wrong product':
        data['ReturnReasonCode'] = 'DELERR'
    elif reason == 'dispensary mistake':
        data['ReturnReasonCode'] = 'DISPMIS'
    elif reason == 'incorrect quantity':
        data['ReturnReasonCode'] = 'INCORAMT'
    elif reason == 'manufacturer mistake':
        data['ReturnReasonCode'] = 'MANFMIS'
    elif reason == 'other':
        data['ReturnReasonCode'] = 'OTHER'
    elif reason == 'patient comp':
        data['ReturnReasonCode'] = 'PATCOMP'
    elif reason == 'promotional product':
        data['ReturnReasonCode'] = 'PROMO'
    elif reason == 'defective':
        data['ReturnReasonCode'] = 'QUALITY'
    else:
        data['ReturnReasonCode'] = 'CC'
        
    
    resp = requests.post(EPI_REST_RMADTL_ENDPOINT, json=data, headers=headers, verify=False, auth=(EPI_USER,EPI_PWD)) #'O:\ERP\Projects\BDurao\LeafTrade APi\E10REST.cer'
    if(resp.status_code == 201):
        print_gui(f'Successful EPICOR REST Request - RMA Line {rmaLine} Created')

def create_epicor_rma_detail_credit(rmaNum, rmaLine):
    EPI_REST_RMADTLCREDIT_ENDPOINT = 'https://epicor10/ERP10Live/api/v1/Erp.BO.RMAProcSvc/RMACreditAdd'

    headers = CaseInsensitiveDict()
    headers["Accept"] = "application/json"
    headers['Content-Type'] = 'application/json'

    data = {}
    data['iRMANum'] = rmaNum
    data['iRMALine'] = rmaLine
    data['iCorrection'] = False
    data['ds'] = {
            "RMAHead": [],
            "RMAHeadAttch": [],
            "RMADtl": [],
            "RMADtlAttch": [],
            "InvcDtl": [],
            "RMARcpt": [],
            "LegalNumGenOpts": [],
            "SelectedSerialNumbers": [],
            "SerialNumberSearch": [],
            "SNFormat": []
        }
   
    
    resp = requests.post(EPI_REST_RMADTLCREDIT_ENDPOINT, json=data, headers=headers, verify=False, auth=(EPI_USER,EPI_PWD))
    if(resp.status_code == 200):
        resp_json = json.loads(resp.text)
        invoice_num = resp_json['parameters']['iInvoiceNum']
        invoice_line = resp_json['parameters']['iInvoiceLine']
        print_gui(f'Successful EPICOR REST Request - Invoice {invoice_num} Ln {invoice_line} Created')

        return [invoice_num, invoice_line]
        

def upcate_epicor_rma_detail_credit(invoiceNum, invoiceLn, unitPrice, custNum, qty, lot):
    EPI_REST_ARINVOICE_ENDPOINT = f'https://epicor10/ERP10Live/api/v1/Erp.BO.ARInvoiceSvc/InvcDtls(CP,{invoiceNum},{invoiceLn})'

    headers = CaseInsensitiveDict()
    headers["Accept"] = "application/json"
    headers['Content-Type'] = 'application/json'

    data = {}
    data['Company'] = "CP"
    data['InvoiceNum'] = int(invoiceNum)
    data['InvoiceLine'] = int(invoiceLn)
    data['UnitPrice'] = str(unitPrice)
    data['CustNum'] = int(custNum)
    data['SellingShipQty'] = str(qty)
    data['LotNum'] = str(lot)
    
    resp = requests.patch(EPI_REST_ARINVOICE_ENDPOINT, json=data, headers=headers, verify=False, auth=(EPI_USER,EPI_PWD)) 
    if(resp.status_code == 204):
        print_gui(f'Successful EPICOR REST Request - Invoice {invoiceNum} Ln {invoiceLn} Updated')

def process_epicor_rmas(rma):
    ##dict_returns = {}
    custNum = rma.iloc[0]['CustNum']
    ##disp = rma.iloc[0]['dispensary']
    ##loc = rma.iloc[0]['CustID']
    rmaNum = create_epicor_rma(custNum)
    print_gui(f'Epicor RMA {rmaNum} created for LT Order.')
    rmaLine = 1
    for i, ln in rma.iterrows():
        credit_id = i
        part = ln['PRODUCT NAME']
        unitPrice = ln['approved_unit_price']
        qty = ln['credit_qty']
        lineDesc = ln['PartDesc']
        lotNum = ln['LOT #']
        notes = ln['notes']
        reason = ln['reason']
        create_epicor_rma_detail(rmaNum, rmaLine, part, lineDesc, unitPrice, qty, lotNum, notes, reason, credit_id)
        print_gui(f'RMA {rmaNum} Ln {rmaLine} done.')
        invoice_detail_arr = create_epicor_rma_detail_credit(rmaNum, rmaLine)
        invoice_num = str(invoice_detail_arr[0])
        invoice_ln = str(invoice_detail_arr[1])
        print_gui(f'RMA Invoice {invoice_num} Ln {invoice_ln} created.')
        upcate_epicor_rma_detail_credit(invoice_num, invoice_ln, unitPrice, custNum, qty, lotNum)
        print_gui(f'RMA Invoice {invoice_num} Ln {invoice_ln} details updated.')
        rmaLine += 1
    ##dict_returns[count] = [credit_id, str(rmaNum), rmaLine, disp, loc, part, lineDesc, qty, unitPrice, lotNum, notes, reason, today]

    ##df = pd.DataFrame.from_dict(dict_returns, orient='index', columns=['credit_id', 'rmaNum', 'rmaLine', 'disp', 'loc', 'part', 'lineDesc', 'qty', 'unitPrice', 'lotNum', 'notes', 'reason', 'date'])
    ##df.to_csv('./transaction_creds.csv')
    #df_history = pd.read_csv('./transaction_creds_history.csv')
    #df_history = pd.concat([df_history, df], ignore_index=True)
    #df_history.to_csv('./transaction_creds_history.csv')
    ##return dict_returns
            
####### GUI FUNCTIONS
def print_gui(whatever):
##    with open('readme.txt', 'a') as f:
##        f.write(whatever)
##        f.close()
##    print(whatever)
    return
    

####### PROGRAM RUN
def fullRun():
    start = datetime.now()
    output = 'Program Start: '+ str(start)
    print(output)

    df_rmas = get_epicor_rmas()
    df_lt_credits = get_lt_credits(df_rmas)
    df_lt_product_variants = get_product_variants(df_lt_credits)
    df_epicor_skus = get_EPICOR_skus()
    df_epicor_customers = get_EPICOR_cust()
    parsed_credits_arr = compare_LT_EPICOR(df_lt_product_variants, df_epicor_skus, df_epicor_customers)
    with ThreadPoolExecutor(max_workers=72) as executor:
        results = executor.map(
                process_epicor_rmas,
                parsed_credits_arr
        )
    
    
    end = datetime.now()
    output = '\nProgram End: '+ str(end)
    print(output)
##    return dict_orders