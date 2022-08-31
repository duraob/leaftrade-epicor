from flask import Flask, render_template, request
from app import app
import lt_epicor, lt_attributes, lt_credits, lt_inventory, lt_order_form
import pandas as pd
import os
from werkzeug.utils import secure_filename

ALLOWED_EXTENSIONS = set(['csv'])

def allowed_file(filename):
    return '.' in filename and \
           filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS
           
def check_transactions():
        if os.path.exists('./transactions_history.csv'):
            if os.stat('./transactions_history.csv').st_size > 0:
                df = pd.read_csv('./transactions_history.csv', index_col=0)
                df['date'] = pd.to_datetime(df['date'])
                
                max_date = df.iloc[df['date'].idxmax()]['date']

                df = df[df['date'] == max_date]
                if not df.empty:
                    orders = df.to_dict('index')
                    return orders

@app.route("/", methods=['GET', 'POST'])
def index():
    alert = request.args.get('alert', '')
    orders = {}
    orders = check_transactions()

    if request.method == 'POST':
        if 'migrate-all-orders' in request.form:
            try:
                lt_epicor.fullRun()
                orders = check_transactions()
                alert = 'Success - LeafTrade Orders Migrated.'
            except:
                alert = 'Error - Could not complete request to migrate all approved orders to Epicor'
        elif 'migrate-all-credits' in request.form:
            try:
                lt_credits.fullRun()
                alert = 'Success - LeafTrade Credits Migrated.'
            except:
                alert = 'Error - Could not complete request to download all approved orders.'
        elif 'migrate-single-order' in request.form:
            orderNo = request.form['orderNum']
            if orderNo:
                try:
                    lt_epicor.oneRun(orderNo)
                    orders = check_transactions()
                    alert = f'Success - LeafTrade {orderNo} Migrated.'
                except:
                    alert = 'Error - Could not complete request for single order.'
        elif 'verify-inventory' in request.form:
            try:
                lt_inventory.fullRun()
                alert = 'Success - LeafTrade Allocations matched to Epicor Inventory in O:\Customer Orders\LeafTrade Files\Inventory.'
            except:
                alert = 'Error - Could not complete request to verify inventory.'
        elif 'pull-order-form' in request.form:
            try:
                lt_order_form.fullRun()
                alert = 'Success - LeafTrade Order Form saved in O:\Order Forms'
            except:
                alert = 'Error - Could not pull down order form.'

    return render_template('index.html', alert=alert, orders=orders)

@app.route("/attributes", methods=['GET', 'POST'])
def attributes():
    alert = request.args.get('alert', None)

    if request.method == 'POST':
        if 'batchDL' in request.form:
            try:
                lt_attributes.fetch_leaftrade_batches()
                alert = 'Success - LeafTrade Batches Retrieved.'
            except:
                alert = 'Error - Could not complete request to download LeafTrade batches.'
        elif 'batchUL' in request.form:
            file = request.files['file']
            if file and allowed_file(file.filename):
                filename = secure_filename(file.filename)
                filePath = os.path.join(app.config['UPLOAD_FOLDER'], filename)
                file.save(filePath)
                try:
                    lt_attributes.patch_batch_list(filePath)
                    alert = 'Success - LeafTrade Batches Updated.'
                except:
                    alert = 'Error - Could not complete request to update LeafTrade batches.'
        elif 'invDL' in request.form:
            try:
                lt_attributes.fetch_leaftrade_inventory()
                alert = 'Success - LeafTrade Inventory Updated.'
            except:
                alert = 'Error - Could not complete request to download LeafTrade inventory.'
        elif 'invUL' in request.form:
            file = request.files['file']
            if file and allowed_file(file.filename):
                filename = secure_filename(file.filename)
                filePath = os.path.join(app.config['UPLOAD_FOLDER'], filename)
                file.save(filePath)
                try:
                    lt_attributes.patch_inv_list(filePath)
                    alert = 'Success - LeafTrade Inventory Updated.'
                except:
                    alert = 'Error - Could not complete request to update LeafTrade inventory.'
                    
    return render_template('attributes.html', alert=alert)