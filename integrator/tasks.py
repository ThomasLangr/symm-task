import json
import time
import hashlib
import requests

from .eshop_api_con import API_URL, API_KEY, headers, RATE_LIMIT, REQUEST_DELAY
from unittest.mock import patch, Mock
from collections import defaultdict
from celery import shared_task
from django.conf import settings
from .models import ProductSync, DataQualityLog

def get_erp_data(file_name):
    with open(f"{file_name}.json") as f:
        data = json.load(f)
    return data

def validate_items(data):
    valid_data = []
    invalid_data = {}

    for item in data:
        sku_id = item.get('id')
        try:
            error_message = ''
            
            if not isinstance(item, dict):
                error_message += "Item is not a dict. "

            if 'id' not in item or 'title' not in item:
                error_message += "Missing id or title. "
        
            if (not isinstance(item.get('price_vat_excl'), (int, float)) or not item.get('price_vat_excl') > 0):
                error_message += f"Price is set to {item.get('price_vat_excl')}. "
    
            if not isinstance(item.get("stocks"), dict):
                error_message += "Stocks value is not a dict. "
   
            if not all(isinstance(stock, int) and stock >= 0 for stock in item.get("stocks").values()):
                error_message += "Stocks count value is ivalid. "
    
            if error_message != '':
                raise ValueError("JSON is invalid.")
            else:
                valid_data.append(item)
            
        except Exception:
            item['error_message'] = error_message
            invalid_data[sku_id] = item
           

    return valid_data, invalid_data


def transform_erp_data(data):
    transformed_data = {}
    
    for item in data:
        sku_id = item['id']
    
        if sku_id not in transformed_data:
            transformed_data[sku_id] = {
                'id': sku_id,
                'title': item['title'],
                #'price_vat_excl': 0,
                'price_vat': 0,
                'stocks': defaultdict(int),
                'attributes': {
                    'color': (item.get("attributes") or {}).get("color") or "N/A"
                }
            }
    
        # Aggregate price if valid
        # Potential error: What if there are 2 different prices for one SKU? (code will take the last one)
        price = item.get('price_vat_excl')
        if price is not None and price > 0:
            transformed_data[sku_id]['price_vat'] = price * 1.21 
        elif price is None or price == 0:
            transformed_data[sku_id]['price_vat'] = None
            
    
        # Aggregate stocks for each location
        stocks = item.get('stocks', {})
        for location, qty in stocks.items():
            if isinstance(qty, (int, float)):
                transformed_data[sku_id]['stocks'][location] += qty


    for sku in transformed_data.values():
        sku['stocks'] = dict(sku['stocks'])
    
    return transformed_data

def get_hash(data):
    hashes = {}
    for sku_id, item in data.items():
        json_data = json.dumps(item, sort_keys=True)
        hashes[sku_id] = {'data_hash':hashlib.sha256(json_data.encode("utf-8")).hexdigest()}
    return hashes

def send_request(method, url, headers, payload):
    for _ in range(100):
        response = requests.request(
            method,
            url,
            json=payload,
            headers=headers)
        if response.status_code == 429:
            print("Unable to send request. Sleep for 1 second and retry.")
            time.sleep(1)
            continue
        return response


def get_mock(method):
    
    if method == 'POST':
        mock = Mock()
        mock.status_code = 201
        mock.json.return_value = {"result": f"success"}
        mock.raise_for_status.return_value = None
        
    elif method == 'PATCH':
        mock = Mock()
        mock.status_code = 200
        mock.json.return_value = {"result": f"success"}
        mock.raise_for_status.return_value = None
        
    elif method == 'FAIL':
        mock = Mock()
        mock.status_code = 429
        mock.raise_for_status.side_effect = requests.HTTPError("429 Too Many Requests")
        
    return mock
        
    
@shared_task
def sync_products(file_name):
    valid_data, invalid_data = validate_items(get_erp_data(file_name))
    transformed = transform_erp_data(valid_data)
    transformed_hash = get_hash(transformed)
    invalid_data_hash = get_hash(invalid_data)

    for id_sku in invalid_data:
        product_hash = invalid_data_hash[id_sku].get('data_hash')
        db_dq, created = DataQualityLog.objects.get_or_create(
                sku = id_sku,
                defaults = {"data_hash": product_hash})
        db_dq.data_hash = product_hash
        db_dq.data_dict = invalid_data[id_sku]
        db_dq.error_message = invalid_data[id_sku].get('error_message')
        db_dq.save()
    
    fail_count = 0
    for id_sku in transformed:        
        product_dict = transformed[id_sku]
        product_hash = transformed_hash[id_sku].get('data_hash')        
        db_obj, created = ProductSync.objects.get_or_create(
                sku = id_sku,
                defaults = {"data_hash": product_hash})
                
        if not created and db_obj.data_hash == product_hash:
            print(id_sku, " was not updated.")
            continue
            
        

        if created:
            method = "POST"
            url = f"{API_URL}/products/"
        else:
            method = "PATCH"
            url = f"{API_URL}/products/{id_sku}/"
                
        fail_count += 1    
        if fail_count.isin([2, 5, 6, 7]):
            mock_list = [get_mock('FAIL'), get_mock(method)]
        elif fail_count == 4:
            mock_list = [get_mock('FAIL'),get_mock('FAIL'),get_mock('FAIL'), get_mock(method)]
        else:
            mock_list = [get_mock(method)]
             
        with patch("requests.request", side_effect = mock_list):
            response = send_request(method, url, headers, product_dict)
        
        if response.status_code == 201 or response.status_code == 200:
            if response.status_code == 201:
                print(id_sku, " was created.")
            elif response.status_code == 200:
                print(id_sku, " was updated.")
            db_obj.data_hash = product_hash
            db_obj.data_dict = product_dict
            db_obj.save()

        time.sleep(REQUEST_DELAY)
    return "Sync_product DONE."