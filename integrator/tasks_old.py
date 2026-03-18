import json
import time
import hashlib
import requests

from unittest.mock import patch, Mock
from collections import defaultdict
from celery import shared_task
from django.conf import settings
from .models import ProductSync, DataQualityLog
from .eshop_api_con import API_URL, headers, RATE_LIMIT, REQUEST_DELAY
from .erp_data_quality import validate_items, consistent_items

def get_erp_data(file_name):
    try:
        with open(f"{file_name}.json") as f:
            data = json.load(f)
            return data
    except FileNotFoundError:
        raise Exception("ERP file not found")



def transform_erp_data(data):
    
    transformed_data = {}
    
    for item in data:
        print(item)
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
        
        # Add VAT to price_vat_excl and save it variable price_vat
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


@shared_task(bind=True, rate_limit=f'1/s', autoretry_for=(requests.exceptions.Timeout,),
             retry_backoff=True, retry_kwargs={'max_retries': 10})
def send_request(self, method, url, headers, product_json):
    # Function sends request POST/PATCH, if it fails with code 429, repeat until succsess or max retries
    
    response = requests.request(
            method,
            url,
            json=product_json,
            headers=headers,
            timeout=5)
    
    if response.status_code == 429:
        raise self.retry(countdown=1)
      
    if response.status_code in (200, 201):
        ProductSync.objects.update_or_create(
            sku=sku_id,
            defaults={
                'data_dict': product_json,
                'data_hash': hashlib.sha256(json.dumps(product_json, sort_keys=True).encode()).hexdigest()
            }
        )
    
    if response.status_code == 201:
        print(id_sku, " was created.")
    elif response.status_code == 200:
        print(id_sku, " was updated.")
    
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
    # Data quality check
    valid_data, invalid_data = validate_items(get_erp_data(file_name)) 
    valid_data, inconsistencies = consistent_items(valid_data)
    dqf_input = invalid_data | inconsistencies
    # Validated data transformation
    transformed = transform_erp_data(valid_data)
    # Hash valid data
    transformed_hash = get_hash(transformed)
    # Hash invalid data
    dqf_input_hash = get_hash(dqf_input)
    print(dqf_input)
    # Save information about invalid and inconsistent items to db
    for id_sku in dqf_input:
        product_hash = dqf_input_hash[id_sku].get('data_hash')
        db_dq, created = DataQualityLog.objects.get_or_create(
                sku = id_sku,
                defaults = {"data_hash": product_hash})
        db_dq.data_hash = product_hash
        db_dq.data_dict = dqf_input[id_sku]
        db_dq.error_message = dqf_input[id_sku].get('error_message')
        db_dq.save()
    
    # POST/PATCH valid items to ESHOP API
    fail_count = 0 # for 429 response testing
    for id_sku in transformed:        
        product_dict = transformed[id_sku]
        product_hash = transformed_hash[id_sku].get('data_hash')        
    
        # Check if id_sku already exists
        db_obj, created = ProductSync.objects.get_or_create(
                sku = id_sku,
                defaults = {"data_hash": product_hash})
        
        # If item exists and it is the same, skip to next item.
        if not created and db_obj.data_hash == product_hash:
            print(id_sku, " was not updated.")
            continue
            
        
        # If it is not created => POST
        if created:
            method = "POST"
            url = f"{API_URL}/products/"
        # If it is created => PATCH
        else:
            method = "PATCH"
            url = f"{API_URL}/products/{id_sku}/"
        
        # Block for Mocking API calls 
        fail_count += 1    
        if fail_count in [2, 5, 6, 7]:
            mock_list = [get_mock('FAIL'), get_mock(method)]
        elif fail_count == 4:
            mock_list = [get_mock('FAIL'),get_mock('FAIL'),get_mock('FAIL'), get_mock(method)]
        else:
            mock_list = [get_mock(method)]
        
        # Send requests
        with patch("requests.request", side_effect = mock_list):
            response = send_request.delay(method, url, headers, product_dict)
    

    return "Sync_product DONE."
