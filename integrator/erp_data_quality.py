import json
from collections import defaultdict

def validate_items(data):
    # Function that splits erp_data into two sets:
    # 1. valid_data - this dataset is send to eshop API
    # 2. invalid_data - this dataset is not send to eshop API, quality errors are uploaded to db
    # List of checks:
    # 1. item is not dict
    # 2. id or title is not in item
    # 3. price_vat_excl is None or price_vat_excl <= 0
    # 4. stocks in item is not dict
    # 5. stocks counts are invalid values
    
    valid_data = []
    invalid_data = {}
    invalid_id_count = 0 
    
    for item in data:
        try:
            error_message = ''
            
            if not isinstance(item, dict):
                error_message += "Item is not a dict. "

            if 'id' not in item or 'title' not in item:
                error_message += "Missing id or title. "
                sku_id = f'invalid_sku_{invalid_id_count}'
                invalid_id_count += 1
            else: 
                sku_id = item.get('id')
        
            if (not isinstance(item.get('price_vat_excl'), (int, float)) or not item.get('price_vat_excl') > 0):
                error_message += f"Price is set to {item.get('price_vat_excl')}. "
    
            if not isinstance(item.get("stocks"), dict):
                error_message += "Stocks value is not a dict. "
   
            if not all(isinstance(stock, int) and stock >= 0 for stock in item.get("stocks").values()):
                error_message += "Stocks count value is invalid. "
    
            if error_message != '':
                raise ValueError("JSON is invalid.")
            else:
                valid_data.append(item)
            
        except Exception:
            item['error_message'] = error_message
            invalid_data[sku_id + '|invalid'] = item
           

    return valid_data, invalid_data

def consistent_items(data):
    # returns sku_id with inconsistent values of title, price_vat_excl and attributes
    grouped = defaultdict(list)

    # group records by id
    for item in data:
        grouped[item["id"]].append(item)

    valid_data = []
    inconsistencies = {}

    for sku_id, items in grouped.items():
        error_message = ''
        
        if len(items) == 1:
            valid_data.extend(items)
            continue  # nothing to compare

        titles = {item.get("title") for item in items}
        prices = {item.get("price_vat_excl") for item in items}
        attrs = {str(item.get("attributes")) for item in items}  # stringify for comparison


        if len(titles) > 1:
            error_message += f'Inconsistent titles: {list(titles)}. '           

        if len(prices) > 1:
            error_message += f'Inconsistent prices: {list(prices)}. '

        if len(attrs) > 1:
            error_message += f'Inconsistent attributes: {list(attrs)}. '
      
        if error_message != '':
            inconsistent_item = {
                'id': sku_id,
                'title': list(titles),
                'price_vat_excl': list(prices),
                'stocks': {},
                'attributes': list(attrs),
                'error_message': error_message
            }
            inconsistencies[sku_id + '|consistency'] = inconsistent_item
        
        else:
            for item in items:
                valid_data.append(item)

    return valid_data, inconsistencies