def extract_variation_data(countryCode, vehicle_type):
    return [
    {
        '$match': {
            'countryCode': countryCode, 
            'published': True,
            "product.categoryCode": f"{countryCode}-{vehicle_type}",
            '_forwardingCode': None
        }
    }, {
        '$unwind': '$variations'
    }, {
        '$group': {
            '_id': {
                'countryCode': '$countryCode', 
                'categoryCode': '$product.categoryCode', 
                'code': '$code', 
                'brand': '$product.brand.name',
                'model': '$product.model', 
                'year': '$variations.year'
            }, 
            'variations': {
                '$first': '$variations'
            }, 
            'relevance': {
                '$first': '$relevance'
            }, 
            'condition': {
                '$first': '$product.condition'
            }
        }
    }, {
        '$project': {
            '_id': 0, 
            'countryCode': '$_id.countryCode', 
            'categoryCode': '$_id.categoryCode', 
            'code': '$_id.code', 
            'brand': '$_id.brand', 
            'model': '$_id.model', 
            'year': '$_id.year', 
            'sku': '$variations.sku', 
            'stock_variation': '$variations.stock', 
            'status': '$variations.status', 
            # 'price_base': '$variations.price.base', 
            # 'price_net': '$variations.price.net', 
            'condition': 1, 
            'relevance': 1
        }
    }
]