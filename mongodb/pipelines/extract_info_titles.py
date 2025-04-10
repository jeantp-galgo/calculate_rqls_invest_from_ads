def extract_info_titles_data(countryCode, vehicle_type, condition):
    return [
  {
    "$unwind": {
      "path": "$variations"
    }
  },
  {
    "$group": {
      "_id": "$_id",
      "code": { "$first": "$code" },
      "country": { "$first": "$countryCode" },
      "title": { "$first": "$title" },
      "categoryCode": { "$first": "$product.categoryCode" },
      "brand": { "$first": "$product.brand.name" },
      "model": { "$first": "$product.model" },
      "year": { "$first": "$product.year" },
      "condition": { "$first": "$product.condition" },  
      "published": { "$first": "$published" },
      "relevance": { "$first": "$relevance" },
      "forwarding_code": { "$first": "$_forwardingCode" }
    }
  },
  {
    "$match": {
      "forwarding_code": None,
      "country": countryCode,
      "categoryCode": vehicle_type,
      "published": True,
      "condition": condition
    }
  },
  {
    "$project": {
      "_id": 1,
      "code": 1,
      "country": 1,
      "title": 1,
      "categoryCode": 1,
      "brand": 1,
      "model": 1,
      "year": 1,
      "published": 1,
      "forwarding_code": 1,
      "condition": 1,
      "relevance": 1
    }
  }
]