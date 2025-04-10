def extract_publications_data():
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
      "displacement": { "$first": "$product.displacement" },
      "type": { "$first": "$product.type" },
      "price_base": { "$first": "$price.base" },
      "price_currency": { "$first": "$price.currencyCode" },
      "price_net": { "$first": "$price.net" },
      "stock": { "$push": { "$toString": "$variations.stock" } },
      "product_pictures": { "$first": "$product.pictures" }, 
      "published": { "$first": "$published" },
      "forwarding_code": { "$first": "$_forwardingCode" },
      "relevance": { "$first": "$relevance" },
      "condition": { "$first": "$product.condition" }
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
      "displacement": 1,
      "type": 1,
      "price_base": 1,
      "price_currency": 1,
      "price_net": 1,
      "stock": 1,
      "published": 1,
      "forwarding_code": 1,
      "relevance": 1,
      "condition": 1,
            "product_pictures": { "$arrayElemAt": ["$product_pictures", 0] } 
        }
    }
]
