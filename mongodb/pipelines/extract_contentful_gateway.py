def aggregation_contentful_gateway():
    return [
        {
            "$project": {
                "_id": 1,
                "externalId": 1,
                "entryId": 1,
                "entryType": 1,
            }
        }
    ]

def aggregation_combinacion():
    return [
        {
            "$group": {
                "_id": "$_id",
                "externalId": {
                    "$first": "$externalId"
                },
                "entryId": {
                    "$first": "$entryId"
                },
                "entryType": {
                    "$first": "$entryType"
                },
                "createdAt": {
                    "$first": "$createdAt"
                },
                "updatedAt": {
                    "$first": "$updatedAt"
                },
                "__v": {
                    "$first": "$__v"
                }
            }
        }
    ]