import keyvalue.sqlitekeyvalue as KeyValue
import keyvalue.parsetriples as ParseTriples
import keyvalue.stemmer as Stemmer
import keyvalue.dynamostorage as KeyValueDynamo
from botocore.exceptions import ClientError

##############################################
#Código SQLite
#############################################

# # Make connections to KeyValue
#kv_labels = KeyValue.SqliteKeyValue("./local_db/sqlite_labels.db","labels",sortKey=True)
#kv_images = KeyValue.SqliteKeyValue("./local_db/sqlite_images.db","images")


# # Process Logic.
# parse_images = ParseTriples.ParseTriples('./data/images.ttl')
# parse_labels = ParseTriples.ParseTriples('./data/labels_en(1)/labels_en.ttl')

# # Insert Images 
# for i in range(50):
#     url = parse_images.getNext()
#     if url is None:
#         break
#     key = url[0] #category
#     relationship = url[1]
#     value = url[2] #Image URL
#     if relationship == 'http://xmlns.com/foaf/0.1/depiction':
#         print("Key: " + key + " value: " + value)
#         kv_images.put(key,value)
#         #kv_images.put(key, len(key), value)

# # Insert Labels
# for i in range(100):
#     url = parse_labels.getNext()
#     if url is None:
#         break
#     value = url[0] #category
#     relationship = url[1]
#     keys = Stemmer.stem(url[2]) #label
#     if relationship == 'http://www.w3.org/2000/01/rdf-schema#label':
#         #img_exists = kv_images.get(value, len(value))
#         img_exists = kv_images.get(value)
#         if img_exists is not None:
#             for key in keys.split(' '):
#                 print("Key: " + key + " value: " + value + " sort:" + str(i))
#                 kv_labels.putSort(key, str(i), value)
#                # kv_labels.put(key, len(key), value)


# # Close KeyValues Storages
# parse_images.close()
# parse_labels.close()

# kv_labels.close()
# kv_images.close()

##############################################
#Código Dynamodb
#############################################


# Make connections to KeyValue
kv_images = KeyValueDynamo.DynamodbKeyValue("images")
kv_labels = KeyValueDynamo.DynamodbKeyValue("labels")

# Process Logic.
parse_images = ParseTriples.ParseTriples('./data/images.ttl')
parse_labels = ParseTriples.ParseTriples('./data/labels_en(1)/labels_en.ttl')

# Insert Images 
for i in range(10000):
    url = parse_images.getNext()
    key = url[0] #category
    relationship = url[1]
    value = url[2] #Image URL
    if relationship == 'http://xmlns.com/foaf/0.1/depiction':
        print("images: " + str(i))
        kv_images.put(key, len(key), value)

# Insert Labels
for i in range(30000):
    url = parse_labels.getNext()
    value = url[0] #category
    relationship = url[1]
    keys = Stemmer.stem(url[2]) #label
    if relationship == 'http://www.w3.org/2000/01/rdf-schema#label':
        img_exists = kv_images.get(value, len(value))
        if 'Item' in img_exists.keys():
            for key in keys.split(' '):
                print("labels: " + str(i))
                kv_labels.put(key, len(key), value)


