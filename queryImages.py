import keyvalue.dynamostorage as KeyValueDynamo
import keyvalue.sqlitekeyvalue as KeyValue
import keyvalue.parsetriples as ParseTripe
import keyvalue.stemmer as Stemmer
import sys

# Make connections to KeyValue
kv_labels = KeyValue.SqliteKeyValue("./local_db/sqlite_labels.db","labels",sortKey=True)
kv_images = KeyValue.SqliteKeyValue("./local_db/sqlite_images.db","images")

# Process Logic.
for arg in range(1, len(sys.argv)):

    search_term = kv_labels.get(Stemmer.stem(sys.argv[arg]))

    if search_term is not None:
        search_img = kv_images.get(search_term)
        print('Term: ' + sys.argv[arg] + ' URL: ' + search_img)

    else:    
        print('No images found with label: ' + sys.argv[arg])


# Close KeyValues Storages
kv_labels.close()
kv_images.close()









