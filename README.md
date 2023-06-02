ORM package for Elasticsearch
=================

Tailored to specific needs modification of **elasticsearch_dsl 7.3**.
If you are familiar with elasticsearch_dsl, you will find that you can freely
use it's documentation working with this package as well.

**Features**

 - *Special structure of representing data*: value of a field is a dictionary 
   containing at least one key: *value*. You don't need to change the way 
   you work with data though, thus:
   
   ```python
   my_document = MyIndex(my_field="field_value") 
   
   OR
   
   my_document.my_field = "field_value"
   ``` 
   will turn into:
   ```json
    "my_field": {
        "value": "field_value",
        "flag": "Some information about the field",
        "pretty_name": "Human-readable representation of the field name"
    }
    ```
 - *Validation levels*: gives an ability to set level of strictness to
   validation for fields.
   1. STRICT: sets index mapping in Elasticsearch to the chosen one, 
      thus you can't save any other type of data in the database.
   2. WARNING: sets index mapping in Elasticsearch to "Text", which gives
      an ability to store any type of data in there. If there is any errors
      during validation, they are stored to the value dictionary 
      under the "flag" key.
   3. DISABLED: sets index mapping in Elasticsearch to "Text", omits any
   validation errors.
      
Full documentation can be found on the Wiki page.

Example of usage
==========

```python
 from es_orm import Document, connect
 from es_orm.fields import Text, Integer, Choices

 # Connecting to Elasticseach database
 connect("http://127.0.0.1:9200")


 # Defining the structure of our Document and mapping
 class MyDocument(Document):
      int = Integer()
      text = Text(validation_level="warning")
      choices = Choices(choices=["first option", "second option"], 
                        validation_level="disabled")

      # Don't forget to set default index
      class Index:
          name = "my_new_index"

 # Creating an object represenging a document in the "my_new_index" index
 new_document = MyDocument(int=521)
 new_document.text = "Something important"
 new_document.choices = "first option"
 
 # Initializing index in the database by creating it and putting the mapping:
 MyDocument.init()

 # Saving the document to the database.
 new_document.save()

 # Retrieving the same document but using Document method:
 doc_id = new_document.meta['id']  # Got the ID of the document in the ES
 db_doc = MyDocument.get(id=doc_id)  # Retrieved the document with ID = doc_id

 # Changing data of the document
 new_data = {
    "int": 999,
    "text": "New information",
    "choices": "second option"
 } 
 db_doc.set(new_data)
 
 # And save it again to the database 
 db_doc.save()
 ```

Wiki: https://github.com/mtsarev06/es_orm/wiki
