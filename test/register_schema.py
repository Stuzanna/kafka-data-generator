from confluent_kafka.schema_registry import SchemaRegistryClient

from adminTools import register_schema

schema_registry_url = "http://localhost:18081"
schema_registry_conf = {'url': schema_registry_url, 'basic.auth.user.info': '<SR_UserName:SR_Password>'}
schema_registry_client = SchemaRegistryClient(schema_registry_conf)

schema_loc = 'remote' # local or remote
schema_id = None
subject = 'customers-value'

serialization = "avro" # avro, json, none
schema_file = './schemas/customer-data-gen-1.avsc' # or .json

# register schema if not already present
register_schema(schema_registry_url, subject, schema_file, serialization)