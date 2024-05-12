from django.db import models
import json

def update_keys(node, kv, new_value):
                    if isinstance(node, list):
                        for i in node:
                            update_keys(i, kv, new_value)
                    elif isinstance(node, dict):
                        if kv in node:
                            if isinstance(node[kv], dict):
                                node[kv].update(new_value)
                            elif isinstance(node[kv], list):
                                node[kv].append(new_value)
                            else:
                                node[kv] = new_value
                    for j in node.values():
                            update_keys(j, kv, new_value)
                    return node

class SharedObject(models.Model):
    key = models.IntegerField(primary_key=True)
    value = models.TextField()

    @classmethod
    def get_value(cls, process_id:int, key=None):
        
        shared_object = cls.objects.filter(key=int(process_id)).first()
        if key and shared_object:
            process = json.loads(shared_object.value)
            return process[key]
        
        return json.loads(shared_object.value) if shared_object else None

    @classmethod
    def modify_value(cls, new_value, process_id, nested_key=None):
        '''
        new_value = value to update, could be nested dictionary
        process_id = process_id to identify the record
        nested_key = dot-separated nested key path (e.g., 'key1.key2') to update nested value
        '''
        shared_object = cls.objects.filter(key=int(process_id)).first()
        if shared_object:
            # Deserialize the value from JSON
            current_value = json.loads(shared_object.value)

            # Check if a nested key is provided
            if nested_key:
                # Split the nested key path into individual keys
                nested_keys = nested_key.split('.')
                # Traverse through the nested keys to get to the nested dictionary
                current_value = update_keys(current_value, nested_keys[-1], new_value)

            else:
                # Update entire value if nested_key is not provided
                current_value.update(new_value)

            # Serialize the updated value back to JSON
            json_value = json.dumps(current_value)

            # Update the value of the shared object in the database
            shared_object.value = json_value
            shared_object.save()

            return json_value
        return None
    
    @classmethod
    def insert_value(cls, new_value, process_id):
        # Check if the entry already exists
        if cls.objects.filter(key=process_id).exists():
            return 'Already exists'
        else:
            # Serialize the new value to JSON
            json_value = json.dumps(new_value)
            # Create a new entry in the database
            cls.objects.create(key=process_id, value=json_value)
            return new_value
        
    @classmethod
    def delete_value(cls, process_id):
        #delete the entry with the specified process_id
        entry = cls.objects.filter(key=process_id)
        if not entry:
            return 'No data found.'
        entry.delete()