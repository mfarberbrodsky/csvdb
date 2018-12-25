import json


class Schema:
    def __init__(self, schema_filename):
        self.load(schema_filename)

    def load(self, schema_filename):
        j = json.load(open(schema_filename))
        self.fields = j["schema"]
        self.name_to_index = {}
        for i, f in enumerate(self.fields):
            self.name_to_index[f["field"]] = i

    def get_all_field_names(self):
        return [field["field"] for field in self.fields]

    def get_field_index(self, field_name):
        return self.name_to_index[field_name]
