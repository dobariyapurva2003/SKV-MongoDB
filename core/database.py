import json

class Database:
    def __init__(self, filename="storage/data.json"):
        self.filename = filename
        self.load_data()

    def load_data(self):
        try:
            with open(self.filename, "r") as f:
                self.data = json.load(f)
        except (FileNotFoundError, json.JSONDecodeError):
            self.data = {}

    def save_data(self):
        with open(self.filename, "w") as f:
            json.dump(self.data, f, indent=4)

    def insert(self, key, value):
        if key in self.data:
            print(f"Key '{key}' already exists. Use update if you want to modify it.")
        else:
            self.data[key] = value
            self.save_data()
            print(f"Inserted key '{key}' successfully.")

    def find(self, key):
        return self.data.get(key, "Key not found")

    def update(self, key, value):
        if key in self.data:
            self.data[key] = value
            self.save_data()
            print(f"Updated key '{key}' successfully.")
        else:
            print("Key not found")

    def delete(self, key):
        if key in self.data:
            del self.data[key]
            self.save_data()
            print(f"Deleted key '{key}' successfully.")
        else:
            print("Key not found")
