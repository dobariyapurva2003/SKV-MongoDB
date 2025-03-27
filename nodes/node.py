class Node:
    def __init__(self, id, role, data):
        self.id = id
        self.role = role
        self.data = data

    def sync(self, data):
        self.data.update(data)
