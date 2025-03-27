class Replication:
    def __init__(self):
        self.replicas = []

    def replicate(self, data):
        for replica in self.replicas:
            replica.sync(data)
