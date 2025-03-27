class Sharding:
    def __init__(self, num_shards):
        self.num_shards = num_shards

    def get_shard(self, key):
        return hash(key) % self.num_shards