from concurrent import futures
import grpc
from core.database import Database
import psdb_pb2
import psdb_pb2_grpc

class PSDBServicer(psdb_pb2_grpc.PSDBServicer):
    def __init__(self, database):
        self.db = database

    def Insert(self, request, context):
        self.db.insert(request.key, request.value)
        return psdb_pb2.Response(message="Insert successful")

    def Find(self, request, context):
        result = self.db.find(request.key)
        return psdb_pb2.Response(message=result)

    def Update(self, request, context):
        result = self.db.update(request.key, request.value)
        return psdb_pb2.Response(message=result)

    def Delete(self, request, context):
        result = self.db.delete(request.key)
        return psdb_pb2.Response(message=result)

def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    psdb_pb2_grpc.add_PSDBServicer_to_server(PSDBServicer(Database()), server)
    server.add_insecure_port("[::]:50051")
    server.start()
    server.wait_for_termination()
