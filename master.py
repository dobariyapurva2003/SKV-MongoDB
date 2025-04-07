from datetime import datetime
import uuid
import grpc
import json
import hashlib
from concurrent import futures
import threading
import time
import database_pb2
import database_pb2_grpc
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("Master")

class WorkerNode:
    def __init__(self, address):
        self.address = address
        self.channel = grpc.insecure_channel(address)
        self.stub = database_pb2_grpc.DatabaseServiceStub(self.channel)
        self.health = True
        self.load = 0

class MasterNode:
    def __init__(self):
        self.workers = {}  # {worker_address: WorkerNode}
        self.database_assignments = {}  # {db_name: primary_worker_address}
        self.lock = threading.Lock()
        
        # Start health check thread
        self.health_thread = threading.Thread(target=self._health_check)
        self.health_thread.daemon = True
        self.health_thread.start()
    
    def add_worker(self, address):
        with self.lock:
            if address not in self.workers:
                self.workers[address] = WorkerNode(address)
                print(f"Added worker {address}")
                return True
        return False
    
    def assign_database(self, db_name, worker_address):
        with self.lock:
            if worker_address in self.workers:
                self.database_assignments[db_name] = worker_address
                return True
        return False
    
    def get_primary_worker(self, db_name):
        with self.lock:
            return self.workers.get(self.database_assignments.get(db_name))
    
    def _health_check(self):
        while True:
            time.sleep(5)
            with self.lock:
                for address, worker in list(self.workers.items()):
                    try:
                        worker.stub.ListDatabases(database_pb2.Empty(), timeout=2)
                        worker.health = True
                    except:
                        worker.health = False
                        for db, worker_addr in list(self.database_assignments.items()):
                            if worker_addr == address:
                                del self.database_assignments[db]
                        del self.workers[address]
                        print(f"Removed failed worker {address}")

class MasterService(database_pb2_grpc.DatabaseServiceServicer):
    def __init__(self, master_node):
        self.master = master_node

    def ListWorkers(self, request, context):
        with self.master.lock:
            workers = list(self.master.workers.keys())
        return database_pb2.WorkerList(workers=workers)
    
    def CreateDatabase(self, request, context):
        worker = self._select_worker_for_db(request.name)
        if not worker:
            context.set_code(grpc.StatusCode.UNAVAILABLE)
            return database_pb2.OperationResponse(success=False, message="No workers available")
        
        try:
            response = worker.stub.CreateDatabase(request)
            if response.success:
                self.master.assign_database(request.name, worker.address)
            return response
        except Exception as e:
            context.set_code(grpc.StatusCode.INTERNAL)
            return database_pb2.OperationResponse(success=False, message=str(e))
    
    def _select_worker_for_db(self, db_name):
        if not self.master.workers:
            return None
        
        hash_val = int(hashlib.md5(db_name.encode()).hexdigest(), 16)
        workers = sorted(self.master.workers.values(), key=lambda w: hash(w.address))
        return workers[hash_val % len(workers)]
    
    def UseDatabase(self, request, context):
        worker = self.master.get_primary_worker(request.name)
        if not worker:
            context.set_code(grpc.StatusCode.NOT_FOUND)
            return database_pb2.OperationResponse(success=False, message="Database not found")
        return worker.stub.UseDatabase(request)
    
    def ListDatabases(self, request, context):
        with self.master.lock:
            return database_pb2.DatabaseList(names=list(self.master.database_assignments.keys()))
        
    def DeleteDatabase(self, request, context):
        worker = self.master.get_primary_worker(request.name)
        if not worker:
            context.set_code(grpc.StatusCode.NOT_FOUND)
            return database_pb2.OperationResponse(success=False, message="Database not found")
        
        try:
            response = worker.stub.DeleteDatabase(request)
            if response.success:
                with self.master.lock:
                    if request.name in self.master.database_assignments:
                        del self.master.database_assignments[request.name]
            return response
        except Exception as e:
            context.set_code(grpc.StatusCode.INTERNAL)
            return database_pb2.OperationResponse(success=False, message=str(e))
    
    def CreateDocument(self, request, context):
        logger.info(f"CreateDocument: db_name={request.db_name}, doc_id={request.doc_id}")
        worker = self.master.get_primary_worker(request.db_name)
        if not worker:
            logger.error(f"No worker found for db_name={request.db_name}")
            context.set_code(grpc.StatusCode.NOT_FOUND)
            return database_pb2.DocumentID()
        
        try:
            logger.info(f"Forwarding CreateDocument to worker: {worker.address}")
            response = worker.stub.CreateDocument(request)
            logger.info(f"Response from worker: {response.doc_id}")
            return response
        except Exception as e:
            logger.error(f"Error forwarding to {worker.address}: {str(e)}")
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(str(e))
            return database_pb2.DocumentID()
    
    def ReadDocument(self, request, context):
        worker = self.master.get_primary_worker(request.db_name)
        if not worker:
            context.set_code(grpc.StatusCode.NOT_FOUND)
            return database_pb2.DocumentResponse()
        return worker.stub.ReadDocument(request)

    def ReadAllDocuments(self, request, context):
        worker = self.master.get_primary_worker(request.name)
        if not worker:
            context.set_code(grpc.StatusCode.NOT_FOUND)
            return database_pb2.DocumentList()
        
        try:
            shard_locations = worker.stub.GetShardLocations(
                database_pb2.DatabaseName(name=request.name)
            )
            all_documents = []
            for shard_worker_addr in shard_locations.workers:
                shard_worker = self.master.workers.get(shard_worker_addr)
                if shard_worker:
                    response = shard_worker.stub.ReadAllDocuments(request)
                    all_documents.extend(response.documents)
            return database_pb2.DocumentList(documents=all_documents)
        except Exception as e:
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(str(e))
            return database_pb2.DocumentList()

    def QueryDocuments(self, request, context):
        worker = self.master.get_primary_worker(request.db_name)
        if not worker:
            context.set_code(grpc.StatusCode.NOT_FOUND)
            return database_pb2.DocumentList()
        
        try:
            shard_locations = worker.stub.GetShardLocations(
                database_pb2.DatabaseName(name=request.db_name))
            matching_docs = []
            for shard_worker_addr in shard_locations.workers:
                shard_worker = self.master.workers.get(shard_worker_addr)
                if shard_worker:
                    response = shard_worker.stub.QueryDocuments(request)
                    matching_docs.extend(response.documents)
            return database_pb2.DocumentList(documents=matching_docs)
        except Exception as e:
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(str(e))
            return database_pb2.DocumentList()

    def UpdateDocument(self, request, context):
        worker = self.master.get_primary_worker(request.db_name)
        if not worker:
            context.set_code(grpc.StatusCode.NOT_FOUND)
            return database_pb2.OperationResponse(success=False, message="Database not found")
        return worker.stub.UpdateDocument(request)

    def DeleteDocument(self, request, context):
        worker = self.master.get_primary_worker(request.db_name)
        if not worker:
            context.set_code(grpc.StatusCode.NOT_FOUND)
            return database_pb2.OperationResponse(success=False, message="Database not found")
        return worker.stub.DeleteDocument(request)

    def ClearDatabase(self, request, context):
        worker = self.master.get_primary_worker(request.name)
        if not worker:
            context.set_code(grpc.StatusCode.NOT_FOUND)
            return database_pb2.OperationResponse(success=False, message="Database not found")
        return worker.stub.ClearDatabase(request)

def serve_master():
    master_node = MasterNode()
    master_node.add_worker("localhost:50051")
    master_node.add_worker("localhost:50052")
    
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    database_pb2_grpc.add_DatabaseServiceServicer_to_server(
        MasterService(master_node), server)
    server.add_insecure_port('[::]:50050')
    server.start()
    print("Master node running on port 50050")
    server.wait_for_termination()

if __name__ == '__main__':
    serve_master()
