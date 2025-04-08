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
        self.document_shards = {}  # {db_name: {doc_id: worker_address}}
        self.lock = threading.Lock()
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
        return self.workers.get(self.database_assignments.get(db_name))
        
    def get_document_worker(self, db_name, doc_id):
            if db_name not in self.document_shards:
                self.document_shards[db_name] = {}
            
            if doc_id not in self.document_shards[db_name]:
                # Assign document to worker using consistent hashing
                if not self.workers:
                    return None
                
                workers = sorted(self.workers.keys())
                hash_val = int(hashlib.md5(doc_id.encode()).hexdigest(), 16)
                worker_idx = hash_val % len(workers)
                self.document_shards[db_name][doc_id] = workers[worker_idx]
            
            return self.document_shards[db_name][doc_id]

    def get_document_replicas(self, db_name, doc_id, replica_count=1):
            primary = self.get_document_worker(db_name, doc_id)
            if not primary:
                print(f"not primary")
                return []
            
            workers = sorted(self.workers.keys())
            if len(workers) <= 1:
                print(f"1 worker")
                return []
            
            primary_idx = workers.index(primary)
            replicas = []
            for i in range(1, replica_count + 1):
                replica_idx = (primary_idx + i) % len(workers)
                if replica_idx != primary_idx:
                    replicas.append(workers[replica_idx])
            
            return replicas
    
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
    
    def GetDocumentReplicas(self, request, context):
        replicas = self.master.get_document_replicas(request.db_name, request.doc_id)
        return database_pb2.WorkerList(workers=replicas)
    
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
    
    def GetPrimaryWorker(self, request, context):
        worker = self.master.get_primary_worker(request.name)
        if not worker:
            context.set_code(grpc.StatusCode.NOT_FOUND)
            return database_pb2.Worker()
        return database_pb2.Worker(worker=worker.address)

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
        
        # Get primary worker for this database
        worker = self.master.get_primary_worker(request.db_name)
        if not worker:
            logger.error(f"No primary worker found for db_name={request.db_name}")
            context.set_code(grpc.StatusCode.NOT_FOUND)
            return database_pb2.DocumentID()
        
        try:
            # Forward to primary worker with timeout
            logger.info(f"Forwarding CreateDocument to primary worker: {worker.address}")
            response = worker.stub.CreateDocument(
                request,
                timeout=5  # 5 second timeout
            )
            
            # Only attempt replication if primary write succeeded
            if response.doc_id:
                replicas = self.master.get_document_replicas(request.db_name, response.doc_id)
                print(replicas)
                for replica_addr in replicas:
                    try:
                        replica_worker = self.master.workers[replica_addr]
                        repon = replica_worker.stub.ReplicateDocument(
                            database_pb2.ReplicateRequest(
                                db_name=request.db_name,
                                doc_id=response.doc_id,
                                document=request.document
                            ),
                            timeout=3  # Shorter timeout for replicas
                        )
                    except Exception as e:
                        logger.error(f"Failed to replicate to {replica_addr}: {str(e)}")
            
            return response
            
        except grpc.RpcError as e:
            logger.error(f"RPC error forwarding to {worker.address}: {e.code()}: {e.details()}")
            context.set_code(e.code())
            context.set_details(e.details())
            return database_pb2.DocumentID()
        except Exception as e:
            logger.error(f"Unexpected error: {str(e)}", exc_info=True)
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(str(e))
            return database_pb2.DocumentID()
    
    # [Add new replication RPC method]
    def GetDocumentLocation(self, request, context):
        worker_addr = self.master.get_document_worker(request.db_name, request.doc_id)
        if not worker_addr:
            context.set_code(grpc.StatusCode.NOT_FOUND)
            return database_pb2.Worker()
        return database_pb2.Worker(worker=worker_addr)
    

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
        logger.info(f"UpdateDocument: db_name={request.db_name}, doc_id={request.doc_id}")
        
        # Find which worker has this document
        worker_addr = self.master.get_document_worker(request.db_name, request.doc_id)
        if not worker_addr:
            context.set_code(grpc.StatusCode.NOT_FOUND)
            return database_pb2.OperationResponse(success=False, message="Document not found")
        
        worker = self.master.workers.get(worker_addr)
        if not worker:
            context.set_code(grpc.StatusCode.NOT_FOUND)
            return database_pb2.OperationResponse(success=False, message="Worker not found")
        
        try:
            print(f"update request sent to wroekr : " ,worker)
            return worker.stub.UpdateDocument(request)
        except Exception as e:
            logger.error(f"Error updating document: {str(e)}")
            context.set_code(grpc.StatusCode.INTERNAL)
            return database_pb2.OperationResponse(success=False, message=str(e))

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
