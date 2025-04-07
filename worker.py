import hashlib
import json
import os
import uuid
from datetime import datetime
from concurrent import futures
import grpc
import database_pb2
import database_pb2_grpc
import logging
import time

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("Worker")

class SimpleJSONDB:
    def __init__(self, db_file):
        self.db_file = db_file
        self.data = {}
        if os.path.exists(db_file):
            with open(db_file, 'r') as f:
                self.data = json.load(f)

    def create_document(self, document, doc_id):
        self.data[doc_id] = document
        self._save()

    def read_document(self, doc_id):
        return self.data.get(doc_id)

    def read_all_documents(self):
        return list(self.data.values())

    def query_documents(self, filter_func):
        return [doc for doc in self.data.values() if filter_func(doc)]

    def update_document(self, doc_id, updates, create_if_missing=False):
        if doc_id in self.data:
            self.data[doc_id].update(updates)
            self._save()
            return True
        elif create_if_missing:
            self.data[doc_id] = updates
            self._save()
            return True
        return False

    def delete_document(self, doc_id):
        if doc_id in self.data:
            del self.data[doc_id]
            self._save()
            return True
        return False

    def clear_database(self):
        self.data = {}
        self._save()

    def _save(self):
        with open(self.db_file, 'w') as f:
            json.dump(self.data, f, indent=2)

class ShardMetadata:
    def __init__(self):
        self.document_shards = {}
        self.worker_channels = {}
        self.shard_workers = set()

    def get_shard_worker(self, doc_id):
        return self.document_shards.get(doc_id)

    def add_shard(self, doc_id, worker_address):
        self.document_shards[doc_id] = worker_address
        self.shard_workers.add(worker_address)
        if worker_address not in self.worker_channels:
            self.worker_channels[worker_address] = grpc.insecure_channel(worker_address)

    def get_all_shard_workers(self):
        return list(self.shard_workers)

class DatabaseManager:
    def __init__(self):
        self.databases = {}
        self.current_db = None
    
    def create_database(self, db_name):
        if db_name in self.databases:
            raise ValueError(f"Database '{db_name}' already exists")
        self.databases[db_name] = {
            'db': SimpleJSONDB(f"{db_name}.json"),
            'shards': ShardMetadata()
        }
        return self.databases[db_name]['db']
    
    def use_database(self, db_name):
        if db_name not in self.databases:
            if os.path.exists(f"{db_name}.json"):
                self.databases[db_name] = {
                    'db': SimpleJSONDB(f"{db_name}.json"),
                    'shards': ShardMetadata()
                }
            else:
                raise ValueError(f"Database '{db_name}' doesn't exist")
        self.current_db = self.databases[db_name]
        return self.current_db['db']

class DatabaseService(database_pb2_grpc.DatabaseServiceServicer):
    def __init__(self, worker_address):
        self.manager = DatabaseManager()
        self.worker_address = worker_address
        self.known_workers = [worker_address]
        self.master_channel = grpc.insecure_channel("localhost:50050")
        self.master_stub = database_pb2_grpc.DatabaseServiceStub(self.master_channel)
        self._discover_workers()

    def _discover_workers(self):
        for _ in range(5):  # Retry 5 times
            try:
                response = self.master_stub.ListWorkers(database_pb2.Empty())
                self.known_workers = list(response.workers)
                logger.info(f"Discovered workers: {self.known_workers}")
                break
            except Exception as e:
                logger.warning(f"Could not discover workers from master: {str(e)}")
                time.sleep(1)  # Wait before retrying
        else:
            self.known_workers = ["localhost:50051", "localhost:50052"]
            logger.info(f"Fallback to default workers: {self.known_workers}")

    def CreateDatabase(self, request, context):
        try:
            self.manager.create_database(request.name)
            return database_pb2.OperationResponse(success=True, message=f"Database '{request.name}' created")
        except Exception as e:
            return database_pb2.OperationResponse(success=False, message=str(e))
    
    def UseDatabase(self, request, context):
        try:
            self.manager.use_database(request.name)
            return database_pb2.OperationResponse(success=True, message=f"Using database '{request.name}'")
        except Exception as e:
            return database_pb2.OperationResponse(success=False, message=str(e))
    
    def ListDatabases(self, request, context):
        dbs = list(self.manager.databases.keys())
        return database_pb2.DatabaseList(names=dbs)
    
    def DeleteDatabase(self, request, context):
        try:
            if request.name in self.manager.databases:
                if os.path.exists(f"{request.name}.json"):
                    os.remove(f"{request.name}.json")
                del self.manager.databases[request.name]
                if self.manager.current_db and self.manager.current_db['db'].db_file == f"{request.name}.json":
                    self.manager.current_db = None
                return database_pb2.OperationResponse(success=True, message=f"Database '{request.name}' deleted")
            return database_pb2.OperationResponse(success=False, message=f"Database '{request.name}' not found")
        except Exception as e:
            return database_pb2.OperationResponse(success=False, message=str(e))
    
    def CreateDocument(self, request, context):
        try:
            # Auto-select database if not set
            if not self.manager.current_db or self.manager.current_db['db'].db_file != f"{request.db_name}.json":
                logger.info(f"Auto-selecting database: {request.db_name}")
                self.manager.use_database(request.db_name)
            
            doc = json.loads(request.document)
            doc_id = request.doc_id or str(uuid.uuid4())
            
            shard_worker = self._select_shard_worker(doc_id)
            logger.info(f"Document {doc_id} assigned to worker {shard_worker}")
            
            if shard_worker == self.worker_address:
                self.manager.current_db['db'].create_document({
                    **doc,
                    '_id': doc_id,
                    '_created_at': datetime.now().isoformat(),
                    '_updated_at': datetime.now().isoformat()
                }, doc_id)
            else:
                if shard_worker not in self.manager.current_db['shards'].worker_channels:
                    self.manager.current_db['shards'].worker_channels[shard_worker] = grpc.insecure_channel(shard_worker)
                stub = database_pb2_grpc.DatabaseServiceStub(
                    self.manager.current_db['shards'].worker_channels[shard_worker]
                )
                response = stub.CreateDocument(request)
                if not response.doc_id:
                    raise ValueError("Failed to create document on shard worker")
                doc_id = response.doc_id
            
            self.manager.current_db['shards'].add_shard(doc_id, shard_worker)
            return database_pb2.DocumentID(doc_id=doc_id)
        except Exception as e:
            logger.error(f"Error creating document: {str(e)}")
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(str(e))
            return database_pb2.DocumentID()
        
    def _select_shard_worker(self, doc_id):
        if not self.known_workers:
            return self.worker_address
        
        worker_hashes = [(int(hashlib.md5(worker.encode()).hexdigest(), 16), worker) for worker in self.known_workers]
        worker_hashes.sort()
        doc_hash = int(hashlib.md5(doc_id.encode()).hexdigest(), 16)
        for hash_val, worker in worker_hashes:
            if doc_hash <= hash_val:
                return worker
        return worker_hashes[0][1]

    def ReadDocument(self, request, context):
        try:
            if not self.manager.current_db or self.manager.current_db['db'].db_file != f"{request.db_name}.json":
                self.manager.use_database(request.db_name)
            
            shard_worker = self.manager.current_db['shards'].get_shard_worker(request.doc_id)
            if not shard_worker:
                context.set_code(grpc.StatusCode.NOT_FOUND)
                return database_pb2.DocumentResponse()
            
            if shard_worker == self.worker_address:
                doc = self.manager.current_db['db'].read_document(request.doc_id)
            else:
                channel = self.manager.current_db['shards'].worker_channels[shard_worker]
                stub = database_pb2_grpc.DatabaseServiceStub(channel)
                response = stub.ReadDocument(database_pb2.DocumentID(
                    db_name=request.db_name,
                    doc_id=request.doc_id
                ))
                doc = json.loads(response.document) if response.document else None
            
            if doc:
                return database_pb2.DocumentResponse(document=json.dumps(doc))
            context.set_code(grpc.StatusCode.NOT_FOUND)
            return database_pb2.DocumentResponse()
        except Exception as e:
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(str(e))
            return database_pb2.DocumentResponse()
    
    def ReadAllDocuments(self, request, context):
        try:
            if not self.manager.current_db or self.manager.current_db['db'].db_file != f"{request.name}.json":
                self.manager.use_database(request.name)
            docs = self.manager.current_db['db'].read_all_documents()
            return database_pb2.DocumentList(documents=[json.dumps(doc) for doc in docs])
        except Exception as e:
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(str(e))
            return database_pb2.DocumentList()
    
    def QueryDocuments(self, request, context):
        try:
            if not self.manager.current_db or self.manager.current_db['db'].db_file != f"{request.db_name}.json":
                self.manager.use_database(request.db_name)
            filter_func = eval(request.filter_expr)
            if not callable(filter_func):
                raise ValueError("Filter expression must be callable")
            docs = self.manager.current_db['db'].query_documents(filter_func)
            return database_pb2.DocumentList(documents=[json.dumps(doc) for doc in docs])
        except Exception as e:
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            context.set_details(str(e))
            return database_pb2.DocumentList()
    
    def UpdateDocument(self, request, context):
        try:
            if not self.manager.current_db or self.manager.current_db['db'].db_file != f"{request.db_name}.json":
                self.manager.use_database(request.db_name)
            
            shard_worker = self.manager.current_db['shards'].get_shard_worker(request.doc_id)
            if not shard_worker and request.create_if_missing:
                return self.CreateDocument(database_pb2.DocumentRequest(
                    db_name=request.db_name,
                    document=request.updates,
                    doc_id=request.doc_id
                ), context)
            
            if not shard_worker:
                context.set_code(grpc.StatusCode.NOT_FOUND)
                return database_pb2.OperationResponse(success=False, message="Document not found")
            
            if shard_worker == self.worker_address:
                success = self.manager.current_db['db'].update_document(
                    request.doc_id,
                    json.loads(request.updates),
                    request.create_if_missing
                )
            else:
                channel = self.manager.current_db['shards'].worker_channels[shard_worker]
                stub = database_pb2_grpc.DatabaseServiceStub(channel)
                response = stub.UpdateDocument(request)
                return response
            
            return database_pb2.OperationResponse(
                success=success,
                message="Document updated" if success else "Document not found"
            )
        except Exception as e:
            context.set_code(grpc.StatusCode.INTERNAL)
            return database_pb2.OperationResponse(success=False, message=str(e))
    
    def DeleteDocument(self, request, context):
        try:
            if not self.manager.current_db or self.manager.current_db['db'].db_file != f"{request.db_name}.json":
                self.manager.use_database(request.db_name)
            
            shard_worker = self.manager.current_db['shards'].get_shard_worker(request.doc_id)
            if not shard_worker:
                context.set_code(grpc.StatusCode.NOT_FOUND)
                return database_pb2.OperationResponse(success=False, message="Document not found")
            
            if shard_worker == self.worker_address:
                success = self.manager.current_db['db'].delete_document(request.doc_id)
            else:
                channel = self.manager.current_db['shards'].worker_channels[shard_worker]
                stub = database_pb2_grpc.DatabaseServiceStub(channel)
                response = stub.DeleteDocument(request)
                return response
            
            if success:
                del self.manager.current_db['shards'].document_shards[request.doc_id]
            return database_pb2.OperationResponse(
                success=success,
                message="Document deleted" if success else "Document not found"
            )
        except Exception as e:
            context.set_code(grpc.StatusCode.INTERNAL)
            return database_pb2.OperationResponse(success=False, message=str(e))
    
    def ClearDatabase(self, request, context):
        try:
            if not self.manager.current_db or self.manager.current_db['db'].db_file != f"{request.name}.json":
                self.manager.use_database(request.name)
            self.manager.current_db['db'].clear_database()
            self.manager.current_db['shards'] = ShardMetadata()
            return database_pb2.OperationResponse(success=True, message="Database cleared")
        except Exception as e:
            context.set_code(grpc.StatusCode.INTERNAL)
            return database_pb2.OperationResponse(success=False, message=str(e))
    
    def GetShardLocations(self, request, context):
        try:
            if request.name not in self.manager.databases:
                context.set_code(grpc.StatusCode.NOT_FOUND)
                return database_pb2.WorkerList()
            workers = self.manager.databases[request.name]['shards'].get_all_shard_workers()
            if not workers:
                workers = [self.worker_address]
            return database_pb2.WorkerList(workers=workers)
        except Exception as e:
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(str(e))
            return database_pb2.WorkerList()

def serve(port):
    worker_address = f"localhost:{port}"
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    database_pb2_grpc.add_DatabaseServiceServicer_to_server(
        DatabaseService(worker_address), server)
    server.add_insecure_port(f'[::]:{port}')
    server.start()
    print(f"Worker node running on port {port}")
    server.wait_for_termination()

if __name__ == '__main__':
    import sys
    port = int(sys.argv[1]) if len(sys.argv) > 1 else 50051
    serve(port)
