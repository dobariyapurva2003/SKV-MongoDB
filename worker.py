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
        self.replica_channels = {}  # Cache for replica channels

    def _check_master_connection(self):
        for _ in range(3):  # Retry 3 times
            try:
                self.master_stub.ListWorkers(database_pb2.Empty(), timeout=2)
                return True
            except:
                time.sleep(1)
        return False

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
            logger.info(f"CreateDocument received for db={request.db_name}, doc_id={request.doc_id}")
            
            if not self.manager.current_db or self.manager.current_db['db'].db_file != f"{request.db_name}.json":
                self.manager.use_database(request.db_name)
            
            doc_id = request.doc_id or str(uuid.uuid4())
            doc_data = {
                **json.loads(request.document),
                '_id': doc_id,
                '_created_at': datetime.now().isoformat(),
                '_updated_at': datetime.now().isoformat(),
                '_primary': True
            }
            
            # Store document locally
            self.manager.current_db['db'].create_document(doc_data, doc_id)
            logger.info(f"Document {doc_id} created successfully")
            
            return database_pb2.DocumentID(doc_id=doc_id)
            
        except json.JSONDecodeError as e:
            logger.error(f"Invalid JSON: {str(e)}")
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            context.set_details("Invalid JSON format")
            return database_pb2.DocumentID()
        except Exception as e:
            logger.error(f"Error creating document: {str(e)}", exc_info=True)
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(str(e))
            return database_pb2.DocumentID()
    
    def ReplicateDocument(self, request, context):
        """Handle document replication from primary"""
        try:
            if not self.manager.current_db or self.manager.current_db['db'].db_file != f"{request.db_name}.json":
                self.manager.use_database(request.db_name)
            
            doc_data = json.loads(request.document)  # Full document from primary
            doc_id = request.doc_id
            
            # If document exists, update it; otherwise, create it
            if doc_id in self.manager.current_db['db'].data:
                print(f"updated replicas")
                current_doc = self.manager.current_db['db'].data[doc_id]
                current_doc.update(doc_data)
                current_doc['_primary'] = False  # Ensure it remains a replica
                current_doc['_updated_at'] = datetime.now().isoformat()
            else:
                doc_data['_primary'] = False
                doc_data['_created_at'] = datetime.now().isoformat()
                doc_data['_updated_at'] = datetime.now().isoformat()
                self.manager.current_db['db'].create_document(doc_data, doc_id)
            
            logger.info(f"Document {doc_id} replicated successfully on {self.worker_address}")
            return database_pb2.OperationResponse(success=True, message="Document replicated")
        except Exception as e:
            logger.error(f"Error replicating document: {str(e)}")
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(str(e))
            return database_pb2.OperationResponse(success=False, message=str(e))
        
    def _replicate_document(self, db_name, doc_id, document_json):
        """Helper to replicate document to all replicas"""
        try:
            replicas = self.master_stub.GetDocumentReplicas(
                database_pb2.DocumentID(db_name=db_name, doc_id=doc_id)
            ).workers
            
            for replica_addr in replicas:
                if replica_addr != self.worker_address:  # Skip self
                    try:
                        if replica_addr not in self.replica_channels:
                            self.replica_channels[replica_addr] = grpc.insecure_channel(replica_addr)
                        
                        stub = database_pb2_grpc.DatabaseServiceStub(self.replica_channels[replica_addr])
                        response = stub.ReplicateDocument(
                            database_pb2.ReplicateRequest(
                                db_name=db_name,
                                doc_id=doc_id,
                                document=document_json
                            ),
                            timeout=3
                        )
                        logger.info(f"Replicated to {replica_addr}: {response.message}")
                    except Exception as e:
                        logger.error(f"Failed to replicate to {replica_addr}: {str(e)}")
        except Exception as e:
            logger.error(f"Failed to get replicas from master: {str(e)}")



    def ReadDocument(self, request, context):
        try:
            if not self.manager.current_db or self.manager.current_db['db'].db_file != f"{request.db_name}.json":
                self.manager.use_database(request.db_name)
            
            # Check if we have the document locally
            doc = self.manager.current_db['db'].read_document(request.doc_id)
            if doc:
                return database_pb2.DocumentResponse(document=json.dumps(doc))
            
            # If not found locally, check with master for location
            worker_response = self.master_stub.GetDocumentLocation(
                database_pb2.DocumentID(
                    db_name=request.db_name,
                    doc_id=request.doc_id
                ))
            
            if not worker_response.worker:
                context.set_code(grpc.StatusCode.NOT_FOUND)
                return database_pb2.DocumentResponse()
            
            # Forward to the correct worker
            if worker_response.worker not in self.replica_channels:
                self.replica_channels[worker_response.worker] = grpc.insecure_channel(worker_response.worker)
            
            stub = database_pb2_grpc.DatabaseServiceStub(self.replica_channels[worker_response.worker])
            return stub.ReadDocument(request)
            
        except Exception as e:
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(str(e))
            return database_pb2.DocumentResponse()
        
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
            logger.info(f"UpdateDocument: db={request.db_name}, doc_id={request.doc_id}")
            
            if not self.manager.current_db or self.manager.current_db['db'].db_file != f"{request.db_name}.json":
                self.manager.use_database(request.db_name)
            
            # Check if document exists locally
            if request.doc_id not in self.manager.current_db['db'].data:
                context.set_code(grpc.StatusCode.NOT_FOUND)
                return database_pb2.OperationResponse(success=False, message="Document not found")
            
            # Apply updates
            updates = json.loads(request.updates)
            current_doc = self.manager.current_db['db'].data[request.doc_id]
            current_doc.update(updates)
            current_doc['_updated_at'] = datetime.now().isoformat()
            
            self.manager.current_db['db']._save()
            
            # If this is primary, replicate to replicas
            if current_doc.get('_primary', False):
                try:
                    replicas = self.master_stub.GetDocumentReplicas(
                        database_pb2.DocumentID(
                            db_name=request.db_name,
                            doc_id=request.doc_id
                        )).workers
                    
                    for replica_addr in replicas:
                        try:
                            if replica_addr not in self.replica_channels:
                                self.replica_channels[replica_addr] = grpc.insecure_channel(replica_addr)
                            
                            stub = database_pb2_grpc.DatabaseServiceStub(self.replica_channels[replica_addr])
                            stub.ReplicateDocument(database_pb2.ReplicateRequest(
                                db_name=request.db_name,
                                doc_id=request.doc_id,
                                document=json.dumps(updates)
                            ))
                        except Exception as e:
                            logger.error(f"Failed to replicate to {replica_addr}: {str(e)}")
                
                except Exception as e:
                    logger.error(f"Failed to get replicas: {str(e)}")
            
            return database_pb2.OperationResponse(success=True, message="Document updated")
        
        except json.JSONDecodeError:
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            return database_pb2.OperationResponse(success=False, message="Invalid JSON format")
        except Exception as e:
            logger.error(f"Error updating document: {str(e)}", exc_info=True)
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
