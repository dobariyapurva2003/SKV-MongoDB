import json
import os
import uuid
from datetime import datetime
from concurrent import futures
import grpc
import database_pb2
import database_pb2_grpc

class SimpleJSONDB:
    def __init__(self, db_file: str = "default_db.json"):
        self.db_file = db_file
        self.data = {}
        self._load_db()
    
    def _load_db(self):
        if os.path.exists(self.db_file):
            with open(self.db_file, 'r') as f:
                try:
                    self.data = json.load(f)
                except json.JSONDecodeError:
                    self.data = {}
    
    def _save_db(self):
        with open(self.db_file, 'w') as f:
            json.dump(self.data, f, indent=2)
    
    def create_document(self, document: dict, doc_id: str = None) -> str:
        doc_id = doc_id or str(uuid.uuid4())
        document['_id'] = doc_id
        document['_created_at'] = datetime.now().isoformat()
        document['_updated_at'] = datetime.now().isoformat()
        self.data[doc_id] = document
        self._save_db()
        return doc_id
    
    def read_document(self, doc_id: str) -> dict:
        return self.data.get(doc_id)
    
    def read_all_documents(self) -> list:
        return list(self.data.values())
    
    def query_documents(self, filter_func: callable) -> list:
        return [doc for doc in self.data.values() if filter_func(doc)]
    
    def update_document(self, doc_id: str, updates: dict, create_if_missing: bool = False) -> bool:
        if doc_id not in self.data:
            if create_if_missing:
                self.create_document(updates, doc_id)
                return True
            return False
        updates['_updated_at'] = datetime.now().isoformat()
        self.data[doc_id].update(updates)
        self._save_db()
        return True
    
    def delete_document(self, doc_id: str) -> bool:
        if doc_id in self.data:
            del self.data[doc_id]
            self._save_db()
            return True
        return False
    
    def clear_database(self):
        self.data = {}
        self._save_db()

class DatabaseManager:
    def __init__(self):
        self.databases = {}
        self.current_db = None
    
    def create_database(self, db_name: str) -> SimpleJSONDB:
        if not db_name.endswith('.json'):
            db_name += '.json'
        if db_name in self.databases:
            raise ValueError(f"Database '{db_name}' already exists")
        db = SimpleJSONDB(db_name)
        self.databases[db_name] = db
        return db
    
    def use_database(self, db_name: str) -> SimpleJSONDB:
        if not db_name.endswith('.json'):
            db_name += '.json'
        if db_name not in self.databases:
            if os.path.exists(db_name):
                db = SimpleJSONDB(db_name)
                self.databases[db_name] = db
            else:
                raise ValueError(f"Database '{db_name}' doesn't exist")
        self.current_db = self.databases[db_name]
        return self.current_db
    
    def list_databases(self) -> list:
        return list(self.databases.keys())
    
    def delete_database(self, db_name: str) -> bool:
        if not db_name.endswith('.json'):
            db_name += '.json'
        if db_name in self.databases:
            if os.path.exists(db_name):
                os.remove(db_name)
            del self.databases[db_name]
            if self.current_db and self.current_db.db_file == db_name:
                self.current_db = None
            return True
        return False

class DatabaseService(database_pb2_grpc.DatabaseServiceServicer):
    def __init__(self):
        self.manager = DatabaseManager()
    
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
        dbs = self.manager.list_databases()
        return database_pb2.DatabaseList(names=dbs)
    
    def DeleteDatabase(self, request, context):
        success = self.manager.delete_database(request.name)
        msg = f"Database '{request.name}' deleted" if success else f"Database '{request.name}' not found"
        return database_pb2.OperationResponse(success=success, message=msg)
    
    def CreateDocument(self, request, context):
        try:
            if not self.manager.current_db:
                raise ValueError("No database selected")
            doc = json.loads(request.document)
            doc_id = self.manager.current_db.create_document(doc, request.doc_id or None)
            return database_pb2.DocumentID(doc_id=doc_id)
        except Exception as e:
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            context.set_details(str(e))
            return database_pb2.DocumentID()
    
    def ReadDocument(self, request, context):
        try:
            if not self.manager.current_db:
                raise ValueError("No database selected")
            doc = self.manager.current_db.read_document(request.doc_id)
            if doc:
                return database_pb2.DocumentResponse(document=json.dumps(doc))
            context.set_code(grpc.StatusCode.NOT_FOUND)
            context.set_details("Document not found")
            return database_pb2.DocumentResponse()
        except Exception as e:
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(str(e))
            return database_pb2.DocumentResponse()
    
    def ReadAllDocuments(self, request, context):
        try:
            if not self.manager.current_db:
                raise ValueError("No database selected")
            docs = self.manager.current_db.read_all_documents()
            return database_pb2.DocumentList(documents=[json.dumps(doc) for doc in docs])
        except Exception as e:
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(str(e))
            return database_pb2.DocumentList()
    
    def QueryDocuments(self, request, context):
        try:
            if not self.manager.current_db:
                raise ValueError("No database selected")
            filter_func = eval(request.filter_expr)
            if not callable(filter_func):
                raise ValueError("Filter expression must be callable")
            docs = self.manager.current_db.query_documents(filter_func)
            return database_pb2.DocumentList(documents=[json.dumps(doc) for doc in docs])
        except Exception as e:
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            context.set_details(str(e))
            return database_pb2.DocumentList()
    
    def UpdateDocument(self, request, context):
        try:
            if not self.manager.current_db:
                raise ValueError("No database selected")
            updates = json.loads(request.updates)
            success = self.manager.current_db.update_document(
                request.doc_id,
                updates,
                request.create_if_missing
            )
            return database_pb2.OperationResponse(
                success=success,
                message="Document updated" if success else "Document not found"
            )
        except Exception as e:
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            context.set_details(str(e))
            return database_pb2.OperationResponse(success=False, message=str(e))
    
    def DeleteDocument(self, request, context):
        try:
            if not self.manager.current_db:
                raise ValueError("No database selected")
            success = self.manager.current_db.delete_document(request.doc_id)
            return database_pb2.OperationResponse(
                success=success,
                message="Document deleted" if success else "Document not found"
            )
        except Exception as e:
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(str(e))
            return database_pb2.OperationResponse(success=False, message=str(e))
    
    def ClearDatabase(self, request, context):
        try:
            if not self.manager.current_db:
                raise ValueError("No database selected")
            self.manager.current_db.clear_database()
            return database_pb2.OperationResponse(success=True, message="Database cleared")
        except Exception as e:
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(str(e))
            return database_pb2.OperationResponse(success=False, message=str(e))

def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    database_pb2_grpc.add_DatabaseServiceServicer_to_server(DatabaseService(), server)
    server.add_insecure_port('[::]:50051')
    server.start()
    print("Worker node running on port 50051")
    server.wait_for_termination()

if __name__ == '__main__':
    serve()
