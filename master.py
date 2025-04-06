import grpc
import json
import database_pb2
import database_pb2_grpc

class DatabaseClient:
    def __init__(self):
        self.channel = grpc.insecure_channel('localhost:50051')
        self.stub = database_pb2_grpc.DatabaseServiceStub(self.channel)
    
    def create_database(self, name):
        return self.stub.CreateDatabase(database_pb2.DatabaseName(name=name))
    
    def use_database(self, name):
        return self.stub.UseDatabase(database_pb2.DatabaseName(name=name))
    
    def list_databases(self):
        return self.stub.ListDatabases(database_pb2.Empty())
    
    def delete_database(self, name):
        return self.stub.DeleteDatabase(database_pb2.DatabaseName(name=name))
    
    def create_document(self, document, doc_id=None):
        return self.stub.CreateDocument(database_pb2.DocumentRequest(
            document=json.dumps(document),
            doc_id=doc_id
        ))
    
    def read_document(self, doc_id):
        return self.stub.ReadDocument(database_pb2.DocumentID(doc_id=doc_id))
    
    def read_all_documents(self):
        return self.stub.ReadAllDocuments(database_pb2.DatabaseName(name=""))
    
    def query_documents(self, filter_expr):
        return self.stub.QueryDocuments(database_pb2.QueryRequest(filter_expr=filter_expr))
    
    def update_document(self, doc_id, updates, create_if_missing=False):
        return self.stub.UpdateDocument(database_pb2.UpdateRequest(
            doc_id=doc_id,
            updates=json.dumps(updates),
            create_if_missing=create_if_missing
        ))
    
    def delete_document(self, doc_id):
        return self.stub.DeleteDocument(database_pb2.DocumentID(doc_id=doc_id))
    
    def clear_database(self):
        return self.stub.ClearDatabase(database_pb2.DatabaseName(name=""))

def print_document(doc):
    print(json.dumps(json.loads(doc), indent=2))

def main():
    client = DatabaseClient()
    
    print("Master Node - Distributed JSON Database CLI")
    print("Database commands: create_db, use_db, list_db, delete_db")
    print("Document commands: create, read, read_all, query, update, delete, clear")
    print("Other commands: exit")
    
    while True:
        command = input("\nEnter command: ").strip().lower()
        
        try:
            if command == "exit":
                print("Exiting...")
                break
            
            elif command == "create_db":
                db_name = input("Enter new database name: ").strip()
                response = client.create_database(db_name)
                print(response.message)
            
            elif command == "use_db":
                db_name = input("Enter database name to use: ").strip()
                response = client.use_database(db_name)
                print(response.message)
            
            elif command == "list_db":
                response = client.list_databases()
                if response.names:
                    print("Available databases:")
                    for name in response.names:
                        print(f"- {name}")
                else:
                    print("No databases available")
            
            elif command == "delete_db":
                db_name = input("Enter database name to delete: ").strip()
                response = client.delete_database(db_name)
                print(response.message)
            
            elif command == "create":
                print("Enter document as JSON:")
                doc_input = input("> ").strip()
                try:
                    doc = json.loads(doc_input)
                    doc_id = input("Enter custom ID (leave empty to generate): ").strip() or None
                    response = client.create_document(doc, doc_id)
                    print(f"Document created with ID: {response.doc_id}")
                except json.JSONDecodeError as e:
                    print(f"Invalid JSON: {e}")
            
            elif command == "read":
                doc_id = input("Enter document ID: ").strip()
                response = client.read_document(doc_id)
                if response.document:
                    print_document(response.document)
                else:
                    print("Document not found")
            
            elif command == "read_all":
                response = client.read_all_documents()
                if response.documents:
                    print(f"Found {len(response.documents)} documents:")
                    for doc in response.documents:
                        print_document(doc)
                        print("-" * 40)
                else:
                    print("No documents found")
            
            elif command == "query":
                print("Enter filter function as Python lambda:")
                print("Example: lambda doc: doc['age'] > 30")
                filter_expr = input("> ").strip()
                response = client.query_documents(filter_expr)
                if response.documents:
                    print(f"Found {len(response.documents)} matching documents:")
                    for doc in response.documents:
                        print_document(doc)
                        print("-" * 40)
                else:
                    print("No matching documents found")
            
            elif command == "update":
                doc_id = input("Enter document ID to update: ").strip()
                print("Enter updates as JSON:")
                updates_input = input("> ").strip()
                try:
                    updates = json.loads(updates_input)
                    response = client.update_document(doc_id, updates)
                    print("Update successful" if response.success else "Update failed")
                except json.JSONDecodeError as e:
                    print(f"Invalid JSON: {e}")
            
            elif command == "delete":
                doc_id = input("Enter document ID to delete: ").strip()
                response = client.delete_document(doc_id)
                print("Delete successful" if response.success else "Delete failed")
            
            elif command == "clear":
                response = client.clear_database()
                print("Database cleared" if response.success else "Clear failed")
            
            else:
                print("Unknown command")
        
        except Exception as e:
            print(f"Error: {e}")

if __name__ == '__main__':
    main()
