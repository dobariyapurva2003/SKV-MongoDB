from datetime import datetime
import random
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
from typing import Dict, List, Optional, Tuple
from enum import Enum
import os

START_FLAG_FILE = "starting_flag.txt"

def check_starting():
    if os.path.exists(START_FLAG_FILE):
        return False
    else:
        with open(START_FLAG_FILE, "w") as f:
            f.write("started")
        return True

class RaftRole(Enum):
    Follower = 1
    Candidate = 2
    Leader = 3

class RaftState:
    def __init__(self, role: RaftRole = RaftRole.Follower):
        self.role = role
        self.leader_id = None
        self.current_term = 0
        self.voted_for = None
        self.election_timeout = random.uniform(1.5, 3.0)
        self.last_heartbeat = time.time()

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
        self.replica_count = 0
        self.last_heartbeat = time.time()

class RaftNode:
    def __init__(self, node_id: str, addr: str, peers: Dict[str, str],
                 on_leader_change=None, on_state_change=None, on_apply=None):
        self.node_id = node_id
        self.addr = addr  # Raft port, not used for gRPC directly
        self.peers = peers  # Now maps to service ports
        self.heartbeat_interval = 5.0  # Leader sends heartbeats every 5s
        self.election_timeout_min = 8.0  # Minimum election timeout
        self.election_timeout_max = 12.0  # Maximum election timeout
        self.rpc_timeout = 3.0  # Timeout for RPC calls
        # Leader stability checks
        self.min_leader_stability = 15.0  # Minimum time to remain leader
        self.last_leadership_change = 0
        self.reset_election_timeout()

        self.peer_channels = {
            peer_id: grpc.insecure_channel(peer_addr)
            for peer_id, peer_addr in peers.items()
        }
        self.peer_stubs = {
            peer_id: database_pb2_grpc.DatabaseServiceStub(channel)
            for peer_id, channel in self.peer_channels.items()
        }
        self.on_leader_change = on_leader_change
        self.on_state_change = on_state_change
        self.on_apply = on_apply
        self.state = RaftState(RaftRole.Follower)
        self.current_term = 0
        self.voted_for = None
        self.leader_id = None
        self.last_heartbeat = time.time()
        self.election_timeout = random.uniform(5.5, 8.0)
        self.stop_flag = False
        self.lock = threading.Lock()

    def reset_election_timeout(self):
        """Set a new random election timeout"""
        self.election_timeout = random.uniform(
            self.election_timeout_min, 
            self.election_timeout_max
        )

    def run(self):
        """Raft event loop"""
        while not self.stop_flag:
            time.sleep(0.1)
            current_time = time.time()
            
            
            if self.state.role == RaftRole.Follower:
                    if current_time - self.state.last_heartbeat > self.state.election_timeout:
                        self._become_candidate()
                
            elif self.state.role == RaftRole.Candidate:
                    if current_time - self.state.last_heartbeat > self.state.election_timeout:
                        self._start_election()
                
            elif self.state.role == RaftRole.Leader:
                    if current_time - self.state.last_heartbeat >  self.heartbeat_interval:
                        self._send_heartbeats()
                        self.state.last_heartbeat = current_time

    def _become_candidate(self):
        """Transition to candidate state and start election"""
        # Only start an election if no leader has been seen recently
        if time.time() - self.state.last_heartbeat < self.election_timeout_min:
            return
        
        self.state.role = RaftRole.Candidate
        self.state.current_term += 1
        self.state.voted_for = self.node_id
        self.state.last_heartbeat = time.time()
        self.state.election_timeout = random.uniform(1.5, 3.0)
            
        if self.on_state_change:
                self.on_state_change(self.state)
            
        logger.info(f"Node {self.node_id} becoming candidate in term {self.state.current_term}")
        self._start_election()

    def _start_election(self):
        """Proper election with RequestVote RPCs"""
        # Require 2/3 of cluster to respond (not just majority)
        required_votes = (len(self.peers) * 2 // 3) + 1
        votes = 1  # Vote for self
        total_nodes = len(self.peers) + 1
            
        if not self.peers:
                logger.info(f"Node {self.node_id} is the only node, becoming leader")
                self._become_leader()
                return
            
        for peer_id, stub in self.peer_stubs.items():
                try:
                    response = stub.RequestVote(
                        database_pb2.VoteRequest(
                            term=self.state.current_term,
                            candidate_id=self.node_id,
                            last_log_index=0,
                            last_log_term=0
                        ),
                        timeout=self.rpc_timeout
                    )
                    
                    if response.term > self.state.current_term:
                        self.state.current_term = response.term
                        self.state.role = RaftRole.Follower
                        self.state.voted_for = None
                        self.state.last_heartbeat = time.time()
                        logger.info(f"Node {self.node_id} stepping down to follower due to higher term {response.term}")
                        return
                    
                    if response.vote_granted:
                        votes += 1
                        logger.info(f"Node {self.node_id} received vote from {peer_id}")
                except Exception as e:
                    logger.warning(f"Failed to get vote from {peer_id}: {str(e)}")
                    continue
            
        if votes >= required_votes:
                logger.info(f"Node {self.node_id} received majority votes ({votes}/{total_nodes})")
                self._become_leader()
        else:
                logger.info(f"Node {self.node_id} did not receive majority votes ({votes}/{total_nodes}), remaining candidate")
                self.state.last_heartbeat = time.time()

    def _become_leader(self):
        """Transition to leader state"""
        if time.time() - self.last_leadership_change < self.min_leader_stability:
            logger.warning(f"Leadership change too frequent! Remaining candidate.")
            return
        self.state.role = RaftRole.Leader
        self.state.leader_id = self.node_id
        self.state.last_heartbeat = time.time()
            
        if self.on_leader_change:
                self.on_leader_change(self.node_id)
        if self.on_state_change:
                self.on_state_change(self.state)
            
        logger.info(f"Node {self.node_id} elected as leader for term {self.state.current_term}")
        self._send_heartbeats()

    def _send_heartbeats(self):
        successful_responses = 0
        responses = []
        logger.info(f"Node {self.node_id} sending heartbeats as leader for term {self.state.current_term}")
        for peer_id, stub in self.peer_stubs.items():
            try:
                response = stub.AppendEntries(
                    database_pb2.AppendEntriesRequest(
                        term=self.state.current_term,
                        leader_id=self.node_id,
                        prev_log_index=0,
                        prev_log_term=0,
                        entries=[],
                        leader_commit=0
                    ),
                    timeout=self.rpc_timeout
                )
                if response.success:
                    successful_responses += 1
                responses.append(response.success)
                #logger.info(f"Heartbeat sent to {peer_id}, response: success={response.success}")
            except Exception as e:
                responses.append(False)
                logger.warning(f"Failed to send heartbeat to {peer_id}: {str(e)}")
                continue
        # # Step down if less than 50% of peers respond
        # if sum(responses) < len(self.peers) / 2:
        #     self._step_down()

        # If less than half of peers respond, step down
        if successful_responses < len(self.peers) / 2:
            self._step_down()

    def _step_down(self):
        self.state.role = RaftRole.Follower
        self.state.leader_id = None
        self.reset_election_timeout()
        logger.info(f"Leader {self.node_id} stepping down due to lack of responses")


    def propose(self, data: bytes):
        future = futures.Future()
        
        if self.state.role == RaftRole.Leader:
                if self.on_apply:
                    self.on_apply(data)
                future.set_result(True)
        else:
                future.set_result(False)
        return future

    def stop(self):
        self.stop_flag = True

    def get_leader_addr(self):
        
        if self.state.leader_id:
                if self.state.leader_id == self.node_id:
                    # Use service port for leader address
                    return self.addr.replace(str(self.addr.split(':')[-1]), str(self.service_port))
                return self.peers.get(self.state.leader_id, "")
        return None

class MasterNode:
    def __init__(self, node_id: str, raft_addr: str, peers: Dict[str, str], service_port: int):
        self.node_id = node_id
        self.service_port = service_port  # Store service port for get_leader_addr
        self.workers: Dict[str, WorkerNode] = {}
        self.database_assignments: Dict[str, str] = {}
        self.document_shards: Dict[str, Dict[str, str]] = {}
        self.lock = threading.Lock()
        
        self.raft_node = RaftNode(
            node_id=node_id,
            addr=raft_addr,
            peers=peers,
            on_leader_change=self._on_leader_change,
            on_state_change=self._on_state_change,
            on_apply=self._on_apply
        )
        self.raft_node.service_port = service_port  # Pass service port to RaftNode
        
        self.health_thread = threading.Thread(target=self._health_check, daemon=True)
        self.health_thread.start()
        self.raft_thread = threading.Thread(target=self._run_raft_loop, daemon=True)
        self.raft_thread.start()

    def _run_raft_loop(self):
        self.raft_node.run()

    def _on_leader_change(self, new_leader_id: Optional[str]):
        if new_leader_id:
            logger.info(f"New leader elected: {new_leader_id}")
        else:
            logger.warning("No leader currently available")

    def _on_state_change(self, new_state: RaftState):
        logger.info(f"Node state changed to: {new_state.role}")

    def is_leader(self) -> bool:
        """Check if this node is currently the leader"""
        return self.raft_node.state.role == RaftRole.Leader

    def _on_apply(self, data: bytes):
        try:
            operation_data = json.loads(data.decode())
            self.apply_operation(operation_data['op'], operation_data['args'])
        except Exception as e:
            logger.error(f"Failed to apply operation: {str(e)}")

    # def is_leader(self) -> bool:
    #     return self.raft_node.state.role == RaftRole.Leader

    def _replicate_operation(self, operation: str, *args) -> bool:
        if not self.is_leader():
            return False
        
        operation_data = json.dumps({
            'op': operation,
            'args': args,
            'timestamp': time.time()
        })
        
        try:
            future = self.raft_node.propose(operation_data.encode())
            return future.result(timeout=2.0)
        except Exception as e:
            logger.error(f"Failed to replicate operation: {str(e)}")
            return False

    def apply_operation(self, operation: str, args: list):
        
            if operation == "add_worker":
                address = args[0]
                if address not in self.workers:
                    self.workers[address] = WorkerNode(address)
                    logger.info(f"Added worker {address} (replicated)")
            
            elif operation == "remove_worker":
                address = args[0]
                if address in self.workers:
                    del self.workers[address]
                    logger.info(f"Removed worker {address} (replicated)")
            
            elif operation == "assign_database":
                db_name, worker_address = args
                if worker_address in self.workers:
                    self.database_assignments[db_name] = worker_address
                    self.workers[worker_address].load += 1
                    logger.info(f"Assigned database {db_name} to {worker_address} (replicated)")
            
            elif operation == "unassign_database":
                db_name = args[0]
                if db_name in self.database_assignments:
                    worker_address = self.database_assignments[db_name]
                    if worker_address in self.workers:
                        self.workers[worker_address].load -= 1
                    del self.database_assignments[db_name]
                    logger.info(f"Unassigned database {db_name} (replicated)")
            
            elif operation == "assign_document":
                db_name, doc_id, worker_address = args
                if db_name not in self.document_shards:
                    self.document_shards[db_name] = {}
                self.document_shards[db_name][doc_id] = worker_address
                logger.info(f"Assigned document {doc_id} to {worker_address} (replicated)")
    def add_worker(self, address: str) -> bool:
        # Normalize worker address
        if not address.startswith("localhost:"):
            address = f"localhost:{address.split(':')[-1]}"
        
        with self.lock:
            if address not in self.workers:
                self.workers[address] = WorkerNode(address)
                logger.info(f"Added new worker: {address}")
                if self.is_leader():
                    return self._replicate_operation("add_worker", address)
                else:
                    leader_addr = self.raft_node.get_leader_addr()
                    if leader_addr:
                        try:
                            channel = grpc.insecure_channel(leader_addr)
                            stub = database_pb2_grpc.DatabaseServiceStub(channel)
                            return stub.AddWorker(database_pb2.Worker(worker=address)).success
                        except:
                            pass
            else:
                # Update existing worker
                self.workers[address].last_heartbeat = time.time()
                self.workers[address].health = True
            return False
        
    def remove_worker(self, address: str) -> bool:
        
        if address in self.workers:
                if self.is_leader():
                    return self._replicate_operation("remove_worker", address)
                else:
                    leader_addr = self.raft_node.get_leader_addr()
                    if leader_addr:
                        try:
                            channel = grpc.insecure_channel(leader_addr)
                            stub = database_pb2_grpc.DatabaseServiceStub(channel)
                            return stub.RemoveWorker(database_pb2.Worker(worker=address)).success
                        except:
                            pass
        return False

    def assign_database(self, db_name: str, worker_address: str) -> bool:
        
        if worker_address in self.workers:
                if self.is_leader():
                    return self._replicate_operation("assign_database", db_name, worker_address)
                else:
                    leader_addr = self.raft_node.get_leader_addr()
                    if leader_addr:
                        try:
                            channel = grpc.insecure_channel(leader_addr)
                            stub = database_pb2_grpc.DatabaseServiceStub(channel)
                            return stub.AssignDatabase(
                                database_pb2.DatabaseAssignment(
                                    db_name=db_name,
                                    worker_address=worker_address
                                )
                            ).success
                        except:
                            pass
        return False

    def unassign_database(self, db_name: str) -> bool:
        
        if db_name in self.database_assignments:
                if self.is_leader():
                    return self._replicate_operation("unassign_database", db_name)
                else:
                    leader_addr = self.raft_node.get_leader_addr()
                    if leader_addr:
                        try:
                            channel = grpc.insecure_channel(leader_addr)
                            stub = database_pb2_grpc.DatabaseServiceStub(channel)
                            return stub.UnassignDatabase(
                                database_pb2.DatabaseName(name=db_name)
                            ).success
                        except:
                            pass
        return False

    def assign_document(self, db_name: str, doc_id: str, worker_address: str) -> bool:
        
        if worker_address in self.workers:
                if self.is_leader():
                    return self._replicate_operation("assign_document", db_name, doc_id, worker_address)
                else:
                    leader_addr = self.raft_node.get_leader_addr()
                    if leader_addr:
                        try:
                            channel = grpc.insecure_channel(leader_addr)
                            stub = database_pb2_grpc.DatabaseServiceStub(channel)
                            return stub.AssignDocument(
                                database_pb2.DocumentAssignment(
                                    db_name=db_name,
                                    doc_id=doc_id,
                                    worker_address=worker_address
                                )
                            ).success
                        except:
                            pass
        return False

    def get_primary_worker(self, db_name: str) -> Optional[WorkerNode]:
        
            return self.workers.get(self.database_assignments.get(db_name))

    def get_document_worker(self, db_name: str, doc_id: str) -> Optional[str]:
        
            if db_name not in self.document_shards:
                self.document_shards[db_name] = {}
            
            if doc_id not in self.document_shards[db_name]:
                if not self.workers:
                    return None
                
                workers = sorted(self.workers.keys())
                hash_val = int(hashlib.md5(doc_id.encode()).hexdigest(), 16)
                worker_idx = hash_val % len(workers)
                worker_address = workers[worker_idx]
                
                if not self.assign_document(db_name, doc_id, worker_address):
                    return None
            
            return self.document_shards[db_name].get(doc_id)

    def get_document_replicas(self, db_name: str, doc_id: str) -> List[str]:
        
            primary_worker = self.get_primary_worker(db_name)
            primary_address = primary_worker.address if primary_worker else None
            workers = [
                w.address for w in self.workers.values()
                if w.health and w.address != primary_address
            ]
            if len(workers) < 1:
                return []
            replica_count = min(3, len(workers))
            workers_sorted = sorted(workers, key=lambda addr: self.workers[addr].replica_count)
            selected = workers_sorted[:replica_count]
            for addr in selected:
                self.workers[addr].replica_count += 1
            return selected

    def decrement_replica_count(self, worker_address: str):
        
            if worker_address in self.workers:
                self.workers[worker_address].replica_count = max(
                    0, self.workers[worker_address].replica_count - 1
                )

    def _health_check(self):
        while True:
            time.sleep(5)
            current_time = time.time()
            
            with self.lock:
                # Mark unresponsive workers
                for address, worker in list(self.workers.items()):
                    if current_time - worker.last_heartbeat > 15:
                        logger.warning(f"Worker {address} marked unhealthy")
                        worker.health = False
                    
                    # Verify worker responsiveness
                    try:
                        worker.stub.ListDatabases(database_pb2.Empty(), timeout=2)
                        worker.health = True
                        worker.last_heartbeat = current_time
                    except:
                        worker.health = False

class MasterService(database_pb2_grpc.DatabaseServiceServicer):
    def __init__(self, master_node: MasterNode):
        self.master = master_node

    def GetLeader(self, request, context):
        leader_id = self.master.raft_node.state.leader_id
        leader_addr = self.master.raft_node.get_leader_addr()
        return database_pb2.LeaderInfo(
            leader_id=leader_id or "",
            leader_address=leader_addr or ""
        )

    def is_leader(self):
        return self.master.raft_node.state.role == RaftRole.Leader
    
    def Heartbeat(self, request, context):
        try:
            if not self.is_leader():
                logger.info(f"Rejecting heartbeat from {request.worker_address} - not leader")
                context.set_code(grpc.StatusCode.FAILED_PRECONDITION)
                return database_pb2.HeartbeatResponse(acknowledged=False)
            logger.info(f"Received heartbeat from worker {request.worker_address}")
            response = database_pb2.HeartbeatResponse(acknowledged=True)
            return response
        except Exception as e:
            logger.error(f"Failed to process heartbeat: {str(e)}", exc_info=True)
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(f"Server error: {str(e)}")
            raise  # Let gRPC handle the error properly

    def RequestVote(self, request, context):
        with self.master.raft_node.lock:
            if request.term < self.master.raft_node.state.current_term:
                return database_pb2.VoteResponse(
                    term=self.master.raft_node.state.current_term,
                    vote_granted=False
                )
            
            if request.term > self.master.raft_node.state.current_term:
                self.master.raft_node.state.current_term = request.term
                self.master.raft_node.state.role = RaftRole.Follower
                self.master.raft_node.state.voted_for = None
            
            if (self.master.raft_node.state.voted_for is None or 
                self.master.raft_node.state.voted_for == request.candidate_id):
                self.master.raft_node.state.voted_for = request.candidate_id
                self.master.raft_node.state.last_heartbeat = time.time()
                return database_pb2.VoteResponse(
                    term=self.master.raft_node.state.current_term,
                    vote_granted=True
                )
            
            return database_pb2.VoteResponse(
                term=self.master.raft_node.state.current_term,
                vote_granted=False
            )

    def AppendEntries(self, request, context):
            
            if request.term < self.master.raft_node.state.current_term:
                return database_pb2.AppendEntriesResponse(
                    term=self.master.raft_node.state.current_term,
                    success=False
                )
            
            if request.term > self.master.raft_node.state.current_term:
                self.master.raft_node.state.current_term = request.term
                self.master.raft_node.state.role = RaftRole.Follower
                self.master.raft_node.state.voted_for = None
                self._step_down()
            
            self.master.raft_node.state.leader_id = request.leader_id
            self.master.raft_node.state.last_heartbeat = time.time()
            
            
            return database_pb2.AppendEntriesResponse(
                term=self.master.raft_node.state.current_term,
                success=True
            )

    def AddWorker(self, request, context):
        if not self.master.is_leader():
            context.set_code(grpc.StatusCode.FAILED_PRECONDITION)
            return database_pb2.OperationResponse(success=False, message="Not the leader")
        success = self.master.add_worker(request.worker)
        return database_pb2.OperationResponse(
            success=success,
            message="Worker registered successfully" if success else "Worker registration failed"
        )

    def RemoveWorker(self, request, context):
        if not self.master.is_leader():
            context.set_code(grpc.StatusCode.FAILED_PRECONDITION)
            return database_pb2.OperationResponse(success=False, message="Not the leader")
        
        success = self.master.remove_worker(request.worker)
        return database_pb2.OperationResponse(
            success=success,
            message="Worker removed successfully" if success else "Worker removal failed"
        )

    def ListWorkers(self, request, context):
        with self.master.lock:
            workers = list(self.master.workers.keys())
        return database_pb2.WorkerList(workers=workers)

    def AssignDatabase(self, request, context):
        if not self.master.is_leader():
            context.set_code(grpc.StatusCode.FAILED_PRECONDITION)
            return database_pb2.OperationResponse(success=False, message="Not the leader")
        
        success = self.master.assign_database(request.db_name, request.worker_address)
        return database_pb2.OperationResponse(
            success=success,
            message="Database assigned successfully" if success else "Database assignment failed"
        )

    def UnassignDatabase(self, request, context):
        if not self.master.is_leader():
            context.set_code(grpc.StatusCode.FAILED_PRECONDITION)
            return database_pb2.OperationResponse(success=False, message="Not the leader")
        
        success = self.master.unassign_database(request.name)
        return database_pb2.OperationResponse(
            success=success,
            message="Database unassigned successfully" if success else "Database unassignment failed"
        )

    def AssignDocument(self, request, context):
        if not self.master.is_leader():
            context.set_code(grpc.StatusCode.FAILED_PRECONDITION)
            return database_pb2.OperationResponse(success=False, message="Not the leader")
        
        success = self.master.assign_document(request.db_name, request.doc_id, request.worker_address)
        return database_pb2.OperationResponse(
            success=success,
            message="Document assigned successfully" if success else "Document assignment failed"
        )

    def GetDocumentReplicas(self, request, context):
        try:
            replicas = self.master.get_document_replicas(request.db_name, request.doc_id)
            # Return WorkerList with the replica addresses
            return database_pb2.WorkerList(workers=replicas)
        except Exception as e:
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(str(e))
            return database_pb2.WorkerList()

    def GetDocumentPrimary(self, request, context):
        """Get the primary worker for a specific document"""
        worker_addr = self.master.get_document_worker(request.db_name, request.doc_id)
        if not worker_addr:
            context.set_code(grpc.StatusCode.NOT_FOUND)
            return database_pb2.Worker()
        
        try:
            # Verify the document exists and is marked as primary
            channel = grpc.insecure_channel(worker_addr)
            stub = database_pb2_grpc.DatabaseServiceStub(channel)
            doc_response = stub.ReadDocument(request)
            if doc_response.document:
                doc = json.loads(doc_response.document)
                if doc.get('_primary', False):
                    return database_pb2.Worker(worker=worker_addr)
        except:
            pass
        
        # If no primary found, select one
        return self.SetDocumentPrimary(
            database_pb2.SetPrimaryRequest(
                db_name=request.db_name,
                doc_id=request.doc_id,
                worker_address=worker_addr
            ),
            context
        )

    def SetDocumentPrimary(self, request, context):
        """Set a specific worker as primary for a document"""
        try:
            channel = grpc.insecure_channel(request.worker_address)
            stub = database_pb2_grpc.DatabaseServiceStub(channel)
            
            # Get current document
            doc_response = stub.ReadDocument(database_pb2.DocumentID(
                db_name=request.db_name,
                doc_id=request.doc_id
            ))
            
            if not doc_response.document:
                return database_pb2.OperationResponse(
                    success=False,
                    message="Document not found on target worker"
                )
            
            # Update document to mark as primary
            doc = json.loads(doc_response.document)
            doc['_primary'] = True
            doc['_updated_at'] = datetime.now().isoformat()
            
            # Update on primary worker
            stub.ReplicateDocument(database_pb2.ReplicateRequest(
                db_name=request.db_name,
                doc_id=request.doc_id,
                document=json.dumps(doc)
            ))
            
            # Get replicas and update them to mark as non-primary
            replicas = self.GetDocumentReplicas(database_pb2.DocumentID(
                db_name=request.db_name,
                doc_id=request.doc_id
            )).workers
            
            for replica in replicas:
                if replica != request.worker_address:
                    try:
                        channel = grpc.insecure_channel(replica)
                        stub = database_pb2_grpc.DatabaseServiceStub(channel)
                        doc['_primary'] = False
                        stub.ReplicateDocument(database_pb2.ReplicateRequest(
                            db_name=request.db_name,
                            doc_id=request.doc_id,
                            document=json.dumps(doc)
                        ))
                    except:
                        continue
            
            return database_pb2.OperationResponse(
                success=True,
                message=f"Worker {request.worker_address} set as primary for document {request.doc_id}"
            )
        except Exception as e:
            return database_pb2.OperationResponse(
                success=False,
                message=str(e)
            )
    
    def CreateDatabase(self, request, context):
        if not self.master.is_leader():
            leader_addr = self.master.raft_node.get_leader_addr()
            if leader_addr:
                try:
                    channel = grpc.insecure_channel(leader_addr)
                    stub = database_pb2_grpc.DatabaseServiceStub(channel)
                    logger.info(f"Forwarding CreateDatabase({request.name}) to leader at {leader_addr}")
                    return stub.CreateDatabase(request)
                except Exception as e:
                    logger.error(f"Failed to forward CreateDatabase to leader: {str(e)}")
                    context.set_code(grpc.StatusCode.UNAVAILABLE)
                    return database_pb2.OperationResponse(
                        success=False,
                        message=f"Failed to forward to leader: {str(e)}"
                    )
            logger.warning("No leader available for CreateDatabase")
            context.set_code(grpc.StatusCode.UNAVAILABLE)
            return database_pb2.OperationResponse(success=False, message="No leader available")

        logger.info(f"Processing CreateDatabase({request.name}) as leader")
        # Wait briefly for worker registration if needed
        start_time = time.time()
        while time.time() - start_time < 5.0:  # 5-second timeout
            worker = self._select_worker_for_db(request.name)
            if worker:
                break
            time.sleep(0.5)
        #worker = self._select_worker_for_db(request.name)
        if not worker:
            logger.error("No workers available for database creation")
            context.set_code(grpc.StatusCode.UNAVAILABLE)
            return database_pb2.OperationResponse(success=False, message="No workers available")

        if not self.master.assign_database(request.name, worker.address):
            logger.error(f"Failed to assign database {request.name} to {worker.address}")
            context.set_code(grpc.StatusCode.INTERNAL)
            return database_pb2.OperationResponse(success=False, message="Failed to assign database")

        logger.info(f"Assigned database {request.name} to worker {worker.address}")
        try:
            response = worker.stub.CreateDatabase(request)
            logger.info(f"Worker {worker.address} created database {request.name}: {response.message}")
            return response
        except Exception as e:
            logger.error(f"Worker {worker.address} failed to create database {request.name}: {str(e)}")
            context.set_code(grpc.StatusCode.INTERNAL)
            return database_pb2.OperationResponse(success=False, message=f"Worker error: {str(e)}")
    
    def _select_worker_for_db(self, db_name: str) -> Optional[WorkerNode]:
        # Get all healthy workers
        healthy_workers = [w for w in self.master.workers.values() if w.health]
        print(f"healthy worker : " , healthy_workers)
        if not healthy_workers:
            return None
        
        # Prefer workers with no existing databases
        unassigned_workers = [
            w for w in healthy_workers
            if w.address not in self.master.database_assignments.values()
        ]
        
        workers_to_consider = unassigned_workers or healthy_workers
        return min(workers_to_consider, key=lambda w: w.load)

    def GetPrimaryWorker(self, request, context):
        worker = self.master.get_primary_worker(request.name)
        if not worker:
            context.set_code(grpc.StatusCode.NOT_FOUND)
            return database_pb2.Worker()
        return database_pb2.Worker(worker=worker.address)

    def GetDocumentPrimary(self, request, context):
        worker_addr = self.master.get_document_worker(request.db_name, request.doc_id)
        if not worker_addr:
            context.set_code(grpc.StatusCode.NOT_FOUND)
            return database_pb2.Worker()
        
        try:
            channel = grpc.insecure_channel(worker_addr)
            stub = database_pb2_grpc.DatabaseServiceStub(channel)
            doc_response = stub.ReadDocument(request)
            if doc_response.document:
                doc = json.loads(doc_response.document)
                if doc.get('_primary', False):
                    return database_pb2.Worker(worker=worker_addr)
        except:
            pass
        
        return self.SetDocumentPrimary(
            database_pb2.SetPrimaryRequest(
                db_name=request.db_name,
                doc_id=request.doc_id,
                worker_address=worker_addr
            ),
            context
        )

    def SetDocumentPrimary(self, request, context):
        try:
            channel = grpc.insecure_channel(request.worker_address)
            stub = database_pb2_grpc.DatabaseServiceStub(channel)
            doc_response = stub.ReadDocument(database_pb2.DocumentID(
                db_name=request.db_name,
                doc_id=request.doc_id
            ))
            if not doc_response.document:
                return database_pb2.OperationResponse(
                    success=False,
                    message="Document not found on target worker"
                )
            doc = json.loads(doc_response.document)
            doc['_primary'] = True
            doc['_updated_at'] = datetime.now().isoformat()
            stub.ReplicateDocument(database_pb2.ReplicateRequest(
                db_name=request.db_name,
                doc_id=request.doc_id,
                document=json.dumps(doc)
            ))
            replicas = self.GetDocumentReplicas(database_pb2.DocumentID(
                db_name=request.db_name,
                doc_id=request.doc_id
            )).workers
            for replica in replicas:
                if replica != request.worker_address:
                    try:
                        channel = grpc.insecure_channel(replica)
                        stub = database_pb2_grpc.DatabaseServiceStub(channel)
                        doc['_primary'] = False
                        stub.ReplicateDocument(database_pb2.ReplicateRequest(
                            db_name=request.db_name,
                            doc_id=request.doc_id,
                            document=json.dumps(doc)
                        ))
                    except:
                        continue
            return database_pb2.OperationResponse(
                success=True,
                message=f"Worker {request.worker_address} set as primary for document {request.doc_id}"
            )
        except Exception as e:
            return database_pb2.OperationResponse(success=False, message=str(e))

    def UseDatabase(self, request, context):
        worker = self.master.get_primary_worker(request.name)
        if not worker:
            context.set_code(grpc.StatusCode.NOT_FOUND)
            return database_pb2.OperationResponse(success=False, message="Database not found")
        return worker.stub.UseDatabase(request)

    def ListDatabases(self, request, context):
            return database_pb2.DatabaseList(names=list(self.master.database_assignments.keys()))

    def DeleteDatabase(self, request, context):
        worker = self.master.get_primary_worker(request.name)
        if not worker:
            context.set_code(grpc.StatusCode.NOT_FOUND)
            return database_pb2.OperationResponse(success=False, message="Database not found")
        try:
            response = worker.stub.DeleteDatabase(request)
            if response.success:
                self.master.unassign_database(request.name)
            return response
        except Exception as e:
            context.set_code(grpc.StatusCode.INTERNAL)
            return database_pb2.OperationResponse(success=False, message=str(e))

    def CreateDocument(self, request, context):
        worker = self.master.get_primary_worker(request.db_name)
        if not worker:
            context.set_code(grpc.StatusCode.NOT_FOUND)
            return database_pb2.DocumentID()
        try:
            response = worker.stub.CreateDocument(request)
            if response.doc_id:
                replicas = self.master.get_document_replicas(request.db_name, response.doc_id)
                for replica_addr in replicas:
                    try:
                        replica_worker = self.master.workers[replica_addr]
                        replica_worker.stub.ReplicateDocument(
                            database_pb2.ReplicateRequest(
                                db_name=request.db_name,
                                doc_id=response.doc_id,
                                document=request.document
                            ),
                            timeout=3
                        )
                    except Exception as e:
                        logger.error(f"Failed to replicate to {replica_addr}: {str(e)}")
                        self.master.decrement_replica_count(replica_addr)
            return response
        except grpc.RpcError as e:
            context.set_code(e.code())
            context.set_details(e.details())
            return database_pb2.DocumentID()
        except Exception as e:
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(str(e))
            return database_pb2.DocumentID()

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
        worker = self.master.get_primary_worker(request.db_name)
        if not worker:
            context.set_code(grpc.StatusCode.NOT_FOUND)
            return database_pb2.OperationResponse(success=False, message="Document not found")
        try:
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

def serve_master(node_id: str, raft_port: int, service_port: int, peers: Dict[str, str]):
    raft_addr = f"localhost:{raft_port}"
    master_node = MasterNode(
        node_id=node_id,
        raft_addr=raft_addr,
        peers=peers,
        service_port=service_port
    )
    
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    database_pb2_grpc.add_DatabaseServiceServicer_to_server(
        MasterService(master_node), server
    )
    server.add_insecure_port(f'[::]:{service_port}')
    server.start()
    
    logger.info(f"Master node {node_id} running:")
    logger.info(f"- Raft port: {raft_port}")
    logger.info(f"- Service port: {service_port}")
    logger.info(f"- Peers: {peers}")
    
    try:
        server.wait_for_termination()
    except KeyboardInterrupt:
        logger.info("Shutting down master node")
        master_node.raft_node.stop()
        server.stop(0)

if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser(description='Start a PSDB master node')
    parser.add_argument('--node-id', required=True, help='Unique node ID')
    parser.add_argument('--raft-port', type=int, required=True, help='Raft communication port (unused)')
    parser.add_argument('--service-port', type=int, required=True, help='gRPC service port')
    parser.add_argument('--peers', nargs='*', default=[], 
                        help='List of peer node_id:host:service_port (space separated)')
    
    args = parser.parse_args()
    
    peers = {}
    for peer in args.peers:
        peer_id, addr = peer.split(':', 1)
        peers[peer_id] = addr
    
    serve_master(
        node_id=args.node_id,
        raft_port=args.raft_port,
        service_port=args.service_port,
        peers=peers
    )
