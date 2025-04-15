import ast
import hashlib
import json
import os
import threading
from typing import List, Dict, Optional
import uuid
from datetime import datetime
from concurrent import futures
import grpc
import database_pb2
import database_pb2_grpc
import logging
import time
from .logger_util import logger
from .simpleJSON import SimpleJSONDB
from .shard_metadata import ShardMetadata
from .index import BPlusTree , BPlusTreeNode , IndexManager
from .database_manager import DatabaseManager
from .database_service import DatabaseService

def serve_worker(port: int, master_addresses: List[str]):
    """
    Start a worker node.
    
    Args:
        port: Port for worker gRPC service
        master_addresses: List of master node addresses (host:port)
    """
    worker_address = f"localhost:{port}"
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    database_pb2_grpc.add_WorkerServiceServicer_to_server(
        DatabaseService(worker_address, master_addresses), server
    )
    server.add_insecure_port(f'[::]:{port}')
    server.start()
    
    logger.info(f"Worker node running on port {port}")
    logger.info(f"Master addresses: {master_addresses}")
    
    try:
        server.wait_for_termination()
    except KeyboardInterrupt:
        logger.info("Shutting down worker node")
        server.stop(0)

if __name__ == '__main__':
    import sys
    port = int(sys.argv[1]) if len(sys.argv) > 1 else 50051
    master_addrs = sys.argv[2:] if len(sys.argv) > 2 else ["localhost:50050"]
    serve_worker(port, master_addrs)
