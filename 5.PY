#!/usr/bin/env python3
"""
ERC20 and NFT Transfer Extractor for Erigon Node with PostgreSQL
Extracts all ERC20, ERC721, and ERC1155 transfers from blocks and stores them in a PostgreSQL database.
Optimized version using bytea for hash storage and NUMERIC for values.
"""

import asyncio
import aiohttp
import asyncpg
import json
import simdjson
import logging
from typing import List, Dict, Optional, Tuple
from datetime import datetime
import time
import os
from decimal import Decimal

# gRPC imports (optional - fallback to JSON-RPC if not available)
try:
    import grpc
    import grpc.aio
    GRPC_AVAILABLE = True
except ImportError:
    GRPC_AVAILABLE = False
    logger.warning("grpcio not installed - gRPC support disabled")

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# ERC20 Transfer event signature
# Transfer(address indexed from, address indexed to, uint256 value)
ERC20_TRANSFER_TOPIC = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"

# ERC721 uses the same Transfer signature but with tokenId indexed (4 topics total)
# Transfer(address indexed from, address indexed to, uint256 indexed tokenId)
ERC721_TRANSFER_TOPIC = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"

# ERC1155 event signatures
# TransferSingle(address indexed operator, address indexed from, address indexed to, uint256 id, uint256 value)
ERC1155_TRANSFER_SINGLE_TOPIC = "0xc3d58168c5ae7397731d063d5bbf3d657854427343f4c083240f7aacaa2d0f62"
# TransferBatch(address indexed operator, address indexed from, address indexed to, uint256[] ids, uint256[] values)
ERC1155_TRANSFER_BATCH_TOPIC = "0x4a39dc06d4c0dbc64b70af90fd698a233a518aa5d07e595d983b8c0526c8f7fb"

def hex_to_bytes(hex_string: str) -> bytes:
    """Convert hex string to bytes, handling 0x prefix"""
    if hex_string.startswith('0x'):
        hex_string = hex_string[2:]
    return bytes.fromhex(hex_string)

def bytes_to_hex(data: bytes) -> str:
    """Convert bytes to hex string with 0x prefix"""
    return '0x' + data.hex()

def hex_to_decimal(hex_string: str) -> Decimal:
    """Convert hex string to Decimal for large numbers"""
    if hex_string.startswith('0x'):
        hex_string = hex_string[2:]
    if not hex_string:
        return Decimal('0')
    return Decimal(int(hex_string, 16))

# Direct tuple types (no dataclasses for performance)
# ERC20Transfer tuple: (block_number, block_hash, transaction_hash, log_index,
#                       contract_address, from_address, to_address, value, timestamp)
# NFTTransfer tuple: (block_number, block_hash, transaction_hash, log_index,
#                     contract_address, from_address, to_address, token_id,
#                     value, token_standard, operator, timestamp)

class ErigonClient:
    def __init__(self, rpc_url: str = "http://localhost:8545"):
        self.rpc_url = rpc_url
        self.session = None

    async def __aenter__(self):
        self.session = aiohttp.ClientSession()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self.session:
            await self.session.close()

    async def rpc_call(self, method: str, params: list) -> dict:
        """Make JSON-RPC call to Erigon node using simdjson for faster parsing"""
        payload = {
            "jsonrpc": "2.0",
            "method": method,
            "params": params,
            "id": 1
        }

        async with self.session.post(self.rpc_url, json=payload) as response:
            # Use simdjson.loads() which handles parser lifecycle internally
            response_bytes = await response.read()
            result = simdjson.loads(response_bytes)
            if "error" in result:
                raise Exception(f"RPC Error: {result['error']}")
            return result["result"]
    
    async def get_latest_block_number(self) -> int:
        """Get the latest block number"""
        result = await self.rpc_call("eth_blockNumber", [])
        return int(result, 16)
    
    async def get_first_block_number(self) -> int:
        """Get the first block number (usually 0 or 1)"""
        # Try to get block 0, if it doesn't exist, try block 1
        try:
            await self.rpc_call("eth_getBlockByNumber", ["0x0", False])
            return 0
        except:
            return 1
    
    async def get_block_by_number(self, block_number: int, full_transactions: bool = False) -> dict:
        """Get block by number"""
        hex_block = hex(block_number)
        return await self.rpc_call("eth_getBlockByNumber", [hex_block, full_transactions])
    
    async def get_logs(self, from_block: int, to_block: int, topics: List[str] = None) -> List[dict]:
        """Get logs for ERC20 transfers"""
        filter_params = {
            "fromBlock": hex(from_block),
            "toBlock": hex(to_block),
            "topics": topics or [ERC20_TRANSFER_TOPIC]
        }
        return await self.rpc_call("eth_getLogs", [filter_params])


class ErigonGrpcClient:
    """
    gRPC client for Erigon using RLP-encoded blocks for maximum performance

    Note: Erigon's gRPC API doesn't have eth_getLogs equivalent.
    This client is for block operations only. Use JSON-RPC for getLogs.

    Uses RLP decoding for efficient binary protocol data handling.
    """

    def __init__(self, grpc_url: str = "localhost:9090"):
        self.grpc_url = grpc_url
        self.channel = None
        self.eth_backend_stub = None

    async def __aenter__(self):
        if not GRPC_AVAILABLE:
            raise RuntimeError("grpcio not installed - cannot use gRPC")

        try:
            # Import generated protobuf stubs
            from remote import ethbackend_pb2, ethbackend_pb2_grpc
            from google.protobuf import empty_pb2

            # Create async gRPC channel
            self.channel = grpc.aio.insecure_channel(self.grpc_url)

            # Initialize ETHBACKEND stub
            self.eth_backend_stub = ethbackend_pb2_grpc.ETHBACKENDStub(self.channel)

            # Store protobuf modules for later use
            self.ethbackend_pb2 = ethbackend_pb2
            self.empty_pb2 = empty_pb2

            logger.debug(f"gRPC client connected to {self.grpc_url}")
            return self
        except ImportError as e:
            raise RuntimeError(f"gRPC proto files not generated: {e}. Run: python3 -m grpc_tools.protoc ...")

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self.channel:
            await self.channel.close()

    async def get_block_by_number(self, block_number: int, full_transactions: bool = False) -> dict:
        """
        Get block by number via gRPC with RLP decoding
        Returns block in JSON-RPC format for compatibility
        """
        import rlp

        # Create block request
        request = self.ethbackend_pb2.BlockRequest(block_height=block_number)

        # Call gRPC method
        response = await self.eth_backend_stub.Block(request)

        # Decode RLP-encoded block
        return self._decode_rlp_block(response.block_rlp, block_number)

    async def get_latest_block_number(self) -> int:
        """Get latest block number via gRPC Syncing method"""
        request = self.empty_pb2.Empty()
        response = await self.eth_backend_stub.Syncing(request)
        return response.current_block

    def _decode_rlp_block(self, block_rlp: bytes, block_number: int) -> dict:
        """
        Decode RLP-encoded block to JSON-RPC format

        RLP block structure: [header, transactions, uncles]
        We only need header for our use case (timestamp)
        """
        import rlp

        # Decode the RLP block
        decoded = rlp.decode(block_rlp)

        if not decoded or len(decoded) < 1:
            raise ValueError("Invalid RLP block structure")

        # Extract header (first element)
        header = decoded[0]

        # Header structure (15 fields):
        # 0: parentHash, 1: sha3Uncles, 2: miner, 3: stateRoot, 4: transactionsRoot
        # 5: receiptsRoot, 6: logsBloom, 7: difficulty, 8: number, 9: gasLimit
        # 10: gasUsed, 11: timestamp, 12: extraData, 13: mixHash, 14: nonce

        if len(header) < 15:
            raise ValueError(f"Invalid header structure, expected 15+ fields, got {len(header)}")

        # Extract timestamp (field 11)
        timestamp_bytes = header[11]
        if isinstance(timestamp_bytes, bytes):
            timestamp = int.from_bytes(timestamp_bytes, byteorder='big') if timestamp_bytes else 0
        else:
            timestamp = int(timestamp_bytes)

        # Return in JSON-RPC compatible format
        return {
            "number": hex(block_number),
            "timestamp": hex(timestamp)
        }

class HybridErigonClient:
    """
    Hybrid client that uses both JSON-RPC and gRPC

    - Uses JSON-RPC for getLogs (gRPC doesn't support it)
    - Can use gRPC for block operations (when available)
    - Automatically falls back to JSON-RPC if gRPC fails
    """

    def __init__(self, rpc_url: str = "http://localhost:8545", grpc_url: str = "localhost:9090", use_grpc: bool = False):
        self.rpc_url = rpc_url
        self.grpc_url = grpc_url
        self.use_grpc = use_grpc and GRPC_AVAILABLE

        self.json_client = None
        self.grpc_client = None

        if self.use_grpc and not GRPC_AVAILABLE:
            logger.warning("gRPC requested but not available - using JSON-RPC only")
            self.use_grpc = False

    async def __aenter__(self):
        # Always initialize JSON-RPC client (needed for getLogs)
        self.json_client = ErigonClient(self.rpc_url)
        await self.json_client.__aenter__()

        # Initialize gRPC client if requested
        if self.use_grpc:
            try:
                self.grpc_client = ErigonGrpcClient(self.grpc_url)
                await self.grpc_client.__aenter__()
                logger.debug("Hybrid client: Using gRPC for blocks, JSON-RPC for logs")
            except Exception as e:
                logger.warning(f"gRPC initialization failed: {e} - falling back to JSON-RPC only")
                self.grpc_client = None
                self.use_grpc = False
        else:
            logger.debug("Hybrid client: Using JSON-RPC only")

        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self.json_client:
            await self.json_client.__aexit__(exc_type, exc_val, exc_tb)
        if self.grpc_client:
            await self.grpc_client.__aexit__(exc_type, exc_val, exc_tb)

    async def get_block_by_number(self, block_number: int, full_transactions: bool = False) -> dict:
        """Get block - tries gRPC first if enabled, falls back to JSON-RPC"""
        if self.use_grpc and self.grpc_client:
            try:
                return await self.grpc_client.get_block_by_number(block_number, full_transactions)
            except (NotImplementedError, Exception) as e:
                # Fallback to JSON-RPC
                if not isinstance(e, NotImplementedError):
                    logger.debug(f"gRPC getBlockByNumber failed, using JSON-RPC: {e}")
                return await self.json_client.get_block_by_number(block_number, full_transactions)
        else:
            return await self.json_client.get_block_by_number(block_number, full_transactions)

    async def get_latest_block_number(self) -> int:
        """Get latest block number - tries gRPC first if enabled, falls back to JSON-RPC"""
        if self.use_grpc and self.grpc_client:
            try:
                return await self.grpc_client.get_latest_block_number()
            except (NotImplementedError, Exception) as e:
                if not isinstance(e, NotImplementedError):
                    logger.debug(f"gRPC getLatestBlockNumber failed, using JSON-RPC: {e}")
                return await self.json_client.get_latest_block_number()
        else:
            return await self.json_client.get_latest_block_number()

    async def get_first_block_number(self) -> int:
        """Get first block number - uses JSON-RPC (no gRPC equivalent)"""
        return await self.json_client.get_first_block_number()

    async def get_logs(self, from_block: int, to_block: int, topics: List[str] = None) -> List[dict]:
        """
        Get logs - ALWAYS uses JSON-RPC (gRPC doesn't support eth_getLogs)
        """
        return await self.json_client.get_logs(from_block, to_block, topics)


class DatabaseManager:
    def __init__(self, connection_string: str = None):
        # Default connection string if not provided
        if connection_string is None:
            connection_string = os.getenv(
                'DATABASE_URL',
                'postgresql://cevat:atl10b31@localhost:5455/erc20_transfers'
            )
        self.connection_string = connection_string
        self.pool = None
    
    async def __aenter__(self):
        await self.init_pool()
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self.pool:
            await self.pool.close()
    
    async def init_pool(self):
        """Initialize connection pool"""
        self.pool = await asyncpg.create_pool(
            self.connection_string,
            min_size=5,
            max_size=20,  # Increased for parallel operations
            command_timeout=60
        )
        await self.init_database()
    
    async def init_database(self):
        """Initialize the database with required tables"""
        async with self.pool.acquire() as conn:
            # Create ERC20 transfers table with bytea for hashes and addresses, NUMERIC for values
            await conn.execute('''
                CREATE TABLE IF NOT EXISTS erc20_transfers (
                    id BIGSERIAL PRIMARY KEY,
                    block_number BIGINT NOT NULL,
                    block_hash BYTEA NOT NULL CHECK (length(block_hash) = 32),
                    transaction_hash BYTEA NOT NULL CHECK (length(transaction_hash) = 32),
                    log_index INTEGER NOT NULL,
                    contract_address BYTEA NOT NULL CHECK (length(contract_address) = 20),
                    from_address BYTEA NOT NULL CHECK (length(from_address) = 20),
                    to_address BYTEA NOT NULL CHECK (length(to_address) = 20),
                    value NUMERIC(78, 0) NOT NULL,
                    timestamp BIGINT NOT NULL,
                    created_at TIMESTAMP DEFAULT NOW(),
                    UNIQUE(transaction_hash, log_index)
                )
            ''')
            
            # Check if we need to migrate existing data from TEXT to NUMERIC
            await self.migrate_value_column_if_needed(conn)

            # Create NFT transfers table for ERC721 and ERC1155
            await conn.execute('''
                CREATE TABLE IF NOT EXISTS nft_transfers (
                    id BIGSERIAL PRIMARY KEY,
                    block_number BIGINT NOT NULL,
                    block_hash BYTEA NOT NULL CHECK (length(block_hash) = 32),
                    transaction_hash BYTEA NOT NULL CHECK (length(transaction_hash) = 32),
                    log_index INTEGER NOT NULL,
                    contract_address BYTEA NOT NULL CHECK (length(contract_address) = 20),
                    from_address BYTEA NOT NULL CHECK (length(from_address) = 20),
                    to_address BYTEA NOT NULL CHECK (length(to_address) = 20),
                    token_id NUMERIC(78, 0) NOT NULL,
                    value NUMERIC(78, 0) NOT NULL DEFAULT 1,
                    token_standard VARCHAR(10) NOT NULL,
                    operator BYTEA CHECK (operator IS NULL OR length(operator) = 20),
                    timestamp BIGINT NOT NULL,
                    created_at TIMESTAMP DEFAULT NOW(),
                    UNIQUE(transaction_hash, log_index, token_id)
                )
            ''')

            # Create processed blocks tracking table
            await conn.execute('''
                CREATE TABLE IF NOT EXISTS processed_blocks (
                    id SERIAL PRIMARY KEY,
                    block_number BIGINT UNIQUE NOT NULL,
                    processed_at TIMESTAMP DEFAULT NOW(),
                    transfer_count INTEGER DEFAULT 0
                )
            ''')
            
            await conn.execute('''
                CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_processed_blocks_number 
                ON processed_blocks(block_number)
            ''')
            
            # Create sync progress table for tracking ranges
            await conn.execute('''
                CREATE TABLE IF NOT EXISTS sync_progress (
                    id INTEGER PRIMARY KEY DEFAULT 1,
                    last_processed_block BIGINT NOT NULL DEFAULT 0,
                    first_block BIGINT,
                    latest_block BIGINT,
                    updated_at TIMESTAMP DEFAULT NOW(),
                    CONSTRAINT single_row CHECK (id = 1)
                )
            ''')

            # Create failed blocks table to track RPC errors for retry
            await conn.execute('''
                CREATE TABLE IF NOT EXISTS failed_blocks (
                    id SERIAL PRIMARY KEY,
                    start_block BIGINT NOT NULL,
                    end_block BIGINT NOT NULL,
                    error_message TEXT,
                    retry_count INTEGER DEFAULT 0,
                    created_at TIMESTAMP DEFAULT NOW(),
                    last_retry_at TIMESTAMP,
                    resolved BOOLEAN DEFAULT FALSE,
                    UNIQUE(start_block, end_block)
                )
            ''')

            await conn.execute('''
                CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_failed_blocks_resolved
                ON failed_blocks(resolved) WHERE resolved = FALSE
            ''')

            logger.info("Database tables and indexes initialized")
    
    async def migrate_value_column_if_needed(self, conn):
        """Migrate value column from TEXT to NUMERIC if needed"""
        try:
            # Check if value column is TEXT
            column_info = await conn.fetchrow('''
                SELECT data_type, character_maximum_length 
                FROM information_schema.columns 
                WHERE table_name = 'erc20_transfers' AND column_name = 'value'
            ''')
            
            if column_info and column_info['data_type'] == 'text':
                logger.info("Migrating value column from TEXT to NUMERIC...")
                
                # Create a backup table
                await conn.execute('''
                    CREATE TABLE IF NOT EXISTS erc20_transfers_backup AS 
                    SELECT * FROM erc20_transfers LIMIT 0
                ''')
                
                # Add new column with NUMERIC type
                await conn.execute('''
                    ALTER TABLE erc20_transfers 
                    ADD COLUMN IF NOT EXISTS value_numeric NUMERIC(78, 0)
                ''')
                
                # Convert existing TEXT values to NUMERIC
                # Handle hex values properly
                await conn.execute('''
                    UPDATE erc20_transfers 
                    SET value_numeric = CASE 
                        WHEN value LIKE '0x%' THEN 
                            ('x' || SUBSTRING(value FROM 3))::bit(LENGTH(SUBSTRING(value FROM 3)) * 4)::bigint::numeric
                        ELSE 
                            value::numeric
                    END
                    WHERE value_numeric IS NULL
                ''')
                
                # Drop old column and rename new one
                await conn.execute('ALTER TABLE erc20_transfers DROP COLUMN value')
                await conn.execute('ALTER TABLE erc20_transfers RENAME COLUMN value_numeric TO value')
                await conn.execute('ALTER TABLE erc20_transfers ALTER COLUMN value SET NOT NULL')
                
                logger.info("Migration completed successfully")
                
        except Exception as e:
            logger.warning(f"Migration check/execution failed: {e}")
            # Continue without migration - might be a fresh install
    
    async def get_sync_progress(self) -> Tuple[int, Optional[int], Optional[int]]:
        """Get sync progress: (last_processed_block, first_block, latest_block)"""
        async with self.pool.acquire() as conn:
            result = await conn.fetchrow('''
                SELECT last_processed_block, first_block, latest_block 
                FROM sync_progress 
                WHERE id = 1
            ''')
            
            if result:
                return result['last_processed_block'], result['first_block'], result['latest_block']
            else:
                # Initialize sync progress
                await conn.execute('''
                    INSERT INTO sync_progress (id, last_processed_block) 
                    VALUES (1, 0)
                ''')
                return 0, None, None
    
    async def update_sync_progress(self, last_processed_block: int, first_block: int = None, latest_block: int = None):
        """Update sync progress"""
        async with self.pool.acquire() as conn:
            if first_block is not None and latest_block is not None:
                await conn.execute('''
                    UPDATE sync_progress 
                    SET last_processed_block = $1, first_block = $2, latest_block = $3, updated_at = NOW()
                    WHERE id = 1
                ''', last_processed_block, first_block, latest_block)
            else:
                await conn.execute('''
                    UPDATE sync_progress 
                    SET last_processed_block = $1, updated_at = NOW()
                    WHERE id = 1
                ''', last_processed_block)
    
    async def is_block_processed(self, block_number: int) -> bool:
        """Check if a block has been processed"""
        async with self.pool.acquire() as conn:
            result = await conn.fetchval('''
                SELECT EXISTS(SELECT 1 FROM processed_blocks WHERE block_number = $1)
            ''', block_number)
            return result
    
    async def get_unprocessed_blocks_in_range(self, start_block: int, end_block: int) -> List[int]:
        """Get list of unprocessed blocks in a range"""
        async with self.pool.acquire() as conn:
            # Generate series of block numbers and find which ones are not processed
            processed_blocks = await conn.fetch('''
                WITH block_range AS (
                    SELECT generate_series($1::bigint, $2::bigint) AS block_number
                )
                SELECT br.block_number
                FROM block_range br
                LEFT JOIN processed_blocks pb ON br.block_number = pb.block_number
                WHERE pb.block_number IS NULL
                ORDER BY br.block_number
            ''', start_block, end_block)
            
            return [row['block_number'] for row in processed_blocks]
    
    async def mark_block_processed(self, block_number: int, transfer_count: int = 0):
        """Mark a block as processed"""
        async with self.pool.acquire() as conn:
            await conn.execute('''
                INSERT INTO processed_blocks (block_number, transfer_count, processed_at)
                VALUES ($1, $2, NOW())
                ON CONFLICT (block_number)
                DO UPDATE SET
                    transfer_count = EXCLUDED.transfer_count,
                    processed_at = NOW()
            ''', block_number, transfer_count)

    async def record_failed_blocks(self, start_block: int, end_block: int, error_message: str):
        """Record a failed block range for later retry"""
        async with self.pool.acquire() as conn:
            await conn.execute('''
                INSERT INTO failed_blocks (start_block, end_block, error_message, created_at)
                VALUES ($1, $2, $3, NOW())
                ON CONFLICT (start_block, end_block)
                DO UPDATE SET
                    error_message = EXCLUDED.error_message,
                    retry_count = failed_blocks.retry_count + 1,
                    last_retry_at = NOW()
            ''', start_block, end_block, error_message)
        logger.warning(f"Recorded failed blocks {start_block}-{end_block}: {error_message[:100]}")

    async def get_failed_blocks(self, max_retries: int = 5) -> List[Tuple[int, int, int]]:
        """Get unresolved failed block ranges that haven't exceeded max retries"""
        async with self.pool.acquire() as conn:
            results = await conn.fetch('''
                SELECT start_block, end_block, retry_count
                FROM failed_blocks
                WHERE resolved = FALSE AND retry_count < $1
                ORDER BY start_block
            ''', max_retries)
            return [(row['start_block'], row['end_block'], row['retry_count']) for row in results]

    async def mark_failed_blocks_resolved(self, start_block: int, end_block: int):
        """Mark a failed block range as resolved after successful retry"""
        async with self.pool.acquire() as conn:
            await conn.execute('''
                UPDATE failed_blocks
                SET resolved = TRUE, last_retry_at = NOW()
                WHERE start_block = $1 AND end_block = $2
            ''', start_block, end_block)
        logger.info(f"Marked failed blocks {start_block}-{end_block} as resolved")

    async def insert_transfers(self, transfers: List[Tuple], skip_duplicate_check: bool = False, conn=None) -> int:
        """Insert ERC20 transfers into database - returns count of actually inserted rows

        Args:
            transfers: List of transfer tuples to insert
            skip_duplicate_check: If True, skip checking for existing records (faster for new blocks)
            conn: Optional connection to use (for transaction batching)
        """
        if not transfers:
            return 0

        # Use provided connection or acquire new one
        if conn is None:
            async with self.pool.acquire() as conn:
                return await self._insert_transfers_impl(conn, transfers, skip_duplicate_check)
        else:
            return await self._insert_transfers_impl(conn, transfers, skip_duplicate_check)

    async def _insert_transfers_impl(self, conn, transfers: List[Tuple], skip_duplicate_check: bool):
        # Transfers are already tuples, no need to convert
        transfer_data = transfers

        if skip_duplicate_check:
            # Fast path: use COPY for bulk insert (much faster than INSERT)
            try:
                # COPY doesn't support ON CONFLICT, but since we checked blocks are unprocessed, duplicates are unlikely
                # Using copy_records_to_table for maximum performance
                await conn.copy_records_to_table(
                    'erc20_transfers',
                    records=transfer_data,
                    columns=['block_number', 'block_hash', 'transaction_hash', 'log_index',
                            'contract_address', 'from_address', 'to_address', 'value', 'timestamp']
                )
                return len(transfers)
            except Exception as e:
                # If COPY fails (e.g., duplicate), fall back to INSERT with ON CONFLICT
                logger.warning(f"COPY failed, falling back to INSERT: {e}")
                await conn.executemany('''
                    INSERT INTO erc20_transfers
                    (block_number, block_hash, transaction_hash, log_index, contract_address,
                     from_address, to_address, value, timestamp)
                    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
                    ON CONFLICT (transaction_hash, log_index) DO NOTHING
                ''', transfer_data)
                return len(transfers)
        else:
            # Slow path: check for existing records first
            # Tuple indices: 0=block_number, 1=block_hash, 2=transaction_hash, 3=log_index,
            #                4=contract_address, 5=from_address, 6=to_address, 7=value, 8=timestamp
            tx_hashes = [t[2] for t in transfers]  # transaction_hash
            log_indices = [t[3] for t in transfers]  # log_index

            existing = await conn.fetch('''
                SELECT transaction_hash, log_index
                FROM erc20_transfers
                WHERE (transaction_hash, log_index) IN (
                    SELECT unnest($1::bytea[]), unnest($2::int[])
                )
            ''', tx_hashes, log_indices)

            existing_set = {(bytes(r['transaction_hash']), r['log_index']) for r in existing}

            # Filter out already existing transfers
            new_transfers = [
                t for t in transfers
                if (t[2], t[3]) not in existing_set  # transaction_hash, log_index
            ]

            if not new_transfers:
                return 0

            # Transfers are already tuples, use directly
            new_transfer_data = new_transfers

            # Use batch insert
            try:
                await conn.executemany('''
                    INSERT INTO erc20_transfers
                    (block_number, block_hash, transaction_hash, log_index, contract_address,
                     from_address, to_address, value, timestamp)
                    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
                ''', new_transfer_data)

                return len(new_transfers)

            except Exception as e:
                logger.error(f"Error inserting transfers: {e}")
                raise

    async def insert_nft_transfers(self, transfers: List[Tuple], skip_duplicate_check: bool = False, conn=None) -> int:
        """Insert NFT transfers into database - returns count of actually inserted rows

        Args:
            transfers: List of NFT transfer tuples to insert
            skip_duplicate_check: If True, skip checking for existing records (faster for new blocks)
            conn: Optional connection to use (for transaction batching)
        """
        if not transfers:
            return 0

        # Use provided connection or acquire new one
        if conn is None:
            async with self.pool.acquire() as conn:
                return await self._insert_nft_transfers_impl(conn, transfers, skip_duplicate_check)
        else:
            return await self._insert_nft_transfers_impl(conn, transfers, skip_duplicate_check)

    async def _insert_nft_transfers_impl(self, conn, transfers: List[Tuple], skip_duplicate_check: bool):
            # Transfers are already tuples, no need to convert
            transfer_data = transfers

            if skip_duplicate_check:
                # Fast path: use COPY for bulk insert (much faster than INSERT)
                try:
                    # COPY doesn't support ON CONFLICT, but since we checked blocks are unprocessed, duplicates are unlikely
                    await conn.copy_records_to_table(
                        'nft_transfers',
                        records=transfer_data,
                        columns=['block_number', 'block_hash', 'transaction_hash', 'log_index',
                                'contract_address', 'from_address', 'to_address', 'token_id',
                                'value', 'token_standard', 'operator', 'timestamp']
                    )
                    return len(transfers)
                except Exception as e:
                    # If COPY fails (e.g., duplicate), fall back to INSERT with ON CONFLICT
                    logger.warning(f"COPY failed for NFT, falling back to INSERT: {e}")
                    await conn.executemany('''
                        INSERT INTO nft_transfers
                        (block_number, block_hash, transaction_hash, log_index, contract_address,
                         from_address, to_address, token_id, value, token_standard, operator, timestamp)
                        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
                        ON CONFLICT (transaction_hash, log_index, token_id) DO NOTHING
                    ''', transfer_data)
                    return len(transfers)
            else:
                # Slow path: check for existing records first
                # Tuple indices: 0=block_number, 1=block_hash, 2=transaction_hash, 3=log_index,
                #                4=contract_address, 5=from_address, 6=to_address, 7=token_id,
                #                8=value, 9=token_standard, 10=operator, 11=timestamp
                tx_hashes = [t[2] for t in transfers]  # transaction_hash
                log_indices = [t[3] for t in transfers]  # log_index
                token_ids = [t[7] for t in transfers]  # token_id

                existing = await conn.fetch('''
                    SELECT transaction_hash, log_index, token_id
                    FROM nft_transfers
                    WHERE (transaction_hash, log_index, token_id) IN (
                        SELECT unnest($1::bytea[]), unnest($2::int[]), unnest($3::numeric[])
                    )
                ''', tx_hashes, log_indices, token_ids)

                existing_set = {(bytes(r['transaction_hash']), r['log_index'], r['token_id']) for r in existing}

                # Filter out already existing transfers
                new_transfers = [
                    t for t in transfers
                    if (t[2], t[3], t[7]) not in existing_set  # transaction_hash, log_index, token_id
                ]

                if not new_transfers:
                    return 0

                # Transfers are already tuples, use directly
                new_transfer_data = new_transfers

                # Use batch insert
                try:
                    await conn.executemany('''
                        INSERT INTO nft_transfers
                        (block_number, block_hash, transaction_hash, log_index, contract_address,
                         from_address, to_address, token_id, value, token_standard, operator, timestamp)
                        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
                    ''', new_transfer_data)

                    return len(new_transfers)

                except Exception as e:
                    logger.error(f"Error inserting NFT transfers: {e}")
                    raise

    async def get_stats(self) -> Dict:
        """Get database statistics"""
        async with self.pool.acquire() as conn:
            # ERC20 stats
            erc20_stats = await conn.fetchrow('''
                SELECT
                    COUNT(*) as total_transfers,
                    COUNT(DISTINCT contract_address) as unique_contracts,
                    COUNT(DISTINCT from_address) as unique_senders,
                    COUNT(DISTINCT to_address) as unique_receivers,
                    MIN(block_number) as first_block,
                    MAX(block_number) as last_block
                FROM erc20_transfers
            ''')

            # NFT stats
            nft_stats = await conn.fetchrow('''
                SELECT
                    COUNT(*) as total_transfers,
                    COUNT(DISTINCT contract_address) as unique_contracts,
                    COUNT(DISTINCT from_address) as unique_senders,
                    COUNT(DISTINCT to_address) as unique_receivers,
                    MIN(block_number) as first_block,
                    MAX(block_number) as last_block
                FROM nft_transfers
            ''')

            # NFT stats by standard
            nft_by_standard = await conn.fetch('''
                SELECT token_standard, COUNT(*) as count
                FROM nft_transfers
                GROUP BY token_standard
            ''')

            processed_blocks_count = await conn.fetchval('''
                SELECT COUNT(*) FROM processed_blocks
            ''')

            # Get table size information
            erc20_size_info = await conn.fetchrow('''
                SELECT
                    pg_size_pretty(pg_total_relation_size('erc20_transfers')) as table_size,
                    pg_size_pretty(pg_relation_size('erc20_transfers')) as data_size,
                    pg_size_pretty(pg_total_relation_size('erc20_transfers') - pg_relation_size('erc20_transfers')) as index_size
            ''')

            nft_size_info = await conn.fetchrow('''
                SELECT
                    pg_size_pretty(pg_total_relation_size('nft_transfers')) as table_size,
                    pg_size_pretty(pg_relation_size('nft_transfers')) as data_size,
                    pg_size_pretty(pg_total_relation_size('nft_transfers') - pg_relation_size('nft_transfers')) as index_size
            ''')

            return {
                'erc20_transfers': erc20_stats['total_transfers'],
                'erc20_unique_contracts': erc20_stats['unique_contracts'],
                'erc20_unique_senders': erc20_stats['unique_senders'],
                'erc20_unique_receivers': erc20_stats['unique_receivers'],
                'erc20_first_block': erc20_stats['first_block'],
                'erc20_last_block': erc20_stats['last_block'],
                'erc20_table_size': erc20_size_info['table_size'],
                'erc20_data_size': erc20_size_info['data_size'],
                'erc20_index_size': erc20_size_info['index_size'],
                'nft_transfers': nft_stats['total_transfers'],
                'nft_unique_contracts': nft_stats['unique_contracts'],
                'nft_unique_senders': nft_stats['unique_senders'],
                'nft_unique_receivers': nft_stats['unique_receivers'],
                'nft_first_block': nft_stats['first_block'],
                'nft_last_block': nft_stats['last_block'],
                'nft_by_standard': {row['token_standard']: row['count'] for row in nft_by_standard},
                'nft_table_size': nft_size_info['table_size'],
                'nft_data_size': nft_size_info['data_size'],
                'nft_index_size': nft_size_info['index_size'],
                'processed_blocks': processed_blocks_count
            }
    
    async def query_transfers_by_address(self, address: str, limit: int = 100) -> List[Dict]:
        """Query transfers for a specific address (example query method)"""
        address_bytes = hex_to_bytes(address)

        async with self.pool.acquire() as conn:
            results = await conn.fetch('''
                SELECT
                    block_number,
                    encode(block_hash, 'hex') as block_hash,
                    encode(transaction_hash, 'hex') as transaction_hash,
                    log_index,
                    encode(contract_address, 'hex') as contract_address,
                    encode(from_address, 'hex') as from_address,
                    encode(to_address, 'hex') as to_address,
                    value,
                    timestamp
                FROM erc20_transfers
                WHERE from_address = $1 OR to_address = $1
                ORDER BY block_number DESC, log_index DESC
                LIMIT $2
            ''', address_bytes, limit)

            return [dict(row) for row in results]

    async def check_indexes_exist(self, table_name: str) -> bool:
        """Check if indexes exist on a table"""
        async with self.pool.acquire() as conn:
            result = await conn.fetchval('''
                SELECT COUNT(*)
                FROM pg_indexes
                WHERE tablename = $1 AND indexname LIKE 'idx_%'
            ''', table_name)
            return result > 0

    async def drop_table_indexes(self, table_name: str):
        """Drop all indexes on a table (except primary key and unique constraints)"""
        async with self.pool.acquire() as conn:
            # Get all index names
            indexes = await conn.fetch('''
                SELECT indexname
                FROM pg_indexes
                WHERE tablename = $1
                AND indexname LIKE 'idx_%'
            ''', table_name)

            for idx in indexes:
                index_name = idx['indexname']
                logger.info(f"Dropping index {index_name}")
                await conn.execute(f'DROP INDEX IF EXISTS {index_name}')

    async def create_erc20_indexes(self):
        """Create indexes for ERC20 transfers table"""
        async with self.pool.acquire() as conn:
            indexes = [
                'CREATE INDEX IF NOT EXISTS idx_erc20_block_number ON erc20_transfers(block_number)',
                'CREATE INDEX IF NOT EXISTS idx_erc20_timestamp ON erc20_transfers(timestamp)',
                'CREATE INDEX IF NOT EXISTS idx_erc20_value ON erc20_transfers(value)',
                'CREATE INDEX IF NOT EXISTS idx_erc20_contract_address ON erc20_transfers(contract_address)',
                'CREATE INDEX IF NOT EXISTS idx_erc20_from_address ON erc20_transfers(from_address)',
                'CREATE INDEX IF NOT EXISTS idx_erc20_to_address ON erc20_transfers(to_address)',
                'CREATE INDEX IF NOT EXISTS idx_erc20_block_hash ON erc20_transfers(block_hash)',
                'CREATE INDEX IF NOT EXISTS idx_erc20_tx_hash ON erc20_transfers(transaction_hash)'
            ]

            for index_sql in indexes:
                logger.info(f"Creating index: {index_sql.split()[5]}")
                await conn.execute(index_sql)

    async def create_nft_indexes(self):
        """Create indexes for NFT transfers table"""
        async with self.pool.acquire() as conn:
            indexes = [
                'CREATE INDEX IF NOT EXISTS idx_nft_block_number ON nft_transfers(block_number)',
                'CREATE INDEX IF NOT EXISTS idx_nft_timestamp ON nft_transfers(timestamp)',
                'CREATE INDEX IF NOT EXISTS idx_nft_token_id ON nft_transfers(token_id)',
                'CREATE INDEX IF NOT EXISTS idx_nft_contract_address ON nft_transfers(contract_address)',
                'CREATE INDEX IF NOT EXISTS idx_nft_from_address ON nft_transfers(from_address)',
                'CREATE INDEX IF NOT EXISTS idx_nft_to_address ON nft_transfers(to_address)',
                'CREATE INDEX IF NOT EXISTS idx_nft_token_standard ON nft_transfers(token_standard)',
                'CREATE INDEX IF NOT EXISTS idx_nft_tx_hash ON nft_transfers(transaction_hash)'
            ]

            for index_sql in indexes:
                logger.info(f"Creating index: {index_sql.split()[5]}")
                await conn.execute(index_sql)

def parse_erc20_transfer(log: dict, block_timestamp: int) -> Optional[Tuple]:
    """Parse ERC20 transfer from log entry - returns tuple directly"""
    try:
        # Ensure it's an ERC20 transfer (topic0 = Transfer event signature)
        if not log.get("topics") or log["topics"][0] != ERC20_TRANSFER_TOPIC:
            return None

        # ERC20 Transfer event has 3 topics: signature, from, to
        # and data contains the value
        topics = log["topics"]
        if len(topics) != 3:
            return None

        # Extract addresses from topics (remove 0x and pad to get address)
        from_address_hex = topics[1][-40:]  # Last 40 chars (20 bytes)
        to_address_hex = topics[2][-40:]    # Last 40 chars (20 bytes)

        # Convert to bytes
        from_address = bytes.fromhex(from_address_hex)
        to_address = bytes.fromhex(to_address_hex)

        # Value is in the data field - convert to Decimal for efficient storage
        value_hex = log["data"] if log["data"] != "0x" else "0x0"
        value = hex_to_decimal(value_hex)

        # Return tuple directly (no dataclass)
        return (
            int(log["blockNumber"], 16),      # block_number
            hex_to_bytes(log["blockHash"]),   # block_hash
            hex_to_bytes(log["transactionHash"]),  # transaction_hash
            int(log["logIndex"], 16),         # log_index
            hex_to_bytes(log["address"]),     # contract_address
            from_address,                     # from_address
            to_address,                       # to_address
            value,                            # value
            block_timestamp                   # timestamp
        )
    except Exception as e:
        logger.warning(f"Error parsing log: {e}")
        return None

def parse_erc721_transfer(log: dict, block_timestamp: int) -> Optional[Tuple]:
    """Parse ERC721 transfer from log entry - returns tuple directly"""
    try:
        # ERC721 Transfer has same signature as ERC20 but with 4 topics
        # Transfer(address indexed from, address indexed to, uint256 indexed tokenId)
        if not log.get("topics") or log["topics"][0] != ERC721_TRANSFER_TOPIC:
            return None

        topics = log["topics"]
        # ERC721 has 4 topics: signature, from, to, tokenId
        if len(topics) != 4:
            return None

        # Extract addresses from topics
        from_address_hex = topics[1][-40:]
        to_address_hex = topics[2][-40:]
        token_id_hex = topics[3]

        from_address = bytes.fromhex(from_address_hex)
        to_address = bytes.fromhex(to_address_hex)
        token_id = hex_to_decimal(token_id_hex)

        # Return tuple directly (no dataclass)
        return (
            int(log["blockNumber"], 16),      # block_number
            hex_to_bytes(log["blockHash"]),   # block_hash
            hex_to_bytes(log["transactionHash"]),  # transaction_hash
            int(log["logIndex"], 16),         # log_index
            hex_to_bytes(log["address"]),     # contract_address
            from_address,                     # from_address
            to_address,                       # to_address
            token_id,                         # token_id
            Decimal('1'),                     # value (ERC721 always transfers 1)
            'ERC721',                         # token_standard
            None,                             # operator
            block_timestamp                   # timestamp
        )
    except Exception as e:
        logger.warning(f"Error parsing ERC721 log: {e}")
        return None

def parse_erc1155_single_transfer(log: dict, block_timestamp: int) -> Optional[Tuple]:
    """Parse ERC1155 TransferSingle from log entry - returns tuple directly"""
    try:
        if not log.get("topics") or log["topics"][0] != ERC1155_TRANSFER_SINGLE_TOPIC:
            return None

        topics = log["topics"]
        # TransferSingle has 4 topics: signature, operator, from, to
        if len(topics) != 4:
            return None

        # Extract addresses from topics
        operator_hex = topics[1][-40:]
        from_address_hex = topics[2][-40:]
        to_address_hex = topics[3][-40:]

        operator = bytes.fromhex(operator_hex)
        from_address = bytes.fromhex(from_address_hex)
        to_address = bytes.fromhex(to_address_hex)

        # Data contains id and value (each 32 bytes)
        data = log["data"]
        if data.startswith('0x'):
            data = data[2:]

        # First 32 bytes = token id, next 32 bytes = value
        token_id = hex_to_decimal('0x' + data[:64])
        value = hex_to_decimal('0x' + data[64:128])

        # Return tuple directly (no dataclass)
        return (
            int(log["blockNumber"], 16),      # block_number
            hex_to_bytes(log["blockHash"]),   # block_hash
            hex_to_bytes(log["transactionHash"]),  # transaction_hash
            int(log["logIndex"], 16),         # log_index
            hex_to_bytes(log["address"]),     # contract_address
            from_address,                     # from_address
            to_address,                       # to_address
            token_id,                         # token_id
            value,                            # value
            'ERC1155',                        # token_standard
            operator,                         # operator
            block_timestamp                   # timestamp
        )
    except Exception as e:
        logger.warning(f"Error parsing ERC1155 single log: {e}")
        return None

def parse_erc1155_batch_transfer(log: dict, block_timestamp: int) -> List[Tuple]:
    """Parse ERC1155 TransferBatch from log entry - returns multiple tuples"""
    transfers = []
    try:
        if not log.get("topics") or log["topics"][0] != ERC1155_TRANSFER_BATCH_TOPIC:
            return transfers

        topics = log["topics"]
        if len(topics) != 4:
            return transfers

        # Extract addresses from topics
        operator_hex = topics[1][-40:]
        from_address_hex = topics[2][-40:]
        to_address_hex = topics[3][-40:]

        operator = bytes.fromhex(operator_hex)
        from_address = bytes.fromhex(from_address_hex)
        to_address = bytes.fromhex(to_address_hex)

        # Data contains arrays of ids and values (ABI encoded)
        data = log["data"]
        if data.startswith('0x'):
            data = data[2:]

        # ABI encoding: offset to ids array, offset to values array, then arrays
        # Each array has length followed by elements
        # Skip first 64 bytes (offsets), then read arrays

        # Parse the ABI-encoded arrays
        ids_offset = int(data[:64], 16) * 2  # Convert to hex string position
        values_offset = int(data[64:128], 16) * 2

        # Read ids array
        ids_length = int(data[ids_offset:ids_offset + 64], 16)
        ids = []
        for i in range(ids_length):
            start = ids_offset + 64 + (i * 64)
            ids.append(hex_to_decimal('0x' + data[start:start + 64]))

        # Read values array
        values_length = int(data[values_offset:values_offset + 64], 16)
        values = []
        for i in range(values_length):
            start = values_offset + 64 + (i * 64)
            values.append(hex_to_decimal('0x' + data[start:start + 64]))

        # Create a tuple for each id/value pair
        block_number = int(log["blockNumber"], 16)
        block_hash = hex_to_bytes(log["blockHash"])
        transaction_hash = hex_to_bytes(log["transactionHash"])
        base_log_index = int(log["logIndex"], 16)
        contract_address = hex_to_bytes(log["address"])

        for idx, (token_id, value) in enumerate(zip(ids, values)):
            transfer = (
                block_number,                             # block_number
                block_hash,                               # block_hash
                transaction_hash,                         # transaction_hash
                base_log_index * 1000 + idx,             # log_index (unique for each in batch)
                contract_address,                         # contract_address
                from_address,                             # from_address
                to_address,                               # to_address
                token_id,                                 # token_id
                value,                                    # value
                'ERC1155',                                # token_standard
                operator,                                 # operator
                block_timestamp                           # timestamp
            )
            transfers.append(transfer)

    except Exception as e:
        logger.warning(f"Error parsing ERC1155 batch log: {e}")

    return transfers

class ERC20Extractor:
    def __init__(self, rpc_url: str = "http://localhost:8545", connection_string: str = None,
                 grpc_url: str = "localhost:9090", use_grpc: bool = False):
        self.rpc_url = rpc_url
        self.grpc_url = grpc_url
        self.use_grpc = use_grpc
        self.connection_string = connection_string
        self.batch_size = 100  # Process blocks in batches
        self.db = None
    
    async def __aenter__(self):
        self.db = DatabaseManager(self.connection_string)
        await self.db.__aenter__()
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self.db:
            await self.db.__aexit__(exc_type, exc_val, exc_tb)
    
    async def determine_start_block(self, requested_start_block: int = None) -> int:
        """Determine the starting block for processing"""
        async with HybridErigonClient(self.rpc_url, self.grpc_url, self.use_grpc) as client:
            # Get sync progress from database
            last_processed, first_block_db, latest_block_db = await self.db.get_sync_progress()
            
            # If a specific start block is requested, use it
            if requested_start_block is not None:
                logger.info(f"Using requested start block: {requested_start_block}")
                return requested_start_block
            
            # If we have processed blocks before, continue from where we left off
            if last_processed > 0:
                start_block = last_processed + 1
                logger.info(f"Resuming from last processed block: {start_block}")
                return start_block
            
            # First time running - start from the first available block
            try:
                first_block = await client.get_first_block_number()
                logger.info(f"Starting from first available block: {first_block}")
                return first_block
            except Exception as e:
                logger.warning(f"Could not determine first block, starting from 0: {e}")
                return 0
    
    async def extract_transfers_from_range(self, start_block: int, end_block: int) -> Tuple[List[Tuple], List[Tuple], int, int, float]:
        """Extract ERC20 and NFT transfers from a range of blocks - returns tuples directly"""
        process_start_time = time.time()
        block_time_span_str = ""
        start_datetime_str = ""

        async with HybridErigonClient(self.rpc_url, self.grpc_url, self.use_grpc) as client:
            # Get timestamps for start and end blocks in parallel
            try:
                start_block_data, end_block_data = await asyncio.gather(
                    client.get_block_by_number(start_block),
                    client.get_block_by_number(end_block)
                )
                start_timestamp = int(start_block_data["timestamp"], 16)
                end_timestamp = int(end_block_data["timestamp"], 16)

                start_datetime = datetime.fromtimestamp(start_timestamp)
                start_datetime_str = start_datetime.strftime('%Y-%m-%d %H:%M:%S')
                time_diff = end_timestamp - start_timestamp

                # Format time span
                hours, remainder = divmod(time_diff, 3600)
                minutes, seconds = divmod(remainder, 60)
                block_time_span_str = f"{hours}h {minutes}m {seconds}s" if hours else f"{minutes}m {seconds}s"

            except Exception as e:
                logger.warning(f"Could not get block timestamps: {e}")

            # Check which blocks in this range haven't been processed
            unprocessed_blocks = await self.db.get_unprocessed_blocks_in_range(start_block, end_block)

            if not unprocessed_blocks:
                logger.info(f"All blocks {start_block}-{end_block} already processed")
                return [], [], 0, 0, 0.0

            # If all blocks are unprocessed, we can skip duplicate checks (faster)
            total_blocks = end_block - start_block + 1
            all_blocks_unprocessed = len(unprocessed_blocks) == total_blocks

            logger.info(f"Found {len(unprocessed_blocks)} unprocessed blocks in range")

            # Get logs for all transfer types in parallel - track RPC time
            all_logs = []

            # Fetch all log types in parallel
            rpc_start = time.time()
            transfer_logs_task = client.get_logs(start_block, end_block, [ERC20_TRANSFER_TOPIC])
            single_logs_task = client.get_logs(start_block, end_block, [ERC1155_TRANSFER_SINGLE_TOPIC])
            batch_logs_task = client.get_logs(start_block, end_block, [ERC1155_TRANSFER_BATCH_TOPIC])

            transfer_logs, single_logs, batch_logs = await asyncio.gather(
                transfer_logs_task,
                single_logs_task,
                batch_logs_task
            )
            rpc_duration = time.time() - rpc_start

            all_logs.extend(transfer_logs)
            all_logs.extend(single_logs)
            all_logs.extend(batch_logs)

            if not all_logs:
                # Mark all blocks in range as processed (no transfers)
                for block_num in range(start_block, end_block + 1):
                    await self.db.mark_block_processed(block_num, 0)
                logger.info(f"No transfers found in blocks {start_block}-{end_block}")
                return [], [], 0, 0, 0.0

            # Group logs by block number to get timestamps
            logs_by_block = {}
            for log in all_logs:
                block_num = int(log["blockNumber"], 16)
                if block_num not in logs_by_block:
                    logs_by_block[block_num] = []
                logs_by_block[block_num].append(log)

            # Get block timestamps for blocks that have logs (in parallel)
            block_timestamps = {}
            block_nums = list(logs_by_block.keys())

            async def get_block_timestamp(block_num):
                try:
                    block_data = await client.get_block_by_number(block_num)
                    return block_num, int(block_data["timestamp"], 16)
                except Exception as e:
                    logger.warning(f"Could not get timestamp for block {block_num}: {e}")
                    return block_num, int(time.time())

            timestamp_results = await asyncio.gather(*[get_block_timestamp(bn) for bn in block_nums])
            block_timestamps = dict(timestamp_results)

            # Parse transfers
            erc20_transfers = []
            nft_transfers = []
            blocks_with_transfers = set()

            for block_num, block_logs in logs_by_block.items():
                timestamp = block_timestamps[block_num]
                block_transfer_count = 0

                for log in block_logs:
                    topic0 = log.get("topics", [None])[0]

                    if topic0 == ERC20_TRANSFER_TOPIC:
                        # Could be ERC20 or ERC721 - check topic count
                        topics = log.get("topics", [])
                        if len(topics) == 3:
                            # ERC20 transfer
                            transfer = parse_erc20_transfer(log, timestamp)
                            if transfer:
                                erc20_transfers.append(transfer)
                                block_transfer_count += 1
                        elif len(topics) == 4:
                            # ERC721 transfer
                            transfer = parse_erc721_transfer(log, timestamp)
                            if transfer:
                                nft_transfers.append(transfer)
                                block_transfer_count += 1

                    elif topic0 == ERC1155_TRANSFER_SINGLE_TOPIC:
                        transfer = parse_erc1155_single_transfer(log, timestamp)
                        if transfer:
                            nft_transfers.append(transfer)
                            block_transfer_count += 1

                    elif topic0 == ERC1155_TRANSFER_BATCH_TOPIC:
                        batch_transfers = parse_erc1155_batch_transfer(log, timestamp)
                        nft_transfers.extend(batch_transfers)
                        block_transfer_count += len(batch_transfers)

                if block_transfer_count > 0:
                    blocks_with_transfers.add(block_num)

                # Mark block as processed
                await self.db.mark_block_processed(block_num, block_transfer_count)

            # Mark blocks without transfers as processed
            for block_num in range(start_block, end_block + 1):
                if block_num not in blocks_with_transfers:
                    await self.db.mark_block_processed(block_num, 0)

            # Insert transfers into database in parallel and track DB insertion time
            db_insert_start = time.time()

            # Run both inserts in parallel, skip duplicate checks if all blocks are new
            insert_tasks = []
            if erc20_transfers:
                insert_tasks.append(self.db.insert_transfers(erc20_transfers, skip_duplicate_check=all_blocks_unprocessed))
            else:
                insert_tasks.append(asyncio.sleep(0))  # Dummy task returning 0

            if nft_transfers:
                insert_tasks.append(self.db.insert_nft_transfers(nft_transfers, skip_duplicate_check=all_blocks_unprocessed))
            else:
                insert_tasks.append(asyncio.sleep(0))  # Dummy task returning 0

            results = await asyncio.gather(*insert_tasks)
            erc20_inserted = results[0] if erc20_transfers else 0
            nft_inserted = results[1] if nft_transfers else 0

            db_insert_duration = time.time() - db_insert_start

            # Calculate total processing time
            process_duration = time.time() - process_start_time

            # Calculate total transfers and transfers per second
            total_transfers = erc20_inserted + nft_inserted
            rpc_tps = total_transfers / rpc_duration if rpc_duration > 0 else 0
            proc_tps = total_transfers / process_duration if process_duration > 0 else 0
            db_tps = total_transfers / db_insert_duration if db_insert_duration > 0 else 0

            # Build log message with transfers per second
            log_msg = f"Blocks {start_block}-{end_block}"
            if start_datetime_str:
                log_msg += f" | {start_datetime_str}"
            if block_time_span_str:
                log_msg += f" | span: {block_time_span_str}"
            log_msg += f" | RPC: {rpc_tps:.0f} tx/s | proc: {proc_tps:.0f} tx/s | DB: {db_tps:.0f} tx/s"
            log_msg += f" | ERC20: {erc20_inserted} | NFT: {nft_inserted}"

            logger.info(log_msg)
            return erc20_transfers, nft_transfers, erc20_inserted, nft_inserted, db_insert_duration

    async def retry_failed_blocks(self, max_retries: int = 5):
        """Retry previously failed block ranges"""
        failed_ranges = await self.db.get_failed_blocks(max_retries)

        if not failed_ranges:
            return

        logger.info(f"Retrying {len(failed_ranges)} previously failed block ranges")

        for start_block, end_block, retry_count in failed_ranges:
            logger.info(f"Retrying blocks {start_block}-{end_block} (attempt {retry_count + 1})")

            try:
                erc20_transfers, nft_transfers, erc20_inserted, nft_inserted, db_time = await self.extract_transfers_from_range(start_block, end_block)
                # Success - mark as resolved
                await self.db.mark_failed_blocks_resolved(start_block, end_block)
                logger.info(f"Successfully inserted {erc20_inserted} ERC20 and {nft_inserted} NFT transfers from retry of blocks {start_block}-{end_block}")

            except Exception as e:
                error_msg = str(e)
                logger.error(f"Retry failed for blocks {start_block}-{end_block}: {error_msg}")
                # Record the failure again (increments retry count)
                await self.db.record_failed_blocks(start_block, end_block, error_msg)

            # Small delay between retries
            await asyncio.sleep(0.5)

    async def sync_all_blocks(self, start_block: int = None, end_block: int = None, max_iterations: int = None):
        """Sync all blocks from start_block to latest

        Args:
            start_block: Starting block number
            end_block: Ending block number
            max_iterations: Maximum number of batches to process (for testing)
        """
        async with HybridErigonClient(self.rpc_url, self.grpc_url, self.use_grpc) as client:
            # Determine start block
            if start_block is None:
                start_block = await self.determine_start_block()

            # Get latest block if end_block not specified
            if end_block is None:
                end_block = await client.get_latest_block_number()

            # Update sync progress with blockchain info
            first_block = await client.get_first_block_number()
            await self.db.update_sync_progress(start_block - 1, first_block, end_block)

            logger.info(f"Starting sync from block {start_block} to {end_block}")
            logger.info(f"Total blocks to process: {end_block - start_block + 1}")
            if max_iterations:
                logger.info(f"Limited to {max_iterations} iterations for testing")

            current_block = start_block
            total_erc20_inserted = 0
            total_nft_inserted = 0
            iteration_count = 0

            while current_block <= end_block:
                # Check iteration limit
                if max_iterations and iteration_count >= max_iterations:
                    logger.info(f"Reached iteration limit ({max_iterations}), stopping")
                    break

                iteration_count += 1
                batch_end = min(current_block + self.batch_size - 1, end_block)

                try:
                    # Get batch timestamps for progress logging in parallel
                    batch_start_datetime_str = ""
                    batch_span_str = ""
                    try:
                        batch_start_data, batch_end_data = await asyncio.gather(
                            client.get_block_by_number(current_block),
                            client.get_block_by_number(batch_end)
                        )
                        batch_start_ts = int(batch_start_data["timestamp"], 16)
                        batch_end_ts = int(batch_end_data["timestamp"], 16)
                        batch_start_datetime_str = datetime.fromtimestamp(batch_start_ts).strftime('%Y-%m-%d %H:%M:%S')
                        span = batch_end_ts - batch_start_ts
                        h, rem = divmod(span, 3600)
                        m, s = divmod(rem, 60)
                        batch_span_str = f"{h}h {m}m {s}s" if h else f"{m}m {s}s"
                    except:
                        pass

                    erc20_transfers, nft_transfers, erc20_inserted, nft_inserted, db_time = await self.extract_transfers_from_range(current_block, batch_end)
                    total_erc20_inserted += erc20_inserted
                    total_nft_inserted += nft_inserted

                    # Update progress
                    await self.db.update_sync_progress(batch_end)

                    # Progress logging
                    progress = ((batch_end - start_block + 1) / (end_block - start_block + 1)) * 100
                    total_inserted = total_erc20_inserted + total_nft_inserted
                    progress_msg = f"Progress: {progress:.1f}% - Block {batch_end}/{end_block}"
                    if batch_start_datetime_str:
                        progress_msg += f" | {batch_start_datetime_str}"
                    if batch_span_str:
                        progress_msg += f" | span: {batch_span_str}"
                    progress_msg += f" | Total: {total_inserted:,} tx"
                    logger.info(progress_msg)

                    current_block = batch_end + 1

                    # Small delay to avoid overwhelming the node
                    await asyncio.sleep(0.1)

                except Exception as e:
                    error_msg = str(e)
                    logger.error(f"Error processing blocks {current_block}-{batch_end}: {error_msg}")
                    # Record failed blocks for later retry instead of marking as processed
                    await self.db.record_failed_blocks(current_block, batch_end, error_msg)
                    current_block = batch_end + 1

            total_inserted = total_erc20_inserted + total_nft_inserted
            logger.info(f"Sync completed! Inserted {total_inserted:,} transfers (ERC20: {total_erc20_inserted:,}, NFT: {total_nft_inserted:,})")

            # Retry any previously failed blocks
            await self.retry_failed_blocks()

            # Print final stats (skip in iteration mode to avoid timeout)
            if max_iterations is None:
                try:
                    stats = await self.db.get_stats()
                    logger.info(f"Database stats: {stats}")
                except Exception as e:
                    logger.warning(f"Could not get stats: {e}")
    
    async def continuous_sync(self, poll_interval: int = 30):
        """Continuously sync new blocks"""
        logger.info(f"Starting continuous sync (polling every {poll_interval}s)")
        
        while True:
            try:
                # Sync any missing blocks
                await self.sync_all_blocks()
                
                # Wait before checking for new blocks
                logger.info(f"Waiting {poll_interval}s for new blocks...")
                await asyncio.sleep(poll_interval)
                
            except KeyboardInterrupt:
                logger.info("Stopping continuous sync...")
                break
            except Exception as e:
                logger.error(f"Error in continuous sync: {e}")
                await asyncio.sleep(poll_interval)

async def main():
    """Main function"""
    import argparse
    
    parser = argparse.ArgumentParser(description="Extract ERC20 transfers from Erigon node to PostgreSQL")
    parser.add_argument("--rpc-url", default="http://localhost:8545", help="Erigon RPC URL")
    parser.add_argument("--db-url", help="PostgreSQL connection string (default: from DATABASE_URL env var)")
    parser.add_argument("--start-block", type=int, help="Starting block number (default: resume from last processed)")
    parser.add_argument("--end-block", type=int, help="Ending block number (default: latest)")
    parser.add_argument("--continuous", action="store_true", help="Run in continuous mode")
    parser.add_argument("--batch-size", type=int, default=100, help="Batch size for processing blocks")
    parser.add_argument("--stats", action="store_true", help="Show database statistics and exit")
    parser.add_argument("--query-address", help="Query transfers for a specific address")
    parser.add_argument("--retry", action="store_true", help="Retry failed blocks only")
    parser.add_argument("--create-indexes", action="store_true", help="Create indexes on both ERC20 and NFT tables and exit")
    parser.add_argument("--drop-indexes", action="store_true", help="Drop all indexes from ERC20 and NFT tables and exit")
    parser.add_argument("--iteration", type=int, help="Limit number of batches to process (for testing)")
    parser.add_argument("--grpc-url", default="localhost:9090", help="Erigon gRPC URL (default: localhost:9090)")
    parser.add_argument("--use-grpc", action="store_true", help="Use gRPC for block operations (experimental)")

    args = parser.parse_args()

    async with ERC20Extractor(args.rpc_url, args.db_url, args.grpc_url, args.use_grpc) as extractor:
        extractor.batch_size = args.batch_size

        if args.create_indexes:
            print("\n=== Creating Indexes ===")
            print("Creating ERC20 indexes...")
            await extractor.db.create_erc20_indexes()
            print("Creating NFT indexes...")
            await extractor.db.create_nft_indexes()
            print("Indexes created successfully")
            return

        if args.drop_indexes:
            print("\n=== Dropping Indexes ===")
            print("Dropping ERC20 indexes...")
            await extractor.db.drop_table_indexes('erc20_transfers')
            print("Dropping NFT indexes...")
            await extractor.db.drop_table_indexes('nft_transfers')
            print("Indexes dropped successfully")
            return

        if args.stats:
            stats = await extractor.db.get_stats()
            print("\n=== Database Statistics ===")
            for key, value in stats.items():
                print(f"{key}: {value}")
            return

        if args.query_address:
            transfers = await extractor.db.query_transfers_by_address(args.query_address)
            print(f"\n=== Transfers for {args.query_address} ===")
            for transfer in transfers:
                print(f"Block {transfer['block_number']}: {transfer['value']} from 0x{transfer['from_address']} to 0x{transfer['to_address']}")
            return

        if args.retry:
            failed = await extractor.db.get_failed_blocks()
            if not failed:
                print("No failed blocks to retry")
                return
            print(f"\n=== Retrying {len(failed)} failed block ranges ===")
            await extractor.retry_failed_blocks()
            return

        if args.continuous:
            await extractor.continuous_sync()
        else:
            await extractor.sync_all_blocks(args.start_block, args.end_block, args.iteration)

if __name__ == "__main__":
    asyncio.run(main())