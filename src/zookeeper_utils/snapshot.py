# This module implements a parser of ZooKeeper snapshots.
# The logic was written by following the serialization logic of `FileSnap::serialize()`[1].
#
# [1] https://github.com/apache/zookeeper/blob/66202cb764c203f64b954a917e421be57d2ae67a/zookeeper-server/src/main/java/org/apache/zookeeper/server/persistence/FileSnap.java#L242

import re
import zlib
import struct
import json
from base64 import b64encode
from zookeeper_utils.txlog import list_txlog_files, get_transaction_ranges
from pathlib import Path
from datetime import datetime, timezone

class SnapshotReader:
    '''
    # GENERAL OVERVIEW OF THE SERIALIZATION STREAM

    The output of data in `FileSnap.serialize()` is handled by `BinaryOutputArchive` [1] which
    internally use the Java standard class DataOutputStream [2].
    The output is then streamed through a CheckedOutputStream [3] which computes an Adler32
    checksum in a streaming fashion.

    # INTEGER NUMBERS

    All the ints and longs are serialized through methods like `DataOutput::writeInt()` and similar
    which results in BigEndian integers using two's complement representation.

    # STRINGS AND BUFFERS

    Strings and byte buffers use custom logic specified in the Jute module [4] which consists of outputting
    the integer length (using the same logic as for other integers) followed by the actual contents.
    A length of -1 signals a null value.
    
    [1] https://github.com/apache/zookeeper/blob/66202cb764c203f64b954a917e421be57d2ae67a/zookeeper-server/src/main/java/org/apache/zookeeper/server/persistence/FileSnap.java#L249
    [2] https://docs.oracle.com/javase%2F8%2Fdocs%2Fapi%2F%2F/java/io/DataOutputStream.html
    [3] https://github.com/apache/zookeeper/blob/66202cb764c203f64b954a917e421be57d2ae67a/zookeeper-server/src/main/java/org/apache/zookeeper/server/persistence/FileSnap.java#L248
    [4] https://github.com/apache/zookeeper/blob/66202cb764c203f64b954a917e421be57d2ae67a/zookeeper-jute/src/main/java/org/apache/jute/BinaryOutputArchive.java#L113
    '''

    def __init__(self, file_reader):
        self.input_stream = file_reader
        self.checksum = 1 # initial value of Adler32 checksum

    def eof(self):
        return len(self.input_stream.peek(1)) == 0

    def read_int(self, label):
        bytes = self.input_stream.read(4)
        if len(bytes) < 4:
            raise ValueError(f"Tried to parse integer {label} but encountered EOF")
        self.checksum = zlib.adler32(bytes, self.checksum)
        val, = struct.unpack('>i', bytes)
        return val

    def read_long(self, label):
        bytes = self.input_stream.read(8)
        if len(bytes) < 8:
            raise ValueError(f"Tried to parse long {label} but encountered EOF")
        self.checksum = zlib.adler32(bytes, self.checksum)
        val, = struct.unpack('>q', bytes)
        return val

    def read_string(self, label):
        val_len = self.read_int(f"{label}.length")
        if val_len == -1:
            return None
        bytes = self.input_stream.read(val_len)
        if len(bytes) < val_len:
            raise ValueError(f"Tried to parse string bytes for {label} but encountered EOF")
        self.checksum = zlib.adler32(bytes, self.checksum)
        val = bytes.decode("utf-8")
        return val

    def read_buffer(self, label):
        val_len = self.read_int(f"{label}.length")
        if val_len == -1:
            return None
        bytes = self.input_stream.read(val_len)
        if len(bytes) < val_len:
            raise ValueError(f"Tried to parse buffer bytes for {label} but encountered EOF")
        self.checksum = zlib.adler32(bytes, self.checksum)
        return bytes

    def read_checksum(self, label):
        checksum = self.read_long(label)
        if '/' != self.read_string(f"{label}.trailing_slash"):
            raise RuntimeError(f"{label} not followed by '/'!")
        return checksum

def snapshot_to_json(snapshot, timestamp_format = "numeric", znode_data_format = None):
    """
    Converts a parsed Zookeeper snapshot into a JSON-serializable dictionary.
    """
    def render_time(timestamp):
        if timestamp_format == "numeric":
            return timestamp
        elif timestamp_format == "iso":
            return datetime.fromtimestamp(timestamp / 1000, timezone.utc).isoformat() + "Z"
        else:
            raise ValueError(f"Invalid timestamp format: {timestamp_format}")

    def header_to_json(header):
        return {
            'magic': header['magic'],
            'version': header['version'],
            'db_id': hex(header['db_id'])
        }

    def session_to_json(session):
        return {
            'id': hex(session['id']),
            'timeout': session['timeout']
        }

    def stat_to_json(stat):
        return {
            'czxid': hex(stat['czxid']),
            'mzxid': hex(stat['mzxid']),
            'ctime': render_time(stat['ctime']),
            'mtime': render_time(stat['mtime']),
            'version': stat['version'],
            'cversion': stat['cversion'],
            'aversion': stat['aversion'],
            'ephemeralOwner': hex(stat['ephemeralOwner']),
            'pzxid': hex(stat['pzxid'])
        }

    def node_to_json(node):
        return {
            'path': node['path'],
            'data': format_znode_data(node['data'], znode_data_format),
            'acl': node['acl'],
            'stat': stat_to_json(node['stat'])
        }

    def digest_to_json(digest):
        return {
            'zxid': hex(digest['zxid']),
            'digest_version': digest['digest_version'],
            'digest': hex(digest['digest'])
        }

    return {
        'header': header_to_json(snapshot['header']),
        'sessions': [ session_to_json(s) for s in snapshot['sessions'] ],
        'ACLs': snapshot['ACLs'],
        'nodes': [ node_to_json(n) for n in snapshot['nodes'] ],
        'digest': digest_to_json(snapshot['digest'])
    }

def format_znode_data(node_data, format):
    if node_data == None:
        return None
    if format == "text":
        return node_data.decode("utf-8")
    if format == "base64":
        return b64encode(node_data).decode('utf-8')
    if format == "json":
        return json.loads(node_data.decode("utf-8")) if len(node_data) > 0 else None

def read_zookeeper_snapshot(file_path, znode_path_filter):
    """
    Reads a Zookeeper snapshot file, computes Adler32 checksums for each chunk of data,
    and parses the data into a dictionary.
    
    :param file_path: Path to the Zookeeper snapshot file
    :return: A tuple containing a list of Adler32 checksums and the parsed data dictionary
    """

    # try:
    with open(file_path, 'rb') as file:
        snapshot_reader = SnapshotReader(file)

        magic = snapshot_reader.read_int('magic')
        version = snapshot_reader.read_int('version')
        db_id = snapshot_reader.read_long('db_id')
        if magic != 1514885966:
            raise RuntimeError(f"Invalid file magic. Expected 1514885966 (\"ZKSN\") but got : {magic}")
        if version != 2:
            raise RuntimeError(f"Invalid snapshot version. Expected 2 but got : {version}")
        if db_id != -1:
            raise RuntimeError(f"Invalid DB_ID. Expected -1 but got : {db_id}")
        
        session_count = snapshot_reader.read_int('session_count')
        sessions = []
        for i in range(session_count):
            session_id = snapshot_reader.read_long('session_id')
            session_timeout = snapshot_reader.read_int('session_timeout')
            sessions.append({
                'id': session_id,
                'timeout': session_timeout
            })

        acl_cache_size = snapshot_reader.read_int('acl_cache_size')
        acl_cache = {}
        for i in range(acl_cache_size):
            acl_index = snapshot_reader.read_long('acl_index')
            acl_list_vector_len = snapshot_reader.read_int('acl_list_vector')
            acl_list = None
            if acl_list_vector_len != -1:
                acl_list = []
                for j in range(acl_list_vector_len):
                    perms = snapshot_reader.read_int('acl_perms')
                    scheme = snapshot_reader.read_string('acl_scheme')
                    acl_id = snapshot_reader.read_string('acl_id')
                    acl_list.append({
                        'perms': perms,
                        'id': {
                            'scheme': scheme,
                            'id': acl_id
                        }
                    })
            acl_cache[acl_index] = acl_list

        nodes = []
        while True:
            node_path = snapshot_reader.read_string("node_path")
            if node_path == '/':
                break
            node_data = snapshot_reader.read_buffer("node_data")
            node_acl = snapshot_reader.read_long("node_acl")
            czxid = snapshot_reader.read_long("node_czxid")
            mzxid = snapshot_reader.read_long("node_mzxid")
            ctime = snapshot_reader.read_long("node_ctime")
            mtime = snapshot_reader.read_long("node_mtime")
            node_version = snapshot_reader.read_int("node_version")
            cversion = snapshot_reader.read_int("node_cversion")
            aversion = snapshot_reader.read_int("node_aversion")
            ephemeralOwner = snapshot_reader.read_long("node_ephemeralOwner")
            pzxid = snapshot_reader.read_long("node_pzxid")
            if not znode_path_filter(node_path):
                continue
            node = {
                'path': node_path,
                'data': node_data,
                'acl': node_acl,
                'stat': {
                    'czxid': czxid,
                    'mzxid': mzxid,
                    'ctime': ctime,
                    'mtime': mtime,
                    'version': node_version,
                    'cversion': cversion,
                    'aversion': aversion,
                    'ephemeralOwner': ephemeralOwner,
                    'pzxid': pzxid,
                }
            }
            nodes.append(node)

        # first checksum following the tree nodes
        computed_checksum1 = snapshot_reader.checksum
        checksum1 = snapshot_reader.read_checksum("CHECKSUM_1")
        if checksum1 != computed_checksum1:
            raise RuntimeError(f"CHECKSUM_1 MISMATCH! computed = {computed_checksum1} expected {checksum1}")

        # digest
        # TODO digest output is configurable
        zxid = snapshot_reader.read_long("zxid")
        digest_version = snapshot_reader.read_int("digest_version")
        digest = snapshot_reader.read_long("digest")

        # second checksum following the digest
        computed_checksum2 = snapshot_reader.checksum
        checksum2 = snapshot_reader.read_checksum("CHECKSUM_2")
        if checksum2 != computed_checksum2:
            raise RuntimeError(f"CHECKSUM_2 MISMATCH! computed = {computed_checksum2} expected {checksum2}")

        # expected EOF
        if not snapshot_reader.eof():
            raise RuntimeError(f"Unexpected trailing data at the end of snapshot file!")

        return {
            'header': {
                'magic': magic,
                'version': version,
                'db_id': db_id
            },
            'sessions': sessions,
            'ACLs': acl_cache,
            'nodes': nodes,
            'digest': {
                'zxid': zxid,
                'digest_version': digest_version,
                'digest': digest
            }
        }
                
# except IOError as e:
#     print(f"Error reading file: {e}")
# except struct.error as e:
#     print(f"Error parsing data: {e}")
# except ValueError as e:
#     print(f"Value error: {e}")

def validate_adler32(file_path):
    # Define the buffer size for reading the file in chunks
    buffer_size = 65536  # 64KB

    # Initialize the Adler-32 checksum
    adler32_checksum = 1  # Starting value for Adler-32 checksum

    try:
        with open(file_path, 'rb') as f:
            # Move to the end of the file - 13 Bytes
            # - 8 bytes of Adler32 checksum
            # - 4 bytes for the length of a string
            # - 1 byte for the string '/'
            f.seek(-13, 2)  
            end_of_checksummed_section = f.tell()

            # parse the checksum serialized in the snapshot file
            checksum_bytes = f.read(8)
            if len(checksum_bytes) < 8:
                raise RuntimeError(f"File is too small!")
            expected_checksum, = struct.unpack('>q', checksum_bytes) # TODO SIGNED long?
            
            f.seek(0)  # Move back to the start of the file

            while f.tell() < end_of_checksummed_section:
                read_size = min(buffer_size, end_of_checksummed_section - f.tell())
                data = f.read(read_size)
                adler32_checksum = zlib.adler32(data, adler32_checksum)
        
        print(f"Expected Adler-32 checksum: {expected_checksum}")
        print(f"Computed Adler-32 checksum: {adler32_checksum}")
        if expected_checksum == adler32_checksum:
            print(f"All OK")
        else:
            raise RuntimeError(f"FILE CORRUPTED! (Checksum mismatch)")

    except IOError as e:
        print(f"Error reading file: {e}")
        return None

def parse_filename(basename):
    if not re.match(r'^snapshot\.[0-9a-fA-F]+$', basename):
        return None
    _,zxid = basename.split(".")
    return int(zxid, 16)

def is_subrange(contained, container):
    a,b = contained
    c,d = container
    return a >= c and b <= d

def validate_snapshot_complete(snapshot_filepath, txlog_dir):
    parsed_snapshot = read_zookeeper_snapshot(snapshot_filepath, lambda f: False)
    snapshot_tx_range = (
        # The zxid in the filename is the last transaction that we are sure happened BEFORE the snapshotting process
        # so to correctly restore the fuzzy snapshot we need the transactions starting from this zxid + 1.
        parse_filename(Path(snapshot_filepath).name) + 1,
        # The digest zxid is the zxid of the last transaction that was processed before the digest was computed.
        # To correctly restore the fuzzy snapshot we need at least all the transaction up to this zxid so that
        # we can recreate the same data tree and compute the digest for comparison.
        # Any tranasctions after this zxid are optional and simply provide us with a more up to date state.
        parsed_snapshot['digest']['zxid']
    )
    log_files = list_txlog_files(txlog_dir)
    available_continuous_tx_ranges = get_transaction_ranges(log_files)
    matches = [ r for r in available_continuous_tx_ranges if is_subrange(snapshot_tx_range, (r[0], r[1])) ]
    if len(matches) == 0:
        return { 'restorable': False }
    elif len(matches) > 1:
        raise RuntimeError("Log files overlap in transaction ranges!")
    else:
        return {
            'restorable': True,
            'log_files': [
                {
                    'name': Path(f['logfile']).name,
                    'tx_count': f['last'] - f['first'] + 1,
                    'lowest_zxid': f['first'],
                    'highest_zxid': f['last'],
                    'required': f['first'] <= snapshot_tx_range[1]
                }
                for f
                in matches[0][2]
            ]
        }
