import re
import zlib
import struct
import json
from base64 import b64encode
import argparse
from filter_globs import path_matches_glob
from zookeeper_txlog import list_txlog_files, get_transaction_ranges
from pathlib import Path

# This module was written by following the serialization logic of `FileSnap::serialize()`[1].
#
# [1] https://github.com/apache/zookeeper/blob/66202cb764c203f64b954a917e421be57d2ae67a/zookeeper-server/src/main/java/org/apache/zookeeper/server/persistence/FileSnap.java#L242

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

def format_znode_data(node_data, format):
    if node_data == None:
        return None
    if format == "text":
        return node_data.decode("utf-8")
    if format == "base64":
        return b64encode(node_data).decode('utf-8')
    if format == "json":
        return json.loads(node_data.decode("utf-8")) if len(node_data) > 0 else None

def read_zookeeper_snapshot(file_path, znode_data_format, znode_path_filter):
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
                'data': format_znode_data(node_data, znode_data_format),
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

def path_include_patterns_to_filter_func(patterns):
    def asterisk_whitelist_func(path): # optimized version for no filter
        return True
    def generic_whitelist_func(path):
        return any(path_matches_glob(path, b) for b in patterns)
    return asterisk_whitelist_func if patterns == ["*"] else generic_whitelist_func

def main():

    print(validate_snapshot_complete("/Users/fghibellini/Downloads/snapshot.95e000ebfc1", "/Users/fghibellini/Downloads"))
    return

    parser = argparse.ArgumentParser(description='Zookeeper snapshot utilities')
    subparsers = parser.add_subparsers(required=True)

    parser_parse = subparsers.add_parser('parse', help='parse a snapshot file')
    parser_parse.set_defaults(subcmd="parse")
    parser_parse.add_argument('filename', help='path to the snapshot file')
    parser_parse.add_argument('--path-include', dest='znode_path_include', nargs='*', help="Paths to include. Use * as wildcard value.")
    parser_parse.add_argument('--data-format', dest='znode_data_format', action='store',
    			choices=["base64", "text", "json"],
                        default="text",
                        help='format used to output the znode\'s data. "text" will parse the data as UTF-8 strings. Keep in mind that ALL the znodes must be encodable in this format so if you specify "json" you need to make sure that all your znodes contain valid JSON. See --path-include to filter.')

    parser_parse = subparsers.add_parser('validate', help='computes an Adler32 checksum and compares it to the one at the end of the file')
    parser_parse.set_defaults(subcmd="validate")
    parser_parse.add_argument('filename', help='path to the snapshot file')

    args = parser.parse_args()

    if args.subcmd == 'parse':
        znode_data_format = args.znode_data_format
        file_path = args.filename
        path_whitelist = args.znode_path_include if len(args.znode_path_include) > 0 else ["*"]
        whitelist_func = path_include_patterns_to_filter_func(path_whitelist)
        result = read_zookeeper_snapshot(file_path, znode_data_format, whitelist_func)
        print(json.dumps(result, indent=4))
    elif args.subcmd == 'validate':
        file_path = args.filename
        validate_adler32(file_path)

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
    parsed_snapshot = read_zookeeper_snapshot(snapshot_filepath, 'base64', lambda f: False)
    snapshot_tx_range = (parse_filename(Path(snapshot_filepath).name), parsed_snapshot['digest']['zxid'])
    print(f"snapshot_tx_range: {snapshot_tx_range}")
    log_files = list_txlog_files(txlog_dir)
    available_continuous_tx_ranges = get_transaction_ranges(log_files)
    print(f"available_continuous_tx_ranges: {available_continuous_tx_ranges}")
    return any(is_subrange(snapshot_tx_range, r) for r in available_continuous_tx_ranges)

if __name__ == '__main__':
    main()

