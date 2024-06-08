import zlib
import struct
import json
from base64 import b64encode

decode_data_as_string = False

# The zookeeper snapshotting modules use the DataOutput::writeInt() and similar to serialize numbers. According to their documentation:
#
#    Writes an int value, which is comprised of four bytes, to the output stream. The byte values to be written, in the order shown, are:
#       (byte)(0xff & (v >> 24))
#       (byte)(0xff & (v >> 16))
#       (byte)(0xff & (v >>  8))
#       (byte)(0xff & v)
#
# The binary numbers are therefore BigEndian integers in two's complement format.

class SnapshotReader:

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


def read_zookeeper_snapshot(file_path):
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
            node_data = snapshot_reader.read_string("node_data") if decode_data_as_string else snapshot_reader.read_buffer("node_data")
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
            node = {
                'path': node_path,
                'data': node_data if decode_data_as_string else (f"data:;base64,{b64encode(node_data).decode('utf-8')}" if node_data else None),
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
            # TODO
            # nodes.append(node)

        # first checksum following the tree nodes
        computed_checksum1 = snapshot_reader.checksum
        checksum1 = snapshot_reader.read_long('CHECKSUM_1')
        if checksum1 != computed_checksum1:
            raise RuntimeError(f"CHECKSUM_1 MISMATCH! computed = {computed_checksum1} expected {checksum1}")
        if '/' != snapshot_reader.read_string('CHECKSUM_1.trailing_slash'):
            raise RuntimeError(f"CHECKSUM_1 not followed by '/'!")

        # TODO digest output is configurable
        # digest
        zxid = snapshot_reader.read_long("zxid")
        digest_version = snapshot_reader.read_int("digest_version")
        digest = snapshot_reader.read_long("digest")
        computed_checksum2 = snapshot_reader.checksum
        checksum2 = snapshot_reader.read_long('CHECKSUM_2')
        if checksum2 != computed_checksum2:
            raise RuntimeError(f"CHECKSUM_2 MISMATCH! computed = {computed_checksum2} expected {checksum2}")
        if '/' != snapshot_reader.read_string('CHECKSUM_2.trailing_slash'):
            raise RuntimeError(f"CHECKSUM_2 not followed by '/'!")

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


def main():
    file_path = '/Users/fghibellini/Downloads/snapshot.95e000ebfc1'  # Replace with the actual path to the snapshot file
    result = read_zookeeper_snapshot(file_path)
    print(json.dumps(result, indent=4))

if __name__ == '__main__':
    main()

