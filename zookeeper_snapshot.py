import zlib
import struct
import json


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
        self.checksum = 0 # initial value of Adler32 checksum

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
        bytes = self.input_stream.read(val_len)
        if len(bytes) < val_len:
            raise ValueError(f"Tried to parse string bytes for {label} but encountered EOF")
        self.checksum = zlib.adler32(bytes, self.checksum)
        val = bytes.decode("utf-8")
        return val


def read_zookeeper_snapshot(file_path):
    """
    Reads a Zookeeper snapshot file, computes Adler32 checksums for each chunk of data,
    and parses the data into a dictionary.
    
    :param file_path: Path to the Zookeeper snapshot file
    :return: A tuple containing a list of Adler32 checksums and the parsed data dictionary
    """
    checksum = 0
    checksums = []
    nodes = {}
    chunk_size = 1024  # Adjust the chunk size as needed

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
            # TODO acl_list_vector_len == -1 ???
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

        #while True:
        #    chunk = file.read(chunk_size)
        #    if not chunk:
        #        break
        #    checksum = zlib.adler32(chunk)
        #    checksums.append(checksum)
        #    
        #    offset = 0
        #    while offset < len(chunk):
        #        if len(chunk) - offset < 16:
        #            break  # Not enough data for a full node header

        #        node_id, parent_id, data_length = struct.unpack('qqi', chunk[offset:offset + 20])
        #        offset += 20
        #        
        #        if len(chunk) - offset < data_length:
        #            break  # Not enough data for the full node data

        #        data = chunk[offset:offset + data_length].decode('utf-8')
        #        offset += data_length

        #        nodes[node_id] = {'parent_id': parent_id, 'data': data}
        return {
            'header': {
                'magic': magic,
                'version': version,
                'db_id': db_id
            },
            'sessions': sessions,
            'ACLs': acl_cache
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

