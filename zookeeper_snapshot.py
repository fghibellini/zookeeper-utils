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
        header_bytes = file.read(16)
        if len(header_bytes) < 16:
            raise ValueError("Invalid snapshot file: header too short")
        checksums.append(zlib.adler32(header_bytes))
        
        _magic, version, db_id = struct.unpack('>4siq', header_bytes)
        magic = _magic.decode("utf-8")
        if magic != "ZKSN":
            raise RuntimeError(f"Invalid file magic. Expected ZKSN but got : {_magic}")
        if version != 2:
            raise RuntimeError(f"Invalid snapshot version. Expected 2 but got : {version}")
        
        # print(f"Magic Number: {magic}, Version: {version}, DB_ID: {db_id}")

        session_count_bytes = file.read(4)
        if len(session_count_bytes) < 4:
            raise ValueError("Expected session count but reached EOF")
        checksums.append(zlib.adler32(session_count_bytes, checksum))
        session_count, = struct.unpack('>i', session_count_bytes)
        print(f"session count: {session_count}")
        sessions = []
        for i in range(session_count):
            session_bytes = file.read(12)
            if len(session_bytes) < 12:
                raise ValueError("Expected session of 12 bytes but reached EOF")
            checksums.append(zlib.adler32(session_bytes, checksum))
            session_id, session_timeout = struct.unpack('>qi', session_bytes)
            sessions.append({
                'id': session_id,
                'timeout': session_timeout
            })

        acl_cloned_long_keymap_size_bytes = file.read(4)
        if len(acl_cloned_long_keymap_size_bytes) < 4:
            raise ValueError("Expected ACL cloned long keymap size but reached EOF")
        checksums.append(zlib.adler32(acl_cloned_long_keymap_size_bytes, checksum))
        cloned_long_keymap_size, = struct.unpack('>i', acl_cloned_long_keymap_size_bytes)
        print(f"acl_cloned_long_keymap_size: {acl_cloned_long_keymap_size_bytes}")
        acl_lists = {}
        for i in range(cloned_long_keymap_size):
            # ac_list_id
            acl_index_bytes = file.read(8)
            if len(acl_index_bytes) < 8:
                raise ValueError("Expected ACL index but reached EOF")
            checksums.append(zlib.adler32(acl_index_bytes, checksum))
            acl_index, = struct.unpack('>q', acl_index_bytes)
            # ac_list_vector
            acl_list_vector_len_bytes = file.read(4)
            if len(acl_list_vector_len_bytes) < 4:
                raise ValueError("Expected ACL list vector length but got EOF")
            checksums.append(zlib.adler32(acl_list_vector_len_bytes, checksum))
            acl_list_vector_len, = struct.unpack('>i', acl_list_vector_len_bytes)
            # TODO acl_list_vector_len == -1 ???
            acl_list = []
            acl_lists[acl_index] = acl_list
            for j in range(acl_list_vector_len):
                # perms
                perms_bytes = file.read(4)
                if len(perms_bytes) < 4:
                    raise ValueError("Expected ACL perms but got EOF")
                checksums.append(zlib.adler32(perms_bytes, checksum))
                perms, = struct.unpack('>i', perms_bytes)
                # scheme
                scheme_length_bytes = file.read(4)
                if len(scheme_length_bytes) < 4:
                    raise ValueError("Expected scheme length but got EOF")
                checksums.append(zlib.adler32(scheme_length_bytes, checksum))
                scheme_length, = struct.unpack('>i', scheme_length_bytes)
                scheme = None
                if scheme_length != -1:
                    scheme_bytes = file.read(scheme_length)
                    if len(scheme_bytes) < scheme_length:
                        raise ValueError("Expected scheme bytes but got EOF")
                    checksums.append(zlib.adler32(scheme_bytes, checksum))
                    scheme = scheme_bytes.decode("utf-8")
                # id
                acl_id_length_bytes = file.read(4)
                if len(acl_id_length_bytes) < 4:
                    raise ValueError("Expected acl_id_bytes but got EOF")
                checksums.append(zlib.adler32(acl_id_length_bytes, checksum))
                acl_id_length, = struct.unpack('>i', acl_id_length_bytes)
                acl_id = None
                if acl_id_length != -1:
                    acl_id_bytes = file.read(acl_id_length)
                    if len(acl_id_bytes) < acl_id_length:
                        raise ValueError("Expected acl_id_bytes but got EOF")
                    checksums.append(zlib.adler32(acl_id_bytes, checksum))
                    acl_id = acl_id_bytes.decode("utf-8")
                acl_list.append({
                    'perms': perms,
                    'id': {
                        'scheme': scheme,
                        'id': acl_id
                    }
                })
                




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
            'ACLs': acl_lists
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

