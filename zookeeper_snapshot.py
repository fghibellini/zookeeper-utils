import zlib
import struct
import json

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

    try:
        with open(file_path, 'rb') as file:
            header_bytes = file.read(16)
            if len(header_bytes) < 16:
                raise ValueError("Invalid snapshot file: header too short")
            checksums.append(zlib.adler32(header_bytes))
            
            _magic, version, db_id = struct.unpack('>4sIQ', header_bytes)
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
            session_count, = struct.unpack('>I', session_count_bytes)
            print(f"session count: {session_count}")
            sessions = []
            for i in range(session_count):
                session_bytes = file.read(12)
                if len(session_bytes) < 12:
                    raise ValueError("Expected session of 12 bytes but reached EOF")
                checksums.append(zlib.adler32(session_bytes, checksum))
                session_id, session_timeout = struct.unpack('>QI', session_bytes)
                sessions.append({
                    'id': session_id,
                    'timeout': session_timeout
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
                'sessions': sessions
            }
                    
    except IOError as e:
        print(f"Error reading file: {e}")
    except struct.error as e:
        print(f"Error parsing data: {e}")
    except ValueError as e:
        print(f"Value error: {e}")

    return nodes

def main():
    file_path = '/Users/fghibellini/Downloads/snapshot.95e000ebfc1'  # Replace with the actual path to the snapshot file
    result = read_zookeeper_snapshot(file_path)
    print(json.dumps(result, indent=4))

if __name__ == '__main__':
    main()

