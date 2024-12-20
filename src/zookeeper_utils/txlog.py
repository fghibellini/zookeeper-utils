import zlib
import struct
from enum import Enum
import pathlib
import re

# Normally the read() function on a stream will return only the data that can be retrieved with a single fetch.
# We typically know exactly how much data we need to proceed so we use this wrapper instead.
def read(reader, count):
    buffer = b""
    while len(buffer) < count:
        chunk = reader.read(count)
        if len(chunk) == 0: # EOF
            break
        buffer += chunk
    return buffer

class TxOpCodes(Enum):
    NOTIFICATION = 0
    CREATE = 1
    DELETE = 2
    EXISTS = 3
    GET_DATA = 4
    SET_DATA = 5
    GET_ACL = 6
    SET_ACL = 7
    GET_CHILDREN = 8
    SYNC = 9
    PING = 11
    GET_CHILDREN2 = 12
    CHECK = 13
    MULTI = 14
    CREATE2 = 15
    RECONFIG = 16
    CHECK_WATCHES = 17
    REMOVE_WATCHES = 18
    CREATE_CONTAINER = 19
    DELETE_CONTAINER = 20
    CREATE_TTL = 21
    MULTI_READ = 22
    AUTH = 100
    SET_WATCHES = 101
    SASL = 102
    GET_EPHEMERALS = 103
    GET_ALL_CHILDREN_NUMBER = 104
    SET_WATCHES2 = 105
    ADD_WATCH = 106
    WHO_AM_I = 107
    CREATE_SESSION = -10
    CLOSE_SESSION = -11
    ERROR = -1

class ExceptionCode(Enum):
    OK = 0
    SYSTEM_ERROR = -1
    RUNTIME_INCONSISTENCY = -2
    DATA_INCONSISTENCY = -3
    CONNECTION_LOSS = -4
    MARSHALLING_ERROR = -5
    UNIMPLEMENTED = -6
    OPERATION_TIMEOUT = -7
    BAD_ARGUMENTS = -8
    UNKNOWN_SESSION = -12
    NEW_CONFIG_NO_QUORUM = -13
    RECONFIG_IN_PROGRESS = -14
    API_ERROR = -100
    NO_NODE = -101
    NO_AUTH = -102
    BAD_VERSION = -103
    NO_CHILDREN_FOR_EPHEMERALS = -108
    NODE_EXISTS = -110
    NOT_EMPTY = -111
    SESSION_EXPIRED = -112
    INVALID_CALLBACK = -113
    INVALID_ACL = -114
    AUTH_FAILED = -115
    SESSION_MOVED = -118
    EPHEMERAL_ON_LOCAL_SESSION = -120

exception_code_lookup_dict = {member.value: member for member in ExceptionCode}


class TxLogReader:

    def __init__(self, file_reader):
        self.input_stream = file_reader

    def read_bool(self, label):
        bytes = self.input_stream.read(1)
        if len(bytes) < 1:
            raise ValueError(f"Tried to parse bool {label} but encountered EOF")
        val, = struct.unpack('>b', bytes)
        return val != 0

    def read_int(self, label):
        bytes = self.input_stream.read(4) # TODO can it return less than 4 simply because it would require multiple page fetches?
        if len(bytes) < 4:
            raise ValueError(f"Tried to parse integer {label} but encountered EOF")
        val, = struct.unpack('>i', bytes)
        return val

    def read_long(self, label):
        bytes = self.input_stream.read(8)
        if len(bytes) < 8:
            raise ValueError(f"Tried to parse long {label} but encountered EOF")
        val, = struct.unpack('>q', bytes)
        return val

    def read_string(self, label):
        val_len = self.read_int(f"{label}.length")
        if val_len == -1:
            return None
        bytes = self.input_stream.read(val_len)
        if len(bytes) < val_len:
            raise ValueError(f"Tried to parse string bytes for {label} but encountered EOF")
        val = bytes.decode("utf-8")
        return val

    def read_buffer(self, label):
        val_len = self.read_int(f"{label}.length")
        if val_len == -1:
            return None
        bytes = self.input_stream.read(val_len)
        if len(bytes) < val_len:
            raise ValueError(f"Tried to parse buffer bytes for {label} but encountered EOF")
        return bytes

    def current_position(self):
        return self.input_stream.tell()

    def skip(self, size):
        pos0 = self.input_stream.tell()
        self.input_stream.seek(size, 1)
        pos1 = self.input_stream.tell()
        if (pos1 - pos0) != size: # TODO this check will not work ast seek always succeeds and tell() will just report the index beyond EOF
            raise RuntimeError(f"Could not skip {size} bytes!")

    def tx_crc_test(self, expected_crc_val, data_len):
        pos = self.input_stream.tell()
        bytes = read(self.input_stream, data_len)
        if len(bytes) < data_len:
            # raise RuntimeError(f"Incomplete transaction - not enough data!")
            return False
        crc = zlib.adler32(bytes)
        tx_end_marker = self.input_stream.read(1)
        if tx_end_marker != b"\x42": # 'B'
            # raise RuntimeError(f"Incomplete transaction - no marker!")
            return False
        self.input_stream.seek(pos)
        return crc == expected_crc_val


def read_zookeeper_txlog(file_path):
    with open(file_path, 'rb') as file:
        txlog_reader = TxLogReader(file)

        magic = txlog_reader.read_int('magic')
        version = txlog_reader.read_int('version')
        db_id = txlog_reader.read_long('db_id')
        if magic != 1514884167: # "ZKLG"
            raise RuntimeError(f"Invalid file magic. Expected 1514884167 (\"ZKLG\") but got : {magic}")
        if version != 2:
            raise RuntimeError(f"Invalid txlog version. Expected 2 but got : {version}")
        # TODO 0 ?
        #if db_id != -1:
        #    raise RuntimeError(f"Invalid txlog DB_ID. Expected -1 but got : {db_id}")

        log_transactions = []
        while True:
            tx_crc = txlog_reader.read_long('tx_crc')
            tx_len = txlog_reader.read_int('tx_len')
            if not txlog_reader.tx_crc_test(tx_crc, tx_len):
                break # this is how we know we reached the end of the txLog
            tx_start_pos = txlog_reader.current_position()
            tx_client_id = txlog_reader.read_long('tx_client_id')
            tx_cxid = txlog_reader.read_int('tx_cxid')
            tx_zxid = txlog_reader.read_long('tx_zxid')
            tx_time = txlog_reader.read_long('tx_time')
            tx_type = txlog_reader.read_int('tx_type')
            tx_header = {
                "client_id": tx_client_id,
                "cxid": tx_cxid,
                "zxid": tx_zxid,
                "time": tx_time,
                "type": tx_type
            }
            # TODO other opcodes?
            if tx_type == TxOpCodes.ERROR.value:
                exception_code = txlog_reader.read_int('err')
                exception_opcode = exception_code_lookup_dict[exception_code]
                tx = {
                    'type': 'ERROR',
                    'error': exception_opcode.name
                }
            elif tx_type == TxOpCodes.SET_DATA.value:
                path = txlog_reader.read_string('path')
                data = txlog_reader.read_buffer('data')
                tx_set_data_version = txlog_reader.read_int('version')
                tx = {
                    'type': 'SET_DATA',
                    'path': path,
                    'data': '...',
                    'version': tx_set_data_version
                }
            elif tx_type == TxOpCodes.CREATE2.value or tx_type == TxOpCodes.CREATE.value:
                path = txlog_reader.read_string('path')
                data = txlog_reader.read_buffer('data')
                acl_len = txlog_reader.read_int('acl_len')
                acl_list = None
                if acl_len != -1:
                    acl_list = []
                    for i in range(acl_len):
                        perms = txlog_reader.read_int('perms')
                        scheme = txlog_reader.read_string('scheme')
                        id = txlog_reader.read_string('id')
                        acl_list.append({
                            'perms': perms,
                            'id': {
                                'scheme': scheme,
                                'id': id
                            }
                        })
                ephemeral = txlog_reader.read_bool('ephemeral')
                parent_cversion = txlog_reader.read_int('parent_cversion')
                tx = {
                    'type': 'CREATE2',
                    'path': path,
                    'data': '...',
                    'ephemeral': ephemeral,
                    'parent_cversion': parent_cversion
                }
            elif tx_type == TxOpCodes.CREATE_SESSION.value:
                timeout = txlog_reader.read_int('timeout')
                tx = {
                    'type': 'CREATE_SESSION',
                    'timeout': timeout
                }
            elif tx_type == TxOpCodes.CLOSE_SESSION.value:
                paths_2_delete_len = txlog_reader.read_int('paths_2_delete_len')
                paths_2_delete = None
                if paths_2_delete_len != -1:
                    paths_2_delete = []
                    for i in range(paths_2_delete_len):
                        path = txlog_reader.read_string('path2delete')
                        paths_2_delete.append(path)
                tx = {
                    'type': 'CLOSE_SESSION',
                    'paths2Delete': paths_2_delete
                }
            elif tx_type == TxOpCodes.DELETE.value:
                path = txlog_reader.read_string('path')
                tx = {
                    'type': 'DELETE',
                    'path': path
                }
            elif tx_type == TxOpCodes.MULTI.value:
                txns = None
                tx_count = txlog_reader.read_int('tx_count')
                if tx_count != -1:
                    txns = []
                    for i in range(tx_count):
                        tx_type = txlog_reader.read_int('tx_type')
                        tx_data = txlog_reader.read_buffer('tx_data')
                        txns.append({
                            'type': tx_type,
                            'data': '...'
                        })
                tx = {
                    'type': 'MULTI',
                    'txns': txns
                }
            else:
                raise ValueError(f"Unknown Tx type {tx_type}!")
            # print(json.dumps(tx, indent=4))
            # TODO V0 create

            tx_end_pos = txlog_reader.current_position()
            processed_tx_len =  tx_end_pos - tx_start_pos
            unprocessed_byte_count = tx_len - processed_tx_len

            digest = None
            # TODO zookeeper.digest.enabled
            # The digest is available only on some of the transaction (e.g. ERROR txs lack it).
            # You know if it's available by checking whether there is unprocessed data within
            # the transaction length after parsing the transaction record.
            if unprocessed_byte_count >= 12: # 12 bytes of digest
                tx_digest_version = txlog_reader.read_int("tx_digest_version")
                tx_tree_digest = txlog_reader.read_long("tx_tree_digest")
                digest = {
                    'version': tx_digest_version,
                    'tree_digest': tx_tree_digest
                }

            txlog_reader.skip(1) # end of transaction marker 0x42

            tx_rec = {
                'tx': tx,
                'header': tx_header,
                'digest': digest
            }

            # print(json.dumps(tx_rec, indent=4))
            log_transactions.append(tx_rec)
        #print(f"Read {len(log_transactions)} transactions!")
        # print(json.dumps(log_transactions, indent=4))
        return log_transactions


def list_txlog_files(dir):
    dir_path = pathlib.Path(dir)
    def parse_filename(basename):
        if not re.match(r'^log\.[0-9a-fA-F]+$', basename):
            return None
        _,zxid = basename.split(".")
        return int(zxid, 16)
    files = ((p, parse_filename(p.name)) for p in dir_path.iterdir())
    unsorted = [ (p,zxid) for (p,zxid) in files if zxid != None ]
    sorted_result = sorted(unsorted, key = lambda x: x[1])
    return [ str(p) for (p,zxid) in sorted_result ]

def get_zxid_range(filename, parsed_txlog):
    if len(parsed_txlog) < 1:
        return None
    return {
        'logfile': filename,
        'first': parsed_txlog[0]['header']['zxid'],
        'last': parsed_txlog[-1]['header']['zxid']
    }

def get_transaction_ranges(files):
    ranges = [ (x['first'], x['last'], [x]) for x in (get_zxid_range(file, read_zookeeper_txlog(file)) for file in files) if x != None ]
    if not ranges:
        return []
    merged = [ranges[0]]
    for start,end,files in ranges[1:]:
        top_start,top_end,top_files = merged[-1]
        if start == top_end + 1:
            merged[-1] = (top_start, end, top_files + files)
        else:
            merged.append((start,end,files))
    return merged
