from sys import argv
import argparse
import json
from zookeeper_utils.snapshot import read_zookeeper_snapshot, validate_snapshot_complete, validate_adler32, snapshot_to_json
from zookeeper_utils.filter_globs import path_matches_glob
from zookeeper_utils.txlog import read_zookeeper_txlog, list_txlog_files, get_transaction_ranges

def mk_filter_function_from_whitelist(patterns):
    def asterisk_whitelist_func(path): # optimized version for no filter
        return True
    def generic_whitelist_func(path):
        return any(path_matches_glob(path, b) for b in patterns)
    return asterisk_whitelist_func if patterns == ["*"] else generic_whitelist_func

def main():

    parser = argparse.ArgumentParser(description='Zookeeper snapshot utilities')
    subparsers = parser.add_subparsers(required=True)

    parser_parse_snapshot = subparsers.add_parser('parse-snapshot', help='parse a snapshot file')
    parser_parse_snapshot.set_defaults(subcmd="parse-snapshot")
    parser_parse_snapshot.add_argument('filename', help='path to the snapshot file')
    parser_parse_snapshot.add_argument('--path-include', dest='znode_path_include', nargs='*', help="Paths to include. Use * as wildcard value.")
    parser_parse_snapshot.add_argument('--timestamp-format', dest='timestamp_format', action='store',
    			choices=["numeric", "iso"],
                        default="iso",
                        help='format used to output timestamps. "numeric" will output timestamps as milliseconds since epoch. "iso" will output timestamps as ISO 8601 strings.')
    parser_parse_snapshot.add_argument('--data-format', dest='znode_data_format', action='store',
    			choices=["base64", "text", "json"],
                        default="text",
                        help='format used to output the znode\'s data. "text" will parse the data as UTF-8 strings. Keep in mind that ALL the znodes must be encodable in this format so if you specify "json" you need to make sure that all your znodes contain valid JSON. See --path-include to filter.')

    parser_parse_txlog = subparsers.add_parser('parse-log', help='parse a txlog file')
    parser_parse_txlog.set_defaults(subcmd="parse-log")
    parser_parse_txlog.add_argument('filename', help='path to the log file')

    parser_tx_ranges = subparsers.add_parser('transaction-ranges', help='scan the log files in a directory and output the contiguous ranges of available transactions')
    parser_tx_ranges.set_defaults(subcmd="transaction-ranges")
    parser_tx_ranges.add_argument('dir', help='directory with log files')

    parser_validate = subparsers.add_parser('checksum', help='computes an Adler32 checksum and compares it to the one at the end of the file')
    parser_validate.set_defaults(subcmd="checksum")
    parser_validate.add_argument('filename', help='path to the snapshot file')

    parser_validate = subparsers.add_parser('is-restorable', help='validates that a snapshot in conjuction with the log files can be restored into a valid state')
    parser_validate.set_defaults(subcmd="is-restorable")
    parser_validate.add_argument('--logdir', help='directory with log files', default='.') # TODO default should be directory containing snapshot
    parser_validate.add_argument('snapshot', help='path to snapshot file')

    args = parser.parse_args(args = ['--help'] if len(argv) <= 1 else argv[1:]) # TODO why is this hackery needed?

    if args.subcmd == 'parse-snapshot':
        znode_data_format = args.znode_data_format
        timestamp_format = args.timestamp_format
        file_path = args.filename
        path_whitelist = args.znode_path_include if len(args.znode_path_include or []) > 0 else ["*"]
        whitelist_func = mk_filter_function_from_whitelist(path_whitelist)
        result = read_zookeeper_snapshot(file_path, whitelist_func)
        json_tree = snapshot_to_json(
            result,
            znode_data_format = znode_data_format,
            timestamp_format = timestamp_format
            )
        print(json.dumps(json_tree, indent=4))
    elif args.subcmd == 'parse-log':
        file_path = args.filename
        result = read_zookeeper_txlog(file_path)
        print(json.dumps(result, indent=4))
    elif args.subcmd == 'transaction-ranges':
        logdir = args.dir
        tx_log_files = list_txlog_files(logdir)
        print(json.dumps(get_transaction_ranges(tx_log_files), indent=4))
    elif args.subcmd == 'checksum':
        file_path = args.filename
        validate_adler32(file_path)
    elif args.subcmd == 'is-restorable':
        logdir = args.logdir
        snapshot_file = args.snapshot
        print(json.dumps(validate_snapshot_complete(snapshot_file, logdir), indent=4))

if __name__ == '__main__':
    main()
