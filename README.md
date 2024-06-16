
# Zookeeper Snapshot Utility

```
usage: zookeeper_snapshot.py [-h] {parse,validate} ...

Zookeeper snapshot utilities

positional arguments:
  {parse,validate}
    parse           parse a snapshot file
    validate        computes an Adler32 checksum and compares it to the one at the end of the file

options:
  -h, --help        show this help message and exit
```

## Commands

### `parse`

Parses a snapshot file and outputs in JSON format. Ideal for piping into [jq](https://jqlang.github.io/jq/) for further processing.

This fails if any of the data is in the wrong format or if the checksums don't match.

```
usage: zookeeper_snapshot.py parse [-h] [--path-include [ZNODE_PATH_INCLUDE ...]] [--data-format {base64,text,json}] filename

positional arguments:
  filename              path to the snapshot file

options:
  -h, --help            show this help message and exit
  --path-include [ZNODE_PATH_INCLUDE ...]
                        Paths to include. Use * as wildcard value.
  --data-format {base64,text,json}
                        format used to output the znode's data. "text" will parse the data as UTF-8 strings. Keep in mind that ALL the znodes must be encodable in this format so if you specify "json" you need to
                        make sure that all your znodes contain valid JSON. See --path-include to filter.
```

### `validate`

Extracts the last committed zxid when the snapshot started being generated from the snapshot filename (`LOWEST_ZXID`) and the zxid in the data-tree
digest computed at the end of the snapshot generation process (`HIGHEST_ZXID`). It then goes over the available log files and checks that all the transactions
between `LOWEST_ZXID` and `HIGHEST_ZXID` (inclusive) are available which is a requirement in order to correctly restore the state of ZooKeeper.

```
$ python3 zookeeper_snapshot.py validate ~/Downloads/snapshot.95e000ebfc1 --logdir ~/Downloads | jq
{
  "restorable": true,
  "log_files": [
    {
      "name": "log.95e000d8b9e",
      "tx_count": 78885,
      "lowest_zxid": 10299332463518,
      "highest_zxid": 10299332542402,
      "required": true
    },
    {
      "name": "log.95e000ebfc3",
      "tx_count": 11683,
      "lowest_zxid": 10299332542403,
      "highest_zxid": 10299332554085,
      "required": true
    }
  ]
}
```

### `checksum`

Computes Adler32 checksum of the snapshot and validates that it matches the one persisted at the end of the file.
This can be used to check that the snapshot written fully - a common problem given that ZooKeeper makes no attempt
at not exposing the snapshot files as they are beeing generated.

**Significantly faster than parsing the file.**

```
usage: zookeeper_snapshot.py checksum [-h] filename

positional arguments:
  filename    path to the snapshot file

options:
  -h, --help  show this help message and exit
```

