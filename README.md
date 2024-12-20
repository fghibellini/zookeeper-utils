
# Zookeeper Utils

> 🚧 WORK IN PROGRESS 🚧
>
> Very much work in progress. I mostly got it to a point where it was able to process all the snapshots that we have at work.

- [x] Inspect snapshots & transaction logs (currently only works with snapshots generated with `zookeeper.digest.enabled = true`, `zookeeper.serializeLastProcessedZxid.enabled = false` - this was introduced in ZK 3.9.0 prior versions would behave as if it was false)
- [x] Compute integrity checks

Provided both as 🐍 **Python library** and 💻 **CLI**.

## CLI

```
usage: zookeeper_snapshot.py [-h] {parse,checksum,validate-is-restorable} ...

Zookeeper snapshot utilities

positional arguments:
  {parse,checksum,validate}
    parse                     parse a snapshot file
    checksum                  computes an Adler32 checksum and compares it to the one at the end of the file
    validate-is-restorable    Validates that a snapshot in conjuction with the log files can be restored into a valid state

options:
  -h, --help            show this help message and exit
```

### Commands

#### `parse-snapshot`

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

<details>

<summary>example output</summary>

```json
{
    "header": {
        "magic": 1514885966,
        "version": 2,
        "db_id": "-0x1"
    },
    "sessions": [
        {
            "id": "0x100004a14420000",
            "timeout": 30000
        }
    ],
    "ACLs": {
        "1": [
            {
                "perms": 1,
                "id": {
                    "scheme": "world",
                    "id": "anyone"
                }
            }
        ],
        "2": [
            {
                "perms": 31,
                "id": {
                    "scheme": "world",
                    "id": "anyone"
                }
            }
        ]
    },
    "nodes": [
        {
            "path": "",
            "data": "",
            "acl": -1,
            "stat": {
                "czxid": "0x0",
                "mzxid": "0x0",
                "ctime": "1970-01-01T00:00:00+00:00Z",
                "mtime": "1970-01-01T00:00:00+00:00Z",
                "version": 0,
                "cversion": 1,
                "aversion": 0,
                "ephemeralOwner": "0x0",
                "pzxid": "0x2"
            }
        },
        {
            "path": "/zookeeper",
            "data": "",
            "acl": -1,
            "stat": {
                "czxid": "0x0",
                "mzxid": "0x0",
                "ctime": "1970-01-01T00:00:00+00:00Z",
                "mtime": "1970-01-01T00:00:00+00:00Z",
                "version": 0,
                "cversion": 0,
                "aversion": 0,
                "ephemeralOwner": "0x0",
                "pzxid": "0x0"
            }
        },
        {
            "path": "/zookeeper/config",
            "data": "",
            "acl": 1,
            "stat": {
                "czxid": "0x0",
                "mzxid": "0x0",
                "ctime": "1970-01-01T00:00:00+00:00Z",
                "mtime": "1970-01-01T00:00:00+00:00Z",
                "version": 0,
                "cversion": 0,
                "aversion": -1,
                "ephemeralOwner": "0x0",
                "pzxid": "0x0"
            }
        },
        {
            "path": "/zookeeper/quota",
            "data": "",
            "acl": -1,
            "stat": {
                "czxid": "0x0",
                "mzxid": "0x0",
                "ctime": "1970-01-01T00:00:00+00:00Z",
                "mtime": "1970-01-01T00:00:00+00:00Z",
                "version": 0,
                "cversion": 0,
                "aversion": 0,
                "ephemeralOwner": "0x0",
                "pzxid": "0x0"
            }
        },
        {
            "path": "/top-level-node",
            "data": "",
            "acl": 2,
            "stat": {
                "czxid": "0x2",
                "mzxid": "0x2",
                "ctime": "2024-12-20T18:33:40.691000+00:00Z",
                "mtime": "2024-12-20T18:33:40.691000+00:00Z",
                "version": 0,
                "cversion": 1,
                "aversion": 0,
                "ephemeralOwner": "0x0",
                "pzxid": "0x3"
            }
        },
        {
            "path": "/top-level-node/child-1",
            "data": "Hello World",
            "acl": 2,
            "stat": {
                "czxid": "0x3",
                "mzxid": "0x3",
                "ctime": "2024-12-20T18:33:51.119000+00:00Z",
                "mtime": "2024-12-20T18:33:51.119000+00:00Z",
                "version": 0,
                "cversion": 0,
                "aversion": 0,
                "ephemeralOwner": "0x0",
                "pzxid": "0x3"
            }
        }
    ],
    "digest": {
        "zxid": "0x3",
        "digest_version": 2,
        "digest": "0xcfe60d81"
    }
}
```

</details>

#### `parse-log`

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

<details>

<summary>example output</summary>

```json
[
    {
        "tx": {
            "type": "CREATE_SESSION",
            "timeout": 30000
        },
        "header": {
            "client_id": 72057970402459648,
            "cxid": 0,
            "zxid": 1,
            "time": 1734720478611,
            "type": -10
        },
        "digest": {
            "version": 2,
            "tree_digest": 1371985504
        }
    },
    {
        "tx": {
            "type": "CREATE2",
            "path": "/top-level-node",
            "data": "...",
            "ephemeral": false,
            "parent_cversion": 1
        },
        "header": {
            "client_id": 72057970402459648,
            "cxid": 2,
            "zxid": 2,
            "time": 1734720499679,
            "type": 1
        },
        "digest": {
            "version": 2,
            "tree_digest": 2853959157
        }
    },
    {
        "tx": {
            "type": "CREATE2",
            "path": "/top-level-node/child-1",
            "data": "...",
            "ephemeral": false,
            "parent_cversion": 1
        },
        "header": {
            "client_id": 72057970402459648,
            "cxid": 3,
            "zxid": 3,
            "time": 1734720504813,
            "type": 1
        },
        "digest": {
            "version": 2,
            "tree_digest": 1446474057
        }
    }
]
```

</details>

#### `validate-is-restorable`

Extracts the last committed zxid when the snapshot started being generated from the snapshot filename (`LOWEST_ZXID`) and the zxid in the data-tree
digest computed at the end of the snapshot generation process (`HIGHEST_ZXID`). It then goes over the available log files and checks that all the transactions
between `LOWEST_ZXID` and `HIGHEST_ZXID` (inclusive) are available which is a requirement in order to correctly restore the state of ZooKeeper.

```
$ python3 zookeeper_snapshot.py validate-is-restorable ~/Downloads/snapshot.95e000ebfc1 --logdir ~/Downloads | jq
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

#### `checksum`

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

## Library

```python
from zookeeper_utils import list_txlog_files, get_transaction_ranges, read_zookeeper_txlog, validate_snapshot_complete, validate_adler32, read_zookeeper_snapshot
```

**TODO** Until reference docs are made available see the module [`cli.py`](./src/zookeeper_utils/cli.py) for examples of invocations.

### Development

#### Setup

- This project has a `pyproject.toml` file (see [StackOverflow Answer](https://stackoverflow.com/a/66472800/3343425)).
- Inside of it, we declare that we use [setup-tools](https://setuptools.pypa.io/en/latest/userguide/quickstart.html) as our _build-backend_.

#### Running the CLI

To run the CLI tool directly from the project directory you can take advantage of [setup-tool's Development Mode](https://setuptools.pypa.io/en/latest/userguide/development_mode.html):

```
python -m venv .venv
source .venv/bin/activate
pip install -e .

# you should now be able to invoke the CLI
# Any edits to the source code will be reflected in the next invocation
# (no need to reinstall)
zookeeper-utils --help
```

#### How to Generate a ZooKeeper Snapshot

```
$ mkdir -p example/{data,logs};

$ docker run -d \
  --name example-zookeeper \
  --restart always \
  -v $(pwd)/example/data:/data \
  -v /Users/fghibellini/code/zookeeper-snapshot-python/example/logs:/datalog \
  -e ZOO_CFG_EXTRA="serializeLastProcessedZxid.enabled=false preAllocSize=1" \
  zookeeper:3.9.3

$ docker exec -it example-zookeeper zkCli.sh
...
[zk: localhost:2181(CONNECTED) 0] ls /
[zookeeper]
[zk: localhost:2181(CONNECTED) 1] create /top-level-node ""
Created /top-level-node
[zk: localhost:2181(CONNECTED) 2] create /top-level-node/child-1 "Hello World"
Created /top-level-node/child-1
[zk: localhost:2181(CONNECTED) 3] <CTRL-D>
2024-12-20 18:33:52,684 [myid:] - INFO  [main:o.a.z.u.ServiceUtils@45] - Exiting JVM with code 0

# now we need to restart zookeeper in order to force it to generate a snapshot (it generates one on startup)
$ docker rm -f example-zookeeper
$ docker run -d \
  --name example-zookeeper \
  --restart always \
  -v $(pwd)/example/data:/data \
  -v /Users/fghibellini/code/zookeeper-snapshot-python/example/logs:/datalog \
  -e ZOO_CFG_EXTRA="serializeLastProcessedZxid.enabled=false preAllocSize=1" \
  zookeeper:3.9.3

$ rm example/data/version-2/snapshot.0 # the first snapshot is empty
$ zk-utils parse-snapshot example/data/version-2/snapshot.*
{
    "header": {
        "magic": 1514885966,
        "version": 2,
        "db_id": "-0x1"
    },
...
```
