
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
usage: zookeeper_snapshot.py parse [-h] [--string-data] filename

positional arguments:
  filename       path to the snapshot file

options:
  -h, --help     show this help message and exit
  --string-data  decode the znode's data as utf-8 strings
```

### `validate`

Computes Adler32 checksum of the snapshot and validates that it matches the one persisted at the end of the file.

**Significantly faster than parsing the file.**

```
usage: zookeeper_snapshot.py validate [-h] filename

positional arguments:
  filename    path to the snapshot file

options:
  -h, --help  show this help message and exit
```

