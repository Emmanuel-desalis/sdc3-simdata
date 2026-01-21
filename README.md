# SDC3-SimData Public Access Tool

This is the readme for the minimal Python script to **list, inspect, and download** the public dataset `PRSIME-21`, also called `sdc3-simdata` hosted on CSCS RGW (Ceph, S3-compatible) **without credentials**.
Metadata of the PRISME-21 dataset are described in the DATA_DICTIONARY.md file. 

## Purpose
This script is **exclusively for accessing the `sdc3-simdata` dataset**. It uses unsigned S3 REST calls over HTTPS.

> **Important:** Constants (`DEFAULT_ENDPOINT`, `DEFAULT_REGION`, `DEFAULT_BUCKET`, `DEFAULT_TENANT`) are fixed for this dataset. Do **not** change them unless debugging.

---

## Requirements
- Python 3.9+
- Internet access to `https://rgw.cscs.ch`
- No external dependencies (standard library only)

---

## CLI Arguments
```
--endpoint   RGW endpoint (default: https://rgw.cscs.ch)
--region     Informational only (default: cscs-zonegroup)
--bucket     Bucket name (default: sdc3-simdata)
--tenant     Tenant name (default: ska)
--dest       Local destination for downloads (default: ./download)
--prefix     Subfolder to scope listing/downloading (e.g., 'SDC3/')
--all        Download entire bucket
--tree       Show directory tree with file counts and sizes
--list       Recursively list all files (optionally under --prefix)
--ascii      Use ASCII characters for tree output
```

---

## Quick Examples

### 1) List all objects
```bash
python sdc3_simdata_downloader.py --list
```

### 2) Show directory tree
```bash
python sdc3_simdata_downloader.py --tree
```

ASCII-only tree (safe for .txt):
```bash
python sdc3_simdata_downloader.py --tree --ascii > tree.txt
```

### 3) Download a subset
```bash
python sdc3_simdata_downloader.py --prefix "SDC3/" --dest ./subset
```

### 4) Download entire dataset
```bash
python sdc3_simdata_downloader.py --all --dest ./sdc3_all
```

---

## Troubleshooting

- **403 AccessDenied when listing**
  - Anonymous listing requires `s3:ListBucket`. If disabled, you can still download known paths using `--prefix`.

- **Encoding issues in tree output**
  - Use `--ascii` when redirecting to `.txt`.

- **Slow output for large listings**
  - Pipe to file or filter with `| head` or `| grep pattern`.

---

## Notes
- Multi-tenancy requires addressing bucket as `TENANT:BUCKET` (e.g., `ska:sdc3-simdata`).
- Downloads skip existing files if sizes match.

---

## License
TBD
