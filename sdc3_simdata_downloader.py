
#!/usr/bin/env python3
# Public downloader for CSCS Ceph RGW (S3-compatible) WITHOUT boto3.
# --list now recursively lists ALL objects (optionally under --prefix).
# --list-top (optional) keeps the former top-level "subfolders + root files" view.

import argparse
import os
import sys
import time
import urllib.parse
import urllib.request
import xml.etree.ElementTree as ET
from typing import Dict, Iterable, List, Optional, Tuple

DEFAULT_ENDPOINT = "https://rgw.cscs.ch"
DEFAULT_REGION = "cscs-zonegroup"  # Not used in unsigned REST; kept for reference.
DEFAULT_BUCKET = "sdc3-simdata"
DEFAULT_TENANT = "ska"

# -------- HTTP helpers --------

def http_get(url: str, headers: Optional[Dict[str, str]] = None, stream_to: Optional[str] = None) -> bytes:
    """GET an unsigned HTTP(S) URL. If stream_to is set, write to file incrementally."""
    req = urllib.request.Request(url, headers=headers or {})
    try:
        with urllib.request.urlopen(req) as resp:
            if stream_to:
                os.makedirs(os.path.dirname(stream_to), exist_ok=True)
                with open(stream_to, "wb") as f:
                    while True:
                        chunk = resp.read(1024 * 1024)
                        if not chunk:
                            break
                        f.write(chunk)
                return b""
            else:
                return resp.read()
    except urllib.error.HTTPError as e:
        body = e.read().decode("utf-8", "ignore")
        raise RuntimeError(f"HTTP {e.code} {e.reason} for {url}\n{body}") from None
    except urllib.error.URLError as e:
        raise RuntimeError(f"Network error for {url}: {e}") from None

def build_bucket_base_url(endpoint: str, tenant: str, bucket: str) -> str:
    # RGW public path-style with tenant:bucket in the path
    tb = f"{tenant}:{bucket}" if tenant else bucket
    return endpoint.rstrip("/") + "/" + urllib.parse.quote(tb, safe=":")  # keep colon

# -------- S3 ListObjectsV2 parsing --------

def list_objects_v2(
    base_url: str,
    prefix: str = "",
    delimiter: Optional[str] = None,
    continuation_token: Optional[str] = None,
    max_keys: int = 1000,
) -> Dict:
    """
    Call S3 ListObjectsV2 anonymously and return a dict with:
    - 'keys': List[Tuple[key, size]]
    - 'common_prefixes': List[str] (when delimiter='/')
    - 'is_truncated': bool
    - 'next_token': Optional[str]
    """
    params = {"list-type": "2", "max-keys": str(max_keys)}
    if prefix:
        params["prefix"] = prefix
    if delimiter:
        params["delimiter"] = delimiter
    if continuation_token:
        params["continuation-token"] = continuation_token

    url = base_url + "?" + urllib.parse.urlencode(params, safe="/:+")
    data = http_get(url)
    root = ET.fromstring(data)
    ns = ""
    if root.tag.startswith("{"):
        ns = root.tag.split("}")[0] + "}"

    def text(elem, name):
        x = elem.find(ns + name)
        return x.text if x is not None else None

    keys: List[Tuple[str, int]] = []
    for c in root.findall(ns + "Contents"):
        k = text(c, "Key")
        s = text(c, "Size")
        if k is None:
            continue
        size = int(s) if s is not None else -1
        keys.append((k, size))

    cps: List[str] = []
    for cp in root.findall(ns + "CommonPrefixes"):
        p = text(cp, "Prefix")
        if p:
            cps.append(p)

    is_truncated = (text(root, "IsTruncated") or "").lower() == "true"
    next_token = text(root, "NextContinuationToken")

    return {
        "keys": keys,
        "common_prefixes": cps,
        "is_truncated": is_truncated,
        "next_token": next_token,
    }

def iter_all_objects(base_url: str, prefix: str = "") -> Iterable[Tuple[str, int]]:
    token = None
    while True:
        page = list_objects_v2(base_url, prefix=prefix, continuation_token=token)
        for k in page["keys"]:
            yield k
        if not page["is_truncated"]:
            break
        token = page["next_token"]

# -------- Download utilities --------

def ensure_parent(path: str):
    d = os.path.dirname(path)
    if d:
        os.makedirs(d, exist_ok=True)

def download_object(base_url: str, key: str, dest_root: str, size: Optional[int]) -> Tuple[str, bool]:
    local_path = os.path.join(dest_root, key)
    ensure_parent(local_path)
    # Skip if size matches (best-effort)
    if size is not None and os.path.exists(local_path):
        try:
            if os.path.getsize(local_path) == int(size):
                return local_path, False
        except OSError:
            pass
    url = base_url + "/" + urllib.parse.quote(key, safe="/:+")
    http_get(url, stream_to=local_path)
    return local_path, True

# -------- Listing helpers --------

def list_top_level(base_url: str) -> Tuple[List[str], List[str]]:
    """Top-level 'subfolders' and root files (kept for --list-top)."""
    page = list_objects_v2(base_url, delimiter="/")
    folders = sorted(page["common_prefixes"])
    root_files = sorted([k for (k, _) in page["keys"] if "/" not in k])
    return folders, root_files

def list_recursive(base_url: str, prefix: str = "") -> Tuple[int, int]:
    """
    Recursively list ALL objects (optionally only under 'prefix').
    Prints "size  key" for each object. Returns (count, total_bytes).
    """
    count = 0
    total_bytes = 0
    for key, size in iter_all_objects(base_url, prefix=prefix):
        print(f"{size:>12}  {key}")
        count += 1
        if size > 0:
            total_bytes += size
    return count, total_bytes


# -------- TREE (hierarchy with aggregated counts/sizes) --------

class TreeNode:
    __slots__ = ("name", "children", "file_count", "total_size")
    def __init__(self, name: str):
        self.name = name               # folder name (without trailing '/')
        self.children: dict[str, "TreeNode"] = {}
        self.file_count = 0            # number of files in this subtree
        self.total_size = 0            # total size of files in this subtree (bytes)

def _get_or_create_child(node: TreeNode, part: str) -> TreeNode:
    if part not in node.children:
        node.children[part] = TreeNode(part)
    return node.children[part]

def build_tree(base_url: str, prefix: str = "") -> TreeNode:
    """
    Build a directory tree from all objects under 'prefix'.
    Only aggregates counts/sizes; does NOT store file names.
    """
    root = TreeNode(prefix.rstrip("/")) if prefix else TreeNode("")
    # Normalize prefix to ensure we split keys relative to it
    preflen = len(prefix)
    for key, size in iter_all_objects(base_url, prefix=prefix):
        # Strip the listing prefix to get a path relative to the root of this view
        rel = key[preflen:] if preflen and key.startswith(prefix) else key
        parts = [p for p in rel.split("/") if p]   # ignore empty components
        # No parts means the key equals the prefix; treat as a root-level file
        cur = root
        # Walk ALL directory parts except the last *if* it is a file; we don't store files as nodes
        if len(parts) > 1:
            for d in parts[:-1]:
                cur = _get_or_create_child(cur, d)
        # Update aggregates at EVERY ancestor (including root)
        # We propagate counts/sizes up the path including the final placement (cur).
        walk = root
        for d in parts[:-1]:
            walk = walk.children[d]
            walk.file_count += 1
            if size > 0:
                walk.total_size += size
        # Finally, add to root as well (covers root files and ensures totals at the top)
        root.file_count += 1
        if size > 0:
            root.total_size += size
    return root


def print_tree(node: TreeNode, base_label: str, *, ascii_mode: bool = False, out=sys.stdout):
    """
    Pretty-print the directory tree.
    Each directory line shows:  DIR/  [<files> files, <total size>]
    """
    # Choose glyphs
    if ascii_mode:
        BRANCH_LAST = "+-- "
        BRANCH_MID  = "+-- "
        TRUNK       = "|   "
        INDENT      = "    "
    else:
        BRANCH_LAST = "└── "
        BRANCH_MID  = "├── "
        TRUNK       = "│   "
        INDENT      = "    "

    def _sorted_children(n: TreeNode):
        return [n.children[k] for k in sorted(n.children.keys(), key=lambda s: s.lower())]

    def _emit_line(prefix: str, name: str, tn: TreeNode, is_last: bool):
        branch = BRANCH_LAST if is_last else BRANCH_MID
        size_str = human_bytes(tn.total_size) if tn.file_count > 0 else "0 B"
        print(f"{prefix}{branch}{name}/  [{tn.file_count} files, {size_str}]", file=out)

    size_str = human_bytes(node.total_size) if node.file_count > 0 else "0 B"
    title = f"{base_label}/  [{node.file_count} files, {size_str}]"
    print(title, file=out)

    def _walk(n: TreeNode, prefix: str):
        kids = _sorted_children(n)
        for i, child in enumerate(kids):
            is_last = (i == len(kids) - 1)
            _emit_line(prefix, child.name, child, is_last)
            next_prefix = prefix + (INDENT if is_last else TRUNK)
            _walk(child, next_prefix)

    _walk(node, "")

# -------- Download orchestration --------

def download_prefix(base_url: str, prefix: str, dest: str) -> Tuple[int, int, int]:
    downloaded = skipped = total = 0
    for key, size in iter_all_objects(base_url, prefix=prefix):
        total += 1
        local_path, did_download = download_object(base_url, key, dest, size)
        if did_download:
            print(f"[get ] {key}  ->  {local_path}")
            downloaded += 1
        else:
            print(f"[skip] {key}  (exists, size matches)")
            skipped += 1
    return downloaded, skipped, total

def normalize_prefix(p: Optional[str]) -> str:
    if not p:
        return ""
    p = p.lstrip("/")
    if p and not p.endswith("/"):
        p += "/"
    return p

def human_bytes(n: int) -> str:
    units = ["B", "KB", "MB", "GB", "TB"]
    i = 0
    f = float(n)
    while f >= 1024 and i < len(units) - 1:
        f /= 1024.0
        i += 1
    return f"{f:.2f} {units[i]}"

# -------- CLI --------

def main():
    ap = argparse.ArgumentParser(description="Public downloader for CSCS RGW (no boto3, unsigned HTTP).")
    ap.add_argument("--endpoint", default=DEFAULT_ENDPOINT, help="RGW endpoint (default: https://rgw.cscs.ch)")
    ap.add_argument("--region", default=DEFAULT_REGION, help="Unused for unsigned HTTP (kept for reference)")
    ap.add_argument("--bucket", default=DEFAULT_BUCKET, help="Bucket (without tenant), e.g., sdc3-simdata")
    ap.add_argument("--tenant", default=DEFAULT_TENANT, help="Tenant, e.g., ska (REQUIRED for anonymous RGW access)")
    ap.add_argument("--dest", default="./download", help="Destination directory")
    ap.add_argument("--prefix", default=None, help="Subfolder/prefix to scope listing/downloading (e.g., 'SDC3/').")
    ap.add_argument("--all", action="store_true", help="Download the entire bucket (recursive)")   
    ap.add_argument("--tree", action="store_true", help="Draw a directory tree. For each folder, show the number of files in its subtree and the total size.")
    ap.add_argument("--list", action="store_true", help="Recursively list ALL files (optionally under --prefix)") 
    ap.add_argument("--ascii", action="store_true", help="Use ASCII characters for the tree instead of Unicode box-drawing.")
    args = ap.parse_args()

    base_url = build_bucket_base_url(args.endpoint, args.tenant, args.bucket)
    prefix = normalize_prefix(args.prefix)

    # --- LIST: recursive (default) ---
    if args.list:
        try:
            print(f"Listing all objects under '{prefix or '/'}' from {base_url} ...\n")
            cnt, total_bytes = list_recursive(base_url, prefix=prefix)
        except Exception as e:
            print(f"Failed to list recursively at {base_url}: {e}\n"
                  "Hints:\n"
                  "  • Ensure --tenant is correct.\n"
                  "  • Anonymous listing requires s3:ListBucket (403 if disabled).\n"
                  "  • To list just known paths, use --prefix or publish a manifest.")
            sys.exit(2)
        print(f"\nTotal objects: {cnt} | Total size: {human_bytes(total_bytes)}")
        return


    # --- TREE: directory hierarchy with aggregated counts/sizes ---
    if args.tree:
        try:
            label = (args.bucket if not args.tenant else f"{args.tenant}:{args.bucket}") + (f"/{prefix}" if prefix else "")
            # Build & print
            root = build_tree(base_url, prefix=prefix)
            print_tree(root, label.rstrip("/"), ascii_mode=args.ascii)
        except Exception as e:
            print(f"Failed to build tree at {base_url}: {e}\n"
                  "Hints:\n"
                  "  • Ensure --tenant is correct.\n"
                  "  • Anonymous listing requires s3:ListBucket (403 if disabled).\n"
                  "  • To scope the tree, pass --prefix (e.g., 'SDC3/').")
            sys.exit(2)
        return

    # --- DOWNLOAD ---
    if not args.all and args.prefix is None:
        print("No action selected. Use --list (recursive), --list-top (top-level), --all (download) or --prefix (download subtree).")
        sys.exit(1)

    scope = "" if args.all else prefix
    os.makedirs(args.dest, exist_ok=True)
    label = scope or "(entire bucket)"
    print(f"\nDownloading '{label}' from {base_url} -> {args.dest} ...")
    t0 = time.time()
    try:
        dl, sk, tot = download_prefix(base_url, scope, args.dest)
    except Exception as e:
        print(f"Download failed: {e}")
        sys.exit(2)
    dt = time.time() - t0
    print(f"\nDone. Total: {tot}, downloaded: {dl}, skipped: {sk}  (elapsed {dt:.1f}s)")

if __name__ == "__main__":
    main()
