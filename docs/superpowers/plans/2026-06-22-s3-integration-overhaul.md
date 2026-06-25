# frappe_s3_integration Overhaul — Implementation Plan (v2, post-review)

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.
>
> **v2 note:** an adversarial review found a CRITICAL data-loss bug (Frappe `content_hash` blob dedup) plus 7 more must-fixes; all are folded in below (search "M1".."M8", "N1".."N14").

**Goal:** Make `frappe_s3_integration` store correct content-types, route *all* files to S3 (transient-local → midnight sweep, nothing permanent on disk), keep a nightly compressed dual backup of both buckets, drop the active image-optimization path, and finalize the two-bucket config — all without losing a single byte while tested in production.

**Architecture:** One engine module (`frappe_s3_integration/s3_core/__init__.py`) owns all S3 mechanics; the midnight cron migrates flagged local files; consumers create normal Frappe Files and a `File.after_insert` hook captures them. Data-safety rests on a strict ordering — upload → HEAD-verify → commit the S3 pointer → only then delete the local copy — **and** on respecting Frappe's `content_hash` blob deduplication (never delete a blob another File still needs; reuse one S3 object for deduped docs).

**Tech Stack:** Frappe v15 (Python), boto3 1.34.x, unittest.mock for tests (moto is NOT installed), Pillow (dormant optimization only).

## Global Constraints

- **No data loss, ever** — tested directly in production. Every destructive step obeys the invariants below.
- **Data-safety invariants:**
  1. **Delete-after-verify-and-commit:** `os.remove(local)` runs only after the S3 object is HEAD-verified present (size matches) *and* the File doc's `custom_s3_key`/`file_url` are **committed**. Ordering: upload → verify → set_value → `commit` → remove.
  2. **Idempotent:** a set `custom_s3_key` short-circuits the sweep; re-runs never double-process or delete an unmigrated file.
  3. **Respect blob dedup (M1/N1):** Frappe shares one physical file across File docs with the same `content_hash`. Deduped docs **reuse one S3 object**; the local blob is removed only when no other File still needs it locally. Never overwrite `File.content_hash` (Frappe's own delete guard relies on it — N12).
  4. **Non-destructive backfill:** content-type backfill uses server-side `copy_object` (`MetadataDirective=REPLACE`, carrying existing metadata); never deletes, never re-downloads; per-object try/except so one bad object can't abort the run.
  5. **Read-only backup:** the backup job only lists + downloads; it never mutates the buckets.
  6. **Never drop existing files:** size enforcement rejects *new* immediate uploads only; the sweep migrates already-on-disk files regardless and merely logs oversize.
  7. **Fail-safe flagging:** the `after_insert` flag is metadata-only; if S3 is disabled/unreachable the file stays local and is retried next night.
- **Respect `disable_s3_operations`** kill-switch in every S3-touching path — including `validate_buckets`, which must stay saveable while disabled so the switch can be toggled during an incident (M4).
- **Frappe module path quirk:** engine = `frappe_s3_integration/s3_core/__init__.py`; scheduler/doctypes = `frappe_s3_integration/frappe_s3_integration/...` (doubled package). Engine import = `frappe_s3_integration.s3_core`.
- **GitNexus (per app `CLAUDE.md`):** run `gitnexus_impact` on each symbol before editing and `gitnexus_detect_changes` before any commit, if the MCP is connected.
- **Commits:** author as `Sakthi Kumar Pugalenthy <sakthi123msd@gmail.com>`, no Claude co-author; **never commit without Sakthi's explicit OK** — leave changes uncommitted for review.
- **Tests:** `bench --site <SITE> run-tests --module <dotted>` where `<SITE>` has `frappe_s3_integration` installed (resolve via `bench --site <s> list-apps`). Mock boto3 with `unittest.mock`; **mock `doc.get` via `side_effect`, never bare `MagicMock(attr=...)`** for Frappe docs (M3), and make safety tests reach the real branch they claim to test (M2).
- **Pre-deploy check (M4):** before the Phase-2 validate change goes live, confirm the live AWS S3 Settings already has exactly one default-public + one default-private bucket.

---

## Phase 1 — Correct content-type + make the sweep data-safe (G1 + sweep hardening)

### Task 1.1: ContentType + chunked hashing + HEAD-verify + list_objects in the engine

**Files:**
- Modify: `frappe_s3_integration/s3_core/__init__.py`
- Test: `frappe_s3_integration/frappe_s3_integration/doctype/aws_s3_settings/test_aws_s3_settings.py`

**Interfaces produced:** `_guess_content_type(filename, fallback='application/octet-stream') -> str`; `S3Connection.verify_object(bucket, key, expected_size=None) -> bool`; `S3Connection.list_objects(bucket, prefix=None) -> Iterator[dict]`. `upload_file_to_bucket` return dict unchanged.

- [ ] **Step 1: Failing tests**

```python
from unittest.mock import MagicMock
from botocore.exceptions import ClientError
from frappe_s3_integration import s3_core
from frappe.tests.utils import FrappeTestCase

class TestS3Engine(FrappeTestCase):
    def _conn(self):
        c = s3_core.S3Connection.__new__(s3_core.S3Connection)
        c.connection = MagicMock()
        return c

    def test_guess_content_type(self):
        self.assertEqual(s3_core._guess_content_type("a.png"), "image/png")
        self.assertEqual(s3_core._guess_content_type("a.pdf"), "application/pdf")
        self.assertEqual(s3_core._guess_content_type("blob"), "application/octet-stream")

    def test_verify_size_match(self):
        c = self._conn(); c.connection.head_object.return_value = {"ContentLength": 10}
        self.assertTrue(c.verify_object("b", "k", 10))
        self.assertFalse(c.verify_object("b", "k", 11))

    def test_verify_absent_is_false(self):
        c = self._conn()
        c.connection.head_object.side_effect = ClientError({"Error": {"Code": "404"}}, "HeadObject")
        self.assertFalse(c.verify_object("b", "k"))

    def test_verify_transient_error_reraises(self):  # M8: don't treat throttle as "absent"
        c = self._conn()
        c.connection.head_object.side_effect = ClientError({"Error": {"Code": "SlowDown"}}, "HeadObject")
        with self.assertRaises(ClientError):
            c.verify_object("b", "k")
```

- [ ] **Step 2: Run, verify FAIL** — `bench --site <SITE> run-tests --module frappe_s3_integration.frappe_s3_integration.doctype.aws_s3_settings.test_aws_s3_settings`

- [ ] **Step 3: Top-of-file helper + import**

```python
import uuid
import hashlib
import mimetypes
import boto3 as s3
import frappe
from botocore.exceptions import ClientError


def _guess_content_type(filename, fallback="application/octet-stream"):
    if not filename:
        return fallback
    return mimetypes.guess_type(filename)[0] or fallback
```

- [ ] **Step 4: `verify_object` (M8 exception handling) + `list_objects`** (after `get_file_from_bucket`)

```python
    def verify_object(self, bucket_name, key, expected_size=None):
        """True only if the object exists (and size matches). Distinguishes a
        definite 404 (False) from a transient error (re-raise so the caller
        rolls back + retries rather than deleting the local copy). Invariant 1."""
        try:
            resp = self.connection.head_object(Bucket=bucket_name, Key=key)
        except ClientError as e:
            code = str(e.response.get("Error", {}).get("Code", ""))
            if code in ("404", "NoSuchKey", "NotFound"):
                return False
            raise
        if expected_size is not None and resp.get("ContentLength") != expected_size:
            return False
        return True

    def list_objects(self, bucket_name, prefix=None):
        paginator = self.connection.get_paginator("list_objects_v2")
        kwargs = {"Bucket": bucket_name}
        if prefix:
            kwargs["Prefix"] = prefix
        for page in paginator.paginate(**kwargs):
            for obj in page.get("Contents", []):
                yield obj
```

- [ ] **Step 5: ContentType + chunked MD5 in `upload_file_to_bucket`** (replace the read/upload block ~208-238). ACL stays as-is — the live public bucket already accepts `public-read` on every upload today, so it supports ACLs (M5: do not add a config flag; rely on the existing working contract).

```python
        file.stream.seek(0)
        hasher = hashlib.md5()
        total = 0
        for chunk in iter(lambda: file.stream.read(8192), b""):
            total += len(chunk)
            hasher.update(chunk)
        if total == 0:
            frappe.throw("Cannot upload an empty file")
        content_hash = hasher.hexdigest()
        file.stream.seek(0)
        try:
            ext = file.filename.rsplit('.', 1)[-1] if '.' in file.filename else ''
            unique_filename = f"{uuid.uuid4()}.{ext}" if ext else str(uuid.uuid4())
            key = f"{self.get_default_upload_folder(bucket_name=bucket_name)}"
            if folder:
                folder = str(folder).strip("/").replace("..", "").replace("//", "/")
                if folder:
                    key += f"/{folder}"
            key += f"/{unique_filename}"
            content_type = getattr(file, "content_type", None) or _guess_content_type(file.filename)
            extra_args = {"ContentType": content_type}
            if allow_public:
                extra_args["ACL"] = "public-read"
            self.connection.upload_fileobj(Fileobj=file, Bucket=bucket_name, Key=key, ExtraArgs=extra_args)
            region = self.connection.meta.region_name
            file_url = f"https://{bucket_name}.s3.dualstack.{region}.amazonaws.com/{key}"
            return {"file_url": file_url, "key": key, "bucket_name": bucket_name, "content_hash": content_hash}
        except Exception as e:
            frappe.log_error(f"Error uploading file: {str(e)}")
            return False
```

- [ ] **Step 6: ContentType in `update_file_in_bucket`** (dormant; default guessed from key — N13)

```python
    def update_file_in_bucket(self, file, bucket_name, key, allow_public=False, content_type=None):
        extra_args = {"ContentType": content_type or _guess_content_type(key)}
        if allow_public:
            extra_args["ACL"] = "public-read"
        self.connection.upload_fileobj(Fileobj=file, Bucket=bucket_name, Key=key, ExtraArgs=extra_args)
```

- [ ] **Step 7: Run, verify PASS. Step 8: Stage** `git -C apps/frappe_s3_integration add -A`.

### Task 1.2: Data-safe midnight sweep — content-type, blob-dedup guard, verify, commit-before-delete

**Files:**
- Modify: `frappe_s3_integration/frappe_s3_integration/process_scheduler.py`
- Test: `frappe_s3_integration/frappe_s3_integration/test_process_scheduler.py`

**Interfaces produced:** `migrate_file_to_s3(file_name, conn)` (commits the S3 pointer, reuses a sibling's object for deduped blobs, deletes local only when unshared); module helpers `_local_path(file)`, `_other_unmigrated_share(file)`, `_migrated_sibling(file)`, `_point_doc_at_s3(file, key, bucket)`.

- [ ] **Step 1: Failing tests that REACH the verify/delete branch (M2) and cover dedup (M1)**

```python
import io
from unittest.mock import MagicMock, patch
from frappe.tests.utils import FrappeTestCase
from frappe_s3_integration.frappe_s3_integration import process_scheduler as ps

PKG = "frappe_s3_integration.frappe_s3_integration.process_scheduler"

class TestMigrateSafety(FrappeTestCase):
    def _file(self, **kw):
        d = dict(custom_s3_key="", is_private=1, file_name="a.png",
                 file_url="/private/files/a.png", name="F1", content_hash="h1",
                 attached_to_doctype=None, attached_to_name=None, attached_to_field=None)
        d.update(kw)
        m = MagicMock(); m.get.side_effect = d.get
        for k, v in d.items():
            setattr(m, k, v)
        return m

    def test_no_delete_when_verify_fails(self):
        conn = MagicMock()
        conn.upload_file_to_private_bucket.return_value = {"key": "k", "bucket_name": "b", "content_hash": "h1"}
        conn.verify_object.return_value = False  # object NOT confirmed present
        f = self._file()
        with patch(f"{PKG}.frappe.get_doc", return_value=f), \
             patch(f"{PKG}._local_path", return_value="/tmp/x"), \
             patch(f"{PKG}.os.path.exists", return_value=True), \
             patch(f"{PKG}.os.path.getsize", return_value=5), \
             patch(f"{PKG}.open", return_value=io.BytesIO(b"12345")), \
             patch(f"{PKG}.os.remove") as rm:
            with self.assertRaises(Exception):
                ps.migrate_file_to_s3("F1", conn)
            conn.verify_object.assert_called_once()   # we actually reached verify
            rm.assert_not_called()                    # and never deleted

    def test_shared_blob_not_deleted_while_sibling_unmigrated(self):  # M1
        conn = MagicMock()
        conn.upload_file_to_private_bucket.return_value = {"key": "k", "bucket_name": "b", "content_hash": "h1"}
        conn.verify_object.return_value = True
        f = self._file()
        with patch(f"{PKG}.frappe.get_doc", return_value=f), \
             patch(f"{PKG}._local_path", return_value="/tmp/x"), \
             patch(f"{PKG}.os.path.exists", return_value=True), \
             patch(f"{PKG}.os.path.getsize", return_value=5), \
             patch(f"{PKG}.open", return_value=io.BytesIO(b"12345")), \
             patch(f"{PKG}._migrated_sibling", return_value=None), \
             patch(f"{PKG}._other_unmigrated_share", return_value=True), \
             patch(f"{PKG}.frappe.db"), \
             patch(f"{PKG}.os.remove") as rm:
            ps.migrate_file_to_s3("F1", conn)
            rm.assert_not_called()   # a sibling still needs the local blob
```

- [ ] **Step 2: Run, verify FAIL.**

- [ ] **Step 3: Rewrite `process_scheduler.py`**

```python
import os
import frappe
from frappe.utils import get_site_path
from werkzeug.datastructures import FileStorage
from frappe_s3_integration.s3_core import getS3Connection, get_proxy_url, _guess_content_type


def _local_path(file):
    """Deterministic local path for a /files or /private/files url; None otherwise.
    Avoids get_file_path's name-matching ambiguity (N11) and traversal (N10)."""
    url = file.file_url or ""
    if url.startswith("/private/files/"):
        rel, base = url[len("/private/files/"):], get_site_path("private", "files")
    elif url.startswith("/files/"):
        rel, base = url[len("/files/"):], get_site_path("public", "files")
    else:
        return None
    path = os.path.normpath(os.path.join(base, rel))
    if not path.startswith(os.path.normpath(base) + os.sep):
        return None  # traversal attempt
    return path


def _other_unmigrated_share(file):
    """True if another File with the same blob still needs the local bytes (M1)."""
    if not file.content_hash:
        return False
    return bool(frappe.get_all("File", filters={
        "content_hash": file.content_hash, "is_private": file.is_private,
        "name": ["!=", file.name], "custom_s3_key": ["in", ["", None]],
    }, limit=1))


def _migrated_sibling(file):
    """An already-migrated File sharing this blob, to reuse its S3 object (M1/N1)."""
    if not file.content_hash:
        return None
    rows = frappe.get_all("File", filters={
        "content_hash": file.content_hash, "is_private": file.is_private,
        "name": ["!=", file.name], "custom_s3_key": ["not in", ["", None]],
    }, fields=["custom_s3_key", "custom_s3_bucket_name"], limit=1)
    return rows[0] if rows else None


def _point_doc_at_s3(file, key, bucket):
    frappe.db.set_value("File", file.name, {
        "file_url": get_proxy_url(file.name, file.file_name),
        "custom_s3_key": key, "custom_s3_bucket_name": bucket,
    })  # NB: never write content_hash here (N12 — Frappe's dedup guard depends on it)


@frappe.whitelist()
def process_unuploaded_documents():
    if not frappe.has_permission("AWS S3 Settings", "read"):
        frappe.throw("Not permitted", frappe.PermissionError)
    conn = getS3Connection()
    if conn.s3_settings.disable_s3_operations:
        return
    files = frappe.get_all("File", filters=[
        ["custom_is_s3_uploaded", "=", 1], ["custom_s3_key", "in", ["", None]],
    ], fields=["name"])
    for f in files:
        try:
            migrate_file_to_s3(f.name, conn)
        except Exception:
            frappe.db.rollback()
            frappe.log_error(frappe.get_traceback(), f"S3 upload failed for File {f.name}")


def migrate_file_to_s3(file_name, conn):
    file = frappe.get_doc("File", file_name)
    if file.custom_s3_key:
        return  # idempotent (invariant 2)

    local_path = _local_path(file)

    # If the local blob is gone, heal from a migrated sibling sharing it (N1) — never lose the pointer
    if not local_path or not os.path.exists(local_path):
        sib = _migrated_sibling(file)
        if sib:
            _point_doc_at_s3(file, sib.custom_s3_key, sib.custom_s3_bucket_name)
            frappe.db.commit()
            return
        frappe.log_error(f"Local file missing & no migrated sibling: {file.name} ({local_path})", "S3 Migration")
        return

    local_size = os.path.getsize(local_path)
    if local_size == 0:
        frappe.log_error(f"Local file is empty: {local_path}", "S3 Migration")
        return

    # If a sibling already migrated this exact blob, reuse its object — no re-upload (M1)
    sib = _migrated_sibling(file)
    if sib:
        _point_doc_at_s3(file, sib.custom_s3_key, sib.custom_s3_bucket_name)
        frappe.db.commit()
        _maybe_remove_local(file, local_path)
        return

    content_type = _guess_content_type(file.file_name)
    with open(local_path, "rb") as f:
        file_obj = FileStorage(stream=f, filename=file.file_name, content_type=content_type)
        s3_resp = (conn.upload_file_to_private_bucket(file_obj) if file.is_private
                   else conn.upload_file_to_public_bucket(file_obj))

    if not s3_resp or not s3_resp.get("content_hash"):
        raise Exception("S3 upload failed or returned no content hash")  # M8: require a real hash
    if not conn.verify_object(s3_resp["bucket_name"], s3_resp["key"], expected_size=local_size):
        raise Exception(f"S3 object verification failed for {file.name}")

    # Persist the durable S3 pointer FIRST, in its own commit (invariant 1 + N2)
    _point_doc_at_s3(file, s3_resp["key"], s3_resp["bucket_name"])
    frappe.db.commit()

    # Best-effort: repoint the attached doc field (non-fatal — must not roll back the pointer, N2)
    try:
        file.reload()
        if (file.attached_to_doctype and file.attached_to_name and file.attached_to_field
                and frappe.db.exists(file.attached_to_doctype, file.attached_to_name)):
            meta = frappe.get_meta(file.attached_to_doctype)
            if meta.has_field(file.attached_to_field):
                frappe.db.set_value(file.attached_to_doctype, file.attached_to_name,
                                    file.attached_to_field, get_proxy_url(file.name, file.file_name))
                frappe.db.commit()
    except Exception:
        frappe.log_error(frappe.get_traceback(), f"S3 attached-doc repoint failed for {file.name}")

    _maybe_remove_local(file, local_path)


def _maybe_remove_local(file, local_path):
    """Delete the local blob only if no other File still needs it locally (M1)."""
    if _other_unmigrated_share(file):
        return  # last sibling to migrate removes the shared blob
    try:
        os.remove(local_path)
    except Exception:
        frappe.log_error(f"Failed to delete local file: {local_path}", "S3 Cleanup")
```

- [ ] **Step 4: Run, verify PASS. Step 5: Stage.**

### Task 1.3: One-time, resumable, non-destructive content-type backfill

**Files:**
- Create: `frappe_s3_integration/frappe_s3_integration/backfill.py`
- Test: `frappe_s3_integration/frappe_s3_integration/test_backfill.py`

**Interfaces produced:** `backfill_content_types(dry_run=True) -> {bucket: {scanned, fixed, errors}, "dry_run": bool}` (whitelisted, System Manager). **Required deploy step (N14):** public files have no read-time MIME self-heal, so this is the *only* remediation for old public objects — run after Phase 1.

- [ ] **Step 1: Failing test** (only octet-stream rewritten, REPLACE + metadata preserved, per-object error doesn't abort)

```python
from unittest.mock import MagicMock, patch
from frappe.tests.utils import FrappeTestCase
from frappe_s3_integration.frappe_s3_integration import backfill

class TestBackfill(FrappeTestCase):
    def test_only_octet_rewritten_and_resilient(self):
        conn = MagicMock(); conn.public_bucket = "pub"; conn.private_bucket = None
        conn.s3_settings.disable_s3_operations = 0
        conn.list_objects.return_value = [
            {"Key": "uploads/a.png", "Size": 10},
            {"Key": "uploads/b.pdf", "Size": 10},
            {"Key": "uploads/c.png", "Size": 10},
        ]
        conn.connection.head_object.side_effect = [
            {"ContentType": "binary/octet-stream", "Metadata": {"x": "1"}},  # a -> fix
            {"ContentType": "application/pdf"},                              # b -> skip
            Exception("transient"),                                          # c -> error, continue
        ]
        with patch.object(backfill, "getS3Connection", return_value=conn), \
             patch.object(backfill.frappe, "has_permission", return_value=True):
            res = backfill.backfill_content_types(dry_run=False)
        conn.connection.copy_object.assert_called_once()
        _, kw = conn.connection.copy_object.call_args
        self.assertEqual(kw["MetadataDirective"], "REPLACE")
        self.assertEqual(kw["ContentType"], "image/png")
        self.assertEqual(kw["Metadata"], {"x": "1"})       # N3 preserved
        self.assertEqual(kw["ACL"], "public-read")
        self.assertEqual(res["pub"], {"scanned": 3, "fixed": 1, "errors": 1})
```

- [ ] **Step 2: Run, verify FAIL.**

- [ ] **Step 3: Implement `backfill.py`**

```python
import frappe
from frappe_s3_integration.s3_core import getS3Connection, _guess_content_type

OCTET = ("binary/octet-stream", "application/octet-stream", "", None)
MAX_SINGLE_COPY = 5 * 1024 ** 3  # S3 single-op copy limit (M6)


@frappe.whitelist()
def backfill_content_types(dry_run=True):
    if not frappe.has_permission("AWS S3 Settings", "write"):
        frappe.throw("Not permitted", frappe.PermissionError)
    if isinstance(dry_run, str):
        dry_run = frappe.json.loads(dry_run)
    conn = getS3Connection()
    if conn.s3_settings.disable_s3_operations:
        frappe.throw("S3 operations are disabled")

    out = {"dry_run": bool(dry_run)}
    for bucket_name, is_public in [(conn.public_bucket, True), (conn.private_bucket, False)]:
        if not bucket_name:
            continue
        scanned = fixed = errors = 0
        for obj in conn.list_objects(bucket_name):
            key = obj["Key"]
            scanned += 1
            try:
                if obj.get("Size", 0) > MAX_SINGLE_COPY:
                    frappe.log_error(f"Skip >5GB object: {bucket_name}/{key}", "S3 Backfill")
                    errors += 1
                    continue
                head = conn.connection.head_object(Bucket=bucket_name, Key=key)
                if head.get("ContentType") not in OCTET:
                    continue
                new_ct = _guess_content_type(key)
                if new_ct == "application/octet-stream":
                    continue
                fixed += 1
                if dry_run:
                    continue
                params = {
                    "Bucket": bucket_name, "Key": key,
                    "CopySource": {"Bucket": bucket_name, "Key": key},
                    "ContentType": new_ct, "MetadataDirective": "REPLACE",
                }
                if head.get("Metadata"):                       # N3: preserve metadata/headers
                    params["Metadata"] = head["Metadata"]
                for h in ("CacheControl", "ContentDisposition", "ContentEncoding", "ContentLanguage"):
                    if head.get(h):
                        params[h] = head[h]
                if head.get("StorageClass") and head["StorageClass"] != "STANDARD":
                    params["StorageClass"] = head["StorageClass"]
                if is_public:
                    params["ACL"] = "public-read"
                conn.connection.copy_object(**params)
            except Exception:                                   # M6: one bad object can't abort the run
                errors += 1
                frappe.log_error(frappe.get_traceback(), f"S3 Backfill failed: {bucket_name}/{key}")
        out[bucket_name] = {"scanned": scanned, "fixed": fixed, "errors": errors}
    return out
```

- [ ] **Step 4: Run, verify PASS. Step 5: Stage.** Sakthi runs `dry_run=True` then `dry_run=False` after Phase 1 deploy.

---

## Phase 2 — Finalize two-bucket config (G5)

### Task 2.1: Global single-default guard that never bricks the kill-switch (M4)

**Files:** Modify `.../doctype/aws_s3_settings/aws_s3_settings.py`; Test `test_aws_s3_settings.py`.

- [ ] **Step 1: Failing tests**

```python
    def _settings(self, rows, disabled=0):
        from frappe_s3_integration.frappe_s3_integration.doctype.aws_s3_settings.aws_s3_settings import AWSS3Settings
        d = AWSS3Settings.__new__(AWSS3Settings)
        d.disable_s3_operations = disabled
        d.s3_bucket_details = [frappe._dict(r) for r in rows]
        return d

    def test_one_each_ok(self):
        self._settings([
            {"bucket_name": "pub", "default_public_bucket": 1, "default_private_bucket": 0},
            {"bucket_name": "prv", "default_public_bucket": 0, "default_private_bucket": 1},
        ]).validate_buckets()

    def test_two_publics_rejected(self):
        with self.assertRaises(Exception):
            self._settings([
                {"bucket_name": "p1", "default_public_bucket": 1, "default_private_bucket": 0},
                {"bucket_name": "p2", "default_public_bucket": 1, "default_private_bucket": 0},
            ]).validate_buckets()

    def test_skipped_when_disabled(self):  # M4: kill-switch stays toggleable
        self._settings([], disabled=1).validate_buckets()  # no throw despite empty config
```

- [ ] **Step 2: Run, verify FAIL.**

- [ ] **Step 3: Rewrite `validate_buckets`**

```python
    def validate_buckets(self):
        if self.disable_s3_operations:
            return  # M4: keep Settings (and the kill-switch) saveable even with incomplete config
        exceptions = []
        for i in self.s3_bucket_details:
            if i.default_private_bucket and i.default_public_bucket:
                exceptions.append(f"bucket {i.get('bucket_name')} is marked both public and private")
            if not i.default_private_bucket and not i.default_public_bucket:
                exceptions.append(f"bucket {i.get('bucket_name')} must be either public or private")
        publics = [i for i in self.s3_bucket_details if i.default_public_bucket]
        privates = [i for i in self.s3_bucket_details if i.default_private_bucket]
        if len(publics) != 1:
            exceptions.append(f"exactly one default PUBLIC bucket is required (found {len(publics)})")
        if len(privates) != 1:
            exceptions.append(f"exactly one default PRIVATE bucket is required (found {len(privates)})")
        if exceptions:
            frappe.throw("The following problems were found:<br>" + "<br>".join(exceptions))
```

- [ ] **Step 4: Run, verify PASS. Step 5: Stage.**

### Task 2.2: Enforce per-bucket size on the immediate upload path

**Files:** Modify `s3_core/__init__.py` (`create_file_and_upload_to_s3`); Test `test_aws_s3_settings.py`.

- [ ] **Step 1: Failing test**

```python
    def test_create_file_rejects_oversize(self):
        from unittest.mock import MagicMock, patch
        from frappe_s3_integration import s3_core
        conn = MagicMock(); conn.validate_file_size.return_value = (True, 100)
        file = MagicMock(filename="big.bin")
        with patch.object(s3_core, "getS3Connection", return_value=conn):
            with self.assertRaises(Exception):
                s3_core.create_file_and_upload_to_s3("FG Item Master", "X", file, is_public_bucket=True)
        conn.upload_file_to_public_bucket.assert_not_called()
```

- [ ] **Step 2: Run, verify FAIL.**

- [ ] **Step 3: Add guard after `connection = getS3Connection()`**

```python
    connection = getS3Connection()
    oversize, max_size = connection.validate_file_size(file, is_public=is_public_bucket)
    if oversize:
        frappe.throw(f"{file.filename} exceeds the max allowed size of {max_size/1024:.2f} MB for this bucket")
```

- [ ] **Step 4: Run, verify PASS. Step 5: Stage.**

---

## Phase 3 — Unwire image optimization (G4)

### Task 3.1: Remove the inline optimize call in the frappe_tools scanner (+ N9 import)

**Files:** `frappe_tools/frappe_tools/api/doc_scanner.py`.

- [ ] **Step 1:** Replace lines ~356-360 — delete the `if "frappe_s3_integration" ... optimize_image(...)` block, leaving `content = output.getvalue()`.
- [ ] **Step 2 (N9):** Check `cint` usage: `grep -n "cint(" apps/frappe_tools/frappe_tools/api/doc_scanner.py`. If line 360 was its only use, drop `cint` from the `from frappe.utils import ...` line (avoid an F401). Confirm `sbool`/others remain.
- [ ] **Step 3:** `grep -n "optimize_image" .../doc_scanner.py` → no matches; `python -m py_compile` the file.
- [ ] **Step 4:** Stage `git -C apps/frappe_tools add -A`.

### Task 3.2: Remove the optimization-log enqueue in essdee_sales V1

**Files:** `essdee_sales/essdee_sales/api/master.py`.

- [ ] **Step 1:** Delete the import (line 8, `create_image_optimazation_log`).
- [ ] **Step 2:** Delete the call (line 309) and the now-dead `image_opt_files = []` + every `image_opt_files.append(file_doc_name)`.
- [ ] **Step 3:** `grep -n "create_image_optimazation_log\|image_opt_files" .../master.py` → none; `py_compile`.
- [ ] **Step 4:** Stage `git -C apps/essdee_sales add -A`.

### Task 3.3: Drop the `*/2` optimization cron (+ N5 deprecate note)

**Files:** `frappe_s3_integration/frappe_s3_integration/hooks.py`; (doc) File Image Settings.

- [ ] **Step 1:** Remove the `*/2 * * * *` entry, leaving only the midnight sweep (Phase 5 adds backup later):

```python
scheduler_events = {
    "cron": {
        "0 0 * * *": [
            "frappe_s3_integration.frappe_s3_integration.process_scheduler.process_unuploaded_documents"
        ]
    }
}
```

- [ ] **Step 2 (N5):** In the File Image Settings doctype JSON, append " (DEPRECATED — optimization now handled by Frappe-native File.optimize)" to the `optimize_images_in_s3` field `description` and set it `read_only: 1`. No code removal — `optimize_image`/`optimization_scheduler`/log doctypes stay dormant.
- [ ] **Step 3:** `py_compile` hooks.py. Deploy needs `bench migrate` (note for Sakthi; never auto-run on prod). Stage.

---

## Phase 4 — Universal S3 capture + unified API (G2)

### Task 4.1: `File.after_insert` capture hook

**Files:** `s3_core/__init__.py` (+`hooks.py`); Test `test_aws_s3_settings.py`.

**Interfaces produced:** `flag_file_for_s3(doc, event=None, *args)` — flags new local Files not already on/headed-for S3.

- [ ] **Step 1: Failing tests (M3 — mock `doc.get` via side_effect)**

```python
    def _doc(self, **kw):
        from unittest.mock import MagicMock
        d = dict(is_folder=0, custom_is_s3_uploaded=0, custom_s3_key="",
                 file_url="/private/files/a.png")
        d.update(kw)
        m = MagicMock(); m.get.side_effect = d.get
        return m

    def test_flags_local_file(self):
        from unittest.mock import patch
        from frappe_s3_integration import s3_core
        doc = self._doc()
        with patch.object(s3_core.frappe.db, "get_single_value", return_value=0):
            s3_core.flag_file_for_s3(doc)
        doc.db_set.assert_called_once_with("custom_is_s3_uploaded", 1, update_modified=False)

    def test_skips_folder(self):
        from frappe_s3_integration import s3_core
        doc = self._doc(is_folder=1); s3_core.flag_file_for_s3(doc); doc.db_set.assert_not_called()

    def test_skips_non_local_url(self):
        from frappe_s3_integration import s3_core
        doc = self._doc(file_url="/api/method/...serve_file?file_id=Y")
        s3_core.flag_file_for_s3(doc); doc.db_set.assert_not_called()

    def test_skips_already_keyed(self):  # N8
        from frappe_s3_integration import s3_core
        doc = self._doc(custom_s3_key="uploads/x.png")
        s3_core.flag_file_for_s3(doc); doc.db_set.assert_not_called()

    def test_skips_when_disabled(self):
        from unittest.mock import patch
        from frappe_s3_integration import s3_core
        doc = self._doc()
        with patch.object(s3_core.frappe.db, "get_single_value", return_value=1):
            s3_core.flag_file_for_s3(doc)
        doc.db_set.assert_not_called()
```

- [ ] **Step 2: Run, verify FAIL.**

- [ ] **Step 3: Implement `flag_file_for_s3`** (near `delete_file_from_s3`)

```python
def flag_file_for_s3(doc, event=None, *args):
    """Capture every new local File for the midnight sweep. Metadata-only;
    never touches S3 at request time (invariant 7)."""
    if doc.get("is_folder"):
        return
    if doc.get("custom_is_s3_uploaded") or doc.get("custom_s3_key"):  # N8
        return
    file_url = doc.get("file_url") or ""
    if not (file_url.startswith("/files/") or file_url.startswith("/private/files/")):
        return
    try:
        if frappe.db.get_single_value("AWS S3 Settings", "disable_s3_operations"):
            return
    except Exception:
        return
    doc.db_set("custom_is_s3_uploaded", 1, update_modified=False)
```

- [ ] **Step 4: Register in `hooks.py`**

```python
doc_events = {
    "File": {
        "after_insert": "frappe_s3_integration.s3_core.flag_file_for_s3",
        "on_trash": "frappe_s3_integration.s3_core.delete_file_from_s3",
    }
}
```

- [ ] **Step 5: Run, verify PASS. Step 6: Stage.** (Deploy needs `bench migrate` for the new doc_event.)

### Task 4.2: Unified `save_file_to_s3` entrypoint (M7 empty-content + local-url guard)

**Files:** `s3_core/__init__.py`; Test `test_aws_s3_settings.py`.

**Interfaces produced:** `save_file_to_s3(content, filename, attached_to_doctype=None, attached_to_name=None, attached_to_field=None, is_private=1, immediate=False, content_type=None) -> (file_url, file_name)`.

- [ ] **Step 1: Failing tests**

```python
    def test_save_immediate_pushes(self):
        from unittest.mock import patch
        from frappe_s3_integration import s3_core
        with patch.object(s3_core, "create_file_and_upload_to_s3", return_value=("/proxy", "F1")) as m:
            url, name = s3_core.save_file_to_s3(b"x", "a.png", "FG Item Master", "X", is_private=0, immediate=True)
        m.assert_called_once(); self.assertEqual((url, name), ("/proxy", "F1"))

    def test_save_rejects_empty(self):  # M7
        from frappe_s3_integration import s3_core
        with self.assertRaises(Exception):
            s3_core.save_file_to_s3(b"", "a.png", immediate=True)
```

- [ ] **Step 2: Run, verify FAIL.**

- [ ] **Step 3: Implement**

```python
def save_file_to_s3(content, filename, attached_to_doctype=None, attached_to_name=None,
                    attached_to_field=None, is_private=1, immediate=False, content_type=None):
    """Single funnel for app code that wants a file on S3.
    immediate=True  -> push synchronously now (caller needs the proxy URL in-request)
    immediate=False -> save locally; the after_insert hook + midnight sweep migrate it."""
    if not content:
        frappe.throw("Cannot save an empty file to S3")  # M7
    if immediate:
        import io
        from werkzeug.datastructures import FileStorage
        file_obj = FileStorage(stream=io.BytesIO(content), filename=filename,
                               content_type=content_type or _guess_content_type(filename))
        return create_file_and_upload_to_s3(attached_to_doctype, attached_to_name, file_obj,
                                            is_public_bucket=not is_private)
    file_doc = frappe.new_doc("File")
    file_doc.file_name = filename
    file_doc.is_private = 1 if is_private else 0
    file_doc.content = content
    if attached_to_doctype:
        file_doc.attached_to_doctype = attached_to_doctype
        file_doc.attached_to_name = attached_to_name
        file_doc.attached_to_field = attached_to_field
    file_doc.insert(ignore_permissions=True)  # after_insert flags it
    if not (file_doc.file_url or "").startswith(("/files/", "/private/files/")):
        frappe.throw(f"File {file_doc.name} was not written locally; cannot schedule S3 upload")  # M7
    return file_doc.file_url, file_doc.name
```

- [ ] **Step 4: Run, verify PASS. Step 5: Stage.**

### Task 4.3: Adopt the unified API in `essdee_sales` operations (reference adoption — N4)

**Files:** `essdee_sales/essdee_sales/api/operations.py` (`upload_file_to_s3`).

> **Scope honesty (N4):** "operable from here" = all paths converge on the engine's `upload_file_to_bucket` + the `after_insert` safety net; `save_file_to_s3` is the canonical helper. We funnel `operations.upload_file_to_s3` as the reference; other consumers (scanner, item_master_v2, master.py, price_list, `operations.upload_files`) are left working and are captured by the hook — not rewritten, to minimize churn on production code.

- [ ] **Step 1:** Replace the hand-rolled push body with a `save_file_to_s3(..., immediate=True)` call (same JSON contract) — see v1 plan body. `py_compile`. No automated test (request-bound); Sakthi verifies in-app.
- [ ] **Step 2:** Stage `git -C apps/essdee_sales add -A`.

---

## Phase 5 — Dual compressed backup (G3)

### Task 5.1: Backup settings fields

**Files:** `.../doctype/aws_s3_settings/aws_s3_settings.json`.

- [ ] **Step 1:** Add to `field_order` (after `disable_s3_operations`) and `fields`:

```json
  {"default": "1", "fieldname": "enable_bucket_backup", "fieldtype": "Check", "label": "Enable Nightly Bucket Backup"},
  {"fieldname": "backup_directory", "fieldtype": "Data", "label": "Backup Directory (blank = site private/backups/s3)"},
  {"default": "7", "fieldname": "backup_retention_count", "fieldtype": "Int", "label": "Backup Retention (archives per bucket)"}
```

- [ ] **Step 2:** `bench --site <SITE> migrate` (note for Sakthi). Stage.

### Task 5.2: Backup job + retention (+ N7 key sanitization)

**Files:** Create `backup.py`; Modify `s3_core/__init__.py` (`download_object`); Test `test_backup.py`.

**Interfaces produced:** `backup_s3_buckets()`; `_prune_old_archives(dir, prefix, keep) -> list[str]`; `_safe_rel_key(key) -> str|None`. `S3Connection.download_object(bucket, key, dest)`.

- [ ] **Step 1: Failing tests (prune + key sanitization)**

```python
import os, tempfile
from frappe.tests.utils import FrappeTestCase
from frappe_s3_integration.frappe_s3_integration import backup

class TestBackup(FrappeTestCase):
    def test_prune_keeps_latest_n(self):
        d = tempfile.mkdtemp()
        names = [f"pub-2026-06-{day:02d}.tar.gz" for day in range(1, 11)]
        for n in names: open(os.path.join(d, n), "w").close()
        removed = backup._prune_old_archives(d, "pub-", keep=7)
        self.assertEqual(len(removed), 3)
        self.assertEqual(sorted(f for f in os.listdir(d) if f.startswith("pub-")), sorted(names)[-7:])

    def test_safe_rel_key(self):
        self.assertEqual(backup._safe_rel_key("uploads/a.png"), "uploads/a.png")
        self.assertIsNone(backup._safe_rel_key("../../etc/passwd"))
        self.assertIsNone(backup._safe_rel_key("uploads/"))   # dir marker
```

- [ ] **Step 2: Run, verify FAIL.**

- [ ] **Step 3: Add `download_object` to `S3Connection`**

```python
    def download_object(self, bucket_name, key, dest_path):
        self.connection.download_file(bucket_name, key, dest_path)
```

- [ ] **Step 4: Implement `backup.py`**

```python
import os
import shutil
import tarfile
import frappe
from frappe.utils import get_site_path, now
from frappe_s3_integration.s3_core import getS3Connection


def _backup_dir(settings):
    path = (settings.get("backup_directory") or "").strip() or get_site_path("private", "backups", "s3")
    os.makedirs(path, exist_ok=True)
    return path


def _safe_rel_key(key):
    """Return a safe relative path for a key, or None to skip (N7)."""
    if not key or key.endswith("/"):
        return None  # directory marker
    rel = os.path.normpath(key)
    if rel.startswith("..") or os.path.isabs(rel):
        return None  # traversal
    return rel


def _prune_old_archives(directory, prefix, keep):
    archives = sorted(f for f in os.listdir(directory)
                      if f.startswith(prefix) and f.endswith(".tar.gz"))
    removed = []
    for f in (archives[:-keep] if keep > 0 else archives):
        try:
            os.remove(os.path.join(directory, f)); removed.append(f)
        except Exception:
            frappe.log_error(f"Failed to prune backup: {f}", "S3 Backup")
    return removed


def backup_s3_buckets():
    """Nightly: download both buckets into one compressed local archive each.
    Read-only on S3 (invariant 5)."""
    conn = getS3Connection()
    settings = conn.s3_settings
    if settings.disable_s3_operations or not settings.get("enable_bucket_backup"):
        return
    keep = int(settings.get("backup_retention_count") or 7)
    base = _backup_dir(settings)
    stamp = now().replace(" ", "_").replace(":", "-")

    seen = set()
    for bucket_name in (conn.private_bucket, conn.public_bucket):
        if not bucket_name or bucket_name in seen:
            continue
        seen.add(bucket_name)
        staging = os.path.join(base, f".staging-{bucket_name}")
        shutil.rmtree(staging, ignore_errors=True)
        os.makedirs(staging, exist_ok=True)
        try:
            for obj in conn.list_objects(bucket_name):
                rel = _safe_rel_key(obj["Key"])
                if not rel:
                    continue
                dest = os.path.join(staging, rel)
                try:
                    os.makedirs(os.path.dirname(dest), exist_ok=True)
                    conn.download_object(bucket_name, obj["Key"], dest)
                except Exception:
                    frappe.log_error(frappe.get_traceback(), f"S3 Backup download failed: {bucket_name}/{obj['Key']}")
            archive = os.path.join(base, f"{bucket_name}-{stamp}.tar.gz")
            with tarfile.open(archive, "w:gz") as tar:
                tar.add(staging, arcname=bucket_name)
        finally:
            shutil.rmtree(staging, ignore_errors=True)  # staging never lingers (invariant 7 spirit)
        _prune_old_archives(base, f"{bucket_name}-", keep)
```

- [ ] **Step 5: Run, verify PASS. Step 6: Stage.**

### Task 5.3: Register backup cron + restore docs (N6/N14)

**Files:** `hooks.py`; Create `frappe_s3_integration/docs/RESTORE.md`.

- [ ] **Step 1: Add the 1 AM backup entry** (after the midnight sweep so the freshest files are on S3 first)

```python
scheduler_events = {
    "cron": {
        "0 0 * * *": ["frappe_s3_integration.frappe_s3_integration.process_scheduler.process_unuploaded_documents"],
        "0 1 * * *": ["frappe_s3_integration.frappe_s3_integration.backup.backup_s3_buckets"]
    }
}
```

- [ ] **Step 2: Write `RESTORE.md`**

```markdown
# Restoring S3 files from a local backup archive

Archives: `<site>/private/backups/s3/<bucket>-<timestamp>.tar.gz` (or the configured
Backup Directory), newest 7 per bucket retained.

1. Extract:  `tar xzf <bucket>-<timestamp>.tar.gz`   # creates ./<bucket>/...
2. Restore into a new (or the same) bucket:
   `aws s3 sync ./<bucket> s3://<new-bucket> --acl public-read`   # public bucket
   `aws s3 sync ./<bucket> s3://<new-bucket>`                      # private bucket
3. If the bucket NAME changed, re-point existing File docs and the settings:
   `frappe.db.sql("update tabFile set custom_s3_bucket_name=%s where custom_s3_bucket_name=%s", (new, old)); frappe.db.commit()`
   then edit AWS S3 Settings → S3 Bucket Details (bucket_name + public/private flags) and save.
4. The archive does NOT store content-types — after restoring into a NEW bucket run
   `backfill_content_types(dry_run=False)` so public objects don't serve as octet-stream (N6/N14).
```

- [ ] **Step 3: Stage.** Deploy needs `bench migrate` for the new cron (note for Sakthi).

---

## Self-Review (v2)

- **Spec coverage:** G1 → 1.1-1.3; G2 → 1.2 (safe sweep) + 4.1-4.3; G3 → 5.1-5.3; G4 → 3.1-3.3; G5 → 2.1-2.2.
- **Review fixes folded in:** M1 (blob-dedup guard + reuse, Task 1.2), M2 (real safety test, 1.2 Step 1), M3 (doc.get mocking, 4.1 Step 1), M4 (kill-switch bypass, 2.1), M5 (ACL left as the proven working contract — no flag, scope-minimal), M6 (per-object try/except, 1.3), M7 (empty-content/local-url guard, 4.2), M8 (verify exception semantics + require content_hash, 1.1/1.2); N1-N14 folded where cheap (heal sibling, separate commits, metadata preserve, honest unification scope, deprecate toggle, restore steps, key sanitization, key short-circuit, cint import, path-traversal guard, don't-overwrite content_hash).
- **Placeholders:** none. **Type consistency:** helper names defined once and consumed consistently.
- **Deploy ordering:** Phase 1 → run backfill; doc_event/cron changes need `bench migrate` on the target site (flagged per task; never auto-run on prod). Confirm live bucket config before Phase 2 (M4).

## Open execution question
- Resolve `<SITE>` (the site with `frappe_s3_integration` installed) and whether to run tests here or rely on Sakthi's production verification — decided at execution handoff.
