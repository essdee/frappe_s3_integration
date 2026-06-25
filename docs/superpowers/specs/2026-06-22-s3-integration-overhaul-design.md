# frappe_s3_integration Overhaul — Design Spec

**Date:** 2026-06-22
**Author:** Sakthi (directing) / implementation by Claude
**Status:** Approved — proceeding to implementation plan

## Context

`frappe_s3_integration` is the engine that owns all S3 mechanics; `frappe_tools`
(image-scan: create-then-push) and `essdee_sales` (compress-then-push-direct) are
consumers. Today the engine is a single ~417-line module
(`frappe_s3_integration/s3_core/__init__.py`) plus a midnight cron, two config
doctypes, and a (mostly dead) image-optimization path.

**Critical operating constraint:** this will be tested directly in **production**.
**No data may be lost.** Every destructive step must be provably safe and
idempotent.

## Goals

- **G1 — Content-type at rest.** S3 objects are uploaded without `ContentType`, so
  everything is stored as `binary/octet-stream` and downloads come back raw/untyped.
- **G2 — S3-only storage.** When the app is installed + configured, *all* file
  operations end up on S3; nothing stays permanently on local disk. Files may land
  on disk transiently, then the midnight (`0 0 * * *`) sweep migrates them and
  deletes the local copy.
- **G3 — Dual backup.** Two copies of everything: the live S3 buckets (primary) +
  a nightly compressed archive of *both* buckets stored locally, restorable via
  AWS CLI into a new bucket.
- **G4 — Unwire optimization.** Stop invoking the custom image optimization in the
  active flow; keep the code dormant. Frappe's native handling covers the
  "optimize" checkbox.
- **G5 — Finalize two-bucket config.** Drive bucket selection + per-bucket size
  limits from the `AWS S3 Settings Bucket Detail` child table; add a global
  single-default guard; actually enforce the limits.
- **Unification.** All consumer upload paths funnel through one entrypoint in
  `frappe_s3_integration`.

## Data-safety invariants (apply to ALL phases)

1. **Delete-after-verify.** A local file is `os.remove`d only after the S3 object
   is confirmed present (HEAD returns, size matches) *and* the File doc's
   `file_url` / `custom_s3_key` are committed to the DB.
2. **Idempotent migration.** Re-running the sweep never double-uploads, never
   corrupts, never deletes an unmigrated file.
3. **Non-destructive backfill.** Content-type backfill uses server-side
   `copy_object` (same key, `MetadataDirective=REPLACE`); it never deletes or
   re-downloads.
4. **Read-only backup.** The backup job only lists + downloads from the buckets;
   it never mutates the source. Restore is a separate, manual AWS-CLI step.
5. **Never drop existing files.** Size enforcement rejects *new* oversize uploads
   at request time, but the sweep migrates already-existing local files regardless
   (logging a warning) — it must never delete a file it refused to upload.
6. **Fail-safe flagging.** The `after_insert` flag is metadata-only and harmless;
   if S3 is disabled/unreachable the file simply stays local and is retried.
7. **Per-file transaction + retry.** Each file migrates in its own commit; on any
   failure the flag/state is left such that the next nightly run retries it.

## Architecture decisions

### G2 — universal capture + hardened sweep
- New `File: after_insert` doc-event flags every newly-created **local** File
  (skip folders + already-S3 files) with `custom_is_s3_uploaded=1`,
  `custom_s3_key=''`. This transparently routes *all* upload paths (including the
  current purely-local `operations.upload_files` and `price_list` bulk) into the
  existing midnight sweep with **no request-time S3 dependency**.
- Bucket chosen at migration time from `File.is_private`.
- Harden `process_unuploaded_documents` / `migrate_file_to_s3`:
  - paginate + batch (replace unbounded `get_all` + commit-per-file),
  - chunked MD5 (stop reading the whole file into memory),
  - delete-after-verify (invariant 1),
  - leave failures flagged for next-night retry (covers S3-down / kill-switch on).

### Unified upload API
- `frappe_s3_integration.api.save_file_to_s3(content, filename, attached_to_doctype=None,
  attached_to_name=None, attached_to_field=None, is_private=1, immediate=False,
  content_type=None)` — owns content-type derivation, bucket selection, and size
  validation in one place.
- `immediate=True` pushes synchronously (caller needs the proxy URL in-request);
  default defers to the midnight sweep.
- Consumers (`frappe_tools` scanner, `essdee_sales` V2 / operations / price_list)
  funnel through it. The `after_insert` flag is the safety net so even unconverted
  callers still comply with G2.

### G1 — content-type at rest
- Set `ContentType` on all **3 write sites**: `upload_file_to_bucket`,
  `update_file_in_bucket`, `migrate_file_to_s3`.
- Source: `file.content_type` if present, else `mimetypes.guess_type(filename)[0]`,
  else `application/octet-stream`.
- **Backfill** existing objects in both buckets via `copy_object` with
  `MetadataDirective=REPLACE` and the corrected `ContentType` (non-destructive,
  invariant 3). Public 302-redirect then serves correctly with no serving change.

### G5 — config finalization
- `AWSS3Settings.validate_buckets`: add a global guard — exactly one row flagged
  `default_public_bucket` and exactly one flagged `default_private_bucket`.
- Enforce `max_file_size` / `max_image_size` (KiB) on the **immediate** upload
  paths (`create_file_and_upload_to_s3`, `save_file_to_s3`); reject oversize new
  uploads with a clear error. The midnight sweep migrates existing local files
  regardless but logs oversize (invariant 5 — never drop a file already on disk).
- (Region is already settings-driven — the boto3 client is built with
  `region_name=settings.region` and URLs use `connection.meta.region_name`. No
  change needed; correction to the earlier map.)

### G4 — unwire optimization
- Remove the inline `optimize_image` call in `frappe_tools` scanner
  (`api/doc_scanner.py`) and the `essdee_sales` V1 enqueue (`api/master.py`).
- Drop the `*/2 * * * *` optimization cron registration in `hooks.py`.
- Keep `optimize_image`, `optimization_scheduler`, and the
  `S3 Image Optimization Log[ Detail]` doctypes in the tree, dormant.

### G3 — backup + restore
- New `s3_core.list_objects` wrapper (paginated `list_objects_v2`), reused by the
  G1 backfill.
- New nightly scheduled job: for each default bucket, download all objects
  preserving key structure into a staging dir, `tar.gz` into a dated archive under
  a configurable backups dir (default: site `private/backups/s3/`).
- **Retention:** keep last 7 archives, auto-prune older. (This local archive is the
  one deliberate exception to G2 — operational backup, separate dir, rotated.)
- Restore documented: `tar xzf <archive>` then `aws s3 sync ./dump s3://<new-bucket>`.

## Known brittleness
- `delete_file_from_bucket` has an inverted return contract (False = success,
  error-log name = failure) that its caller `delete_file_from_s3` depends on.
  **Document** the contract in a docstring; do **not** change the semantics
  (changing it risks the delete path for no functional gain).
- `upload_file_to_bucket` reads the entire file into memory to compute MD5 — switch
  to chunked hashing (folded into the G1 edit of that function; matters for a large
  midnight backlog of big files like videos).
- Doubled-package dotted paths in `hooks.py` scheduler_events are fragile but
  correct; leave as-is.

## Build order (each phase independently shippable, TDD)
1. **G1** content-type at 3 write sites + non-destructive backfill.
2. **G5** global single-default guard + size enforcement + region in settings.
3. **G4** unwire optimization (remove call sites, drop cron, keep code).
4. **G2** `after_insert` capture hook + hardened/idempotent sweep + unified API + funnel consumers.
5. **G3** `list_objects` + nightly compressed backup + retention + restore docs.

## Out of scope
- Replacing the public 302-redirect with presigned URLs (unnecessary once
  ContentType is correct at rest).
- A third offsite backup target (live S3 + local archive is sufficient for now).
- Real-time S3 interception (rejected in favor of transient-local + midnight sweep).
- Removing the dormant optimization code.

## Open questions
None blocking. Region default stays `ap-south-1` if settings field is empty.
