# Copyright (c) 2026, sakthi123msd@gmail.com and contributors
# For license information, please see license.txt
"""Console-triggered, background-queued normalization of S3 keys to Frappe's files/ layout.

By design this is NOT a migrate patch — run it manually from the terminal after pulling:

    bench --site <site> execute frappe_s3_integration.s3_normalize.enqueue_normalization
    # dry run (logs what it WOULD do, touches nothing):
    bench --site <site> execute frappe_s3_integration.s3_normalize.enqueue_normalization --kwargs "{'dry_run': 1}"

enqueue_normalization() counts the mis-keyed files, sizes a job timeout to that backlog,
and enqueues the real re-keying on the `long` queue. The worker (_normalize) is idempotent
and RESUMABLE — if it hits the timeout it stops mid-way (per-file commits) and a re-run
finishes the rest.

Scope: only files whose custom_s3_key is OUTSIDE files/ / private/files/ are touched.
Data-safety invariant (never in neither place): the bytes always exist in >=1 location —
the old S3 object is deleted only AFTER the new one is verified, and a local copy is
deleted only AFTER the S3 object is verified with a matching size. Shared blobs (dedup)
repoint every sibling before the old object is dropped; public copies keep public-read.
"""

import os
import re

import frappe
from frappe.utils import cint, get_files_path

FRAPPE_PREFIXES = ("files/", "private/files/")

# Every S3-backed file (used by the local-cleanup sweep).
S3_BACKED_FILTERS = [
	["custom_is_s3_uploaded", "=", 1],
	["custom_s3_key", "is", "set"],
]

# Only mis-keyed, S3-backed files (key set and NOT already under files/ or private/files/).
# A list-of-lists lets us put two `not like` conditions on the same column.
MISKEYED_FILTERS = [
	["custom_is_s3_uploaded", "=", 1],
	["custom_s3_key", "is", "set"],
	["custom_s3_key", "not like", "files/%"],
	["custom_s3_key", "not like", "private/files/%"],
]

# Dynamic job timeout, sized to the backlog (all overridable via site_config).
SECONDS_PER_FILE = 3        # verify + copy + verify + db round-trips per object, with headroom
TIMEOUT_FLOOR = 600         # 10 min minimum
TIMEOUT_CAP = 24 * 3600     # 24 h ceiling


def _correct_key(file_name, is_private):
	from frappe_s3_integration.s3_core import _s3_safe_filename

	prefix = "private/files/" if is_private else "files/"
	return prefix + _s3_safe_filename(file_name or "file")


def _local_path(file_name, is_private):
	"""On-disk path of a File's local copy, matching how Frappe names it on save
	(save_file_on_filesystem sanitizes /\\%?# to _). Used to find + delete the local copy."""
	safe = re.sub(r"[/\\%?#]", "_", file_name or "")
	return get_files_path(safe, is_private=bool(is_private))


def _repoint_attached_field(f, proxy_url):
	"""Best-effort: point the attached doc's Attach field at the new proxy url.
	Singles-aware — a Single doctype (e.g. Website Settings) stores values in tabSingles,
	so repoint via set_single_value rather than the deprecated single-through-set_value route.
	Guarded — only rewrite a field that STILL serves a local /files path, so we never clobber
	an external URL, a cleared field, or an already-proxied value (the proxy url is invariant
	under re-keying, so an already-correct field needs no write)."""
	if not (f.attached_to_doctype and f.attached_to_field):
		return
	try:
		meta = frappe.get_meta(f.attached_to_doctype)
		if not meta.has_field(f.attached_to_field):
			return
		single = meta.issingle
		if single:
			current = frappe.db.get_single_value(f.attached_to_doctype, f.attached_to_field)
		elif f.attached_to_name and frappe.db.exists(f.attached_to_doctype, f.attached_to_name):
			current = frappe.db.get_value(f.attached_to_doctype, f.attached_to_name, f.attached_to_field)
		else:
			return
		if not _is_stale_local_url(current):
			return
		if single:
			frappe.db.set_single_value(f.attached_to_doctype, f.attached_to_field, proxy_url, update_modified=False)
		else:
			frappe.db.set_value(
				f.attached_to_doctype, f.attached_to_name,
				f.attached_to_field, proxy_url, update_modified=False,
			)
	except Exception:
		frappe.log_error(frappe.get_traceback(), f"S3 normalize: attach repoint failed ({f.name})")


def _miskeyed_count():
	if "custom_s3_key" not in frappe.db.get_table_columns("File"):
		return 0
	return frappe.db.count("File", MISKEYED_FILTERS)


def _timeout_for(count):
	"""Job timeout sized to the backlog: count * seconds/file, clamped to [floor, cap]."""
	spf = cint(frappe.conf.get("s3_normalize_seconds_per_file")) or SECONDS_PER_FILE
	floor = cint(frappe.conf.get("s3_normalize_timeout_floor")) or TIMEOUT_FLOOR
	cap = cint(frappe.conf.get("s3_normalize_timeout_cap")) or TIMEOUT_CAP
	return max(floor, min(count * spf, cap))


def enqueue_normalization(dry_run=0):
	"""Console entry point. Count the mis-keyed files, size a timeout to that backlog, and
	enqueue the worker on the `long` queue. Returns the plan (also printed)."""
	dry_run = cint(dry_run)
	count = _miskeyed_count()
	if not count:
		print("[s3 normalize] nothing to do — 0 mis-keyed files")
		return {"queued": 0}
	timeout = _timeout_for(count)
	frappe.enqueue(
		"frappe_s3_integration.s3_normalize._normalize",
		queue="long",
		timeout=timeout,
		job_name="s3_normalize",
		dry_run=dry_run,
	)
	print(
		f"[s3 normalize] queued {count} mis-keyed file(s) on 'long' queue "
		f"(timeout={timeout}s, dry_run={bool(dry_run)})"
	)
	return {"queued": count, "timeout": timeout, "dry_run": bool(dry_run)}


def _normalize(dry_run=0):
	"""Background worker: re-key every mis-keyed S3 object into files/ layout. Idempotent +
	resumable (per-file commits). Enqueued by enqueue_normalization — never a migrate patch."""
	dry_run = cint(dry_run)
	if "custom_s3_key" not in frappe.db.get_table_columns("File"):
		return

	from frappe_s3_integration.s3_core import getS3Connection, get_proxy_url

	if frappe.db.get_single_value("AWS S3 Settings", "disable_s3_operations"):
		frappe.log_error("S3 disabled — normalization skipped", "S3 Normalize")
		return

	# S3 unreachable/misconfigured: skip cleanly instead of crashing the job.
	try:
		conn = getS3Connection()
	except Exception:
		frappe.log_error(frappe.get_traceback(), "S3 Normalize: connection failed — skipped")
		return

	try:
		from rq.timeouts import JobTimeoutException
	except Exception:  # pragma: no cover
		class JobTimeoutException(Exception):
			pass

	files = frappe.get_all(
		"File",
		filters=MISKEYED_FILTERS,
		fields=[
			"name", "file_name", "is_private", "custom_s3_key", "custom_s3_bucket_name",
			"attached_to_doctype", "attached_to_name", "attached_to_field",
		],
	)

	rekeyed = local_removed = skipped = errors = 0
	processed = set()  # File names already repointed as part of a shared-blob group
	for f in files:
		try:
			if f.name in processed:
				continue
			key = f.custom_s3_key
			bucket = f.custom_s3_bucket_name
			if not key or not bucket:
				continue
			# Defensive (the query already excludes these): never touch a correct key.
			if key.startswith(FRAPPE_PREFIXES):
				skipped += 1
				continue

			# Verify the source object exists before doing anything (never lose the pointer).
			if not conn.verify_object(bucket, key):
				errors += 1
				frappe.log_error(
					f"normalize: source missing {bucket}/{key} (File {f.name}) — left as-is",
					"S3 Normalize")
				continue
			new_key = conn._unique_key(bucket, _correct_key(f.file_name, f.is_private))

			if dry_run:
				rekeyed += 1
				frappe.logger("s3").info(
					f"[s3 normalize dry-run] would re-key {bucket}/{key} -> {new_key} (File {f.name})")
				continue

			# Server-side copy old -> new. COPY preserves content-type/metadata but NOT the
			# ACL — a copied PUBLIC object defaults to private, so re-apply public-read or the
			# serve_file redirect would 403 (letterheads/images go blank).
			copy_params = {
				"Bucket": bucket, "Key": new_key,
				"CopySource": {"Bucket": bucket, "Key": key},
			}
			if not f.is_private:
				copy_params["ACL"] = "public-read"
			conn.connection.copy_object(**copy_params)
			if not conn.verify_object(bucket, new_key):
				errors += 1
				frappe.log_error(
					f"normalize: copy verify failed {bucket}/{new_key} (File {f.name}) — old kept",
					"S3 Normalize")
				continue

			# Shared-blob safe: repoint EVERY File that referenced the OLD key to the new one,
			# so dropping the old object can never strand a sibling that shared the same blob.
			sharers = frappe.get_all(
				"File",
				filters={"custom_s3_key": key, "custom_s3_bucket_name": bucket},
				fields=["name", "file_name", "attached_to_doctype", "attached_to_name", "attached_to_field"],
			)
			for s in sharers:
				s_proxy = get_proxy_url(s.name, s.file_name)
				frappe.db.set_value(
					"File", s.name,
					{"custom_s3_key": new_key, "file_url": s_proxy},
					update_modified=False,
				)
				_repoint_attached_field(s, s_proxy)
				processed.add(s.name)
			frappe.db.commit()
			conn.delete_file_from_bucket(key, bucket)  # no File references old key now — safe
			key = new_key
			rekeyed += len(sharers)

			# Target = S3: drop this re-keyed file's lingering LOCAL copy, but ONLY once the
			# S3 object is verified present AND its size matches (never in neither place).
			if f.file_name:
				local_abs = _local_path(f.file_name, f.is_private)
				if os.path.exists(local_abs):
					if conn.verify_object(bucket, key, expected_size=os.path.getsize(local_abs)):
						os.remove(local_abs)
						local_removed += 1
					else:
						skipped += 1
						frappe.log_error(
							f"normalize: local kept, S3 unverified/size-mismatch {bucket}/{key} "
							f"(File {f.name})", "S3 Normalize")
		except JobTimeoutException:
			# Deadline hit: persist what's done and stop — a re-run finishes the rest.
			frappe.db.commit()
			frappe.log_error(
				f"normalize: job timeout after {rekeyed} re-keyed — re-run to finish", "S3 Normalize")
			raise
		except Exception:
			errors += 1
			frappe.log_error(frappe.get_traceback(), f"S3 normalize failed for File {f.get('name')}")

	frappe.db.commit()
	print(
		f"[s3 normalize] {'DRY-RUN ' if dry_run else ''}done: candidates={len(files)} "
		f"rekeyed={rekeyed} local_removed={local_removed} skipped={skipped} errors={errors}"
	)


# ---------------------------------------------------------------------------------------
# Local-copy cleanup sweep — free disk by deleting local copies of files already on S3.
# Run:  bench --site <site> execute frappe_s3_integration.s3_normalize.enqueue_local_cleanup
# (dry run: append  --kwargs "{'dry_run': 1}"). SAFE: a local file is deleted only after
# its S3 object is verified present WITH a matching size (never removes the only copy).
# Independent of the key-normalization above — covers ALL S3-backed files, not just
# mis-keyed ones. Idempotent + resumable (no DB writes; re-run skips already-gone locals).
# ---------------------------------------------------------------------------------------

def _s3_backed_count():
	if "custom_s3_key" not in frappe.db.get_table_columns("File"):
		return 0
	return frappe.db.count("File", S3_BACKED_FILTERS)


def enqueue_local_cleanup(dry_run=0):
	"""Console entry point: size a timeout to the S3-backed file count and enqueue the
	local-cleanup worker on the `long` queue."""
	dry_run = cint(dry_run)
	count = _s3_backed_count()
	if not count:
		print("[s3 local-cleanup] nothing to do — 0 S3-backed files")
		return {"queued": 0}
	timeout = _timeout_for(count)
	frappe.enqueue(
		"frappe_s3_integration.s3_normalize._cleanup_local",
		queue="long",
		timeout=timeout,
		job_name="s3_local_cleanup",
		dry_run=dry_run,
	)
	print(
		f"[s3 local-cleanup] queued scan of {count} S3-backed file(s) on 'long' queue "
		f"(timeout={timeout}s, dry_run={bool(dry_run)})"
	)
	return {"queued": count, "timeout": timeout, "dry_run": bool(dry_run)}


def _cleanup_local(dry_run=0):
	"""Background worker: delete the LOCAL copy of every S3-backed file, but ONLY after the
	S3 object is verified present with a matching size. Never removes the last copy."""
	dry_run = cint(dry_run)
	if "custom_s3_key" not in frappe.db.get_table_columns("File"):
		return

	from frappe_s3_integration.s3_core import getS3Connection

	if frappe.db.get_single_value("AWS S3 Settings", "disable_s3_operations"):
		frappe.log_error("S3 disabled — local cleanup skipped", "S3 Local Cleanup")
		return
	try:
		conn = getS3Connection()
	except Exception:
		frappe.log_error(frappe.get_traceback(), "S3 Local Cleanup: connection failed — skipped")
		return

	try:
		from rq.timeouts import JobTimeoutException
	except Exception:  # pragma: no cover
		class JobTimeoutException(Exception):
			pass

	files = frappe.get_all(
		"File",
		filters=S3_BACKED_FILTERS,
		fields=["name", "file_name", "is_private", "custom_s3_key", "custom_s3_bucket_name"],
	)

	removed = kept = no_local = errors = 0
	for f in files:
		try:
			if not f.file_name or not f.custom_s3_key or not f.custom_s3_bucket_name:
				continue
			local_abs = _local_path(f.file_name, f.is_private)
			if not os.path.exists(local_abs):
				no_local += 1
				continue
			# Verify S3 has it (present + size match) before removing the local copy.
			if conn.verify_object(f.custom_s3_bucket_name, f.custom_s3_key, expected_size=os.path.getsize(local_abs)):
				if dry_run:
					frappe.logger("s3").info(f"[s3 local-cleanup dry-run] would delete {local_abs} (File {f.name})")
				else:
					os.remove(local_abs)
				removed += 1
			else:
				kept += 1
				frappe.log_error(
					f"local-cleanup: kept {local_abs} — S3 unverified/size-mismatch "
					f"{f.custom_s3_bucket_name}/{f.custom_s3_key} (File {f.name})", "S3 Local Cleanup")
		except JobTimeoutException:
			frappe.log_error(f"local-cleanup: job timeout after {removed} removed — re-run to finish", "S3 Local Cleanup")
			raise
		except Exception:
			errors += 1
			frappe.log_error(frappe.get_traceback(), f"S3 local cleanup failed for File {f.get('name')}")

	print(
		f"[s3 local-cleanup] {'DRY-RUN ' if dry_run else ''}done: scanned={len(files)} "
		f"removed={removed} kept(size-mismatch)={kept} no_local={no_local} errors={errors}"
	)


# ---------------------------------------------------------------------------------------
# Attach-field backfill — repoint stale Attach fields at already-migrated files (invariant 2).
# Fixes docs whose Attach field STILL holds /files/<x> though the File is already on S3.
# Covers the two gaps a pre-repoint migration left behind:
#   • Single doctypes (Website Settings.app_logo) — updated via set_single_value.
#   • dedup-shared blobs — the 2nd File doc reused a sibling's S3 object and never had its
#     own attach field repointed.
# Run:  bench --site <site> execute frappe_s3_integration.s3_normalize.enqueue_attach_backfill
#   (dry run: append  --kwargs "{'dry_run': 1}")
# SAFE + idempotent: only rewrites a field that STILL holds a local /files url; never touches
# S3 or local disk (pure DB repoint to the file's own proxy url). Resumable (per-file commits).
# ---------------------------------------------------------------------------------------

ATTACH_BACKFILL_FILTERS = [
	["custom_is_s3_uploaded", "=", 1],
	["custom_s3_key", "is", "set"],
	["attached_to_field", "is", "set"],
]


def _attach_backfill_count():
	if "custom_s3_key" not in frappe.db.get_table_columns("File"):
		return 0
	return frappe.db.count("File", ATTACH_BACKFILL_FILTERS)


def _is_stale_local_url(val):
	"""A value still pointing at Frappe's local disk (the migration never repointed it)."""
	return isinstance(val, str) and (val.startswith("/files/") or val.startswith("/private/files/"))


def _expected_local_url(s3_key):
	"""The pre-migration local url that a Frappe-layout S3 key was built from:
	'private/files/x.pdf' -> '/private/files/x.pdf', 'files/x.png' -> '/files/x.png'.
	Returns None for a mis-keyed object (normalize those first) — used as an IDENTITY
	check so the field is only repointed when its stale local url is THIS file's own."""
	if isinstance(s3_key, str) and s3_key.startswith(FRAPPE_PREFIXES):
		return "/" + s3_key
	return None


def _current_attach_value(doctype, name, field, issingle):
	"""Read the attach field's CURRENT value — singles come from tabSingles."""
	if issingle:
		return frappe.db.get_single_value(doctype, field)
	return frappe.db.get_value(doctype, name, field)


def enqueue_attach_backfill(dry_run=0):
	"""Console entry point: size a timeout to the attached S3-backed file count and enqueue
	the attach-field backfill worker on the `long` queue. Returns the plan (also printed)."""
	dry_run = cint(dry_run)
	count = _attach_backfill_count()
	if not count:
		print("[s3 attach-backfill] nothing to do — 0 attached S3-backed files")
		return {"queued": 0}
	timeout = _timeout_for(count)
	frappe.enqueue(
		"frappe_s3_integration.s3_normalize._backfill_attached_fields",
		queue="long",
		timeout=timeout,
		job_name="s3_attach_backfill",
		dry_run=dry_run,
	)
	print(
		f"[s3 attach-backfill] queued scan of {count} attached S3-backed file(s) on 'long' queue "
		f"(timeout={timeout}s, dry_run={bool(dry_run)})"
	)
	return {"queued": count, "timeout": timeout, "dry_run": bool(dry_run)}


def _backfill_attached_fields(dry_run=0):
	"""Background worker: for every S3-backed File whose attached field STILL holds a local
	/files url, repoint that field at the File's proxy url. Singles-aware, idempotent, and
	resumable (per-file commits). Never touches an already-proxied / external / empty value."""
	dry_run = cint(dry_run)
	if "custom_s3_key" not in frappe.db.get_table_columns("File"):
		return

	from frappe_s3_integration.s3_core import get_proxy_url

	try:
		from rq.timeouts import JobTimeoutException
	except Exception:  # pragma: no cover
		class JobTimeoutException(Exception):
			pass

	files = frappe.get_all(
		"File",
		filters=ATTACH_BACKFILL_FILTERS,
		fields=["name", "file_name", "custom_s3_key",
		        "attached_to_doctype", "attached_to_name", "attached_to_field"],
	)

	repointed = skipped = errors = 0
	for f in files:
		try:
			dt, dn, fld = f.attached_to_doctype, f.attached_to_name, f.attached_to_field
			if not (dt and fld):
				skipped += 1
				continue
			meta = frappe.get_meta(dt)
			if not meta.has_field(fld):
				skipped += 1
				continue
			if not meta.issingle and not (dn and frappe.db.exists(dt, dn)):
				skipped += 1
				continue
			current = _current_attach_value(dt, dn, fld, meta.issingle)
			# IDENTITY guard: only fix a field that STILL holds THIS file's OWN pre-migration
			# local url (== '/' + its Frappe-layout key). This skips an already-proxied value,
			# an external URL, a cleared field — AND a newer sibling File on the same field, so
			# we never repoint a record to an older attachment (Frappe leaves the old File's
			# attached_to_field set when an Attach field is replaced).
			expected = _expected_local_url(f.custom_s3_key)
			if not expected or current != expected:
				skipped += 1
				continue
			proxy = get_proxy_url(f.name, f.file_name)
			if dry_run:
				frappe.logger("s3").info(
					f"[attach-backfill dry-run] would repoint {dt}/{dn}.{fld}: {current} -> proxy (File {f.name})")
				repointed += 1
				continue
			if meta.issingle:
				frappe.db.set_single_value(dt, fld, proxy, update_modified=False)
			else:
				frappe.db.set_value(dt, dn, fld, proxy, update_modified=False)
			frappe.db.commit()
			repointed += 1
		except JobTimeoutException:
			frappe.db.commit()
			frappe.log_error(
				f"attach-backfill: job timeout after {repointed} repointed — re-run to finish",
				"S3 Attach Backfill")
			raise
		except Exception:
			errors += 1
			frappe.log_error(frappe.get_traceback(), f"S3 attach backfill failed for File {f.get('name')}")

	if repointed and not dry_run:
		# A repointed Single (Website Settings.app_logo) is written straight to tabSingles,
		# bypassing its on_update website-cache rebuild — refresh once so the new proxy url
		# is served immediately, without a manual `bench clear-cache`.
		frappe.clear_cache()
	print(
		f"[s3 attach-backfill] {'DRY-RUN ' if dry_run else ''}done: scanned={len(files)} "
		f"repointed={repointed} skipped={skipped} errors={errors}"
	)


# ---------------------------------------------------------------------------------------
# content_hash backfill — every S3-backed File must carry a content_hash (invariant 4):
# dedup (_migrated_sibling) and both shared-blob delete guards depend on it. New migrations
# backfill it from local bytes, but files migrated BEFORE that fix (local copy now gone)
# can still miss it. This tool hashes the LOCAL copy when one still exists, else streams
# the S3 object (chunked md5 — matches Frappe's own content hashing).
# Run:  bench --site <site> execute frappe_s3_integration.s3_normalize.enqueue_hash_backfill
#   (dry run: append  --kwargs "{'dry_run': 1}")
# SAFE: pure metadata write (content_hash only) — never touches S3 objects or local disk.
# Idempotent + resumable (per-file commits; re-run skips files that already have a hash).
# ---------------------------------------------------------------------------------------

HASHLESS_FILTERS = [
	["custom_is_s3_uploaded", "=", 1],
	["custom_s3_key", "is", "set"],
	["content_hash", "is", "not set"],
]


def _hashless_count():
	if "custom_s3_key" not in frappe.db.get_table_columns("File"):
		return 0
	return frappe.db.count("File", HASHLESS_FILTERS)


def _md5_of_stream(read):
	"""Chunked md5 over any read(n) callable (local file or S3 body)."""
	import hashlib

	h = hashlib.md5()
	while True:
		chunk = read(8192)
		if not chunk:
			break
		h.update(chunk)
	return h.hexdigest()


def _hash_backfill_timeout(count):
	"""Streaming-hash is bandwidth-bound, so size the timeout by the backlog's BYTES as
	well as its count (whichever is larger), clamped to the same cap. Overridable via
	site_config (s3_normalize_throughput_mbps). The worker is resumable either way."""
	rows = frappe.get_all("File", filters=HASHLESS_FILTERS, fields=["file_size"])
	total_bytes = sum(r.file_size or 0 for r in rows)
	throughput = float(frappe.conf.get("s3_normalize_throughput_mbps") or 0.5) * 1024 * 1024
	byte_secs = int(total_bytes / throughput * 2)  # 2x safety buffer
	cap = cint(frappe.conf.get("s3_normalize_timeout_cap")) or TIMEOUT_CAP
	return min(max(_timeout_for(count), byte_secs), cap)


def enqueue_hash_backfill(dry_run=0):
	"""Console entry point: size a timeout to the hashless S3-backed backlog (count AND
	bytes) and enqueue the content-hash backfill worker on the `long` queue."""
	dry_run = cint(dry_run)
	count = _hashless_count()
	if not count:
		print("[s3 hash-backfill] nothing to do — 0 S3-backed files without content_hash")
		return {"queued": 0}
	timeout = _hash_backfill_timeout(count)
	frappe.enqueue(
		"frappe_s3_integration.s3_normalize._backfill_content_hashes",
		queue="long",
		timeout=timeout,
		job_name="s3_hash_backfill",
		dry_run=dry_run,
	)
	print(
		f"[s3 hash-backfill] queued {count} hashless S3-backed file(s) on 'long' queue "
		f"(timeout={timeout}s, dry_run={bool(dry_run)})"
	)
	return {"queued": count, "timeout": timeout, "dry_run": bool(dry_run)}


def _backfill_content_hashes(dry_run=0):
	"""Background worker: compute + store content_hash for every S3-backed File missing one.
	Prefers the local copy (fast, no bandwidth); else streams the S3 object. Idempotent +
	resumable (per-file commits)."""
	dry_run = cint(dry_run)
	if "custom_s3_key" not in frappe.db.get_table_columns("File"):
		return

	from frappe_s3_integration.s3_core import getS3Connection

	if frappe.db.get_single_value("AWS S3 Settings", "disable_s3_operations"):
		frappe.log_error("S3 disabled — hash backfill skipped", "S3 Hash Backfill")
		return
	try:
		conn = getS3Connection()
	except Exception:
		frappe.log_error(frappe.get_traceback(), "S3 Hash Backfill: connection failed — skipped")
		return

	try:
		from rq.timeouts import JobTimeoutException
	except Exception:  # pragma: no cover
		class JobTimeoutException(Exception):
			pass

	files = frappe.get_all(
		"File",
		filters=HASHLESS_FILTERS,
		fields=["name", "file_name", "is_private", "custom_s3_key", "custom_s3_bucket_name"],
	)

	hashed = errors = 0
	for f in files:
		try:
			if not (f.custom_s3_key and f.custom_s3_bucket_name):
				continue
			if dry_run:
				# report the candidate without downloading anything
				frappe.logger("s3").info(
					f"[s3 hash-backfill dry-run] would compute+store content_hash for File {f.name}")
				hashed += 1
				continue
			digest = None
			# 1) local copy still on disk AND size-matches the S3 object — hash it without
			#    downloading. The hash must describe the bytes the File actually SERVES (S3),
			#    so a stale/replaced local leftover must never be trusted blindly: a wrong
			#    content_hash would poison dedup + the shared-blob delete guards.
			if f.file_name:
				local_abs = _local_path(f.file_name, f.is_private)
				if os.path.isfile(local_abs) and conn.verify_object(
						f.custom_s3_bucket_name, f.custom_s3_key,
						expected_size=os.path.getsize(local_abs)):
					with open(local_abs, "rb") as fh:
						digest = _md5_of_stream(fh.read)
			# 2) else stream the S3 object itself — always correct.
			if digest is None:
				obj = conn.get_file_from_bucket(f.custom_s3_key, f.custom_s3_bucket_name)
				digest = _md5_of_stream(obj["Body"].read)
			frappe.db.set_value("File", f.name, "content_hash", digest, update_modified=False)
			frappe.db.commit()
			hashed += 1
		except JobTimeoutException:
			frappe.db.commit()
			frappe.log_error(
				f"hash-backfill: job timeout after {hashed} hashed — re-run to finish", "S3 Hash Backfill")
			raise
		except Exception:
			errors += 1
			frappe.log_error(frappe.get_traceback(), f"S3 hash backfill failed for File {f.get('name')}")

	print(
		f"[s3 hash-backfill] {'DRY-RUN ' if dry_run else ''}done: candidates={len(files)} "
		f"hashed={hashed} errors={errors}"
	)


def diagnose_local(sample=8):
	"""READ-ONLY diagnostic: why aren't local copies being removed? Reports the File-doc +
	S3 state for a sample of on-disk files. Run:
	  bench --site <site> execute frappe_s3_integration.s3_normalize.diagnose_local
	"""
	from frappe_s3_integration.s3_core import getS3Connection

	try:
		conn = getS3Connection()
	except Exception:
		conn = None

	total = frappe.db.count("File")
	s3_yes = frappe.db.count("File", [["custom_is_s3_uploaded", "=", 1]])
	print(f"[diagnose] File docs: total={total}  custom_is_s3_uploaded=1:{s3_yes}  not-on-s3:{total - s3_yes}")

	for is_private in (1, 0):
		base = get_files_path(is_private=is_private)
		try:
			names = [n for n in os.listdir(base) if os.path.isfile(os.path.join(base, n))]
		except Exception:
			names = []
		print(f"\n[diagnose] {'private' if is_private else 'public'}/files: {len(names)} files on disk (sampling {min(sample, len(names))})")
		for n in names[:sample]:
			# Which File doc owns this on-disk file? Try file_name, then key-ends-with, then url-ends-with.
			rows = frappe.get_all("File", filters={"file_name": n},
				fields=["name", "file_name", "is_private", "custom_is_s3_uploaded", "custom_s3_key", "custom_s3_bucket_name"], limit=1)
			if not rows:
				rows = frappe.get_all("File", filters=[["custom_s3_key", "like", "%" + n]],
					fields=["name", "file_name", "is_private", "custom_is_s3_uploaded", "custom_s3_key", "custom_s3_bucket_name"], limit=1)
			if not rows:
				rows = frappe.get_all("File", filters=[["file_url", "like", "%" + n]],
					fields=["name", "file_name", "is_private", "custom_is_s3_uploaded", "custom_s3_key", "custom_s3_bucket_name"], limit=1)
			if not rows:
				print(f"  {n}: NO File doc -> orphan on disk (safe to ignore)")
				continue
			f = rows[0]
			on_s3 = "n/a"
			if conn and f.custom_is_s3_uploaded and f.custom_s3_key:
				try:
					on_s3 = conn.verify_object(f.custom_s3_bucket_name, f.custom_s3_key,
						expected_size=os.path.getsize(os.path.join(base, n)))
				except Exception as e:
					on_s3 = f"ERR({type(e).__name__})"
			matches = os.path.basename(_local_path(f.file_name, f.is_private)) == n
			print(f"  {n}: s3_uploaded={f.custom_is_s3_uploaded} verified_on_s3={on_s3} "
			      f"my_lookup_matches={matches} key={f.custom_s3_key}")


# ---------------------------------------------------------------------------------------
# Orphan report — on-disk files that NO File doc references (unreachable by the app).
# These are what's left after migration (migrated files' local copies were already
# removed). READ-ONLY: counts + total size so you can decide how to reclaim the space.
#   bench --site <site> execute frappe_s3_integration.s3_normalize.orphan_report
# ---------------------------------------------------------------------------------------

def _referenced_basenames():
	"""Every on-disk basename that SOME File doc could legitimately own — anything not in
	this set is an orphan (no File doc points at it)."""
	refs = set()
	for f in frappe.get_all("File", filters={"is_folder": 0},
			fields=["file_name", "file_url", "custom_s3_key"]):
		if f.file_name:
			refs.add(re.sub(r"[/\\%?#]", "_", f.file_name))
		for v in (f.file_url, f.custom_s3_key):
			if v:
				base = v.split("?")[0].rstrip("/").rsplit("/", 1)[-1]
				if base:
					refs.add(base)
	return refs


def _iter_orphans(refs):
	"""Yield (abs_path, is_private, size) for every on-disk file with no owning File doc."""
	for is_private in (1, 0):
		base = get_files_path(is_private=is_private)
		try:
			names = os.listdir(base)
		except Exception:
			continue
		for n in names:
			if n in refs:
				continue
			p = os.path.join(base, n)
			if not os.path.isfile(p):
				continue
			try:
				yield p, is_private, os.path.getsize(p)
			except Exception:
				continue


def orphan_report():
	"""READ-ONLY: count + total size of orphan files (on disk, no File doc references them)."""
	refs = _referenced_basenames()
	priv_n = priv_b = pub_n = pub_b = 0
	for _p, is_private, sz in _iter_orphans(refs):
		if is_private:
			priv_n += 1; priv_b += sz
		else:
			pub_n += 1; pub_b += sz
	gb = lambda b: b / (1024 ** 3)
	print(f"[orphans] private/files: {priv_n} orphan file(s), {gb(priv_b):.2f} GB")
	print(f"[orphans] public/files : {pub_n} orphan file(s), {gb(pub_b):.2f} GB")
	print(f"[orphans] TOTAL: {priv_n + pub_n} orphan file(s), {gb(priv_b + pub_b):.2f} GB reclaimable")
	print("[orphans] (orphan = on disk but NO File doc references it — unreachable by the app)")
	return {"private": priv_n, "public": pub_n, "bytes": priv_b + pub_b}
