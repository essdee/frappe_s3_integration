import hashlib
import os

import frappe
from frappe.utils import get_site_path
from rq.timeouts import JobTimeoutException
from werkzeug.datastructures import FileStorage

from frappe_s3_integration.s3_core import (
	getS3Connection, get_proxy_url, _guess_content_type, _s3_key_from_file_url, child_attach_repoint,
)


def _hash_local_file(path):
	"""Chunked md5 of a local file — matches Frappe's content_hash and the S3 upload hash."""
	h = hashlib.md5()
	with open(path, "rb") as f:
		for chunk in iter(lambda: f.read(8192), b""):
			h.update(chunk)
	return h.hexdigest()


def _local_path(file):
	"""Deterministic local path for a /files or /private/files url; None otherwise.
	Avoids get_file_path's name-matching ambiguity (N11) and path traversal (N10)."""
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
	"""True if another File with the same physical blob still needs the local bytes (M1).

	Only a sibling that is actually GOING to migrate may hold the blob back. A row that
	was never flagged (inserted while disable_s3_operations was on) will never migrate,
	so without the custom_is_s3_uploaded filter it would block the delete forever."""
	if not file.content_hash:
		return False
	return bool(frappe.get_all("File", filters={
		"content_hash": file.content_hash, "is_private": file.is_private,
		"name": ["!=", file.name],
		"custom_is_s3_uploaded": 1,
		"custom_s3_key": ["in", ["", None]],
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
	"""Repoint a File doc at an S3 object. Preserve (never clobber) Frappe's
	content_hash — dedup and both delete guards depend on it."""
	frappe.db.set_value("File", file.name, {
		"file_url": get_proxy_url(file.name, file.file_name),
		"custom_s3_key": key,
		"custom_s3_bucket_name": bucket,
	})


def _repoint_attached(file):
	"""Invariant 2: point the attached doc's Attach field at this file's S3 proxy url, so
	the parent record (Website Settings.app_logo, Employee.image, ...) no longer serves a
	now-deleted /files/<x> path. Called from EVERY migration path — the dedup shortcuts and
	the local-heal path must repoint too, not just the fresh-upload path.

	Identity-guarded: only overwrite the parent field when it STILL holds THIS file's own
	pre-migration local url. Here file.file_url is still that local url — _point_doc_at_s3
	writes the DB row, not the in-memory doc. This preserves an external URL, a cleared
	field, or a NEWER attachment on the same field, and prevents repointing the field to an
	OLDER sibling File that also links the same (doctype, name, field) — Frappe leaves the
	old File's attached_to_field set when an Attach field is replaced.

	Singles-aware: a Single doctype (e.g. Website Settings) stores values in tabSingles, so
	repoint via set_single_value rather than the deprecated single-through-set_value route.
	Best-effort + isolated commit — must never raise and roll back the durable S3 pointer (N2)."""
	if not (file.attached_to_doctype and file.attached_to_field):
		return
	try:
		meta = frappe.get_meta(file.attached_to_doctype)
		proxy = get_proxy_url(file.name, file.file_name)
		if not meta.has_field(file.attached_to_field):
			# Not a parent field — may be a CHILD-table Attach field (e.g. Essdee Bulk Payment
			# .advance_image lives on child 'Essdee Bulk Payment Entry'). file.file_url is still
			# this file's local url here, so it's the identity to match child rows against.
			if child_attach_repoint(file.attached_to_doctype, file.attached_to_name,
			                        file.attached_to_field, file.file_url, proxy):
				frappe.db.commit()
			return
		single = meta.issingle
		if single:
			current = frappe.db.get_single_value(file.attached_to_doctype, file.attached_to_field)
		elif file.attached_to_name and frappe.db.exists(file.attached_to_doctype, file.attached_to_name):
			current = frappe.db.get_value(file.attached_to_doctype, file.attached_to_name,
			                              file.attached_to_field)
		else:
			return
		if current != file.file_url:
			return  # field moved on (external / cleared / newer file) — never clobber it
		if single:
			frappe.db.set_single_value(file.attached_to_doctype, file.attached_to_field,
			                           proxy, update_modified=False)
		else:
			frappe.db.set_value(file.attached_to_doctype, file.attached_to_name,
			                    file.attached_to_field, proxy, update_modified=False)
		frappe.db.commit()
	except Exception:
		frappe.log_error(frappe.get_traceback(), f"S3 attached-doc repoint failed for {file.name}")


SWEEP_TIMEOUT_FLOOR = 3 * 60 * 60             # never below the previous fixed budget
SWEEP_TIMEOUT_CAP = 24 * 60 * 60              # a bad estimate must not pin a worker forever
S3_THROUGHPUT_MBPS = 0.5                        # effective MB/s (~500 KB/s: conservative bench->S3 uplink)
PER_FILE_OVERHEAD_S = 1.0                      # per-file fixed cost (get_doc + dedup + head + commit + rm)
SWEEP_SAFETY_BUFFER = 2.0                      # slack for variance / retries / queue contention
DEFAULT_UNKNOWN_FILE_SIZE = 10 * 1024 * 1024   # assumed bytes when size unknown and blob unstat-able


def _pending_migration_stats():
	"""(total_bytes, count) for local Files flagged for S3 but not yet migrated.
	Trust file_size when >0; else stat the on-disk blob; else a realistic default."""
	rows = frappe.get_all("File", filters=[
		["custom_is_s3_uploaded", "=", 1],
		["custom_s3_key", "in", ["", None]],
	], fields=["file_url", "file_size"])
	total = 0
	for r in rows:
		size = r.file_size or 0
		if size <= 0:
			path = _local_path(frappe._dict(file_url=r.file_url))
			try:
				size = os.path.getsize(path) if path and os.path.exists(path) else DEFAULT_UNKNOWN_FILE_SIZE
			except OSError:
				size = DEFAULT_UNKNOWN_FILE_SIZE
		total += size
	return total, len(rows)


def _sweep_timeout():
	"""Enqueue timeout sized to the actual backlog: MAX of byte vs per-file bottleneck,
	times a safety buffer, clamped to [floor, cap]. All knobs overridable via site_config."""
	conf = frappe.get_conf()
	throughput = float(conf.get("s3_sweep_throughput_mbps") or S3_THROUGHPUT_MBPS)
	per_file = float(conf.get("s3_sweep_per_file_overhead_s") or PER_FILE_OVERHEAD_S)
	buffer_ = float(conf.get("s3_sweep_safety_buffer") or SWEEP_SAFETY_BUFFER)
	floor_ = int(conf.get("s3_sweep_timeout_floor") or SWEEP_TIMEOUT_FLOOR)
	cap_ = int(conf.get("s3_sweep_timeout_cap") or SWEEP_TIMEOUT_CAP)
	total_bytes, count = _pending_migration_stats()
	byte_seconds = total_bytes / (throughput * 1024 * 1024)
	count_seconds = count * per_file
	raw = max(byte_seconds, count_seconds) * buffer_
	return int(min(cap_, max(floor_, raw)))


@frappe.whitelist()
def process_unuploaded_documents():
	"""Scheduler entry (cron). Offload the migration sweep to the long queue with a
	timeout sized to the pending backlog (see _sweep_timeout) so a large migrate+remove
	backlog isn't killed by the default short timeout. Deduplicated so a slow run can't
	overlap the next night's run."""
	if not frappe.has_permission("AWS S3 Settings", "read"):
		frappe.throw("Not permitted", frappe.PermissionError)
	frappe.enqueue(
		run_unuploaded_documents_sweep,
		queue="long",
		timeout=_sweep_timeout(),
		job_id="frappe_s3_integration::migrate_sweep",
		deduplicate=True,
	)


def run_unuploaded_documents_sweep():
	"""Migrate every flagged local File to S3 and delete the local copy. Runs on the
	long queue (see process_unuploaded_documents); each file is committed + deleted
	independently so a 3-hour budget covers a large backlog."""
	conn = getS3Connection()
	if conn.s3_settings.disable_s3_operations:
		return
	files = frappe.get_all("File", filters=[
		["custom_is_s3_uploaded", "=", 1],
		["custom_s3_key", "in", ["", None]],
	], fields=["name"])
	for f in files:
		try:
			migrate_file_to_s3(f.name, conn)
		except JobTimeoutException:
			# Deadline reached: stop cleanly instead of swallowing it and running
			# unbounded. Remaining files resume next night (idempotent + dedup'd).
			frappe.db.rollback()
			raise
		except Exception:
			frappe.db.rollback()
			frappe.log_error(frappe.get_traceback(), f"S3 upload failed for File {f.name}")
	if files:
		# A repointed Single (e.g. Website Settings.app_logo) is written straight to
		# tabSingles, bypassing its on_update website-cache rebuild — refresh once so the
		# new proxy url is served without a manual `bench clear-cache`.
		frappe.clear_cache()


def migrate_file_to_s3(file_name, conn):
	file = frappe.get_doc("File", file_name)
	if file.custom_s3_key:
		return  # already migrated — idempotent (invariant 2)

	local_path = _local_path(file)

	# Local bytes gone: heal from a migrated sibling sharing the blob (N1) — never lose the
	# pointer. Match ONLY by content_hash (byte-safe); a shared url is NOT proof of shared
	# content once local copies are deleted (a recycled filename can hold different bytes).
	if not local_path or not os.path.exists(local_path):
		sib = _migrated_sibling(file)
		if sib and conn.verify_object(sib.custom_s3_bucket_name, sib.custom_s3_key):
			_point_doc_at_s3(file, sib.custom_s3_key, sib.custom_s3_bucket_name)
			frappe.db.commit()
			_repoint_attached(file)  # invariant 2: this doc's attach field too
			return
		frappe.log_error(f"Local file missing & no recoverable sibling: {file.name} ({local_path})", "S3 Migration")
		return

	local_size = os.path.getsize(local_path)
	if local_size == 0:
		frappe.log_error(f"Local file is empty: {local_path}", "S3 Migration")
		return

	# Ensure the blob's content_hash is stored — dedup (_migrated_sibling) and both
	# delete guards depend on it; backfill from local bytes if Frappe never set one.
	if not file.content_hash:
		file.content_hash = _hash_local_file(local_path)
		frappe.db.set_value("File", file.name, "content_hash", file.content_hash, update_modified=False)

	# A sibling already migrated this exact blob -> reuse its object, no re-upload (M1).
	# Verify the sibling's object still exists before trusting it & deleting local bytes.
	sib = _migrated_sibling(file)
	if sib and conn.verify_object(sib.custom_s3_bucket_name, sib.custom_s3_key):
		_point_doc_at_s3(file, sib.custom_s3_key, sib.custom_s3_bucket_name)
		frappe.db.commit()
		_repoint_attached(file)  # invariant 2: dedup'd sibling still owns its own attach field
		_maybe_remove_local(file, local_path)
		return

	content_type = _guess_content_type(file.file_name)
	# Mirror Frappe's own layout as the S3 key: files/<name> or private/files/<name>.
	s3_key = _s3_key_from_file_url(file.file_url)
	with open(local_path, "rb") as f:
		file_obj = FileStorage(stream=f, filename=file.file_name, content_type=content_type)
		if file.is_private:
			s3_resp = conn.upload_file_to_private_bucket(file_obj, key=s3_key)
		else:
			s3_resp = conn.upload_file_to_public_bucket(file_obj, key=s3_key)

	if not s3_resp or not s3_resp.get("content_hash"):
		raise Exception("S3 upload failed or returned no content hash")  # M8

	# Invariant 1: confirm the object is really there before touching DB / local file.
	if not conn.verify_object(s3_resp["bucket_name"], s3_resp["key"], expected_size=local_size):
		raise Exception(f"S3 object verification failed for {file.name}")

	# Persist the durable S3 pointer FIRST, in its own commit (invariant 1 + N2).
	_point_doc_at_s3(file, s3_resp["key"], s3_resp["bucket_name"])
	frappe.db.commit()

	# Invariant 2: repoint the attached doc's field (singles-aware, isolated non-fatal commit).
	_repoint_attached(file)

	_maybe_remove_local(file, local_path)


def _maybe_remove_local(file, local_path):
	"""Delete the local blob only if no other File still needs it locally (M1)."""
	if _other_unmigrated_share(file):
		return  # the last sibling to migrate removes the shared blob
	try:
		os.remove(local_path)
	except Exception:
		frappe.log_error(f"Failed to delete local file: {local_path}", "S3 Cleanup")
