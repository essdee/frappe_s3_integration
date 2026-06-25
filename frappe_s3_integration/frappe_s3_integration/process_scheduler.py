import hashlib
import os

import frappe
from frappe.utils import get_site_path
from werkzeug.datastructures import FileStorage

from frappe_s3_integration.s3_core import getS3Connection, get_proxy_url, _guess_content_type


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
	"""True if another File with the same physical blob still needs the local bytes (M1)."""
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
	"""Repoint a File doc at an S3 object. Preserve (never clobber) Frappe's
	content_hash — dedup and both delete guards depend on it."""
	frappe.db.set_value("File", file.name, {
		"file_url": get_proxy_url(file.name, file.file_name),
		"custom_s3_key": key,
		"custom_s3_bucket_name": bucket,
	})


SWEEP_TIMEOUT = 3 * 60 * 60  # 3 hours — migrating + deleting a large local-file backlog is slow


@frappe.whitelist()
def process_unuploaded_documents():
	"""Scheduler entry (cron). Offload the migration sweep to the long queue with a
	3-hour timeout so a large migrate+remove backlog isn't killed by the default
	short timeout. Deduplicated so a slow run can't overlap the next night's run."""
	if not frappe.has_permission("AWS S3 Settings", "read"):
		frappe.throw("Not permitted", frappe.PermissionError)
	frappe.enqueue(
		run_unuploaded_documents_sweep,
		queue="long",
		timeout=SWEEP_TIMEOUT,
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
		except Exception:
			frappe.db.rollback()
			frappe.log_error(frappe.get_traceback(), f"S3 upload failed for File {f.name}")


def migrate_file_to_s3(file_name, conn):
	file = frappe.get_doc("File", file_name)
	if file.custom_s3_key:
		return  # already migrated — idempotent (invariant 2)

	local_path = _local_path(file)

	# Local bytes gone: heal from a migrated sibling sharing the blob (N1) — never lose the pointer.
	if not local_path or not os.path.exists(local_path):
		sib = _migrated_sibling(file)
		if sib and conn.verify_object(sib.custom_s3_bucket_name, sib.custom_s3_key):
			_point_doc_at_s3(file, sib.custom_s3_key, sib.custom_s3_bucket_name)
			frappe.db.commit()
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
		_maybe_remove_local(file, local_path)
		return

	content_type = _guess_content_type(file.file_name)
	with open(local_path, "rb") as f:
		file_obj = FileStorage(stream=f, filename=file.file_name, content_type=content_type)
		if file.is_private:
			s3_resp = conn.upload_file_to_private_bucket(file_obj)
		else:
			s3_resp = conn.upload_file_to_public_bucket(file_obj)

	if not s3_resp or not s3_resp.get("content_hash"):
		raise Exception("S3 upload failed or returned no content hash")  # M8

	# Invariant 1: confirm the object is really there before touching DB / local file.
	if not conn.verify_object(s3_resp["bucket_name"], s3_resp["key"], expected_size=local_size):
		raise Exception(f"S3 object verification failed for {file.name}")

	# Persist the durable S3 pointer FIRST, in its own commit (invariant 1 + N2).
	_point_doc_at_s3(file, s3_resp["key"], s3_resp["bucket_name"])
	frappe.db.commit()

	# Best-effort: repoint the attached-doc field. Non-fatal — must not roll back the pointer (N2).
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
		return  # the last sibling to migrate removes the shared blob
	try:
		os.remove(local_path)
	except Exception:
		frappe.log_error(f"Failed to delete local file: {local_path}", "S3 Cleanup")
