# Copyright (c) 2026, sakthi123msd@gmail.com and contributors
# For license information, please see license.txt
"""One-time, resumable, non-destructive content-type backfill for objects already
on S3 that were written before ContentType was set (stored as octet-stream).

Public files have no read-time MIME self-heal (they 302-redirect to the raw S3
object), so this is the ONLY remediation for old public objects (N14)."""

import frappe
from frappe.utils import sbool

from frappe_s3_integration.s3_core import getS3Connection, _guess_content_type

OCTET = ("binary/octet-stream", "application/octet-stream", "", None)
MAX_SINGLE_COPY = 5 * 1024 ** 3  # S3 single-operation copy limit (M6)


@frappe.whitelist()
def backfill_content_types(dry_run=True):
	"""Re-tag octet-stream objects with a guessed content-type via server-side
	copy_object (MetadataDirective=REPLACE). Non-destructive (invariant 4); each
	object is isolated so one failure can't abort the run (M6). Re-runnable —
	already-correct objects are skipped."""
	if not frappe.has_permission("AWS S3 Settings", "write"):
		frappe.throw("Not permitted", frappe.PermissionError)
	dry_run = sbool(dry_run)
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
					continue  # nothing better to set
				fixed += 1
				if dry_run:
					continue
				params = {
					"Bucket": bucket_name,
					"Key": key,
					"CopySource": {"Bucket": bucket_name, "Key": key},
					"ContentType": new_ct,
					"MetadataDirective": "REPLACE",
				}
				if head.get("Metadata"):  # REPLACE drops unspecified metadata — carry user metadata forward
					params["Metadata"] = head["Metadata"]
				if is_public:
					params["ACL"] = "public-read"
				conn.connection.copy_object(**params)
			except Exception:
				errors += 1
				frappe.log_error(frappe.get_traceback(), f"S3 Backfill failed: {bucket_name}/{key}")
		out[bucket_name] = {"scanned": scanned, "fixed": fixed, "errors": errors}
	return out
