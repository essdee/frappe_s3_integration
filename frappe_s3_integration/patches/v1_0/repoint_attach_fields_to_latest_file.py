# Copyright (c) 2026, sakthi123msd@gmail.com and contributors
# For license information, please see license.txt

"""Repair Attach fields that were repointed to an older File record.

Frappe retains the old File row when an attachment is replaced. The previous
S3 backfill could therefore see several File rows for the same document field
and, when their stale URLs were identical, let the oldest row win.

This patch is registered in ``patches.txt`` and applies during ``bench migrate``.
It can also be reviewed manually before migration:

    bench --site <site> execute \
        frappe_s3_integration.patches.v1_0.repoint_attach_fields_to_latest_file.execute \
        --kwargs '{"dry_run": 1}'

The repair is deliberately conservative: it only changes a duplicate target
whose current value still identifies one of its File rows. Empty, external,
unrelated, removed, and child-table fields are left untouched.
"""

from collections import Counter
from itertools import groupby
from operator import attrgetter

import frappe
from frappe.utils import cint

from frappe_s3_integration.s3_core import get_proxy_url


ATTACH_FIELD_TYPES = {"Attach", "Attach Image"}
FILE_FIELDS = [
	"name",
	"file_name",
	"file_url",
	"creation",
	"custom_is_s3_uploaded",
	"custom_s3_key",
	"attached_to_doctype",
	"attached_to_name",
	"attached_to_field",
]
FILE_FILTERS = [
	["is_folder", "=", 0],
	["attached_to_doctype", "is", "set"],
	["attached_to_name", "is", "set"],
	["attached_to_field", "is", "set"],
]
FILE_ORDER = (
	"attached_to_doctype asc, attached_to_name asc, attached_to_field asc, "
	"creation desc, name desc"
)
SAMPLE_LIMIT = 20


def execute(dry_run=0):
	"""Point duplicate attachment targets at their newest File by creation time."""
	dry_run = bool(cint(dry_run))
	files = frappe.get_all(
		"File",
		filters=FILE_FILTERS,
		fields=FILE_FIELDS,
		order_by=FILE_ORDER,
	)

	stats = Counter()
	samples = []
	target = attrgetter("attached_to_doctype", "attached_to_name", "attached_to_field")
	for _, rows in groupby(files, key=target):
		rows = list(rows)
		stats["targets_scanned"] += 1
		outcome, detail = _repair_attach_target(rows, dry_run=dry_run)
		stats[outcome] += 1
		if detail and len(samples) < SAMPLE_LIMIT:
			samples.append(detail)

	updated = stats["would_update" if dry_run else "updated"]
	if updated and not dry_run:
		frappe.db.commit()
		# Direct writes bypass Single DocType cache invalidation. Clearing once is
		# cheap and also keeps regular cached documents consistent after migration.
		frappe.clear_cache()

	result = {
		"dry_run": dry_run,
		"files_scanned": len(files),
		"targets_scanned": stats.pop("targets_scanned", 0),
		"would_update" if dry_run else "updated": updated,
		"outcomes": dict(stats),
		"sample_changes": samples,
	}
	print(f"[s3 latest-attachment repair] {result}")
	return result


def _repair_attach_target(files, dry_run=False):
	"""Repair one (doctype, document, field) group; newest File must be first."""
	if len(files) < 2:
		return "single_file", None
	if not any(f.custom_is_s3_uploaded and f.custom_s3_key for f in files):
		return "not_s3_related", None

	latest = files[0]
	if not latest.file_url:
		return "latest_url_missing", None

	doctype = latest.attached_to_doctype
	docname = latest.attached_to_name
	fieldname = latest.attached_to_field
	if not frappe.db.exists("DocType", doctype):
		return "doctype_missing", None

	meta = frappe.get_meta(doctype)
	field = meta.get_field(fieldname)
	if not field or field.fieldtype not in ATTACH_FIELD_TYPES:
		# Parent metadata cannot safely identify a particular child-table row.
		return "unsupported_or_child_field", None
	if not meta.issingle and not frappe.db.exists(doctype, docname):
		return "document_missing", None

	current = _current_attach_value(doctype, docname, fieldname, meta.issingle)
	if not current:
		return "field_cleared", None
	if current == latest.file_url:
		return "already_latest", None

	known_urls = set()
	for file in files:
		known_urls.update(_file_urls(file))
	if current not in known_urls:
		return "unrelated_current_value", None

	detail = {
		"doctype": doctype,
		"document": docname,
		"field": fieldname,
		"previous_url": current,
		"latest_url": latest.file_url,
		"latest_file": latest.name,
		"latest_creation": str(latest.creation),
	}
	if dry_run:
		return "would_update", detail

	_set_attach_value(doctype, docname, fieldname, latest.file_url, meta.issingle)
	return "updated", detail


def _file_urls(file):
	"""Return URLs that may historically identify this File in an Attach field."""
	urls = {file.file_url, get_proxy_url(file.name, file.file_name)}
	urls.add(f"/api/method/frappe_s3_integration.s3_core.serve_file?file_id={file.name}")
	if isinstance(file.custom_s3_key, str) and file.custom_s3_key.startswith(("files/", "private/files/")):
		urls.add("/" + file.custom_s3_key)
	return {url for url in urls if url}


def _current_attach_value(doctype, docname, fieldname, is_single):
	if is_single:
		return frappe.db.get_single_value(doctype, fieldname)
	return frappe.db.get_value(doctype, docname, fieldname)


def _set_attach_value(doctype, docname, fieldname, value, is_single):
	if is_single:
		frappe.db.set_single_value(doctype, fieldname, value, update_modified=False)
	else:
		frappe.db.set_value(doctype, docname, fieldname, value, update_modified=False)
