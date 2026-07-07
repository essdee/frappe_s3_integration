# Copyright (c) 2026, sakthi123msd@gmail.com and contributors
# For license information, please see license.txt
"""Serve stale local file urls from S3 — a before_request fallback.

After migration a File's bytes live on S3 and its local /files/<x> copy is deleted,
but hardcoded local urls survive all over the site: letterhead/rich-text HTML content,
old emails, print formats, cached pages. Those requests then break:

  - public  /files/<x>          -> nginx miss -> @webserver -> Frappe 404
  - private /private/files/<x>  -> download_private_file looks up File by file_url,
                                   which is now the proxy url -> 403

This hook catches exactly those requests and 302-redirects them to the file's proxy
url (which enforces permissions for private files and redirects public ones to S3).

Cost: existing public files are still served by nginx and never reach this code;
private hits pay one startswith + stat and then fall through to core. It never
overrides core serving: if the local file exists, or no S3-backed File matches,
the request proceeds untouched.

Security parity with core: /private/files responses must not become an existence
oracle. Core returns a uniform 403 to Guests and to users without read permission —
so this hook only redirects a private path for a logged-in user WITH read permission
on the matching File; everyone else falls through to core's uniform 403.

The lookup key is the request path itself: normalization guarantees custom_s3_key
mirrors Frappe's layout ('files/<name>' / 'private/files/<name>'), so the stale local
url minus its leading slash IS the S3 key.
"""

import os

import frappe
from frappe.utils import get_site_path
from werkzeug.exceptions import HTTPException
from werkzeug.utils import redirect

_PREFIXES = ("/files/", "/private/files/")


def _local_disk_path(path):
	"""On-disk location of a /files or /private/files url; None on traversal."""
	if path.startswith("/private/files/"):
		rel = path[len("/private/files/"):]
		# honor a site_config private_path override, like core's send_private_file
		base = get_site_path(frappe.local.conf.get("private_path", "private"), "files")
	else:
		rel, base = path[len("/files/"):], get_site_path("public", "files")
	abs_path = os.path.normpath(os.path.join(base, rel))
	if not abs_path.startswith(os.path.normpath(base) + os.sep):
		return None  # traversal attempt — let core reject it
	return abs_path


def _may_see_private(file_name):
	"""Core-parity gate for /private/files: only a logged-in user WITH read permission
	on the File may receive the redirect — anyone else gets core's uniform 403, so the
	302-vs-403 difference can never become an existence oracle for private files."""
	if getattr(frappe, "session", None) is None or frappe.session.user in (None, "", "Guest"):
		return False
	from frappe.core.doctype.file.file import has_permission as file_has_permission

	return bool(file_has_permission(frappe.get_doc("File", file_name), "read"))


def redirect_missing_local_files():
	"""before_request hook: if a /files or /private/files request has no local file but
	an S3-backed File doc owns that exact path, redirect to its proxy url. Best-effort —
	any surprise falls through to core behavior, never blocks a request."""
	try:
		request = getattr(frappe.local, "request", None)
		path = getattr(request, "path", None) or ""
		if not path.startswith(_PREFIXES):
			return
		if request.method not in ("GET", "HEAD"):
			return
		disk = _local_disk_path(path)
		if not disk or os.path.exists(disk):
			return  # local copy exists (or bad path) — core serves it as always
		key = path.lstrip("/")
		row = frappe.db.get_value(
			"File",
			{"custom_s3_key": key, "custom_is_s3_uploaded": 1},
			["name", "file_name", "custom_s3_key"],
			as_dict=True,
			order_by="creation asc",
		)
		# SQL collation folds case/trailing spaces — require byte-exact key equality so a
		# case-variant request can never be redirected to a DIFFERENT file.
		if not row or row.custom_s3_key != key:
			return  # not one of ours — normal 404/403 applies
		if path.startswith("/private/files/") and not _may_see_private(row.name):
			return  # keep core's uniform 403 — never leak private-file existence
		from frappe_s3_integration.s3_core import get_proxy_url

		# Raise the redirect as an HTTPException so app.py returns it directly
		# (before_request return values are discarded). 302: never cached permanently.
		exc = HTTPException(response=redirect(get_proxy_url(row.name, row.file_name), code=302))
		raise exc
	except HTTPException:
		raise
	except Exception:
		# defer_insert: GET/HEAD transactions are rolled back after the response, which
		# would silently discard a plain Error Log row — redis-backed insert survives.
		frappe.log_error(title="S3 local-url fallback failed",
		                 message=frappe.get_traceback(), defer_insert=True)
