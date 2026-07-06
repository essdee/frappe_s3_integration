# Copyright (c) 2026, sakthi123msd@gmail.com and contributors
# For license information, please see license.txt
"""File controller override for S3-backed files."""

import frappe
from frappe import _
from frappe.core.doctype.file.file import File


class S3File(File):
	"""Make Frappe's File controller work for files that live on S3.

	After migration a File's url becomes our serve_file proxy
	(`/api/method/frappe_s3_integration.s3_core.serve_file/...`), so the bytes are on
	S3, not local disk. Frappe's local-disk code paths then break:
	  - validate_file_on_disk() -> get_full_path() -> is_safe_path() rejects the
	    relative /api/method path (only http/https are whitelisted) -> "Cannot access
	    file path" when re-saving a migrated File.
	  - get_content() -> open(get_full_path()) tries to open the proxy url as a local
	    path -> FileNotFoundError (e.g. ERPNext Bank Statement Import reading the sheet).
	Both are guarded for our S3-backed files; everything else delegates to core.
	"""

	def validate_file_on_disk(self):
		if self.file_url and "frappe_s3_integration.s3_core.serve_file" in self.file_url:
			return
		return super().validate_file_on_disk()

	def get_content(self) -> bytes:
		# S3-backed files have no local copy; fetch the bytes from S3 instead of
		# open()ing the proxy url. Mirrors core's decode-to-str-if-text behaviour.
		if not self.get("content") and self.get("custom_is_s3_uploaded") and self.get("custom_s3_key"):
			from frappe_s3_integration.s3_core import getS3Connection

			conn = getS3Connection()
			if conn.s3_settings.disable_s3_operations:
				frappe.throw(_("S3 file access is temporarily disabled"))
			obj = conn.get_file_from_bucket(self.custom_s3_key, self.custom_s3_bucket_name)
			self._content = obj["Body"].read()
			try:
				self._content = self._content.decode()
			except UnicodeDecodeError:
				pass  # binary (xlsx, png, …) stays bytes
			return self._content
		return super().get_content()
