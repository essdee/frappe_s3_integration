# Copyright (c) 2026, sakthi123msd@gmail.com and contributors
# For license information, please see license.txt
"""File controller override for S3-backed files."""

from frappe.core.doctype.file.file import File


class S3File(File):
	"""Skip Frappe's on-disk validation for files that live on S3.

	After migration a File's url becomes our serve_file proxy
	(`/api/method/frappe_s3_integration.s3_core.serve_file/...`), which is a remote file
	with no local disk copy. Every other File.validate_* step already guards on
	is_remote_file; validate_file_on_disk does not, and its get_full_path() ->
	is_safe_path() rejects the relative /api/method path (is_safe_path only whitelists
	http/https). So re-saving an S3-backed File raised "Cannot access file path".
	Guard validate_file_on_disk the same way its siblings are guarded.
	"""

	def validate_file_on_disk(self):
		if self.file_url and "frappe_s3_integration.s3_core.serve_file" in self.file_url:
			return
		return super().validate_file_on_disk()
