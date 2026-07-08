# Copyright (c) 2026, sakthi123msd@gmail.com and Contributors
# See license.txt
"""upload_file_to_bucket must collision-guard EVERY key (even a caller-supplied one), so a
recycled filename can't overwrite an existing S3 object and make its File doc serve the
wrong content."""

import io
from unittest.mock import MagicMock

from frappe.tests.utils import FrappeTestCase
from werkzeug.datastructures import FileStorage

from frappe_s3_integration.s3_core import S3Connection


def _conn(exists_sequence):
	"""A bare S3Connection with a stubbed transport + verify_object result sequence."""
	conn = S3Connection.__new__(S3Connection)      # bypass __init__ (no real AWS)
	conn.connection = MagicMock()
	conn.connection.meta.region_name = "ap-south-1"
	conn.verify_object = MagicMock(side_effect=exists_sequence)
	return conn


def _file(content=b"data", name="report.pdf"):
	return FileStorage(stream=io.BytesIO(content), filename=name, content_type="application/pdf")


class TestUploadKeyCollision(FrappeTestCase):
	def _uploaded_key(self, conn):
		return conn.connection.upload_fileobj.call_args.kwargs["Key"]

	def test_supplied_key_is_suffixed_when_object_already_exists(self):
		# object at the supplied key exists -> must NOT overwrite; a suffix is added.
		conn = _conn([True, False])  # taken, then the suffixed candidate is free
		resp = conn.upload_file_to_bucket(_file(), bucket_name="bkt", allow_public=False,
		                                  key="private/files/report.pdf")
		used = self._uploaded_key(conn)
		self.assertNotEqual(used, "private/files/report.pdf")   # never overwrote the original
		self.assertTrue(used.startswith("private/files/report"))
		self.assertTrue(used.endswith(".pdf"))
		self.assertEqual(resp["key"], used)                     # doc stores the ACTUAL key

	def test_supplied_key_kept_when_object_is_free(self):
		# no collision -> the caller's Frappe-layout key is used unchanged.
		conn = _conn([False])
		conn.upload_file_to_bucket(_file(), bucket_name="bkt", allow_public=False,
		                           key="private/files/unique.pdf")
		self.assertEqual(self._uploaded_key(conn), "private/files/unique.pdf")

	def test_empty_file_is_rejected(self):
		conn = _conn([False])
		import frappe
		with self.assertRaises(frappe.exceptions.ValidationError):
			conn.upload_file_to_bucket(_file(content=b""), bucket_name="bkt", key="private/files/x.pdf")
