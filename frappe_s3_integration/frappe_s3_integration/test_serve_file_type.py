# Copyright (c) 2026, sakthi123msd@gmail.com and Contributors
# See license.txt
"""_stream_from_s3 must send a usable Content-Type and filename for objects stored
as octet-stream under a name without an extension."""

import io
from unittest.mock import MagicMock, patch

import frappe
from frappe.tests.utils import FrappeTestCase

from frappe_s3_integration import s3_core

JPEG = b"\xff\xd8\xff\xe0\x00\x10JFIF" + b"\x00" * 20000
PNG = b"\x89PNG\r\n\x1a\n" + b"\x00" * 100
GIF = b"GIF89a" + b"\x00" * 100
PDF = b"%PDF-1.7\n" + b"\x00" * 100
WEBP = b"RIFF\x24\x00\x00\x00WEBPVP8 " + b"\x00" * 100


def _stream(content, file_name, content_type="application/octet-stream"):
	conn = MagicMock()
	conn.s3_settings.disable_s3_operations = 0
	conn.get_file_from_bucket.return_value = {"ContentType": content_type, "Body": io.BytesIO(content)}
	file_doc = frappe._dict(custom_s3_key="k", custom_s3_bucket_name="prv", file_name=file_name)
	with patch.object(s3_core, "getS3Connection", return_value=conn):
		response = s3_core._stream_from_s3(file_doc)
	return response, b"".join(response.response)


class TestServeFileType(FrappeTestCase):
	def test_octet_stream_without_extension_is_served_as_jpeg(self):
		response, body = _stream(JPEG, "LR_100043087565_INV2627-05430")

		self.assertEqual(response.mimetype, "image/jpeg")
		self.assertEqual(
			response.headers["Content-Disposition"], 'inline; filename="LR_100043087565_INV2627-05430.jpg"'
		)
		self.assertEqual(body, JPEG)

	def test_other_signatures_are_detected(self):
		cases = ((PNG, "image/png", ".png"), (GIF, "image/gif", ".gif"),
		         (PDF, "application/pdf", ".pdf"), (WEBP, "image/webp", ".webp"))
		for content, mimetype, extension in cases:
			with self.subTest(mimetype=mimetype):
				response, body = _stream(content, "scan")
				self.assertEqual(response.mimetype, mimetype)
				self.assertIn(f'filename="scan{extension}"', response.headers["Content-Disposition"])
				self.assertEqual(body, content)

	def test_stored_content_type_wins(self):
		response, _ = _stream(PNG, "scan", content_type="image/png")

		self.assertEqual(response.mimetype, "image/png")
		self.assertIn('filename="scan"', response.headers["Content-Disposition"])

	def test_extension_in_name_is_kept(self):
		response, _ = _stream(JPEG, "scan.jpeg")

		self.assertEqual(response.mimetype, "image/jpeg")
		self.assertIn('filename="scan.jpeg"', response.headers["Content-Disposition"])

	def test_unknown_bytes_stay_octet_stream(self):
		response, body = _stream(b"plain bytes", "blob")

		self.assertEqual(response.mimetype, "application/octet-stream")
		self.assertIn('filename="blob"', response.headers["Content-Disposition"])
		self.assertEqual(body, b"plain bytes")

	def test_empty_object_streams_nothing(self):
		response, body = _stream(b"", "blob")

		self.assertEqual(response.mimetype, "application/octet-stream")
		self.assertEqual(body, b"")
