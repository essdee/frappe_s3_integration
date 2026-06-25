# Copyright (c) 2026, sakthi123msd@gmail.com and Contributors
# See license.txt

from unittest.mock import MagicMock, patch

from frappe.tests.utils import FrappeTestCase

from frappe_s3_integration.frappe_s3_integration import backfill


def _conn():
	conn = MagicMock()
	conn.public_bucket = "pub"
	conn.private_bucket = None
	conn.s3_settings.disable_s3_operations = 0
	return conn


class TestBackfill(FrappeTestCase):
	def test_only_octet_rewritten_and_resilient(self):
		conn = _conn()
		conn.list_objects.return_value = [
			{"Key": "uploads/a.png", "Size": 10},
			{"Key": "uploads/b.pdf", "Size": 10},
			{"Key": "uploads/c.png", "Size": 10},
		]
		conn.connection.head_object.side_effect = [
			{"ContentType": "binary/octet-stream", "Metadata": {"x": "1"}},  # a -> fix
			{"ContentType": "application/pdf"},                               # b -> skip
			Exception("transient"),                                          # c -> error, continue
		]
		with patch.object(backfill, "getS3Connection", return_value=conn), \
		     patch.object(backfill.frappe, "has_permission", return_value=True):
			res = backfill.backfill_content_types(dry_run=False)
		conn.connection.copy_object.assert_called_once()
		_, kw = conn.connection.copy_object.call_args
		self.assertEqual(kw["MetadataDirective"], "REPLACE")
		self.assertEqual(kw["ContentType"], "image/png")
		self.assertEqual(kw["Metadata"], {"x": "1"})       # N3: metadata preserved
		self.assertEqual(kw["ACL"], "public-read")         # public bucket keeps public ACL
		self.assertEqual(res["pub"], {"scanned": 3, "fixed": 1, "errors": 1})

	def test_dry_run_makes_no_changes(self):
		conn = _conn()
		conn.list_objects.return_value = [{"Key": "uploads/a.png", "Size": 10}]
		conn.connection.head_object.return_value = {"ContentType": "binary/octet-stream"}
		with patch.object(backfill, "getS3Connection", return_value=conn), \
		     patch.object(backfill.frappe, "has_permission", return_value=True):
			res = backfill.backfill_content_types(dry_run=True)
		conn.connection.copy_object.assert_not_called()
		self.assertEqual(res["pub"], {"scanned": 1, "fixed": 1, "errors": 0})
		self.assertTrue(res["dry_run"])

	def test_skips_oversize_object(self):
		conn = _conn()
		conn.list_objects.return_value = [{"Key": "big.bin", "Size": 6 * 1024 ** 3}]  # >5GB
		with patch.object(backfill, "getS3Connection", return_value=conn), \
		     patch.object(backfill.frappe, "has_permission", return_value=True):
			res = backfill.backfill_content_types(dry_run=False)
		conn.connection.head_object.assert_not_called()
		self.assertEqual(res["pub"], {"scanned": 1, "fixed": 0, "errors": 1})
