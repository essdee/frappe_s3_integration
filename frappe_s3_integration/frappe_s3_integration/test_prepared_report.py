import gzip
from unittest.mock import MagicMock, patch

import frappe
from frappe.cache_manager import clear_controller_cache
from frappe.tests.utils import FrappeTestCase

from frappe_s3_integration import prepared_report
from frappe_s3_integration.overrides import S3File


class TestS3PreparedReport(FrappeTestCase):
    def test_prepared_report_controller_override_is_registered(self):
        frappe.cache.delete_value("app_hooks")
        clear_controller_cache("Prepared Report")
        self.assertIsInstance(
            frappe.new_doc("Prepared Report"), prepared_report.S3PreparedReport
        )

    def test_proxy_query_does_not_hide_gzip_attachment(self):
        payload = b'{"columns":[],"result":[]}'
        compressed = gzip.compress(payload)
        attachment = frappe._dict(
            name="FILE-1",
            file_name="report.json.gz",
            file_url=(
                "/api/method/frappe_s3_integration.s3_core.serve_file/"
                "report.json.gz?file_id=FILE-1"
            ),
        )
        file_doc = MagicMock()
        file_doc.get_content.return_value = compressed
        doc = prepared_report.S3PreparedReport(
            {"doctype": "Prepared Report", "name": "PREPARED-1"}
        )

        with (
            patch.object(prepared_report, "get_attachments", return_value=[attachment]),
            patch.object(prepared_report.frappe, "get_doc", return_value=file_doc),
        ):
            data, file_name = doc.get_prepared_data(with_file_name=True)

        self.assertEqual(data, payload)
        self.assertEqual(file_name, "report.json.gz")

    def test_file_name_is_authoritative_when_proxy_shape_changes(self):
        attachment = frappe._dict(
            file_name="report.json.gz",
            file_url="/api/method/frappe_s3_integration.s3_core.serve_file?file_id=FILE-1",
        )
        self.assertTrue(prepared_report._is_gzip_attachment(attachment))

    def test_s3_file_keeps_gzip_as_raw_bytes(self):
        compressed = gzip.compress(b'{"result":[]}')
        doc = S3File(
            {
                "doctype": "File",
                "file_name": "report.json.gz",
                "file_url": (
                    "/api/method/frappe_s3_integration.s3_core.serve_file/"
                    "report.json.gz?file_id=FILE-1"
                ),
                "custom_is_s3_uploaded": 1,
                "custom_s3_key": "private/files/report.json.gz",
                "custom_s3_bucket_name": "private-bucket",
                "is_private": 1,
            }
        )
        connection = MagicMock()
        connection.s3_settings.disable_s3_operations = 0
        connection.get_file_from_bucket.return_value = {
            "Body": MagicMock(read=lambda: compressed)
        }

        with patch(
            "frappe_s3_integration.s3_core.getS3Connection",
            return_value=connection,
        ):
            content = doc.get_content()

        self.assertIsInstance(content, bytes)
        self.assertEqual(gzip.decompress(content), b'{"result":[]}')
