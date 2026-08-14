# Copyright (c) 2026, sakthi123msd@gmail.com and Contributors
# See license.txt

import os
from unittest import TestCase
from unittest.mock import patch
from urllib.parse import quote

import frappe

from frappe_s3_integration.frappe_s3_integration import process_scheduler as ps
from frappe_s3_integration.s3_core import _s3_key_from_file_url


class TestEncodedFileUrls(TestCase):
	filenames = (
		"WhatsApp Image 2024-07-25 at 5.34.35 PM8d89af.jpeg",
		"WhatsApp Image 2025-01-09 at 6.00.44 PM6fb012.jpeg",
	)

	def test_encoded_whatsapp_names_resolve_to_local_files_and_s3_keys(self):
		base = "/site/private/files"
		with patch.object(ps, "get_site_path", return_value=base):
			for filename in self.filenames:
				url = f"/private/files/{quote(filename, safe='')}"
				with self.subTest(filename=filename):
					self.assertEqual(
						ps._local_path(frappe._dict(file_url=url)),
						os.path.join(base, filename),
					)
					self.assertEqual(
						_s3_key_from_file_url(url),
						f"private/files/{filename}",
					)

	def test_encoded_parent_directory_is_rejected(self):
		url = "/private/files/%2e%2e/site_config.json"
		with patch.object(ps, "get_site_path", return_value="/site/private/files"):
			self.assertIsNone(ps._local_path(frappe._dict(file_url=url)))
		self.assertIsNone(_s3_key_from_file_url(url))
