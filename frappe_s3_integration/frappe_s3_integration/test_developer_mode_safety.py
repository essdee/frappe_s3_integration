# Copyright (c) 2026, sakthi123msd@gmail.com and Contributors
# See license.txt

from unittest.mock import MagicMock, patch

import frappe
from frappe.tests.utils import FrappeTestCase

from frappe_s3_integration import s3_core


class TestDeveloperModeS3Safety(FrappeTestCase):
	def _config(self, *, developer_mode=1, allow=None):
		config = frappe._dict(developer_mode=developer_mode)
		if allow is not None:
			config.s3_allow_operations = allow
		return patch.object(s3_core.frappe, "conf", config)

	def _settings(self, disabled=0):
		settings = MagicMock()
		settings.get.side_effect = {
			"disable_s3_operations": disabled,
			"aws_key": "key",
			"region": "ap-south-1",
		}.get
		settings.disable_s3_operations = disabled
		settings.aws_key = "key"
		settings.aws_secret = "secret"
		settings.region = "ap-south-1"
		settings.s3_bucket_details = []
		settings.get_password.return_value = "secret"
		return settings

	def test_developer_site_is_blocked_without_explicit_opt_in(self):
		with self._config(developer_mode=1), patch.object(
			s3_core.frappe, "get_single", return_value=self._settings()
		), patch.object(s3_core.s3, "client") as client:
			connection = s3_core.S3Connection()

		self.assertFalse(connection.operations_allowed)
		self.assertIsNone(connection.connection)
		client.assert_not_called()

	def test_developer_site_can_opt_in_explicitly(self):
		with self._config(developer_mode=1, allow=1), patch.object(
			s3_core.frappe, "get_single", return_value=self._settings()
		), patch.object(s3_core.s3, "client", return_value=MagicMock()) as client:
			connection = s3_core.S3Connection()

		self.assertTrue(connection.operations_allowed)
		client.assert_called_once()

	def test_production_mode_keeps_existing_behaviour_without_opt_in(self):
		with self._config(developer_mode=0), patch.object(
			s3_core.frappe, "get_single", return_value=self._settings()
		), patch.object(s3_core.s3, "client", return_value=MagicMock()) as client:
			connection = s3_core.S3Connection()

		self.assertTrue(connection.operations_allowed)
		client.assert_called_once()

	def test_settings_kill_switch_still_wins_after_developer_opt_in(self):
		with self._config(developer_mode=1, allow=1), patch.object(
			s3_core.frappe, "get_single", return_value=self._settings(disabled=1)
		), patch.object(s3_core.s3, "client") as client:
			connection = s3_core.S3Connection()

		self.assertFalse(connection.operations_allowed)
		client.assert_not_called()

	def test_new_local_file_is_not_flagged_for_s3_on_protected_developer_site(self):
		doc = MagicMock()
		doc.get.side_effect = {
			"is_folder": 0,
			"custom_is_s3_uploaded": 0,
			"custom_s3_key": "",
			"file_url": "/private/files/local.pdf",
		}.get
		with self._config(developer_mode=1):
			s3_core.flag_file_for_s3(doc)
		doc.db_set.assert_not_called()

	def test_local_delete_never_deletes_remote_object_on_protected_developer_site(self):
		doc = frappe._dict(
			name="FILE-1",
			custom_is_s3_uploaded=1,
			custom_s3_key="private/files/report.pdf",
			custom_s3_bucket_name="production-private",
		)
		with self._config(developer_mode=1), patch.object(
			s3_core, "getS3Connection"
		) as get_connection, patch.object(s3_core.frappe.db, "count") as count:
			s3_core.delete_file_from_s3(doc, "on_trash")

		get_connection.assert_not_called()
		count.assert_not_called()

	def test_cached_blocked_connection_rejects_every_mutation(self):
		connection = s3_core.S3Connection.__new__(s3_core.S3Connection)
		connection.connection = MagicMock()
		connection.operations_allowed = False
		connection.s3_settings = self._settings()

		mutations = (
			lambda: connection.create_bucket("new"),
			lambda: connection.delete_bucket("old"),
			lambda: connection.update_file_in_bucket(MagicMock(), "bucket", "key"),
			lambda: connection.copy_object_to_bucket("a", "k", "b", "f", False),
			lambda: connection.delete_file_from_bucket("key", "bucket"),
		)
		for mutation in mutations:
			with self.assertRaises(frappe.ValidationError):
				mutation()

		connection.connection.create_bucket.assert_not_called()
		connection.connection.delete_bucket.assert_not_called()
		connection.connection.upload_fileobj.assert_not_called()
		connection.connection.copy_object.assert_not_called()
		connection.connection.delete_object.assert_not_called()
