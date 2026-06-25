# Copyright (c) 2025, sakthi123msd@gmail.com and Contributors
# See license.txt

from unittest.mock import MagicMock, patch

import frappe
from botocore.exceptions import ClientError
from frappe.tests.utils import FrappeTestCase

from frappe_s3_integration import s3_core


class TestAWSS3Settings(FrappeTestCase):
	pass


class TestS3Engine(FrappeTestCase):
	"""Engine helpers — content-type, HEAD-verify, list_objects (Task 1.1)."""

	def _conn(self):
		c = s3_core.S3Connection.__new__(s3_core.S3Connection)
		c.connection = MagicMock()
		return c

	def test_guess_content_type(self):
		self.assertEqual(s3_core._guess_content_type("a.png"), "image/png")
		self.assertEqual(s3_core._guess_content_type("a.pdf"), "application/pdf")
		self.assertEqual(s3_core._guess_content_type("blob"), "application/octet-stream")
		self.assertEqual(s3_core._guess_content_type(None), "application/octet-stream")

	def test_verify_size_match(self):
		c = self._conn()
		c.connection.head_object.return_value = {"ContentLength": 10}
		self.assertTrue(c.verify_object("b", "k", 10))
		self.assertFalse(c.verify_object("b", "k", 11))

	def test_verify_size_none_just_checks_existence(self):
		c = self._conn()
		c.connection.head_object.return_value = {"ContentLength": 999}
		self.assertTrue(c.verify_object("b", "k"))

	def test_verify_absent_is_false(self):
		c = self._conn()
		c.connection.head_object.side_effect = ClientError({"Error": {"Code": "404"}}, "HeadObject")
		self.assertFalse(c.verify_object("b", "k"))

	def test_verify_transient_error_reraises(self):
		# M8: a throttle/5xx must NOT be mistaken for "object absent"
		c = self._conn()
		c.connection.head_object.side_effect = ClientError({"Error": {"Code": "SlowDown"}}, "HeadObject")
		with self.assertRaises(ClientError):
			c.verify_object("b", "k")

	def test_list_objects_paginates(self):
		c = self._conn()
		paginator = MagicMock()
		paginator.paginate.return_value = [
			{"Contents": [{"Key": "a"}, {"Key": "b"}]},
			{},  # empty page (no Contents key)
			{"Contents": [{"Key": "c"}]},
		]
		c.connection.get_paginator.return_value = paginator
		keys = [o["Key"] for o in c.list_objects("bucket")]
		self.assertEqual(keys, ["a", "b", "c"])


class TestValidateBuckets(FrappeTestCase):
	"""Global single-default guard that never bricks the kill-switch (Task 2.1 / M4)."""

	def _settings(self, rows, disabled=0):
		from frappe_s3_integration.frappe_s3_integration.doctype.aws_s3_settings.aws_s3_settings import AWSS3Settings
		d = AWSS3Settings.__new__(AWSS3Settings)
		d.disable_s3_operations = disabled
		d.s3_bucket_details = [frappe._dict(r) for r in rows]
		return d

	def test_one_each_ok(self):
		self._settings([
			{"bucket_name": "pub", "default_public_bucket": 1, "default_private_bucket": 0},
			{"bucket_name": "prv", "default_public_bucket": 0, "default_private_bucket": 1},
		]).validate_buckets()

	def test_two_publics_rejected(self):
		with self.assertRaises(Exception):
			self._settings([
				{"bucket_name": "p1", "default_public_bucket": 1, "default_private_bucket": 0},
				{"bucket_name": "p2", "default_public_bucket": 1, "default_private_bucket": 0},
			]).validate_buckets()

	def test_public_only_ok(self):
		# public-only is a valid shape (matches current prod config) — must not throw
		self._settings([
			{"bucket_name": "pub", "default_public_bucket": 1, "default_private_bucket": 0},
		]).validate_buckets()

	def test_both_flags_on_one_row_rejected(self):
		with self.assertRaises(Exception):
			self._settings([
				{"bucket_name": "x", "default_public_bucket": 1, "default_private_bucket": 1},
				{"bucket_name": "prv", "default_public_bucket": 0, "default_private_bucket": 1},
			]).validate_buckets()

	def test_skipped_when_disabled(self):
		# M4: keep Settings (and the kill-switch) saveable even with incomplete config
		self._settings([], disabled=1).validate_buckets()


class TestUploadSizeGuard(FrappeTestCase):
	"""Per-bucket size enforcement on the immediate upload path (Task 2.2)."""

	def test_create_file_rejects_oversize(self):
		conn = MagicMock()
		conn.validate_file_size.return_value = (True, 100)  # oversize, max 100 KiB
		file = MagicMock(filename="big.bin")
		with patch.object(s3_core, "getS3Connection", return_value=conn):
			with self.assertRaises(Exception):
				s3_core.create_file_and_upload_to_s3("FG Item Master", "X", file, is_public_bucket=True)
		conn.upload_file_to_public_bucket.assert_not_called()

	def test_validate_file_size_zero_means_no_limit(self):
		# A blanked/0 bucket limit must mean "no cap" — never reject every upload.
		c = s3_core.S3Connection.__new__(s3_core.S3Connection)
		c.public_bucket = "pub"
		c.bucket_restrictions = {"pub": {"image_max": 0, "file_max": 0}}
		f = MagicMock(filename="big.bin")
		oversize, _max = c.validate_file_size(f, is_public=True)
		self.assertFalse(oversize)
		f.stream.seek.assert_not_called()  # returned before reading the stream


class TestFlagFile(FrappeTestCase):
	"""File.after_insert capture hook (Task 4.1)."""

	def _doc(self, **kw):
		d = dict(is_folder=0, custom_is_s3_uploaded=0, custom_s3_key="",
		         file_url="/private/files/a.png")
		d.update(kw)
		m = MagicMock()
		m.get.side_effect = d.get
		return m

	def test_flags_local_file(self):
		doc = self._doc()
		with patch.object(s3_core.frappe.db, "get_single_value", return_value=0):
			s3_core.flag_file_for_s3(doc)
		doc.db_set.assert_called_once_with("custom_is_s3_uploaded", 1, update_modified=False)

	def test_flags_public_local_file(self):
		doc = self._doc(file_url="/files/a.png")
		with patch.object(s3_core.frappe.db, "get_single_value", return_value=0):
			s3_core.flag_file_for_s3(doc)
		doc.db_set.assert_called_once_with("custom_is_s3_uploaded", 1, update_modified=False)

	def test_skips_folder(self):
		doc = self._doc(is_folder=1)
		s3_core.flag_file_for_s3(doc)
		doc.db_set.assert_not_called()

	def test_skips_non_local_url(self):
		doc = self._doc(file_url="/api/method/frappe_s3_integration.s3_core.serve_file/x?file_id=Y")
		s3_core.flag_file_for_s3(doc)
		doc.db_set.assert_not_called()

	def test_skips_already_keyed(self):  # N8
		doc = self._doc(custom_s3_key="uploads/x.png")
		s3_core.flag_file_for_s3(doc)
		doc.db_set.assert_not_called()

	def test_skips_already_flagged(self):
		doc = self._doc(custom_is_s3_uploaded=1)
		s3_core.flag_file_for_s3(doc)
		doc.db_set.assert_not_called()

	def test_skips_when_disabled(self):
		doc = self._doc()
		with patch.object(s3_core.frappe.db, "get_single_value", return_value=1):
			s3_core.flag_file_for_s3(doc)
		doc.db_set.assert_not_called()


class TestVisibilityToggle(FrappeTestCase):
	"""is_private public<->private flip moves the S3 object to the matching bucket."""

	def _doc(self, **kw):
		d = dict(custom_is_s3_uploaded=1, custom_s3_key="uploads/x.png",
		         custom_s3_bucket_name="pub", is_private=1, file_name="x.png", name="F1")
		d.update(kw)
		m = MagicMock()
		m.get.side_effect = d.get
		for k, v in d.items():
			setattr(m, k, v)
		m.has_value_changed.return_value = True
		return m

	def _conn(self):
		conn = MagicMock()
		conn.public_bucket = "pub"
		conn.private_bucket = "prv"
		conn.s3_settings.disable_s3_operations = 0
		conn.copy_object_to_bucket.return_value = "uploads/new.png"
		conn.verify_object.return_value = True
		return conn

	def test_public_to_private_moves_object(self):
		doc = self._doc(is_private=1, custom_s3_bucket_name="pub")
		conn = self._conn()
		with patch.object(s3_core, "getS3Connection", return_value=conn), \
		     patch.object(s3_core.frappe.db, "count", return_value=0), \
		     patch.object(s3_core.frappe.db, "set_value") as sv:
			s3_core.handle_is_private_change(doc)
		conn.copy_object_to_bucket.assert_called_once_with("pub", "uploads/x.png", "prv", "x.png", False)
		sv.assert_called_once()
		conn.delete_file_from_bucket.assert_called_once_with("uploads/x.png", "pub")

	def test_private_to_public_moves_with_public_acl(self):
		doc = self._doc(is_private=0, custom_s3_bucket_name="prv", custom_s3_key="uploads/y.png")
		conn = self._conn()
		with patch.object(s3_core, "getS3Connection", return_value=conn), \
		     patch.object(s3_core.frappe.db, "count", return_value=0), \
		     patch.object(s3_core.frappe.db, "set_value"):
			s3_core.handle_is_private_change(doc)
		conn.copy_object_to_bucket.assert_called_once_with("prv", "uploads/y.png", "pub", "x.png", True)

	def test_shared_key_old_object_not_deleted(self):
		doc = self._doc(is_private=1, custom_s3_bucket_name="pub")
		conn = self._conn()
		with patch.object(s3_core, "getS3Connection", return_value=conn), \
		     patch.object(s3_core.frappe.db, "count", return_value=2), \
		     patch.object(s3_core.frappe.db, "set_value"):
			s3_core.handle_is_private_change(doc)
		conn.delete_file_from_bucket.assert_not_called()

	def test_no_move_when_is_private_unchanged(self):
		doc = self._doc()
		doc.has_value_changed.return_value = False
		conn = self._conn()
		with patch.object(s3_core, "getS3Connection", return_value=conn):
			s3_core.handle_is_private_change(doc)
		conn.copy_object_to_bucket.assert_not_called()

	def test_skips_non_s3_file(self):
		doc = self._doc(custom_s3_key="")
		conn = self._conn()
		with patch.object(s3_core, "getS3Connection", return_value=conn):
			s3_core.handle_is_private_change(doc)
		conn.copy_object_to_bucket.assert_not_called()

	def test_skips_when_target_bucket_missing(self):
		# Must NOT raise — a missing bucket can't be allowed to roll back File.save().
		doc = self._doc(is_private=1, custom_s3_bucket_name="pub")
		conn = self._conn()
		conn.private_bucket = None  # no private bucket configured
		with patch.object(s3_core, "getS3Connection", return_value=conn), \
		     patch.object(s3_core.frappe, "log_error"):
			s3_core.handle_is_private_change(doc)
		conn.copy_object_to_bucket.assert_not_called()

	def test_skips_when_disabled(self):
		# Must NOT raise while S3 is disabled.
		doc = self._doc(is_private=1)
		conn = self._conn()
		conn.s3_settings.disable_s3_operations = 1
		with patch.object(s3_core, "getS3Connection", return_value=conn):
			s3_core.handle_is_private_change(doc)
		conn.copy_object_to_bucket.assert_not_called()


class TestServeResilience(FrappeTestCase):
	"""serve_file / _stream_from_s3 must not 500 on the kill switch or a missing object."""

	def test_public_serve_works_when_disabled(self):
		conn = MagicMock()
		conn.s3_settings.disable_s3_operations = 1
		conn.s3_settings.region = "ap-south-1"
		conn.connection = None  # disabled -> no client; must NOT be dereferenced
		fdoc = frappe._dict(name="F1", file_name="x.png", is_private=0,
		                    custom_s3_key="uploads/x.png", custom_s3_bucket_name="pub",
		                    custom_is_s3_uploaded=1)
		with patch.object(s3_core.frappe.db, "get_value", return_value=fdoc), \
		     patch.object(s3_core, "getS3Connection", return_value=conn):
			resp = s3_core.serve_file(file_id="F1")
		self.assertEqual(resp.status_code, 302)
		self.assertIn("pub.s3.dualstack.ap-south-1", resp.headers["Location"])

	def test_stream_raises_503_when_disabled(self):
		from werkzeug.exceptions import ServiceUnavailable
		conn = MagicMock()
		conn.s3_settings.disable_s3_operations = 1
		fdoc = frappe._dict(custom_s3_key="k", custom_s3_bucket_name="prv", file_name="x.png")
		with patch.object(s3_core, "getS3Connection", return_value=conn):
			with self.assertRaises(ServiceUnavailable):
				s3_core._stream_from_s3(fdoc)

	def test_stream_missing_object_raises_notfound(self):
		conn = MagicMock()
		conn.s3_settings.disable_s3_operations = 0
		conn.get_file_from_bucket.side_effect = ClientError({"Error": {"Code": "NoSuchKey"}}, "GetObject")
		fdoc = frappe._dict(custom_s3_key="k", custom_s3_bucket_name="prv", file_name="x.png")
		with patch.object(s3_core, "getS3Connection", return_value=conn):
			with self.assertRaises(frappe.exceptions.NotFound):
				s3_core._stream_from_s3(fdoc)
