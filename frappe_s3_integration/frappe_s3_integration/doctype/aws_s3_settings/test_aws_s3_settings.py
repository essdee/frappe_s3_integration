# Copyright (c) 2025, sakthi123msd@gmail.com and Contributors
# See license.txt

import base64
import io
import uuid
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

	def test_s3_safe_filename_preserves_name_and_blocks_traversal(self):
		# Issue 1: keep the uploaded name intact, but never allow a path segment.
		f = s3_core._s3_safe_filename
		self.assertEqual(f("invoice.pdf"), "invoice.pdf")               # real name preserved
		self.assertEqual(f("My Report (final).xlsx"), "My Report (final).xlsx")
		self.assertEqual(f("café résumé.pdf"), "café résumé.pdf")       # unicode kept as-is
		self.assertEqual(f("../../etc/passwd"), "passwd")              # traversal stripped
		self.assertEqual(f("/abs/secret.key"), "secret.key")           # basename only
		self.assertNotIn("/", f("../../etc/passwd"))
		self.assertEqual(f(".."), "file")                             # degenerate -> fallback
		self.assertEqual(f(""), "file")
		self.assertNotIn("\x00", f("a\x00b.pdf"))                      # control chars stripped
		self.assertLessEqual(len(f("a" * 300 + ".pdf")), 200)          # length-capped

	def test_s3_key_from_file_url_mirrors_frappe(self):
		# Issue 1: S3 key mirrors Frappe's own on-disk path.
		f = s3_core._s3_key_from_file_url
		self.assertEqual(f("/files/a.png"), "files/a.png")
		self.assertEqual(f("/private/files/a.png"), "private/files/a.png")
		self.assertIsNone(f("/api/method/x.serve_file/a.png?file_id=Y"))  # proxy url, not a local path
		self.assertIsNone(f(""))

	def test_upload_key_mirrors_frappe_layout_when_no_key(self):
		# Direct upload with no explicit key -> Frappe layout: files/<name> (public), flat.
		c = self._conn()
		c.connection.meta.region_name = "ap-south-1"
		c.verify_object = MagicMock(return_value=False)   # no collision
		file = MagicMock(filename="my file.pdf", content_type="application/pdf")
		file.stream = io.BytesIO(b"12345")
		res = c.upload_file_to_bucket(file, bucket_name="b", allow_public=True)
		self.assertEqual(res["key"], "files/my file.pdf")             # flat, Frappe layout, real name
		self.assertIn("files/my%20file.pdf", res["file_url"])         # percent-encoded url

	def test_upload_uses_explicit_frappe_key_when_free(self):
		# The migration sweep passes the file's Frappe path as the key -> used unchanged when
		# the object is free (collision-guarded, but no suffix without a real collision).
		c = self._conn()
		c.connection.meta.region_name = "ap-south-1"
		c.verify_object = MagicMock(return_value=False)   # no collision
		file = MagicMock(filename="x.pdf", content_type="application/pdf")
		file.stream = io.BytesIO(b"12345")
		res = c.upload_file_to_bucket(file, bucket_name="b", allow_public=False, key="private/files/x.pdf")
		self.assertEqual(res["key"], "private/files/x.pdf")

	def test_copy_object_moves_between_frappe_folders(self):
		# Visibility flip mirrors Frappe: files/<name> <-> private/files/<name>, name kept.
		c = self._conn()
		to_private = c.copy_object_to_bucket("pub", "files/invoice.pdf", "prv", "invoice.pdf", make_public=False)
		self.assertEqual(to_private, "private/files/invoice.pdf")
		to_public = c.copy_object_to_bucket("prv", "private/files/invoice.pdf", "pub", "invoice.pdf", make_public=True)
		self.assertEqual(to_public, "files/invoice.pdf")
		self.assertEqual(c.connection.copy_object.call_count, 2)

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
		loc = resp.headers["Location"]
		self.assertIn("s3.ap-south-1.amazonaws.com/pub/uploads/x.png", loc)  # path-style
		self.assertNotIn("pub.s3", loc)  # bucket NOT in the hostname (dotted-bucket safe)

	def test_public_url_is_path_style_for_dotted_bucket(self):
		# Dotted bucket names (hr.essdee.fit.public) MUST use path-style — a virtual-hosted
		# url (<bucket>.s3...) breaks HTTPS and redirects/crashes in the browser.
		url = s3_core._s3_https_url("hr.essdee.fit.public", "files/012++(1).jpg", "ap-south-1")
		self.assertTrue(url.startswith("https://s3.ap-south-1.amazonaws.com/hr.essdee.fit.public/"))
		self.assertNotIn("hr.essdee.fit.public.s3", url)  # never in the hostname

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


class TestS3FileOverride(FrappeTestCase):
	"""File controller override: S3-backed files (serve_file proxy url) skip on-disk validation."""

	PROXY = "/api/method/frappe_s3_integration.s3_core.serve_file/10697.jpg?file_id=abc"

	def test_file_controller_is_overridden(self):
		from frappe_s3_integration.overrides import S3File
		doc = frappe.get_doc({"doctype": "File", "file_name": "x.png", "file_url": "/files/x.png"})
		self.assertIsInstance(doc, S3File)

	def test_validate_file_on_disk_skips_s3_proxy(self):
		from frappe_s3_integration.overrides import S3File
		# The reported failure was inside validate_file_on_disk -> get_full_path ->
		# is_safe_path, raising "Cannot access file path" for the /api/method proxy url.
		# The override must skip it cleanly (return None) instead of raising.
		doc = frappe.get_doc({
			"doctype": "File",
			"file_name": "10697.jpg",
			"file_url": self.PROXY,
			"custom_is_s3_uploaded": 1,
		})
		self.assertIsInstance(doc, S3File)
		self.assertIsNone(doc.validate_file_on_disk())

	def test_local_file_still_validated_by_core(self):
		from frappe_s3_integration.overrides import S3File
		doc = frappe.get_doc({"doctype": "File", "file_name": "nope.png",
		                      "file_url": "/files/nope-does-not-exist.png", "is_private": 0})
		self.assertIsInstance(doc, S3File)
		# not an S3 proxy url -> delegates to core, which rejects a missing local file
		with self.assertRaises(Exception):
			doc.validate_file_on_disk()

	def test_get_content_fetches_s3_backed_file_from_s3(self):
		from frappe_s3_integration.overrides import S3File
		# S3-backed file (proxy url, no local copy): get_content must fetch from S3,
		# not open() the proxy path (which raised FileNotFoundError in Bank Statement Import).
		doc = frappe.get_doc({
			"doctype": "File", "file_name": "Statement.xlsx", "file_url": self.PROXY,
			"custom_is_s3_uploaded": 1, "custom_s3_key": "private/files/Statement.xlsx",
			"custom_s3_bucket_name": "b", "is_private": 1,
		})
		self.assertIsInstance(doc, S3File)
		conn = MagicMock()
		conn.s3_settings.disable_s3_operations = 0
		conn.get_file_from_bucket.return_value = {"Body": MagicMock(read=lambda: b"\x89PNG binary")}
		with patch.object(s3_core, "getS3Connection", return_value=conn):
			content = doc.get_content()
		conn.get_file_from_bucket.assert_called_once_with("private/files/Statement.xlsx", "b")
		self.assertEqual(content, b"\x89PNG binary")  # binary stays bytes

	def test_pdf_body_html_inlines_s3_proxy_image(self):
		# The letterhead / private S3 image bug: frappe can't resolve our proxy url, so
		# images go blank in PDF. The pdf_body_html hook base64-inlines them from S3.
		from frappe_s3_integration.pdf_print import inline_s3_images
		f = MagicMock(file_name="logo.png")
		f.is_downloadable.return_value = True
		f.get_content.return_value = b"PNGDATA"
		html = '<div><img src="/api/method/frappe_s3_integration.s3_core.serve_file/logo.png?file_id=ABC"></div>'
		with patch.object(frappe.db, "exists", return_value=True), \
		     patch.object(frappe, "get_doc", return_value=f):
			out = inline_s3_images(html)
		self.assertIn("data:image/png;base64," + base64.b64encode(b"PNGDATA").decode(), out)
		self.assertNotIn("serve_file", out)  # proxy url replaced

	def test_inlines_migrated_letterhead_local_url(self):
		# The letterhead-blank bug: migrate_file_to_s3 rewrites the File's file_url to the
		# proxy and DELETES the local file, but never rewrites HTML that hardcodes the old
		# path — a Letter Head's content still says <img src="/files/Letter_Head.png">.
		# Resolve that local url to the migrated File by name and inline it from S3.
		from frappe_s3_integration.pdf_print import inline_s3_images
		f = MagicMock(file_name="Letter_Head.png")
		f.get.side_effect = lambda k, d=None: {
			"custom_is_s3_uploaded": 1, "custom_s3_key": "files/Letter_Head.png"}.get(k, d)
		f.is_downloadable.return_value = True
		f.get_content.return_value = b"LHDATA"
		html = '<div><img src="/files/Letter_Head.png"></div>'
		with patch.object(frappe, "get_all", return_value=["FILE-0001"]), \
		     patch.object(frappe, "get_doc", return_value=f):
			out = inline_s3_images(html)
		self.assertIn("data:image/png;base64," + base64.b64encode(b"LHDATA").decode(), out)
		self.assertNotIn("/files/Letter_Head.png", out)  # stale local url replaced

	def test_local_url_left_untouched_when_not_on_s3(self):
		# A /files/ image that was NEVER migrated (no S3-backed File by that name) must be
		# left exactly as-is — local files render fine; don't touch what isn't ours.
		from frappe_s3_integration.pdf_print import inline_s3_images
		html = '<div><img src="/files/plain_local.png"></div>'
		with patch.object(frappe, "get_all", return_value=[]):
			self.assertEqual(inline_s3_images(html), html)

	def test_pdf_body_html_leaves_non_s3_html_untouched(self):
		from frappe_s3_integration.pdf_print import inline_s3_images
		html = '<div><img src="https://cdn.example.com/logo.png"></div>'
		self.assertEqual(inline_s3_images(html), html)  # fast path, no change

	def test_render_letterhead_override_inlines_s3_header(self):
		# Reports (General Ledger etc.): the S3 letterhead was blank because reports don't
		# use pdf_body_html. The render_letterhead_for_print override base64s its images.
		from frappe_s3_integration import pdf_print
		import frappe.utils.print_format as pf
		proxy = '<img src="/api/method/frappe_s3_integration.s3_core.serve_file/lh.png?file_id=LH">'
		f = MagicMock(file_name="lh.png")
		f.is_downloadable.return_value = True
		f.get_content.return_value = b"LHDATA"
		with patch.object(pf, "render_letterhead_for_print", return_value={"header": proxy, "footer": ""}), \
		     patch.object(frappe.db, "exists", return_value=True), \
		     patch.object(frappe, "get_doc", return_value=f):
			out = pdf_print.render_letterhead_for_print(letterhead="X")
		self.assertIn("data:image/png;base64," + base64.b64encode(b"LHDATA").decode(), out["header"])
		self.assertNotIn("serve_file", out["header"])  # proxy url replaced in the letterhead

	def test_get_content_decodes_text_and_delegates_for_local(self):
		from frappe_s3_integration.overrides import S3File
		# text content from S3 is decoded to str (mirrors core)
		doc = frappe.get_doc({
			"doctype": "File", "file_name": "a.csv", "file_url": self.PROXY,
			"custom_is_s3_uploaded": 1, "custom_s3_key": "files/a.csv",
			"custom_s3_bucket_name": "b", "is_private": 0,
		})
		conn = MagicMock()
		conn.s3_settings.disable_s3_operations = 0
		conn.get_file_from_bucket.return_value = {"Body": MagicMock(read=lambda: b"a,b,c")}
		with patch.object(s3_core, "getS3Connection", return_value=conn):
			self.assertEqual(doc.get_content(), "a,b,c")
		# a non-S3 File must NOT touch S3 — delegates to core
		local = frappe.get_doc({"doctype": "File", "file_name": "x", "content": "hello"})
		self.assertIsInstance(local, S3File)
		self.assertEqual(local.get_content(), "hello")
