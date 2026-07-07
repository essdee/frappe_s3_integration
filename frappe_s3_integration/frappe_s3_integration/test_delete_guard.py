# Copyright (c) 2026, sakthi123msd@gmail.com and Contributors
# See license.txt
"""Invariant 3: deleting a File doc removes the S3 object ONLY when no other File doc
references the same object (dedup'd siblings share one blob)."""

from unittest.mock import MagicMock, patch

import frappe
from frappe.tests.utils import FrappeTestCase

from frappe_s3_integration import s3_core

PKG = "frappe_s3_integration.s3_core"


def _doc(**kw):
	d = dict(name="F1", custom_is_s3_uploaded=1, custom_s3_key="files/a.png",
	         custom_s3_bucket_name="bkt")
	d.update(kw)
	return frappe._dict(d)


class TestDeleteGuard(FrappeTestCase):
	def _run(self, doc, other_refs=0, disabled=0, delete_res=None):
		conn = MagicMock()
		conn.s3_settings.disable_s3_operations = disabled
		conn.delete_file_from_bucket.return_value = delete_res
		with patch(f"{PKG}.getS3Connection", return_value=conn) as gc, \
		     patch(f"{PKG}.frappe.db.count", return_value=other_refs) as cnt:
			s3_core.delete_file_from_s3(doc, "on_trash")
		return conn, gc, cnt

	def test_last_reference_deletes_s3_object(self):
		conn, gc, cnt = self._run(_doc(), other_refs=0)
		conn.delete_file_from_bucket.assert_called_once_with("files/a.png", "bkt")
		# the sibling count must exclude THIS doc and match on key + bucket
		filters = cnt.call_args.kwargs["filters"]
		self.assertEqual(filters["custom_s3_key"], "files/a.png")
		self.assertEqual(filters["custom_s3_bucket_name"], "bkt")
		self.assertEqual(filters["name"], ["!=", "F1"])

	def test_shared_object_is_kept_when_siblings_remain(self):
		# dedup: another File doc still points at the same key -> S3 object must survive.
		conn, gc, cnt = self._run(_doc(), other_refs=2)
		conn.delete_file_from_bucket.assert_not_called()
		gc.assert_not_called()  # doesn't even need an S3 connection

	def test_non_s3_file_never_touches_s3(self):
		# a plain local File doc must delete fine even if S3 is broken/unconfigured.
		with patch(f"{PKG}.getS3Connection", side_effect=Exception("no s3 configured")) as gc:
			s3_core.delete_file_from_s3(_doc(custom_is_s3_uploaded=0), "on_trash")
		gc.assert_not_called()

	def test_missing_key_is_a_noop(self):
		with patch(f"{PKG}.getS3Connection") as gc:
			s3_core.delete_file_from_s3(_doc(custom_s3_key=""), "on_trash")
		gc.assert_not_called()

	def test_kill_switch_blocks_only_the_real_s3_delete(self):
		# disable_s3_operations: the LAST reference can't remove its object -> throw
		# (never strand an unreachable S3 object silently)...
		with self.assertRaises(frappe.ValidationError):
			self._run(_doc(), other_refs=0, disabled=1)
		# ...but deleting a SHARED sibling doesn't touch S3, so it must still work.
		conn, gc, cnt = self._run(_doc(), other_refs=1, disabled=1)
		conn.delete_file_from_bucket.assert_not_called()

	def test_s3_delete_error_raises(self):
		with self.assertRaises(frappe.ValidationError):
			self._run(_doc(), other_refs=0, delete_res="ERR-LOG-001")
