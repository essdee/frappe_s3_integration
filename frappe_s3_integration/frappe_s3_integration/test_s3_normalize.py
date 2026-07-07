# Copyright (c) 2026, sakthi123msd@gmail.com and Contributors
# See license.txt
"""Data-safety + behaviour tests for frappe_s3_integration.s3_normalize."""

from unittest.mock import MagicMock, patch

import frappe
from frappe.tests.utils import FrappeTestCase

from frappe_s3_integration import s3_normalize as norm

PMOD = "frappe_s3_integration.s3_normalize"


def _file(**kw):
	d = dict(name="F1", file_name="a.jpg", is_private=1, custom_s3_key="uploads/uuid1/a.jpg",
	         custom_s3_bucket_name="bkt", attached_to_doctype=None, attached_to_name=None,
	         attached_to_field=None)
	d.update(kw)
	return frappe._dict(d)


class TestS3Normalize(FrappeTestCase):
	def _run(self, files, conn, local_exists=False, local_size=5, disabled=0, dry_run=0):
		conn._unique_key = MagicMock(side_effect=lambda b, k: k)  # no collision suffix
		with patch(f"{PMOD}.frappe.db.get_table_columns", return_value=["custom_s3_key"]), \
		     patch(f"{PMOD}.frappe.db.get_single_value", return_value=disabled), \
		     patch("frappe_s3_integration.s3_core.getS3Connection", return_value=conn), \
		     patch("frappe_s3_integration.s3_core.get_proxy_url",
		           side_effect=lambda n, fn=None: f"/api/method/frappe_s3_integration.s3_core.serve_file/{fn}?file_id={n}"), \
		     patch(f"{PMOD}.frappe.get_all", return_value=files), \
		     patch(f"{PMOD}.frappe.db.set_value") as setv, \
		     patch(f"{PMOD}.frappe.db.commit"), \
		     patch(f"{PMOD}.frappe.log_error"), \
		     patch(f"{PMOD}.get_files_path", return_value="/tmp/local/a.jpg"), \
		     patch(f"{PMOD}.os.path.exists", return_value=local_exists), \
		     patch(f"{PMOD}.os.path.getsize", return_value=local_size), \
		     patch(f"{PMOD}.os.remove") as rm:
			norm._normalize(dry_run=dry_run)
		return setv, rm

	# ---- re-key correctness ------------------------------------------------------------
	def test_rekeys_object_outside_files_folder(self):
		conn = MagicMock()
		conn.verify_object.return_value = True
		setv, rm = self._run([_file(is_private=1)], conn)
		args = conn.connection.copy_object.call_args.kwargs
		self.assertEqual(args["Key"], "private/files/a.jpg")
		self.assertEqual(args["CopySource"], {"Bucket": "bkt", "Key": "uploads/uuid1/a.jpg"})
		self.assertEqual(setv.call_args.args[2]["custom_s3_key"], "private/files/a.jpg")
		conn.delete_file_from_bucket.assert_called_once_with("uploads/uuid1/a.jpg", "bkt")

	def test_public_file_goes_to_files_prefix(self):
		conn = MagicMock()
		conn.verify_object.return_value = True
		setv, rm = self._run([_file(is_private=0, custom_s3_key="uploads/u/x.png", file_name="x.png")], conn)
		self.assertEqual(conn.connection.copy_object.call_args.kwargs["Key"], "files/x.png")

	# ---- production-safety fixes -------------------------------------------------------
	def test_public_copy_reapplies_public_read_acl(self):
		# COPY drops the ACL -> a copied public object would 403 on the serve_file redirect.
		conn = MagicMock()
		conn.verify_object.return_value = True
		setv, rm = self._run([_file(is_private=0, custom_s3_key="uploads/u/x.png", file_name="x.png")], conn)
		self.assertEqual(conn.connection.copy_object.call_args.kwargs.get("ACL"), "public-read")

	def test_private_copy_has_no_public_acl(self):
		conn = MagicMock()
		conn.verify_object.return_value = True
		setv, rm = self._run([_file(is_private=1)], conn)
		self.assertNotIn("ACL", conn.connection.copy_object.call_args.kwargs)

	def test_shared_blob_repoints_all_siblings_before_deleting_old(self):
		# Two File docs share one S3 object (dedup). Re-key must repoint BOTH to the new key
		# and delete the old object exactly once — never strand the sibling.
		conn = MagicMock()
		conn.verify_object.return_value = True
		a = _file(name="F1", file_name="a.jpg", custom_s3_key="uploads/uuid1/a.jpg")
		b = _file(name="F2", file_name="a.jpg", custom_s3_key="uploads/uuid1/a.jpg")
		setv, rm = self._run([a, b], conn)
		conn.connection.copy_object.assert_called_once()
		conn.delete_file_from_bucket.assert_called_once_with("uploads/uuid1/a.jpg", "bkt")
		repointed = {c.args[1]: c.args[2]["custom_s3_key"] for c in setv.call_args_list}
		self.assertEqual(repointed, {"F1": "private/files/a.jpg", "F2": "private/files/a.jpg"})

	# ---- never-in-neither-place --------------------------------------------------------
	def test_copy_verify_failure_keeps_old_and_skips_update(self):
		conn = MagicMock()
		conn.verify_object.side_effect = [True, False]  # source ok, copy NOT verified
		setv, rm = self._run([_file()], conn)
		setv.assert_not_called()
		conn.delete_file_from_bucket.assert_not_called()

	def test_source_missing_is_skipped_safely(self):
		conn = MagicMock()
		conn.verify_object.side_effect = [False]  # source doesn't exist
		setv, rm = self._run([_file()], conn)
		conn.connection.copy_object.assert_not_called()
		setv.assert_not_called()
		conn.delete_file_from_bucket.assert_not_called()

	def test_correctly_keyed_file_is_left_completely_untouched(self):
		# "only those like that": a key already under files/ is skipped entirely.
		conn = MagicMock()
		conn.verify_object.return_value = True
		setv, rm = self._run([_file(custom_s3_key="private/files/a.jpg")], conn, local_exists=True)
		conn.connection.copy_object.assert_not_called()
		conn.delete_file_from_bucket.assert_not_called()
		setv.assert_not_called()
		rm.assert_not_called()

	def test_local_removed_only_when_s3_verified_with_matching_size(self):
		conn = MagicMock()
		conn.verify_object.return_value = True  # source ok, copy ok, size matches
		setv, rm = self._run([_file(custom_s3_key="uploads/uuid1/a.jpg")], conn, local_exists=True, local_size=5)
		rm.assert_called_once_with("/tmp/local/a.jpg")

	def test_local_kept_when_s3_size_mismatch_after_rekey(self):
		conn = MagicMock()
		conn.verify_object.side_effect = [True, True, False]  # source, copy, size-check
		setv, rm = self._run([_file(custom_s3_key="uploads/uuid1/a.jpg")], conn, local_exists=True, local_size=99)
		rm.assert_not_called()  # DATA SAFETY: never delete local without a verified S3 copy

	# ---- dry run + disabled ------------------------------------------------------------
	def test_dry_run_touches_nothing(self):
		conn = MagicMock()
		conn.verify_object.return_value = True
		setv, rm = self._run([_file()], conn, local_exists=True, dry_run=1)
		conn.connection.copy_object.assert_not_called()
		conn.delete_file_from_bucket.assert_not_called()
		setv.assert_not_called()
		rm.assert_not_called()

	def test_disabled_s3_is_a_noop(self):
		conn = MagicMock()
		setv, rm = self._run([_file()], conn, disabled=1)
		conn.connection.copy_object.assert_not_called()
		rm.assert_not_called()

	# ---- dynamic timeout + enqueue -----------------------------------------------------
	def test_timeout_scales_with_count(self):
		with patch(f"{PMOD}.frappe.conf", {}):  # no site_config overrides
			self.assertEqual(norm._timeout_for(0), norm.TIMEOUT_FLOOR)        # floor
			self.assertEqual(norm._timeout_for(10 ** 9), norm.TIMEOUT_CAP)    # cap
			self.assertEqual(norm._timeout_for(1000), 1000 * norm.SECONDS_PER_FILE)

	def test_enqueue_queues_long_with_sized_timeout(self):
		with patch(f"{PMOD}._miskeyed_count", return_value=1000), \
		     patch(f"{PMOD}.frappe.conf", {}), \
		     patch(f"{PMOD}.frappe.enqueue") as enq:
			out = norm.enqueue_normalization()
		enq.assert_called_once()
		self.assertEqual(enq.call_args.kwargs["queue"], "long")
		self.assertEqual(enq.call_args.kwargs["timeout"], 1000 * norm.SECONDS_PER_FILE)
		self.assertEqual(out["queued"], 1000)

	def test_enqueue_noop_when_nothing_to_do(self):
		with patch(f"{PMOD}._miskeyed_count", return_value=0), \
		     patch(f"{PMOD}.frappe.enqueue") as enq:
			out = norm.enqueue_normalization()
		enq.assert_not_called()
		self.assertEqual(out["queued"], 0)
