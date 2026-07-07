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

	# ---- local-copy cleanup sweep ------------------------------------------------------
	def _run_cleanup(self, files, conn, local_exists=False, local_size=5, disabled=0, dry_run=0):
		with patch(f"{PMOD}.frappe.db.get_table_columns", return_value=["custom_s3_key"]), \
		     patch(f"{PMOD}.frappe.db.get_single_value", return_value=disabled), \
		     patch("frappe_s3_integration.s3_core.getS3Connection", return_value=conn), \
		     patch(f"{PMOD}.frappe.get_all", return_value=files), \
		     patch(f"{PMOD}.frappe.log_error"), \
		     patch(f"{PMOD}.get_files_path", return_value="/tmp/local/a.jpg"), \
		     patch(f"{PMOD}.os.path.exists", return_value=local_exists), \
		     patch(f"{PMOD}.os.path.getsize", return_value=local_size), \
		     patch(f"{PMOD}.os.remove") as rm:
			norm._cleanup_local(dry_run=dry_run)
		return rm

	def test_local_cleanup_removes_verified_local(self):
		conn = MagicMock()
		conn.verify_object.return_value = True  # S3 present + size matches
		rm = self._run_cleanup([_file(custom_s3_key="files/a.jpg", is_private=0)], conn, local_exists=True, local_size=5)
		rm.assert_called_once_with("/tmp/local/a.jpg")

	def test_local_cleanup_keeps_on_size_mismatch(self):
		conn = MagicMock()
		conn.verify_object.return_value = False  # S3 missing / size mismatch
		rm = self._run_cleanup([_file(custom_s3_key="files/a.jpg")], conn, local_exists=True, local_size=99)
		rm.assert_not_called()  # DATA SAFETY: never delete the only copy

	def test_local_cleanup_dry_run_touches_nothing(self):
		conn = MagicMock()
		conn.verify_object.return_value = True
		rm = self._run_cleanup([_file(custom_s3_key="files/a.jpg")], conn, local_exists=True, dry_run=1)
		rm.assert_not_called()

	def test_local_cleanup_no_local_is_a_noop(self):
		conn = MagicMock()
		conn.verify_object.return_value = True
		rm = self._run_cleanup([_file(custom_s3_key="files/a.jpg")], conn, local_exists=False)
		rm.assert_not_called()
		conn.verify_object.assert_not_called()  # no S3 call when there's no local file

	def test_enqueue_local_cleanup_queues_long_with_sized_timeout(self):
		with patch(f"{PMOD}._s3_backed_count", return_value=500), \
		     patch(f"{PMOD}.frappe.conf", {}), \
		     patch(f"{PMOD}.frappe.enqueue") as enq:
			out = norm.enqueue_local_cleanup()
		enq.assert_called_once()
		self.assertEqual(enq.call_args.kwargs["queue"], "long")
		self.assertEqual(enq.call_args.kwargs["timeout"], 500 * norm.SECONDS_PER_FILE)
		self.assertEqual(out["queued"], 500)

	# ---- attach-field backfill (invariant 2 for already-migrated data) ------------------
	def _run_backfill(self, files, meta_singles, current, has_field=True, exists=True, dry_run=0):
		"""meta_singles: set of doctypes that are Single. current: the attach field's value."""
		def get_meta(dt):
			m = MagicMock(issingle=dt in meta_singles)
			m.has_field.return_value = has_field
			return m
		with patch(f"{PMOD}.frappe.db.get_table_columns", return_value=["custom_s3_key"]), \
		     patch(f"{PMOD}.frappe.get_all", return_value=files), \
		     patch(f"{PMOD}.frappe.get_meta", side_effect=get_meta), \
		     patch(f"{PMOD}.frappe.db.exists", return_value=exists), \
		     patch(f"{PMOD}._current_attach_value", return_value=current), \
		     patch("frappe_s3_integration.s3_core.get_proxy_url", side_effect=lambda n, fn=None: f"PROXY:{n}"), \
		     patch(f"{PMOD}.frappe.db.set_single_value") as ssv, \
		     patch(f"{PMOD}.frappe.db.set_value") as sv, \
		     patch(f"{PMOD}.frappe.db.commit"), \
		     patch(f"{PMOD}.frappe.clear_cache"), \
		     patch(f"{PMOD}.frappe.log_error"):
			norm._backfill_attached_fields(dry_run=dry_run)
		return ssv, sv

	def test_backfill_single_uses_set_single_value(self):
		# key 'files/logo.png' -> identity url '/files/logo.png' == the field's stale value.
		f = _file(name="F1", custom_s3_key="files/logo.png",
		          attached_to_doctype="Website Settings",
		          attached_to_name="Website Settings", attached_to_field="app_logo")
		ssv, sv = self._run_backfill([f], {"Website Settings"}, current="/files/logo.png")
		ssv.assert_called_once_with("Website Settings", "app_logo", "PROXY:F1", update_modified=False)
		sv.assert_not_called()

	def test_backfill_dedup_sibling_uses_set_value(self):
		# the 2nd shared-blob File: its own Employee.image still holds ITS local url -> repointed.
		f = _file(name="F2", custom_s3_key="files/img.png",
		          attached_to_doctype="Employee", attached_to_name="EMP-2", attached_to_field="image")
		ssv, sv = self._run_backfill([f], set(), current="/files/img.png")
		sv.assert_called_once_with("Employee", "EMP-2", "image", "PROXY:F2", update_modified=False)
		ssv.assert_not_called()

	def test_backfill_private_files_url_is_repointed(self):
		f = _file(name="F1", custom_s3_key="private/files/x.png",
		          attached_to_doctype="Employee", attached_to_name="EMP-1", attached_to_field="image")
		ssv, sv = self._run_backfill([f], set(), current="/private/files/x.png")
		sv.assert_called_once()

	def test_backfill_already_proxied_is_left_untouched(self):
		# idempotent: a value the migration already fixed is not clobbered.
		f = _file(name="F1", custom_s3_key="files/x.png",
		          attached_to_doctype="Employee", attached_to_name="EMP-1", attached_to_field="image")
		ssv, sv = self._run_backfill([f], set(),
			current="/api/method/frappe_s3_integration.s3_core.serve_file/x?file_id=F1")
		sv.assert_not_called()
		ssv.assert_not_called()

	def test_backfill_external_url_is_left_untouched(self):
		f = _file(name="F1", custom_s3_key="files/x.png",
		          attached_to_doctype="Employee", attached_to_name="EMP-1", attached_to_field="image")
		ssv, sv = self._run_backfill([f], set(), current="https://cdn.example.com/x.png")
		sv.assert_not_called()
		ssv.assert_not_called()

	def test_backfill_wrong_file_is_not_repointed(self):
		# DATA SAFETY: the field holds ANOTHER (newer) file's local url -> identity mismatch,
		# so we never downgrade the record to this older file. (finding #1)
		f = _file(name="F1", custom_s3_key="files/mine.png",
		          attached_to_doctype="Employee", attached_to_name="EMP-1", attached_to_field="image")
		ssv, sv = self._run_backfill([f], set(), current="/files/someone_elses.png")
		sv.assert_not_called()
		ssv.assert_not_called()

	def test_backfill_miskeyed_is_skipped(self):
		# a mis-keyed object (not files/ layout) cannot be identity-matched -> skipped
		# (normalize re-keys those first).
		f = _file(name="F1", custom_s3_key="uploads/uuid/x.png",
		          attached_to_doctype="Employee", attached_to_name="EMP-1", attached_to_field="image")
		ssv, sv = self._run_backfill([f], set(), current="/files/x.png")
		sv.assert_not_called()
		ssv.assert_not_called()

	def test_backfill_dry_run_touches_nothing(self):
		f = _file(name="F1", custom_s3_key="files/img.png",
		          attached_to_doctype="Employee", attached_to_name="EMP-1", attached_to_field="image")
		ssv, sv = self._run_backfill([f], set(), current="/files/img.png", dry_run=1)
		sv.assert_not_called()
		ssv.assert_not_called()

	def test_backfill_regular_missing_row_skipped(self):
		f = _file(name="F1", custom_s3_key="files/img.png",
		          attached_to_doctype="Employee", attached_to_name="GONE", attached_to_field="image")
		ssv, sv = self._run_backfill([f], set(), current="/files/img.png", exists=False)
		sv.assert_not_called()
		ssv.assert_not_called()

	def test_backfill_missing_field_skipped(self):
		f = _file(name="F1", custom_s3_key="files/img.png",
		          attached_to_doctype="Employee", attached_to_name="EMP-1", attached_to_field="ghost")
		ssv, sv = self._run_backfill([f], set(), current="/files/img.png", has_field=False)
		sv.assert_not_called()
		ssv.assert_not_called()

	def test_backfill_error_isolation_continues_past_failure(self):
		# one File blows up mid-loop -> it is logged + counted, the rest still get repointed.
		f_bad = _file(name="BAD", custom_s3_key="files/a.png",
		              attached_to_doctype="BoomType", attached_to_name="X", attached_to_field="image")
		f_ok = _file(name="F2", custom_s3_key="files/b.png",
		             attached_to_doctype="Employee", attached_to_name="EMP-2", attached_to_field="image")

		def get_meta(dt):
			if dt == "BoomType":
				raise Exception("boom")
			m = MagicMock(issingle=False)
			m.has_field.return_value = True
			return m

		with patch(f"{PMOD}.frappe.db.get_table_columns", return_value=["custom_s3_key"]), \
		     patch(f"{PMOD}.frappe.get_all", return_value=[f_bad, f_ok]), \
		     patch(f"{PMOD}.frappe.get_meta", side_effect=get_meta), \
		     patch(f"{PMOD}.frappe.db.exists", return_value=True), \
		     patch(f"{PMOD}._current_attach_value", return_value="/files/b.png"), \
		     patch("frappe_s3_integration.s3_core.get_proxy_url", side_effect=lambda n, fn=None: f"PROXY:{n}"), \
		     patch(f"{PMOD}.frappe.db.set_value") as sv, \
		     patch(f"{PMOD}.frappe.db.commit"), \
		     patch(f"{PMOD}.frappe.clear_cache"), \
		     patch(f"{PMOD}.frappe.log_error") as le:
			norm._backfill_attached_fields()
		sv.assert_called_once_with("Employee", "EMP-2", "image", "PROXY:F2", update_modified=False)
		le.assert_called()  # the boom was logged, not swallowed silently

	def test_enqueue_attach_backfill_queues_long_with_sized_timeout(self):
		with patch(f"{PMOD}._attach_backfill_count", return_value=250), \
		     patch(f"{PMOD}.frappe.conf", {}), \
		     patch(f"{PMOD}.frappe.enqueue") as enq:
			out = norm.enqueue_attach_backfill()
		enq.assert_called_once()
		self.assertEqual(enq.call_args.kwargs["queue"], "long")
		self.assertEqual(enq.call_args.kwargs["timeout"], 250 * norm.SECONDS_PER_FILE)
		self.assertEqual(out["queued"], 250)

	def test_enqueue_attach_backfill_noop_when_nothing(self):
		with patch(f"{PMOD}._attach_backfill_count", return_value=0), \
		     patch(f"{PMOD}.frappe.enqueue") as enq:
			out = norm.enqueue_attach_backfill()
		enq.assert_not_called()
		self.assertEqual(out["queued"], 0)

	# ---- content_hash backfill (invariant 4) --------------------------------------------
	def _run_hash_backfill(self, files, conn, local_exists=False, local_bytes=b"LOCAL",
	                       disabled=0, dry_run=0):
		import io
		with patch(f"{PMOD}.frappe.db.get_table_columns", return_value=["custom_s3_key"]), \
		     patch(f"{PMOD}.frappe.db.get_single_value", return_value=disabled), \
		     patch("frappe_s3_integration.s3_core.getS3Connection", return_value=conn), \
		     patch(f"{PMOD}.frappe.get_all", return_value=files), \
		     patch(f"{PMOD}.get_files_path", return_value="/tmp/local/a.jpg"), \
		     patch(f"{PMOD}.os.path.isfile", return_value=local_exists), \
		     patch(f"{PMOD}.os.path.getsize", return_value=len(local_bytes)), \
		     patch(f"{PMOD}.open", return_value=io.BytesIO(local_bytes), create=True), \
		     patch(f"{PMOD}.frappe.db.set_value") as setv, \
		     patch(f"{PMOD}.frappe.db.commit"), \
		     patch(f"{PMOD}.frappe.log_error") as le:
			norm._backfill_content_hashes(dry_run=dry_run)
		return setv, le

	def test_hash_backfill_prefers_local_copy_when_size_matches_s3(self):
		import hashlib
		conn = MagicMock()
		conn.verify_object.return_value = True  # S3 size == local size
		setv, le = self._run_hash_backfill([_file()], conn, local_exists=True, local_bytes=b"LOCAL")
		conn.get_file_from_bucket.assert_not_called()  # no S3 download needed
		setv.assert_called_once_with("File", "F1", "content_hash",
			hashlib.md5(b"LOCAL").hexdigest(), update_modified=False)

	def test_hash_backfill_distrusts_local_on_size_mismatch(self):
		# DATA SAFETY: a stale/replaced local leftover must not poison the hash — when its
		# size differs from the S3 object, hash the S3 bytes (what the File actually serves).
		import hashlib
		import io
		conn = MagicMock()
		conn.verify_object.return_value = False  # local size != S3 size
		conn.get_file_from_bucket.return_value = {"Body": io.BytesIO(b"S3TRUTH")}
		setv, le = self._run_hash_backfill([_file()], conn, local_exists=True, local_bytes=b"STALE!")
		setv.assert_called_once_with("File", "F1", "content_hash",
			hashlib.md5(b"S3TRUTH").hexdigest(), update_modified=False)

	def test_hash_backfill_streams_s3_when_no_local(self):
		import hashlib
		import io
		conn = MagicMock()
		conn.get_file_from_bucket.return_value = {"Body": io.BytesIO(b"S3BYTES")}
		setv, le = self._run_hash_backfill([_file()], conn, local_exists=False)
		conn.get_file_from_bucket.assert_called_once_with("uploads/uuid1/a.jpg", "bkt")
		setv.assert_called_once_with("File", "F1", "content_hash",
			hashlib.md5(b"S3BYTES").hexdigest(), update_modified=False)

	def test_hash_backfill_dry_run_writes_and_downloads_nothing(self):
		conn = MagicMock()
		setv, le = self._run_hash_backfill([_file()], conn, dry_run=1)
		setv.assert_not_called()
		conn.get_file_from_bucket.assert_not_called()  # dry run must not download objects
		conn.verify_object.assert_not_called()

	def test_hash_backfill_kill_switch_is_a_noop(self):
		conn = MagicMock()
		setv, le = self._run_hash_backfill([_file()], conn, disabled=1)
		setv.assert_not_called()
		conn.get_file_from_bucket.assert_not_called()

	def test_hash_backfill_timeout_scales_with_bytes(self):
		# streaming-hash is bandwidth-bound: a big-bytes backlog must beat the count-based
		# floor (e.g. 2 GB at 0.5 MB/s * 2x buffer ~ 16384s >> 600s floor).
		rows = [frappe._dict(file_size=1024 ** 3), frappe._dict(file_size=1024 ** 3)]
		with patch(f"{PMOD}.frappe.get_all", return_value=rows), \
		     patch(f"{PMOD}.frappe.conf", {}):
			t = norm._hash_backfill_timeout(2)
		self.assertGreater(t, norm.TIMEOUT_FLOOR)
		self.assertLessEqual(t, norm.TIMEOUT_CAP)

	def test_hash_backfill_error_isolation(self):
		# first file's S3 stream blows up -> logged, second still hashed.
		import hashlib
		import io
		conn = MagicMock()
		conn.get_file_from_bucket.side_effect = [Exception("boom"), {"Body": io.BytesIO(b"OK")}]
		f1 = _file(name="F1")
		f2 = _file(name="F2", custom_s3_key="uploads/uuid2/b.jpg")
		setv, le = self._run_hash_backfill([f1, f2], conn)
		setv.assert_called_once_with("File", "F2", "content_hash",
			hashlib.md5(b"OK").hexdigest(), update_modified=False)
		le.assert_called()

	def test_enqueue_hash_backfill_queues_long_with_sized_timeout(self):
		with patch(f"{PMOD}._hashless_count", return_value=400), \
		     patch(f"{PMOD}._hash_backfill_timeout", return_value=1234) as hbt, \
		     patch(f"{PMOD}.frappe.enqueue") as enq:
			out = norm.enqueue_hash_backfill()
		enq.assert_called_once()
		hbt.assert_called_once_with(400)
		self.assertEqual(enq.call_args.kwargs["queue"], "long")
		self.assertEqual(enq.call_args.kwargs["timeout"], 1234)  # the bytes-aware sizing
		self.assertEqual(out["queued"], 400)

	def test_enqueue_hash_backfill_noop_when_nothing(self):
		with patch(f"{PMOD}._hashless_count", return_value=0), \
		     patch(f"{PMOD}.frappe.enqueue") as enq:
			out = norm.enqueue_hash_backfill()
		enq.assert_not_called()
		self.assertEqual(out["queued"], 0)
