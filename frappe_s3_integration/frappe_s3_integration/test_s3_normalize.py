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

	# ---- sibling sync (same file uploaded -> all File docs point to S3) ------------------
	def _run_sibling_sync(self, files, twin, verify=True, disabled=0, dry_run=0, local_here=False):
		conn = MagicMock()
		conn.verify_object.return_value = verify
		with patch(f"{PMOD}.frappe.db.get_table_columns", return_value=["custom_s3_key"]), \
		     patch(f"{PMOD}.frappe.db.get_single_value", return_value=disabled), \
		     patch("frappe_s3_integration.s3_core.getS3Connection", return_value=conn), \
		     patch(f"{PMOD}.frappe.get_all", return_value=files), \
		     patch(f"{PMOD}._migrated_twin", return_value=twin), \
		     patch(f"{PMOD}.get_files_path", return_value="/tmp/x"), \
		     patch(f"{PMOD}.os.path.isfile", return_value=local_here), \
		     patch(f"{PMOD}.os.path.getsize", return_value=5), \
		     patch("frappe_s3_integration.s3_core.get_proxy_url", side_effect=lambda n, fn=None: f"PROXY:{n}"), \
		     patch(f"{PMOD}._repoint_attached_field") as rep, \
		     patch(f"{PMOD}.frappe.db.set_value") as setv, \
		     patch(f"{PMOD}.frappe.db.commit"), \
		     patch(f"{PMOD}.frappe.clear_cache") as cc, \
		     patch(f"{PMOD}.frappe.log_error"):
			norm._sync_s3_siblings(dry_run=dry_run)
		return conn, setv, rep, cc

	def test_sibling_sync_points_straggler_at_twin(self):
		# the 2nd upload of the same file, stuck on a dead local url, is pointed at the twin's
		# S3 object + its attach field repointed (identity-guarded on the straggler's own url).
		f = _file(name="S1", file_url="/private/files/a.png", custom_s3_key="",
		          attached_to_doctype="Employee", attached_to_name="EMP-2", attached_to_field="image")
		f.content_hash = "h1"
		twin = frappe._dict(custom_s3_key="private/files/a.png", custom_s3_bucket_name="bkt")
		conn, setv, rep, cc = self._run_sibling_sync([f], twin)
		setv.assert_called_once_with("File", "S1", {
			"custom_s3_key": "private/files/a.png",
			"custom_s3_bucket_name": "bkt",
			"custom_is_s3_uploaded": 1,
			"file_url": "PROXY:S1",
		}, update_modified=False)
		rep.assert_called_once_with(f, "PROXY:S1", expected="/private/files/a.png")
		cc.assert_called_once()  # refresh caches (a synced Single would otherwise serve stale)

	def test_sibling_sync_hashes_local_bytes_before_matching(self):
		# a hashless straggler with local bytes: hash from its OWN bytes (stored), then match.
		import io, hashlib
		f = _file(name="S1", file_url="/private/files/a.png")
		f.content_hash = ""
		twin = frappe._dict(custom_s3_key="private/files/a.png", custom_s3_bucket_name="bkt")
		conn = MagicMock(); conn.verify_object.return_value = True
		with patch(f"{PMOD}.frappe.db.get_table_columns", return_value=["custom_s3_key"]), \
		     patch(f"{PMOD}.frappe.db.get_single_value", return_value=0), \
		     patch("frappe_s3_integration.s3_core.getS3Connection", return_value=conn), \
		     patch(f"{PMOD}.frappe.get_all", return_value=[f]), \
		     patch(f"{PMOD}._migrated_twin", return_value=twin), \
		     patch(f"{PMOD}.get_files_path", return_value="/tmp/x"), \
		     patch(f"{PMOD}.os.path.isfile", return_value=True), \
		     patch(f"{PMOD}.os.path.getsize", return_value=5), \
		     patch(f"{PMOD}.open", return_value=io.BytesIO(b"BYTES"), create=True), \
		     patch("frappe_s3_integration.s3_core.get_proxy_url", side_effect=lambda n, fn=None: f"PROXY:{n}"), \
		     patch(f"{PMOD}._repoint_attached_field"), \
		     patch(f"{PMOD}.frappe.db.set_value") as setv, \
		     patch(f"{PMOD}.frappe.db.commit"), \
		     patch(f"{PMOD}.frappe.clear_cache"), \
		     patch(f"{PMOD}.frappe.log_error"):
			norm._sync_s3_siblings()
		# the computed hash was persisted, and the twin's object was size-verified (5 bytes)
		setv.assert_any_call("File", "S1", "content_hash", hashlib.md5(b"BYTES").hexdigest(), update_modified=False)
		self.assertEqual(conn.verify_object.call_args.kwargs.get("expected_size"), 5)

	def test_sibling_sync_skips_when_no_twin(self):
		# no migrated twin -> this local copy is the only one; leave it for the normal sweep.
		f = _file(name="S1", file_url="/private/files/a.png")
		conn, setv, rep, cc = self._run_sibling_sync([f], twin=None)
		setv.assert_not_called()
		rep.assert_not_called()

	def test_sibling_sync_skips_when_twin_object_missing(self):
		# DATA SAFETY: never point a doc at an S3 object that isn't actually there.
		f = _file(name="S1", file_url="/private/files/a.png")
		twin = frappe._dict(custom_s3_key="private/files/a.png", custom_s3_bucket_name="bkt")
		conn, setv, rep, cc = self._run_sibling_sync([f], twin, verify=False)
		setv.assert_not_called()

	def test_sibling_sync_skips_non_local_url(self):
		f = _file(name="S1", file_url="https://cdn.example.com/x.png")
		twin = frappe._dict(custom_s3_key="k", custom_s3_bucket_name="b")
		conn, setv, rep, cc = self._run_sibling_sync([f], twin)
		setv.assert_not_called()

	def test_sibling_sync_dry_run_writes_nothing(self):
		f = _file(name="S1", file_url="/private/files/a.png")
		twin = frappe._dict(custom_s3_key="private/files/a.png", custom_s3_bucket_name="bkt")
		conn, setv, rep, cc = self._run_sibling_sync([f], twin, dry_run=1)
		setv.assert_not_called()
		rep.assert_not_called()
		cc.assert_not_called()

	def test_sibling_sync_kill_switch_is_a_noop(self):
		f = _file(name="S1", file_url="/private/files/a.png")
		twin = frappe._dict(custom_s3_key="private/files/a.png", custom_s3_bucket_name="bkt")
		conn, setv, rep, cc = self._run_sibling_sync([f], twin, disabled=1)
		setv.assert_not_called()

	def test_migrated_twin_matches_by_content_hash(self):
		f = _file(name="S1", is_private=1, file_url="/private/files/a.png")
		f.content_hash = "h1"
		hit = [frappe._dict(custom_s3_key="k", custom_s3_bucket_name="b")]
		with patch(f"{PMOD}.frappe.get_all", return_value=hit) as ga:
			twin = norm._migrated_twin(f)
		self.assertEqual(twin.custom_s3_key, "k")
		self.assertEqual(ga.call_args.kwargs["filters"]["content_hash"], "h1")

	def test_migrated_twin_never_matches_by_url_without_hash(self):
		# DATA SAFETY (critical): a shared url is NOT proof of shared content (recycled generic
		# filenames), so a hashless straggler must return NO twin — never a url-based guess.
		f = _file(name="S1", file_url="/private/files/a.png")
		f.content_hash = ""
		with patch(f"{PMOD}.frappe.get_all") as ga:
			twin = norm._migrated_twin(f)
		self.assertIsNone(twin)
		ga.assert_not_called()  # no query at all — url is never trusted as content identity

	# ---- _repoint_attached_field identity guard -----------------------------------------
	def _repoint_field(self, current, expected):
		f = frappe._dict(name="S1", file_name="a.png", attached_to_doctype="Employee",
		                 attached_to_name="EMP-1", attached_to_field="image")
		meta = MagicMock(issingle=False)
		meta.has_field.return_value = True
		with patch(f"{PMOD}.frappe.get_meta", return_value=meta), \
		     patch(f"{PMOD}.frappe.db.exists", return_value=True), \
		     patch(f"{PMOD}.frappe.db.get_value", return_value=current), \
		     patch(f"{PMOD}.frappe.db.set_value") as sv, \
		     patch(f"{PMOD}.frappe.log_error"):
			norm._repoint_attached_field(f, "PROXY", expected=expected)
		return sv

	def test_repoint_expected_guard_blocks_a_different_file(self):
		# the field points at a DIFFERENT file's url -> never clobbered (finding 6).
		sv = self._repoint_field(current="/private/files/OTHER.png", expected="/private/files/a.png")
		sv.assert_not_called()

	def test_repoint_expected_guard_repoints_own_url(self):
		sv = self._repoint_field(current="/private/files/a.png", expected="/private/files/a.png")
		sv.assert_called_once_with("Employee", "EMP-1", "image", "PROXY", update_modified=False)

	def test_enqueue_sibling_sync_queues_long_with_sized_timeout(self):
		with patch(f"{PMOD}._sibling_sync_count", return_value=300), \
		     patch(f"{PMOD}.frappe.conf", {}), \
		     patch(f"{PMOD}.frappe.enqueue") as enq:
			out = norm.enqueue_sibling_sync()
		enq.assert_called_once()
		self.assertEqual(enq.call_args.kwargs["queue"], "long")
		self.assertEqual(enq.call_args.kwargs["timeout"], 300 * norm.SECONDS_PER_FILE)
		self.assertEqual(out["queued"], 300)

	def test_enqueue_sibling_sync_noop_when_nothing(self):
		with patch(f"{PMOD}._sibling_sync_count", return_value=0), \
		     patch(f"{PMOD}.frappe.enqueue") as enq:
			out = norm.enqueue_sibling_sync()
		enq.assert_not_called()
		self.assertEqual(out["queued"], 0)

	# ---- child-table Attach fields (Essdee Bulk Payment.advance_image) ------------------
	def _child_repoint(self, has_child_field=True, rows=("R1", "R2"), dry_run=False):
		from frappe_s3_integration import s3_core
		CORE = "frappe_s3_integration.s3_core"
		parent_meta = MagicMock()
		tf = MagicMock(); tf.options = "Essdee Bulk Payment Entry"
		parent_meta.get_table_fields.return_value = [tf]
		child_meta = MagicMock(); child_meta.has_field.return_value = has_child_field

		def get_meta(dt):
			return parent_meta if dt == "Essdee Bulk Payment" else child_meta
		with patch(f"{CORE}.frappe.get_meta", side_effect=get_meta), \
		     patch(f"{CORE}.frappe.get_all", return_value=list(rows)) as ga, \
		     patch(f"{CORE}.frappe.db.set_value") as sv, \
		     patch(f"{CORE}.frappe.log_error"):
			n = s3_core.child_attach_repoint("Essdee Bulk Payment", "BLK-1", "advance_image",
			                                 "/private/files/x.jpg", "PROXY", dry_run=dry_run)
		return n, ga, sv

	def test_child_repoint_updates_matching_rows(self):
		n, ga, sv = self._child_repoint()
		self.assertEqual(n, 2)
		# identity-scoped query: this parent + the field == this file's own local url
		filters = ga.call_args.kwargs["filters"]
		self.assertEqual(filters["parent"], "BLK-1")
		self.assertEqual(filters["parenttype"], "Essdee Bulk Payment")
		self.assertEqual(filters["advance_image"], "/private/files/x.jpg")
		sv.assert_any_call("Essdee Bulk Payment Entry", "R1", "advance_image", "PROXY", update_modified=False)
		self.assertEqual(sv.call_count, 2)

	def test_child_repoint_dry_run_counts_but_writes_nothing(self):
		n, ga, sv = self._child_repoint(dry_run=True)
		self.assertEqual(n, 2)
		sv.assert_not_called()

	def test_child_repoint_zero_when_field_not_on_child(self):
		n, ga, sv = self._child_repoint(has_child_field=False)
		self.assertEqual(n, 0)
		ga.assert_not_called()   # no child doctype has the field -> no query
		sv.assert_not_called()

	def test_backfill_routes_child_field_to_child_repoint(self):
		# a File whose attached_to_field is a CHILD field -> backfill delegates to child_attach_repoint.
		f = _file(name="F1", custom_s3_key="private/files/x.jpg",
		          attached_to_doctype="Essdee Bulk Payment", attached_to_name="BLK-1",
		          attached_to_field="advance_image")
		meta = MagicMock(issingle=False)
		meta.has_field.return_value = False   # not a PARENT field
		with patch(f"{PMOD}.frappe.db.get_table_columns", return_value=["custom_s3_key"]), \
		     patch(f"{PMOD}.frappe.get_all", return_value=[f]), \
		     patch(f"{PMOD}.frappe.get_meta", return_value=meta), \
		     patch(f"{PMOD}.frappe.db.exists", return_value=True), \
		     patch("frappe_s3_integration.s3_core.get_proxy_url", side_effect=lambda n, fn=None: f"PROXY:{n}"), \
		     patch("frappe_s3_integration.s3_core.child_attach_repoint", return_value=3) as car, \
		     patch(f"{PMOD}.frappe.db.commit"), \
		     patch(f"{PMOD}.frappe.clear_cache"), \
		     patch(f"{PMOD}.frappe.log_error"):
			norm._backfill_attached_fields()
		car.assert_called_once_with("Essdee Bulk Payment", "BLK-1", "advance_image",
		                            "/private/files/x.jpg", "PROXY:F1", dry_run=False)

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

	# ---- orphan forensics (read-only) ---------------------------------------------------
	def test_age_bucket_boundaries(self):
		self.assertEqual(norm._age_bucket(0.5), "<1d")
		self.assertEqual(norm._age_bucket(3), "1-7d")
		self.assertEqual(norm._age_bucket(20), "7-30d")
		self.assertEqual(norm._age_bucket(400), ">365d")

	def test_orphan_pattern_classification(self):
		self.assertEqual(norm._orphan_pattern("Invoice Scan.pdf"), "human-named")   # spaces
		self.assertEqual(norm._orphan_pattern("aabbccddeeff00112233445566.jpeg"), "hash/uuid-named")
		self.assertEqual(norm._orphan_pattern("attachment_20240101_export.pdf"), "long-auto-named")
		self.assertEqual(norm._orphan_pattern("x.png"), "short-named")

	def test_orphan_forensics_categorizes_and_is_read_only(self):
		orphans = [
			("/p/private/files/aabbccddeeff00112233445566.jpeg", 1, 2048),  # hash-named
			("/p/private/files/Invoice Scan.pdf", 1, 4096),                 # human-named
			("/p/public/files/logo_old.png", 0, 512),                      # short-named
		]
		with patch(f"{PMOD}._referenced_basenames", return_value=set()), \
		     patch(f"{PMOD}.frappe.db.count", return_value=100), \
		     patch(f"{PMOD}._iter_orphans", return_value=iter(orphans)), \
		     patch(f"{PMOD}.os.path.getmtime", return_value=0.0), \
		     patch(f"{PMOD}.frappe.get_all", return_value=[]) as ga, \
		     patch(f"{PMOD}.frappe.db.set_value") as setv, \
		     patch(f"{PMOD}.os.remove") as rm:
			out = norm.orphan_forensics(sample=5)
		# categorization
		self.assertEqual(out["orphans"], 3)
		self.assertEqual(out["bytes"], 2048 + 4096 + 512)
		self.assertEqual(out["private"], 2)
		self.assertEqual(out["public"], 1)
		self.assertEqual(out["by_ext"][".jpeg"], 1)
		self.assertEqual(out["by_pattern"]["hash/uuid-named"], 1)
		self.assertEqual(out["by_pattern"]["human-named"], 1)
		self.assertEqual(out["by_pattern"]["short-named"], 1)
		# strictly read-only
		setv.assert_not_called()
		rm.assert_not_called()

	def test_orphan_forensics_flags_surviving_sibling(self):
		# a surviving File doc sharing the stem -> classified as a re-upload/dedup leftover.
		orphans = [("/p/private/files/aabbccddeeff00112233445566.jpeg", 1, 2048)]
		sib = [frappe._dict(name="F9", file_name="aabbccddeeff00112233445566.jpeg",
		                    attached_to_doctype="Purchase Invoice", attached_to_name="PI-1")]
		with patch(f"{PMOD}._referenced_basenames", return_value=set()), \
		     patch(f"{PMOD}.frappe.db.count", return_value=1), \
		     patch(f"{PMOD}._iter_orphans", return_value=iter(orphans)), \
		     patch(f"{PMOD}.os.path.getmtime", return_value=0.0), \
		     patch(f"{PMOD}.frappe.get_all", return_value=sib) as ga:
			out = norm.orphan_forensics(sample=5)
		# the sibling lookup was performed (LIKE on the 24-char stem prefix)
		self.assertTrue(ga.called)
		self.assertEqual(out["orphans"], 1)
