# Copyright (c) 2026, sakthi123msd@gmail.com and Contributors
# See license.txt

import io
from unittest.mock import MagicMock, patch

import frappe
from frappe.tests.utils import FrappeTestCase

from frappe_s3_integration.frappe_s3_integration import process_scheduler as ps

PKG = "frappe_s3_integration.frappe_s3_integration.process_scheduler"


def _file(**kw):
	d = dict(custom_s3_key="", is_private=1, file_name="a.png",
	         file_url="/private/files/a.png", name="F1", content_hash="h1",
	         attached_to_doctype=None, attached_to_name=None, attached_to_field=None)
	d.update(kw)
	m = MagicMock()
	m.get.side_effect = d.get
	for k, v in d.items():
		setattr(m, k, v)
	return m


class TestMigrateSafety(FrappeTestCase):
	def _run(self, conn, file, **over):
		cfg = dict(local_path="/tmp/x", exists=True, getsize=5,
		           migrated_sibling=None, other_unmigrated=False)
		cfg.update(over)
		with patch(f"{PKG}.frappe.get_doc", return_value=file), \
		     patch(f"{PKG}._local_path", return_value=cfg["local_path"]), \
		     patch(f"{PKG}.os.path.exists", return_value=cfg["exists"]), \
		     patch(f"{PKG}.os.path.getsize", return_value=cfg["getsize"]), \
		     patch(f"{PKG}.open", return_value=io.BytesIO(b"12345")), \
		     patch(f"{PKG}._migrated_sibling", return_value=cfg["migrated_sibling"]), \
		     patch(f"{PKG}._other_unmigrated_share", return_value=cfg["other_unmigrated"]), \
		     patch(f"{PKG}._point_doc_at_s3") as point, \
		     patch(f"{PKG}.frappe.db"), \
		     patch(f"{PKG}.frappe.get_meta"), \
		     patch(f"{PKG}.frappe.log_error"), \
		     patch(f"{PKG}.os.remove") as rm:
			raised = None
			try:
				ps.migrate_file_to_s3("F1", conn)
			except Exception as e:  # noqa: BLE001
				raised = e
			return rm, point, raised

	def test_no_delete_when_verify_fails(self):
		# M2: must actually REACH verify, then refuse to delete / commit the pointer.
		conn = MagicMock()
		conn.upload_file_to_private_bucket.return_value = {"key": "k", "bucket_name": "b", "content_hash": "h1"}
		conn.verify_object.return_value = False
		rm, point, raised = self._run(conn, _file())
		self.assertIsNotNone(raised)
		conn.verify_object.assert_called_once()
		point.assert_not_called()
		rm.assert_not_called()

	def test_require_content_hash_before_verify(self):
		# M8: no content hash -> raise BEFORE verify, never delete.
		conn = MagicMock()
		conn.upload_file_to_private_bucket.return_value = {"key": "k", "bucket_name": "b"}
		rm, point, raised = self._run(conn, _file())
		self.assertIsNotNone(raised)
		conn.verify_object.assert_not_called()
		rm.assert_not_called()

	def test_shared_blob_not_deleted_while_sibling_unmigrated(self):
		# M1: another File still needs the local blob -> keep it.
		conn = MagicMock()
		conn.upload_file_to_private_bucket.return_value = {"key": "k", "bucket_name": "b", "content_hash": "h1"}
		conn.verify_object.return_value = True
		rm, point, raised = self._run(conn, _file(), other_unmigrated=True)
		self.assertIsNone(raised)
		point.assert_called_once()
		rm.assert_not_called()

	def test_delete_when_unshared(self):
		conn = MagicMock()
		conn.upload_file_to_private_bucket.return_value = {"key": "k", "bucket_name": "b", "content_hash": "h1"}
		conn.verify_object.return_value = True
		rm, point, raised = self._run(conn, _file(), other_unmigrated=False)
		self.assertIsNone(raised)
		rm.assert_called_once()

	def test_reuse_migrated_sibling_no_upload(self):
		# M1: dedup -> reuse the verified sibling's S3 object, never re-upload.
		conn = MagicMock()
		conn.verify_object.return_value = True
		sib = MagicMock(custom_s3_key="ks", custom_s3_bucket_name="bs")
		f = _file()
		rm, point, raised = self._run(conn, f, migrated_sibling=sib)
		self.assertIsNone(raised)
		conn.upload_file_to_private_bucket.assert_not_called()
		point.assert_called_once_with(f, "ks", "bs")
		rm.assert_called_once()

	def test_heal_when_local_missing_with_sibling(self):
		# N1: local bytes gone but a verified migrated sibling exists -> repoint, don't lose the doc.
		conn = MagicMock()
		conn.verify_object.return_value = True
		sib = MagicMock(custom_s3_key="ks", custom_s3_bucket_name="bs")
		f = _file()
		rm, point, raised = self._run(conn, f, exists=False, migrated_sibling=sib)
		self.assertIsNone(raised)
		conn.upload_file_to_private_bucket.assert_not_called()
		point.assert_called_once_with(f, "ks", "bs")  # actually repointed (not a no-op)
		rm.assert_not_called()

	def test_local_missing_no_hash_sibling_is_left_for_manual_recovery(self):
		# Straggler with NO content_hash whose local bytes are gone: we must NOT guess a twin
		# from a shared url (recycled names can hold different bytes) — leave it + log.
		conn = MagicMock()
		f = _file(content_hash="")
		rm, point, raised = self._run(conn, f, exists=False, migrated_sibling=None)
		self.assertIsNone(raised)
		conn.upload_file_to_private_bucket.assert_not_called()
		point.assert_not_called()  # never repointed onto an unverified-content object
		rm.assert_not_called()

	def test_idempotent_when_already_keyed(self):
		conn = MagicMock()
		rm, point, raised = self._run(conn, _file(custom_s3_key="already"))
		self.assertIsNone(raised)
		conn.upload_file_to_private_bucket.assert_not_called()
		point.assert_not_called()
		rm.assert_not_called()

	def test_backfills_missing_content_hash_before_dedup(self):
		# Sakthi's "doesn't store the hash" gap: a File reaching the sweep with no
		# content_hash must get one stored (dedup + delete guards depend on it).
		conn = MagicMock()
		conn.upload_file_to_private_bucket.return_value = {"key": "k", "bucket_name": "b", "content_hash": "h"}
		conn.verify_object.return_value = True
		f = _file(content_hash="")  # no hash stored
		with patch(f"{PKG}.frappe.get_doc", return_value=f), \
		     patch(f"{PKG}._local_path", return_value="/tmp/x"), \
		     patch(f"{PKG}.os.path.exists", return_value=True), \
		     patch(f"{PKG}.os.path.getsize", return_value=5), \
		     patch(f"{PKG}.open", return_value=io.BytesIO(b"12345")), \
		     patch(f"{PKG}._hash_local_file", return_value="BACKFILLED"), \
		     patch(f"{PKG}._migrated_sibling", return_value=None), \
		     patch(f"{PKG}._other_unmigrated_share", return_value=False), \
		     patch(f"{PKG}._point_doc_at_s3"), \
		     patch(f"{PKG}.frappe.db") as db, \
		     patch(f"{PKG}.frappe.get_meta"), \
		     patch(f"{PKG}.os.remove"):
			ps.migrate_file_to_s3("F1", conn)
		db.set_value.assert_any_call("File", "F1", "content_hash", "BACKFILLED", update_modified=False)
		self.assertEqual(f.content_hash, "BACKFILLED")


class TestInvariant2Repoint(FrappeTestCase):
	"""Invariant 2: once a file is on S3, the doc that attached it must serve the proxy url —
	NOT a now-deleted /files/<x> path. Must hold on EVERY migration path + be singles-aware."""

	def _repoint(self, file, issingle, has_field=True, exists=True, current="__match__"):
		# By default the parent field still holds THIS file's own local url (the repoint case).
		if current == "__match__":
			current = file.file_url
		meta = MagicMock(issingle=issingle)
		meta.has_field.return_value = has_field
		with patch(f"{PKG}.frappe.get_meta", return_value=meta), \
		     patch(f"{PKG}.get_proxy_url", return_value="PROXY"), \
		     patch(f"{PKG}.frappe.db.exists", return_value=exists), \
		     patch(f"{PKG}.frappe.db.get_value", return_value=current), \
		     patch(f"{PKG}.frappe.db.get_single_value", return_value=current), \
		     patch(f"{PKG}.frappe.db.set_single_value") as ssv, \
		     patch(f"{PKG}.frappe.db.set_value") as sv, \
		     patch(f"{PKG}.frappe.db.commit") as commit, \
		     patch(f"{PKG}.frappe.log_error"):
			ps._repoint_attached(file)
			return ssv, sv, commit

	def test_single_uses_set_single_value(self):
		# Website Settings.app_logo lives in tabSingles — a plain set_value is deprecated for it.
		f = _file(attached_to_doctype="Website Settings",
		          attached_to_name="Website Settings", attached_to_field="app_logo")
		ssv, sv, commit = self._repoint(f, issingle=True)
		ssv.assert_called_once_with("Website Settings", "app_logo", "PROXY", update_modified=False)
		sv.assert_not_called()
		commit.assert_called_once()

	def test_regular_uses_set_value(self):
		f = _file(attached_to_doctype="Employee", attached_to_name="EMP-1", attached_to_field="image")
		ssv, sv, commit = self._repoint(f, issingle=False)
		sv.assert_called_once_with("Employee", "EMP-1", "image", "PROXY", update_modified=False)
		ssv.assert_not_called()
		commit.assert_called_once()

	def test_no_attach_is_noop(self):
		ssv, sv, commit = self._repoint(_file(), issingle=False)  # attached_to_* all None
		sv.assert_not_called()
		ssv.assert_not_called()
		commit.assert_not_called()

	def test_missing_field_skips(self):
		f = _file(attached_to_doctype="Employee", attached_to_name="EMP-1", attached_to_field="ghost")
		ssv, sv, commit = self._repoint(f, issingle=False, has_field=False)
		sv.assert_not_called()
		commit.assert_not_called()

	def test_regular_missing_row_skips(self):
		# non-single whose parent row was deleted -> never write, never commit.
		f = _file(attached_to_doctype="Employee", attached_to_name="GONE", attached_to_field="image")
		ssv, sv, commit = self._repoint(f, issingle=False, exists=False)
		sv.assert_not_called()
		ssv.assert_not_called()
		commit.assert_not_called()

	def test_skips_when_field_moved_to_external_url(self):
		# data-safety: user set the field to an external URL -> never clobber it.
		f = _file(attached_to_doctype="Employee", attached_to_name="EMP-1", attached_to_field="image")
		ssv, sv, commit = self._repoint(f, issingle=False, current="https://cdn.example.com/x.png")
		sv.assert_not_called()
		ssv.assert_not_called()
		commit.assert_not_called()

	def test_skips_when_field_cleared(self):
		# user cleared the attachment -> don't resurrect it.
		f = _file(attached_to_doctype="Employee", attached_to_name="EMP-1", attached_to_field="image")
		ssv, sv, commit = self._repoint(f, issingle=False, current=None)
		sv.assert_not_called()
		commit.assert_not_called()

	def test_skips_when_field_points_to_newer_file(self):
		# an older orphan File still links this field; a NEWER file now owns it -> don't downgrade.
		f = _file(attached_to_doctype="Employee", attached_to_name="EMP-1", attached_to_field="image",
		          file_url="/private/files/old.png")
		ssv, sv, commit = self._repoint(f, issingle=False, current="/private/files/new.png")
		sv.assert_not_called()
		ssv.assert_not_called()
		commit.assert_not_called()

	def test_child_table_field_routes_to_child_repoint(self):
		# attached_to_field is a CHILD field (not on the parent) -> repoint child rows.
		f = _file(attached_to_doctype="Essdee Bulk Payment", attached_to_name="BLK-1",
		          attached_to_field="advance_image", file_url="/private/files/x.jpg")
		meta = MagicMock()
		meta.has_field.return_value = False  # not a parent field
		with patch(f"{PKG}.frappe.get_meta", return_value=meta), \
		     patch(f"{PKG}.get_proxy_url", return_value="PROXY"), \
		     patch(f"{PKG}.child_attach_repoint", return_value=2) as car, \
		     patch(f"{PKG}.frappe.db") as db, \
		     patch(f"{PKG}.frappe.log_error"):
			ps._repoint_attached(f)
		car.assert_called_once_with("Essdee Bulk Payment", "BLK-1", "advance_image",
		                            "/private/files/x.jpg", "PROXY")
		db.commit.assert_called_once()   # committed since a child row was repointed

	def test_single_skips_when_logo_replaced(self):
		# app_logo was replaced by a newer logo file -> the older File must not restore the old one.
		f = _file(attached_to_doctype="Website Settings", attached_to_name="Website Settings",
		          attached_to_field="app_logo", file_url="/files/old_logo.png")
		ssv, sv, commit = self._repoint(f, issingle=True, current="/files/new_logo.png")
		ssv.assert_not_called()
		commit.assert_not_called()

	# ---- every migration path must invoke the repoint ---------------------------------
	def _migrate(self, conn, file, **over):
		cfg = dict(exists=True, getsize=5, migrated_sibling=None, other_unmigrated=False)
		cfg.update(over)
		with patch(f"{PKG}.frappe.get_doc", return_value=file), \
		     patch(f"{PKG}._local_path", return_value="/tmp/x"), \
		     patch(f"{PKG}.os.path.exists", return_value=cfg["exists"]), \
		     patch(f"{PKG}.os.path.getsize", return_value=cfg["getsize"]), \
		     patch(f"{PKG}.open", return_value=io.BytesIO(b"12345")), \
		     patch(f"{PKG}._migrated_sibling", return_value=cfg["migrated_sibling"]), \
		     patch(f"{PKG}._other_unmigrated_share", return_value=cfg["other_unmigrated"]), \
		     patch(f"{PKG}._point_doc_at_s3"), \
		     patch(f"{PKG}.frappe.db"), \
		     patch(f"{PKG}.os.remove"), \
		     patch(f"{PKG}._repoint_attached") as rep:
			ps.migrate_file_to_s3("F1", conn)
		return rep

	def test_normal_upload_path_repoints(self):
		conn = MagicMock()
		conn.upload_file_to_private_bucket.return_value = {"key": "k", "bucket_name": "b", "content_hash": "h1"}
		conn.verify_object.return_value = True
		f = _file(attached_to_doctype="Employee", attached_to_name="EMP-1", attached_to_field="image")
		rep = self._migrate(conn, f)
		rep.assert_called_once_with(f)

	def test_dedup_sibling_path_repoints(self):
		# the 2nd employee sharing one image: reuse sibling's object AND fix its own field.
		conn = MagicMock()
		conn.verify_object.return_value = True
		sib = MagicMock(custom_s3_key="ks", custom_s3_bucket_name="bs")
		f = _file(attached_to_doctype="Employee", attached_to_name="EMP-2", attached_to_field="image")
		rep = self._migrate(conn, f, migrated_sibling=sib)
		rep.assert_called_once_with(f)

	def test_local_heal_path_repoints(self):
		# local bytes gone, healed from a migrated sibling -> still fix the attach field.
		conn = MagicMock()
		conn.verify_object.return_value = True
		sib = MagicMock(custom_s3_key="ks", custom_s3_bucket_name="bs")
		f = _file(attached_to_doctype="Website Settings",
		          attached_to_name="Website Settings", attached_to_field="app_logo")
		rep = self._migrate(conn, f, exists=False, migrated_sibling=sib)
		rep.assert_called_once_with(f)


class TestSweepOrchestrator(FrappeTestCase):
	def test_worker_rollback_and_continue_on_failure(self):
		conn = MagicMock()
		conn.s3_settings.disable_s3_operations = 0
		calls = []

		def fake_migrate(name, c):
			calls.append(name)
			if name == "F2":
				raise Exception("boom")

		with patch(f"{PKG}.getS3Connection", return_value=conn), \
		     patch(f"{PKG}.frappe.get_all", return_value=[frappe._dict(name="F1"), frappe._dict(name="F2")]) as ga, \
		     patch(f"{PKG}.migrate_file_to_s3", side_effect=fake_migrate), \
		     patch(f"{PKG}.frappe.db") as db, \
		     patch(f"{PKG}.frappe.clear_cache") as cc, \
		     patch(f"{PKG}.frappe.log_error") as le:
			ps.run_unuploaded_documents_sweep()

		self.assertEqual(calls, ["F1", "F2"])      # continued past the failure
		db.rollback.assert_called_once()            # rolled back only the failed one
		le.assert_called_once()
		cc.assert_called_once()                     # cache refreshed once after the sweep
		filters = ga.call_args.kwargs.get("filters")
		self.assertIn(["custom_s3_key", "in", ["", None]], filters)  # only unmigrated files

	def test_entry_enqueues_long_queue_dynamic_timeout_dedup(self):
		# The scheduler entry must offload to the long queue with a backlog-sized
		# timeout (see _sweep_timeout) so a large migrate+remove backlog isn't killed
		# by the default short timeout. Deduplicated + on a stable job_id.
		with patch(f"{PKG}.frappe.has_permission", return_value=True), \
		     patch(f"{PKG}._sweep_timeout", return_value=4321) as swt, \
		     patch(f"{PKG}.frappe.enqueue") as enq:
			ps.process_unuploaded_documents()
		swt.assert_called_once()
		enq.assert_called_once()
		self.assertIs(enq.call_args.args[0], ps.run_unuploaded_documents_sweep)
		self.assertEqual(enq.call_args.kwargs["queue"], "long")
		self.assertEqual(enq.call_args.kwargs["timeout"], 4321)  # the dynamic value, not a fixed 3h
		self.assertTrue(enq.call_args.kwargs["deduplicate"])
		self.assertTrue(enq.call_args.kwargs.get("job_id"))

	def test_sweep_timeout_floor_scale_and_cap(self):
		# Dynamic timeout = clamp(max(bytes/throughput, count*overhead) * buffer, floor, cap).
		def compute(total_bytes, count):
			with patch(f"{PKG}.frappe.get_conf", return_value={}), \
			     patch(f"{PKG}._pending_migration_stats", return_value=(total_bytes, count)):
				return ps._sweep_timeout()
		GB = 1024 ** 3
		self.assertEqual(compute(0, 0), ps.SWEEP_TIMEOUT_FLOOR)              # empty backlog -> floor
		# a byte backlog that lands mid-range regardless of the configured throughput
		mid_secs = (ps.SWEEP_TIMEOUT_FLOOR + ps.SWEEP_TIMEOUT_CAP) / 2
		mid_bytes = int(mid_secs / ps.SWEEP_SAFETY_BUFFER * ps.S3_THROUGHPUT_MBPS * 1024 * 1024)
		big = compute(mid_bytes, 0)
		self.assertGreater(big, ps.SWEEP_TIMEOUT_FLOOR)                     # byte backlog scales up
		self.assertLess(big, ps.SWEEP_TIMEOUT_CAP)
		self.assertEqual(compute(10000 * GB, 10), ps.SWEEP_TIMEOUT_CAP)     # huge -> clamped to cap
		many = compute(0, 30000)                                           # many tiny files -> count-bound
		self.assertGreater(many, ps.SWEEP_TIMEOUT_FLOOR)
		self.assertLess(many, ps.SWEEP_TIMEOUT_CAP)

	def test_sweep_stops_cleanly_on_job_timeout(self):
		# A JobTimeoutException must PROPAGATE (deadline stops the sweep) and NOT be
		# swallowed by the broad except — else the sweep runs past its budget unbounded.
		conn = MagicMock()
		conn.s3_settings.disable_s3_operations = 0

		def fake_migrate(name, c):
			raise ps.JobTimeoutException("deadline")

		with patch(f"{PKG}.getS3Connection", return_value=conn), \
		     patch(f"{PKG}.frappe.get_all", return_value=[frappe._dict(name="F1"), frappe._dict(name="F2")]), \
		     patch(f"{PKG}.migrate_file_to_s3", side_effect=fake_migrate), \
		     patch(f"{PKG}.frappe.db") as db, \
		     patch(f"{PKG}.frappe.log_error") as le:
			with self.assertRaises(ps.JobTimeoutException):
				ps.run_unuploaded_documents_sweep()
		db.rollback.assert_called_once()   # rolled back the in-flight file
		le.assert_not_called()             # a timeout is not logged as an upload failure
