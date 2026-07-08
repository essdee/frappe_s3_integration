# Copyright (c) 2026, sakthi123msd@gmail.com and Contributors
# See license.txt

import os
import tarfile
import tempfile
from unittest.mock import MagicMock, patch

from frappe.tests.utils import FrappeTestCase

from frappe_s3_integration.frappe_s3_integration import backup


class TestBackup(FrappeTestCase):
	def test_prune_keeps_latest_n(self):
		d = tempfile.mkdtemp()
		names = [f"pub-2026-06-{day:02d}.tar.gz" for day in range(1, 11)]  # 10 archives
		for n in names:
			open(os.path.join(d, n), "w").close()
		removed = backup._prune_old_archives(d, "pub-", keep=7)
		self.assertEqual(len(removed), 3)
		self.assertEqual(sorted(f for f in os.listdir(d) if f.startswith("pub-")), sorted(names)[-7:])

	def test_prune_keep_zero_removes_all(self):
		d = tempfile.mkdtemp()
		for day in range(1, 4):
			open(os.path.join(d, f"pub-2026-06-0{day}.tar.gz"), "w").close()
		removed = backup._prune_old_archives(d, "pub-", keep=0)
		self.assertEqual(len(removed), 3)

	def test_default_backup_dir_is_outside_frappe_backups(self):
		# Regression: bucket archives must NOT land in Frappe's own private/backups/ — its
		# native backup cleanup (delete_temp_backups) os.remove()s every entry there and
		# crashes (IsADirectoryError) on a subdirectory, which broke the scheduled S3 backup.
		from frappe.utils import get_backups_path
		settings = MagicMock()
		settings.get.side_effect = lambda *a, **k: ""   # no backup_directory override -> default
		with patch.object(backup.os, "makedirs"):
			d = backup._backup_dir(settings)
		frappe_backups = os.path.abspath(get_backups_path())
		self.assertFalse(os.path.abspath(d).startswith(frappe_backups + os.sep),
		                 f"bucket backups must not live inside {frappe_backups}")

	def test_backup_directory_setting_is_honored(self):
		# a configured backup_directory (e.g. a mounted remote path) is used verbatim.
		settings = MagicMock()
		settings.get.side_effect = lambda k, *a: "/mnt/backup-box/essdee" if k == "backup_directory" else ""
		with patch.object(backup.os, "makedirs") as mk:
			d = backup._backup_dir(settings)
		self.assertEqual(d, "/mnt/backup-box/essdee")
		mk.assert_called_once_with("/mnt/backup-box/essdee", exist_ok=True)

	def test_safe_rel_key(self):
		self.assertEqual(backup._safe_rel_key("uploads/a.png"), "uploads/a.png")
		self.assertIsNone(backup._safe_rel_key("../../etc/passwd"))
		self.assertIsNone(backup._safe_rel_key("uploads/"))   # directory marker
		self.assertIsNone(backup._safe_rel_key(""))

	def _conn(self, d):
		conn = MagicMock()
		conn.s3_settings.disable_s3_operations = 0
		conn.s3_settings.get.side_effect = {
			"enable_bucket_backup": 1, "backup_directory": d, "backup_retention_count": 7,
		}.get
		conn.private_bucket = "prv"
		conn.public_bucket = "pub"
		conn.list_objects.side_effect = lambda b: [{"Key": f"{b}/a.txt", "Size": 3}]
		return conn

	def test_backup_archives_have_bytes_and_is_read_only(self):
		d = tempfile.mkdtemp()
		conn = self._conn(d)

		def fake_download(bucket, key, dest):
			with open(dest, "w") as f:
				f.write("abc")

		conn.download_object.side_effect = fake_download
		with patch.object(backup, "getS3Connection", return_value=conn):
			backup.run_backup_s3_buckets()

		archives = sorted(f for f in os.listdir(d) if f.endswith(".tar.gz"))
		self.assertEqual(len(archives), 2)  # one per bucket
		self.assertEqual(conn.download_object.call_count, 2)  # actually downloaded both objects
		self.assertFalse(any(f.startswith(".staging") for f in os.listdir(d)))  # staging cleaned

		# each archive really contains the downloaded bytes (not an empty tar)
		for arch in archives:
			with tarfile.open(os.path.join(d, arch)) as tar:
				files = [m for m in tar.getmembers() if m.isfile()]
				self.assertTrue(files, f"{arch} is empty")
				self.assertEqual(tar.extractfile(files[0]).read(), b"abc")

		# read-only: no mutating call on the connection
		mutating = ("delete", "put", "upload", "copy", "write")
		bad = [c[0] for c in conn.mock_calls if any(m in c[0].lower() for m in mutating)]
		self.assertEqual(bad, [], f"backup made mutating S3 calls: {bad}")

	def test_degraded_run_keeps_old_archives_and_writes_no_partial(self):
		# MUST-FIX: a night where downloads fail must NOT write a partial archive or prune good history.
		d = tempfile.mkdtemp()
		# pre-existing good archives
		for day in (1, 2):
			open(os.path.join(d, f"prv-2026-06-0{day}.tar.gz"), "w").close()
			open(os.path.join(d, f"pub-2026-06-0{day}.tar.gz"), "w").close()
		conn = self._conn(d)
		conn.download_object.side_effect = Exception("S3 down mid-run")
		with patch.object(backup, "getS3Connection", return_value=conn):
			backup.run_backup_s3_buckets()
		# no new archives written, none pruned -> still exactly the 4 originals
		archives = sorted(f for f in os.listdir(d) if f.endswith(".tar.gz"))
		self.assertEqual(len(archives), 4)

	def test_backup_aborts_when_mount_required_but_target_not_mounted(self):
		# no-local-space guarantee: a down mount must skip the run, never write locally.
		conn = self._conn("/mnt/backup-box/erp")
		with patch.object(backup.frappe, "conf", {"s3_backup_require_mount": 1}), \
		     patch.object(backup.frappe, "log_error"), \
		     patch.object(backup, "_is_mounted", return_value=False), \
		     patch.object(backup, "getS3Connection", return_value=conn):
			backup.run_backup_s3_buckets()
		conn.list_objects.assert_not_called()  # never even started -> zero local writes

	def test_backup_runs_when_mount_required_and_target_mounted(self):
		d = tempfile.mkdtemp()
		conn = self._conn(d)
		conn.download_object.side_effect = lambda b, k, dest: open(dest, "w").write("abc")
		with patch.object(backup.frappe, "conf", {"s3_backup_require_mount": 1}), \
		     patch.object(backup, "_is_mounted", return_value=True), \
		     patch.object(backup, "getS3Connection", return_value=conn):
			backup.run_backup_s3_buckets()
		self.assertEqual(len([f for f in os.listdir(d) if f.endswith(".tar.gz")]), 2)

	def test_mount_check_skipped_when_flag_off(self):
		# without the opt-in flag, an unmounted/plain dir still backs up (default behaviour).
		d = tempfile.mkdtemp()
		conn = self._conn(d)
		conn.download_object.side_effect = lambda b, k, dest: open(dest, "w").write("abc")
		with patch.object(backup.frappe, "conf", {}), \
		     patch.object(backup, "_is_mounted", return_value=False) as im, \
		     patch.object(backup, "getS3Connection", return_value=conn):
			backup.run_backup_s3_buckets()
		im.assert_not_called()  # guard not consulted when the flag is off
		self.assertEqual(len([f for f in os.listdir(d) if f.endswith(".tar.gz")]), 2)

	def test_is_mounted_walks_to_existing_ancestor(self):
		with patch.object(backup.os.path, "exists", side_effect=lambda p: p == "/mnt/box"), \
		     patch.object(backup.os.path, "ismount", side_effect=lambda p: p == "/mnt/box"):
			self.assertTrue(backup._is_mounted("/mnt/box/erp/2026"))   # ancestor is the mount
		with patch.object(backup.os.path, "exists", side_effect=lambda p: p == "/mnt/box"), \
		     patch.object(backup.os.path, "ismount", return_value=False):
			self.assertFalse(backup._is_mounted("/mnt/box/erp/2026"))  # nothing mounted

	def test_backup_skipped_when_disabled(self):
		conn = MagicMock()
		conn.s3_settings.disable_s3_operations = 1
		with patch.object(backup, "getS3Connection", return_value=conn):
			backup.run_backup_s3_buckets()
		conn.list_objects.assert_not_called()

	def test_entry_enqueues_long_queue_3h_dedup(self):
		# Downloading whole buckets can take a long time -> long queue + 3h timeout.
		with patch.object(backup.frappe, "enqueue") as enq:
			backup.backup_s3_buckets()
		enq.assert_called_once()
		self.assertIs(enq.call_args.args[0], backup.run_backup_s3_buckets)
		self.assertEqual(enq.call_args.kwargs["queue"], "long")
		self.assertEqual(enq.call_args.kwargs["timeout"], 3 * 60 * 60)
		self.assertTrue(enq.call_args.kwargs["deduplicate"])
		self.assertTrue(enq.call_args.kwargs.get("job_id"))
