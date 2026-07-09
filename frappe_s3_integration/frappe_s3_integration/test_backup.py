# Copyright (c) 2026, sakthi123msd@gmail.com and Contributors
# See license.txt

import io
import os
import shutil
import socket
import sys
import tarfile
import tempfile
import threading
import time
import unittest
from unittest.mock import MagicMock, patch

import frappe
from frappe.tests.utils import FrappeTestCase

from frappe_s3_integration.frappe_s3_integration import backup

try:
	import paramiko
	HAS_PARAMIKO = True
except ImportError:  # paramiko is only needed for remote SSH backup
	HAS_PARAMIKO = False


class _FakeRemoteFile(io.BytesIO):
	"""Stand-in for a paramiko SFTPFile: a real buffer we can inspect, plus set_pipelined()."""
	def set_pipelined(self, value):
		pass

	def close(self):
		pass  # keep the buffer readable after the tar stream closes


if HAS_PARAMIKO:
	class _StubSSHServer(paramiko.ServerInterface):
		"""Password auth (test/pw) only, so we exercise the real password path."""
		def check_auth_password(self, username, password):
			return paramiko.AUTH_SUCCESSFUL if (username == "test" and password == "pw") \
				else paramiko.AUTH_FAILED

		def check_channel_request(self, kind, chanid):
			return paramiko.OPEN_SUCCEEDED

		def get_allowed_auths(self, username):
			return "password"

	class _StubSFTPHandle(paramiko.SFTPHandle):
		def stat(self):
			try:
				return paramiko.SFTPAttributes.from_stat(os.fstat(self.readfile.fileno()))
			except OSError as e:
				return paramiko.SFTPServer.convert_errno(e.errno)

	class _StubSFTP(paramiko.SFTPServerInterface):
		"""A minimal SFTP server backed by a temp directory (paramiko demo pattern)."""
		ROOT = None

		def _real(self, path):
			return _StubSFTP.ROOT + self.canonicalize(path)

		def list_folder(self, path):
			out = []
			try:
				for f in os.listdir(self._real(path)):
					a = paramiko.SFTPAttributes.from_stat(os.stat(os.path.join(self._real(path), f)))
					a.filename = f
					out.append(a)
			except OSError as e:
				return paramiko.SFTPServer.convert_errno(e.errno)
			return out

		def stat(self, path):
			try:
				return paramiko.SFTPAttributes.from_stat(os.stat(self._real(path)))
			except OSError as e:
				return paramiko.SFTPServer.convert_errno(e.errno)

		lstat = stat

		def open(self, path, flags, attr):
			try:
				fd = os.open(self._real(path), flags | getattr(os, "O_BINARY", 0), 0o666)
			except OSError as e:
				return paramiko.SFTPServer.convert_errno(e.errno)
			if flags & os.O_WRONLY:
				mode = "ab" if flags & os.O_APPEND else "wb"
			elif flags & os.O_RDWR:
				mode = "a+b" if flags & os.O_APPEND else "r+b"
			else:
				mode = "rb"
			try:
				f = os.fdopen(fd, mode)
			except OSError as e:
				return paramiko.SFTPServer.convert_errno(e.errno)
			h = _StubSFTPHandle(flags)
			h.filename = self._real(path)
			h.readfile = h.writefile = f
			return h

		def remove(self, path):
			try:
				os.remove(self._real(path))
			except OSError as e:
				return paramiko.SFTPServer.convert_errno(e.errno)
			return paramiko.SFTP_OK

		def rename(self, oldpath, newpath):
			try:
				os.rename(self._real(oldpath), self._real(newpath))
			except OSError as e:
				return paramiko.SFTPServer.convert_errno(e.errno)
			return paramiko.SFTP_OK


@unittest.skipUnless(HAS_PARAMIKO, "paramiko not installed")
class TestBackupSSHIntegration(FrappeTestCase):
	"""End-to-end: the real backup code streaming to a real (in-process) SFTP server."""

	@classmethod
	def setUpClass(cls):
		super().setUpClass()
		import logging
		# paramiko logs the client disconnect ("Connection reset by peer") as an error — silence
		# it so the test output stays clean; it's expected teardown noise, not a failure.
		logging.getLogger("paramiko").setLevel(logging.CRITICAL)
		cls._host_key = paramiko.RSAKey.generate(2048)

	def setUp(self):
		self.root = tempfile.mkdtemp(prefix="s3bkp_sftp_")
		_StubSFTP.ROOT = self.root
		self._lsock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
		self._lsock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
		self._lsock.bind(("127.0.0.1", 0))
		self._lsock.listen(1)
		self._lsock.settimeout(15)
		self.port = self._lsock.getsockname()[1]

		def _serve():
			try:
				conn, _ = self._lsock.accept()
				t = paramiko.Transport(conn)
				t.add_server_key(self._host_key)
				t.set_subsystem_handler("sftp", paramiko.SFTPServer, _StubSFTP)
				t.start_server(server=_StubSSHServer())
				while t.is_active():
					time.sleep(0.05)
			except Exception:
				pass

		self._thread = threading.Thread(target=_serve, daemon=True)
		self._thread.start()
		time.sleep(0.1)
		# accept the ephemeral host key for _connect_sftp (real known_hosts stays untouched)
		frappe.conf["s3_backup_ssh_autoadd"] = 1

	def tearDown(self):
		frappe.conf.pop("s3_backup_ssh_autoadd", None)
		try:
			self._lsock.close()
		except Exception:
			pass
		shutil.rmtree(self.root, ignore_errors=True)

	def _ssh(self):
		return {"host": "127.0.0.1", "port": self.port, "user": "test",
		        "password": "pw", "directory": "/"}

	def _s3_conn(self, blobs, private="prvbkt", public=None):
		conn = MagicMock()
		conn.private_bucket, conn.public_bucket = private, public
		conn.list_objects.return_value = [{"Key": k, "Size": len(v)} for k, v in blobs.items()]
		conn.get_file_from_bucket.side_effect = lambda key, bucket: {"Body": io.BytesIO(blobs[key])}
		return conn

	def test_connect_stream_verify_and_retention(self):
		blobs = {"files/logo.png": b"\x89PNG-logo-bytes",
		         "private/files/statement.pdf": b"%PDF-1.4 hi " * 40,
		         "files/emptydir/": b""}          # directory marker -> must be skipped
		conn = self._s3_conn(blobs)
		client, sftp = backup._connect_sftp(self._ssh())
		try:
			self.assertTrue(client.get_transport().is_authenticated())   # real password auth

			ok = backup._stream_bucket_to_remote(conn, "prvbkt", sftp, "/", "2026-07-09_12-00-00")
			self.assertTrue(ok)                                          # size-verify passed
			snap = os.path.join(self.root, "prvbkt-2026-07-09_12-00-00.tar.gz")
			self.assertTrue(os.path.exists(snap))
			with tarfile.open(snap, "r:gz") as t:                       # a VALID gzip tar landed
				self.assertEqual(sorted(m.name for m in t.getmembers()),
				                 ["prvbkt/files/logo.png", "prvbkt/private/files/statement.pdf"])
				self.assertEqual(t.extractfile("prvbkt/files/logo.png").read(), blobs["files/logo.png"])
				self.assertEqual(t.extractfile("prvbkt/private/files/statement.pdf").read(),
				                 blobs["private/files/statement.pdf"])

			backup._stream_bucket_to_remote(conn, "prvbkt", sftp, "/", "2026-07-09_13-00-00")
			backup._prune_remote(sftp, "/", "prvbkt", keep=1)           # retention keeps newest
			self.assertEqual(
				sorted(f for f in os.listdir(self.root) if f.startswith("prvbkt-") and f.endswith(".tar.gz")),
				["prvbkt-2026-07-09_13-00-00.tar.gz"])
		finally:
			sftp.close()
			client.close()

	def test_full_run_reaps_stale_part_and_writes_snapshot(self):
		# the whole _run_remote_backup path (connect -> reap -> stream -> prune -> close).
		open(os.path.join(self.root, "prvbkt-stale.tar.gz.part"), "wb").close()  # leftover from a killed run
		conn = self._s3_conn({"files/a.txt": b"alpha", "files/b.txt": b"bravo"})
		backup._run_remote_backup(conn, self._ssh(), keep=2)

		self.assertFalse(os.path.exists(os.path.join(self.root, "prvbkt-stale.tar.gz.part")))  # reaped
		snaps = [f for f in os.listdir(self.root) if f.startswith("prvbkt-") and f.endswith(".tar.gz")]
		self.assertEqual(len(snaps), 1)                                # one snapshot written
		with tarfile.open(os.path.join(self.root, snaps[0]), "r:gz") as t:
			self.assertEqual(sorted(m.name for m in t.getmembers()),
			                 ["prvbkt/files/a.txt", "prvbkt/files/b.txt"])


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

	# ---- remote SSH-push backup (snapshots to another computer) --------------------------
	def test_ssh_settings_none_when_host_blank(self):
		s = MagicMock()
		s.get.side_effect = lambda k, *a: ""
		self.assertIsNone(backup._ssh_settings(s))

	def test_ssh_settings_reads_encrypted_password(self):
		s = MagicMock()
		s.get.side_effect = {"backup_ssh_host": "box", "backup_ssh_port": 2222,
		                     "backup_ssh_user": "u", "backup_ssh_directory": "/backup"}.get
		s.get_password.return_value = "secret"
		out = backup._ssh_settings(s)
		self.assertEqual(out, {"host": "box", "port": 2222, "user": "u",
		                       "password": "secret", "directory": "/backup"})
		s.get_password.assert_called_once_with("backup_ssh_password", raise_exception=False)

	def test_run_backup_routes_to_remote_when_ssh_set(self):
		conn = MagicMock()
		conn.s3_settings.disable_s3_operations = 0
		conn.s3_settings.get.side_effect = {"enable_bucket_backup": 1, "backup_retention_count": 5}.get
		with patch.object(backup, "getS3Connection", return_value=conn), \
		     patch.object(backup, "_ssh_settings", return_value={"host": "box"}), \
		     patch.object(backup, "_run_remote_backup") as rr, \
		     patch.object(backup, "_run_local_backup") as rl:
			backup.run_backup_s3_buckets()
		rr.assert_called_once()
		rl.assert_not_called()

	def test_run_backup_routes_to_local_when_no_ssh(self):
		conn = MagicMock()
		conn.s3_settings.disable_s3_operations = 0
		conn.s3_settings.get.side_effect = {"enable_bucket_backup": 1}.get
		with patch.object(backup, "getS3Connection", return_value=conn), \
		     patch.object(backup, "_ssh_settings", return_value=None), \
		     patch.object(backup, "_run_remote_backup") as rr, \
		     patch.object(backup, "_run_local_backup") as rl:
			backup.run_backup_s3_buckets()
		rl.assert_called_once()
		rr.assert_not_called()

	def _stream_conn(self, blobs):
		conn = MagicMock()
		conn.list_objects.return_value = [{"Key": k, "Size": len(v)} for k, v in blobs.items()]
		conn.get_file_from_bucket.side_effect = lambda key, bucket: {"Body": io.BytesIO(blobs[key])}
		return conn

	def test_stream_bucket_writes_valid_tar_and_renames(self):
		conn = self._stream_conn({"files/a.txt": b"abc", "files/b.txt": b"hello"})
		fake = _FakeRemoteFile()
		sftp = MagicMock()
		sftp.open.return_value = fake
		sftp.stat.side_effect = lambda p: MagicMock(st_size=len(fake.getvalue()))  # full size landed
		sftp.remove.side_effect = IOError  # no leftover final to replace
		ok = backup._stream_bucket_to_remote(conn, "bkt", sftp, "/backup", "2026-07-09_10-00-00")
		self.assertTrue(ok)
		# wrote to a .part then renamed to the final snapshot name
		self.assertTrue(sftp.open.call_args.args[0].endswith("bkt-2026-07-09_10-00-00.tar.gz.part"))
		src, dst = sftp.rename.call_args.args
		self.assertTrue(src.endswith(".part") and dst.endswith("bkt-2026-07-09_10-00-00.tar.gz"))
		# the streamed bytes are a valid gzip tar containing both objects
		with tarfile.open(fileobj=io.BytesIO(fake.getvalue()), mode="r:gz") as t:
			self.assertEqual(sorted(m.name for m in t.getmembers()),
			                 ["bkt/files/a.txt", "bkt/files/b.txt"])
			self.assertEqual(t.extractfile("bkt/files/a.txt").read(), b"abc")

	def test_stream_bucket_incomplete_discards_partial(self):
		# an object failing mid-stream -> no rename, partial removed, previous snapshots kept.
		conn = MagicMock()
		conn.list_objects.return_value = [{"Key": "files/a.txt", "Size": 3}, {"Key": "files/b.txt", "Size": 5}]

		def get_file(key, bucket):
			if key == "files/b.txt":
				raise Exception("S3 down mid-stream")
			return {"Body": io.BytesIO(b"abc")}
		conn.get_file_from_bucket.side_effect = get_file
		sftp = MagicMock()
		sftp.open.return_value = _FakeRemoteFile()
		with patch.object(backup.frappe, "log_error"):
			ok = backup._stream_bucket_to_remote(conn, "bkt", sftp, "/backup", "STAMP")
		self.assertFalse(ok)
		sftp.rename.assert_not_called()
		self.assertTrue(any(c.args[0].endswith(".part") for c in sftp.remove.call_args_list))

	def test_stream_discards_when_remote_size_short(self):
		# silent tail truncation (remote disk full on last blocks): remote file smaller than
		# what we streamed -> snapshot discarded, never renamed to final, history kept.
		conn = self._stream_conn({"files/a.txt": b"abc", "files/b.txt": b"hello"})
		fake = _FakeRemoteFile()
		sftp = MagicMock()
		sftp.open.return_value = fake
		sftp.stat.side_effect = lambda p: MagicMock(st_size=len(fake.getvalue()) - 3)  # 3 bytes lost
		with patch.object(backup.frappe, "log_error"):
			ok = backup._stream_bucket_to_remote(conn, "bkt", sftp, "/backup", "STAMP")
		self.assertFalse(ok)
		sftp.rename.assert_not_called()   # never enshrine a truncated snapshot as good
		self.assertTrue(any(str(c.args[0]).endswith(".part") for c in sftp.remove.call_args_list))

	def test_remote_backup_reaps_stale_parts(self):
		# leftover .part from an earlier killed run is reclaimed at the start of the next run.
		conn = MagicMock()
		conn.private_bucket, conn.public_bucket = "prv", None
		sftp = MagicMock()
		sftp.listdir.return_value = ["prv-old.tar.gz.part", "prv-2026-07-01.tar.gz", "keep.txt"]
		with patch.object(backup, "_connect_sftp", return_value=(MagicMock(), sftp)), \
		     patch.object(backup, "_stream_bucket_to_remote", return_value=False), \
		     patch.object(backup.frappe, "log_error"):
			backup._run_remote_backup(conn, {"user": "u", "password": "p", "directory": "/d"}, 5)
		removed = [c.args[0] for c in sftp.remove.call_args_list]
		self.assertIn("/d/prv-old.tar.gz.part", removed)        # stale partial reaped
		self.assertNotIn("/d/prv-2026-07-01.tar.gz", removed)   # real snapshot untouched

	def test_remote_backup_clear_message_when_paramiko_missing(self):
		with patch.object(backup, "_connect_sftp", side_effect=ImportError("No module named paramiko")), \
		     patch.object(backup.frappe, "log_error") as le:
			backup._run_remote_backup(MagicMock(), {"user": "u", "password": "p", "directory": "/d"}, 5)
		self.assertIn("paramiko", " ".join(str(c.args[0]) for c in le.call_args_list))

	def test_prune_remote_keeps_newest_n(self):
		sftp = MagicMock()
		sftp.listdir.return_value = (
			[f"bkt-2026-07-0{d}.tar.gz" for d in range(1, 6)] + ["bkt-x.tar.gz.part", "other.txt"])
		with patch.object(backup.frappe, "log_error"):
			backup._prune_remote(sftp, "/backup", "bkt", keep=2)
		removed = [c.args[0] for c in sftp.remove.call_args_list]
		self.assertEqual(len(removed), 3)                       # 5 snapshots, keep 2 -> remove 3 oldest
		self.assertTrue(all(r.endswith(".tar.gz") for r in removed))  # never the .part / other files

	def test_remote_backup_skips_when_creds_missing(self):
		with patch.object(backup, "_connect_sftp") as cs, patch.object(backup.frappe, "log_error") as le:
			backup._run_remote_backup(MagicMock(), {"user": "", "password": "", "directory": ""}, 5)
		cs.assert_not_called()
		le.assert_called_once()

	def test_remote_backup_skips_on_connect_failure(self):
		with patch.object(backup, "_connect_sftp", side_effect=Exception("host key unknown")), \
		     patch.object(backup.frappe, "log_error") as le:
			backup._run_remote_backup(MagicMock(), {"user": "u", "password": "p", "directory": "/d"}, 5)
		le.assert_called()

	def test_connect_sftp_password_only_and_verifies_host_key(self):
		fake_paramiko = MagicMock()
		client = MagicMock()
		fake_paramiko.SSHClient.return_value = client
		with patch.dict(sys.modules, {"paramiko": fake_paramiko}), \
		     patch.object(backup.frappe, "conf", {}):
			c, sftp = backup._connect_sftp({"host": "box", "port": 22, "user": "u", "password": "p"})
		kw = client.connect.call_args.kwargs
		self.assertEqual(kw["password"], "p")
		self.assertFalse(kw["look_for_keys"])         # password auth only — never a key/agent
		self.assertFalse(kw["allow_agent"])
		client.load_system_host_keys.assert_called_once()
		client.set_missing_host_key_policy.assert_called_once()   # RejectPolicy (autoadd off) = MITM-safe
		fake_paramiko.RejectPolicy.assert_called_once()

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
