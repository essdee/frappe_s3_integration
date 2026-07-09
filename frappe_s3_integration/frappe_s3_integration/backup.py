# Copyright (c) 2026, sakthi123msd@gmail.com and contributors
# For license information, please see license.txt
"""Nightly dual backup: snapshot both S3 buckets into one compressed .tar.gz each, keeping
only the newest N. Read-only on S3 (invariant 5) — never mutates the source buckets.

Target is chosen by AWS S3 Settings:
  • SSH Host set  -> stream each snapshot STRAIGHT to another computer over SSH/SFTP
    (username + password + directory), using ~0 local disk here. This is the second copy,
    off both S3 and this server.
  • SSH Host blank -> write to a local directory (default private/s3_bucket_backups).
Restore is a manual step (untar the snapshot on the other machine)."""

import os
import posixpath
import shutil
import tarfile

import frappe
from frappe.utils import get_site_path, now

from frappe_s3_integration.s3_core import getS3Connection


def _backup_dir(settings):
	"""Where the bucket archives are written. Defaults OUTSIDE Frappe's own
	private/backups/ — Frappe's native backup cleanup (delete_temp_backups) os.remove()s
	every ENTRY in private/backups and raises IsADirectoryError on a subdirectory, which
	broke the site's scheduled S3 backup. `backup_directory` may point anywhere the server
	can write, including a mounted remote path to keep the second copy off this machine."""
	path = (settings.get("backup_directory") or "").strip() or get_site_path("private", "s3_bucket_backups")
	os.makedirs(path, exist_ok=True)
	return path


def _is_mounted(path):
	"""True if `path` (or its nearest existing ancestor) sits on a MOUNTED filesystem.
	Used to refuse writing when a remote backup target isn't actually mounted, so archives
	never silently fall back onto this server's local disk."""
	p = os.path.abspath(path)
	while not os.path.exists(p) and p != os.path.dirname(p):
		p = os.path.dirname(p)
	return os.path.ismount(p)


def _safe_rel_key(key):
	"""Return a safe relative path for an S3 key, or None to skip it (N7):
	directory markers and any traversal/absolute key are skipped."""
	if not key or key.endswith("/"):
		return None
	rel = os.path.normpath(key)
	if rel.startswith("..") or os.path.isabs(rel):
		return None
	return rel


def _prune_old_archives(directory, prefix, keep):
	"""Keep the newest `keep` archives for a bucket (date-stamped names sort lexically)."""
	archives = sorted(f for f in os.listdir(directory)
	                  if f.startswith(prefix) and f.endswith(".tar.gz"))
	removed = []
	for f in (archives[:-keep] if keep > 0 else archives):
		try:
			os.remove(os.path.join(directory, f))
			removed.append(f)
		except Exception:
			frappe.log_error(f"Failed to prune backup: {f}", "S3 Backup")
	return removed


BACKUP_TIMEOUT = 3 * 60 * 60  # 3 hours — downloading both buckets in full can be slow


def backup_s3_buckets():
	"""Scheduler entry (cron). Offload the backup to the long queue with a 3-hour
	timeout (downloading whole buckets is slow). Deduplicated to avoid overlap."""
	frappe.enqueue(
		run_backup_s3_buckets,
		queue="long",
		timeout=BACKUP_TIMEOUT,
		job_id="frappe_s3_integration::bucket_backup",
		deduplicate=True,
	)


def _buckets(conn):
	"""The distinct configured buckets to back up (private + public, de-duplicated)."""
	seen = []
	for b in (conn.private_bucket, conn.public_bucket):
		if b and b not in seen:
			seen.append(b)
	return seen


def run_backup_s3_buckets():
	conn = getS3Connection()
	settings = conn.s3_settings
	if settings.disable_s3_operations or not settings.get("enable_bucket_backup"):
		return
	keep = int(settings.get("backup_retention_count") or 7)
	ssh = _ssh_settings(settings)
	if ssh:
		_run_remote_backup(conn, ssh, keep)      # push snapshots to another computer
	else:
		_run_local_backup(conn, settings, keep)  # keep snapshots on local disk


def _stamp():
	return now().replace(" ", "_").replace(":", "-")


# ---- remote target: stream snapshots to another computer over SSH/SFTP -----------------

def _ssh_settings(settings):
	"""SSH push config from AWS S3 Settings, or None when 'SSH Host' is blank (= local mode).
	Password comes from the encrypted Password field, decrypted in memory only."""
	host = (settings.get("backup_ssh_host") or "").strip()
	if not host:
		return None
	try:
		password = settings.get_password("backup_ssh_password", raise_exception=False)
	except Exception:
		password = None
	return {
		"host": host,
		"port": int(settings.get("backup_ssh_port") or 22),
		"user": (settings.get("backup_ssh_user") or "").strip(),
		"password": password,
		"directory": (settings.get("backup_ssh_directory") or "").strip(),
	}


def _connect_sftp(ssh):
	"""Open an SFTP session to the backup box using username + password (no keys/agent).
	Host key is verified against known_hosts (secure) unless site_config sets
	{"s3_backup_ssh_autoadd": 1}. Caller must close the returned client."""
	import paramiko

	client = paramiko.SSHClient()
	client.load_system_host_keys()
	if frappe.conf.get("s3_backup_ssh_autoadd"):
		client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
	else:
		client.set_missing_host_key_policy(paramiko.RejectPolicy())
	client.connect(
		hostname=ssh["host"], port=ssh["port"], username=ssh["user"], password=ssh["password"],
		timeout=30, banner_timeout=30, auth_timeout=30, allow_agent=False, look_for_keys=False,
	)
	return client, client.open_sftp()


def _stream_bucket_to_remote(conn, bucket_name, sftp, remote_dir, stamp):
	"""Stream one bucket into `<dir>/<bucket>-<stamp>.tar.gz` on the remote, object by object,
	with NO local staging. Writes to a .part file and renames on full success; on any failure
	the partial file is removed and previous snapshots are kept (never erode the second copy).
	Returns True only when every object made it into the archive."""
	remote_final = posixpath.join(remote_dir, f"{bucket_name}-{stamp}.tar.gz")
	remote_part = remote_final + ".part"
	expected = archived = 0
	ok = False
	rf = sftp.open(remote_part, "wb")
	try:
		rf.set_pipelined(True)
		with tarfile.open(fileobj=rf, mode="w|gz") as tar:
			for obj in conn.list_objects(bucket_name):
				rel = _safe_rel_key(obj["Key"])
				if not rel:
					continue
				expected += 1
				body = conn.get_file_from_bucket(obj["Key"], bucket_name)["Body"]
				try:
					info = tarfile.TarInfo(name=f"{bucket_name}/{rel}")
					info.size = int(obj.get("Size") or 0)
					tar.addfile(info, fileobj=body)
					archived += 1
				finally:
					try:
						body.close()
					except Exception:
						pass
		ok = expected == archived
	except Exception:
		frappe.log_error(frappe.get_traceback(), f"S3 Backup: streaming {bucket_name} to remote failed")
	finally:
		try:
			rf.close()
		except Exception:
			pass
	if ok:
		try:
			try:
				sftp.remove(remote_final)   # replace any leftover with the same name
			except IOError:
				pass
			sftp.rename(remote_part, remote_final)
			return True
		except Exception:
			frappe.log_error(frappe.get_traceback(), f"S3 Backup: finalising {bucket_name} snapshot failed")
	# failed / incomplete -> drop the partial, keep history
	try:
		sftp.remove(remote_part)
	except Exception:
		pass
	if not ok:
		frappe.log_error(
			f"S3 Backup incomplete for {bucket_name}: {archived}/{expected} objects — "
			f"partial snapshot discarded, previous snapshots kept.", "S3 Backup")
	return False


def _prune_remote(sftp, remote_dir, bucket_name, keep):
	"""Keep only the newest `keep` snapshots for a bucket on the remote (date-stamped names
	sort chronologically). Only removes finished .tar.gz files, never .part."""
	try:
		names = sorted(n for n in sftp.listdir(remote_dir)
		               if n.startswith(f"{bucket_name}-") and n.endswith(".tar.gz"))
	except Exception:
		frappe.log_error(frappe.get_traceback(), f"S3 Backup: listing remote for prune failed ({bucket_name})")
		return
	for n in (names[:-keep] if keep > 0 else names):
		try:
			sftp.remove(posixpath.join(remote_dir, n))
		except Exception:
			frappe.log_error(f"Failed to prune remote snapshot: {n}", "S3 Backup")


def _run_remote_backup(conn, ssh, keep):
	if not (ssh["user"] and ssh["password"] and ssh["directory"]):
		frappe.log_error("S3 Backup: SSH host set but user/password/directory missing — skipped", "S3 Backup")
		return
	try:
		client, sftp = _connect_sftp(ssh)
	except Exception:
		frappe.log_error(
			frappe.get_traceback(),
			"S3 Backup: SSH connect failed — skipped (check host in known_hosts, user/password, directory)")
		return
	stamp = _stamp()
	try:
		for bucket_name in _buckets(conn):
			if _stream_bucket_to_remote(conn, bucket_name, sftp, ssh["directory"], stamp):
				_prune_remote(sftp, ssh["directory"], bucket_name, keep)
	finally:
		for c in (sftp, client):
			try:
				c.close()
			except Exception:
				pass


# ---- local target (fallback when no SSH host is configured) -----------------------------

def _run_local_backup(conn, settings, keep):
	# Keep this server's disk minimal: if backup_directory is a mount and site_config sets
	# {"s3_backup_require_mount": 1}, refuse to run when it isn't mounted (no local fallback).
	configured = (settings.get("backup_directory") or "").strip()
	if configured and frappe.conf.get("s3_backup_require_mount") and not _is_mounted(configured):
		frappe.log_error(
			f"S3 Backup: target {configured} is not a mounted filesystem — skipping this run "
			f"so archives never fall back to local disk.", "S3 Backup")
		return
	base = _backup_dir(settings)
	stamp = _stamp()
	for bucket_name in _buckets(conn):
		staging = os.path.join(base, f".staging-{bucket_name}")
		shutil.rmtree(staging, ignore_errors=True)
		os.makedirs(staging, exist_ok=True)
		expected = downloaded = 0
		try:
			for obj in conn.list_objects(bucket_name):
				rel = _safe_rel_key(obj["Key"])
				if not rel:
					continue
				expected += 1
				dest = os.path.join(staging, rel)
				try:
					os.makedirs(os.path.dirname(dest), exist_ok=True)
					conn.download_object(bucket_name, obj["Key"], dest)
					downloaded += 1
				except Exception:
					frappe.log_error(frappe.get_traceback(), f"S3 Backup download failed: {bucket_name}/{obj['Key']}")
			if downloaded != expected:
				# Degraded run: do NOT write a partial archive and do NOT prune good history.
				frappe.log_error(
					f"S3 Backup incomplete for {bucket_name}: {downloaded}/{expected} objects downloaded. "
					f"Keeping previous archives; skipping archive + prune this run.",
					"S3 Backup",
				)
				continue
			archive = os.path.join(base, f"{bucket_name}-{stamp}.tar.gz")
			with tarfile.open(archive, "w:gz") as tar:
				tar.add(staging, arcname=bucket_name)
		finally:
			shutil.rmtree(staging, ignore_errors=True)  # staging never lingers permanently
		_prune_old_archives(base, f"{bucket_name}-", keep)
