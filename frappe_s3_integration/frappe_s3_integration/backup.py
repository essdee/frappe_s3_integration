# Copyright (c) 2026, sakthi123msd@gmail.com and contributors
# For license information, please see license.txt
"""Nightly dual backup: download both S3 buckets into one compressed local archive
each, with retention. Read-only on S3 (data-safety invariant 5) — never mutates the
source buckets. The local archive is the second copy so neither S3 nor the server is
a single point of failure. Restore is a manual AWS-CLI step (see docs/RESTORE.md)."""

import os
import shutil
import tarfile

import frappe
from frappe.utils import get_site_path, now

from frappe_s3_integration.s3_core import getS3Connection


def _backup_dir(settings):
	path = (settings.get("backup_directory") or "").strip() or get_site_path("private", "backups", "s3")
	os.makedirs(path, exist_ok=True)
	return path


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


def run_backup_s3_buckets():
	conn = getS3Connection()
	settings = conn.s3_settings
	if settings.disable_s3_operations or not settings.get("enable_bucket_backup"):
		return
	keep = int(settings.get("backup_retention_count") or 7)
	base = _backup_dir(settings)
	stamp = now().replace(" ", "_").replace(":", "-")

	seen = set()
	for bucket_name in (conn.private_bucket, conn.public_bucket):
		if not bucket_name or bucket_name in seen:
			continue
		seen.add(bucket_name)
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
				# A bad night must never erode the second copy (data-safety).
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
