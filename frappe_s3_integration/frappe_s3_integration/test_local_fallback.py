# Copyright (c) 2026, sakthi123msd@gmail.com and Contributors
# See license.txt
"""Tests for the before_request fallback that serves stale local urls from S3."""

from unittest.mock import MagicMock, patch

import frappe
from frappe.tests.utils import FrappeTestCase
from werkzeug.exceptions import HTTPException

from frappe_s3_integration import local_fallback as lf

MOD = "frappe_s3_integration.local_fallback"


def _row(name="F1", file_name="logo.png", key="files/logo.png"):
	return frappe._dict(name=name, file_name=file_name, custom_s3_key=key)


class TestLocalFallback(FrappeTestCase):
	def _run(self, path, method="GET", local_exists=False, row=None, may_private=True):
		"""Run the hook with a mocked request; return (HTTPException|None, get_value, log_error)."""
		req = MagicMock(path=path, method=method)
		old = getattr(frappe.local, "request", None)
		frappe.local.request = req
		try:
			with patch(f"{MOD}.os.path.exists", return_value=local_exists), \
			     patch(f"{MOD}.frappe.db.get_value", return_value=row) as gv, \
			     patch(f"{MOD}._may_see_private", return_value=may_private), \
			     patch("frappe_s3_integration.s3_core.get_proxy_url",
			           side_effect=lambda n, fn=None: f"/api/proxy?file_id={n}"), \
			     patch(f"{MOD}.frappe.log_error") as le:
				try:
					lf.redirect_missing_local_files()
					return None, gv, le
				except HTTPException as e:
					return e, gv, le
		finally:
			if old is None:
				try:
					del frappe.local.request
				except AttributeError:
					pass
			else:
				frappe.local.request = old

	# ---- the fix: stale local url -> 302 to the proxy ----------------------------------
	def test_missing_public_file_redirects_to_proxy(self):
		exc, gv, le = self._run("/files/logo.png", row=_row())
		self.assertIsNotNone(exc)
		resp = exc.get_response()
		self.assertEqual(resp.status_code, 302)
		self.assertEqual(resp.headers["Location"], "/api/proxy?file_id=F1")
		# looked up by the exact Frappe-layout key
		self.assertEqual(gv.call_args.args[1]["custom_s3_key"], "files/logo.png")

	def test_missing_private_file_redirects_for_permitted_user(self):
		exc, gv, le = self._run("/private/files/doc.pdf",
			row=_row(name="F2", file_name="doc.pdf", key="private/files/doc.pdf"),
			may_private=True)
		self.assertIsNotNone(exc)
		self.assertEqual(exc.get_response().headers["Location"], "/api/proxy?file_id=F2")

	# ---- security: no private-file existence oracle --------------------------------------
	def test_private_file_not_redirected_without_permission(self):
		# Guest / unauthorized user must get core's uniform 403 — a 302 would leak that the
		# file exists (plus its docname + file_name). No oracle.
		exc, gv, le = self._run("/private/files/doc.pdf",
			row=_row(name="F2", file_name="doc.pdf", key="private/files/doc.pdf"),
			may_private=False)
		self.assertIsNone(exc)

	def test_public_file_needs_no_permission(self):
		exc, gv, le = self._run("/files/logo.png", row=_row(), may_private=False)
		self.assertIsNotNone(exc)  # public files redirect regardless of session

	def test_may_see_private_denies_guest(self):
		with patch(f"{MOD}.frappe") as fr:
			fr.session.user = "Guest"
			self.assertFalse(lf._may_see_private("F1"))

	# ---- exactness: collation folding must never redirect to a different file ------------
	def test_case_variant_key_is_not_redirected(self):
		# SQL utf8-ci matches 'files/LOGO.PNG' to key 'files/logo.png' — the hook must
		# require byte-exact equality and fall through instead.
		exc, gv, le = self._run("/files/LOGO.PNG", row=_row(key="files/logo.png"))
		self.assertIsNone(exc)

	# ---- never intervene when core can serve --------------------------------------------
	def test_existing_local_file_is_untouched(self):
		exc, gv, le = self._run("/files/here.png", local_exists=True, row=_row(file_name="here.png"))
		self.assertIsNone(exc)
		gv.assert_not_called()  # no DB hit when the local file exists

	def test_no_matching_file_doc_falls_through(self):
		exc, gv, le = self._run("/files/ghost.png", row=None)
		self.assertIsNone(exc)  # normal 404 applies

	def test_non_file_paths_are_ignored(self):
		for p in ("/app", "/api/method/ping", "/assets/js/x.js", "/filesystem"):
			exc, gv, le = self._run(p)
			self.assertIsNone(exc)
			gv.assert_not_called()

	def test_non_get_methods_are_ignored(self):
		exc, gv, le = self._run("/files/logo.png", method="POST", row=_row())
		self.assertIsNone(exc)
		gv.assert_not_called()

	def test_traversal_path_is_ignored(self):
		exc, gv, le = self._run("/files/../../site_config.json")
		self.assertIsNone(exc)
		gv.assert_not_called()

	# ---- must never break a request ------------------------------------------------------
	def test_unexpected_error_is_swallowed_and_logged_deferred(self):
		req = MagicMock(path="/files/x.png", method="GET")
		old = getattr(frappe.local, "request", None)
		frappe.local.request = req
		try:
			with patch(f"{MOD}.os.path.exists", return_value=False), \
			     patch(f"{MOD}.frappe.db.get_value", side_effect=Exception("db down")), \
			     patch(f"{MOD}.frappe.log_error") as le:
				lf.redirect_missing_local_files()  # must NOT raise
			le.assert_called_once()
			# GET/HEAD transactions get rolled back — the log must survive via redis
			self.assertTrue(le.call_args.kwargs.get("defer_insert"))
		finally:
			if old is None:
				try:
					del frappe.local.request
				except AttributeError:
					pass
			else:
				frappe.local.request = old

	def test_hook_is_registered(self):
		self.assertIn(f"{MOD}.redirect_missing_local_files", frappe.get_hooks("before_request"))
