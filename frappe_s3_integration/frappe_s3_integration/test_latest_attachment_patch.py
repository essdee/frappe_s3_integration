# Copyright (c) 2026, sakthi123msd@gmail.com and contributors
# For license information, please see license.txt

from types import SimpleNamespace
from unittest import TestCase
from unittest.mock import MagicMock, patch

from frappe_s3_integration.patches.v1_0 import repoint_attach_fields_to_latest_file as repair


def _file(name, creation, file_url, s3=True, key=None, **overrides):
	values = {
		"name": name,
		"file_name": "photo.jpg",
		"file_url": file_url,
		"creation": creation,
		"custom_is_s3_uploaded": int(s3),
		"custom_s3_key": key if key is not None else ("private/files/photo.jpg" if s3 else None),
		"attached_to_doctype": "Employee",
		"attached_to_name": "EMP-1",
		"attached_to_field": "image",
	}
	values.update(overrides)
	return SimpleNamespace(**values)


class TestLatestAttachmentPatch(TestCase):
	def _frappe(self, current, is_single=False, fieldtype="Attach Image"):
		field = SimpleNamespace(fieldtype=fieldtype)
		meta = SimpleNamespace(issingle=is_single, get_field=MagicMock(return_value=field))
		db = SimpleNamespace(
			exists=MagicMock(return_value=True),
			get_value=MagicMock(return_value=current),
			get_single_value=MagicMock(return_value=current),
			set_value=MagicMock(),
			set_single_value=MagicMock(),
			commit=MagicMock(),
		)
		return SimpleNamespace(db=db, get_meta=MagicMock(return_value=meta), clear_cache=MagicMock())

	def test_newest_file_wins_when_current_points_to_old_file(self):
		new = _file("NEW", "2026-08-06 12:00:00", "PROXY:NEW")
		old = _file("OLD", "2026-08-05 12:00:00", "PROXY:OLD")
		fake = self._frappe(current="PROXY:OLD")
		with patch.object(repair, "frappe", fake), patch.object(
			repair, "get_proxy_url", side_effect=lambda name, _filename: f"PROXY:{name}"
		):
			outcome, detail = repair._repair_attach_target([new, old])

		self.assertEqual(outcome, "updated")
		self.assertEqual(detail["latest_file"], "NEW")
		fake.db.set_value.assert_called_once_with(
			"Employee", "EMP-1", "image", "PROXY:NEW", update_modified=False
		)

	def test_same_stale_local_url_still_selects_newest_file(self):
		new = _file("NEW", "2026-08-06 12:00:00", "PROXY:NEW")
		old = _file("OLD", "2026-08-05 12:00:00", "PROXY:OLD")
		fake = self._frappe(current="/private/files/photo.jpg")
		with patch.object(repair, "frappe", fake), patch.object(
			repair, "get_proxy_url", side_effect=lambda name, _filename: f"PROXY:{name}"
		):
			outcome, _ = repair._repair_attach_target([new, old])

		self.assertEqual(outcome, "updated")
		fake.db.set_value.assert_called_once_with(
			"Employee", "EMP-1", "image", "PROXY:NEW", update_modified=False
		)

	def test_cleared_external_and_unrelated_fields_are_not_resurrected(self):
		new = _file("NEW", "2026-08-06 12:00:00", "PROXY:NEW")
		old = _file("OLD", "2026-08-05 12:00:00", "PROXY:OLD")
		for current, expected in ((None, "field_cleared"), ("https://cdn/x.jpg", "unrelated_current_value")):
			with self.subTest(current=current):
				fake = self._frappe(current=current)
				with patch.object(repair, "frappe", fake), patch.object(
					repair, "get_proxy_url", side_effect=lambda name, _filename: f"PROXY:{name}"
				):
					outcome, _ = repair._repair_attach_target([new, old])
				self.assertEqual(outcome, expected)
				fake.db.set_value.assert_not_called()

	def test_single_doctype_uses_single_value_write(self):
		new = _file("NEW", "2026-08-06 12:00:00", "PROXY:NEW")
		old = _file("OLD", "2026-08-05 12:00:00", "PROXY:OLD")
		fake = self._frappe(current="PROXY:OLD", is_single=True)
		with patch.object(repair, "frappe", fake), patch.object(
			repair, "get_proxy_url", side_effect=lambda name, _filename: f"PROXY:{name}"
		):
			outcome, _ = repair._repair_attach_target([new, old])

		self.assertEqual(outcome, "updated")
		fake.db.set_single_value.assert_called_once_with(
			"Employee", "image", "PROXY:NEW", update_modified=False
		)
		fake.db.set_value.assert_not_called()

	def test_latest_local_file_wins_over_older_s3_file(self):
		new = _file("NEW", "2026-08-06 12:00:00", "/private/files/new.jpg", s3=False)
		old = _file("OLD", "2026-08-05 12:00:00", "PROXY:OLD", s3=True)
		fake = self._frappe(current="PROXY:OLD")
		with patch.object(repair, "frappe", fake), patch.object(
			repair, "get_proxy_url", side_effect=lambda name, _filename: f"PROXY:{name}"
		):
			outcome, _ = repair._repair_attach_target([new, old])

		self.assertEqual(outcome, "updated")
		fake.db.set_value.assert_called_once_with(
			"Employee", "EMP-1", "image", "/private/files/new.jpg", update_modified=False
		)

	def test_child_or_non_attach_field_is_skipped(self):
		new = _file("NEW", "2026-08-06 12:00:00", "PROXY:NEW")
		old = _file("OLD", "2026-08-05 12:00:00", "PROXY:OLD")
		fake = self._frappe(current="PROXY:OLD", fieldtype="Data")
		with patch.object(repair, "frappe", fake):
			outcome, _ = repair._repair_attach_target([new, old])

		self.assertEqual(outcome, "unsupported_or_child_field")
		fake.db.set_value.assert_not_called()

	def test_execute_requests_deterministic_newest_first_order_and_commits_once(self):
		new = _file("NEW", "2026-08-06 12:00:00", "PROXY:NEW")
		old = _file("OLD", "2026-08-05 12:00:00", "PROXY:OLD")
		fake = self._frappe(current="PROXY:OLD")
		fake.get_all = MagicMock(return_value=[new, old])
		with patch.object(repair, "frappe", fake), patch.object(
			repair, "get_proxy_url", side_effect=lambda name, _filename: f"PROXY:{name}"
		):
			result = repair.execute()

		self.assertEqual(result["updated"], 1)
		self.assertEqual(fake.get_all.call_args.kwargs["order_by"], repair.FILE_ORDER)
		fake.db.commit.assert_called_once_with()
		fake.clear_cache.assert_called_once_with()

	def test_dry_run_reports_change_without_writing_or_committing(self):
		new = _file("NEW", "2026-08-06 12:00:00", "PROXY:NEW")
		old = _file("OLD", "2026-08-05 12:00:00", "PROXY:OLD")
		fake = self._frappe(current="PROXY:OLD")
		fake.get_all = MagicMock(return_value=[new, old])
		with patch.object(repair, "frappe", fake), patch.object(
			repair, "get_proxy_url", side_effect=lambda name, _filename: f"PROXY:{name}"
		):
			result = repair.execute(dry_run=1)

		self.assertEqual(result["would_update"], 1)
		fake.db.set_value.assert_not_called()
		fake.db.commit.assert_not_called()
