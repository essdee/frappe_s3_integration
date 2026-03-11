import frappe
from urllib.parse import quote


def _build_proxy_url(file_id, file_name=None):
	if file_name:
		safe_name = quote(file_name, safe="")
		return f"/api/method/frappe_s3_integration.s3_core.serve_file/{safe_name}?file_id={file_id}"
	return f"/api/method/frappe_s3_integration.s3_core.serve_file?file_id={file_id}"


def execute():
	"""Replace direct S3 URLs with proxy URLs for all S3-uploaded files."""

	# Handle FG Item Master child table FIRST (before file_url is changed,
	# since the lookup matches File docs by their current S3 file_url)
	if frappe.db.exists("DocType", "FG Item Master"):
		_migrate_fg_item_master_images()

	files = frappe.get_all("File",
		filters={"custom_is_s3_uploaded": 1, "custom_s3_key": ["is", "set"]},
		fields=["name", "file_name", "file_url", "attached_to_doctype",
				"attached_to_name", "attached_to_field"],
	)
	for f in files:
		proxy_url = _build_proxy_url(f.name, f.file_name)
		old_url = f.file_url

		# Skip if already migrated with filename in path
		if old_url and "serve_file/" in old_url:
			continue

		frappe.db.set_value("File", f.name, "file_url", proxy_url, update_modified=False)

		# Update attached document field if it holds the old S3 URL
		if f.attached_to_doctype and f.attached_to_name and f.attached_to_field:
			try:
				meta = frappe.get_meta(f.attached_to_doctype)
				if meta.has_field(f.attached_to_field):
					current_val = frappe.db.get_value(
						f.attached_to_doctype, f.attached_to_name, f.attached_to_field
					)
					if current_val and old_url and current_val == old_url:
						frappe.db.set_value(
							f.attached_to_doctype, f.attached_to_name,
							f.attached_to_field, proxy_url, update_modified=False
						)
			except Exception:
				frappe.log_error(
					f"S3 URL migration failed for {f.attached_to_doctype}/{f.attached_to_name}",
					"S3 URL Migration"
				)

	frappe.db.commit()


def _migrate_fg_item_master_images():
	"""Migrate S3 URLs in FG Item Master catalogue image child table."""
	rows = frappe.db.sql("""
		SELECT ci.name, ci.catelog_image
		FROM `tabFG Item Master Catalogue Image` ci
		WHERE ci.catelog_image LIKE '%%s3%%amazonaws.com%%'
	""", as_dict=True)
	for row in rows:
		# Find matching File doc by the old S3 URL
		file_data = frappe.db.get_value("File",
			{"file_url": row.catelog_image},
			["name", "file_name"], as_dict=True)
		if not file_data:
			continue
		proxy_url = _build_proxy_url(file_data.name, file_data.file_name)
		frappe.db.set_value(
			"FG Item Master Catalogue Image", row.name,
			"catelog_image", proxy_url, update_modified=False
		)
