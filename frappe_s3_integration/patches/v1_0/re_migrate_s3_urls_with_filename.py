import frappe
from frappe_s3_integration.patches.v1_0.migrate_s3_urls_to_proxy import execute as migrate_execute


def execute():
	"""Re-run S3 URL migration to include filename in proxy URL path.
	This ensures Frappe's frontend can detect file types for previews.
	"""
	migrate_execute()
