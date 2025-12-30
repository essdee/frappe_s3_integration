import frappe
from frappe_s3_integration.s3_core import getS3Connection
from frappe.utils.file_manager import get_file_path
import os
from werkzeug.datastructures import FileStorage

@frappe.whitelist()
def process_unuploaded_documents():
    conn = getS3Connection()
    if conn.s3_settings.disable_s3_operations:
        return
    files = frappe.get_all(
        "File",
        filters=[
            ["custom_is_s3_uploaded", "=", 1],
            ["custom_s3_key", "in", ["", None]],
        ],
        fields=["name", "file_url", "is_private"],
    )
    for f in files:
        try:
            migrate_file_to_s3(f.name, conn)
        except Exception:
            frappe.log_error(
                frappe.get_traceback(),
                f"S3 upload failed for File {f.name}"
            )

def migrate_file_to_s3(file_name, conn):
    file = frappe.get_doc("File", file_name)

    if file.custom_s3_key:
        return
    local_path = get_file_path(file.file_url)

    if not os.path.exists(local_path):
        frappe.log_error(
            f"Local file missing: {local_path}",
            "S3 Migration"
        )
        return

    with open(local_path, "rb") as f:
        file_obj = FileStorage(
            stream=f,
            filename=file.file_name,
            content_type=None
        )

        if file.is_private:
            s3_resp = conn.upload_file_to_private_bucket(file_obj)
        else:
            s3_resp = conn.upload_file_to_public_bucket(file_obj)

    if not s3_resp:
        raise Exception("S3 upload failed")

    frappe.db.set_value(
        "File",
        file.name,
        {
            "file_url": s3_resp["file_url"],
            "custom_s3_key": s3_resp["key"],
            "custom_s3_bucket_name": s3_resp["bucket_name"],
        },
    )

    file.reload()
    if file.attached_to_doctype and file.attached_to_name and file.attached_to_field:
        frappe.db.set_value(file.attached_to_doctype, file.attached_to_name, file.attached_to_field, file.file_url)

    try:
        os.remove(local_path)
    except Exception:
        frappe.log_error(
            f"Failed to delete local file: {local_path}",
            "S3 Cleanup"
        )

    