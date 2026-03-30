import frappe
from frappe_s3_integration.s3_core import getS3Connection, get_proxy_url
from frappe.utils.file_manager import get_file_path
import os
from werkzeug.datastructures import FileStorage

@frappe.whitelist()
def process_unuploaded_documents():
    if not frappe.has_permission("AWS S3 Settings", "read"):
        frappe.throw("Not permitted", frappe.PermissionError)
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
            frappe.db.commit()
        except Exception:
            frappe.db.rollback()
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

    if os.path.getsize(local_path) == 0:
        frappe.log_error(
            f"Local file is empty: {local_path}",
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

    proxy_url = get_proxy_url(file.name, file.file_name)
    update_fields = {
        "file_url": proxy_url,
        "custom_s3_key": s3_resp["key"],
        "custom_s3_bucket_name": s3_resp["bucket_name"],
    }
    if s3_resp.get("content_hash"):
        update_fields["content_hash"] = s3_resp["content_hash"]
    frappe.db.set_value("File", file.name, update_fields)

    file.reload()
    # Validate before updating attached doc
    if (file.attached_to_doctype and file.attached_to_name and file.attached_to_field
            and frappe.db.exists(file.attached_to_doctype, file.attached_to_name)):
        meta = frappe.get_meta(file.attached_to_doctype)
        if meta.has_field(file.attached_to_field):
            frappe.db.set_value(
                file.attached_to_doctype, file.attached_to_name,
                file.attached_to_field, proxy_url
            )

    try:
        os.remove(local_path)
    except Exception:
        frappe.log_error(
            f"Failed to delete local file: {local_path}",
            "S3 Cleanup"
        )
