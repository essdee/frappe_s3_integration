"""Prepared Report compatibility for S3-backed gzip attachments."""

import gzip
from urllib.parse import urlsplit

import frappe
from frappe import _
from frappe.core.doctype.prepared_report.prepared_report import PreparedReport
from frappe.desk.form.load import get_attachments


def _is_gzip_attachment(attachment):
    """Identify gzip files without relying on a query-free ``file_url`` suffix."""
    file_name = str(attachment.get("file_name") or "").lower()
    url_path = urlsplit(str(attachment.get("file_url") or "")).path.lower()
    return file_name.endswith(".gz") or url_path.endswith(".gz")


class S3PreparedReport(PreparedReport):
    """Read Prepared Report JSON from local storage or the S3 File override.

    Frappe selects the attachment with ``file_url.endswith('.gz')``. S3 proxy URLs
    put ``?file_id=...`` after the filename, so that check rejects a valid gzip
    attachment. The stable File.file_name (with URL-path fallback) is authoritative.
    """

    def get_prepared_data(self, with_file_name=False):
        attachments = get_attachments(self.doctype, self.name)
        if not attachments:
            frappe.throw(
                _("No attachment found for the prepared report"),
                title=_("Attachment Not Found"),
            )

        attachment = next(
            (item for item in attachments if _is_gzip_attachment(item)), None
        )
        if not attachment:
            frappe.throw(
                _("No gzip attachment found for the prepared report"),
                title=_("Attachment Not Found"),
            )

        content = frappe.get_doc("File", attachment.name).get_content()
        if isinstance(content, str):
            content = content.encode()
        try:
            data = gzip.decompress(content)
        except (gzip.BadGzipFile, EOFError, OSError, TypeError):
            frappe.throw(
                _("The prepared report attachment is not a valid gzip file"),
                title=_("Invalid Prepared Report"),
            )

        if with_file_name:
            return data, attachment.file_name
        return data
