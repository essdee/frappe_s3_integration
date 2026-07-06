# Copyright (c) 2026, sakthi123msd@gmail.com and contributors
# For license information, please see license.txt
"""pdf_body_html hook — base64-inline S3-backed images in print/PDF.

Frappe base64-inlines images before wkhtmltopdf (inline_private_images), but its helper
resolves the File via find_file_by_url() which matches file_url EXACTLY; our serve_file
proxy url carries a ?file_id= query that gets stripped from the lookup, so it never finds
the File and never inlines. The image is then left as the proxy url, which wkhtmltopdf
can't fetch (no login session), so private S3 images — letterheads especially — come out
BLANK in the PDF.

This is registered as frappe's `pdf_body_html` hook (a proper extension point, not a
monkeypatch): render the body as frappe would, then base64-inline our proxy images from
S3. Everything else is untouched, and frappe's own inline_private_images afterward simply
skips the already-inlined data: URIs.
"""

import base64
import mimetypes
from urllib.parse import parse_qs, urlparse

import frappe

_PROXY_MARK = "frappe_s3_integration.s3_core.serve_file"


def _s3_img_data_uri(src):
	"""base64 data: URI for an S3 proxy image src, or None if not resolvable/allowed."""
	try:
		file_id = (parse_qs(urlparse(src).query).get("file_id") or [None])[0]
		if file_id and frappe.db.exists("File", file_id):
			f = frappe.get_doc("File", file_id)
			if not f.is_downloadable():
				return None
			mime = mimetypes.guess_type(f.file_name or "")[0]
			if not mime or not mime.startswith("image/"):
				return None
			return "data:%s;base64,%s" % (mime, base64.b64encode(f.get_content()).decode())
	except Exception:
		frappe.logger("pdf").error("S3 inline image failed for %s" % src, exc_info=True)
	return None


def inline_s3_images(html):
	"""Replace every <img> whose src is our S3 serve_file proxy with a base64 data URI."""
	if not html or _PROXY_MARK not in html:
		return html  # fast path: nothing S3 to inline
	from bs4 import BeautifulSoup

	soup = BeautifulSoup(html, "html.parser")
	for img in soup.find_all("img"):
		src = img.get("src") or ""
		if _PROXY_MARK in src:
			uri = _s3_img_data_uri(src)
			if uri:
				img["src"] = uri
	return str(soup)


def pdf_body_html(jenv=None, template=None, print_format=None, args=None, **kwargs):
	"""frappe `pdf_body_html` hook: render the body as core does, then inline S3 images."""
	from frappe.utils.pdf import pdf_body_html as _default

	html = _default(template=template, args=args)
	return inline_s3_images(html)


@frappe.whitelist()
def report_to_pdf(html, orientation="Landscape"):
	"""override_whitelisted_methods target for REPORTS (General Ledger etc.). Reports
	don't go through the pdf_body_html hook, so inline S3 images here before the PDF is
	built; then delegate to core."""
	from frappe.utils.print_format import report_to_pdf as _default

	return _default(inline_s3_images(html), orientation)


@frappe.whitelist()
def render_letterhead_for_print(letterhead=None, doc=None):
	"""override_whitelisted_methods target: the report letterhead header/footer html is
	rendered here — inline its S3 images so an S3 letterhead logo isn't blank in report
	PDFs (this is the path that was still broken)."""
	from frappe.utils.print_format import render_letterhead_for_print as _default

	rendered = _default(letterhead=letterhead, doc=doc)
	if isinstance(rendered, dict):
		for key in ("header", "footer"):
			if rendered.get(key):
				rendered[key] = inline_s3_images(rendered[key])
	return rendered
