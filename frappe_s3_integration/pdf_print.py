# Copyright (c) 2026, sakthi123msd@gmail.com and contributors
# For license information, please see license.txt
"""Base64-inline S3-backed images in print/PDF (letterheads + private images).

Two things break letterheads/images in PDFs after an S3 migration:

1. Frappe base64-inlines images before wkhtmltopdf (inline_private_images), but its
   helper resolves the File via find_file_by_url() which matches file_url EXACTLY; our
   serve_file proxy url carries a ?file_id= query that gets stripped from the lookup, so
   it never finds the File and never inlines. wkhtmltopdf can't fetch the proxy url (no
   login session), so private S3 images come out BLANK.

2. migrate_file_to_s3() rewrites the *File's* file_url to the proxy and DELETES the local
   file, but it never rewrites HTML that hardcodes the old local path. A Letter Head's
   `content` holds `<img src="/files/Letter_Head_xxx.png">` literally — after migration
   that local file is gone, so the letterhead renders BLANK, and (case 1's) proxy inliner
   never matches it because the src is still the plain /files/ url, not the proxy.

`inline_s3_images` fixes both: for every <img> it inlines from S3 when the src is either
(a) our serve_file proxy, or (b) a /files/ or /private/files/ path whose file was migrated
to S3 (matched by file name). Non-migrated local files are left untouched.

Wired in as frappe's `pdf_body_html`, `pdf_header_html`, `pdf_footer_html` hooks (proper
extension points, not monkeypatches) and the report `report_to_pdf` /
`render_letterhead_for_print` whitelisted methods — so every PDF path is covered.
"""

import base64
import mimetypes
from urllib.parse import parse_qs, unquote, urlparse

import frappe

_PROXY_MARK = "frappe_s3_integration.s3_core.serve_file"
_LOCAL_PREFIXES = ("/files/", "/private/files/")


def _data_uri_from_file(f):
	"""base64 data: URI for an S3-backed image File doc, or None if not usable."""
	try:
		if not f.get("custom_is_s3_uploaded") or not f.get("custom_s3_key"):
			return None  # not on S3 — leave the url as-is (local file or genuinely missing)
		if not f.is_downloadable():
			return None
		mime = mimetypes.guess_type(f.file_name or "")[0]
		if not mime or not mime.startswith("image/"):
			return None
		content = f.get_content()
		if isinstance(content, str):
			# get_content() decodes text blobs to str; an image should never decode, but
			# guard anyway so we always base64 raw bytes.
			content = content.encode("latin-1", "ignore")
		return "data:%s;base64,%s" % (mime, base64.b64encode(content).decode())
	except Exception:
		frappe.logger("pdf").error("S3 inline image failed for File %s" % f.get("name"), exc_info=True)
	return None


def _data_uri_from_proxy(src):
	"""Resolve our serve_file proxy src (…?file_id=<name>) to a base64 data: URI."""
	file_id = (parse_qs(urlparse(src).query).get("file_id") or [None])[0]
	if file_id and frappe.db.exists("File", file_id):
		return _data_uri_from_file(frappe.get_doc("File", file_id))
	return None


def _data_uri_from_local_url(src):
	"""Resolve a hardcoded /files/X or /private/files/X img src to its migrated S3 File.

	After migration the local file is deleted but such HTML (letterheads especially) still
	points at the old local path — find the File by name (S3-backed only) and inline from
	S3. Returns None if no migrated File matches (then the src is left untouched: a local
	file that still exists renders normally, a genuinely-missing one can't be recovered).
	"""
	name = unquote(urlparse(src).path.rsplit("/", 1)[-1])
	if not name:
		return None
	is_private = 1 if src.startswith("/private/files/") else 0
	# Prefer a File whose visibility matches the url prefix; fall back to any S3-backed one.
	for filters in (
		{"file_name": name, "custom_is_s3_uploaded": 1, "is_private": is_private},
		{"file_name": name, "custom_is_s3_uploaded": 1},
	):
		rows = frappe.get_all("File", filters=filters, pluck="name", order_by="creation desc", limit=1)
		if rows:
			return _data_uri_from_file(frappe.get_doc("File", rows[0]))
	return None


def inline_s3_images(html):
	"""Replace every <img> whose src is an S3-backed file (serve_file proxy OR a migrated
	/files/ /private/files/ path) with a base64 data URI. Local-only files are untouched."""
	if not html:
		return html
	# Fast path: only pay for BeautifulSoup if there's a proxy url or a local-file url to check.
	if _PROXY_MARK not in html and not any(p in html for p in _LOCAL_PREFIXES):
		return html
	from bs4 import BeautifulSoup

	soup = BeautifulSoup(html, "html.parser")
	changed = False
	for img in soup.find_all("img"):
		src = img.get("src") or ""
		uri = None
		if _PROXY_MARK in src:
			uri = _data_uri_from_proxy(src)
		elif src.startswith(_LOCAL_PREFIXES):
			uri = _data_uri_from_local_url(src)
		if uri:
			img["src"] = uri
			changed = True
	# Only reserialize if we actually inlined something — otherwise return the original
	# html byte-for-byte (don't let BeautifulSoup normalize markup we didn't need to touch).
	return str(soup) if changed else html


def pdf_body_html(jenv=None, template=None, print_format=None, args=None, **kwargs):
	"""frappe `pdf_body_html` hook: render the body as core does, then inline S3 images."""
	from frappe.utils.pdf import pdf_body_html as _default

	html = _default(template=template, args=args)
	return inline_s3_images(html)


def pdf_header_html(soup=None, head=None, content=None, styles=None, html_id=None, css=None, path=None, **kwargs):
	"""frappe `pdf_header_html` hook: the page header (where a document print's LETTERHEAD
	lands as a wkhtmltopdf --header-html temp file) is rendered here — inline its S3 images
	so an S3/migrated letterhead logo isn't blank."""
	from frappe.utils.pdf import pdf_header_html as _default

	return inline_s3_images(_default(soup, head, content, styles, html_id, css, path))


def pdf_footer_html(soup=None, head=None, content=None, styles=None, html_id=None, css=None, path=None, **kwargs):
	"""frappe `pdf_footer_html` hook: same as the header, for the page footer."""
	from frappe.utils.pdf import pdf_footer_html as _default

	return inline_s3_images(_default(soup, head, content, styles, html_id, css, path))


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
	rendered here — inline its S3 images so an S3/migrated letterhead logo isn't blank in
	report PDFs (this is the path that was still broken)."""
	from frappe.utils.print_format import render_letterhead_for_print as _default

	rendered = _default(letterhead=letterhead, doc=doc)
	if isinstance(rendered, dict):
		for key in ("header", "footer"):
			if rendered.get(key):
				rendered[key] = inline_s3_images(rendered[key])
	return rendered
