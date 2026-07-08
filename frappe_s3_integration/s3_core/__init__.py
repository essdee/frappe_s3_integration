
import uuid
import hashlib
import mimetypes
import os
import re
import unicodedata
from urllib.parse import quote
import boto3 as s3
import frappe
from botocore.exceptions import ClientError


def _guess_content_type(filename):
	"""Best-effort MIME type from a filename; never returns None."""
	if not filename:
		return "application/octet-stream"
	return mimetypes.guess_type(filename)[0] or "application/octet-stream"


def _s3_safe_filename(filename, fallback="file"):
	"""S3-safe BASENAME that preserves the uploaded name as closely as possible
	while guaranteeing no path traversal and a valid, length-capped key segment."""
	name = os.path.basename(filename or "")
	name = name.replace("\\", "").replace("\x00", "")
	name = "".join(ch for ch in name if unicodedata.category(ch)[0] != "C")
	name = name.strip().strip(".")
	name = re.sub(r"[^\w.\-() ]", "_", name, flags=re.UNICODE)
	name = re.sub(r"\s+", " ", name).strip()
	if not name or name in (".", ".."):
		name = fallback
	if len(name) > 200:
		root, ext = os.path.splitext(name)
		name = root[:200 - len(ext)] + ext
	return name


def _s3_key_from_file_url(file_url):
	"""Map a local File url to its S3 key, mirroring Frappe's own on-disk layout:
	'/files/x.pdf' -> 'files/x.pdf', '/private/files/x.pdf' -> 'private/files/x.pdf'.
	Returns None if it isn't a local /files path (traversal-safe)."""
	url = (file_url or "").split("?", 1)[0]
	if not (url.startswith("/files/") or url.startswith("/private/files/")):
		return None
	parts = [p for p in url.split("/") if p and p not in (".", "..")]
	return "/".join(parts)


def getS3Connection():
		"""
			This method is a placeholder for the S3 connection.
			It is used to create an S3 connection object.
		"""
		global connection
		site_name = frappe.local.site
		if site_name not in connection:
			connection[site_name] = S3Connection()
		return connection[site_name]

def invalidate_s3_connection():
		"""Remove the cached S3 connection for the current site so it is recreated on next use."""
		global connection
		site_name = frappe.local.site
		connection.pop(site_name, None)

image_extensions = [
	"jpg",
	"jpeg",
	"png",
	"gif",
	"bmp",
	"tiff",
	"tif",
	"webp",
	"heif",
	"heic",
	"svg"
]

class S3Connection:
	"""
		This class is a placeholder for the S3 connection.
		It is used to create an S3 connection object.
	"""

	def __init__(self, *args, **kwargs):
		self.connection = None
		self.setup_s3_settings()
		if self.s3_settings.disable_s3_operations:
			return
		if not self.s3_settings.aws_key or not self.s3_settings.aws_secret:
			frappe.throw("Please set AWS Access Key ID and Secret Access Key in S3 Settings")
		if not self.s3_settings.region:
			frappe.throw("Please set AWS Region Name in S3 Settings")
		self.connection = s3.client(
			service_name='s3',
			aws_access_key_id=self.s3_settings.get('aws_key'),
			aws_secret_access_key=self.s3_settings.get_password('aws_secret'),
			region_name=self.s3_settings.get('region'),
		)

	def setup_s3_settings(self):
		self.s3_settings = frappe.get_single("AWS S3 Settings")
		self.setup_private_bucket()
		self.setup_public_bucket()
		self.construct_bucket_restrictions()

	def setup_public_bucket(self):
		self.public_bucket= None
		for i in self.s3_settings.s3_bucket_details:
			if i.default_public_bucket:
				self.public_bucket = i.bucket_name
				break

	def setup_private_bucket(self):
		self.private_bucket= None
		for i in self.s3_settings.s3_bucket_details:
			if i.default_private_bucket:
				self.private_bucket = i.bucket_name
				break

	def construct_bucket_restrictions(self):
		self.bucket_restrictions = {}
		for i in self.s3_settings.s3_bucket_details:
			self.bucket_restrictions[i.get('bucket_name')] = {
				"image_max" : i.get('max_image_size'),
				"file_max" : i.get('max_file_size')
			}

	def get_bucket_size(self, bucket_name, file):
		ext = file.filename.rsplit('.', 1)[-1].lower() if '.' in file.filename else ''
		if ext in image_extensions:
			return self.bucket_restrictions[bucket_name]['image_max']
		return self.bucket_restrictions[bucket_name]['file_max']
	
	def get_pre_signed_url(self, file, content_type=None):
		file_doc = frappe.get_doc("File", file)
		from frappe.core.doctype.file.file import has_permission as file_has_permission
		if not file_has_permission(file_doc, "read"):
			frappe.throw("You don't have permission to access this file", frappe.PermissionError)

		if not file_doc.custom_s3_key or not file_doc.custom_s3_bucket_name or not file_doc.custom_is_s3_uploaded:
			frappe.throw("Can't generate url for non s3 uploaded files")
		return self.generate_temporary_url(
			bucket_name=file_doc.custom_s3_bucket_name,
			key=file_doc.custom_s3_key,
			content_type=content_type,
		)
	
	def generate_temporary_url(
		self,
		bucket_name,
		key,
		expires_in=3600,
		inline=True,
		content_type=None,
	):
		"""
		Generate a temporary (pre-signed) URL to access a file.
		"""

		if self.s3_settings.disable_s3_operations:
			frappe.throw("S3 operations are disabled")

		try:
			params = {
				"Bucket": bucket_name,
				"Key": key
			}

			if inline:
				params["ResponseContentDisposition"] = "inline"
			if content_type:
				params["ResponseContentType"] = content_type

			url = self.connection.generate_presigned_url(
				ClientMethod="get_object",
				Params=params,
				ExpiresIn=expires_in
			)

			return url

		except ClientError as e:
			frappe.log_error(str(e), "S3 Presigned URL Error")
			return None
	
	def create_bucket(self, bucket_name):
		"""
		Create a new S3 bucket.
		"""
		try:
			self.connection.create_bucket(Bucket=bucket_name)
			return True
		except Exception as e:
			frappe.log_error(f"Error creating bucket: {str(e)}")
			return False
		
	def delete_bucket(self, bucket_name):
		"""
		Delete an S3 bucket.
		"""
		try:
			self.connection.delete_bucket(Bucket=bucket_name)
			return True
		except Exception as e:
			frappe.log_error(f"Error deleting bucket: {str(e)}")
			return False
		
	def get_bucket_list(self):
		"""
		Get a list of all S3 buckets.
		"""
		try:
			response = self.connection.list_buckets()
			return [bucket['Name'] for bucket in response['Buckets']]
		except Exception as e:
			frappe.log_error(f"Error getting bucket list: {str(e)}")
			return []
		
	def get_default_upload_folder(self, bucket_name):
		for i in self.s3_settings.s3_bucket_details:
			if i.get('bucket_name') == bucket_name:
				return i.get('default_folder')
		return 'uploads'
		
	def upload_file_to_public_bucket(self, file, key=None):
		"""Upload to the default public bucket. `key` mirrors Frappe's path when known."""
		if not self.public_bucket:
			frappe.throw("No public bucket found in S3 Settings")
		return self.upload_file_to_bucket(file, self.public_bucket, allow_public=True, key=key)

	def upload_file_to_private_bucket(self, file, key=None):
		"""Upload to the default private bucket. `key` mirrors Frappe's path when known."""
		if not self.private_bucket:
			frappe.throw("No private bucket found in S3 Settings")
		return self.upload_file_to_bucket(file, self.private_bucket, allow_public=False, key=key)

	def _unique_key(self, bucket_name, key):
		"""Frappe-style collision guard: if `key` already exists, insert a short random
		suffix before the extension until it's free. Only needed for direct API uploads
		that don't pass through Frappe's local-file dedup."""
		candidate = key
		while self.verify_object(bucket_name, candidate):
			root, ext = os.path.splitext(key)
			candidate = f"{root}{uuid.uuid4().hex[:6]}{ext}"
		return candidate

	def upload_file_to_bucket(self, file, bucket_name=None, allow_public = False, key=None):
		"""Upload a file to an S3 bucket. `key` is the object key; when omitted it mirrors
		Frappe's own layout — files/<name> (public) / private/files/<name> (private) — with
		a collision-safe suffix, so the S3 path matches how Frappe stores the file."""
		if not bucket_name:
			frappe.throw("Please provide a bucket name")
		# Validate non-empty + compute content hash WITHOUT loading the whole file in memory
		file.stream.seek(0)
		hasher = hashlib.md5()
		total = 0
		for chunk in iter(lambda: file.stream.read(8192), b""):
			total += len(chunk)
			hasher.update(chunk)
		if total == 0:
			frappe.throw("Cannot upload an empty file")
		content_hash = hasher.hexdigest()
		file.stream.seek(0)
		try:
			if not key:
				# No Frappe path supplied (direct API upload): build one the Frappe way.
				prefix = "files" if allow_public else "private/files"
				key = f"{prefix}/{_s3_safe_filename(file.filename)}"
			# ALWAYS collision-guard the key — even a caller-supplied one. This app deletes
			# local copies after migrating, so a recycled generic filename (report.pdf) can
			# map two DIFFERENT files to the same url-derived key; without this the second
			# upload would OVERWRITE the first object and the first File doc would then serve
			# the wrong content. Legitimate dedup never reaches here (it reuses the sibling).
			key = self._unique_key(bucket_name, key)
			content_type = getattr(file, "content_type", None) or _guess_content_type(file.filename)
			extra_args = {"ContentType": content_type}
			if allow_public:
				extra_args["ACL"] = "public-read"
			self.connection.upload_fileobj(
				Fileobj=file,
				Bucket=bucket_name,
				Key=key,
				ExtraArgs=extra_args,
			)
			region = self.connection.meta.region_name
			file_url = _s3_https_url(bucket_name, key, region)
			return {
				"file_url": file_url,
				"key" : key,
				"bucket_name" : bucket_name,
				"content_hash": content_hash,
			}
		except Exception as e:
			frappe.log_error(f"Error uploading file: {str(e)}")
			return False
	
	def get_file_from_bucket(self, key, bucket_name):
		object = self.connection.get_object(Bucket = bucket_name, Key=key)
		return object

	def verify_object(self, bucket_name, key, expected_size=None):
		"""True only if the object exists (and, if given, its size matches).
		Distinguishes a definite 404 (return False) from a transient error
		(re-raise, so the caller rolls back + retries rather than deleting the
		only local copy). Data-safety invariant 1."""
		try:
			resp = self.connection.head_object(Bucket=bucket_name, Key=key)
		except ClientError as e:
			code = str(e.response.get("Error", {}).get("Code", ""))
			if code in ("404", "NoSuchKey", "NotFound"):
				return False
			raise
		if expected_size is not None and resp.get("ContentLength") != expected_size:
			return False
		return True

	def list_objects(self, bucket_name, prefix=None):
		"""Yield every object dict ({'Key','Size',...}) in a bucket, paginated."""
		paginator = self.connection.get_paginator("list_objects_v2")
		kwargs = {"Bucket": bucket_name}
		if prefix:
			kwargs["Prefix"] = prefix
		for page in paginator.paginate(**kwargs):
			for obj in page.get("Contents", []):
				yield obj

	def download_object(self, bucket_name, key, dest_path):
		"""Download one object to a local path (used by the read-only backup job)."""
		self.connection.download_file(bucket_name, key, dest_path)
		

	def update_file_in_bucket(self, file, bucket_name, key, allow_public=False, content_type=None):
		
		extra_args = {"ContentType": content_type or _guess_content_type(key)}
		if allow_public:
			extra_args["ACL"] = "public-read"
		self.connection.upload_fileobj(
			Fileobj=file,
			Bucket=bucket_name,
			Key=key,
			ExtraArgs=extra_args,
		)

	def copy_object_to_bucket(self, src_bucket, src_key, dest_bucket, filename, make_public):
		"""Server-side copy into another bucket (visibility toggle). Mirror Frappe: the
		object moves between files/ and private/files/ keeping its filename, so the name
		is stable across public<->private flips."""
		basename = src_key.rsplit("/", 1)[-1] or _s3_safe_filename(filename)
		prefix = "files" if make_public else "private/files"
		new_key = f"{prefix}/{basename}"
		params = {
			"Bucket": dest_bucket,
			"Key": new_key,
			"CopySource": {"Bucket": src_bucket, "Key": src_key},
		}
		if make_public:
			params["ACL"] = "public-read"
		self.connection.copy_object(**params)
		return new_key

	def delete_file_from_bucket(self, file_name, bucket_name=None):
		"""
		Delete a file from an S3 bucket.
		"""
		if not bucket_name:
			frappe.throw("Please provide a bucket name")
		try:
			self.connection.delete_object(Bucket=bucket_name, Key=file_name)
			return False
		except Exception as e:
			error_log = frappe.log_error(f"Error deleting file: {str(e)}")
			return error_log.name
		
	def validate_file_size(self, file, is_public = False):
		bucket = None
		if is_public:
			bucket = self.public_bucket
		else:
			bucket = self.private_bucket
		if not bucket:
			frappe.throw("Setup the S3 Settings")
		max_size = self.get_bucket_size(bucket_name=bucket, file=file)
		if not max_size:
			return False, max_size  # unset/0 limit means "no cap" — never reject everything
		file.stream.seek(0, 2)
		file_size = file.stream.tell() / 1024
		file.stream.seek(0)
		if max_size < file_size:
			return True, max_size
		return False, max_size
	

def create_file_and_upload_to_s3(doctype, docname, file, is_public_bucket=True, folder=None):
	connection = getS3Connection()
	oversize, max_size = connection.validate_file_size(file, is_public=is_public_bucket)
	if oversize:
		frappe.throw(f"{file.filename} exceeds the max allowed size of {max_size / 1024:.2f} MB for this bucket")
	s3_resp = None
	if is_public_bucket:
		s3_resp = connection.upload_file_to_public_bucket(file)
	else:
		s3_resp = connection.upload_file_to_private_bucket(file)
	if not s3_resp:
		frappe.throw("Error uploading file to S3")
	file_doc = frappe.new_doc("File")
	file_doc.update({
		"file_name": file.filename,
		"file_url": s3_resp.get('file_url'),
		"is_private": 0 if is_public_bucket else 1,
		"attached_to_doctype": doctype,
		"attached_to_name": docname,
		"custom_s3_bucket_name": s3_resp.get('bucket_name'),
		"custom_s3_key": s3_resp.get('key'),
		"custom_is_s3_uploaded": 1,
		"content_hash": s3_resp.get('content_hash'),
	})
	file_doc.save()
	# Set proxy URL after save (need the doc name for the URL)
	proxy_url = get_proxy_url(file_doc.name, file_doc.file_name)
	file_doc.db_set("file_url", proxy_url, update_modified=False)
	return proxy_url, file_doc.name


def flag_file_for_s3(doc, event=None, *args):
	"""File.after_insert hook: capture every new LOCAL file for the midnight sweep.
	Metadata-only; never touches S3 at request time (invariant 7)."""
	if doc.get("is_folder"):
		return
	if doc.get("custom_is_s3_uploaded") or doc.get("custom_s3_key"):
		return
	file_url = doc.get("file_url") or ""
	if not (file_url.startswith("/files/") or file_url.startswith("/private/files/")):
		return
	try:
		if frappe.db.get_single_value("AWS S3 Settings", "disable_s3_operations"):
			return
	except Exception:
		return
	doc.db_set("custom_is_s3_uploaded", 1, update_modified=False)


def handle_is_private_change(doc, event=None, *args):
	"""File.on_update hook: when an S3-backed File's visibility flips, move its object
	to the matching bucket (public<->private) and re-ACL, then drop the old object if
	no other File still references it. Requires both default buckets to exist."""
	if not (doc.get("custom_is_s3_uploaded") and doc.get("custom_s3_key")):
		return
	if not doc.has_value_changed("is_private"):
		return
	conn = getS3Connection()
	# Best-effort: the visibility move must NEVER roll back the File.save(). If S3 is
	# disabled or the matching bucket isn't configured, the is_private flip still
	# persists; the object just stays put (logged where it matters).
	if conn.s3_settings.disable_s3_operations:
		return
	make_public = not doc.is_private
	dest_bucket = conn.public_bucket if make_public else conn.private_bucket
	if not dest_bucket:
		frappe.log_error(
			f"is_private toggled on {doc.name} but no default "
			f"{'public' if make_public else 'private'} bucket configured — S3 object not moved",
			"S3 Visibility",
		)
		return
	old_bucket, old_key = doc.custom_s3_bucket_name, doc.custom_s3_key
	if dest_bucket == old_bucket:
		return  # already in the correct bucket

	new_key = conn.copy_object_to_bucket(old_bucket, old_key, dest_bucket, doc.file_name, make_public)
	if not conn.verify_object(dest_bucket, new_key):
		frappe.log_error(f"Failed to move {doc.name} to {dest_bucket} — S3 object not moved", "S3 Visibility")
		return

	frappe.db.set_value("File", doc.name, {
		"custom_s3_bucket_name": dest_bucket,
		"custom_s3_key": new_key,
		"file_url": get_proxy_url(doc.name, doc.file_name),
	})
	doc.custom_s3_bucket_name = dest_bucket
	doc.custom_s3_key = new_key

	# Drop the old object only if no other File still references it (shared-blob safe).
	others = frappe.db.count("File", filters={
		"custom_s3_key": old_key,
		"custom_s3_bucket_name": old_bucket,
		"name": ["!=", doc.name],
	})
	if others == 0:
		conn.delete_file_from_bucket(old_key, old_bucket)


def delete_file_from_s3(doc, event, *args):
	"""File.on_trash hook (invariant 3): remove the S3 object ONLY when this is the LAST
	File doc referencing it — dedup'd siblings share one object, so deleting one File must
	never break the others. Touches the S3 connection only for S3-backed files, so a
	broken/unconfigured S3 setup can't block deleting ordinary local File docs — and,
	intentionally, a shared-sibling delete (no S3 delete needed) also proceeds when the
	connection is unavailable or the kill switch is on."""
	if not doc.get('custom_is_s3_uploaded', None):
		return
	key = doc.get('custom_s3_key', None)
	if not key:
		return
	# Skip S3 deletion if other File docs still reference the same object (shared blob).
	other_refs = frappe.db.count("File", filters={
		"custom_s3_key": key,
		"custom_s3_bucket_name": doc.get('custom_s3_bucket_name'),
		"name": ["!=", doc.name],
	})
	if other_refs > 0:
		return
	conn = getS3Connection()
	if conn.s3_settings.disable_s3_operations:
		frappe.throw("Can't Delete the file, The File has uploaded to s3 please enable s3 settings to remove the file from s3 also")
	res = conn.delete_file_from_bucket(key, doc.get('custom_s3_bucket_name', None))
	if res:
		frappe.throw(f"Can't Delete the file view the <a href='/app/error-log/{res}'></a>")


def get_proxy_url(file_id, file_name=None):
	"""Generate the proxy URL to store in file_url.
	Includes file_name in the path so Frappe's frontend can detect the file type
	from the extension (used for image preview, video preview, etc.).
	"""
	from urllib.parse import quote
	if file_name:
		safe_name = quote(file_name, safe="")
		return f"/api/method/frappe_s3_integration.s3_core.serve_file/{safe_name}?file_id={file_id}"
	return f"/api/method/frappe_s3_integration.s3_core.serve_file?file_id={file_id}"


def child_attach_repoint(parent_doctype, parent_name, field, expected_url, proxy_url, dry_run=False):
	"""Repoint a CHILD-table Attach field to the S3 proxy url.

	When a file is attached to an Attach field that lives on a CHILD doctype (e.g.
	`Essdee Bulk Payment.advance_image` — the field is on child doctype
	`Essdee Bulk Payment Entry`), Frappe records attached_to_doctype/name = the PARENT and
	attached_to_field = the child fieldname, so meta.has_field(parent, field) is False and a
	plain parent repoint can't reach it. Here we update every child ROW of this parent whose
	`field` STILL equals `expected_url` (this file's own pre-migration local url) to
	`proxy_url`. Identity-guarded by that filter — a row pointing at a DIFFERENT file (a
	different url) is never touched. Returns the number of rows repointed (or that WOULD be,
	when dry_run). Best-effort: never raises."""
	if not (parent_doctype and parent_name and field and expected_url):
		return 0
	n = 0
	try:
		child_doctypes = set()
		for tf in frappe.get_meta(parent_doctype).get_table_fields():
			try:
				if frappe.get_meta(tf.options).has_field(field):
					child_doctypes.add(tf.options)
			except Exception:
				continue
		for cdt in child_doctypes:
			rows = frappe.get_all(cdt, filters={
				"parenttype": parent_doctype, "parent": parent_name, field: expected_url,
			}, pluck="name")
			for r in rows:
				if not dry_run:
					frappe.db.set_value(cdt, r, field, proxy_url, update_modified=False)
				n += 1
	except Exception:
		frappe.log_error(frappe.get_traceback(), f"S3 child attach repoint failed ({parent_doctype}.{field})")
	return n


def _s3_https_url(bucket, key, region):
	"""Path-style S3 URL: https://s3.<region>.amazonaws.com/<bucket>/<key>.
	Path-style (bucket in the PATH, not the hostname) is REQUIRED for buckets whose name
	contains dots (e.g. hr.essdee.fit.public): a virtual-hosted url like
	<bucket>.s3.<region>.amazonaws.com breaks HTTPS — the wildcard cert *.s3... doesn't
	cover the extra dotted labels — so the browser fails / mis-redirects."""
	from urllib.parse import quote

	return f"https://s3.{region}.amazonaws.com/{bucket}/{quote(key, safe='/')}"


@frappe.whitelist(allow_guest=True)
def serve_file(file_id=None):
	"""
	Serve S3 files. For public files, redirects directly to the S3 URL
	(no server proxying). For private files, streams through server
	with Frappe permission checks.
	"""
	if not file_id:
		raise frappe.exceptions.NotFound

	file_doc = frappe.db.get_value("File", file_id,
		["name", "file_name", "is_private", "custom_s3_key",
		 "custom_s3_bucket_name", "custom_is_s3_uploaded"],
		as_dict=True)

	if not file_doc or not file_doc.custom_is_s3_uploaded or not file_doc.custom_s3_key:
		raise frappe.exceptions.NotFound

	# Private files require login + Frappe permission check + proxy streaming
	if file_doc.is_private:
		if frappe.session.user == "Guest":
			raise frappe.PermissionError
		from frappe.core.doctype.file.file import has_permission as file_has_permission
		full_doc = frappe.get_doc("File", file_id)
		if not file_has_permission(full_doc, "read"):
			raise frappe.PermissionError

		return _stream_from_s3(file_doc)

	# Public files — redirect to the direct S3 URL (no server bandwidth used).
	# Build from settings (not conn.connection) so it still works when the
	# disable_s3_operations kill switch is ON — public objects stay reachable.
	conn = getS3Connection()
	region = conn.s3_settings.region
	# Path-style url — dotted bucket names (hr.essdee.fit.public) can't use virtual-hosted.
	s3_url = _s3_https_url(file_doc.custom_s3_bucket_name, file_doc.custom_s3_key, region)

	from werkzeug.utils import redirect
	return redirect(s3_url, code=302)


def _stream_from_s3(file_doc):
	"""Stream private file content from S3 through the server."""
	conn = getS3Connection()
	if conn.s3_settings.disable_s3_operations:
		from werkzeug.exceptions import ServiceUnavailable
		raise ServiceUnavailable("S3 file access is temporarily disabled")

	try:
		s3_obj = conn.get_file_from_bucket(
			file_doc.custom_s3_key, file_doc.custom_s3_bucket_name
		)
	except ClientError as e:
		code = str(e.response.get("Error", {}).get("Code", ""))
		if code in ("404", "NoSuchKey", "NoSuchBucket", "NotFound"):
			raise frappe.exceptions.NotFound
		raise

	content_type = s3_obj.get("ContentType")
	if not content_type or content_type in ("binary/octet-stream", "application/octet-stream"):
		content_type = mimetypes.guess_type(file_doc.file_name)[0] or "application/octet-stream"

	from werkzeug.wrappers import Response

	def stream_body():
		body = s3_obj["Body"]
		while True:
			chunk = body.read(8192)
			if not chunk:
				break
			yield chunk

	response = Response(stream_body(), content_type=content_type)
	response.headers["Content-Disposition"] = f'inline; filename="{file_doc.file_name}"'
	return response


connection = {}
