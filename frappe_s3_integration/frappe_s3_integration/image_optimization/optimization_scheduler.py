import frappe
from frappe_s3_integration.s3_core import  getS3Connection
from frappe.utils import cint
import io
from PIL import Image

def pending_optimization_logs():

	setting = frappe.get_single("File Image Settings")
	if not setting.get("optimize_images_in_s3"):
		return
	
	processes = frappe.get_all("S3 Image Optimization Log", filters = {
		"status" : "Pending"
	})
	s3_connection = getS3Connection()
	for i in processes:
		doc = frappe.get_doc("S3 Image Optimization Log", i['name'])
		process_image_optimizations(doc, s3_connection, setting)


def process_image_optimizations(doc, s3_service , settings):
	try:
		update_message(doc, 'Processing')
		for i in doc.optimisation_details:
			if not frappe.db.exists("File", i.file):
				continue
			file_doc = frappe.get_doc("File", i.file)
			if not file_doc.get('custom_is_s3_uploaded'):
				i.before_optimization_size = file_doc.get('file_size')
				i.after_optimization_size = file_doc.get('file_size')
				continue

			image_file = s3_service.get_file_from_bucket(file_doc.get('custom_s3_key'), file_doc.get('custom_s3_bucket_name'))
			file_content = image_file['Body'].read()
			i.before_optimization_size = len(file_content)
			file_content = optimize_image(file_content, image_file['ContentType'], optimize=True, quality=cint(settings.get("image_optimization_quantity")))
			i.after_optimization_size = len(file_content)
			file_content = io.BytesIO(file_content)
			s3_service.update_file_in_bucket(file_content, file_doc.get('custom_s3_bucket_name'), file_doc.get('custom_s3_key'), False if file_doc.get('is_private') else True)
			update_message(doc, 'Success')
	except Exception as e:
		error_log = frappe.log_error()
		update_message(doc, 'Failed', error_log=error_log.name)


def optimize_image(content, content_type, max_width=2560, max_height=1440, optimize=True, quality=85):
	if content_type == "image/svg+xml":
		return content

	try:
		image = Image.open(io.BytesIO(content))
		exif = image.getexif()
		width, height = image.size
		aspect_ratio = width / height
		if width > max_width or height > max_height:
			if aspect_ratio > (max_width / max_height):
				new_width = max_width
				new_height = int(max_width / aspect_ratio)
			else:
				new_height = max_height
				new_width = int(max_height * aspect_ratio)
		else:
			new_width, new_height = width, height

		image_format = content_type.split("/")[-1].upper()
		if image_format in ["OCTET-STREAM", "X-OCTET-STREAM", "BINARY", "UNKNOWN"]:
			image_format = image.format 
		if not image_format:
			image_format = "JPEG"
		image.thumbnail((new_width, new_height), Image.Resampling.LANCZOS)

		output = io.BytesIO()
		image.save(
			output,
			format=image_format,
			optimize=optimize,
			quality=quality,
			save_all=True if image_format == "gif" else None,
			exif=exif,
		)
		optimized_content = output.getvalue()
		return optimized_content if len(optimized_content) < len(content) else content
	except Exception as e:
		frappe.msgprint(frappe._("Failed to optimize image: {0}").format(str(e)))
		return content


def update_message(msg_doc, status, error_log=None):
	if status == 'Failed':
		msg_doc.status = 'Failed'
		msg_doc.error_log = error_log
	else :
		msg_doc.status = status
	msg_doc.save()
	frappe.db.commit()