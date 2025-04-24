# Copyright (c) 2025, sakthi123msd@gmail.com and contributors
# For license information, please see license.txt

from frappe_s3_integration.s3_core import getS3Connection
import frappe
from frappe.model.document import Document


class AWSS3Settings(Document):

	def before_validate(self):
		self.validate_buckets()

	def validate_buckets(self):
		exceptions = []
		for i in self.s3_bucket_details:
			if i.default_private_bucket and i.default_public_bucket:
				exceptions.append(f"for bucket {i.get('bucket_name')} has both public and private access which is invalid")
			
			if not i.default_private_bucket and not i.default_public_bucket:
				exceptions.append(f"for bucket {i.get('bucket_name')} needs to be private or public")
		
		if exceptions and len(exceptions) > 0:
			frappe.throw("The Following Exceptions Occured <br>"+"<br>".join(exceptions))

	def on_update(self):
		conn = getS3Connection()
		conn.setup_s3_settings()