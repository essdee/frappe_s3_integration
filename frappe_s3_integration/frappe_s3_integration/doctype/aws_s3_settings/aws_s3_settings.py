# Copyright (c) 2025, sakthi123msd@gmail.com and contributors
# For license information, please see license.txt

from frappe_s3_integration.s3_core import invalidate_s3_connection
import frappe
from frappe.model.document import Document


class AWSS3Settings(Document):

	def before_validate(self):
		self.validate_buckets()

	def validate_buckets(self):
		# M4: keep Settings (and the disable_s3_operations kill-switch) saveable even
		# with incomplete config, so the switch can always be toggled during an incident.
		if self.disable_s3_operations:
			return
		exceptions = []
		for i in self.s3_bucket_details:
			if i.default_private_bucket and i.default_public_bucket:
				exceptions.append(f"bucket {i.get('bucket_name')} is marked both public and private")
			if not i.default_private_bucket and not i.default_public_bucket:
				exceptions.append(f"bucket {i.get('bucket_name')} must be either public or private")

		# At most one default of each type — prevents the ambiguous "first-match-wins"
		# selection bug — without requiring both to exist (some sites run public-only).
		publics = [i for i in self.s3_bucket_details if i.default_public_bucket]
		privates = [i for i in self.s3_bucket_details if i.default_private_bucket]
		if len(publics) > 1:
			exceptions.append(f"only one default PUBLIC bucket is allowed (found {len(publics)})")
		if len(privates) > 1:
			exceptions.append(f"only one default PRIVATE bucket is allowed (found {len(privates)})")

		if exceptions:
			frappe.throw("The following problems were found:<br>" + "<br>".join(exceptions))

	def on_update(self):
		invalidate_s3_connection()