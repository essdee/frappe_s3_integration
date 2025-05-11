# Copyright (c) 2025, sakthi123msd@gmail.com and contributors
# For license information, please see license.txt

import frappe
from frappe.model.document import Document


class S3ImageOptimizationLog(Document):
	pass


def create_image_optimazation_log(ref_doctype, ref_docname, files_list):

	if not files_list:
		return

	doc = frappe.new_doc("S3 Image Optimization Log")
	doc.reference_doctype = ref_doctype
	doc.reference_docname = ref_docname
	doc.status = 'Pending'
	for i in files_list:
		doc.append("optimisation_details", {
			"file" : i
		})

	doc.save()