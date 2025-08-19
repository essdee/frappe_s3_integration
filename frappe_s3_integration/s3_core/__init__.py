
import uuid
import boto3 as s3
import frappe

def getS3Connection():
        """
            This method is a placeholder for the S3 connection.
            It is used to create an S3 connection object.
        """
        global connection
        if connection is None:
            connection = S3Connection()
        return connection

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
        self.setup_s3_settings()
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
        ext = file.filename.rsplit('.', 1)[-1] if '.' in file.filename else ''
        if ext in image_extensions:
            return self.bucket_restrictions[bucket_name]['image_max']
        return self.bucket_restrictions[bucket_name]['file_max']
    
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
        
    def upload_file_to_public_bucket(self, file, folder = None):
        """
        Upload a file to an S3 bucket.
        """
        if not self.public_bucket:
            frappe.throw("No public bucket found in S3 Settings")
        return self.upload_file_to_bucket(file, self.public_bucket, allow_public=True, folder=folder)
    
    def upload_file_to_private_bucket(self, file, folder = None):
        """
        Upload a file to an S3 bucket.
        """
        if not self.private_bucket:
            frappe.throw("No private bucket found in S3 Settings")
        return self.upload_file_to_bucket(file, self.private_bucket, allow_public=False, folder= folder)
        
    def upload_file_to_bucket(self, file, bucket_name=None, allow_public = False, folder = None):
        """
        Upload a file to an S3 bucket.
        """
        if not bucket_name:
            frappe.throw("Please provide a bucket name")
        try:
            ext = file.filename.rsplit('.', 1)[-1] if '.' in file.filename else ''
            unique_filename = f"{uuid.uuid4()}.{ext}" if ext else str(uuid.uuid4())
            key = f"{self.get_default_upload_folder(bucket_name=bucket_name)}"
            if folder:
                folder = str(folder)
                if not folder.startswith('/'):
                    folder = f"/{folder}"
                key += folder
            key += f"/{unique_filename}"
            self.connection.upload_fileobj(
                Fileobj=file,
                Bucket=bucket_name,
                Key=key,
                ExtraArgs={"ACL": "public-read"} if allow_public else None
            )
            region = self.connection.meta.region_name
            file_url = f"https://{bucket_name}.s3.dualstack.{region}.amazonaws.com/{key}"
            return {
                "file_url": file_url,
                "key" : key,
                "bucket_name" : bucket_name,
            }
        except Exception as e:
            frappe.log_error(f"Error uploading file: {str(e)}")
            return False
    
    def get_file_from_bucket(self, key, bucket_name):
        object = self.connection.get_object(Bucket = bucket_name, Key=key)
        return object
        

    def update_file_in_bucket(self, file, bucket_name, key, allow_public = False):
        
        self.connection.upload_fileobj(
            Fileobj=file,
            Bucket=bucket_name,
            Key = key,
            ExtraArgs = {"ACL" : "public-read"} if allow_public else None
        )

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
        file.stream.seek(0, 2)
        file_size = file.stream.tell()
        file_size = file_size/ 1024
        file.stream.seek(0)
        if max_size < file_size:
            return True, max_size
        return False, max_size
    

def create_file_and_upload_to_s3(doctype, docname, file, is_public_bucket = True, folder = None):
    connection = getS3Connection()
    s3_resp = None
    if is_public_bucket:
        s3_resp = connection.upload_file_to_public_bucket(file, folder=folder)
    else:
        s3_resp = connection.upload_file_to_private_bucket(file, folder = folder)
    if not s3_resp:
        frappe.throw("Error uploading file to S3")
    file_doc = frappe.new_doc("File")
    file_doc.update({
        "file_name": file.filename,
        "file_url": s3_resp.get('file_url'),
        "is_private": 0,
        "attached_to_doctype": doctype,
        "attached_to_name": docname,
        "custom_s3_bucket_name" : s3_resp.get('bucket_name'),
        "custom_s3_key" : s3_resp.get('key'),
        "custom_is_s3_uploaded" : 1,
    })
    file_doc.save()
    return file_doc.get('file_url'), file_doc.get('name')
        

        
def delete_file_from_s3(doc, event, *args):
    conn = getS3Connection()
    if doc.get('custom_is_s3_uploaded', None):
        key = doc.get('custom_s3_key', None)
        res = conn.delete_file_from_bucket(key, doc.get('custom_s3_bucket_name', None))
        if res:
            frappe.throw(f"Can't Delete the file view the <a href='/app/error-log/{res}'></a>")


connection : S3Connection | None = None
