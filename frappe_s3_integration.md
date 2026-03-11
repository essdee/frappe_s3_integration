# Frappe S3 Integration

A Frappe application that integrates AWS S3 storage with Frappe's file management system, enabling seamless upload, download, and management of files on Amazon S3.

**Author:** Sakthi Kumar P (sakthi123msd@gmail.com)
**License:** MIT
**Python:** >=3.10
**Frappe:** ~15.0.0

---

## Table of Contents

- [Architecture Overview](#architecture-overview)
- [Dependencies](#dependencies)
- [App Structure](#app-structure)
- [DocTypes](#doctypes)
- [Core S3 Module](#core-s3-module)
- [Custom Fields on File DocType](#custom-fields-on-file-doctype)
- [Hooks & Events](#hooks--events)
- [Scheduled Tasks](#scheduled-tasks)
- [Workflows](#workflows)
- [API Reference](#api-reference)
- [Configuration Guide](#configuration-guide)
- [S3 File Proxy Endpoint](#s3-file-proxy-endpoint)
- [Security Fixes](#security-fixes)
- [Migration Patch](#migration-patch)

---

## Architecture Overview

```
┌─────────────────────────────────────────────────────┐
│                   Frappe Framework                    │
│                                                      │
│  ┌──────────┐   ┌────────────┐   ┌───────────────┐  │
│  │  File     │   │ Doc Events │   │  Scheduler    │  │
│  │  DocType  │──▶│  on_trash  │   │  (cron jobs)  │  │
│  └────┬─────┘   └─────┬──────┘   └───────┬───────┘  │
│       │               │                   │          │
└───────┼───────────────┼───────────────────┼──────────┘
        │               │                   │
        ▼               ▼                   ▼
┌─────────────────────────────────────────────────────┐
│              frappe_s3_integration                    │
│                                                      │
│  ┌────────────────┐  ┌──────────────────────────┐   │
│  │  s3_core        │  │  process_scheduler       │   │
│  │  ─────────────  │  │  ──────────────────────  │   │
│  │  S3Connection   │  │  Daily migration of      │   │
│  │  Upload/Download│  │  unuploaded files to S3  │   │
│  │  Delete/Update  │  └──────────────────────────┘   │
│  │  Pre-signed URLs│                                  │
│  │  File Validation│  ┌──────────────────────────┐   │
│  │  Proxy Endpoint │  │  optimization_scheduler   │   │
│  └───────┬────────┘  │  ──────────────────────────│   │
│          │           │  Image compression &       │   │
│          │           │  resizing via Pillow       │   │
│          │           └─────────────┬──────────────┘   │
│          │                         │                  │
└──────────┼─────────────────────────┼──────────────────┘
           │                         │
           ▼                         ▼
┌─────────────────────────────────────────────────────┐
│                    AWS S3 (boto3)                     │
│                                                      │
│   ┌─────────────────┐    ┌─────────────────────┐    │
│   │  Public Bucket   │    │  Private Bucket      │    │
│   │  (ACL:public-read│    │  (Pre-signed URLs)   │    │
│   └─────────────────┘    └─────────────────────┘    │
└─────────────────────────────────────────────────────┘
```

---

## Dependencies

| Package    | Version    | Purpose                       |
|------------|------------|-------------------------------|
| `boto3`    | `1.37.37`  | AWS SDK for Python            |
| `botocore` | `1.37.37`  | AWS SDK core library          |
| `Pillow`   | (implicit) | Image optimization via PIL    |
| `frappe`   | `~15.0.0`  | Managed by bench              |

---

## App Structure

```
frappe_s3_integration/
├── frappe_s3_integration/
│   ├── __init__.py                          # Package init (version 0.0.1)
│   ├── hooks.py                             # Frappe hooks configuration
│   ├── modules.txt                          # Module: "Frappe S3 Integration"
│   ├── patches.txt                          # DB migration patches
│   ├── config/
│   │   └── __init__.py
│   ├── patches/
│   │   ├── __init__.py
│   │   └── v1_0/
│   │       ├── __init__.py
│   │       └── migrate_s3_urls_to_proxy.py  # Migrate S3 URLs to proxy URLs
│   ├── s3_core/
│   │   └── __init__.py                      # Core S3 operations (S3Connection class, proxy endpoint)
│   ├── frappe_s3_integration/
│   │   ├── doctype/
│   │   │   ├── aws_s3_settings/             # Main settings (Single DocType)
│   │   │   ├── aws_s3_settings_bucket_detail/  # Bucket config (Child Table)
│   │   │   ├── file_image_settings/         # Image optimization settings
│   │   │   ├── s3_image_optimization_log/   # Optimization log entries
│   │   │   └── s3_image_optimization_log_detail/  # Optimization log child
│   │   ├── process_scheduler.py             # File migration background job
│   │   └── image_optimization/
│   │       └── optimization_scheduler.py    # Image optimization scheduler
│   ├── fixtures/
│   │   └── custom_field.json                # Custom fields for File DocType
│   ├── templates/
│   │   └── pages/
│   └── public/
│       ├── css/
│       └── js/
├── pyproject.toml
├── README.md
└── license.txt
```

---

## DocTypes

### 1. AWS S3 Settings (Single DocType)

The central configuration document for all S3 operations. Only one instance exists per site.

**Path:** `frappe_s3_integration/frappe_s3_integration/doctype/aws_s3_settings/`

| Field                  | Type     | Description                            |
|------------------------|----------|----------------------------------------|
| `aws_key`              | Data     | AWS Access Key ID                      |
| `aws_secret`           | Password | AWS Secret Access Key                  |
| `region`               | Data     | AWS Region (e.g., `us-east-1`)         |
| `s3_bucket_details`    | Table    | Child table of bucket configurations   |
| `disable_s3_operations`| Check    | Master switch to disable all S3 ops    |

**Permissions:** System Manager (Full CRUD)

**Validation Logic:**
- `before_validate` → calls `validate_buckets()`
- Each bucket must be marked as either private **or** public (not both)
- `on_update` → refreshes the global `S3Connection` instance by calling `getS3Connection().setup_s3_settings()`

---

### 2. AWS S3 Settings Bucket Detail (Child Table)

Per-bucket configuration. Each row defines one S3 bucket and its behavior.

**Path:** `frappe_s3_integration/frappe_s3_integration/doctype/aws_s3_settings_bucket_detail/`

| Field                    | Type  | Default | Description                           |
|--------------------------|-------|---------|---------------------------------------|
| `bucket_name`            | Data  | —       | S3 bucket name (Required)             |
| `default_folder`         | Data  | —       | Default upload folder path (Required) |
| `default_private_bucket` | Check | 0       | Mark as the default private bucket    |
| `default_public_bucket`  | Check | 0       | Mark as the default public bucket     |
| `max_image_size`         | Int   | 350     | Max image upload size (KB)            |
| `max_file_size`          | Int   | 5120    | Max general file upload size (KB)     |

**Constraint:** A bucket cannot be both `default_private_bucket` and `default_public_bucket`.

---

### 3. File Image Settings (Single DocType)

Controls the image optimization feature.

**Path:** `frappe_s3_integration/frappe_s3_integration/doctype/file_image_settings/`

| Field                          | Type   | Default | Description                     |
|--------------------------------|--------|---------|---------------------------------|
| `optimize_images_in_s3`        | Check  | 0       | Enable/disable optimization     |
| `image_optimization_quantity`  | Select | —       | Quality: 100, 90, 80, 70, 60, 50 |

**Permissions:** System Manager

---

### 4. S3 Image Optimization Log

Tracks each image optimization batch job.

**Path:** `frappe_s3_integration/frappe_s3_integration/doctype/s3_image_optimization_log/`

| Field                  | Type   | Description                               |
|------------------------|--------|-------------------------------------------|
| `reference_doctype`    | Link   | DocType containing the images             |
| `reference_docname`    | Data   | Document name/ID                          |
| `status`               | Select | Pending / Processing / Success / Failed   |
| `optimisation_details` | Table  | Child table with per-file optimization data |

**Helper Function:**
```python
create_image_optimazation_log(ref_doctype, ref_docname, files_list)
```
Creates a new log document with status `Pending` and populates child rows from `files_list`.

---

### 5. S3 Image Optimization Log Detail (Child Table)

Per-file optimization results within an optimization log.

**Path:** `frappe_s3_integration/frappe_s3_integration/doctype/s3_image_optimization_log_detail/`

| Field                       | Type  | Description                          |
|-----------------------------|-------|--------------------------------------|
| `file`                      | Data  | Reference to File document name      |
| `before_optimization_size`  | Float | File size before optimization (read-only) |
| `after_optimization_size`   | Float | File size after optimization (read-only)  |

---

## Core S3 Module

**Path:** `frappe_s3_integration/s3_core/__init__.py`

This module contains all S3 interaction logic via the `S3Connection` class and associated helper functions.

### S3Connection Class

#### Initialization

```python
conn = S3Connection()
```

- Loads settings from the `AWS S3 Settings` document
- Validates AWS credentials (`aws_key`, `aws_secret`, `region`) exist
- Creates a `boto3` S3 client with the provided credentials
- Raises an error if `disable_s3_operations` is checked

#### Connection Singleton

```python
conn = getS3Connection()
```

Returns a cached `S3Connection` instance per Frappe site. Creates a new connection on the first call. Uses the site name as the cache key.

---

### Upload Operations

#### `upload_file_to_public_bucket(file, folder=None)`
Uploads a file to the configured public bucket with `ACL: public-read`.

**Returns:** `{"file_url": str, "key": str, "bucket_name": str}` or `False`

#### `upload_file_to_private_bucket(file, folder=None)`
Uploads a file to the configured private bucket (no public ACL).

**Returns:** `{"file_url": str, "key": str, "bucket_name": str}` or `False`

#### `upload_file_to_bucket(file, bucket_name, allow_public=False, folder=None)`
Core upload method used by both public/private wrappers.

**S3 Key Format:**
```
{default_folder}/{folder}/{uuid}.{extension}
```

**URL Format:**
```
https://{bucket}.s3.dualstack.{region}.amazonaws.com/{key}
```

#### `create_file_and_upload_to_s3(doctype, docname, file, is_public_bucket=True, folder=None)`
High-level helper that uploads a file to S3 **and** creates a corresponding Frappe `File` document with all custom S3 fields populated. Sets `is_private` based on `is_public_bucket` parameter. Stores a proxy URL in `file_url` instead of a direct S3 URL.

**Returns:** `(proxy_url, file_name)`

---

### Download / Access Operations

#### `get_file_from_bucket(key, bucket_name)`
Retrieves the raw file object from S3.

**Returns:** boto3 response object (contains `Body`, `ContentType`, etc.)

#### `get_pre_signed_url(file, content_type=None)`
Generates a temporary pre-signed URL for accessing a private file. Includes a Frappe permission check — only users with read access to the File document can generate URLs.

- `content_type` — Optional. Sets `ResponseContentType` on the pre-signed URL for correct browser rendering.

**Returns:** Pre-signed URL string or `None`

#### `generate_temporary_url(bucket_name, key, expires_in=3600, inline=True, content_type=None)`
Low-level method to create a pre-signed GET URL.

- Default expiry: **1 hour** (3600 seconds)
- Sets `ResponseContentDisposition: inline` for browser viewing
- `content_type` — Optional. Sets `ResponseContentType` for correct MIME type in browser.

**Returns:** URL string or `None`

---

### Delete Operations

#### `delete_file_from_bucket(file_name, bucket_name)`
Deletes an object from S3 by key and bucket name.

**Returns:** `False` on success, error log name on failure

#### `delete_file_from_s3(doc, event, *args)`
**Hook function** triggered when a `File` document is trashed.

- Checks `custom_is_s3_uploaded` flag
- Raises error if S3 operations are disabled
- Deletes the S3 object and logs errors

---

### Update Operations

#### `update_file_in_bucket(file, bucket_name, key, allow_public=False)`
Re-uploads a file to an existing S3 key, effectively replacing the object. Used during image optimization to overwrite the original with the optimized version.

---

### Validation Operations

#### `validate_file_size(file, is_public=False)`
Checks if a file exceeds the configured maximum size for its bucket.

**Returns:** `(exceeded: bool, max_size: int)`

#### `get_bucket_size(bucket_name, file)`
Determines the max allowed size based on file extension:
- Image extensions → `max_image_size` (default 350 KB)
- All others → `max_file_size` (default 5120 KB)

**Recognized image extensions:** `jpg, jpeg, png, gif, bmp, tiff, tif, webp, heif, heic, svg`

---

### Bucket Management

#### `get_bucket_list()`
Lists all S3 buckets in the AWS account.

#### `create_bucket(bucket_name)`
Creates a new S3 bucket. Returns `True`/`False`.

#### `delete_bucket(bucket_name)`
Deletes an S3 bucket. Returns `True`/`False`.

#### `get_default_upload_folder(bucket_name)`
Returns the configured default folder for a bucket, or `"uploads"` as fallback.

---

## Custom Fields on File DocType

The app adds three custom fields to Frappe's standard `File` DocType via fixtures:

| Field Name                 | Type  | Label           | Description                          |
|----------------------------|-------|-----------------|--------------------------------------|
| `custom_s3_bucket_name`    | Data  | S3 Bucket Name  | Which S3 bucket the file resides in  |
| `custom_s3_key`            | Data  | s3_key          | The S3 object key (path)             |
| `custom_is_s3_uploaded`    | Check | Is S3 uploaded  | Whether the file has been uploaded to S3 |

**Fixture file:** `frappe_s3_integration/fixtures/custom_field.json`

---

## Hooks & Events

**Path:** `frappe_s3_integration/hooks.py`

### Document Events

```python
doc_events = {
    "File": {
        "on_trash": "frappe_s3_integration.s3_core.delete_file_from_s3",
    }
}
```

When a `File` document is deleted (trashed), the corresponding S3 object is automatically deleted.

### Fixtures

```python
fixtures = [
    {
        "dt": "Custom Field",
        "filters": [["module", "=", "Frappe S3 Integration"]]
    }
]
```

Installs the three custom fields on the `File` DocType during `bench migrate`.

---

## Scheduled Tasks

| Schedule           | Method                                                                                              | Purpose                            |
|--------------------|-----------------------------------------------------------------------------------------------------|------------------------------------|
| Every 2 minutes    | `frappe_s3_integration.frappe_s3_integration.image_optimization.optimization_scheduler.pending_optimization_logs` | Process pending image optimizations |
| Daily at midnight  | `frappe_s3_integration.frappe_s3_integration.process_scheduler.process_unuploaded_documents`          | Migrate unuploaded files to S3     |

### Image Optimization Scheduler

**Path:** `frappe_s3_integration/frappe_s3_integration/image_optimization/optimization_scheduler.py`

**Flow:**
1. `pending_optimization_logs()` runs every 2 minutes
2. Checks if optimization is enabled in `File Image Settings`
3. Fetches all `S3 Image Optimization Log` documents with `status = Pending`
4. For each log, calls `process_image_optimizations()`:
   - Downloads the image from S3
   - Calls `optimize_image()` to compress/resize
   - Re-uploads the optimized version to the same S3 key
   - Records before/after sizes
   - Updates log status to `Success` or `Failed`

**`optimize_image()` behavior:**
- Resizes images exceeding 2560x1440 (maintains aspect ratio)
- Compresses with the configured quality level
- Preserves EXIF data
- Handles animated GIFs (preserves animation)
- Skips SVGs (no optimization)
- Only saves the optimized version if it is smaller than the original

### File Migration Scheduler

**Path:** `frappe_s3_integration/frappe_s3_integration/process_scheduler.py`

**Flow:**
1. `process_unuploaded_documents()` runs daily at midnight
2. Finds `File` documents marked as S3 uploaded but missing an S3 key
3. For each file, calls `migrate_file_to_s3()`:
   - Reads the local file from disk
   - Uploads to the appropriate bucket (public or private based on `is_private` flag)
   - Updates the `File` document with S3 metadata (`file_url`, `custom_s3_key`, `custom_s3_bucket_name`)
   - Updates attached document field references if applicable
   - Deletes the local file copy

---

## Workflows

### File Upload Workflow

```
Application Code
       │
       ▼
create_file_and_upload_to_s3(doctype, docname, file, is_public_bucket, folder)
       │
       ├──▶ upload_file_to_public_bucket()  ── ACL: public-read
       │    or
       ├──▶ upload_file_to_private_bucket() ── no public ACL
       │
       ▼
upload_file_to_bucket()
       │
       ├── Generate UUID filename: {uuid}.{ext}
       ├── Construct key: {default_folder}/{folder}/{uuid}.{ext}
       ├── Upload via boto3 connection.upload_fileobj()
       │
       ▼
Create File Document
       │
       ├── Set is_private based on bucket type
       ├── Set custom_s3_key = S3 key
       ├── Set custom_s3_bucket_name = bucket
       ├── Set custom_is_s3_uploaded = 1
       ├── Set file_url = proxy URL (/api/method/...serve_file?file_id=...)
       │
       ▼
Return (proxy_url, file_name)
```

### File Download Workflow

```
Application Code
       │
       ▼
get_pre_signed_url(file_name)
       │
       ├── Load File document
       ├── Read custom_s3_key and custom_s3_bucket_name
       │
       ▼
generate_temporary_url(bucket, key, expires_in=3600)
       │
       ├── boto3 generate_presigned_url('get_object')
       ├── ResponseContentDisposition: inline
       │
       ▼
Return pre-signed URL (valid for 1 hour)
```

### File Deletion Workflow

```
User trashes File document
       │
       ▼
doc_events["File"]["on_trash"] triggered
       │
       ▼
delete_file_from_s3(doc, event)
       │
       ├── Check custom_is_s3_uploaded == 1
       ├── Error if S3 operations disabled
       │
       ▼
delete_file_from_bucket(key, bucket_name)
       │
       ▼
boto3 connection.delete_object(Bucket, Key)
```

### Image Optimization Workflow

```
Application creates S3 Image Optimization Log
       │ (status: Pending, with file list)
       │
       ▼
Scheduler (every 2 minutes)
       │
       ▼
pending_optimization_logs()
       │
       ├── Check File Image Settings → optimize_images_in_s3
       ├── Fetch all Pending logs
       │
       ▼
process_image_optimizations(log)
       │
       ├── Set status → Processing
       ├── For each file in optimisation_details:
       │    ├── Download from S3
       │    ├── optimize_image(content, content_type, quality)
       │    │    ├── Resize if > 2560x1440
       │    │    ├── Compress with quality setting
       │    │    └── Keep original if optimized is larger
       │    ├── Re-upload to same S3 key
       │    └── Record before/after sizes
       │
       ▼
Set status → Success / Failed
```

### File Migration Workflow (Daily)

```
Scheduler (daily at midnight)
       │
       ▼
process_unuploaded_documents()
       │
       ├── Find File docs with custom_is_s3_uploaded=1 but no custom_s3_key
       │
       ▼
migrate_file_to_s3(file_name)
       │
       ├── Load local file from disk
       ├── Upload to public/private bucket (based on is_private)
       ├── Update File doc (file_url=proxy URL, custom_s3_key, custom_s3_bucket_name)
       ├── Validate attached_to fields (check doc exists, field exists)
       ├── Update attached document field references with proxy URL
       └── Delete local file copy
```

---

## API Reference

### Whitelisted Methods

| Method | Endpoint | Description |
|--------|----------|-------------|
| `serve_file` | `frappe_s3_integration.s3_core.serve_file` | Proxy endpoint for S3 files (allow_guest) |
| `process_unuploaded_documents` | `frappe_s3_integration.frappe_s3_integration.process_scheduler.process_unuploaded_documents` | Manually trigger file migration to S3 (requires AWS S3 Settings read permission) |

**Usage from client:**
```javascript
frappe.call({
    method: 'frappe_s3_integration.frappe_s3_integration.process_scheduler.process_unuploaded_documents',
    callback: function(r) {
        console.log(r);
    }
});
```

### Programmatic API (Python)

```python
from frappe_s3_integration.s3_core import getS3Connection, get_proxy_url

conn = getS3Connection()

# Upload to public bucket
result = conn.upload_file_to_public_bucket(file_obj, folder="invoices")
# result = {"file_url": "https://...", "key": "uploads/invoices/uuid.pdf", "bucket_name": "my-bucket"}

# Upload to private bucket
result = conn.upload_file_to_private_bucket(file_obj, folder="confidential")

# Generate pre-signed URL for private file access (with permission check)
url = conn.get_pre_signed_url(file_name="FILE-00001")

# Generate pre-signed URL with specific content type
url = conn.get_pre_signed_url(file_name="FILE-00001", content_type="image/png")

# Upload and create File document in one call (returns proxy URL)
proxy_url, file_name = create_file_and_upload_to_s3(
    doctype="Sales Invoice",
    docname="SINV-00001",
    file=file_obj,
    is_public_bucket=True,
    folder="invoices"
)

# Generate proxy URL for an existing File doc
proxy_url = get_proxy_url("FILE-00001")

# Validate file size before upload
exceeded, max_size = conn.validate_file_size(file_obj, is_public=True)

# Delete file from S3
conn.delete_file_from_bucket(s3_key, bucket_name)

# Get raw file from S3
response = conn.get_file_from_bucket(s3_key, bucket_name)
content = response['Body'].read()

# List buckets
buckets = conn.get_bucket_list()
```

---

## Configuration Guide

### Step 1: Install the App

```bash
bench get-app frappe_s3_integration
bench --site your-site.localhost install-app frappe_s3_integration
bench --site your-site.localhost migrate
```

### Step 2: Configure AWS S3 Settings

Navigate to **AWS S3 Settings** in the Frappe desk and configure:

1. **AWS Key** — Your AWS Access Key ID
2. **AWS Secret** — Your AWS Secret Access Key
3. **Region** — AWS region (e.g., `ap-south-1`, `us-east-1`)

### Step 3: Add Bucket Configurations

In the **S3 Bucket Details** table, add at least one bucket:

| Field | Example Value | Notes |
|-------|---------------|-------|
| Bucket Name | `my-app-public` | Must already exist in AWS |
| Default Folder | `uploads` | Files are stored under this prefix |
| Default Public Bucket | ✓ | Check for public access |
| Default Private Bucket | ☐ | Leave unchecked if public |
| Max Image Size | `350` | KB |
| Max File Size | `5120` | KB |

Add a second bucket for private files if needed.

### Step 4: (Optional) Enable Image Optimization

Navigate to **File Image Settings** and:
1. Check **Optimize Images in S3**
2. Select a quality level (e.g., `80`)

The scheduler will automatically optimize pending images every 2 minutes.

### Step 5: Verify

Upload a file through your application and verify:
- The `File` document has `custom_is_s3_uploaded = 1`
- `custom_s3_key` and `custom_s3_bucket_name` are populated
- The `file_url` is a proxy URL: `/api/method/frappe_s3_integration.s3_core.serve_file?file_id=FILE-XXXX`
- Files are streamed through the server from S3 with proper permission checks
- Public files are accessible to guests, private files require read permission

---

## Key Design Decisions

1. **UUID-based filenames** — All uploaded files get a UUID filename to avoid naming collisions
2. **Dual-stack URLs** — Uses `s3.dualstack.{region}.amazonaws.com` for IPv4/IPv6 compatibility
3. **Per-site connection caching** — The `S3Connection` is cached per Frappe site to avoid repeated initialization
4. **Separate size limits for images vs files** — Each bucket can have different max sizes for images and general files
5. **Non-destructive optimization** — Optimized images are only saved if smaller than the original
6. **Graceful disable** — The `disable_s3_operations` flag pauses all S3 activity without losing configuration
7. **Proxy URLs** — `file_url` stores a proxy URL instead of direct S3 URLs to prevent infrastructure leakage and enforce Frappe permissions
8. **Public files use 302 redirect** — Avoids server bandwidth for public files while still hiding the S3 URL structure
9. **Private files stream through server** — Ensures files are never accessible without proper Frappe permissions

---

## S3 File Proxy Endpoint

### Problem

Previously, `file_url` stored direct S3 URLs like `https://bucket.s3.dualstack.region.amazonaws.com/key`. This caused:
- Private bucket files were inaccessible (S3 URL requires AWS auth)
- Public files leaked infrastructure details (bucket name, region, key structure)
- No Frappe permission enforcement — anyone with the URL could bypass file access controls

### Solution

All `file_url` values now store a proxy URL:
```
/api/method/frappe_s3_integration.s3_core.serve_file?file_id=FILE-XXXX
```

### `serve_file(file_id)` Endpoint

**Path:** `frappe_s3_integration.s3_core.serve_file`
**Decorator:** `@frappe.whitelist(allow_guest=True)`

Behavior:
- Streams file content from S3 through the server with correct `Content-Type`
- **Permission check:** Uses Frappe's native `has_permission(file_doc, "read")` which:
  - Returns `True` for public files (guest-accessible)
  - Enforces role/ownership/sharing permissions for private files
- **Not found:** Returns 404 if file doesn't exist or isn't an S3 file

### `get_proxy_url(file_id)` Helper

```python
from frappe_s3_integration.s3_core import get_proxy_url
url = get_proxy_url("FILE-00001")
# Returns: "/api/method/frappe_s3_integration.s3_core.serve_file?file_id=FILE-00001"
```

---

## Security Fixes

| # | Vulnerability | Fix |
|---|--------------|-----|
| 1 | `is_private` hardcoded to `0` in `create_file_and_upload_to_s3()` | Set based on `is_public_bucket` parameter |
| 2 | No permission check in `get_pre_signed_url()` | Added `has_permission(file_doc, "read")` check |
| 3 | `extra` dict in `generate_temporary_url()` allows arbitrary S3 API parameter injection | Removed `extra`, added safe `content_type` param |
| 4 | `@frappe.whitelist()` on `process_unuploaded_documents()` with no role check | Added `frappe.has_permission("AWS S3 Settings", "read")` |
| 5 | Path traversal via `folder` parameter in `upload_file_to_bucket()` | Sanitize: strip, remove `..`, remove `//` |
| 6 | Unvalidated `attached_to_doctype` in `migrate_file_to_s3()` | Check `frappe.db.exists()` and `meta.has_field()` before `set_value` |
| 7 | Custom fields editable by users via UI | Set `read_only: 1` and `hidden: 1` in fixtures |
| 8 | Direct S3 URLs leak bucket/key structure | Replaced with proxy URLs everywhere |

---

## Migration Patch

**Patch:** `frappe_s3_integration.patches.v1_0.migrate_s3_urls_to_proxy`

Run via `bench migrate --site your-site`.

### What it does:

1. Migrates FG Item Master catalogue images first (looks up File docs by their current S3 `file_url`, so this must run before step 2)
2. Finds all File documents with `custom_is_s3_uploaded=1` and a valid `custom_s3_key`
3. Replaces `file_url` with the proxy URL format
4. Updates `attached_to` document fields if they hold the old S3 URL
5. Skips files already migrated (checks for `serve_file` in URL)

### Consumer App Changes

**essdee_sales:** `upload_file_to_s3()` now sets `file_url` to proxy URL after File doc save.

**frappe_tools:** `load_scanned_document_details()` uses `content_type` parameter instead of `extra` dict when calling `get_pre_signed_url()`.
