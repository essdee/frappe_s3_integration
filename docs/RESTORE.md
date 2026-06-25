# Restoring S3 files from a local backup archive

The nightly job (`backup_s3_buckets`, 1 AM) writes one compressed archive per bucket to:

    <site>/private/backups/s3/<bucket>-<timestamp>.tar.gz

(or the **Backup Directory** configured in *AWS S3 Settings*). The newest
`Backup Retention` archives per bucket are kept (default 7); older ones are pruned.

This local archive is the **second copy** of everything — the live S3 buckets are the
first. If S3 is lost, restore from the archive; if the server is lost, the files are
still in S3.

## Restore steps

1. **Extract** the archive (creates `./<bucket>/...` preserving the key structure):

   ```bash
   tar xzf <bucket>-<timestamp>.tar.gz
   ```

2. **Sync into a bucket** (new or existing) with the AWS CLI:

   ```bash
   # public bucket — keep objects publicly readable
   aws s3 sync ./<bucket> s3://<new-bucket> --acl public-read

   # private bucket
   aws s3 sync ./<bucket> s3://<new-bucket>
   ```

3. **If the bucket NAME changed**, re-point existing File docs and the settings:

   ```python
   # bench --site <site> console
   frappe.db.sql("update `tabFile` set custom_s3_bucket_name=%s where custom_s3_bucket_name=%s",
                 ("<new-bucket>", "<old-bucket>"))
   frappe.db.commit()
   ```

   Then open **AWS S3 Settings → S3 Bucket Details**, update `bucket_name` and the
   public/private default flags, and save (this clears the cached connection).

4. **Re-tag content-types.** The archive does not preserve S3 ContentType metadata, so
   after syncing into a NEW bucket run the backfill so public objects don't serve as
   `binary/octet-stream`:

   ```python
   from frappe_s3_integration.frappe_s3_integration.backfill import backfill_content_types
   backfill_content_types(dry_run=True)   # preview
   backfill_content_types(dry_run=False)  # apply
   ```

## Notes

- The backup job is **read-only** on the buckets — it only lists + downloads.
- Restore is intentionally manual (AWS CLI), so a bad automated job can never overwrite
  good live data.
