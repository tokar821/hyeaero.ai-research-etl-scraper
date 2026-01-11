# Akamai Object Storage Setup Guide

Complete guide for obtaining and configuring Akamai Object Storage credentials for the ETL pipeline.

## Required Information

To use Akamai Object Storage with this ETL pipeline, you need **5 pieces of information**:

1. **Access Key** (also called Access Key ID)
2. **Secret Key** (also called Secret Access Key)
3. **Endpoint URL** (S3-compatible API endpoint)
4. **Bucket Name** (your storage bucket name)
5. **Region** (optional, but recommended)

---

## Step-by-Step Guide to Get Your Credentials

### Step 1: Log in to Akamai Cloud Manager

1. Go to [Akamai Cloud Manager](https://control.akamai.com) (or your Akamai control panel)
2. Log in with your Akamai account credentials
3. Navigate to **Object Storage** or **Cloud Storage** section in the sidebar

**Note**: If you're using Akamai's Linode-based object storage (Akamai acquired Linode), you may need to access it through:
- [Linode Cloud Manager](https://cloud.linode.com)
- Or through the Akamai Object Storage interface

### Step 2: Create an Access Key

1. In the Object Storage section, find and click on **Access Keys** (or **API Keys**)
2. Click the **Create Access Key** button (or **Generate New Key**)
3. Fill in the form:
   - **Label/Name**: Give it a descriptive name (e.g., "HyeAero-ETL-Pipeline")
   - **Region**: Select the region where your bucket is located (e.g., `us-east-1`, `us-southeast-1`, `eu-central-1`)
   - **Permissions**: Select permissions needed:
     - **Read** and **Write** for bucket access
     - Or **Full Access** if needed (be cautious with this)
4. Click **Create Access Key** or **Generate**

### Step 3: Save Your Credentials

⚠️ **CRITICAL**: The secret key is **only shown once** when created. Save it immediately!

You'll see a dialog with:
- **Access Key** (or Access Key ID): `AKIAIOSFODNN7EXAMPLE`
- **Secret Key** (or Secret Access Key): `wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY`

**Action Required**:
- Copy both keys immediately
- Store them securely (password manager, encrypted file, etc.)
- **You cannot retrieve the secret key again** after closing this dialog

### Step 4: Get Your Endpoint URL

The endpoint URL depends on your region. Here are common formats:

**Akamai/Linode Object Storage endpoints by region:**

| Region | Endpoint Format | Example |
|--------|----------------|---------|
| US East (Atlanta) | `https://us-southeast-1.linodeobjects.com` | `https://us-southeast-1.linodeobjects.com` |
| US East (Newark) | `https://us-east-1.linodeobjects.com` | `https://us-east-1.linodeobjects.com` |
| US West (Fremont) | `https://us-west-1.linodeobjects.com` | `https://us-west-1.linodeobjects.com` |
| EU Central (Frankfurt) | `https://eu-central-1.linodeobjects.com` | `https://eu-central-1.linodeobjects.com` |
| EU West (London) | `https://eu-west-1.linodeobjects.com` | `https://eu-west-1.linodeobjects.com` |
| AP South (Singapore) | `https://ap-south-1.linodeobjects.com` | `https://ap-south-1.linodeobjects.com` |

**How to find your endpoint:**
1. Go to **Object Storage** → **Buckets** in Cloud Manager
2. Click on your bucket (or create one if you don't have one)
3. Look for **Endpoint** or **S3 Endpoint** in the bucket details
4. It will typically show: `[region].linodeobjects.com` or similar

**If using pure Akamai NetStorage (legacy):**
- Endpoint format: `https://[hostname].ns.akamai.com`
- Contact your Akamai account manager for the specific endpoint

### Step 5: Get Your Bucket Name

1. Go to **Object Storage** → **Buckets** in Cloud Manager
2. You'll see a list of your buckets
3. If you don't have a bucket yet:
   - Click **Create Bucket**
   - Choose a unique name (e.g., `hyeaero-etl-data`)
   - Select the same region where your access key was created
   - Click **Create**
4. Note down the **exact bucket name** (case-sensitive)

### Step 6: Get Your Region

The region identifier is typically:
- Part of your endpoint URL (e.g., `us-southeast-1` from `us-southeast-1.linodeobjects.com`)
- Or shown in your bucket details
- Common regions: `us-east-1`, `us-west-1`, `eu-central-1`, `ap-south-1`

---

## Alternative: If You're Using Pure Akamai NetStorage

If you're using Akamai's legacy NetStorage (not S3-compatible Object Storage), you may need:

1. **CP Code** (Content Provider Code)
2. **Upload Hostname** (e.g., `[cpcode].upload.akamai.com`)
3. **Download Hostname** (e.g., `[cpcode].download.akamai.com`)
4. **Upload Key** and **Upload Directory**
5. Contact your Akamai account manager for S3-compatible API access or migration

**Note**: The ETL pipeline is built for S3-compatible API. If you only have NetStorage, you may need to:
- Request S3-compatible API access from Akamai
- Or migrate to Akamai Object Storage (Linode-based)

---

## Configuration Example

Once you have all the information, create your `.env` file:

```bash
# Copy the example file
cp .env.example .env
```

Edit `.env` with your actual values:

```env
# Environment: dev, prod, or local (local enables dry-run mode)
ENVIRONMENT=local

# Dry-run mode (true/false). When true, no actual uploads to storage occur.
DRY_RUN=true

# Logging level: DEBUG, INFO, WARNING, ERROR, CRITICAL
LOG_LEVEL=INFO

# Akamai Object Storage Configuration
AKAMAI_ACCESS_KEY=AKIAIOSFODNN7EXAMPLE           # ← Your access key from Step 3
AKAMAI_SECRET_KEY=wJalrXUtnFEMI/K7MDENG/...      # ← Your secret key from Step 3
AKAMAI_ENDPOINT=https://us-east-1.linodeobjects.com  # ← Your endpoint from Step 4
AKAMAI_BUCKET_NAME=hyeaero-etl-data              # ← Your bucket name from Step 5
AKAMAI_REGION=us-east-1                          # ← Your region from Step 6
```

---

## Testing Your Configuration

After setting up your `.env` file, test the connection:

```bash
# Install dependencies (if not already done)
pip install -r requirements.txt

# Run the example script in dry-run mode first
python example_usage.py
```

You should see output like:
```
2024-01-15 14:30:45 | config.config_loader | INFO | Environment: local
2024-01-15 14:30:45 | config.config_loader | INFO | Dry-run mode: True
2024-01-15 14:30:46 | storage.akamai_client | INFO | [DRY-RUN] Would upload to: s3://hyeaero-etl-data/raw/controller/2024-01-15/listing_12345.json
```

Once verified in dry-run mode, set `DRY_RUN=false` and test actual uploads.

---

## Troubleshooting

### Error: "Failed to verify bucket access"
- **Check**: Your access key has proper permissions for the bucket
- **Check**: Bucket name is correct (case-sensitive)
- **Check**: Region matches between access key and bucket

### Error: "Connection error" or "Invalid endpoint"
- **Check**: Endpoint URL is correct and accessible
- **Check**: Region matches the endpoint region
- **Check**: No typos in the endpoint URL

### Error: "Invalid credentials"
- **Check**: Access key and secret key are correct (no extra spaces)
- **Check**: Keys haven't been rotated/deleted
- **Check**: You're using the correct region for the access key

### Error: "Access Denied"
- **Check**: Access key has Read/Write permissions for the bucket
- **Check**: Bucket permissions allow your access key
- **Check**: No IP restrictions on your access key

---

## Security Best Practices

1. ✅ **Never commit `.env` file to git** (already in `.gitignore`)
2. ✅ **Use separate access keys for dev/prod environments**
3. ✅ **Rotate access keys regularly** (every 90 days recommended)
4. ✅ **Use least-privilege permissions** (only Read/Write, not Full Access)
5. ✅ **Store secret keys in secure password managers**
6. ✅ **Use environment-specific buckets** (e.g., `hyeaero-dev`, `hyeaero-prod`)

---

## Quick Reference Checklist

Before starting, ensure you have:

- [ ] Access Key (from Step 3)
- [ ] Secret Key (from Step 3) - **Saved securely!**
- [ ] Endpoint URL (from Step 4)
- [ ] Bucket Name (from Step 5)
- [ ] Region (from Step 6)
- [ ] `.env` file created with all values
- [ ] Tested connection in dry-run mode

---

## Need Help?

If you're unable to find these settings in your Akamai account:

1. **Check your Akamai service plan**: Not all plans include Object Storage
2. **Contact Akamai Support**: They can guide you to the right interface
3. **Contact your Akamai Account Manager**: They can enable Object Storage if needed
4. **Check Akamai Documentation**: 
   - [Akamai Cloud Computing Docs](https://techdocs.akamai.com/cloud-computing/)
   - [Linode Object Storage Docs](https://www.linode.com/docs/platform/object-storage/)

---

## Summary

You need these **5 values** to configure the ETL pipeline:

| Variable | Where to Find | Example |
|----------|---------------|---------|
| `AKAMAI_ACCESS_KEY` | Access Keys section → Create Access Key | `AKIAIOSFODNN7EXAMPLE` |
| `AKAMAI_SECRET_KEY` | Shown once when creating access key | `wJalrXUtnFEMI/K7MDENG/...` |
| `AKAMAI_ENDPOINT` | Bucket details or region endpoint | `https://us-east-1.linodeobjects.com` |
| `AKAMAI_BUCKET_NAME` | Buckets section → Your bucket name | `hyeaero-etl-data` |
| `AKAMAI_REGION` | Same as endpoint region or bucket region | `us-east-1` |

Once you have all 5, update your `.env` file and you're ready to go! 🚀
