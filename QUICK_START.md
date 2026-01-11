# Quick Start Guide

Get up and running with the ETL pipeline in 5 minutes.

## Prerequisites Check

Before starting, make sure you have:

- ✅ Python 3.12 installed (`python --version`)
- ✅ Akamai Object Storage account
- ✅ Access to Akamai Cloud Manager

## Step 1: Get Your Akamai Credentials (5-10 minutes)

**👉 Follow the detailed guide: [AKAMAI_SETUP_GUIDE.md](./AKAMAI_SETUP_GUIDE.md)**

You need these 5 values:
1. Access Key
2. Secret Key
3. Endpoint URL
4. Bucket Name
5. Region

## Step 2: Install Dependencies (1 minute)

```bash
# Create and activate virtual environment
python -m venv venv

# Windows
venv\Scripts\activate

# Linux/Mac
source venv/bin/activate

# Install packages
pip install -r requirements.txt
```

## Step 3: Configure Environment (2 minutes)

```bash
# Copy example env file
cp .env.example .env

# Edit .env with your credentials
# Use your favorite text editor
notepad .env  # Windows
# or
nano .env     # Linux/Mac
```

Fill in your `.env` file:
```env
ENVIRONMENT=local
DRY_RUN=true
LOG_LEVEL=INFO
AKAMAI_ACCESS_KEY=your_access_key_here
AKAMAI_SECRET_KEY=your_secret_key_here
AKAMAI_ENDPOINT=https://us-east-1.linodeobjects.com
AKAMAI_BUCKET_NAME=your-bucket-name
AKAMAI_REGION=us-east-1
```

## Step 4: Test Connection (1 minute)

```bash
# Test in dry-run mode (safe, no actual uploads)
python example_usage.py
```

Expected output:
```
2024-01-15 14:30:45 | config.config_loader | INFO | Environment: local
2024-01-15 14:30:45 | config.config_loader | INFO | Dry-run mode: True
2024-01-15 14:30:46 | storage.akamai_client | INFO | Storage client initialized in DRY-RUN mode - no actual uploads will occur
2024-01-15 14:30:46 | storage.akamai_client | INFO | [DRY-RUN] Would upload to: s3://your-bucket-name/raw/controller/2024-01-15/listing_12345.json
```

✅ **Success!** If you see output like above, your configuration is working.

## Step 5: Test Real Upload (Optional)

Once dry-run works, test a real upload:

1. Edit `.env`: Change `DRY_RUN=false`
2. Run again: `python example_usage.py`
3. Check your Akamai bucket to verify files were uploaded

## Common Issues

### "Failed to verify bucket access"
- ✅ Check your Access Key has Read/Write permissions
- ✅ Verify bucket name is correct (case-sensitive)

### "Invalid endpoint" or "Connection error"
- ✅ Check endpoint URL is correct
- ✅ Verify region matches your bucket region

### "Invalid credentials"
- ✅ Double-check Access Key and Secret Key (no extra spaces)
- ✅ Ensure keys haven't been deleted/rotated

### Module not found errors
- ✅ Make sure virtual environment is activated
- ✅ Run `pip install -r requirements.txt` again

## Next Steps

Now that you're set up, you can:

1. **Start building scrapers** using the storage client
2. **Review the code examples** in `example_usage.py`
3. **Read the full documentation** in `README.md`
4. **Check the detailed Akamai guide** in `AKAMAI_SETUP_GUIDE.md`

## Need Help?

- 📖 Read [AKAMAI_SETUP_GUIDE.md](./AKAMAI_SETUP_GUIDE.md) for detailed credential setup
- 📖 Read [README.md](./README.md) for full documentation
- 🐛 Check error messages in logs for specific issues

---

**Time to complete**: ~10-15 minutes (mostly getting Akamai credentials)

**Ready? Let's go!** 🚀
