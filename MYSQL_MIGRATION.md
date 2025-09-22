# MySQL Migration Guide

This guide will help you migrate from SQLite to MySQL for AWS deployment.

## Prerequisites

- Docker and Docker Compose installed
- Python virtual environment activated

## Step 1: Start Local MySQL

```bash
# Start MySQL and phpMyAdmin
docker-compose up -d

# Check that containers are running
docker-compose ps
```

- MySQL will be available at `localhost:3306`
- phpMyAdmin will be available at `http://localhost:8080`
  - Username: `root`
  - Password: `root_password`

## Step 2: Run Migration

```bash
# Activate virtual environment
.venv\Scripts\Activate.ps1

# Preview what will be migrated (dry run)
cd backend
python app/tools/migrate_to_mysql.py --dry-run

# Run the actual migration
python app/tools/migrate_to_mysql.py --mysql-user ace --mysql-password ace_pw

# Or with root user
python app/tools/migrate_to_mysql.py --mysql-user root --mysql-password root_password
```

## Step 3: Update Environment

The application will automatically use MySQL once the migration is complete. The DATABASE_URL is already configured for:

```
mysql+pymysql://ace:ace_pw@localhost:3306/ace_sem
```

## Step 4: Test Application

```bash
# Start the backend
cd backend
uvicorn app.main:app --host 0.0.0.0 --port 8000 --reload

# Start the frontend (in another terminal)
cd frontend
npm run dev
```

Visit `http://localhost:3000` to verify everything works.

## Step 5: Clean Up Codebase

```bash
# Preview cleanup (dry run)
cd backend
python cleanup_codebase.py --dry-run

# Run cleanup
python cleanup_codebase.py --confirm
```

## AWS RDS Configuration

For AWS deployment, you'll need to:

1. Create an RDS MySQL instance
2. Update DATABASE_URL environment variable:
   ```
   mysql+pymysql://username:password@rds-endpoint:3306/database_name
   ```
3. Ensure security groups allow connection from your application

## Verification

After migration, verify:

- [ ] All data migrated correctly
- [ ] Application starts without errors
- [ ] Frontend loads and displays data
- [ ] Airtable sync still works
- [ ] LLM cataloguing still works
- [ ] All API endpoints respond correctly

## Rollback

If you need to rollback to SQLite:

1. Update `database_url` in `config.py` back to SQLite path
2. Restore your SQLite database file
3. Restart the application

## Files Structure After Cleanup

Essential files that will remain:
- `app/tools/migrate_to_mysql.py` - Migration script
- `app/tools/airtable_sync_fixed.py` - Airtable synchronization  
- `app/tools/process_uncatalogued.py` - LLM cataloguing
- Core application files

Files that will be removed:
- All CSV exports and temporary data
- Debug scripts
- One-time migration tools
- Development SQLite database
