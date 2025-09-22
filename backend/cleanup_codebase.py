#!/usr/bin/env python3
"""
Clean up unnecessary files from the codebase before AWS deployment
"""

import os
from pathlib import Path
import click


def get_files_to_delete():
    """Get list of files/directories to delete"""
    
    base_dir = Path(".")
    
    files_to_delete = [
        # CSV files (temporary exports and old data)
        "uncatalogued_urls.csv",
        "zero_brands_urls.csv", 
        "airtable_urls_export.csv",
        "airtable_export.csv",
        "sem-catalogue-verticals.csv",
        "sem-missing.csv",
        "sem-pages.csv",
        "export_db.py",
        
        # Debug scripts
        "app/tools/debug_url_comparison.py",
        "app/tools/debug_sync_results.py", 
        "app/tools/debug_airtable.py",
        
        # Export scripts (one-time use)
        "app/tools/export_airtable_urls.py",
        "app/tools/export_zero_brands.py",
        "app/tools/export_airtable.py",
        "app/tools/export_uncatalogued_urls.py",
        
        # Old/unused tools
        "app/tools/airtable_sync.py",  # replaced by airtable_sync_fixed.py
        "app/tools/airtable_sync_simple.py",  # debugging version
        "app/tools/fix_catalogued_flags.py",  # one-time migration
        "app/tools/add_catalogued_column.py",  # one-time migration
        "app/tools/fix_database_schema.py",  # one-time migration
        "app/tools/check_database.py",  # debugging tool
        "app/tools/check_app_database.py",  # debugging tool
        
        # Development database - KEEP for now
        # "data/dev.db",  # Keep current SQLite as backup
        "data/latest/",  # temporary data directory
    ]
    
    return [base_dir / f for f in files_to_delete if (base_dir / f).exists()]


def get_files_to_keep():
    """Essential files to keep"""
    return [
        "app/tools/migrate_to_mysql.py",
        "app/tools/airtable_sync_fixed.py", 
        "app/tools/process_uncatalogued.py",
        "cleanup_codebase.py",  # this file
    ]


@click.command()
@click.option("--dry-run", is_flag=True, help="Show what would be deleted without actually deleting")
@click.option("--confirm", is_flag=True, help="Skip confirmation prompt")
def main(dry_run: bool, confirm: bool):
    """Clean up unnecessary files from the codebase"""
    
    files_to_delete = get_files_to_delete()
    
    if not files_to_delete:
        print("✅ No files to delete - codebase is already clean!")
        return
        
    print("🧹 Codebase cleanup - Files to delete:")
    for file_path in files_to_delete:
        size = ""
        if file_path.is_file():
            size_bytes = file_path.stat().st_size
            if size_bytes > 1024:
                size = f" ({size_bytes // 1024}KB)"
            else:
                size = f" ({size_bytes}B)"
        elif file_path.is_dir():
            size = " (directory)"
            
        print(f"  🗑️  {file_path}{size}")
    
    total_files = len([f for f in files_to_delete if f.is_file()])
    total_dirs = len([f for f in files_to_delete if f.is_dir()])
    
    print(f"\n📊 Total: {total_files} files, {total_dirs} directories")
    
    if dry_run:
        print("🔍 DRY RUN - No files were deleted")
        return
        
    if not confirm:
        response = input("\nProceed with deletion? (y/N): ")
        if response.lower() != 'y':
            print("❌ Cancelled")
            return
    
    # Delete files and directories
    deleted_count = 0
    for file_path in files_to_delete:
        try:
            if file_path.is_file():
                file_path.unlink()
                deleted_count += 1
                print(f"✅ Deleted file: {file_path}")
            elif file_path.is_dir():
                import shutil
                shutil.rmtree(file_path)
                deleted_count += 1
                print(f"✅ Deleted directory: {file_path}")
        except Exception as e:
            print(f"❌ Failed to delete {file_path}: {e}")
    
    print(f"\n🎉 Cleanup complete! Deleted {deleted_count} items")
    print("📦 Codebase is now ready for AWS deployment")


if __name__ == "__main__":
    main()
