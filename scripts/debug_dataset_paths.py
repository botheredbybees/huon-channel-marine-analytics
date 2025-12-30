#!/usr/bin/env python3
"""
Debug script to examine what dataset_path values are actually in the database.

Usage:
    export DB_PASSWORD=your_password
    python scripts/debug_dataset_paths.py
"""

import os
import sys
import psycopg2

db_config = {
    'host': os.getenv('DB_HOST', 'localhost'),
    'port': int(os.getenv('DB_PORT', 5433)),
    'database': os.getenv('DB_NAME', 'marine_db'),
    'user': os.getenv('DB_USER', 'marine_user'),
    'password': os.getenv('DB_PASSWORD'),
}

if not db_config['password']:
    print("ERROR: DB_PASSWORD environment variable not set")
    sys.exit(1)

try:
    conn = psycopg2.connect(**db_config)
    cursor = conn.cursor()
    
    print("\n" + "=" * 100)
    print("DATASET PATHS IN DATABASE")
    print("=" * 100)
    
    cursor.execute("""
        SELECT id, uuid, title, dataset_path 
        FROM metadata 
        ORDER BY id
    """)
    
    for row in cursor.fetchall():
        record_id, uuid, title, path = row
        title_short = (title[:50] + '...') if title and len(title) > 50 else title
        
        print(f"\nID: {record_id}")
        print(f"  UUID:  {uuid}")
        print(f"  Title: {title_short}")
        print(f"  Path:  {path}")
        
        if path:
            # Check if file/directory exists
            from pathlib import Path
            p = Path(path)
            if p.exists():
                print(f"  ✓ Path exists")
                if p.is_dir():
                    print(f"    └─ Is directory")
                    xml_path = p / 'metadata.xml'
                    if xml_path.exists():
                        print(f"    └─ metadata.xml found")
                    else:
                        print(f"    ✗ metadata.xml NOT found")
                        # List what's in the directory
                        try:
                            items = list(p.iterdir())[:5]
                            if items:
                                print(f"    └─ Directory contains: {', '.join(i.name for i in items)}")
                        except:
                            pass
                else:
                    print(f"    └─ Is file")
            else:
                print(f"  ✗ Path does NOT exist")
    
    cursor.close()
    conn.close()
    
    print("\n" + "=" * 100)
    
except Exception as e:
    print(f"ERROR: {e}")
    sys.exit(1)
