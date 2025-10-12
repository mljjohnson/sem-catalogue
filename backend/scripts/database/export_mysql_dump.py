"""
Export MySQL database to SQL dump file
"""
import pymysql
import json
from datetime import date

def export_database_to_sql():
    """Export MySQL database to SQL dump file"""
    try:
        print("=== MYSQL DATABASE EXPORT ===")
        conn = pymysql.connect(
            host='localhost', 
            port=3307, 
            user='ace', 
            password='ace_pw', 
            database='ace_sem',
            charset='utf8mb4'
        )
        cursor = conn.cursor()

        # Get MySQL connection info
        cursor.execute("SELECT DATABASE(), USER(), @@hostname, @@port")
        db_info = cursor.fetchone()
        print(f"Exporting from:")
        print(f"  Database: {db_info[0]}")
        print(f"  User: {db_info[1]}")
        print(f"  Host: {db_info[2]}")
        print(f"  Port: {db_info[3]}")

        # Get all tables
        cursor.execute('SHOW TABLES')
        tables = [row[0] for row in cursor.fetchall()]
        print(f"\nFound tables: {tables}")

        with open('database_dump.sql', 'w', encoding='utf-8') as f:
            f.write("-- MySQL Database Dump\n")
            f.write("-- Generated automatically\n")
            f.write(f"-- Database: {db_info[0]}\n")
            f.write(f"-- Host: {db_info[2]}:{db_info[3]}\n\n")
            f.write("SET FOREIGN_KEY_CHECKS = 0;\n")
            f.write("SET SQL_MODE = 'NO_AUTO_VALUE_ON_ZERO';\n")
            f.write("SET AUTOCOMMIT = 0;\n")
            f.write("START TRANSACTION;\n\n")
            
            for table in tables:
                print(f"Exporting table: {table}")
                
                # Get table structure
                cursor.execute(f'SHOW CREATE TABLE {table}')
                create_table = cursor.fetchone()[1]
                f.write(f"-- Table structure for {table}\n")
                f.write(f"DROP TABLE IF EXISTS `{table}`;\n")
                f.write(f"{create_table};\n\n")
                
                # Get table data
                cursor.execute(f"SELECT * FROM {table}")
                rows = cursor.fetchall()
                
                if rows:
                    # Get column info
                    cursor.execute(f"DESCRIBE {table}")
                    columns = [col[0] for col in cursor.fetchall()]
                    
                    f.write(f"-- Data for table {table}\n")
                    f.write("LOCK TABLES `{}` WRITE;\n".format(table))
                    f.write(f"INSERT INTO `{table}` ({', '.join([f'`{col}`' for col in columns])}) VALUES\n")
                    
                    for i, row in enumerate(rows):
                        values = []
                        for val in row:
                            if val is None:
                                values.append('NULL')
                            elif isinstance(val, str):
                                # Escape single quotes and backslashes
                                escaped = val.replace('\\', '\\\\').replace("'", "\\'")
                                values.append(f"'{escaped}'")
                            elif isinstance(val, (list, dict)):
                                # Handle JSON columns
                                json_str = json.dumps(val).replace('\\', '\\\\').replace("'", "\\'")
                                values.append(f"'{json_str}'")
                            elif isinstance(val, date):
                                values.append(f"'{val}'")
                            elif isinstance(val, bool):
                                values.append('1' if val else '0')
                            else:
                                values.append(str(val))
                        
                        if i == len(rows) - 1:
                            f.write(f"({', '.join(values)});\n")
                        else:
                            f.write(f"({', '.join(values)}),\n")
                    
                    f.write("UNLOCK TABLES;\n\n")
                else:
                    f.write(f"-- No data for table {table}\n\n")
            
            f.write("COMMIT;\n")
            f.write("SET FOREIGN_KEY_CHECKS = 1;\n")

        # Show summary
        import os
        file_size = os.path.getsize('database_dump.sql')
        print(f"\n✅ Database exported to database_dump.sql")
        print(f"File size: {file_size:,} bytes ({file_size/1024/1024:.1f} MB)")
        
        # Show record counts
        print(f"\nRecord counts:")
        for table in tables:
            cursor.execute(f"SELECT COUNT(*) FROM {table}")
            count = cursor.fetchone()[0]
            print(f"  {table}: {count:,} records")
            
        cursor.close()
        conn.close()
        
        print(f"\n🎉 Export complete!")
        
    except Exception as e:
        print(f"\n❌ Export failed: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    export_database_to_sql()



