#!/usr/bin/env python3
"""
Validate and fix known data quality issues in the measurements table.
Implements corrections for:
  - Parameter code misidentification (e.g., 'ph'/'PH' as phosphate vs pH)
  - Unit conversion issues (e.g., wind_speed in cm/s vs m/s)
  - Negative pressure values from atmospheric offset
  - Outlier values requiring flagging

This script is designed to be run AFTER metadata enrichment scripts.
It includes validation before and after corrections.

Usage:
    python validate_and_fix_data_issues.py [--dry-run]

Environment variables:
    DB_HOST: Database host (default: localhost)
    DB_PORT: Database port (default: 5433)
    DB_NAME: Database name (default: marine_db)
    DB_USER: Database user (default: marine_user)
    DB_PASSWORD: Database password (required for authentication)
    DRY_RUN: If set to 1, run in dry-run mode (no database changes)
"""

import os
import psycopg2
import logging
import sys
from datetime import datetime

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class DataValidator:
    """Validate and fix known data quality issues."""
    
    def __init__(self, db_config: dict, dry_run: bool = False):
        self.db_config = db_config
        self.dry_run = dry_run
        self.conn = None
        self.stats = {
            'phosphate_fixed': 0,
            'wind_speed_fixed': 0,
            'pressure_flagged': 0,
            'silicate_flagged': 0,
            'issues_detected': 0,
            'issues_corrected': 0,
        }
    
    def connect(self):
        """Connect to PostgreSQL database."""
        try:
            self.conn = psycopg2.connect(**self.db_config)
            logger.info(f"Connected to {self.db_config['database']} at {self.db_config['host']}:{self.db_config['port']}")
            if self.dry_run:
                logger.warning("*** DRY RUN MODE - No database changes will be committed ***")
        except psycopg2.OperationalError as e:
            logger.error(f"Failed to connect to database: {e}")
            logger.error("Check your environment variables: DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASSWORD")
            sys.exit(1)
    
    def disconnect(self):
        """Disconnect from database."""
        if self.conn:
            self.conn.close()
            logger.info("Disconnected from database")
    
    def execute_fix(self, description: str, query: str, params: tuple = ()) -> int:
        """Execute a fix query and return rows affected."""
        cursor = self.conn.cursor()
        rows_affected = 0
        
        try:
            cursor.execute(query, params)
            rows_affected = cursor.rowcount
            
            if self.dry_run:
                self.conn.rollback()
                logger.info(f"[DRY RUN] {description}: {rows_affected} rows would be affected")
            else:
                self.conn.commit()
                logger.info(f"✓ {description}: {rows_affected} rows affected")
                self.stats['issues_corrected'] += 1
            
            return rows_affected
            
        except psycopg2.Error as e:
            logger.error(f"Database error during '{description}': {e}")
            self.conn.rollback()
            return 0
        except Exception as e:
            logger.error(f"Unexpected error during '{description}': {e}")
            self.conn.rollback()
            return 0
        finally:
            cursor.close()
    
    def validate_phosphate_issue(self) -> bool:
        """Validate that phosphate parameter misidentification exists."""
        cursor = self.conn.cursor()
        
        try:
            query = """
                SELECT COUNT(*) 
                FROM measurements 
                WHERE parameter_code IN ('ph', 'PH')
                  AND value BETWEEN 0.0 AND 33.0
                  AND metadata_id IN (11, 12, 16, 17, 24, 27, 30, 34)
            """
            cursor.execute(query)
            count = cursor.fetchone()[0]
            
            if count > 0:
                logger.info(f"Found {count} potentially misidentified phosphate values")
                self.stats['issues_detected'] += 1
                return True
            return False
        finally:
            cursor.close()
    
    def fix_phosphate_parameters(self) -> int:
        """Rename 'ph'/'PH' parameters to 'PHOSPHATE' where value indicates phosphate."""
        query = """
            UPDATE measurements 
            SET parameter_code = 'PHOSPHATE'
            WHERE parameter_code IN ('ph', 'PH')
              AND value BETWEEN 0.0 AND 33.0
              AND metadata_id IN (11, 12, 16, 17, 24, 27, 30, 34)
        """
        rows = self.execute_fix("Fix phosphate parameter misidentification", query)
        self.stats['phosphate_fixed'] = rows
        return rows
    
    def validate_wind_speed_issue(self) -> bool:
        """Validate that wind_speed unit conversion is needed."""
        cursor = self.conn.cursor()
        
        try:
            query = """
                SELECT COUNT(*), MAX(value) 
                FROM measurements 
                WHERE parameter_code = 'wind_speed'
                  AND metadata_id = 11
                  AND value > 50
            """
            cursor.execute(query)
            result = cursor.fetchone()
            count, max_val = result if result else (0, None)
            
            if count and count > 0:
                logger.info(f"Found {count} wind_speed values > 50 (max={max_val}) - likely cm/s not m/s")
                self.stats['issues_detected'] += 1
                return True
            return False
        finally:
            cursor.close()
    
    def fix_wind_speed_units(self) -> int:
        """Convert wind_speed from cm/s to m/s by dividing by 100."""
        query = """
            UPDATE measurements
            SET value = value / 100,
                units = 'm/s'
            WHERE parameter_code = 'wind_speed'
              AND metadata_id = 11
              AND value > 50
        """
        rows = self.execute_fix("Fix wind_speed unit conversion (cm/s to m/s)", query)
        self.stats['wind_speed_fixed'] = rows
        return rows
    
    def validate_pressure_issue(self) -> bool:
        """Validate that negative pressure values exist."""
        cursor = self.conn.cursor()
        
        try:
            query = """
                SELECT COUNT(*) 
                FROM measurements 
                WHERE parameter_code IN ('PRES', 'pressure', 'PRESSURE')
                  AND value < 0
            """
            cursor.execute(query)
            count = cursor.fetchone()[0]
            
            if count > 0:
                logger.info(f"Found {count} negative pressure values (likely atmospheric offset)")
                self.stats['issues_detected'] += 1
                return True
            return False
        finally:
            cursor.close()
    
    def flag_negative_pressure_values(self) -> int:
        """Flag negative pressure values as questionable."""
        query = """
            UPDATE measurements
            SET quality_flag = 2
            WHERE parameter_code IN ('PRES', 'pressure', 'PRESSURE')
              AND value < 0
              AND (quality_flag IS NULL OR quality_flag = 1)
        """
        rows = self.execute_fix("Flag negative pressure values (quality=2)", query)
        
        # Also add comment to surface measurements
        if rows > 0:
            comment_query = """
                UPDATE measurements
                SET comments = CONCAT(COALESCE(comments, ''), ' | Negative value: atmospheric offset applied')
                WHERE parameter_code IN ('PRES', 'pressure', 'PRESSURE')
                  AND value < 0
                  AND depth < 1
            """
            self.execute_fix("Add comments to atmospheric offset pressures", comment_query)
        
        self.stats['pressure_flagged'] = rows
        return rows
    
    def validate_silicate_issue(self) -> bool:
        """Validate that extreme silicate values exist."""
        cursor = self.conn.cursor()
        
        try:
            query = """
                SELECT COUNT(*), MAX(value)
                FROM measurements 
                WHERE parameter_code IN ('SIO4', 'silicate', 'SILICATE')
                  AND value > 500
            """
            cursor.execute(query)
            result = cursor.fetchone()
            count, max_val = result if result else (0, None)
            
            if count and count > 0:
                logger.info(f"Found {count} silicate values > 500 (max={max_val}) - outliers")
                self.stats['issues_detected'] += 1
                return True
            return False
        finally:
            cursor.close()
    
    def flag_silicate_outliers(self) -> int:
        """Flag extreme silicate values as bad data."""
        query = """
            UPDATE measurements
            SET quality_flag = 3
            WHERE parameter_code IN ('SIO4', 'silicate', 'SILICATE')
              AND value > 500
        """
        rows = self.execute_fix("Flag silicate outliers (quality=3)", query)
        self.stats['silicate_flagged'] = rows
        return rows
    
    def run_validation(self):
        """Run validation workflow."""
        logger.info("Starting data validation and correction")
        self.connect()
        
        try:
            logger.info("\n" + "=" * 70)
            logger.info("VALIDATION PHASE: Detecting known data quality issues")
            logger.info("=" * 70)
            
            issues = []
            
            if self.validate_phosphate_issue():
                issues.append("phosphate_misidentification")
            
            if self.validate_wind_speed_issue():
                issues.append("wind_speed_unit_conversion")
            
            if self.validate_pressure_issue():
                issues.append("negative_pressure_values")
            
            if self.validate_silicate_issue():
                issues.append("silicate_outliers")
            
            if not issues:
                logger.info("✓ No validation issues detected")
                logger.info("Database appears to be clean")
                self.disconnect()
                return
            
            logger.info(f"\nDetected {len(issues)} issue type(s):")
            for issue in issues:
                logger.info(f"  - {issue}")
            
            logger.info("\n" + "=" * 70)
            logger.info("CORRECTION PHASE: Applying fixes")
            logger.info("=" * 70)
            
            if "phosphate_misidentification" in issues:
                self.fix_phosphate_parameters()
            
            if "wind_speed_unit_conversion" in issues:
                self.fix_wind_speed_units()
            
            if "negative_pressure_values" in issues:
                self.flag_negative_pressure_values()
            
            if "silicate_outliers" in issues:
                self.flag_silicate_outliers()
        
        except Exception as e:
            logger.error(f"Fatal error during validation: {e}")
        finally:
            self.disconnect()
            self._print_summary()
    
    def _print_summary(self):
        """Print validation and correction summary."""
        logger.info("\n" + "=" * 70)
        logger.info("DATA VALIDATION & CORRECTION SUMMARY")
        logger.info("=" * 70)
        logger.info(f"Issues detected:             {self.stats['issues_detected']}")
        logger.info(f"Issues corrected:            {self.stats['issues_corrected']}")
        logger.info(f"Phosphate values fixed:      {self.stats['phosphate_fixed']}")
        logger.info(f"Wind speed values fixed:     {self.stats['wind_speed_fixed']}")
        logger.info(f"Pressure values flagged:     {self.stats['pressure_flagged']}")
        logger.info(f"Silicate values flagged:     {self.stats['silicate_flagged']}")
        logger.info("=" * 70)
        
        if self.dry_run:
            logger.info("\n⚠ DRY RUN MODE: No changes were committed to the database")
            logger.info("Remove --dry-run flag to apply corrections")


if __name__ == '__main__':
    dry_run = '--dry-run' in sys.argv or os.getenv('DRY_RUN', '0') == '1'
    
    # Build db_config from environment variables
    # Use correct defaults matching docker-compose.yml
    db_config = {
        'host': os.getenv('DB_HOST', 'localhost'),
        'port': int(os.getenv('DB_PORT', 5433)),
        'database': os.getenv('DB_NAME', 'marine_db'),
        'user': os.getenv('DB_USER', 'marine_user'),
        'password': os.getenv('DB_PASSWORD'),  # No default - must be provided
    }
    
    # Validate required password
    if not db_config['password']:
        logger.error("DB_PASSWORD environment variable not set")
        logger.error("Set it with: export DB_PASSWORD=<your_password>")
        sys.exit(1)
    
    validator = DataValidator(db_config, dry_run=dry_run)
    validator.run_validation()
