#!/usr/bin/env python3
"""
Enrich metadata table from ISO 19115-3 XML files in AODN dataset directories.
Non-destructive: only updates NULL/empty fields.

Enhanced with:
- AODN UUID extraction from XML metadata
- Deduplication logic to prevent re-ingestion of AODN datasets
- aodn_uuid field population for AODN-sourced data
- Parameter extraction from XML contentInfo sections
- Automatic update of parameter_mappings table
- Automatic update of config_parameter_mapping.json file
- Improved logging for debugging and audit trails

This script extracts metadata from XML files located in dataset directories
and populates empty fields in the metadata table. It's designed to run
independently of the main ETL pipeline.

Usage:
    python enrich_metadata_from_xml.py

Environment variables:
    DB_HOST: Database host (default: localhost)
    DB_PORT: Database port (default: 5433)
    DB_NAME: Database name (default: marine_db)
    DB_USER: Database user (default: marine_user)
    DB_PASSWORD: Database password (required for authentication)
    AODN_DATA_PATH: Path to AODN_data directory (default: /AODN_data)
"""

import os
import psycopg2
from pathlib import Path
from xml.etree import ElementTree as ET
from typing import Dict, Optional, Tuple, List
import logging
from datetime import datetime
import sys
import re
import json

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# XML namespace mappings for ISO 19115-3
NAMESPACES = {
    'gmd': 'http://www.isotc211.org/2005/gmd',
    'gco': 'http://www.isotc211.org/2005/gco',
    'gml': 'http://www.opengis.net/gml/3.2.1',
    'srv': 'http://www.isotc211.org/2005/srv',
    'mdb': 'http://www.isotc211.org/2005/mdb',
    'mrc': 'http://standards.iso.org/iso/19115/-3/mrc/1.0',
}


def find_element_with_namespace(root: ET.Element, local_tag: str) -> Optional[ET.Element]:
    """Find element by local tag name, ignoring namespace."""
    for elem in root.iter():
        tag_local = elem.tag.split('}')[-1] if '}' in elem.tag else elem.tag
        if tag_local == local_tag:
            return elem
    return None


def find_elements_by_path_generic(root: ET.Element, path_parts: list) -> Optional[ET.Element]:
    """Find element by path, matching local names without namespace."""
    def local_name(tag):
        return tag.split('}')[-1] if '}' in tag else tag
    
    def search_path(current, remaining_path):
        if not remaining_path:
            return current
        
        target_tag = remaining_path[0]
        rest_path = remaining_path[1:]
        
        for child in current:
            if local_name(child.tag) == target_tag:
                result = search_path(child, rest_path)
                if result is not None:
                    return result
        return None
    
    return search_path(root, path_parts)


class MetadataEnricher:
    """Extract and enrich metadata from ISO 19115-3 XML files."""
    
    def __init__(self, db_config: dict, aodn_data_path: str):
        self.db_config = db_config
        self.aodn_data_path = Path(aodn_data_path)
        self.conn = None
        self.config_json_path = Path(__file__).parent.parent / 'config_parameter_mapping.json'
        self.stats = {
            'files_found': 0,
            'files_processed': 0,
            'files_failed': 0,
            'files_deduplicated': 0,
            'rows_updated': 0,
            'aodn_uuids_extracted': 0,
            'fields_enriched': 0,
            'parameters_found': 0,
            'parameters_added': 0,
        }
        
    def connect(self):
        """Connect to PostgreSQL database."""
        try:
            self.conn = psycopg2.connect(**self.db_config)
            logger.info(f"Connected to {self.db_config['database']} at {self.db_config['host']}:{self.db_config['port']}")
        except psycopg2.OperationalError as e:
            logger.error(f"Failed to connect to database: {e}")
            logger.error("Check your environment variables: DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASSWORD")
            sys.exit(1)
        
    def disconnect(self):
        """Disconnect from database."""
        if self.conn:
            self.conn.close()
            logger.info("Disconnected from database")
    
    def find_metadata_xml_files(self) -> Dict[str, Path]:
        """
        Find all metadata.xml files in AODN_data directory.
        Searches both {dataset_path}/metadata/metadata.xml and {dataset_path}/metadata.xml
        
        Returns:
            dict: Mapping of dataset_path -> xml_file_path
        """
        xml_files = {}
        
        if not self.aodn_data_path.exists():
            logger.error(f"AODN_DATA_PATH does not exist: {self.aodn_data_path}")
            return xml_files
        
        logger.info(f"Scanning for metadata.xml files in {self.aodn_data_path}")
        
        # Pattern 1: .../metadata/metadata.xml
        pattern1 = '**/metadata/metadata.xml'
        for xml_file in self.aodn_data_path.glob(pattern1):
            try:
                # Dataset path is parent of metadata directory
                dataset_dir = xml_file.parent.parent
                dataset_path = str(dataset_dir)
                xml_files[dataset_path] = xml_file
                logger.debug(f"Found metadata file: {dataset_path} -> {xml_file}")
            except Exception as e:
                logger.warning(f"Could not extract dataset path from {xml_file}: {e}")
        
        # Pattern 2: .../metadata.xml (fallback)
        pattern2 = '**/metadata.xml'
        for xml_file in self.aodn_data_path.glob(pattern2):
            if xml_file.parent.name != 'metadata':  # Skip if already found
                try:
                    dataset_dir = xml_file.parent
                    dataset_path = str(dataset_dir)
                    if dataset_path not in xml_files:
                        xml_files[dataset_path] = xml_file
                        logger.debug(f"Found metadata file (alt): {dataset_path} -> {xml_file}")
                except Exception as e:
                    logger.warning(f"Could not extract dataset path from {xml_file}: {e}")
            
        self.stats['files_found'] = len(xml_files)
        logger.info(f"Found {len(xml_files)} metadata.xml files")
        return xml_files
    
    def find_metadata_record_by_path(self, dataset_path: str) -> Optional[Tuple[int, str]]:
        """
        Find metadata record ID by dataset_path.
        
        Args:
            dataset_path: Full path to dataset directory
            
        Returns:
            Tuple of (metadata.id, dataset_path) if found, None otherwise
        """
        cursor = self.conn.cursor()
        try:
            logger.debug(f"Looking up metadata record for dataset_path: {dataset_path}")
            cursor.execute(
                "SELECT id, dataset_path FROM metadata WHERE dataset_path = %s LIMIT 1",
                [dataset_path]
            )
            result = cursor.fetchone()
            
            if result:
                record_id, db_path = result
                logger.info(f"✓ Found metadata record: dataset_path={dataset_path} -> metadata.id={record_id}")
                return (record_id, db_path)
            else:
                logger.warning(f"✗ No metadata record found for dataset_path: {dataset_path}")
                return None
                
        except psycopg2.Error as e:
            logger.error(f"Database error during record lookup for {dataset_path}: {e}")
            return None
        finally:
            cursor.close()
    
    def parse_iso_19115_xml(self, xml_path: Path) -> Tuple[Dict[str, any], Optional[str], List[Dict]]:
        """
        Extract metadata fields and parameters from ISO 19115-3 XML file.
        
        Returns:
            Tuple of (metadata_dict, aodn_uuid, parameters_list)
        """
        logger.debug(f"Parsing XML: {xml_path}")
        try:
            tree = ET.parse(xml_path)
            root = tree.getroot()
            
            metadata = {
                'abstract': None,
                'credit': None,
                'topic_category': None,
                'west': None,
                'east': None,
                'south': None,
                'north': None,
                'time_start': None,
                'time_end': None,
                'lineage': None,
                'supplemental_info': None,
                'license_url': None,
            }
            
            # Extract AODN UUID
            aodn_uuid = self._extract_aodn_uuid(root)
            if aodn_uuid:
                logger.info(f"✓ Extracted AODN UUID from XML: {aodn_uuid}")
                self.stats['aodn_uuids_extracted'] += 1
            
            # Extract metadata fields
            self._extract_metadata_fields(root, metadata)
            
            # Extract parameters
            parameters = self._extract_parameters(root)
            if parameters:
                logger.info(f"✓ Extracted {len(parameters)} parameter(s) from XML")
                self.stats['parameters_found'] += len(parameters)
            
            return metadata, aodn_uuid, parameters
            
        except ET.ParseError as e:
            logger.error(f"XML parsing error in {xml_path}: {e}")
            self.stats['files_failed'] += 1
            return {}, None, []
        except Exception as e:
            logger.error(f"Unexpected error parsing {xml_path}: {e}")
            self.stats['files_failed'] += 1
            return {}, None, []
    
    def _extract_metadata_fields(self, root: ET.Element, metadata: dict):
        """Extract standard metadata fields from XML."""
        # Extract abstract
        abstract_xpath = './/gmd:abstract/gco:CharacterString'
        abstract_elem = root.find(abstract_xpath, NAMESPACES)
        if abstract_elem is not None and abstract_elem.text:
            metadata['abstract'] = abstract_elem.text[:1000]
            logger.debug(f"  Found abstract ({len(abstract_elem.text)} chars)")
        
        # Extract credit
        credit_xpath = './/gmd:credit/gco:CharacterString'
        credit_elem = root.find(credit_xpath, NAMESPACES)
        if credit_elem is not None and credit_elem.text:
            metadata['credit'] = credit_elem.text[:500]
            logger.debug(f"  Found credit ({len(credit_elem.text)} chars)")
        
        # Extract spatial extent
        self._extract_spatial_extent(root, metadata)
        
        # Extract temporal extent
        self._extract_temporal_extent(root, metadata)
        
        # Extract lineage
        lineage_xpath = './/gmd:lineage/gmd:LI_Lineage/gmd:statement/gco:CharacterString'
        lineage_elem = root.find(lineage_xpath, NAMESPACES)
        if lineage_elem is not None and lineage_elem.text:
            metadata['lineage'] = lineage_elem.text[:1000]
            logger.debug(f"  Found lineage ({len(lineage_elem.text)} chars)")
        
        # Extract license
        license_xpath = './/gmd:MD_LegalConstraints/gmd:otherConstraints/gco:CharacterString'
        license_elem = root.find(license_xpath, NAMESPACES)
        if license_elem is not None and license_elem.text:
            metadata['license_url'] = license_elem.text[:500]
            logger.debug(f"  Found license info")
        
        non_null_fields = [k for k, v in metadata.items() if v is not None]
        logger.info(f"  Extracted {len(non_null_fields)} metadata fields: {', '.join(non_null_fields)}")
    
    def _extract_parameters(self, root: ET.Element) -> List[Dict]:
        """
        Extract parameter definitions from ISO 19115-3 XML contentInfo sections.
        
        Returns:
            List of parameter dictionaries with keys: raw_name, standard_code, namespace, unit, description
        """
        parameters = []
        
        # Search for dimension elements (older format)
        for dimension in root.findall('.//gmd:dimension', NAMESPACES):
            param = self._parse_dimension_element(dimension)
            if param:
                parameters.append(param)
        
        # Search for attribute elements (newer ISO 19115-3 format)
        for attribute in root.findall('.//mrc:attribute', NAMESPACES):
            param = self._parse_attribute_element(attribute)
            if param:
                parameters.append(param)
        
        return parameters
    
    def _parse_dimension_element(self, dimension: ET.Element) -> Optional[Dict]:
        """Parse gmd:dimension element for parameter information."""
        try:
            # Get sequence identifier (parameter name)
            seq_id = dimension.find('.//gmd:sequenceIdentifier/gco:MemberName/gco:aName/gco:CharacterString', NAMESPACES)
            if seq_id is None or not seq_id.text:
                return None
            
            raw_name = seq_id.text.strip().upper()
            
            # Get descriptor (description)
            descriptor = dimension.find('.//gmd:descriptor/gco:CharacterString', NAMESPACES)
            description = descriptor.text.strip() if descriptor is not None and descriptor.text else None
            
            # Get units
            unit_elem = dimension.find('.//gml:unitOfMeasure', NAMESPACES)
            unit = unit_elem.text.strip() if unit_elem is not None and unit_elem.text else "unknown"
            
            # Generate standard code and namespace
            standard_code = self._generate_standard_code(raw_name)
            namespace = self._determine_namespace(raw_name)
            
            return {
                'raw_name': raw_name,
                'standard_code': standard_code,
                'namespace': namespace,
                'unit': unit,
                'description': description or f"Parameter {raw_name}",
            }
        except Exception as e:
            logger.warning(f"Error parsing dimension element: {e}")
            return None
    
    def _parse_attribute_element(self, attribute: ET.Element) -> Optional[Dict]:
        """Parse mrc:attribute element for parameter information."""
        try:
            # Get member name
            member_name = attribute.find('.//mrc:memberName/gco:CharacterString', NAMESPACES)
            if member_name is None or not member_name.text:
                return None
            
            raw_name = member_name.text.strip().upper()
            
            # Get description
            definition = attribute.find('.//mrc:definition/gco:CharacterString', NAMESPACES)
            description = definition.text.strip() if definition is not None and definition.text else None
            
            # Get units
            unit_elem = attribute.find('.//mrc:units/gml:BaseUnit/gml:identifier', NAMESPACES)
            unit = unit_elem.text.strip() if unit_elem is not None and unit_elem.text else "unknown"
            
            # Generate standard code and namespace
            standard_code = self._generate_standard_code(raw_name)
            namespace = self._determine_namespace(raw_name)
            
            return {
                'raw_name': raw_name,
                'standard_code': standard_code,
                'namespace': namespace,
                'unit': unit,
                'description': description or f"Parameter {raw_name}",
            }
        except Exception as e:
            logger.warning(f"Error parsing attribute element: {e}")
            return None
    
    def _generate_standard_code(self, raw_name: str) -> str:
        """Generate standard parameter code from raw name."""
        # Common abbreviations
        replacements = {
            'TEMPERATURE': 'TEMP',
            'SALINITY': 'PSAL',
            'CHLOROPHYLL': 'CPHL',
            'OXYGEN': 'DOXY',
            'PRESSURE': 'PRES',
            'CONDUCTIVITY': 'COND',
        }
        
        for full, abbr in replacements.items():
            if full in raw_name:
                return abbr
        
        # Default: use raw name
        return raw_name
    
    def _determine_namespace(self, raw_name: str) -> str:
        """Determine appropriate namespace for parameter."""
        # BODC common parameters
        bodc_params = ['TEMP', 'PSAL', 'CPHL', 'DOXY', 'PRES', 'DEPTH']
        if any(p in raw_name for p in bodc_params):
            return 'bodc'
        
        # CF standard names
        cf_params = ['VELOCITY', 'WIND', 'WAVE']
        if any(p in raw_name for p in cf_params):
            return 'cf'
        
        # Default to custom
        return 'custom'
    
    def _extract_aodn_uuid(self, root: ET.Element) -> Optional[str]:
        """Extract AODN UUID from ISO 19115-3 XML metadata."""
        try:
            # Try metadataIdentifier path
            uuid_elem = find_elements_by_path_generic(
                root, 
                ['metadataIdentifier', 'MD_Identifier', 'code', 'CharacterString']
            )
            if uuid_elem is not None and uuid_elem.text:
                return uuid_elem.text.strip()
            
            # Try Anchor element
            uuid_elem = find_elements_by_path_generic(
                root,
                ['metadataIdentifier', 'MD_Identifier', 'code', 'Anchor']
            )
            if uuid_elem is not None and uuid_elem.text:
                return uuid_elem.text.strip()
            
            # Try fileIdentifier path
            uuid_elem = find_elements_by_path_generic(
                root,
                ['fileIdentifier', 'CharacterString']
            )
            if uuid_elem is not None and uuid_elem.text:
                return uuid_elem.text.strip()
            
            return None
        except Exception as e:
            logger.warning(f"Error extracting AODN UUID: {e}")
            return None
    
    def check_aodn_uuid_exists(self, aodn_uuid: str) -> bool:
        """Check if AODN UUID already exists in metadata table."""
        cursor = self.conn.cursor()
        try:
            cursor.execute(
                "SELECT id FROM metadata WHERE aodn_uuid = %s LIMIT 1",
                [aodn_uuid]
            )
            result = cursor.fetchone()
            exists = result is not None
            
            if exists:
                logger.info(f"⚠ AODN UUID {aodn_uuid} already exists in database (id={result[0]})")
            
            return exists
        except psycopg2.Error as e:
            logger.error(f"Database error checking AODN UUID {aodn_uuid}: {e}")
            return False
        finally:
            cursor.close()
    
    def _extract_spatial_extent(self, root: ET.Element, metadata: dict):
        """Extract geographic bounding box from XML."""
        try:
            bbox_xpath = './/gmd:EX_GeographicBoundingBox'
            bbox = root.find(bbox_xpath, NAMESPACES)
            
            if bbox is not None:
                west = bbox.find('./gmd:westBoundLongitude/gco:Decimal', NAMESPACES)
                east = bbox.find('./gmd:eastBoundLongitude/gco:Decimal', NAMESPACES)
                south = bbox.find('./gmd:southBoundLatitude/gco:Decimal', NAMESPACES)
                north = bbox.find('./gmd:northBoundLatitude/gco:Decimal', NAMESPACES)
                
                try:
                    if west is not None and west.text: metadata['west'] = float(west.text)
                    if east is not None and east.text: metadata['east'] = float(east.text)
                    if south is not None and south.text: metadata['south'] = float(south.text)
                    if north is not None and north.text: metadata['north'] = float(north.text)
                except ValueError as e:
                    logger.warning(f"Could not parse spatial extent values: {e}")
        except Exception as e:
            logger.warning(f"Could not extract spatial extent: {e}")
    
    def _extract_temporal_extent(self, root: ET.Element, metadata: dict):
        """Extract temporal coverage from XML."""
        try:
            begin = root.find('.//gmd:beginPosition', NAMESPACES)
            end = root.find('.//gmd:endPosition', NAMESPACES)
            
            if begin is not None and begin.text:
                metadata['time_start'] = begin.text
            if end is not None and end.text:
                metadata['time_end'] = end.text
        except Exception as e:
            logger.warning(f"Could not extract temporal extent: {e}")
    
    def update_metadata_table(self, record_id: int, dataset_path: str, metadata: dict, aodn_uuid: Optional[str] = None) -> int:
        """Update metadata table with extracted values."""
        cursor = self.conn.cursor()
        fields_updated = 0
        
        try:
            logger.info(f"Updating record id={record_id} (dataset_path={dataset_path})")
            
            # Update aodn_uuid if present
            if aodn_uuid:
                cursor.execute(
                    "UPDATE metadata SET aodn_uuid = %s WHERE id = %s RETURNING id",
                    [aodn_uuid, record_id]
                )
                if cursor.rowcount > 0:
                    fields_updated += 1
                    self.conn.commit()
                    logger.info(f"  ✓ Updated aodn_uuid = {aodn_uuid}")
            
            # Update other fields only if NULL
            other_updates = {k: v for k, v in metadata.items() if v is not None}
            
            if other_updates:
                null_conditions = ' OR '.join([f'{k} IS NULL' for k in other_updates.keys()])
                set_clause = ', '.join([f'{k} = %s' for k in other_updates.keys()])
                
                cursor.execute(
                    f"UPDATE metadata SET {set_clause} WHERE id = %s AND ({null_conditions}) RETURNING id",
                    list(other_updates.values()) + [record_id]
                )
                
                if cursor.rowcount > 0:
                    fields_updated += len(other_updates)
                    self.stats['fields_enriched'] += len(other_updates)
                    self.conn.commit()
                    logger.info(f"  ✓ Enriched {len(other_updates)} fields")
            
            if fields_updated > 0:
                self.stats['rows_updated'] += 1
                
        except psycopg2.Error as e:
            logger.error(f"✗ Database error updating record id={record_id}: {e}")
            self.conn.rollback()
        finally:
            cursor.close()
        
        return fields_updated
    
    def add_parameters_to_database(self, parameters: List[Dict]):
        """Add new parameters to parameter_mappings table."""
        cursor = self.conn.cursor()
        
        try:
            for param in parameters:
                # Check if parameter already exists
                cursor.execute(
                    "SELECT id FROM parameter_mappings WHERE raw_parameter_name = %s",
                    [param['raw_name']]
                )
                
                if cursor.fetchone() is None:
                    # Insert new parameter
                    cursor.execute(
                        """
                        INSERT INTO parameter_mappings 
                        (raw_parameter_name, standard_code, namespace, unit, source, description)
                        VALUES (%s, %s, %s, %s, 'xml', %s)
                        """,
                        [param['raw_name'], param['standard_code'], param['namespace'], 
                         param['unit'], param['description']]
                    )
                    self.stats['parameters_added'] += 1
                    logger.info(f"  ✓ Added parameter: {param['raw_name']} -> {param['standard_code']} ({param['namespace']})")
                else:
                    logger.debug(f"  - Parameter already exists: {param['raw_name']}")
            
            self.conn.commit()
            
        except psycopg2.Error as e:
            logger.error(f"Database error adding parameters: {e}")
            self.conn.rollback()
        finally:
            cursor.close()
    
    def update_config_json(self, parameters: List[Dict]):
        """Update config_parameter_mapping.json with new parameters."""
        try:
            # Load existing config
            if self.config_json_path.exists():
                with open(self.config_json_path, 'r') as f:
                    config = json.load(f)
            else:
                logger.warning(f"Config file not found: {self.config_json_path}")
                config = {'parameter_mapping': {}}
            
            # Add new parameters
            param_mapping = config.get('parameter_mapping', {})
            added_count = 0
            
            for param in parameters:
                if param['raw_name'] not in param_mapping:
                    param_mapping[param['raw_name']] = [
                        param['standard_code'],
                        param['namespace'],
                        param['unit']
                    ]
                    added_count += 1
                    logger.info(f"  ✓ Added to config JSON: {param['raw_name']}")
            
            if added_count > 0:
                config['parameter_mapping'] = param_mapping
                
                # Save updated config
                with open(self.config_json_path, 'w') as f:
                    json.dump(config, f, indent=2)
                
                logger.info(f"✓ Updated {self.config_json_path} with {added_count} new parameter(s)")
            else:
                logger.debug("No new parameters to add to config JSON")
                
        except Exception as e:
            logger.error(f"Error updating config JSON: {e}")
    
    def run_enrichment(self):
        """Main enrichment workflow."""
        logger.info("=" * 70)
        logger.info("STARTING METADATA ENRICHMENT")
        logger.info("=" * 70)
        self.connect()
        
        try:
            xml_files = self.find_metadata_xml_files()
            
            if not xml_files:
                logger.warning("No metadata.xml files found. Nothing to process.")
                return
            
            logger.info(f"\nProcessing {len(xml_files)} metadata files...\n")
            
            for dataset_path, xml_path in xml_files.items():
                logger.info(f"\n{'='*70}")
                logger.info(f"PROCESSING: {dataset_path}")
                logger.info(f"File: {xml_path}")
                logger.info(f"{'='*70}")
                
                # Find record by dataset_path
                result = self.find_metadata_record_by_path(dataset_path)
                if not result:
                    logger.error(f"Cannot process {dataset_path}: no metadata record found")
                    self.stats['files_failed'] += 1
                    continue
                
                record_id, db_path = result
                
                # Parse XML and extract metadata and parameters
                metadata, aodn_uuid, parameters = self.parse_iso_19115_xml(xml_path)
                
                # Check for deduplication
                if aodn_uuid and self.check_aodn_uuid_exists(aodn_uuid):
                    logger.warning(f"⚠ SKIPPING: AODN dataset {aodn_uuid} already exists")
                    self.stats['files_deduplicated'] += 1
                    continue
                
                # Update metadata table
                fields_updated = self.update_metadata_table(record_id, dataset_path, metadata, aodn_uuid)
                
                # Process parameters if found
                if parameters:
                    logger.info(f"Processing {len(parameters)} parameter(s)")
                    self.add_parameters_to_database(parameters)
                    self.update_config_json(parameters)
                
                self.stats['files_processed'] += 1
                logger.info(f"✓ COMPLETED: {dataset_path}\n")
        
        except Exception as e:
            logger.error(f"✗ FATAL ERROR during enrichment: {e}")
            import traceback
            traceback.print_exc()
        finally:
            self.disconnect()
            self._print_summary()
    
    def _print_summary(self):
        """Print enrichment summary statistics."""
        logger.info("\n" + "=" * 70)
        logger.info("METADATA ENRICHMENT SUMMARY")
        logger.info("=" * 70)
        logger.info(f"XML files found:          {self.stats['files_found']}")
        logger.info(f"Files processed:          {self.stats['files_processed']}")
        logger.info(f"Files deduplicated:       {self.stats['files_deduplicated']}")
        logger.info(f"Files failed:             {self.stats['files_failed']}")
        logger.info(f"AODN UUIDs extracted:     {self.stats['aodn_uuids_extracted']}")
        logger.info(f"Records updated:          {self.stats['rows_updated']}")
        logger.info(f"Total fields enriched:    {self.stats['fields_enriched']}")
        logger.info(f"Parameters found:         {self.stats['parameters_found']}")
        logger.info(f"Parameters added to DB:   {self.stats['parameters_added']}")
        logger.info("=" * 70)


if __name__ == '__main__':
    db_config = {
        'host': os.getenv('DB_HOST', 'localhost'),
        'port': int(os.getenv('DB_PORT', 5433)),
        'database': os.getenv('DB_NAME', 'marine_db'),
        'user': os.getenv('DB_USER', 'marine_user'),
        'password': os.getenv('DB_PASSWORD'),
    }
    
    if not db_config['password']:
        logger.error("DB_PASSWORD environment variable not set")
        sys.exit(1)
    
    aodn_path = os.getenv('AODN_DATA_PATH', '/AODN_data')
    
    enricher = MetadataEnricher(db_config, aodn_path)
    enricher.run_enrichment()
