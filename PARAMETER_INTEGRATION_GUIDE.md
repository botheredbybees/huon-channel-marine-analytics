# Parameter Table Integration - Implementation Guide

## Overview
This guide implements automatic parameter population across three scripts:
1. `populate_metadata.py` - Extract parameters from ISO19115-3 XML metadata
2. `populate_measurements.py` - Standardize parameters using mappings
3. `init_parameters.sh` - Orchestrate initialization sequence

## Implementation Steps

### STEP 1: Update populate_metadata.py

Add these functions after the `extract_distribution_urls()` function (around line 600):

```python
def extract_parameters_from_xml(root, metadata_id: int) -> List[Dict]:
    """Extract parameter information from ISO 19115-3 metadata XML."""
    parameters = []
    logger.debug("  [PARAMETERS] Starting parameter extraction from XML...")
    
    sample_dims = find_all_elements_by_tag_suffix(root, 'MDSampleDimension')
    logger.debug(f"  [PARAMETERS] Found {len(sample_dims)} MDSampleDimension elements")
    
    for idx, sample_dim in enumerate(sample_dims, 1):
        param_info = {
            'metadata_id': metadata_id,
            'parameter_code': None,
            'parameter_label': None,
            'aodn_parameter_uri': None,
            'unit_name': None,
            'unit_uri': None,
            'imos_parameter_uri': None,
            'imos_unit_uri': None
        }
        
        # Extract parameter code from mcc:code
        code_elem = find_element_by_tag_suffix(sample_dim, 'code')
        if code_elem:
            anchor_elem = None
            for child in code_elem:
                if child.tag.endswith('}Anchor'):
                    anchor_elem = child
                    break
            
            if anchor_elem is not None:
                param_info['parameter_label'] = get_element_text(anchor_elem)
                param_info['aodn_parameter_uri'] = anchor_elem.get(
                    '{http://www.w3.org/1999/xlink}href', ''
                )
                
                if param_info['aodn_parameter_uri']:
                    uri_parts = param_info['aodn_parameter_uri'].rstrip('/').split('/')
                    param_info['parameter_code'] = uri_parts[-1] if uri_parts else param_info['parameter_label']
                else:
                    param_info['parameter_code'] = param_info['parameter_label']
                
                logger.debug(f"  [PARAM {idx}] Found: {param_info['parameter_label']}")
            else:
                param_text = get_element_text(code_elem)
                if param_text:
                    param_info['parameter_label'] = param_text
                    param_info['parameter_code'] = param_text.replace(' ', '_').upper()
        
        # Extract unit from gml:unitDefinition
        unit_def = find_element_by_tag_suffix(sample_dim, 'unitDefinition')
        if unit_def:
            unit_name_elem = find_element_by_tag_suffix(unit_def, 'name')
            if unit_name_elem:
                param_info['unit_name'] = get_element_text(unit_name_elem)
            
            unit_id_elem = find_element_by_tag_suffix(unit_def, 'identifier')
            if unit_id_elem:
                param_info['unit_uri'] = get_element_text(unit_id_elem)
        
        if param_info['parameter_code'] and param_info['parameter_label']:
            parameters.append(param_info)
            logger.debug(f"  [PARAM {idx}] ✓ Added to extraction list")
    
    logger.debug(f"  [PARAMETERS] Extracted {len(parameters)} valid parameters")
    return parameters


def insert_parameters(cursor, parameters: List[Dict]):
    """Insert or update parameters for a dataset."""
    if not parameters:
        return 0, 0
    
    inserted = 0
    updated = 0
    
    for param in parameters:
        try:
            cursor.execute("""
                INSERT INTO parameters (
                    metadata_id, parameter_code, parameter_label,
                    aodn_parameter_uri, unit_name, unit_uri,
                    imos_parameter_uri, imos_unit_uri
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (metadata_id, parameter_code) 
                DO UPDATE SET
                    parameter_label = EXCLUDED.parameter_label,
                    aodn_parameter_uri = EXCLUDED.aodn_parameter_uri,
                    unit_name = EXCLUDED.unit_name,
                    unit_uri = EXCLUDED.unit_uri
                RETURNING (xmax = 0) AS inserted
            """, (
                param['metadata_id'],
                param['parameter_code'],
                param['parameter_label'],
                param['aodn_parameter_uri'],
                param['unit_name'],
                param['unit_uri'],
                param['imos_parameter_uri'],
                param['imos_unit_uri']
            ))
            
            result = cursor.fetchone()
            if result and result[0]:
                inserted += 1
            else:
                updated += 1
                
        except Exception as e:
            logger.error(f"      ✗ Failed to insert parameter {param.get('parameter_code')}: {e}")
            continue
    
    if inserted > 0 or updated > 0:
        logger.info(f"    ✓ Parameters: {inserted} inserted, {updated} updated")
    return inserted, updated
```

### STEP 2: Modify populate_metadata_table() function

Find the `populate_metadata_table()` function and add parameter extraction after metadata insertion.

After successful metadata insertion/update, add:

```python
            # Extract and insert parameters from XML
            try:
                # Get the metadata_id for the just-inserted/updated record
                cursor.execute("SELECT id FROM metadata WHERE dataset_path = %s", 
                             (dataset.get('dataset_path'),))
                result = cursor.fetchone()
                if result:
                    metadata_id = result[0]
                    
                    # Re-parse XML to get root element
                    xml_path = find_metadata_xml(Path(dataset.get('dataset_path')))
                    if xml_path:
                        tree = ET.parse(xml_path)
                        root = tree.getroot()
                        
                        # Extract parameters from XML
                        parameters = extract_parameters_from_xml(root, metadata_id)
                        
                        # Insert parameters
                        if parameters:
                            insert_parameters(cursor, parameters)
            except Exception as e:
                logger.warning(f"  ⚠ Parameter extraction failed: {e}")
```

### STEP 3: Update populate_measurements.py

Add parameter standardization function at the top (after imports):

```python
def standardize_parameter(cursor, raw_name: str, namespace: str = 'custom') -> tuple:
    """
    Standardize parameter name using parameter_mappings table.
    
    Args:
        cursor: Database cursor
        raw_name: Raw parameter name from data file
        namespace: Namespace (default: 'custom')
        
    Returns:
        (standard_code, namespace, unit) tuple
    """
    # Try to find existing mapping
    cursor.execute("""
        SELECT standard_code, namespace, unit
        FROM parameter_mappings
        WHERE raw_parameter_name = %s
        LIMIT 1
    """, (raw_name,))
    
    result = cursor.fetchone()
    
    if result:
        logger.debug(f"      Mapped '{raw_name}' → '{result[0]}' ({result[1]})")
        return result
    
    # No mapping found - create standardized version
    standard_code = raw_name.upper().replace(' ', '_')
    logger.debug(f"      No mapping for '{raw_name}', using '{standard_code}'")
    
    # Optionally create new mapping
    try:
        cursor.execute("""
            INSERT INTO parameter_mappings (raw_parameter_name, standard_code, namespace, unit)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (raw_parameter_name) DO NOTHING
        """, (raw_name, standard_code, namespace, 'unknown'))
    except:
        pass  # Mapping already exists or error - continue anyway
    
    return (standard_code, namespace, 'unknown')
```

### STEP 4: Update measurement extraction to use standardization

In both `CSVExtractor.extract()` and `NetCDFExtractor.extract()`, when creating measurements, change:

```python
# OLD CODE:
measurements.append((
    timestamp,
    metadata_id,
    location_id,
    param_name,  # parameter_code
    'custom',  # namespace
    value,
    'unknown',  # uom
    # ... rest of fields
))

# NEW CODE:
standardized = standardize_parameter(self.cursor, param_name)
measurements.append((
    timestamp,
    metadata_id,
    location_id,
    standardized[0],  # standard_code
    standardized[1],  # namespace
    value,
    standardized[2],  # unit from mapping
    # ... rest of fields
))
```

### STEP 5: Create initialization script

Create `init_parameters.sh` in the repository root (see separate file)

## Testing

After implementation, test with:

```bash
chmod +x init_parameters.sh
./init_parameters.sh
```

Expected outcomes:
1. parameter_mappings table populated with standard mappings
2. parameters table populated with dataset-specific parameter metadata
3. measurements table uses standardized parameter codes
4. All tables properly linked via foreign keys

## Verification Queries

```sql
-- Check parameter coverage
SELECT 
    p.parameter_code,
    p.parameter_label,
    COUNT(DISTINCT m.id) as measurement_count
FROM parameters p
LEFT JOIN measurements m ON p.parameter_code = m.parameter_code
GROUP BY p.parameter_code, p.parameter_label
ORDER BY measurement_count DESC;

-- Check mapping usage
SELECT 
    pm.raw_parameter_name,
    pm.standard_code,
    COUNT(m.id) as usage_count
FROM parameter_mappings pm
LEFT JOIN measurements m ON pm.standard_code = m.parameter_code
GROUP BY pm.raw_parameter_name, pm.standard_code
ORDER BY usage_count DESC;

-- Check data integrity
SELECT 
    'Orphaned measurements' as issue,
    COUNT(*) as count
FROM measurements m
WHERE NOT EXISTS (
    SELECT 1 FROM parameters p 
    WHERE p.parameter_code = m.parameter_code 
    AND p.metadata_id = m.metadata_id
)
UNION ALL
SELECT 
    'Parameters without measurements',
    COUNT(*)
FROM parameters p
WHERE NOT EXISTS (
    SELECT 1 FROM measurements m
    WHERE m.parameter_code = p.parameter_code
    AND m.metadata_id = p.metadata_id
);
```

## Notes

- Error handling ensures metadata insertion succeeds even if parameter extraction fails
- ON CONFLICT clauses prevent duplicate errors during updates
- The initialization sequence must be followed in order
- Parameter extraction from XML requires ISO 19115-3 MDSampleDimension elements
- Standardization creates new mappings automatically for unmapped parameters

## Files Modified

1. `populate_metadata.py` - Added parameter extraction from XML
2. `populate_measurements.py` - Added parameter standardization
3. `init_parameters.sh` - New orchestration script
4. `PARAMETER_INTEGRATION_GUIDE.md` - This guide

## Troubleshooting

### No parameters extracted from XML
- Check if metadata.xml contains MDSampleDimension elements
- Verify ISO 19115-3 format (not ISO 19115-1)
- Check logs for parsing errors

### Measurements not using standardized codes
- Ensure parameter_mappings table is populated first
- Check if standardize_parameter() function is being called
- Verify cursor is passed to extractors

### Foreign key violations
- Ensure metadata exists before inserting parameters
- Check that measurements reference existing parameter codes
- Verify metadata_id matches between tables