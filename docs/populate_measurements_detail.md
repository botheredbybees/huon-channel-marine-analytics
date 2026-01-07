# populate_measurements.py - Detailed Documentation

## Overview

`populate_measurements.py` is the core ETL script that extracts oceanographic measurements from CSV and NetCDF files, standardizes parameters, validates locations, and loads data into the `measurements` table.

**Script Version:** 4.0 (**Metadata-Based Parameter Detection**)  
**Dependencies:** `psycopg2`, `pandas`, `numpy`, `netCDF4`, `cftime`, `xml.etree.ElementTree`  
**Estimated Runtime:** 5-30 minutes per dataset (varies by file size)

---

## Critical v4.0 Change: Metadata-Based Parameter Detection

### What Changed

**Previously (v3.x):** Parameter codes were detected from NetCDF variable names or CSV column names, leading to ambiguities like 'PH' being confused between pH (acidity) and phosphate (nutrient).

**Now (v4.0):** Parameter codes are extracted from **metadata XML CF standard_name** fields, which are the authoritative source of truth. This eliminates misidentification at the source.

### Why This Matters

This change addresses the **root cause** of all recent data quality issues (#5-8):
- Issue #5: PH parameter ambiguity (pH vs phosphate)
- Issue #6: Negative turbidity values  
- Issue #7: Negative chlorophyll values
- Issue #8: Negative fluorescence values

All of these stemmed from using NetCDF variable names instead of metadata CF standard_name.

### How It Works

```python
# Extract from metadata XML (NEW v4.0)
param_mapping = extract_parameters_from_metadata(metadata_id, cursor)
# Returns: {'TEMP': 'TEMP', 'PO4': 'PO4', 'PSAL': 'PSAL', ...}

# Uses CF Standard Name Table mapping
CF_STANDARD_NAME_TO_CODE = {
    'mole_concentration_of_phosphate_in_sea_water': 'PO4',  # NOT 'PH'!
    'sea_water_ph_reported_on_total_scale': 'PH',          # Actual pH
    'sea_water_temperature': 'TEMP',
    # ... ~30 standard mappings
}
```

**Fallback:** If metadata XML is unavailable, falls back to column-based detection (CSV files).

---

## Key Features

### v4.0 Changes (January 2026)

- **🆕 Metadata-First Approach** - CF standard_name from metadata XML is authoritative source
- **🛡️ Prevents Parameter Misidentification** - Eliminates PH/phosphate confusion at source  
- **📝 XML Parsing** - Extracts contentInfo/dimension elements from ISO 19115-3 metadata
- **🔄 CF Standard Name Mapping** - Maps ~30 CF standard names to BODC/custom codes
- **🔽 Fallback Detection** - Column-based detection when metadata unavailable (CSV files)

### v3.3 Features (Preserved)

- Multi-parameter CSV extraction
- Intelligent parameter detection
- Unit inference
- Smart PH/phosphorus disambiguation (now redundant with v4.0)
- QC column filtering

### v3.2 Features (Preserved)

- PostGIS-free pure SQL implementation
- Updated connection config (port 5433)
- Location matching with coordinate proximity
- Enhanced error logging

### Guardrails

✓ **Metadata-Authoritative** - Uses CF standard_name as source of truth  
✓ **Upsert-Safe** - `INSERT ... ON CONFLICT DO NOTHING`  
✓ **Audit Trail** - QC flags track all modifications  
✓ **Schema Validation** - Type checking before database write  
✓ **Error Recovery** - Failed rows skipped with logging, no transaction rollback  
✓ **QC Column Filtering** - Quality control columns excluded from measurements table  
✓ **PostGIS-Free** - Pure SQL queries for maximum portability  

---

## Related Scripts

After running this script, you should run:
- **`populate_parameters_from_measurements.py`** - Populates the `parameters` table with records for each unique parameter code found in measurements.

---

## Database Connection (v3.2+)

```python
def get_db_connection():
    """Create database connection."""
    return psycopg2.connect(
        host="localhost",
        port=5433,        # Changed from 5432
        dbname="marine_db",
        user="marine_user",
        password="marine_pass123"  # Changed from marine_pass
    )
```

---

## Metadata-Based Parameter Detection (v4.0)

### CF Standard Name Extraction

The script extracts parameter codes from metadata XML using this workflow:

1. **Query metadata XML** from `metadata.metadata_content`
2. **Parse XML** using ElementTree
3. **Find contentInfo sections** with dimension/attribute elements
4. **Extract CF standard_name** from `gmd:name/gco:CharacterString`
5. **Extract NetCDF variable** from `gmd:sequenceIdentifier`  
6. **Map to parameter code** using `CF_STANDARD_NAME_TO_CODE` table

```python
def extract_parameters_from_metadata(metadata_id: int, cursor) -> Dict[str, str]:
    """
    Extract parameter codes from metadata XML using CF standard_name.
    
    Returns:
        Dict mapping NetCDF variable names to parameter codes
        Example: {'TEMP': 'TEMP', 'PO4': 'PO4', 'PSAL': 'PSAL'}
    """
    # Get metadata XML
    cursor.execute("""
        SELECT metadata_content 
        FROM metadata 
        WHERE id = %s
    """, (metadata_id,))
    
    metadata_xml = cursor.fetchone()[0]
    root = ET.fromstring(metadata_xml)
    
    # Parse contentInfo sections
    param_mapping = {}
    for element in content_info.findall('.//gmd:dimension', namespaces):
        netcdf_var = element.find('.//gmd:sequenceIdentifier').text
        cf_standard_name = element.find('.//gmd:name').text
        
        if cf_standard_name in CF_STANDARD_NAME_TO_CODE:
            param_code = CF_STANDARD_NAME_TO_CODE[cf_standard_name]
            param_mapping[netcdf_var] = param_code
    
    return param_mapping
```

### CF Standard Name to Parameter Code Mapping

| CF Standard Name | Parameter Code | Namespace | Notes |
|------------------|----------------|-----------|-------|
| `mole_concentration_of_phosphate_in_sea_water` | **PO4** | bodc | **Critical for Issue #5 fix** |
| `sea_water_ph_reported_on_total_scale` | **PH** | bodc | True pH (acidity) |
| `sea_water_temperature` | TEMP | custom | |
| `sea_water_practical_salinity` | PSAL | custom | |
| `sea_water_pressure` | PRES | custom | |
| `mass_concentration_of_chlorophyll_a_in_sea_water` | CPHL | custom | |
| `mole_concentration_of_nitrate_in_sea_water` | NO3 | bodc | |
| `mole_concentration_of_silicate_in_sea_water` | SIO4 | bodc | |

**Full mapping table**: See `CF_STANDARD_NAME_TO_CODE` in script (~30 mappings)

### Example: Issue #5 Resolution

**Before v4.0:**
```python
# NetCDF variable name: 'PH'
# Ambiguous! Could be pH or phosphate
params = detect_parameters(['PH'])  # Returns 'ph' or 'phosphate' based on values
```

**After v4.0:**
```python
# Metadata XML contains:
# <gmd:name>mole_concentration_of_phosphate_in_sea_water</gmd:name>
# <gmd:sequenceIdentifier>PH</gmd:sequenceIdentifier>

params = extract_parameters_from_metadata(metadata_id, cursor)
# Returns: {'PH': 'PO4'}  # Unambiguous! It's phosphate (PO4)
```

---

## Fallback Detection (CSV Files)

For CSV files without NetCDF metadata, the script uses keyword-based detection:

```python
PARAMETER_KEYWORDS = {
    'TEMP': ['temp', 'temperature', 'sst'],
    'PSAL': ['sal', 'salinity', 'psal'],
    'PRES': ['pres', 'pressure'],
    'PH': ['ph_total', 'ph_insitu', 'ph_seawater'],
    'PO4': ['phosphate', 'po4', 'phos'],
    # ... more keywords
}

def detect_parameters_fallback(columns) -> Dict[str, str]:
    """Fallback: Detect from column names when metadata unavailable."""
    detected = {}
    for param_code, keywords in PARAMETER_KEYWORDS.items():
        for col in columns:
            if any(keyword in col.lower() for keyword in keywords):
                detected[col] = param_code
                break
    return detected
```

**Note:** This is a **fallback only**. NetCDF files should always use metadata-based detection.

---

## NetCDF Extraction Workflow (v4.0)

```python
class NetCDFExtractor:
    def extract(self, file_path: Path, metadata_id: int, dataset_path: str) -> list:
        ds = xr.open_dataset(file_path)
        
        # **NEW v4.0**: Get parameter mapping from metadata XML
        param_mapping = extract_parameters_from_metadata(metadata_id, cursor)
        
        if not param_mapping:
            logger.warning("No metadata, using fallback")
            param_mapping = detect_parameters_fallback(list(ds.data_vars))
        
        measurements = []
        for netcdf_var, param_code in param_mapping.items():
            var_data = ds[netcdf_var]
            
            # Extract time-series data
            for i, (time_val, value) in enumerate(zip(times, values)):
                if not np.isnan(value):
                    namespace = 'bodc' if param_code in ['PO4', 'PH', 'NO3', 'SIO4'] else 'custom'
                    
                    measurements.append((
                        timestamp,
                        metadata_id,
                        location_id,
                        param_code,  # From metadata XML!
                        namespace,
                        float(value),
                        'unknown',
                        None,
                        None,
                        1
                    ))
        
        return measurements
```

---

## Supported Parameters (v4.0)

### BODC Namespace Parameters

| Parameter | CF Standard Name | Code | Description |
|-----------|------------------|------|-------------|
| Phosphate | `mole_concentration_of_phosphate_in_sea_water` | PO4 | Nutrient concentration |
| pH | `sea_water_ph_reported_on_total_scale` | PH | Acidity/alkalinity |
| Nitrate | `mole_concentration_of_nitrate_in_sea_water` | NO3 | Nutrient concentration |
| Silicate | `mole_concentration_of_silicate_in_sea_water` | SIO4 | Nutrient concentration |

### Custom Namespace Parameters

| Parameter | CF Standard Name | Code |
|-----------|------------------|------|
| Temperature | `sea_water_temperature` | TEMP |
| Salinity | `sea_water_practical_salinity` | PSAL |
| Pressure | `sea_water_pressure` | PRES |
| Oxygen | `mole_concentration_of_dissolved_molecular_oxygen_in_sea_water` | DOXY |
| Chlorophyll | `mass_concentration_of_chlorophyll_a_in_sea_water` | CPHL |
| Turbidity | `sea_water_turbidity` | TURB |

---

## Location Handling

```python
def get_or_create_location(cursor, latitude: float, longitude: float, metadata_id: int) -> Optional[int]:
    # Validate coordinates
    if not (-90 <= latitude <= 90) or not (-180 <= longitude <= 180):
        return None
    
    # Find existing location (within 11 meters)
    cursor.execute("""
        SELECT id 
        FROM locations 
        WHERE ABS(latitude - %s) < 0.0001 
          AND ABS(longitude - %s) < 0.0001
        LIMIT 1
    """, (latitude, longitude))
    
    result = cursor.fetchone()
    if result:
        return result[0]
    
    # Create new location
    cursor.execute("""
        INSERT INTO locations (latitude, longitude)
        VALUES (%s, %s)
        RETURNING id
    """, (latitude, longitude))
    
    return cursor.fetchone()[0]
```

---

## Post-Processing

### Populate Parameters Table

After measurements are loaded, populate the parameters table:

```bash
python scripts/populate_parameters_from_measurements.py
```

This script:
- Extracts unique parameter codes from measurements
- Creates parameter records with proper UUIDs
- Links to parameter_mappings for enriched metadata
- Generates human-readable labels

---

## Troubleshooting

### No Parameters Detected from Metadata

**Symptom:**
```
⚠ No parameters found in metadata, using fallback detection
```

**Cause:** Metadata XML missing contentInfo sections or CF standard_name fields

**Solution:**
1. Check metadata XML has `<gmd:contentInfo>` sections
2. Verify CF standard_name present in `<gmd:name>` elements
3. Fallback detection will activate automatically

### Parameter Code Not in CF Mapping

**Symptom:**
```
⚠ CF standard_name 'custom_parameter' not in mapping table
```

**Cause:** New parameter type not in `CF_STANDARD_NAME_TO_CODE`

**Solution:**
Add mapping to script:

```python
CF_STANDARD_NAME_TO_CODE = {
    # ... existing mappings
    'custom_parameter': 'CUSTOM_CODE',
}
```

### Metadata XML Parse Error

**Symptom:**
```
❌ Failed to extract parameters from metadata: XML parse error
```

**Cause:** Malformed XML in `metadata.metadata_content`

**Solution:**
1. Check XML validity: `cat metadata.xml | xmllint --noout -`
2. Fix XML encoding issues
3. Script will fall back to column-based detection

---

## Performance Optimization

### Batch Size Tuning

```python
# Default: 1000 rows per batch
inserter = BatchInserter(cursor, batch_size=1000)

# Large datasets (>1M measurements)
inserter = BatchInserter(cursor, batch_size=5000)
```

### Index Optimization

```sql
CREATE INDEX IF NOT EXISTS idx_measurements_time ON measurements(time);
CREATE INDEX IF NOT EXISTS idx_measurements_parameter ON measurements(parameter_code);
CREATE INDEX IF NOT EXISTS idx_measurements_location ON measurements(location_id);
CREATE INDEX IF NOT EXISTS idx_locations_coords ON locations(latitude, longitude);
```

---

## Version History

### v4.0 (January 2026)
- ✅ Metadata-first parameter detection using CF standard_name
- ✅ Eliminates parameter misidentification (fixes Issues #5-8 root cause)
- ✅ XML parsing with ISO 19115-3 namespace support
- ✅ CF Standard Name to parameter code mapping table (~30 entries)
- ✅ Fallback to column-based detection when metadata unavailable

### v3.3 (January 2026)
- Smart PH/phosphorus disambiguation (now redundant with v4.0)
- Value-based parameter detection
- Automatic PO4 code mapping

### v3.2 (December 2025)
- PostGIS removed (pure SQL)
- Updated connection config
- Schema compatibility fixes

### v3.1 (Previous)
- Multi-parameter CSV extraction
- QC column filtering
- Unit inference

### v3.0 (Previous)
- NetCDF time parsing
- Batch processing
- Error recovery

---

## References

- [CF Standard Name Table](http://cfconventions.org/Data/cf-standard-names/current/build/cf-standard-name-table.html)
- [BODC Parameter Vocabulary](https://vocab.nerc.ac.uk/collection/P01/current/)
- [ISO 19115-3 Metadata Standard](https://www.iso.org/standard/32579.html)
- [Project README](../README.md)
- [Data Quality Issues and Fixes](DATA_QUALITY_ISSUES_AND_FIXES.md) - Issue #9: Root Cause Analysis
- [Database Schema Documentation](database_schema.md)
- [init.sql - Schema Definition](../init.sql)

---

*Last Updated: January 8, 2026*  
*Script Version: 4.0 (Metadata-Based Parameter Detection)*  
*Maintained by: Huon Channel Marine Analytics Project*