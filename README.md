# Clinical Trials Data Pipeline for IPF Research

A comprehensive data pipeline that collects, processes, and enriches clinical trial data for Idiopathic Pulmonary Fibrosis (IPF) from multiple sources and integrates with medical terminology libraries.

## Features

### Data Sources
- **ClinicalTrials.gov** - US clinical trials database (API v2)
- **EU Clinical Trials** - European clinical trials registry

### Medical Library Integrations
- **RxNorm** - Drug name normalization and standardization
- **MedDRA** - Medical Dictionary for Regulatory Activities for adverse event coding
- **FDA OpenFDA** - Additional drug and adverse event information

### Key Capabilities
- ✅ Automated data collection from multiple sources
- ✅ Drug name normalization using RxNorm
- ✅ Adverse event extraction and MedDRA coding
- ✅ Data deduplication across sources
- ✅ Multi-format export (CSV, JSON, SQLite)
- ✅ Comprehensive summary statistics
- ✅ Configurable rate limiting and retry logic

## Project Structure

```
data_pipeline/
├── data_sources/           # Data collection clients
│   ├── clinical_trials_gov.py
│   └── eu_clinical_trials.py
├── medical_libraries/      # Medical terminology integration
│   ├── rxnorm_client.py
│   └── meddra_client.py
├── pipeline/              # Data processing pipeline
│   ├── data_processor.py
│   └── orchestrator.py
├── storage/               # Data persistence
│   └── database.py
├── config.py             # Configuration management
├── main.py               # Entry point
└── requirements.txt      # Python dependencies
```

## Installation

### Prerequisites
- Python 3.8 or higher
- pip package manager

### Setup

1. **Clone or navigate to the project directory**
```bash
cd D:\CT_FDA\data_pipeline
```

2. **Install dependencies**
```bash
pip install -r requirements.txt
```

3. **Configure environment (optional)**
```bash
cp .env.example .env
# Edit .env with your settings
```

## Usage

### Basic Usage

Run the pipeline with default settings:
```bash
python main.py
```

### Custom Collection Limits

Collect specific numbers of trials:
```bash
python main.py --us-trials 100 --eu-trials 50
```

### Rate Limiting

Adjust request delays to respect API limits:
```bash
python main.py --request-delay 2.0
```

### Output Configuration

Specify custom output directory:
```bash
python main.py --output-dir ../my_data
```

### Complete Example

```bash
python main.py \
  --us-trials 200 \
  --eu-trials 100 \
  --request-delay 1.5 \
  --output-dir ../ipf_data \
  --log-level INFO
```

## Command Line Options

| Option | Description | Default |
|--------|-------------|---------|
| `--us-trials` | Maximum US trials to collect | 100 |
| `--eu-trials` | Maximum EU trials to collect | 50 |
| `--request-delay` | Delay between requests (seconds) | 1.0 |
| `--output-dir` | Output directory for exports | ../data |
| `--log-level` | Logging level (DEBUG, INFO, WARNING, ERROR) | INFO |

## Output Files

The pipeline generates multiple output files:

### 1. CSV Export
**File**: `clinical_trials_YYYYMMDD_HHMMSS.csv`

Basic trial information in CSV format, suitable for Excel/analysis tools.

**Columns**:
- trial_id, source, title, condition, status, phase
- study_type, sponsor, start_date, completion_date, enrollment
- url, countries, interventions, normalized_drug_count
- adverse_event_count, meddra_codes

### 2. Detailed JSON Export
**File**: `clinical_trials_detailed_YYYYMMDD_HHMMSS.json`

Complete trial data including nested structures:
- Normalized drug information (RxNorm data)
- Adverse events with MedDRA coding
- Full descriptions and eligibility criteria

### 3. Database Export
**File**: `clinical_trials_db_export_YYYYMMDD_HHMMSS.csv`

Direct export from SQLite database.

### 4. Pipeline Summary
**File**: `pipeline_summary_YYYYMMDD_HHMMSS.json`

Execution statistics and summary:
- Total trials collected and stored
- Status and phase distributions
- Country distribution
- Drug distribution
- Adverse events analysis
- Enrollment statistics

### 5. SQLite Database
**File**: `clinical_trials.db`

Persistent database with indexed data for querying.

## Data Schema

### EnrichedClinicalTrial

```python
{
    "trial_id": "NCT12345678",
    "source": "clinicaltrials.gov",
    "title": "Trial Title",
    "condition": "Idiopathic Pulmonary Fibrosis",
    "status": "COMPLETED",
    "phase": "PHASE3",
    "study_type": "INTERVENTIONAL",
    "sponsor": "Sponsor Name",
    "start_date": "2020-01-01",
    "completion_date": "2023-12-31",
    "enrollment": 500,
    "url": "https://clinicaltrials.gov/study/NCT12345678",
    "description": "Brief summary...",
    "eligibility_criteria": "Inclusion/exclusion criteria...",
    "countries": ["United States", "Canada", "Germany"],
    "interventions": ["pirfenidone", "placebo"],
    "normalized_drugs": {
        "pirfenidone": {
            "rxcui": "745467",
            "name": "pirfenidone",
            "brand_names": ["Esbriet"],
            "generic_name": "pirfenidone",
            "ingredients": ["pirfenidone"]
        }
    },
    "adverse_events": [...],
    "meddra_codes": ["10041144", "10027159"],
    "data_collection_date": "2026-01-20",
    "processing_timestamp": "2026-01-20T22:53:01.772000"
}
```

## API Information

### ClinicalTrials.gov API v2
- Base URL: `https://clinicaltrials.gov/api/v2/studies`
- Rate Limit: ~1 request/second recommended
- Documentation: https://clinicaltrials.gov/data-api/api

### RxNorm API
- Base URL: `https://rxnav.nlm.nih.gov/REST`
- No authentication required
- Documentation: https://lhncbc.nlm.nih.gov/RxNav/APIs/

### FDA OpenFDA
- Base URL: `https://api.fda.gov/drug/label.json`
- No authentication required
- Documentation: https://open.fda.gov/apis/

## Configuration

### Environment Variables

Create a `.env` file with:

```bash
# API Configuration
CLINICAL_TRIALS_API_BASE_URL=https://clinicaltrials.gov/api/v2/studies
EU_CLINICAL_TRIALS_BASE_URL=https://euclinicaltrials.eu
RXNORM_API_BASE_URL=https://rxnav.nlm.nih.gov/REST
MEDDRA_API_BASE_URL=https://api.fda.gov/drug/label.json

# Rate Limiting
REQUEST_DELAY=1.0
MAX_RETRIES=3

# Database
DATABASE_URL=sqlite:///clinical_trials.db
MONGODB_URI=mongodb://localhost:27017/clinical_trials

# Output
OUTPUT_DIR=../data
LOG_LEVEL=INFO
```

## Database Schema

### SQLite Table: trials

```sql
CREATE TABLE trials (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    trial_id TEXT UNIQUE NOT NULL,
    source TEXT NOT NULL,
    title TEXT NOT NULL,
    condition TEXT,
    status TEXT,
    phase TEXT,
    study_type TEXT,
    sponsor TEXT,
    start_date TEXT,
    completion_date TEXT,
    enrollment INTEGER,
    url TEXT,
    description TEXT,
    eligibility_criteria TEXT,
    countries TEXT,           -- JSON array
    interventions TEXT,       -- JSON array
    normalized_drugs TEXT,    -- JSON object
    adverse_events TEXT,      -- JSON array
    meddra_codes TEXT,        -- JSON array
    data_collection_date TEXT,
    processing_timestamp TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

## Troubleshooting

### Common Issues

**Issue**: RxNorm API returns 404 for some drugs
- **Cause**: Drug name not in RxNorm database or non-standard naming
- **Solution**: These are logged as warnings; the pipeline continues

**Issue**: EU Clinical Trials returns no results
- **Cause**: Website structure may have changed or search limitations
- **Solution**: Check EU Clinical Trials website manually; US trials still collected

**Issue**: Rate limiting errors (429)
- **Cause**: Too many requests to API
- **Solution**: Increase `--request-delay` parameter

**Issue**: Database locked error
- **Cause**: Another process accessing database
- **Solution**: Close other connections or wait for completion

## Performance

### Expected Runtime
- **10 trials**: ~1 minute
- **100 trials**: ~5-10 minutes
- **1000 trials**: ~30-60 minutes

Runtime depends on:
- Number of interventions per trial (drug normalization)
- Network latency
- Rate limiting delays

### Optimization Tips
1. Use appropriate `--request-delay` for your network
2. Start with small collections to test
3. Run during off-peak hours for better API response
4. Monitor logs for errors/warnings

## Examples

### Example 1: Quick Test
```bash
python main.py --us-trials 10 --eu-trials 5
```

### Example 2: Comprehensive Collection
```bash
python main.py --us-trials 500 --eu-trials 100 --request-delay 1.5
```

### Example 3: Production Run
```bash
python main.py \
  --us-trials 1000 \
  --eu-trials 200 \
  --request-delay 2.0 \
  --output-dir /data/ipf_trials \
  --log-level INFO
```

## Data Quality

### Validation
- Trial IDs are checked for uniqueness
- Dates are validated and normalized
- Countries and interventions are extracted and cleaned
- Duplicate trials across sources are merged

### Completeness
- Some fields may be empty if not provided by source
- Drug normalization depends on RxNorm coverage
- Adverse events extracted from text descriptions

## License

This project is for research purposes.

## Support

For issues or questions, please refer to the documentation or check the logs in `clinical_trials_pipeline.log`.

## Changelog

### Version 1.0.0 (2026-01-20)
- Initial release
- ClinicalTrials.gov API v2 integration
- EU Clinical Trials scraper
- RxNorm drug normalization
- MedDRA adverse event coding
- SQLite storage
- Multiple export formats
- Comprehensive logging and error handling
