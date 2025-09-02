# ReconScripts

A comprehensive collection of Python scripts for database reconciliation, data analysis, and business intelligence across multiple tenant databases. This toolkit provides automated solutions for identifying data inconsistencies, validating business rules, and generating detailed reports.

## 🚀 Features

- **Multi-tenant Database Support**: Works across Arsenal and Thea warehouse databases
- **Parallel Processing**: ThreadPoolExecutor for efficient concurrent operations
- **Comprehensive Reporting**: CSV output with detailed analysis results
- **Error Handling**: Robust error handling and logging
- **Modular Design**: Easy to extend and customize for specific use cases

## 📋 Prerequisites

- Python 3.7+
- MySQL/MariaDB access
- Network connectivity to database hosts

## 🛠️ Setup

1. **Clone the repository**
   ```bash
   git clone git@github.com:Lakshay-Nailwal/Recon-Python-Scripts.git
   cd Recon-Python-Scripts
   ```

2. **Install dependencies**
   ```bash
   pip install -r requirements.txt
   ```

3. **Configure environment variables**
   ```bash
   # Copy the sample config file
   cp config.env.sample config.env
   
   # Edit config.env with your actual database credentials
   nano config.env
   ```

## ⚙️ Configuration

The `config.env` file contains all necessary configuration for database connections and API tokens.

### Required Configuration

- `MERCURY_DB_CONFIG`: JSON configuration for mercury database connection
- `VAULT_DB_CONFIG`: JSON configuration for vault database connection  
- `PARTNER_DB_CONFIG`: JSON configuration for partner database connection
- `PROD_TOKEN`: Production API token

### Database Configuration Format

Each database configuration should be in JSON format with the following fields:
```json
{
  "host": "your-database-host.com",
  "user": "your_username",
  "password": "your_password",
  "port": 3306
}
```

## 📁 Available Scripts

### 🔍 Core Analysis Scripts

#### `runAnyQueryAcrossArsenalAndThea.py`
**Purpose**: Execute custom SQL queries across all tenant databases
- Runs queries on both Arsenal and Thea warehouses
- Supports parallel processing with configurable worker threads
- Outputs results to CSV files

```bash
python3 runAnyQueryAcrossArsenalAndThea.py
```

#### `getAllArsenal.py` & `getAllWarehouse.py`
**Purpose**: Utility scripts to retrieve tenant lists
- Used by other scripts to get available tenants
- Supports filtering and custom queries

### 📊 Reconciliation Scripts

#### `UCODE_NEVER_INWARDED_IN_DESTINATION/`
**Purpose**: Identifies purchase issues where inward invoices are missing
- Analyzes purchase issues created after 2025-08-26
- Checks for missing inward invoices in destination tenants
- Generates detailed CSV reports

```bash
cd UCODE_NEVER_INWARDED_IN_DESTINATION
python3 ucodeNeverInward.py
```

#### `INVALID_INVOICE_IN_PR/`
**Purpose**: Validates invoice references in purchase issues
- Checks invoice validity across tenant boundaries
- Identifies mismatched invoice-tenant relationships
- Excludes regular Easysol purchase types

```bash
cd INVALID_INVOICE_IN_PR
python3 invalidInvoiceInPR.py
```

#### `DC_CREATED_STR_NOT_CREATED/`
**Purpose**: Identifies DC (Delivery Challan) created but STR (Stock Transfer Request) not created
- Cross-references DC and STR creation status
- Helps identify process gaps in stock transfer workflow

```bash
cd DC_CREATED_STR_NOT_CREATED
python3 dcCreatedStrNotCreated.py
```

#### `DUPLICATE_STR_INWARD_INVOICE/`
**Purpose**: Detects duplicate STR entries in inward invoices
- Identifies potential data duplication issues
- Helps maintain data integrity

```bash
cd DUPLICATE_STR_INWARD_INVOICE
python3 duplicateStrInwardInvoice.py
```

#### `PR_SALES_AND_NON_REGULAR_VENDOR/`
**Purpose**: Analyzes PR sales and non-regular vendor types
- Contains two sub-scripts:
  - `prSales.py`: Analyzes PR sales data
  - `nonRegularVendorType.py`: Identifies non-regular vendor types

```bash
cd PR_SALES_AND_NON_REGULAR_VENDOR
python3 prSales.py
python3 nonRegularVendorType.py
```

#### `STR_CREATED_QUANTITY_SAME_AMOUNT_MISMATCH/`
**Purpose**: Identifies STR entries with quantity-amount mismatches
- Validates consistency between quantities and amounts
- Helps identify pricing or calculation errors

```bash
cd STR_CREATED_QUANTITY_SAME_AMOUNT_MISMATCH
python3 strCreatedQunatitySameAmountMismatch.py
```

#### `STR_CREATED_RETURN_QUANTITY_DIFFERENT/`
**Purpose**: Analyzes STR return quantity discrepancies
- Compares created vs returned quantities
- Identifies potential return processing issues

```bash
cd STR_CREATED_RETURN_QUANTITY_DIFFERENT
python3 strCreatedReturnQunatityDifferent.py
```

#### `MULTI_CN_FOR_STR_INWARD/`
**Purpose**: Identifies STR inward invoices with multiple credit/debit notes
- Analyzes inward invoices for StockTransferReturn and ICSReturn types
- Detects cases where multiple credit/debit notes are associated with single inward invoice
- Helps identify potential data inconsistencies in return processing

```bash
cd MULTI_CN_FOR_STR_INWARD
python3 multiCNForStrInward.py
```

#### `NO_CN_FOR_STR_INWARD/`
**Purpose**: Identifies STR inward invoices without corresponding credit/debit notes
- Analyzes inward invoices for StockTransferReturn and ICSReturn types
- Detects missing credit/debit note associations
- Helps identify incomplete return processing workflows

```bash
cd NO_CN_FOR_STR_INWARD
python3 noCNForStrInward.py
```

## 📈 Output and Reports

All scripts generate CSV files in their respective `CSV_FILES` directories with detailed analysis results. Reports include:

- **Data Validation Results**: Pass/fail status for each record
- **Error Details**: Specific issues identified
- **Summary Statistics**: Counts and percentages
- **Cross-tenant Analysis**: Multi-tenant relationship validation

## ⚡ Performance Optimization

### Threading Configuration
Most scripts use ThreadPoolExecutor for parallel processing. You can configure the number of workers:

```python
# In individual scripts
processAllTenants(tenants, max_workers=10)

# Or set globally in config.env
MAX_WORKERS=10
```

### Database Connection Pooling
Scripts implement efficient database connection management:
- Connection reuse where possible
- Proper connection cleanup
- Error handling for connection failures

## 🔧 Utility Scripts

### `baseCodeStructureFile.py`
**Purpose**: Template/base structure for creating new reconciliation scripts
- Provides standard imports and setup
- Includes thread-safe CSV writing functionality
- Contains boilerplate for multi-tenant processing
- Features configurable batch processing and worker threads
- **Usage**: Copy this file as a starting point for new reconciliation scripts

```python
# Key features included:
- ThreadPoolExecutor setup
- Thread-safe CSV operations
- Standard error handling
- Multi-tenant processing framework
- Configurable batch sizes and worker counts
```

### `csv_utils.py`
**Purpose**: CSV file manipulation utilities
- Thread-safe CSV writing
- Batch processing capabilities
- Error handling for file operations

### `getDBConnection.py`
**Purpose**: Database connection management
- Centralized connection creation
- Environment-based configuration
- Error handling and logging

### `pdi.py`
**Purpose**: Partner Detail ID to tenant mapping
- Maintains tenant relationship mappings
- Used by cross-tenant validation scripts

### `token_switcher.py`
**Purpose**: API token management
- Handles token rotation and switching
- Supports multiple environment tokens

## 🛡️ Security and Best Practices

### Security Notes
- Never commit `config.env` to version control
- The `config.env` file is already in `.gitignore`
- Use strong passwords and rotate them regularly
- Consider using environment variables for production deployments

### Error Handling
Scripts include comprehensive error handling and logging:
- Database connection failures
- Query execution errors
- File I/O issues
- Network connectivity problems

## 🔍 Troubleshooting

### Common Issues

1. **Database Connection Errors**
   ```bash
   # Verify database credentials in config.env
   # Check network connectivity to database hosts
   # Ensure database user has proper permissions
   ```

2. **Import Errors**
   ```bash
   # Make sure all dependencies are installed
   pip install -r requirements.txt
   
   # Check that you're running scripts from the correct directory
   ```

3. **Environment Variable Issues**
   ```bash
   # Ensure config.env is in the root directory
   # Verify the file format (no extra spaces, proper JSON syntax)
   # Check for missing quotes in JSON values
   ```

4. **Threading Issues**
   ```bash
   # Reduce max_workers if experiencing connection limits
   # Check database connection pool settings
   # Monitor system resources during execution
   ```

### Performance Tips

1. **Optimize Worker Count**
   - Start with 5-10 workers for most scripts
   - Increase based on database capacity and network bandwidth
   - Monitor database connection limits

2. **Batch Processing**
   - Large datasets are processed in batches
   - Adjust batch sizes based on available memory
   - Use appropriate CSV chunk sizes

3. **Database Optimization**
   - Ensure proper indexes on queried columns
   - Monitor query execution plans
   - Consider read replicas for heavy analysis

## 📞 Getting Help

If you encounter issues:

1. **Check Console Output**: Look for detailed error messages
2. **Verify Configuration**: Ensure `config.env` is properly set up
3. **Test Dependencies**: Run `pip list` to verify all packages are installed
4. **Database Connectivity**: Test connections manually if needed
5. **Check Logs**: Review any generated log files for additional details

## 🤝 Contributing

To contribute to this project:

1. Fork the repository
2. Create a feature branch
3. **Use the base template**: Copy `baseCodeStructureFile.py` as a starting point for new scripts
4. Add your scripts following the existing pattern
5. Update this README with new script documentation
6. Submit a pull request

### 📝 Creating New Reconciliation Scripts

When creating a new reconciliation script:

1. **Copy the base template**:
   ```bash
   cp baseCodeStructureFile.py NEW_SCRIPT_NAME.py
   ```

2. **Customize the template**:
   - Update the `process_tenant()` function with your specific logic
   - Modify SQL queries and data processing logic
   - Adjust batch sizes and worker counts as needed
   - Add specific error handling for your use case

3. **Follow the naming convention**:
   - Use descriptive folder names in UPPERCASE
   - Create a `CSV_FILES/` subdirectory for output
   - Use consistent naming patterns

4. **Test thoroughly**:
   - Test with a small subset of tenants first
   - Verify CSV output format and content
   - Check error handling scenarios

## 📄 License

This project is proprietary and confidential. Please ensure compliance with your organization's data handling policies.

---

**Note**: This toolkit is designed for internal business intelligence and data reconciliation. Always ensure you have proper authorization before running scripts against production databases.
