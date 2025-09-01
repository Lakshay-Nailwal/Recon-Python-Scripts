# ReconScripts

A collection of Python scripts for database reconciliation and data analysis.

## Setup

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

## Configuration

The `config.env` file contains all the necessary configuration for database connections and API tokens.

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

## Available Scripts

### UCODE_NEVER_INWARDED_IN_DESTINATION
Analyzes purchase issues and checks for missing inward invoices.

```bash
cd UCODE_NEVER_INWARDED_IN_DESTINATION
python3 ucodeNeverInward.py
```

### DC_CREATED_STR_NOT_CREATED
Checks for DC created but STR not created scenarios.

```bash
cd DC_CREATED_STR_NOT_CREATED
python3 dcCreatedStrNotCreated.py
```

## Output

Scripts generate CSV files in their respective `CSV_FILES` directories with the analysis results.

## Threading

Most scripts use ThreadPoolExecutor for parallel processing. You can configure the number of workers by setting `MAX_WORKERS` in your config file.

## Error Handling

Scripts include comprehensive error handling and logging. Check the console output for any errors during execution.

## Security Notes

- Never commit `config.env` to version control
- The `config.env` file is already in `.gitignore`
- Use strong passwords and rotate them regularly
- Consider using environment variables for production deployments

## Troubleshooting

### Common Issues

1. **Database Connection Errors**
   - Verify database credentials in `config.env`
   - Check network connectivity to database hosts
   - Ensure database user has proper permissions

2. **Import Errors**
   - Make sure all dependencies are installed: `pip install -r requirements.txt`
   - Check that you're running scripts from the correct directory

3. **Environment Variable Issues**
   - Ensure `config.env` is in the root directory of the project
   - Verify the file format (no extra spaces, proper JSON syntax)

### Getting Help

If you encounter issues:
1. Check the console output for error messages
2. Verify your configuration in `config.env`
3. Ensure all dependencies are installed
4. Check database connectivity
