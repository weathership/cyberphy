# Iceberg Browser - CloudTrail Events UI

A simple web-based UI for browsing and searching CloudTrail events stored in Apache Iceberg tables.

## Features

- **Real-time Event Viewing**: Browse CloudTrail events with pagination
- **Advanced Filtering**: Filter by event name, user identity, source IP, and region
- **Event Details**: Click on any event to view complete details
- **Statistics Dashboard**: View summary statistics and event distributions
- **Responsive Design**: Clean, modern UI that works on desktop and mobile

## Prerequisites

Ensure the following services are running:
- PostgreSQL (localhost:5438) - Iceberg catalog
- MinIO (localhost:9010) - S3-compatible storage
- CloudTrail events have been ingested into Iceberg

## Installation

1. Install Flask (if not already installed):
```bash
pip install flask
```

Or use the project dependencies:
```bash
pip install -e .
```

## Usage

### Start the Web UI

```bash
python iceberg_browser.py
```

The UI will be available at: **http://localhost:5050**

### Browse Events

1. Open http://localhost:5050 in your browser
2. View statistics at the top of the page
3. Use filters to search for specific events
4. Click on any event row to see detailed information
5. Use pagination controls to navigate through results

### Filter Options

- **Event Name**: Filter by CloudTrail event type (e.g., "StartInstances", "CreateBucket")
- **User Identity**: Filter by IAM user or role
- **Source IP**: Filter by source IP address
- **Region**: Filter by AWS region

### API Endpoints

The browser provides a REST API for programmatic access:

- `GET /api/tables` - List all Iceberg tables
- `GET /api/schema` - Get table schema
- `GET /api/stats` - Get table statistics
- `GET /api/summary` - Get aggregated summary data
- `GET /api/events?limit=N&offset=M` - Query events with pagination
- `GET /api/event/<event_id>` - Get detailed event information

### Example API Usage

```bash
# Get summary statistics
curl http://localhost:5050/api/summary

# Get events with filters
curl "http://localhost:5050/api/events?event_name=StartInstances&limit=10"

# Get specific event details
curl http://localhost:5050/api/event/<event-id>
```

## Configuration

The Iceberg catalog configuration is defined in `iceberg_browser.py`:

```python
CATALOG_CONFIG = {
    "uri": "postgresql://postgres:postgres@localhost:5438/iceberg",
    "s3.endpoint": "http://localhost:9010",
    "s3.access-key-id": "minioadmin",
    "s3.secret-access-key": "minioadmin",
    "s3.path-style-access": "true",
    "warehouse": "s3://cyberphy/iceberg/warehouse",
}
```

Modify these settings if your services are running on different hosts/ports.

## Architecture

```
┌─────────────┐
│   Browser   │
│  (HTML/JS)  │
└──────┬──────┘
       │ HTTP/REST
┌──────▼──────────┐
│  Flask Server   │
│ iceberg_browser │
└──────┬──────────┘
       │ PyIceberg
┌──────▼──────────┐
│ Iceberg Catalog │
│   PostgreSQL    │
└──────┬──────────┘
       │
┌──────▼──────────┐
│  Iceberg Data   │
│     MinIO       │
└─────────────────┘
```

## Troubleshooting

### Connection Errors

If you see connection errors, verify services are running:

```bash
# Check PostgreSQL
nc -zv localhost 5438

# Check MinIO
nc -zv localhost 9010

# Or use devenv
devenv up
```

### No Events Displayed

1. Verify the Flink DataGen job is running and generating events
2. Check that events are being written to the Iceberg table:
   ```bash
   psql -h localhost -p 5438 -U postgres -d iceberg -c "SELECT * FROM iceberg_tables;"
   ```
3. Check MinIO console at http://localhost:9011 (user: minioadmin, pass: minioadmin)

### Performance Issues

For large datasets:
- Use filters to narrow down results
- Adjust the page size in the code (default: 50 events per page)
- Consider adding Iceberg partition filters for better performance

## Development

### Project Structure

```
cybersec/
├── iceberg_browser.py      # Flask application and API endpoints
└── templates/
    └── index.html          # Web UI (HTML + CSS + JavaScript)
```

### Adding New Features

To add new filter fields:
1. Add the filter input in `templates/index.html`
2. Update `applyFilters()` JavaScript function
3. Handle the filter in the `/api/events` endpoint in `iceberg_browser.py`

### Extending the API

Add new endpoints in `iceberg_browser.py`:

```python
@app.route("/api/custom_endpoint")
def custom_endpoint():
    # Your logic here
    return jsonify({"data": "value"})
```

## Security Notes

⚠️ **This is a development tool.** For production use:

- Add authentication and authorization
- Use HTTPS
- Implement rate limiting
- Add input validation and sanitization
- Use environment variables for sensitive configuration
- Restrict CORS policies

## License

Apache License 2.0 - See LICENSE file for details
