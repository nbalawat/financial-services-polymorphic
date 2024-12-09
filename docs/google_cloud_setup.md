# Setting Up Google Cloud Credentials

To use Google Cloud services (Firestore, Spanner, AlloyDB, BigTable) in the benchmark, follow these steps:

1. **Create a Google Cloud Project**:
   - Go to the [Google Cloud Console](https://console.cloud.google.com)
   - Create a new project or select an existing one
   - Note your Project ID

2. **Enable Required APIs**:
   - Go to "APIs & Services" > "Library"
   - Search for and enable:
     - Cloud Firestore API
     - Cloud Spanner API
     - AlloyDB API
     - Cloud Bigtable API

3. **Create a Service Account**:
   - Go to "IAM & Admin" > "Service Accounts"
   - Click "Create Service Account"
   - Name it something like "benchmark-service-account"
   - Grant the following roles:
     - Cloud Firestore User
     - Cloud Spanner Database User
     - AlloyDB Client
     - Bigtable User

4. **Generate Service Account Key**:
   - Select your service account
   - Go to "Keys" tab
   - Click "Add Key" > "Create New Key"
   - Choose JSON format
   - Download the key file

5. **Configure Environment**:
   - Copy `.env.example` to `.env`
   - Set `GOOGLE_APPLICATION_CREDENTIALS` to the path of your downloaded key file
   - Set `GOOGLE_CLOUD_PROJECT` to your Project ID

6. **Initialize Firestore**:
   - Go to Firestore in the Google Cloud Console
   - Choose Native mode
   - Select a location close to you
   - Create the database

## Security Notes:
- Never commit your service account key to version control
- Keep your key file secure and restrict its permissions
- Consider using environment variables or secret management in production

## Cost Considerations:
- Monitor your usage in the Google Cloud Console
- Set up billing alerts
- Consider using the free tier for testing
- Clean up resources after benchmarking
