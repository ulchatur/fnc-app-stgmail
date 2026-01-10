import datetime
import json
import requests
import os
import io
import logging
import traceback
import csv
from azure.storage.blob import BlobServiceClient
from azure.communication.email import EmailClient
import azure.functions as func

# Setup logging
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

def get_access_token():
    """Get Azure access token using service principal credentials"""
    try:
        logger.info("Starting token acquisition...")
        
        TENANT_ID = os.environ.get("TENANT_ID")
        CLIENT_ID = os.environ.get("CLIENT_ID")
        CLIENT_SECRET = os.environ.get("CLIENT_SECRET")
        
        # Detailed validation
        if not TENANT_ID:
            raise ValueError("TENANT_ID environment variable is not set")
        if not CLIENT_ID:
            raise ValueError("CLIENT_ID environment variable is not set")
        if not CLIENT_SECRET:
            raise ValueError("CLIENT_SECRET environment variable is not set")
        
        logger.info(f"TENANT_ID: {TENANT_ID[:8]}... (length: {len(TENANT_ID)})")
        logger.info(f"CLIENT_ID: {CLIENT_ID[:8]}... (length: {len(CLIENT_ID)})")
        logger.info(f"CLIENT_SECRET: {'*' * 8}... (length: {len(CLIENT_SECRET)})")
        
        url = f"https://login.microsoftonline.com/{TENANT_ID}/oauth2/token"
        payload = {
            "grant_type": "client_credentials",
            "client_id": CLIENT_ID,
            "client_secret": CLIENT_SECRET,
            "resource": "https://management.azure.com/"
        }
        
        logger.info(f"Requesting token from: {url}")
        response = requests.post(url, data=payload, timeout=30)
        
        if response.status_code != 200:
            logger.error(f"Token request failed with status {response.status_code}")
            logger.error(f"Response: {response.text}")
            response.raise_for_status()
        
        token_data = response.json()
        logger.info("Access token acquired successfully")
        return token_data["access_token"]
        
    except requests.exceptions.Timeout as e:
        logger.error(f"Timeout while getting access token: {str(e)}")
        raise Exception(f"Authentication timeout: {str(e)}")
    except requests.exceptions.RequestException as e:
        logger.error(f"Request error getting access token: {str(e)}")
        if hasattr(e.response, 'text'):
            logger.error(f"Error response: {e.response.text}")
        raise Exception(f"Authentication failed: {str(e)}")
    except KeyError as e:
        logger.error(f"Missing key in token response: {str(e)}")
        raise Exception(f"Invalid token response: {str(e)}")
    except Exception as e:
        logger.error(f"Unexpected error getting access token: {str(e)}")
        logger.error(f"Error type: {type(e).__name__}")
        logger.error(f"Traceback: {traceback.format_exc()}")
        raise

def get_previous_month_range():
    """Calculate the first and last day of the previous month"""
    try:
        today = datetime.date.today()
        first_day_this_month = today.replace(day=1)
        last_day_prev_month = first_day_this_month - datetime.timedelta(days=1)
        first_day_prev_month = last_day_prev_month.replace(day=1)
        
        start_date = first_day_prev_month.isoformat()
        end_date = last_day_prev_month.isoformat()
        
        logger.info(f"Date range calculated: {start_date} to {end_date}")
        return start_date, end_date
        
    except Exception as e:
        logger.error(f"Error calculating date range: {str(e)}")
        raise

def get_all_subscriptions(token):
    """Fetch all subscriptions accessible to the service principal"""
    try:
        logger.info("Fetching subscriptions...")
        url = "https://management.azure.com/subscriptions?api-version=2020-01-01"
        headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json"
        }
        
        response = requests.get(url, headers=headers, timeout=30)
        
        if response.status_code != 200:
            logger.error(f"Subscription fetch failed with status {response.status_code}")
            logger.error(f"Response: {response.text}")
            response.raise_for_status()
        
        subscriptions = response.json().get("value", [])
        logger.info(f"Found {len(subscriptions)} subscriptions")
        
        if not subscriptions:
            logger.warning("No subscriptions found for this service principal")
        else:
            for sub in subscriptions[:3]:  # Log first 3 subscriptions
                logger.info(f"  - {sub.get('displayName')} ({sub.get('subscriptionId')})")
        
        return subscriptions
        
    except requests.exceptions.Timeout as e:
        logger.error(f"Timeout fetching subscriptions: {str(e)}")
        raise Exception(f"Subscription fetch timeout: {str(e)}")
    except requests.exceptions.RequestException as e:
        logger.error(f"Error fetching subscriptions: {str(e)}")
        if hasattr(e.response, 'text'):
            logger.error(f"Error response: {e.response.text}")
        raise Exception(f"Failed to fetch subscriptions: {str(e)}")
    except Exception as e:
        logger.error(f"Unexpected error fetching subscriptions: {str(e)}")
        logger.error(f"Traceback: {traceback.format_exc()}")
        raise

def fetch_cost_for_subscription(token, subscription_id, start_date, end_date):
    """Fetch cost data for a specific subscription"""
    try:
        logger.info(f"Fetching cost for subscription: {subscription_id}")
        
        url = f"https://management.azure.com/subscriptions/{subscription_id}/providers/Microsoft.CostManagement/query?api-version=2023-03-01"
        headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json"
        }
        
        body = {
            "type": "ActualCost",
            "timeframe": "Custom",
            "timePeriod": {
                "from": start_date,
                "to": end_date
            },
            "dataset": {
                "granularity": "None",
                "aggregation": {
                    "totalCost": {
                        "name": "Cost",
                        "function": "Sum"
                    }
                }
            }
        }
        
        response = requests.post(url, headers=headers, json=body, timeout=60)
        
        if response.status_code != 200:
            logger.warning(f"Cost fetch failed for {subscription_id} with status {response.status_code}")
            logger.warning(f"Response: {response.text}")
            return {"properties": {"rows": [], "columns": []}}
        
        cost_data = response.json()
        rows = cost_data.get("properties", {}).get("rows", [])
        logger.info(f"  Cost data retrieved: {len(rows)} rows")
        
        return cost_data
        
    except requests.exceptions.Timeout as e:
        logger.warning(f"Timeout fetching cost for {subscription_id}: {str(e)}")
        return {"properties": {"rows": [], "columns": []}}
    except Exception as e:
        logger.warning(f"Error fetching cost for {subscription_id}: {str(e)}")
        return {"properties": {"rows": [], "columns": []}}

def generate_csv(all_costs_data, start_date, end_date):
    """Generate CSV with all subscriptions cost data"""
    try:
        logger.info("Generating CSV file...")
        
        csv_buffer = io.StringIO()
        csv_writer = csv.writer(csv_buffer)
        
        # Write headers
        headers = ["Subscription Name", "Subscription ID", "From Date", "To Date", "Total Cost (USD)", "Status"]
        csv_writer.writerow(headers)
        
        total_cost_all = 0.0
        
        # Write data for each subscription
        for sub_data in all_costs_data:
            subscription_name = sub_data["subscription_name"]
            subscription_id = sub_data["subscription_id"]
            cost_data = sub_data["cost_data"]
            
            # Extract cost
            rows = cost_data.get("properties", {}).get("rows", [])
            if not rows or len(rows) == 0:
                total_cost = 0.0
                status = "No usage data"
            else:
                total_cost = float(rows[0][0]) if len(rows[0]) > 0 else 0.0
                status = "Active" if total_cost > 0 else "No charges"
            
            total_cost_all += total_cost
            
            csv_writer.writerow([
                subscription_name,
                subscription_id,
                start_date,
                end_date,
                round(total_cost, 2),
                status
            ])
        
        # Add total row
        csv_writer.writerow([])
        csv_writer.writerow([
            "TOTAL",
            "",
            "",
            "",
            round(total_cost_all, 2),
            ""
        ])
        
        # Add summary info
        csv_writer.writerow([])
        csv_writer.writerow([f"Report Generated: {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"])
        csv_writer.writerow([f"Total Subscriptions: {len(all_costs_data)}"])
        
        csv_content = csv_buffer.getvalue()
        csv_buffer.close()
        
        logger.info(f"CSV file generated successfully ({len(all_costs_data)} subscriptions, Total: ${round(total_cost_all, 2)})")
        return csv_content, total_cost_all
        
    except Exception as e:
        logger.error(f"Error generating CSV: {str(e)}")
        logger.error(f"Traceback: {traceback.format_exc()}")
        raise

def upload_to_blob_storage(csv_content, filename):
    """Upload CSV file to Azure Blob Storage"""
    try:
        logger.info("Uploading CSV to Azure Blob Storage...")
        
        # Get Blob Storage connection string from environment variable
        STORAGE_CONNECTION_STRING = os.environ.get("AZURE_STORAGE_CONNECTION_STRING")
        CONTAINER_NAME = os.environ.get("BLOB_CONTAINER_NAME", "azure-cost-reports")
        
        if not STORAGE_CONNECTION_STRING:
            raise ValueError("AZURE_STORAGE_CONNECTION_STRING environment variable is not set")
        
        logger.info(f"Container Name: {CONTAINER_NAME}")
        logger.info(f"Blob Name: {filename}")
        
        # Create BlobServiceClient
        blob_service_client = BlobServiceClient.from_connection_string(STORAGE_CONNECTION_STRING)
        
        # Get container client (create container if it doesn't exist)
        container_client = blob_service_client.get_container_client(CONTAINER_NAME)
        try:
            container_client.create_container()
            logger.info(f"Container '{CONTAINER_NAME}' created")
        except Exception as e:
            if "ContainerAlreadyExists" in str(e):
                logger.info(f"Container '{CONTAINER_NAME}' already exists")
            else:
                logger.warning(f"Container check warning: {str(e)}")
        
        # Get blob client and upload
        blob_client = blob_service_client.get_blob_client(container=CONTAINER_NAME, blob=filename)
        
        # Upload CSV content
        blob_client.upload_blob(csv_content, overwrite=True)
        
        blob_url = blob_client.url
        logger.info(f"✓ CSV uploaded successfully to: {blob_url}")
        
        return blob_url
        
    except ValueError as ve:
        logger.error(f"Configuration error: {str(ve)}")
        raise
    except Exception as e:
        logger.error(f"Error uploading to blob storage: {str(e)}")
        logger.error(f"Traceback: {traceback.format_exc()}")
        raise

def send_email_with_acs(blob_url, filename, start_date, end_date, total_cost, subscription_count):
    """Send email notification using Azure Communication Service"""
    try:
        logger.info("Sending email via Azure Communication Service...")
        
        # Get ACS connection string and sender email from environment variables
        ACS_CONNECTION_STRING = os.environ.get("ACS_CONNECTION_STRING")
        SENDER_EMAIL = os.environ.get("ACS_SENDER_EMAIL")  # Format: DoNotReply@yourdomain.com
        RECIPIENT_EMAIL = os.environ.get("RECIPIENT_EMAIL", "vardhanullas7@gmail.com")
        
        if not ACS_CONNECTION_STRING:
            raise ValueError("ACS_CONNECTION_STRING environment variable is not set")
        if not SENDER_EMAIL:
            raise ValueError("ACS_SENDER_EMAIL environment variable is not set")
        
        logger.info(f"Sender: {SENDER_EMAIL}")
        logger.info(f"Recipient: {RECIPIENT_EMAIL}")
        
        # Create EmailClient
        email_client = EmailClient.from_connection_string(ACS_CONNECTION_STRING)
        
        # Prepare email content
        subject = f"Azure Cost Report - {start_date} to {end_date}"
        
        html_content = f"""
        <html>
            <body style="font-family: Arial, sans-serif; line-height: 1.6; color: #333;">
                <div style="max-width: 600px; margin: 0 auto; padding: 20px;">
                    <h2 style="color: #0078D4; border-bottom: 3px solid #0078D4; padding-bottom: 10px;">
                        Azure Cost Report
                    </h2>
                    
                    <div style="background-color: #f5f5f5; padding: 15px; border-radius: 5px; margin: 20px 0;">
                        <h3 style="margin-top: 0; color: #0078D4;">Report Summary</h3>
                        <table style="width: 100%; border-collapse: collapse;">
                            <tr>
                                <td style="padding: 8px 0;"><strong>Period:</strong></td>
                                <td style="padding: 8px 0;">{start_date} to {end_date}</td>
                            </tr>
                            <tr>
                                <td style="padding: 8px 0;"><strong>Total Subscriptions:</strong></td>
                                <td style="padding: 8px 0;">{subscription_count}</td>
                            </tr>
                            <tr>
                                <td style="padding: 8px 0;"><strong>Total Cost:</strong></td>
                                <td style="padding: 8px 0; color: #0078D4; font-size: 18px;">
                                    <strong>${total_cost:.2f} USD</strong>
                                </td>
                            </tr>
                            <tr>
                                <td style="padding: 8px 0;"><strong>Report File:</strong></td>
                                <td style="padding: 8px 0;">{filename}</td>
                            </tr>
                            <tr>
                                <td style="padding: 8px 0;"><strong>Generated:</strong></td>
                                <td style="padding: 8px 0;">{datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</td>
                            </tr>
                        </table>
                    </div>
                    
                    <div style="background-color: #e3f2fd; padding: 15px; border-radius: 5px; margin: 20px 0; border-left: 4px solid #0078D4;">
                        <p style="margin: 0;"><strong>📊 Download Report:</strong></p>
                        <p style="margin: 10px 0 0 0;">
                            <a href="{blob_url}" 
                               style="display: inline-block; padding: 10px 20px; background-color: #0078D4; color: white; text-decoration: none; border-radius: 5px; font-weight: bold;">
                                Download CSV Report
                            </a>
                        </p>
                    </div>
                    
                    <p>The detailed Azure cost report has been generated and uploaded to Blob Storage.</p>
                    
                    <p><strong>Report Contents:</strong></p>
                    <ul>
                        <li>Individual subscription costs breakdown</li>
                        <li>Subscription status (Active/No charges/No usage)</li>
                        <li>Date range: {start_date} to {end_date}</li>
                        <li>Total cost summary: <strong>${total_cost:.2f} USD</strong></li>
                    </ul>
                    
                    <div style="margin-top: 30px; padding-top: 20px; border-top: 1px solid #ddd; font-size: 12px; color: #666;">
                        <p>This is an automated report generated by Azure Cost Management Function.</p>
                        <p>The CSV file is available in Azure Blob Storage for download.</p>
                        <p>If you have any questions, please contact your Azure administrator.</p>
                    </div>
                </div>
            </body>
        </html>
        """
        
        # Plain text version (fallback)
        plain_text_content = f"""
Azure Cost Report - {start_date} to {end_date}

Report Summary:
- Period: {start_date} to {end_date}
- Total Subscriptions: {subscription_count}
- Total Cost: ${total_cost:.2f} USD
- Report File: {filename}
- Generated: {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}

Download Report: {blob_url}

This is an automated report generated by Azure Cost Management Function.
        """
        
        # Prepare email message
        message = {
            "senderAddress": SENDER_EMAIL,
            "recipients": {
                "to": [{"address": RECIPIENT_EMAIL}]
            },
            "content": {
                "subject": subject,
                "plainText": plain_text_content,
                "html": html_content
            }
        }
        
        # Send email
        logger.info("Sending email via ACS...")
        poller = email_client.begin_send(message)
        result = poller.result()
        
        logger.info(f"✓ Email sent successfully!")
        logger.info(f"  Message ID: {result['id']}")
        logger.info(f"  Status: {result['status']}")
        logger.info(f"  Recipient: {RECIPIENT_EMAIL}")
        
        return True
        
    except ValueError as ve:
        logger.error(f"Configuration error: {str(ve)}")
        raise
    except Exception as e:
        logger.error(f"Error sending email via ACS: {str(e)}")
        logger.error(f"Error type: {type(e).__name__}")
        logger.error(f"Traceback: {traceback.format_exc()}")
        raise

def main(req: func.HttpRequest) -> func.HttpResponse:
    """Main function entry point"""
    logger.info('=' * 80)
    logger.info('Azure Cost Report to Blob Storage with Email - Starting execution')
    logger.info('=' * 80)
    
    try:
        # Step 1: Validate environment variables
        logger.info("Step 1: Validating environment variables...")
        required_vars = [
            "TENANT_ID", 
            "CLIENT_ID", 
            "CLIENT_SECRET", 
            "AZURE_STORAGE_CONNECTION_STRING",
            "ACS_CONNECTION_STRING",
            "ACS_SENDER_EMAIL"
        ]
        missing_vars = [var for var in required_vars if not os.environ.get(var)]
        
        if missing_vars:
            error_msg = f"Missing environment variables: {', '.join(missing_vars)}"
            logger.error(error_msg)
            logger.error("Please configure these in Azure Function App Settings")
            return func.HttpResponse(
                body=json.dumps({
                    "error": error_msg,
                    "details": "Configure environment variables in Azure Portal → Function App → Configuration → Application Settings"
                }),
                status_code=500,
                mimetype="application/json"
            )
        
        logger.info("✓ All environment variables present")
        
        # Step 2: Get access token
        logger.info("Step 2: Acquiring Azure access token...")
        token = get_access_token()
        logger.info("✓ Access token acquired")
        
        # Step 3: Calculate date range
        logger.info("Step 3: Calculating date range...")
        start_date, end_date = get_previous_month_range()
        logger.info(f"✓ Date range: {start_date} to {end_date}")
        
        # Step 4: Fetch subscriptions
        logger.info("Step 4: Fetching all subscriptions...")
        subscriptions = get_all_subscriptions(token)
        
        if not subscriptions:
            logger.warning("No subscriptions found")
            return func.HttpResponse(
                body=json.dumps({
                    "error": "No subscriptions found",
                    "details": "The service principal has no access to any subscriptions"
                }),
                status_code=404,
                mimetype="application/json"
            )
        
        logger.info(f"✓ Found {len(subscriptions)} subscriptions")
        
        # Step 5: Fetch cost data for each subscription
        logger.info("Step 5: Fetching cost data for all subscriptions...")
        all_costs_data = []
        
        for idx, subscription in enumerate(subscriptions, 1):
            sub_id = subscription.get("subscriptionId")
            sub_name = subscription.get("displayName", "Unknown")
            
            logger.info(f"  [{idx}/{len(subscriptions)}] Processing: {sub_name}")
            
            cost_data = fetch_cost_for_subscription(token, sub_id, start_date, end_date)
            
            all_costs_data.append({
                "subscription_id": sub_id,
                "subscription_name": sub_name,
                "cost_data": cost_data
            })
        
        logger.info("✓ Cost data fetched for all subscriptions")
        
        # Step 6: Generate CSV file
        logger.info("Step 6: Generating CSV report...")
        csv_content, total_cost = generate_csv(all_costs_data, start_date, end_date)
        logger.info("✓ CSV report generated")
        
        # Step 7: Upload to Blob Storage
        filename = f"azure_cost_report_{start_date}_to_{end_date}.csv"
        logger.info(f"Step 7: Uploading CSV to Blob Storage: {filename}")
        
        blob_url = upload_to_blob_storage(csv_content, filename)
        logger.info("✓ CSV uploaded to Blob Storage successfully")
        
        # Step 8: Send email notification via Azure Communication Service
        logger.info("Step 8: Sending email notification...")
        send_email_with_acs(blob_url, filename, start_date, end_date, total_cost, len(all_costs_data))
        logger.info("✓ Email notification sent successfully")
        
        logger.info('=' * 80)
        logger.info('Execution completed successfully!')
        logger.info('=' * 80)
        
        return func.HttpResponse(
            body=json.dumps({
                "status": "success",
                "message": "Cost report CSV uploaded to Blob Storage and email notification sent",
                "blob_url": blob_url,
                "report_period": f"{start_date} to {end_date}",
                "total_subscriptions": len(all_costs_data),
                "total_cost": round(total_cost, 2),
                "filename": filename,
                "email_sent": True
            }),
            status_code=200,
            mimetype="application/json"
        )
        
    except ValueError as ve:
        error_msg = f"Configuration error: {str(ve)}"
        logger.error('=' * 80)
        logger.error('EXECUTION FAILED - Configuration Error')
        logger.error('=' * 80)
        logger.error(error_msg)
        logger.error(f"Traceback: {traceback.format_exc()}")
        
        return func.HttpResponse(
            body=json.dumps({
                "error": error_msg,
                "type": "ConfigurationError",
                "traceback": traceback.format_exc()
            }),
            status_code=500,
            mimetype="application/json"
        )
        
    except requests.exceptions.RequestException as re:
        error_msg = f"Azure API error: {str(re)}"
        logger.error('=' * 80)
        logger.error('EXECUTION FAILED - API Error')
        logger.error('=' * 80)
        logger.error(error_msg)
        logger.error(f"Traceback: {traceback.format_exc()}")
        
        return func.HttpResponse(
            body=json.dumps({
                "error": error_msg,
                "type": "APIError",
                "traceback": traceback.format_exc()
            }),
            status_code=500,
            mimetype="application/json"
        )
        
    except Exception as e:
        error_msg = f"Unexpected error: {str(e)}"
        logger.error('=' * 80)
        logger.error('EXECUTION FAILED - Unexpected Error')
        logger.error('=' * 80)
        logger.error(error_msg)
        logger.error(f"Error type: {type(e).__name__}")
        logger.error(f"Traceback: {traceback.format_exc()}")
        
        return func.HttpResponse(
            body=json.dumps({
                "error": str(e),
                "type": type(e).__name__,
                "traceback": traceback.format_exc()
            }),
            status_code=500,
            mimetype="application/json"
        )