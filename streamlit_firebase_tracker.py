import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import re
from typing import List, Dict, Any, Union, Optional
from datetime import datetime, timedelta, timezone
from email.mime.base import MIMEBase
from email import encoders
import io

# streamlit_firebase_tracker.py
import streamlit as st
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import plotly.express as px
import plotly.graph_objects as go
import hashlib
import json
import os
import firebase_admin
from firebase_admin import credentials, firestore
import uuid

# Define farm names and columns
FARM_COLUMNS = ['A: Kebun Sendiri', 'B: Kebun DeYe', 'C: Kebun Asan', 'D: Kebun Uncle']
OLD_FARM_COLUMNS = ['Farm A', 'Farm B', 'Farm C', 'Farm D']

# Revenue estimation constants
BUYERS = ['Green', 'Kedah', 'YY', 'Lukut', 'PD']
FRUIT_SIZES = ['>600g', '>500g', '>400g', '>300g', 'Reject']
DEFAULT_DISTRIBUTION = {'>600g': 10, '>500g': 20, '>400g': 30, '>300g': 30, 'Reject': 10}
BAKUL_TO_KG = 15  # 1 bakul = 15kg

# Set page config
st.set_page_config(
    page_title="Bunga di Kebun",
    page_icon="🌷",
    layout="wide"
)

# Initialize session state variables
if 'logged_in' not in st.session_state:
    st.session_state.logged_in = False
if 'username' not in st.session_state:
    st.session_state.username = ""
if 'role' not in st.session_state:
    st.session_state.role = ""
if 'storage_mode' not in st.session_state:
    st.session_state.storage_mode = "Checking..."
if 'needs_rerun' not in st.session_state:
    st.session_state.needs_rerun = False
if 'current_user_data' not in st.session_state:
    st.session_state.current_user_data = pd.DataFrame(columns=['Date'] + FARM_COLUMNS)
if 'csv_backup_enabled' not in st.session_state:
    st.session_state.csv_backup_enabled = True  # Default: OFF for fast saves
if 'revenue_transactions' not in st.session_state:
    st.session_state.revenue_transactions = []

# Helper functions for safe formatting
def format_currency(amount):
    """Safely format currency without f-strings"""
    return "RM {:,.2f}".format(amount)

def format_percentage(value):
    """Safely format percentage without f-strings"""
    return "{:.1f}%".format(value)

# Add this helper function near the top of your code with other helper functions

# Updated generate_estimate_id function with Malaysia timezone
def generate_estimate_id(estimate_date, total_bakul, username=None):
    """
    Generate a meaningful ID for revenue estimates with bakul count
    Format: YYYYMMDD-HHMMSS-###B (e.g., 20250528-143052-100B)
    Uses Malaysia time (UTC+8)
    """
    from datetime import datetime, timezone, timedelta
    
    # Create Malaysia timezone (UTC+8)
    malaysia_tz = timezone(timedelta(hours=8))
    
    # Current timestamp in Malaysia time
    now = datetime.now(malaysia_tz)
    
    # Format the estimate date
    if isinstance(estimate_date, str):
        # If date is string, parse it
        date_obj = datetime.strptime(estimate_date, '%Y-%m-%d')
    else:
        # If date is date object, convert to datetime
        date_obj = datetime.combine(estimate_date, datetime.min.time())
    
    # Create ID components using Malaysia time
    date_part = date_obj.strftime('%Y%m%d')  # 20250528
    time_part = now.strftime('%H%M%S')       # 203045 (8:30:45 PM Malaysia time)
    bakul_part = f"{total_bakul}B"           # 300B
    
    # Option 4: YYYYMMDD-HHMMSS-###B
    estimate_id = f"{date_part}-{time_part}-{bakul_part}"
    
    # Optional: Add username if provided
    if username:
        estimate_id = f"{date_part}-{time_part}-{bakul_part}-{username}"
    
    return estimate_id

# Then in your revenue_estimate_tab() function, replace this line:
# OLD: 'id': str(uuid.uuid4()),
# NEW: 'id': generate_estimate_id(estimate_date, total_bakul, st.session_state.username),


def parse_date_string(date_str: str, current_year: int = None) -> Optional[datetime]:
    """
    Parse various date string formats into a datetime object.
    """
    if not current_year:
        current_year = datetime.now().year
    
    # Handle natural language dates
    if date_str.lower() == "today":
        return datetime.now()
    elif date_str.lower() == "yesterday":
        return datetime.now() - timedelta(days=1)
    elif date_str.lower() == "last week":
        today = datetime.now().date()
        start_of_last_week = (today - timedelta(days=today.weekday() + 7))
        return datetime.combine(start_of_last_week, datetime.min.time())
    elif date_str.lower() == "this month":
        today = datetime.now().date()
        return datetime.combine(today.replace(day=1), datetime.min.time())
    
    # Define month name mapping
    month_map = {
        "january": 1, "jan": 1,
        "february": 2, "feb": 2,
        "march": 3, "mar": 3,
        "april": 4, "apr": 4,
        "may": 5,
        "june": 6, "jun": 6,
        "july": 7, "jul": 7,
        "august": 8, "aug": 8,
        "september": 9, "sep": 9,
        "october": 10, "oct": 10,
        "november": 11, "nov": 11,
        "december": 12, "dec": 12
    }
    
    # Try month name formats: "May 1" or "1 May"
    for month_name, month_num in month_map.items():
        # Pattern for "Month Day" format (e.g., "May 1")
        pattern_1 = rf'\b{month_name}\s+(\d{{1,2}})(?:st|nd|rd|th)?\b'
        match = re.search(pattern_1, date_str.lower())
        if match:
            day = int(match.group(1))
            return datetime(current_year, month_num, day)
        
        # Pattern for "Day Month" format (e.g., "1 May")
        pattern_2 = rf'\b(\d{{1,2}})(?:st|nd|rd|th)?\s+{month_name}\b'
        match = re.search(pattern_2, date_str.lower())
        if match:
            day = int(match.group(1))
            return datetime(current_year, month_num, day)
    
    # Try standard date formats with strptime
    formats = [
        '%Y-%m-%d', '%d/%m/%Y', '%m/%d/%Y', 
        '%d-%m-%Y', '%m-%d-%Y',
        '%d/%m/%y', '%m/%d/%y',
        '%d-%m-%y', '%m-%d-%y'
    ]
    
    for fmt in formats:
        try:
            return datetime.strptime(date_str, fmt)
        except ValueError:
            continue
    
    # If all parsing attempts fail
    return None

# Firebase connection - Fixed version
def connect_to_firebase():
    try:
        # Check if Firebase is already initialized
        if not firebase_admin._apps:
            # Use Streamlit secrets directly - fixed approach
            if 'firebase_credentials' in st.secrets:
                # Convert the secrets to a proper dictionary
                firebase_secrets = dict(st.secrets["firebase_credentials"])
                
                # Ensure the private_key has proper line breaks
                if 'private_key' in firebase_secrets:
                    # Replace escaped newlines with actual newlines
                    firebase_secrets['private_key'] = firebase_secrets['private_key'].replace('\\n', '\n')
                
                cred = credentials.Certificate(firebase_secrets)
                firebase_admin.initialize_app(cred)
                
                # Test the connection
                db = firestore.client()
                # Try a simple operation to verify connection
                test_collection = db.collection('test')
                test_collection.limit(1).get()
                
                return db
            else:
                st.error("Firebase credentials not found in secrets")
                initialize_session_storage()
                return None
        else:
            # Return existing Firestore client if Firebase is already initialized
            return firestore.client()
    except Exception as e:
        st.error("Firebase connection error: " + str(e))
        st.error("Falling back to session storage...")
        initialize_session_storage()
        return None

# Initialize session state as fallback
def initialize_session_storage():
    if 'users' not in st.session_state:
        # Create default admin user
        st.session_state.users = {
            "admin": {
                "password": hashlib.sha256("admin".encode()).hexdigest(),
                "role": "admin"
            }
        }
    
    if 'farm_data' not in st.session_state:
        st.session_state.farm_data = {}

# Firebase collection access with fallback - Fixed version
def get_users_collection():
    db = connect_to_firebase()
    if db:
        try:
            # Try to access the users collection
            users = db.collection('users')
            # Test by getting a document (but don't fail if empty)
            try:
                users.limit(1).get()
            except:
                pass  # Collection might be empty, that's okay
            return users
        except Exception as e:
            st.error("Error accessing users collection: " + str(e))
            return None
    return None

def get_farm_data_collection():
    db = connect_to_firebase()
    if db:
        try:
            # Try to access the farm_data collection
            farm_data = db.collection('farm_data')
            # Test by getting a document (but don't fail if empty)
            try:
                farm_data.limit(1).get()
            except:
                pass  # Collection might be empty, that's okay
            return farm_data
        except Exception as e:
            st.error("Error accessing farm_data collection: " + str(e))
            return None
    return None

def get_revenue_data_collection():
    db = connect_to_firebase()
    if db:
        try:
            revenue_data = db.collection('revenue_data')
            try:
                revenue_data.limit(1).get()
            except:
                pass
            return revenue_data
        except Exception as e:
            st.error("Error accessing revenue_data collection: " + str(e))
            return None
    return None

# Password hashing
def hash_password(password):
    return hashlib.sha256(password.encode()).hexdigest()

# User management with fallbacks - Fixed version
def add_user(username, password, role="user"):
    users = get_users_collection()
    if users:
        # Firebase storage
        try:
            # Check if username exists
            user_docs = users.where("username", "==", username).limit(1).get()
            if len(list(user_docs)) > 0:
                return False
                
            # Add new user
            user_data = {
                "username": username,
                "password": hash_password(password),
                "role": role,
                "created_at": firestore.SERVER_TIMESTAMP
            }
            
            # Use username as document ID for easy lookup
            users.document(username).set(user_data)
            return True
        except Exception as e:
            st.error("Error adding user to Firebase: " + str(e))
            # Fallback to session state
            pass
    
    # Session state storage
    if 'users' not in st.session_state:
        initialize_session_storage()
        
    if username in st.session_state.users:
        return False
    
    st.session_state.users[username] = {
        "password": hash_password(password),
        "role": role
    }
    return True

def verify_user(username, password):
    users = get_users_collection()
    if users:
        # Firebase storage
        try:
            # Get user document directly by username as ID
            user_doc = users.document(username).get()
            if user_doc.exists:
                user_data = user_doc.to_dict()
                if user_data and user_data["password"] == hash_password(password):
                    return user_data["role"]
            return None
        except Exception as e:
            st.error("Error verifying user from Firebase: " + str(e))
            # Fallback to session state
            pass
    
    # Session state storage
    if 'users' not in st.session_state:
        initialize_session_storage()
        
    if username in st.session_state.users and st.session_state.users[username]["password"] == hash_password(password):
        return st.session_state.users[username]["role"]
    return None

# Data functions with fallbacks - Fixed version
def load_data(username):
    farm_data = get_farm_data_collection()
    if farm_data:
        # Firebase storage
        try:
            # Get all records for this user
            user_data_docs = farm_data.where("username", "==", username).get()
            
            if not user_data_docs:
                return pd.DataFrame(columns=['Date'] + FARM_COLUMNS)
            
            # Convert to DataFrame
            records = []
            for doc in user_data_docs:
                doc_data = doc.to_dict()
                if doc_data:  # Make sure data exists
                    records.append(doc_data)
            
            if not records:
                return pd.DataFrame(columns=['Date'] + FARM_COLUMNS)
            
            df = pd.DataFrame(records)
            
            # Drop Firebase document ID if present
            if 'document_id' in df.columns:
                df = df.drop('document_id', axis=1)
            
            # Drop username field for display
            if 'username' in df.columns:
                df = df.drop('username', axis=1)
            
            # Ensure Date is datetime
            if 'Date' in df.columns:
                df['Date'] = pd.to_datetime(df['Date'])
            
            # Convert old farm column names to new ones if needed
            for old_col, new_col in zip(OLD_FARM_COLUMNS, FARM_COLUMNS):
                if old_col in df.columns and new_col not in df.columns:
                    df[new_col] = df[old_col]
                    df = df.drop(old_col, axis=1)
            
            # Ensure all farm columns exist
            for col in FARM_COLUMNS:
                if col not in df.columns:
                    df[col] = 0
            
            return df
        except Exception as e:
            st.error("Error loading data from Firebase: " + str(e))
            # Fallback to session state
            pass
    
    # Session state storage
    if 'farm_data' not in st.session_state:
        initialize_session_storage()
        
    if username in st.session_state.farm_data:
        df = pd.DataFrame(st.session_state.farm_data[username])
        
        # Convert old farm column names to new ones if needed
        for old_col, new_col in zip(OLD_FARM_COLUMNS, FARM_COLUMNS):
            if old_col in df.columns and new_col not in df.columns:
                df[new_col] = df[old_col]
                df = df.drop(old_col, axis=1)
        
        # Ensure all farm columns exist
        for col in FARM_COLUMNS:
            if col not in df.columns:
                df[col] = 0
        
        if not df.empty and 'Date' in df.columns:
            df['Date'] = pd.to_datetime(df['Date'])
        return df
    return pd.DataFrame(columns=['Date'] + FARM_COLUMNS)

# REPLACE your current save_data() function with this fixed version:
def save_data(df, username):
    farm_data = get_farm_data_collection()
    if farm_data:
        # Firebase storage - COMPLETELY FIXED VERSION WITH DELETION
        try:
            # STEP 1: Get ALL existing records for this user
            existing_docs = farm_data.where("username", "==", username).get()
            existing_dates = {}
            
            # Build a map of existing dates and their document IDs
            for doc in existing_docs:
                doc_data = doc.to_dict()
                if 'Date' in doc_data:
                    doc_date = pd.to_datetime(doc_data['Date']).date()
                    existing_dates[doc_date] = doc.id
            
            # STEP 2: Process current DataFrame records
            current_dates = set()
            records = df.to_dict('records')
            
            for record in records:
                # Add username to each record
                record['username'] = username
                
                # Convert pandas Timestamp to string for Firebase
                if 'Date' in record:
                    if isinstance(record['Date'], pd.Timestamp):
                        record_date = record['Date'].date()
                        record['Date'] = record['Date'].isoformat()
                    else:
                        record_date = pd.to_datetime(record['Date']).date()
                    
                    current_dates.add(record_date)
                
                # Ensure all values are JSON serializable
                for key, value in record.items():
                    if pd.isna(value):
                        record[key] = 0
                    elif isinstance(value, (np.integer, np.floating)):
                        record[key] = int(value) if isinstance(value, np.integer) else float(value)
                
                # Check if this date already exists
                if record_date in existing_dates:
                    # UPDATE existing record
                    doc_id = existing_dates[record_date]
                    farm_data.document(doc_id).set(record)
                    print("Updated existing record for " + str(record_date))
                else:
                    # ADD new record
                    farm_data.add(record)
                    print("Added new record for " + str(record_date))
            
            # STEP 3: DELETE records that exist in Firebase but NOT in current DataFrame
            dates_to_delete = set(existing_dates.keys()) - current_dates
            
            if dates_to_delete:
                print("🔥 DELETING " + str(len(dates_to_delete)) + " records from Firebase...")
                for date_to_delete in dates_to_delete:
                    if date_to_delete in existing_dates:
                        doc_id = existing_dates[date_to_delete]
                        farm_data.document(doc_id).delete()
                        print("🗑️ DELETED record for " + str(date_to_delete))
                        st.success("Deleted " + str(date_to_delete) + " from Firebase!")
            
            return True
        except Exception as e:
            st.error("Error saving data to Firebase: " + str(e))
            # Fallback to session state
            pass
    
    # Session state storage (unchanged)
    if 'farm_data' not in st.session_state:
        initialize_session_storage()
        
    st.session_state.farm_data[username] = df.to_dict('records')
    return True

# Revenue data functions
def load_revenue_data(username):
    revenue_data = get_revenue_data_collection()
    if revenue_data:
        try:
            user_revenue_docs = revenue_data.where("username", "==", username).get()
            transactions = []
            for doc in user_revenue_docs:
                doc_data = doc.to_dict()
                if doc_data:
                    transactions.append(doc_data)
            return transactions
        except Exception as e:
            st.error("Error loading revenue data from Firebase: " + str(e))
    
    # Fallback to session state
    return [t for t in st.session_state.revenue_transactions if t.get('username') == username]

def save_revenue_data(transactions, username):
    revenue_data = get_revenue_data_collection()
    if revenue_data:
        try:
            # Delete existing data for user
            existing_docs = revenue_data.where("username", "==", username).get()
            for doc in existing_docs:
                doc.reference.delete()
            
            # Add new data
            for transaction in transactions:
                transaction['username'] = username
                revenue_data.add(transaction)
            return True
        except Exception as e:
            st.error("Error saving revenue data to Firebase: " + str(e))
    
    # Fallback to session state
    st.session_state.revenue_transactions = [
        t for t in st.session_state.revenue_transactions if t.get('username') != username
    ]
    
    for transaction in transactions:
        transaction['username'] = username
        st.session_state.revenue_transactions.append(transaction)
    
    return True

# Revenue calculation functions
def calculate_bakul_distribution(total_bakul, distribution_percentages):
    bakul_per_size = {}
    remaining_bakul = total_bakul
    
    for i, size in enumerate(FRUIT_SIZES[:-1]):
        percentage = distribution_percentages[size]
        bakul_count = int(total_bakul * percentage / 100)
        bakul_per_size[size] = bakul_count
        remaining_bakul -= bakul_count
    
    bakul_per_size[FRUIT_SIZES[-1]] = max(0, remaining_bakul)
    return bakul_per_size

def validate_estimate_data(estimate):
    required_keys = [
        'selected_buyers', 'buyer_distribution', 'buyer_bakul_allocation',
        'buyer_prices', 'bakul_per_size', 'total_revenue'
    ]
    
    missing_keys = []
    for key in required_keys:
        if key not in estimate:
            missing_keys.append(key)
        elif estimate[key] is None:
            missing_keys.append(key + " (null)")
    
    return missing_keys

def create_formatted_csv_backup(username):
    """
    Create a properly formatted CSV backup with clean column names and sorting
    """
    try:
        # Use current displayed data (after deletions/edits) instead of loading from database
        current_data = st.session_state.current_user_data.copy()
        
        if current_data.empty:
            return None, "No data available for backup"
        
        # Create a copy for formatting
        formatted_data = current_data.copy()
        
        # Ensure Date is datetime
        formatted_data['Date'] = pd.to_datetime(formatted_data['Date'])
        
        # Sort by date - NEWEST FIRST (descending order)
        formatted_data = formatted_data.sort_values('Date', ascending=False).reset_index(drop=True)
        
        # Keep original column names (don't rename them)
        # Column names stay as: A: Kebun Sendiri, B: Kebun DeYe, etc.
        
        # Calculate Total Bunga and Total Bakul
        formatted_data['Total Bunga'] = (
            formatted_data['A: Kebun Sendiri'] + 
            formatted_data['B: Kebun DeYe'] + 
            formatted_data['C: Kebun Asan'] + 
            formatted_data['D: Kebun Uncle']
        )
        formatted_data['Total Bakul'] = (formatted_data['Total Bunga'] / 40).round().astype(int)
        
        # Select columns in the exact order you want (keep original names)
        final_columns = ['Date', 'A: Kebun Sendiri', 'B: Kebun DeYe', 'C: Kebun Asan', 'D: Kebun Uncle', 'Total Bunga', 'Total Bakul']
        formatted_data = formatted_data[final_columns]
        
        # Format date as YYYY-MM-DD (clean format)
        formatted_data['Date'] = formatted_data['Date'].dt.strftime('%Y-%m-%d')
        
        # Format all numeric columns with thousand separators (commas)
        numeric_columns = ['A: Kebun Sendiri', 'B: Kebun DeYe', 'C: Kebun Asan', 'D: Kebun Uncle', 'Total Bunga', 'Total Bakul']
        for col in numeric_columns:
            formatted_data[col] = formatted_data[col].apply(lambda x: "{:,}".format(int(x)))
        
        # Create CSV in memory with clean formatting
        csv_buffer = io.StringIO()
        formatted_data.to_csv(csv_buffer, index=False)
        csv_content = csv_buffer.getvalue()
        
        return csv_content, "✅ CSV backup created: " + str(len(formatted_data)) + " records"
        
    except Exception as e:
        return None, "❌ Backup failed: " + str(e)

def send_email_notification_with_csv_backup(date, farm_data, username):
    """
    Send email notification with properly formatted CSV backup attachment
    """
    try:
        # Email settings (same as before)
        sender_email = "hqtong2013@gmail.com"
        receiver_email = "powchooyeo88@gmail.com"
        
        # Get password from Streamlit secrets
        password_source = "not found"
        try:
            password = st.secrets["email_password"]
            password_source = "top-level secret"
        except (KeyError, TypeError):
            try:
                password = st.secrets["general"]["email_password"]
                password_source = "general section secret"
            except (KeyError, TypeError):
                return False, "Email password not found in Streamlit secrets."
        
        # Calculate totals for this entry
        total_bunga = sum(farm_data.values())
        total_bakul = int(total_bunga / 40)
        
        # Format date with day name
        if isinstance(date, str):
            try:
                date_obj = datetime.strptime(date, '%Y-%m-%d')
            except:
                date_obj = datetime.strptime(str(date), '%Y-%m-%d')
        else:
            date_obj = date
            
        day_name = date_obj.strftime('%A')
        date_formatted = date_obj.strftime('%Y-%m-%d')
        
        # Create message
        message = MIMEMultipart('mixed')
        message["From"] = sender_email
        message["To"] = receiver_email
        message["Subject"] = "Total Bunga " + date_formatted + ": " + "{:,}".format(total_bunga) + " bunga, " + str(total_bakul) + " bakul + CSV Backup"
        
        # FIXED: Format farm details with proper HTML line breaks
        farm_info = "A: Kebun Sendiri: " + "{:,}".format(farm_data['A: Kebun Sendiri']) + " Bunga<br>"
        farm_info += "B: Kebun DeYe&nbsp;&nbsp;&nbsp;: " + "{:,}".format(farm_data['B: Kebun DeYe']) + " Bunga<br>"
        farm_info += "C: Kebun Asan&nbsp;&nbsp;&nbsp;: " + "{:,}".format(farm_data['C: Kebun Asan']) + " Bunga<br>"
        farm_info += "D: Kebun Uncle&nbsp;&nbsp;: " + "{:,}".format(farm_data['D: Kebun Uncle']) + " Bunga"
        
        # Get Malaysia time
        malaysia_tz = timezone(timedelta(hours=8))
        malaysia_time = datetime.now(malaysia_tz).strftime('%Y-%m-%d %H:%M:%S')
        
        # HTML email format (to support bold and red color)
        html_body = """<!DOCTYPE html>
<html>
<head>
    <meta charset="UTF-8">
    <style>
        body { font-family: Arial, sans-serif; line-height: 1.6; }
        .highlight { color: #FF0000; font-weight: bold; }
        .farm-details { 
            font-family: Courier New, monospace; 
            background-color: #f5f5f5;
            padding: 10px;
            border-radius: 5px;
            margin: 10px 0;
        }
    </style>
</head>
<body>
<p>New flower data has been added to Bunga di Kebun system.</p>

<p class="highlight">Date: """ + date_formatted + " (" + day_name + """)</p>

<p class="highlight">Total bunga: """ + "{:,}".format(total_bunga) + """</p>

<p class="highlight">Total bakul: """ + str(total_bakul) + """</p>

<p><strong>Farm Details:</strong></p>

<div class="farm-details">
""" + farm_info + """
</div>

<p><strong>System Information:</strong></p>

<p>Password retrieved from: """ + password_source + """</p>

<p>Timestamp: """ + malaysia_time + """ (Malaysia Time)</p>

<p>This is an automated notification from Bunga di Kebun System.</p>
</body>
</html>"""
        
        # Send ONLY HTML version (no plain text to avoid duplication)
        message.attach(MIMEText(html_body, "html"))
        
        # CREATE FORMATTED CSV BACKUP
        csv_content, backup_status = create_formatted_csv_backup(username)
        
        if csv_content:
            # Create attachment
            csv_attachment = MIMEBase('application', 'octet-stream')
            csv_attachment.set_payload(csv_content.encode('utf-8'))
            encoders.encode_base64(csv_attachment)
            
            # Add header for attachment
            filename = "bunga_backup_" + username + "_" + date_formatted + ".csv"
            csv_attachment.add_header(
                'Content-Disposition',
                'attachment; filename="' + filename + '"'
            )
            
            # Attach the CSV to the email
            message.attach(csv_attachment)
        
        # Send email
        with smtplib.SMTP_SSL("smtp.gmail.com", 465) as server:
            server.login(sender_email, password)
            server.send_message(message)
        
        st.success("Email sent with formatted CSV backup! " + backup_status)
        return True, ""
        
    except Exception as e:
        error_message = str(e)
        st.error("Email error: " + error_message)
        return False, error_message

def send_email_notification_simple(date, farm_data, username):
    """
    Send email notification WITHOUT CSV backup attachment (much faster)
    """
    try:
        # Email settings (same as CSV version)
        sender_email = "hqtong2013@gmail.com"
        receiver_email = "hq_tong@hotmail.com"
        
        # Get password from Streamlit secrets
        password_source = "not found"
        try:
            password = st.secrets["email_password"]
            password_source = "top-level secret"
        except (KeyError, TypeError):
            try:
                password = st.secrets["general"]["email_password"]
                password_source = "general section secret"
            except (KeyError, TypeError):
                return False, "Email password not found in Streamlit secrets."
        
        # Calculate totals for this entry
        total_bunga = sum(farm_data.values())
        total_bakul = int(total_bunga / 40)
        
        # Format date with day name
        if isinstance(date, str):
            try:
                date_obj = datetime.strptime(date, '%Y-%m-%d')
            except:
                date_obj = datetime.strptime(str(date), '%Y-%m-%d')
        else:
            date_obj = date
            
        day_name = date_obj.strftime('%A')
        date_formatted = date_obj.strftime('%Y-%m-%d')
        
        # Create message (NO ATTACHMENT)
        message = MIMEMultipart('mixed')
        message["From"] = sender_email
        message["To"] = receiver_email
        message["Subject"] = "Total Bunga " + date_formatted + ": " + "{:,}".format(total_bunga) + " bunga, " + str(total_bakul) + " bakul"
        
        # Format farm details with proper HTML line breaks
        farm_info = "A: Kebun Sendiri: " + "{:,}".format(farm_data['A: Kebun Sendiri']) + " Bunga<br>"
        farm_info += "B: Kebun DeYe&nbsp;&nbsp;&nbsp;: " + "{:,}".format(farm_data['B: Kebun DeYe']) + " Bunga<br>"
        farm_info += "C: Kebun Asan&nbsp;&nbsp;&nbsp;: " + "{:,}".format(farm_data['C: Kebun Asan']) + " Bunga<br>"
        farm_info += "D: Kebun Uncle&nbsp;&nbsp;: " + "{:,}".format(farm_data['D: Kebun Uncle']) + " Bunga"
        
        # Get Malaysia time
        malaysia_tz = timezone(timedelta(hours=8))
        malaysia_time = datetime.now(malaysia_tz).strftime('%Y-%m-%d %H:%M:%S')
        
        # HTML email format
        html_body = """<!DOCTYPE html>
<html>
<head>
    <meta charset="UTF-8">
    <style>
        body { font-family: Arial, sans-serif; line-height: 1.6; }
        .highlight { color: #FF0000; font-weight: bold; }
        .farm-details { 
            font-family: Courier New, monospace; 
            background-color: #f5f5f5;
            padding: 10px;
            border-radius: 5px;
            margin: 10px 0;
        }
    </style>
</head>
<body>
<p>New flower data has been added to Bunga di Kebun system.</p>

<p class="highlight">Date: """ + date_formatted + " (" + day_name + """)</p>

<p class="highlight">Total bunga: """ + "{:,}".format(total_bunga) + """</p>

<p class="highlight">Total bakul: """ + str(total_bakul) + """</p>

<p><strong>Farm Details:</strong></p>

<div class="farm-details">
""" + farm_info + """
</div>

<p><strong>System Information:</strong></p>

<p>Password retrieved from: """ + password_source + """</p>

<p>Timestamp: """ + malaysia_time + """ (Malaysia Time)</p>

<p><em>Note: CSV backup disabled for faster processing</em></p>

<p>This is an automated notification from Bunga di Kebun System.</p>
</body>
</html>"""
        
        # Send HTML email (NO CSV ATTACHMENT)
        message.attach(MIMEText(html_body, "html"))
        
        # Send email
        with smtplib.SMTP_SSL("smtp.gmail.com", 465) as server:
            server.login(sender_email, password)
            server.send_message(message)
        
        return True, ""
        
    except Exception as e:
        error_message = str(e)
        return False, error_message

# Function to add data for the current user with confirmation step - Fixed version
def add_data(date, farm_1, farm_2, farm_3, farm_4, confirmed=False):
    # If not confirmed yet, return without adding data
    if not confirmed:
        return "confirm", {
            'date': date,
            'farm_data': {
                FARM_COLUMNS[0]: farm_1,
                FARM_COLUMNS[1]: farm_2,
                FARM_COLUMNS[2]: farm_3,
                FARM_COLUMNS[3]: farm_4
            }
        }
    
    try:
        # Create a new row
        new_row = pd.DataFrame({
            'Date': [pd.Timestamp(date)],
            FARM_COLUMNS[0]: [int(farm_1)],
            FARM_COLUMNS[1]: [int(farm_2)],
            FARM_COLUMNS[2]: [int(farm_3)],
            FARM_COLUMNS[3]: [int(farm_4)]
        })
        
        # Check if date already exists
        if not st.session_state.current_user_data.empty:
            existing_dates = pd.to_datetime(st.session_state.current_user_data['Date']).dt.date
            new_date = pd.Timestamp(date).date()
            if new_date in existing_dates.values:
                st.error("Data for " + str(date) + " already exists. Please edit the existing entry or choose a different date.")
                return "error", None
        
        # Append to the existing data
        st.session_state.current_user_data = pd.concat([st.session_state.current_user_data, new_row], ignore_index=True)
        
        # Sort by date
        st.session_state.current_user_data = st.session_state.current_user_data.sort_values(by='Date').reset_index(drop=True)
        
        # Save the data
        if save_data(st.session_state.current_user_data, st.session_state.username):
            st.session_state.needs_rerun = True
            farm_data = {
                FARM_COLUMNS[0]: farm_1,
                FARM_COLUMNS[1]: farm_2,
                FARM_COLUMNS[2]: farm_3,
                FARM_COLUMNS[3]: farm_4
            }
        
            # CHECK THE TOGGLE STATE
            if st.session_state.csv_backup_enabled:
                # Send email WITH CSV backup (slower)
                success, error_message = send_email_notification_with_csv_backup(date, farm_data, st.session_state.username)
                backup_status = "with CSV backup"
            else:
                # Send email WITHOUT CSV backup (faster)
                success, error_message = send_email_notification_simple(date, farm_data, st.session_state.username)
                backup_status = "without CSV backup"
            
            if success:
                st.success("Data added and email sent " + backup_status + "!")
            else:
                if "Email password not found" in error_message:
                    st.warning("Data added but email notification could not be sent: " + error_message)
                else:
                    st.warning("Data added but failed to send notification: " + error_message)
                    
            return "success", None
        else:
            # If save fails, revert the change
            # Don't reload data on save failure - keep current session data
            pass
            return "error", None
            
    except Exception as e:
        st.error("Error adding data: " + str(e))
        return "error", None

# Format number with thousands separator
def format_number(number):
    return "{:,}".format(int(number))

# Login and registration page
def login_page():
    st.title("🌷 Bunga di Kebun - Login")
    
    # Create tabs for login and registration
    login_tab, register_tab = st.tabs(["Login", "Register"])
    
    with login_tab:
        with st.form("login_form"):
            username = st.text_input("Username")
            password = st.text_input("Password", type="password")
            submitted = st.form_submit_button("Login")
            
            if submitted:
                if username and password:
                    role = verify_user(username, password)
                    if role:
                        # Set all session state variables at once
                        st.session_state.logged_in = True
                        st.session_state.username = username
                        st.session_state.role = role
                        st.session_state.current_user_data = load_data(username)
                        
                        # Show success message
                        st.success("Welcome back, " + username + "!")
                        
                        # IMMEDIATE RERUN - This is the key fix!
                        st.rerun()
                    else:
                        st.error("Invalid username or password")
                else:
                    st.error("Please enter both username and password")
    
    with register_tab:
        with st.form("register_form"):
            new_username = st.text_input("Choose a Username")
            new_password = st.text_input("Choose a Password", type="password")
            confirm_password = st.text_input("Confirm Password", type="password")
            submitted = st.form_submit_button("Register")
            
            if submitted:
                if not new_username or not new_password:
                    st.error("Username and password are required")
                elif new_password != confirm_password:
                    st.error("Passwords do not match")
                else:
                    if add_user(new_username, new_password):
                        st.success("Registration successful! You can now login.")
                    else:
                        st.error("Username already exists or registration failed")

    st.markdown("---")
    st.info("New user? Please register an account to get started.")

# Enhanced revenue estimation tab with flexible distribution methods
# Enhanced revenue estimation tab with flexible distribution methods
# Enhanced revenue estimation tab with flexible distribution methods - FIXED VERSION
# Enhanced revenue estimation tab with flexible distribution methods - COMPLETE FIXED VERSION
def revenue_estimate_tab():
    """Revenue estimation interface with flexible bakul and buyer distribution"""
    st.header("💰 Revenue Estimate")
    
    user_transactions = load_revenue_data(st.session_state.username)
    
    price_entry_tab, history_tab = st.tabs(["Price Entry", "History"])
    
    with price_entry_tab:
        st.subheader("Revenue Estimation Calculator")
        
        # STEP 1: Buyer Selection
        st.subheader("Step 1: Select Buyers")
        
        buyer_selection_cols = st.columns(len(BUYERS))
        selected_buyers = []
        
        for i, buyer in enumerate(BUYERS):
            with buyer_selection_cols[i]:
                if st.checkbox("Include " + buyer, key="select_" + buyer):
                    selected_buyers.append(buyer)
        
        if selected_buyers:
            st.success("✅ Selected buyers: " + ', '.join(selected_buyers))
        else:
            st.warning("⚠️ Please select at least one buyer")
        
        st.markdown("---")
        
        # STEP 2: Basic inputs
        st.subheader("Step 2: Basic Information")
        
        col1, col2 = st.columns(2)
        
        with col1:
            estimate_date = st.date_input("Estimate Date", datetime.now().date())
        with col2:
            # Only show total bakul input if using percentage method
            if 'bakul_method' not in st.session_state:
                st.session_state.bakul_method = "percentage"
            
            if st.session_state.bakul_method == "percentage":
                total_bakul = st.number_input("Total Bakul", min_value=0, value=100, step=1)
            else:
                total_bakul = 0  # Will be calculated from individual inputs
        
        # STEP 3: Fruit Size Distribution with toggle
        st.subheader("Step 3: Fruit Size Distribution")
        
        # Toggle between percentage and direct bakul input
        distribution_method = st.radio(
            "Distribution Method:",
            ["By Percentage", "By Bakul Count"],
            key="dist_method",
            horizontal=True,
            help="Choose how to distribute fruit sizes"
        )
        
        st.session_state.bakul_method = "percentage" if distribution_method == "By Percentage" else "direct"
        
        distribution_percentages = {}
        bakul_per_size = {}
        
        if distribution_method == "By Percentage":
            # Original percentage method
            st.write("**Enter percentage for each fruit size:**")
            dist_cols = st.columns(len(FRUIT_SIZES))
            
            for i, size in enumerate(FRUIT_SIZES):
                with dist_cols[i]:
                    distribution_percentages[size] = st.number_input(
                        size + " (%)",
                        min_value=0.0,
                        max_value=100.0,
                        value=float(DEFAULT_DISTRIBUTION[size]),
                        step=0.1,
                        key="dist_pct_" + size
                    )
            
            total_percentage = sum(distribution_percentages.values())
            
            if abs(total_percentage - 100.0) > 0.1:
                st.error("❌ Fruit size distribution must total 100%. Current total: " + format_percentage(total_percentage))
                bakul_per_size = {}
            else:
                st.success("✅ Fruit size distribution: " + format_percentage(total_percentage))
                bakul_per_size = calculate_bakul_distribution(total_bakul, distribution_percentages)
        
        else:
            # Direct bakul count method
            st.write("**Enter number of bakul for each fruit size:**")
            dist_cols = st.columns(len(FRUIT_SIZES))
            
            for i, size in enumerate(FRUIT_SIZES):
                with dist_cols[i]:
                    bakul_per_size[size] = st.number_input(
                        size + " (bakul)",
                        min_value=0,
                        value=DEFAULT_DISTRIBUTION[size] if size != 'Reject' else 10,
                        step=1,
                        key="dist_bakul_" + size
                    )
            
            # Calculate total bakul and percentages
            total_bakul = sum(bakul_per_size.values())
            
            if total_bakul > 0:
                for size in FRUIT_SIZES:
                    distribution_percentages[size] = (bakul_per_size[size] / total_bakul) * 100
                st.success(f"✅ Total Bakul: {total_bakul}")
            else:
                st.error("❌ Total bakul must be greater than 0")
        
        # Display bakul distribution
        if bakul_per_size and sum(bakul_per_size.values()) > 0:
            st.write("**Bakul Distribution:**")
            bakul_display_cols = st.columns(len(FRUIT_SIZES))
            for i, size in enumerate(FRUIT_SIZES):
                with bakul_display_cols[i]:
                    percentage = distribution_percentages.get(size, 0)
                    st.info(f"{size}: {bakul_per_size[size]} bakul ({percentage:.1f}%)")
        
        # STEP 4: Buyer Distribution with toggle
        buyer_distribution = {}
        buyer_bakul_allocation = {}
        total_buyer_percentage = 0
        
        if selected_buyers and bakul_per_size and sum(bakul_per_size.values()) > 0:
            st.subheader("Step 4: Buyer Distribution")
            
            # Toggle between percentage and direct bakul allocation
            buyer_method = st.radio(
                "Buyer Distribution Method:",
                ["By Percentage", "By Bakul Allocation"],
                key="buyer_method",
                horizontal=True,
                help="Choose how to distribute bakul among buyers"
            )
            
            if buyer_method == "By Percentage":
                # Original percentage method
                st.write("**Enter percentage for each buyer:**")
                default_buyer_percentage = 100.0 / len(selected_buyers) if selected_buyers else 0
                buyer_dist_cols = st.columns(len(selected_buyers))
                
                for i, buyer in enumerate(selected_buyers):
                    with buyer_dist_cols[i]:
                        buyer_distribution[buyer] = st.number_input(
                            buyer + " (%)",
                            min_value=0.0,
                            max_value=100.0,
                            value=default_buyer_percentage,
                            step=0.1,
                            key="buyer_dist_pct_" + buyer
                        )
                
                total_buyer_percentage = sum(buyer_distribution.values())
                
                if abs(total_buyer_percentage - 100.0) > 0.1:
                    st.error("❌ Buyer distribution must total 100%. Current total: " + format_percentage(total_buyer_percentage))
                else:
                    st.success("✅ Buyer distribution: " + format_percentage(total_buyer_percentage))
                    
                    # Calculate buyer bakul allocation from percentages
                    for buyer in selected_buyers:
                        buyer_bakul_allocation[buyer] = {}
                        for size in FRUIT_SIZES:
                            buyer_bakul_count = int(bakul_per_size[size] * buyer_distribution[buyer] / 100)
                            buyer_bakul_allocation[buyer][size] = buyer_bakul_count
            
            else:
                # Direct bakul allocation method
                st.write("**Enter bakul allocation for each buyer by fruit size:**")
                
                # Initialize buyer allocation
                for buyer in selected_buyers:
                    buyer_bakul_allocation[buyer] = {}
                
                # Create input grid: Buyers as columns, Fruit sizes as rows
                st.write("**Allocation Grid:**")
                
                for size in FRUIT_SIZES:
                    st.write(f"**{size}** (Available: {bakul_per_size[size]} bakul)")
                    
                    size_cols = st.columns(len(selected_buyers) + 1)  # +1 for total column
                    
                    size_total = 0
                    for i, buyer in enumerate(selected_buyers):
                        with size_cols[i]:
                            default_allocation = bakul_per_size[size] // len(selected_buyers)
                            buyer_bakul_allocation[buyer][size] = st.number_input(
                                f"{buyer}",
                                min_value=0,
                                value=default_allocation,
                                step=1,
                                key=f"buyer_alloc_{buyer}_{size}"
                            )
                            size_total += buyer_bakul_allocation[buyer][size]
                    
                    # Show total and validation
                    with size_cols[-1]:
                        if size_total == bakul_per_size[size]:
                            st.success(f"✅ Total: {size_total}")
                        elif size_total < bakul_per_size[size]:
                            st.warning(f"⚠️ Total: {size_total} (Short: {bakul_per_size[size] - size_total})")
                        else:
                            st.error(f"❌ Total: {size_total} (Over: {size_total - bakul_per_size[size]})")
                
                # Validate total allocation
                allocation_valid = True
                for size in FRUIT_SIZES:
                    size_total = sum(buyer_bakul_allocation[buyer][size] for buyer in selected_buyers)
                    if size_total != bakul_per_size[size]:
                        allocation_valid = False
                        break
                
                if allocation_valid:
                    st.success("✅ All bakul properly allocated to buyers!")
                    
                    # Calculate buyer percentages for display
                    total_all_bakul = sum(bakul_per_size.values())
                    for buyer in selected_buyers:
                        buyer_total = sum(buyer_bakul_allocation[buyer][size] for size in FRUIT_SIZES)
                        buyer_distribution[buyer] = (buyer_total / total_all_bakul) * 100 if total_all_bakul > 0 else 0
                    
                    total_buyer_percentage = 100.0  # Should be 100% if properly allocated
                else:
                    st.error("❌ Bakul allocation doesn't match fruit size distribution. Please adjust the numbers.")
        
        # STEP 5: Pricing Section
        buyer_prices = {}
        if selected_buyers:
            st.subheader("Step 5: Pricing (RM per kg)")
            
            for buyer in selected_buyers:
                buyer_prices[buyer] = {}
                st.write("**💼 " + buyer + "**")
                
                price_cols = st.columns(len(FRUIT_SIZES))
                for i, size in enumerate(FRUIT_SIZES):
                    with price_cols[i]:
                        buyer_prices[buyer][size] = st.number_input(
                            size,
                            min_value=0.00,
                            value=2.50,
                            step=0.01,
                            format="%.2f",
                            key="price_" + buyer + "_" + size
                        )
        
        # REAL-TIME REVENUE CALCULATION AND DISPLAY
        total_revenue = 0
        revenue_breakdown = {}
        
        # Check if all conditions are met for calculation
        bakul_valid = bakul_per_size and sum(bakul_per_size.values()) > 0
        
        if buyer_method == "By Percentage":
            buyer_valid = abs(total_buyer_percentage - 100.0) < 0.1 if total_buyer_percentage > 0 else False
        else:
            # For direct allocation, check if allocation is valid
            buyer_valid = allocation_valid if 'allocation_valid' in locals() else False
        
        can_calculate = bakul_valid and buyer_valid and len(selected_buyers) > 0 and buyer_bakul_allocation
        
        if can_calculate:
            # Calculate revenue in real-time
            for buyer in selected_buyers:
                buyer_revenue = 0
                revenue_breakdown[buyer] = {}
                
                for size in FRUIT_SIZES:
                    bakul_count = buyer_bakul_allocation[buyer][size]
                    kg_total = bakul_count * BAKUL_TO_KG
                    price_per_kg = buyer_prices[buyer][size]
                    revenue = kg_total * price_per_kg
                    
                    buyer_revenue += revenue
                    revenue_breakdown[buyer][size] = {
                        'bakul': bakul_count,
                        'kg': kg_total,
                        'price': price_per_kg,
                        'revenue': revenue
                    }
                
                total_revenue += buyer_revenue
            
            # REAL-TIME DISPLAY of revenue breakdown
            st.markdown("---")
            st.subheader("💰 Live Revenue Breakdown")
            
            # Create columns for better layout
            breakdown_col, summary_col = st.columns([2, 1])
            
            with breakdown_col:
                for buyer in selected_buyers:
                    buyer_total = sum(revenue_breakdown[buyer][size]['revenue'] for size in FRUIT_SIZES)
                    
                    # Use expander for each buyer to save space
                    with st.expander("**" + buyer + " - " + format_currency(buyer_total) + "**", expanded=True):
                        for size in FRUIT_SIZES:
                            details = revenue_breakdown[buyer][size]
                            if details['bakul'] > 0:  # Only show sizes with bakul allocation
                                detail_text = "• {}: {} bakul × {}kg × {} = {}".format(
                                    size, 
                                    details['bakul'], 
                                    BAKUL_TO_KG, 
                                    format_currency(details['price']), 
                                    format_currency(details['revenue'])
                                )
                                st.write(detail_text)
            
            with summary_col:
                st.markdown("### 🎯 Revenue Summary")
                st.metric(
                    label="Total Estimated Revenue", 
                    value=format_currency(total_revenue),
                    help="Updates automatically as you change prices"
                )
                
                # Additional summary metrics
                st.metric(
                    label="Total Bakul", 
                    value=str(total_bakul)
                )
                
                st.metric(
                    label="Revenue per Bakul", 
                    value=format_currency(total_revenue / total_bakul if total_bakul > 0 else 0)
                )
        
        # SAVE ESTIMATE FORM
        st.markdown("---")
        st.subheader("Save This Estimate")
        
        with st.form("save_estimate_form"):
            st.write("**Current Configuration Summary:**")
            if can_calculate:
                st.success("✅ Ready to save - Total Revenue: " + format_currency(total_revenue))
                
                col_summary = st.columns(3)
                with col_summary[0]:
                    st.write("**Buyers:** " + ', '.join(selected_buyers))
                with col_summary[1]:
                    st.write("**Total Bakul:** " + str(total_bakul))
                with col_summary[2]:
                    st.write("**Date:** " + str(estimate_date))
                
                # Show distribution methods used
                st.info(f"🔧 Fruit Size: {distribution_method} | Buyer: {buyer_method}")
            else:
                if not bakul_valid:
                    st.error("❌ Fix fruit size distribution")
                elif not buyer_valid:
                    if buyer_method == "By Percentage":
                        st.error("❌ Fix buyer distribution (must total 100%)")
                    else:
                        st.error("❌ Fix bakul allocation (must match fruit size totals)")
                elif not selected_buyers:
                    st.error("❌ Select at least one buyer")
                else:
                    st.warning("⚠️ Complete all steps above to save estimate")
            
            submitted = st.form_submit_button("💾 Save Estimate", disabled=not can_calculate)
            
            if submitted and can_calculate:
                # FIXED: Save with Malaysia timezone
                from datetime import timezone, timedelta
                malaysia_tz = timezone(timedelta(hours=8))
                malaysia_time = datetime.now(malaysia_tz)
                
                estimate = {
                    'id': generate_estimate_id(estimate_date, total_bakul, st.session_state.username),
                    'date': estimate_date.isoformat(),
                    'total_bakul': total_bakul,
                    'distribution_method': distribution_method,
                    'buyer_method': buyer_method,
                    'distribution_percentages': distribution_percentages,
                    'bakul_per_size': bakul_per_size,
                    'selected_buyers': selected_buyers,
                    'buyer_distribution': buyer_distribution,
                    'buyer_bakul_allocation': buyer_bakul_allocation,
                    'buyer_prices': buyer_prices,
                    'revenue_breakdown': revenue_breakdown,
                    'total_revenue': total_revenue,
                    'created_at': malaysia_time.isoformat()  # FIXED: Use Malaysia timezone
                }
                
                user_transactions.append(estimate)
                
                if save_revenue_data(user_transactions, st.session_state.username):
                    st.success("✅ Revenue estimate saved successfully!")
                    st.rerun()
                else:
                    st.error("❌ Failed to save estimate")
    
    with history_tab:
        st.subheader("Revenue Estimate History")
        
        if not user_transactions:
            st.info("No revenue estimates found. Create your first estimate in the Price Entry tab.")
            return
        
        # FIXED: Sort transactions by created_at (saved time) in descending order (newest first)
        try:
            sorted_transactions = sorted(
                user_transactions, 
                key=lambda x: x.get('created_at', '1900-01-01T00:00:00'), 
                reverse=True
            )
        except:
            # Fallback to sorting by date if created_at is not available
            sorted_transactions = sorted(
                user_transactions, 
                key=lambda x: x.get('date', '1900-01-01'), 
                reverse=True
            )
        
        # Display summary table
        summary_data = []
        for transaction in sorted_transactions:
            # Safely get revenue with fallback
            try:
                revenue_amount = transaction.get('total_revenue', 0)
                if revenue_amount is None:
                    revenue_amount = 0
                revenue_formatted = "{:,.2f}".format(float(revenue_amount))
            except (ValueError, TypeError):
                revenue_formatted = "0.00"
            
            # Safely get other fields
            transaction_date = transaction.get('date', 'Unknown')
            transaction_id = transaction.get('id', 'Unknown')
            total_bakul = transaction.get('total_bakul', 0)
            
            # Show methods used (new feature)
            dist_method = transaction.get('distribution_method', 'N/A')
            buyer_method = transaction.get('buyer_method', 'N/A')
            methods = f"{dist_method[:3]}/{buyer_method[:3]}"  # Abbreviated
            
            # Safely get buyers list
            buyers_list = transaction.get('selected_buyers', [])
            if isinstance(buyers_list, list):
                buyers_str = ', '.join(buyers_list)
            else:
                buyers_str = str(buyers_list)
            
            # FIXED: Handle Malaysia timezone for display
            created_at = transaction.get('created_at', 'Unknown')
            if created_at != 'Unknown':
                try:
                    # Parse ISO timestamp and convert to Malaysia time if needed
                    if '+' in created_at or created_at.endswith('Z'):
                        # Already has timezone info
                        created_dt = datetime.fromisoformat(created_at.replace('Z', '+00:00'))
                        # Convert to Malaysia timezone
                        malaysia_tz = timezone(timedelta(hours=8))
                        created_dt = created_dt.astimezone(malaysia_tz)
                        created_str = created_dt.strftime('%Y-%m-%d %H:%M')
                    else:
                        # Assume it's already Malaysia time
                        if len(created_at) >= 19:
                            created_str = created_at[:16].replace('T', ' ')
                        elif len(created_at) >= 10:
                            created_str = created_at[:10]
                        else:
                            created_str = created_at
                except:
                    # Fallback formatting
                    if len(created_at) >= 19:
                        created_str = created_at[:16].replace('T', ' ')
                    elif len(created_at) >= 10:
                        created_str = created_at[:10]
                    else:
                        created_str = 'Unknown'
            else:
                created_str = 'Unknown'
            
            summary_data.append({
                'Estimate Date': transaction_date,
                'ID': transaction_id,
                'Total Bakul': total_bakul,
                'Methods': methods,
                'Buyers': buyers_str,
                'Total Revenue (RM)': revenue_formatted,
                'Saved At': created_str
            })
        
        summary_df = pd.DataFrame(summary_data)
        st.dataframe(summary_df, use_container_width=True, hide_index=True)
        
        # FIXED: Detailed View Section - REMOVE "Unknown time" from dropdown
        st.subheader("Detailed View")
        
        transaction_options = []
        for x in range(len(sorted_transactions)):
            transaction = sorted_transactions[x]
            
            # FIXED: Handle Malaysia timezone for dropdown display - NO "Unknown time"
            created_at = transaction.get('created_at', 'Unknown')
            if created_at != 'Unknown':
                try:
                    # Parse ISO timestamp and convert to Malaysia time if needed
                    if '+' in created_at or created_at.endswith('Z'):
                        # Already has timezone info
                        created_dt = datetime.fromisoformat(created_at.replace('Z', '+00:00'))
                        # Convert to Malaysia timezone
                        malaysia_tz = timezone(timedelta(hours=8))
                        created_dt = created_dt.astimezone(malaysia_tz)
                        created_display = created_dt.strftime('%Y-%m-%d %H:%M:%S')
                        # Create option text with creation time
                        option_text = f"{transaction['date']} - {transaction['id']} (Saved: {created_display})"
                    else:
                        # Assume it's already Malaysia time
                        if len(created_at) >= 19:
                            created_display = created_at[:19].replace('T', ' ')
                            # Create option text with creation time
                            option_text = f"{transaction['date']} - {transaction['id']} (Saved: {created_display})"
                        else:
                            # FIXED: Don't show "Unknown time" - just show without saved time
                            option_text = f"{transaction['date']} - {transaction['id']}"
                except:
                    # FIXED: Fallback - don't show "Unknown time"
                    option_text = f"{transaction['date']} - {transaction['id']}"
            else:
                # FIXED: No saved time available - don't show "Unknown time"
                option_text = f"{transaction['date']} - {transaction['id']}"
            
            transaction_options.append(option_text)
        
        selected_transaction_idx = st.selectbox(
            "Select estimate to view details (sorted by save time - newest first)",
            range(len(sorted_transactions)),
            format_func=lambda x: transaction_options[x]
        )
        
        selected_transaction = sorted_transactions[selected_transaction_idx]
        
        # Validate transaction data
        missing_keys = validate_estimate_data(selected_transaction)
        
        if missing_keys:
            st.error("❌ Selected estimate is missing data: " + ', '.join(missing_keys))
            st.info("This estimate might be from an older version.")
        else:
            # Display detailed breakdown
            col1, col2 = st.columns(2)
            
            with col1:
                st.write("**Estimate Details:**")
                st.write("- Date: " + selected_transaction['date'])
                st.write("- ID: " + selected_transaction['id'])
                st.write("- Total Bakul: " + str(selected_transaction['total_bakul']))
                st.write("- Total Revenue: " + format_currency(selected_transaction['total_revenue']))
                st.write("- Buyers: " + ', '.join(selected_transaction['selected_buyers']))
                
                # FIXED: Show creation time in Malaysia timezone - only if available
                created_at = selected_transaction.get('created_at', 'Unknown')
                if created_at != 'Unknown':
                    try:
                        # Parse ISO timestamp and convert to Malaysia time if needed
                        if '+' in created_at or created_at.endswith('Z'):
                            # Already has timezone info
                            created_dt = datetime.fromisoformat(created_at.replace('Z', '+00:00'))
                            # Convert to Malaysia timezone
                            malaysia_tz = timezone(timedelta(hours=8))
                            created_dt = created_dt.astimezone(malaysia_tz)
                            created_display = created_dt.strftime('%Y-%m-%d %H:%M:%S')
                        else:
                            # Assume it's already Malaysia time
                            if len(created_at) >= 19:
                                created_display = created_at[:19].replace('T', ' ')
                            else:
                                created_display = created_at
                        st.write("- **Saved At: " + created_display + " (MY)**")
                    except:
                        # Don't show anything if parsing fails
                        pass
                
                # Show methods used (if available)
                if 'distribution_method' in selected_transaction:
                    st.write("- Fruit Size Method: " + selected_transaction['distribution_method'])
                if 'buyer_method' in selected_transaction:
                    st.write("- Buyer Method: " + selected_transaction['buyer_method'])
                
                # FIXED: Fruit Size Distribution in correct order (600, 500, 400, 300, Reject)
                st.write("**Fruit Size Distribution:**")
                size_order = ['>600g', '>500g', '>400g', '>300g', 'Reject']
                for size in size_order:
                    if size in selected_transaction['distribution_percentages']:
                        percentage = selected_transaction['distribution_percentages'][size]
                        bakul_count = selected_transaction['bakul_per_size'][size]
                        st.write("- " + size + ": " + format_percentage(percentage) + " (" + str(bakul_count) + " bakul)")
            
            with col2:
                st.write("**Revenue Breakdown by Buyer:**")
                
                for buyer in selected_transaction['selected_buyers']:
                    buyer_total = 0
                    buyer_total_bakul = 0  # FIXED: Track total bakul for each buyer
                    st.write("**" + buyer + ":**")
                    
                    # FIXED: Display in correct order (600, 500, 400, 300, Reject)
                    size_order = ['>600g', '>500g', '>400g', '>300g', 'Reject']
                    for size in size_order:
                        bakul_count = selected_transaction['buyer_bakul_allocation'][buyer][size]
                        price = selected_transaction['buyer_prices'][buyer][size]
                        revenue = bakul_count * BAKUL_TO_KG * price
                        buyer_total += revenue
                        buyer_total_bakul += bakul_count  # FIXED: Add to total bakul count
                        
                        if bakul_count > 0:  # Only show non-zero allocations
                            detail_text = "  {}: {} bakul × {} = {}".format(
                                size, bakul_count, format_currency(price), format_currency(revenue)
                            )
                            st.write(detail_text)
                    
                    # FIXED: Show subtotal with bakul count in brackets
                    st.write("  **Subtotal: " + format_currency(buyer_total) + " (" + str(buyer_total_bakul) + " bakul)**")
                    st.write("")
        
        # Delete functionality
        st.subheader("Delete Estimate")
        if st.button("🗑️ Delete Selected Estimate", type="secondary"):
            updated_transactions = [t for t in user_transactions if t['id'] != selected_transaction['id']]
            if save_revenue_data(updated_transactions, st.session_state.username):
                st.success("Estimate deleted successfully!")
                st.rerun()
            else:
                st.error("Failed to delete estimate")
# Main app function - Keep your existing logic with revenue tab added
def main_app():
    st.title("🌷 Bunga di Kebun - Welcome, " + st.session_state.username + "!")
    
    # Display storage mode
    storage_color = "🟢" if "Firebase" in st.session_state.storage_mode else "🟡"
    st.caption(storage_color + " Storage mode: " + st.session_state.storage_mode)
    
    # Create tabs for different functions - ADD REVENUE TAB
    tab1, tab2, tab3 = st.tabs(["📝 Data Entry", "📊 Data Analysis", "💰 Revenue Estimate"])
    
    # Tab 1: Data Entry (KEEP YOUR EXISTING CODE)
    with tab1:
        st.header("Add New Data")
        # CSV BACKUP TOGGLE
        col_toggle, col_status = st.columns([1, 2])
        
        with col_toggle:
            csv_enabled = st.toggle(
                "📊 CSV Backup", 
                value=st.session_state.csv_backup_enabled,
                help="Toggle CSV backup attachment in emails"
            )
            # Update session state when toggle changes
            st.session_state.csv_backup_enabled = csv_enabled
        
        with col_status:
            if st.session_state.csv_backup_enabled:
                st.success("✅ CSV backup ENABLED (slower saves)")
            else:
                st.warning("⚠️ CSV backup DISABLED (faster saves)")
        
        # Add separator
        st.markdown("---")
        
        # Add session state for data confirmation
        if 'confirm_data' not in st.session_state:
            st.session_state.confirm_data = False
            st.session_state.data_to_confirm = None
            
        # Show confirmation dialog if needed
        if st.session_state.confirm_data and st.session_state.data_to_confirm:
            data = st.session_state.data_to_confirm
            date = data['date']
            farm_data = data['farm_data']
            
            # Calculate totals for display
            total_bunga = sum(farm_data.values())
            total_bakul = int(total_bunga / 40)
            
            # Format date with day name
            if isinstance(date, str):
                try:
                    date_obj = datetime.strptime(date, '%Y-%m-%d')
                except:
                    date_obj = datetime.strptime(str(date), '%Y-%m-%d')
            else:
                date_obj = date
            
            day_name = date_obj.strftime('%A')
            date_formatted = date_obj.strftime('%Y-%m-%d')
            
            # Add custom CSS for compact layout
            st.markdown("""
            <style>
                div.block-container {
                    padding-top: 1rem;
                    padding-bottom: 1rem;
                }
                
                .stAlert {
                    padding: 0.5rem !important;
                    margin-bottom: 0.5rem !important;
                }
                
                p {
                    margin-bottom: 0.2rem !important;
                    font-size: 0.9rem !important;
                }
                
                .stButton button {
                    padding: 0.3rem 1rem !important;
                    height: auto !important;
                    min-height: 0 !important;
                    margin: 0.2rem 0 !important;
                    font-size: 0.9rem !important;
                }
                
                div[data-testid="column"]:nth-child(1) .stButton > button {
                    background-color: #2e7d32 !important;
                    color: white !important;
                    border: 1px solid #1b5e20 !important;
                }
                
                div[data-testid="column"]:nth-child(2) .stButton > button {
                    background-color: #fff9c4 !important;
                    color: #333 !important;
                    border: 1px solid #fbc02d !important;
                }
                
                .farm-row {
                    margin: 0.1rem 0 !important;
                    padding: 0 !important;
                    font-size: 1.1rem !important;
                    font-weight: bold !important;
                    color: #ff0000 !important;
                }
                
                .red-data {
                    font-weight: bold !important;
                    color: #ff0000 !important;
                }
                
                .blue-data {
                    font-weight: bold !important;
                    color: #0000ff !important;
                }
                
                .date-info {
                    margin-bottom: 0.2rem !important;
                    font-size: 1rem !important;
                }
                
                .stats-item {
                    margin-bottom: 0.2rem !important;
                    font-size: 1rem !important;
                }
            </style>
            """, unsafe_allow_html=True)
            
            # Show warning
            st.warning("⚠️ Please Confirm Before Save")
            
            # Date line
            st.markdown("""
            <div class="date-info">
                <b>Date:</b> <span class="red-data">""" + date_formatted + " (" + day_name + """)</span>
            </div>
            """, unsafe_allow_html=True)
            
            # Farm details section
            st.markdown("<b>Farm Details:</b>", unsafe_allow_html=True)
            
            # Display each farm on its own line
            for farm, value in farm_data.items():
                # Shorten farm name to save space
                short_name = farm.split(":")[0] + ":" + farm.split(":")[1].replace("Kebun ", "")
                st.markdown("<div class='farm-row'>" + short_name + " " + format_number(value) + "</div>", unsafe_allow_html=True)
            
            # Total Bunga and Total Bakul in blue
            st.markdown("""
            <div class="stats-item">
                <b>Total Bunga:</b> <span class="blue-data">""" + format_number(total_bunga) + """</span>
            </div>
            <div class="stats-item">
                <b>Total Bakul:</b> <span class="blue-data">""" + format_number(total_bakul) + """</span>
            </div>
            """, unsafe_allow_html=True)
            
            # Add a small separator
            st.markdown("<hr style='margin: 0.5rem 0; border-color: #eee;'>", unsafe_allow_html=True)
            
            # Create a row for the buttons
            button_col1, button_col2 = st.columns(2)
            
            with button_col1:
                # Confirm button
                if st.button("✅ CONFIRM & SAVE", key="confirm_save"):
                    # Add data with confirmation flag
                    result, _ = add_data(
                        date,
                        farm_data[FARM_COLUMNS[0]],
                        farm_data[FARM_COLUMNS[1]],
                        farm_data[FARM_COLUMNS[2]],
                        farm_data[FARM_COLUMNS[3]],
                        confirmed=True
                    )
                    
                    if result == "success":
                        st.success("Data for " + date_formatted + " added successfully!")
                        # Reset confirmation state
                        st.session_state.confirm_data = False
                        st.session_state.data_to_confirm = None
                        st.rerun()
            
            with button_col2:
                # Cancel button
                if st.button("❌ CANCEL", key="cancel_save"):
                    # Reset confirmation state
                    st.session_state.confirm_data = False
                    st.session_state.data_to_confirm = None
                    st.rerun()
        
        if not st.session_state.confirm_data:
            # Form for data entry
            with st.form("data_entry_form", clear_on_submit=False):
                # Date picker
                today = datetime.now().date()
                date = st.date_input("Select Date", today)
                
                # Create a row with 4 columns for farm inputs
                col1, col2, col3, col4 = st.columns(4)
                
                with col1:
                    farm_1 = st.number_input("" + FARM_COLUMNS[0] + " (Bunga)", min_value=0, value=0, step=1)
                
                with col2:
                    farm_2 = st.number_input("" + FARM_COLUMNS[1] + " (Bunga)", min_value=0, value=0, step=1)
                    
                with col3:
                    farm_3 = st.number_input("" + FARM_COLUMNS[2] + " (Bunga)", min_value=0, value=0, step=1)
                    
                with col4:
                    farm_4 = st.number_input("" + FARM_COLUMNS[3] + " (Bunga)", min_value=0, value=0, step=1)
                
                # Submit button
                submitted = st.form_submit_button("Review Data")
                
                if submitted:
                    result, data = add_data(date, farm_1, farm_2, farm_3, farm_4)
                    
                    if result == "confirm":
                        # Set confirmation state
                        st.session_state.confirm_data = True
                        st.session_state.data_to_confirm = data
                        st.rerun()
        
        # Display the current data
        st.header("Current Data")
        
        if not st.session_state.current_user_data.empty:
            # Format the date column to display only the date part
            display_df = st.session_state.current_user_data.copy()
            
            # Keep only the required columns - Date and Farm columns
            display_df = display_df[['Date'] + FARM_COLUMNS]
            
            if 'Date' in display_df.columns:
                display_df['Date'] = pd.to_datetime(display_df['Date']).dt.date
            
            # Format numbers with thousand separators
            for col in FARM_COLUMNS:
                if col in display_df.columns:
                    display_df[col] = display_df[col].apply(format_number)
            
            # Add row numbers starting from 1
            display_df.index = display_df.index + 1
            
            st.dataframe(display_df, use_container_width=True)
            
            # Allow downloading the data
            csv = st.session_state.current_user_data.to_csv(index=False).encode('utf-8')
            st.download_button(
                label="Download Data as CSV",
                data=csv,
                file_name=st.session_state.username + "_bunga_data_export.csv",
                mime="text/csv"
            )
        else:
            st.info("No data available. Add data using the form above.")
    
    # Tab 2: Data Analysis - KEEP YOUR EXISTING CODE
    with tab2:
        st.header("Bunga Production Analysis")
        
        if st.session_state.current_user_data.empty:
            st.info("No data available for analysis. Please add data in the Data Entry tab.")
        else:
            # Use the data already in datetime format
            analysis_df = st.session_state.current_user_data.copy()
            
            # Keep only necessary columns
            analysis_cols = ['Date'] + FARM_COLUMNS
            all_cols = set(analysis_df.columns)
            for col in all_cols:
                if col not in analysis_cols:
                    analysis_df = analysis_df.drop(col, axis=1)
            
            # Date range filter
            st.subheader("Filter by Date Range")
            
            # Get min and max dates from data
            analysis_df['Date'] = pd.to_datetime(analysis_df['Date'])
            min_date = analysis_df['Date'].min().date()
            max_date = analysis_df['Date'].max().date()
            
            # Create date range slider
            date_range = st.date_input(
                "Select date range",
                value=(min_date, max_date),
                min_value=min_date,
                max_value=max_date
            )
            
            # Check if two dates were selected
            if len(date_range) == 2:
                start_date, end_date = date_range
                # Filter the data
                filtered_df = analysis_df[
                    (analysis_df['Date'] >= pd.Timestamp(start_date)) & 
                    (analysis_df['Date'] <= pd.Timestamp(end_date))
                ]
                
                # Calculate total bunga for the filtered data
                total_bunga = int(filtered_df[FARM_COLUMNS].sum().sum())
                
                # Calculate total bakul
                total_bakul = int(total_bunga / 40)
                
                # Display totals prominently
                st.markdown("""
                <div style="background-color: #ffeeee; padding: 15px; border-radius: 5px; margin-bottom: 20px;">
                    <h1 style="color: #ff0000; font-weight: bold; font-size: 2.5em; text-align: center;">
                        Total Bunga: """ + format_number(total_bunga) + """
                    </h1>
                </div>
                """, unsafe_allow_html=True)
                
                st.markdown("""
                <div style="background-color: #eeeeff; padding: 15px; border-radius: 5px; margin-bottom: 20px;">
                    <h1 style="color: #0000ff; font-weight: bold; font-size: 2.5em; text-align: center;">
                        Total Bakul: """ + format_number(total_bakul) + """
                    </h1>
                </div>
                """, unsafe_allow_html=True)
                
                # Show filtered data
                st.subheader("Filtered Data")
                
                # Reorganize columns
                filtered_display = filtered_df.copy()
                
                # Add day of week and calculate total bunga for each row
                filtered_display['Day'] = filtered_display['Date'].dt.strftime('%A')
                filtered_display['Total Bunga'] = filtered_display[FARM_COLUMNS].sum(axis=1).astype(int)
                filtered_display['Date'] = filtered_display['Date'].dt.date
                
                # Reorder columns
                filtered_display = filtered_display[['Date', 'Day', 'Total Bunga'] + FARM_COLUMNS]
                
                # Format numbers
                filtered_display['Total Bunga'] = filtered_display['Total Bunga'].apply(format_number)
                for col in FARM_COLUMNS:
                    filtered_display[col] = filtered_display[col].apply(format_number)
                
                # Add row numbers
                filtered_display.index = filtered_display.index + 1
                
                st.dataframe(filtered_display, use_container_width=True)
                
                # Farm summary statistics
                st.subheader("Farm Totals")
                
                summary = pd.DataFrame({
                    'Farm': FARM_COLUMNS,
                    'Total': [int(filtered_df[col].sum()) for col in FARM_COLUMNS]
                })
                
                # Add total row
                total_row = pd.DataFrame({
                    'Farm': ['Total All Farms'],
                    'Total': [int(filtered_df[FARM_COLUMNS].sum().sum())]
                })
                summary = pd.concat([summary, total_row], ignore_index=True)
                
                # Format numbers
                summary['Total'] = summary['Total'].apply(format_number)
                summary.index = summary.index + 1
                
                st.dataframe(summary, use_container_width=True)
                
                # Create visualizations
                st.subheader("Visualizations")
                
                # Farm comparison visualization
                farm_totals = pd.DataFrame({
                    'Farm': FARM_COLUMNS,
                    'Total Bunga': [int(filtered_df[col].sum()) for col in FARM_COLUMNS]
                })
                
                farm_totals['Total Bakul'] = (farm_totals['Total Bunga'] / 40).apply(lambda x: round(x, 1))
                
                # Color settings
                farm_colors = px.colors.qualitative.Set3[:len(FARM_COLUMNS)]
                bunga_color = "#ff0000"
                bakul_color = "#0000ff"
                
                # Chart type selection
                chart_type = st.radio("Select Chart Type", ["Bar Chart", "Pie Chart"], horizontal=True)
                
                # Create two columns for Bunga and Bakul visualizations
                bunga_col, bakul_col = st.columns(2)
                
                with bunga_col:
                    st.markdown("<h4 style='color: " + bunga_color + ";'>Total Bunga</h4>", unsafe_allow_html=True)
                    if chart_type == "Bar Chart":
                        fig_bunga = px.bar(
                            farm_totals,
                            x='Farm',
                            y='Total Bunga',
                            color='Farm',
                            title="Bunga Production by Farm",
                            color_discrete_sequence=farm_colors
                        )
                        fig_bunga.update_layout(
                            yaxis=dict(tickformat=","),
                            title={'text': "Bunga Production by Farm", 'font': {'color': bunga_color}}
                        )
                        st.plotly_chart(fig_bunga, use_container_width=True)
                    else:
                        fig_bunga = px.pie(
                            farm_totals,
                            values='Total Bunga',
                            names='Farm',
                            title="Bunga Production Distribution",
                            color='Farm',
                            color_discrete_sequence=farm_colors
                        )
                        fig_bunga.update_traces(
                            texttemplate="%{value:,}",
                            hovertemplate="%{label}: %{value:,} Bunga<extra></extra>"
                        )
                        fig_bunga.update_layout(
                            title={'text': "Bunga Production Distribution", 'font': {'color': bunga_color}}
                        )
                        st.plotly_chart(fig_bunga, use_container_width=True)
                
                with bakul_col:
                    st.markdown("<h4 style='color: " + bakul_color + ";'>Total Bakul</h4>", unsafe_allow_html=True)
                    if chart_type == "Bar Chart":
                        fig_bakul = px.bar(
                            farm_totals,
                            x='Farm',
                            y='Total Bakul',
                            color='Farm',
                            title="Bakul Production by Farm",
                            color_discrete_sequence=farm_colors
                        )
                        fig_bakul.update_layout(
                            title={'text': "Bakul Production by Farm", 'font': {'color': bakul_color}}
                        )
                        st.plotly_chart(fig_bakul, use_container_width=True)
                    else:
                        fig_bakul = px.pie(
                            farm_totals,
                            values='Total Bakul',
                            names='Farm',
                            title="Bakul Production Distribution",
                            color='Farm',
                            color_discrete_sequence=farm_colors
                        )
                        fig_bakul.update_traces(
                            texttemplate="%{value:.1f}",
                            hovertemplate="%{label}: %{value:.1f} Bakul<extra></extra>"
                        )
                        fig_bakul.update_layout(
                            title={'text': "Bakul Production Distribution", 'font': {'color': bakul_color}}
                        )
                        st.plotly_chart(fig_bakul, use_container_width=True)
                
                # Daily production charts
                st.subheader("Daily Production")
                
                daily_totals = filtered_df.copy()
                daily_totals['Day'] = daily_totals['Date'].dt.strftime('%A')
                daily_totals['Date_Display'] = daily_totals['Date'].dt.strftime('%Y-%m-%d')
                daily_totals['Total Bunga'] = daily_totals[FARM_COLUMNS].sum(axis=1)
                daily_totals['Total Bakul'] = (daily_totals['Total Bunga'] / 40).apply(lambda x: round(x, 1))
                
                daily_bunga_col, daily_bakul_col = st.columns(2)
                
                with daily_bunga_col:
                    st.markdown("<h4 style='color: " + bunga_color + ";'>Daily Bunga</h4>", unsafe_allow_html=True)
                    fig_daily_bunga = px.scatter(
                        daily_totals,
                        x='Date',
                        y='Total Bunga',
                        title="Daily Bunga Production",
                        size='Total Bunga',
                        size_max=15,
                    )
                    fig_daily_bunga.update_layout(
                        xaxis=dict(
                            title="Date",
                            tickformat="%Y-%m-%d"
                        ),
                        yaxis=dict(
                            title="Total Bunga",
                            tickformat=","
                        ),
                        title={'text': "Daily Bunga Production", 'font': {'color': bunga_color}}
                    )
                    fig_daily_bunga.update_traces(
                        hovertemplate="Date: %{x|%Y-%m-%d}<br>Total: %{y:,} Bunga<extra></extra>",
                        marker=dict(color=bunga_color, opacity=0.8)
                    )
                    st.plotly_chart(fig_daily_bunga, use_container_width=True)
                
                with daily_bakul_col:
                    st.markdown("<h4 style='color: " + bakul_color + ";'>Daily Bakul</h4>", unsafe_allow_html=True)
                    fig_daily_bakul = px.scatter(
                        daily_totals,
                        x='Date',
                        y='Total Bakul',
                        title="Daily Bakul Production",
                        size='Total Bakul',
                        size_max=15,
                    )
                    fig_daily_bakul.update_layout(
                        xaxis=dict(
                            title="Date",
                            tickformat="%Y-%m-%d"
                        ),
                        yaxis=dict(title="Total Bakul"),
                        title={'text': "Daily Bakul Production", 'font': {'color': bakul_color}}
                    )
                    fig_daily_bakul.update_traces(
                        hovertemplate="Date: %{x|%Y-%m-%d}<br>Total: %{y:.1f} Bakul<extra></extra>",
                        marker=dict(color=bakul_color, opacity=0.8)
                    )
                    st.plotly_chart(fig_daily_bakul, use_container_width=True)
                
                # Option to download filtered data
                csv = filtered_df.to_csv(index=False).encode('utf-8')
                st.download_button(
                    label="Download Filtered Data as CSV",
                    data=csv,
                    file_name=st.session_state.username + "_bunga_data_" + str(start_date) + "_to_" + str(end_date) + ".csv",
                    mime="text/csv"
                )
            else:
                st.info("Please select both start and end dates.")
    
    # Tab 3: Revenue Estimate - NEW TAB
    with tab3:
        revenue_estimate_tab()

# Sidebar options - Keep your existing implementation
def sidebar_options():
    st.sidebar.header("User: " + st.session_state.username)
    
    # Logout button
    if st.sidebar.button("Logout"):
        st.session_state.logged_in = False
        st.session_state.username = ""
        st.session_state.role = ""
        st.session_state.current_user_data = pd.DataFrame(columns=['Date'] + FARM_COLUMNS)
        st.session_state.needs_rerun = True
        return

    # Data Management
    st.sidebar.header("Data Management")

    # Data editing section
    if not st.session_state.current_user_data.empty:
        st.sidebar.subheader("Edit or Delete Records")
        
        if 'Date' in st.session_state.current_user_data.columns:
            st.session_state.current_user_data['Date'] = pd.to_datetime(st.session_state.current_user_data['Date'])
            dates = st.session_state.current_user_data['Date'].dt.date.unique()
            selected_date = st.sidebar.selectbox("Select date to edit/delete:", dates)
            
            date_idx = st.session_state.current_user_data[st.session_state.current_user_data['Date'].dt.date == selected_date].index[0]
            
            st.sidebar.text("Current values for " + str(selected_date) + ":")
            current_row = st.session_state.current_user_data.iloc[date_idx]
            
            formatted_values = []
            for col in FARM_COLUMNS:
                formatted_values.append(col + ": " + format_number(current_row[col]))
            
            st.sidebar.text("\n".join(formatted_values))
            
            # Edit form
            with st.sidebar.expander("Edit this record"):
                with st.form("edit_form"):
                    edit_values = []
                    
                    for i, col in enumerate(FARM_COLUMNS):
                        edit_values.append(st.number_input(
                            col, 
                            value=int(current_row[col]), 
                            min_value=0
                        ))
                    
                    if st.form_submit_button("Update Record"):
                        # Update the values
                        for i, col in enumerate(FARM_COLUMNS):
                            st.session_state.current_user_data.at[date_idx, col] = edit_values[i]
                        
                        # Save to database
                        if save_data(st.session_state.current_user_data, st.session_state.username):
                            st.sidebar.success("Record for " + str(selected_date) + " updated!")
                            st.session_state.needs_rerun = True
            
            # Delete option
            delete_key = "delete_" + str(selected_date)
            if delete_key not in st.session_state:
                st.session_state[delete_key] = False
            
            if st.sidebar.button("Delete record for " + str(selected_date)):
                st.session_state[delete_key] = True
            
            if st.session_state[delete_key]:
                confirm = st.sidebar.checkbox("I confirm I want to delete this record", key="confirm_" + str(selected_date))
                if confirm:
                    # Drop the row
                    st.session_state.current_user_data = st.session_state.current_user_data.drop(date_idx).reset_index(drop=True)
                    
                    # Save to database
                    if save_data(st.session_state.current_user_data, st.session_state.username):
                        st.sidebar.success("Record for " + str(selected_date) + " deleted!")
                        st.session_state.needs_rerun = True
                        st.session_state[delete_key] = False
                        # FORCE RERUN IMMEDIATELY to update charts
                        st.rerun()
                else:
                    if st.sidebar.button("Cancel deletion"):
                        st.session_state[delete_key] = False

    # Upload CSV file
    st.sidebar.subheader("Import Data")
    uploaded_file = st.sidebar.file_uploader("Upload existing data (CSV)", type="csv")
    if uploaded_file is not None:
        try:
            uploaded_df = pd.read_csv(uploaded_file)
            
            # Check if the required columns exist
            required_cols = ['Date']
            has_old_cols = all(col in uploaded_df.columns for col in OLD_FARM_COLUMNS)
            has_new_cols = all(col in uploaded_df.columns for col in FARM_COLUMNS)
            
            if 'Date' in uploaded_df.columns and (has_old_cols or has_new_cols):
                uploaded_df['Date'] = pd.to_datetime(uploaded_df['Date'])
                
                # Convert old column names to new ones if needed
                if has_old_cols and not has_new_cols:
                    for old_col, new_col in zip(OLD_FARM_COLUMNS, FARM_COLUMNS):
                        uploaded_df[new_col] = uploaded_df[old_col]
                        uploaded_df = uploaded_df.drop(old_col, axis=1)
                
                action = st.sidebar.radio("Select action", ["Replace current data", "Append to current data"])
                
                if st.sidebar.button("Confirm Import"):
                    if action == "Replace current data":
                        st.session_state.current_user_data = uploaded_df
                    else:
                        combined = pd.concat([st.session_state.current_user_data, uploaded_df])
                        st.session_state.current_user_data = combined.drop_duplicates(subset=['Date']).sort_values(by='Date').reset_index(drop=True)
                    
                    if save_data(st.session_state.current_user_data, st.session_state.username):
                        st.sidebar.success("Data imported successfully!")
                        st.session_state.needs_rerun = True
            else:
                required_cols_str = ", ".join(['Date'] + FARM_COLUMNS)
                st.sidebar.error("CSV must contain columns: " + required_cols_str)
        except Exception as e:
            st.sidebar.error("Error importing data: " + str(e))

    # Clear all data button
    st.sidebar.subheader("Clear Data")
    if 'show_clear_confirm' not in st.session_state:
        st.session_state.show_clear_confirm = False

    if st.sidebar.button("Clear All Data"):
        st.session_state.show_clear_confirm = True

    if st.session_state.show_clear_confirm:
        confirm = st.sidebar.checkbox("I confirm I want to delete all data", key="confirm_clear_all")
        if confirm:
            st.session_state.current_user_data = pd.DataFrame(columns=['Date'] + FARM_COLUMNS)
            
            if save_data(st.session_state.current_user_data, st.session_state.username):
                st.sidebar.success("All data cleared!")
                st.session_state.needs_rerun = True
                st.session_state.show_clear_confirm = False
        else:
            if st.sidebar.button("Cancel clear"):
                st.session_state.show_clear_confirm = False

    # Storage info
    st.sidebar.markdown("---")
    st.sidebar.subheader("Storage Information")
    storage_color = "🟢" if "Firebase" in st.session_state.storage_mode else "🟡"
    st.sidebar.info(storage_color + " Data Storage Mode: " + st.session_state.storage_mode)
    
    if st.session_state.storage_mode == "Session State":
        st.sidebar.warning("Data is stored in browser session only. For permanent storage, download your data regularly.")

    # Footer
    st.sidebar.markdown("---")
    st.sidebar.markdown("🌷 Bunga di Kebun - Firebase Storage v1.0")
    st.sidebar.text("User: " + st.session_state.username + " (" + st.session_state.role + ")")

# Initialize app - Fixed version
def initialize_app():
    # Try Firebase first
    users = get_users_collection()
    if users:
        try:
            # Check if admin user exists
            admin_doc = users.document("admin").get()
            if not admin_doc.exists:
                # Create admin user if it doesn't exist
                add_user("admin", "admin", "admin")
            return
        except Exception as e:
            st.error("Error initializing Firebase: " + str(e))
            # Fallback to session state
            pass
    
    # Initialize session state storage
    initialize_session_storage()

# Determine storage mode at startup - Fixed version
def check_storage_mode():
    db = connect_to_firebase()
    if db:
        try:
            # Quick test of Firebase connection
            users = db.collection('users')
            # Try to get documents but don't fail if collection is empty
            try:
                list(users.limit(1).get())
            except:
                pass  # Collection might be empty, that's okay
            
            st.session_state.storage_mode = "Firebase Database"
            return
        except Exception as e:
            st.error("Firebase connection test failed: " + str(e))
    
    st.session_state.storage_mode = "Session State"

# Initialize app on first run
initialize_app()

# Main application logic
if st.session_state.storage_mode == "Checking...":
    check_storage_mode()

if not st.session_state.logged_in:
    login_page()
else:
    main_app()
    sidebar_options()
