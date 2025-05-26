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

# Define buyers and fruit sizes for revenue calculation
BUYERS = ['Green', 'Kedah', 'YY', 'Lukut', 'PD']
FRUIT_SIZES = ['>600g', '>500g', '>400g', '>300g', 'Reject']
DEFAULT_DISTRIBUTION = {'>600g': 10, '>500g': 20, '>400g': 30, '>300g': 30, 'Reject': 10}

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
    st.session_state.csv_backup_enabled = True
if 'revenue_transactions' not in st.session_state:
    st.session_state.revenue_transactions = []
if 'current_transaction' not in st.session_state:
    st.session_state.current_transaction = None
if 'scenarios' not in st.session_state:
    st.session_state.scenarios = {}

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
        st.error(f"Firebase connection error: {str(e)}")
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
            st.error(f"Error accessing users collection: {e}")
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
            st.error(f"Error accessing farm_data collection: {e}")
            return None
    return None

def get_revenue_data_collection():
    """Get Firebase collection for revenue transactions"""
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
            st.error(f"Error accessing revenue_data collection: {e}")
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
            st.error(f"Error adding user to Firebase: {e}")
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
            st.error(f"Error verifying user from Firebase: {e}")
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
            st.error(f"Error loading data from Firebase: {e}")
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

def load_revenue_data(username):
    """Load revenue transaction data for a user"""
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
            st.error(f"Error loading revenue data from Firebase: {e}")
            pass
    
    # Fallback to session state
    if 'revenue_transactions' not in st.session_state:
        st.session_state.revenue_transactions = []
    
    return [t for t in st.session_state.revenue_transactions if t.get('username') == username]

def save_revenue_data(transactions, username):
    """Save revenue transaction data"""
    revenue_data = get_revenue_data_collection()
    if revenue_data:
        try:
            # Get existing transactions for this user
            existing_docs = revenue_data.where("username", "==", username).get()
            
            # Delete existing transactions
            for doc in existing_docs:
                doc.reference.delete()
            
            # Add new transactions
            for transaction in transactions:
                transaction['username'] = username
                revenue_data.add(transaction)
            
            return True
        except Exception as e:
            st.error(f"Error saving revenue data to Firebase: {e}")
            pass
    
    # Fallback to session state
    if 'revenue_transactions' not in st.session_state:
        st.session_state.revenue_transactions = []
    
    # Remove existing transactions for this user and add new ones
    st.session_state.revenue_transactions = [
        t for t in st.session_state.revenue_transactions if t.get('username') != username
    ]
    
    for transaction in transactions:
        transaction['username'] = username
        st.session_state.revenue_transactions.append(transaction)
    
    return True

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
                    print(f"Updated existing record for {record_date}")
                else:
                    # ADD new record
                    farm_data.add(record)
                    print(f"Added new record for {record_date}")
            
            # STEP 3: DELETE records that exist in Firebase but NOT in current DataFrame
            dates_to_delete = set(existing_dates.keys()) - current_dates
            
            if dates_to_delete:
                print(f"🔥 DELETING {len(dates_to_delete)} records from Firebase...")
                for date_to_delete in dates_to_delete:
                    if date_to_delete in existing_dates:
                        doc_id = existing_dates[date_to_delete]
                        farm_data.document(doc_id).delete()
                        print(f"🗑️ DELETED record for {date_to_delete}")
                        st.success(f"Deleted {date_to_delete} from Firebase!")
            
            return True
        except Exception as e:
            st.error(f"Error saving data to Firebase: {e}")
            # Fallback to session state
            pass
    
    # Session state storage (unchanged)
    if 'farm_data' not in st.session_state:
        initialize_session_storage()
        
    st.session_state.farm_data[username] = df.to_dict('records')
    return True

# ALSO ADD this helper function to your app:

def add_single_record(date, farm_1, farm_2, farm_3, farm_4, username):
    """
    Add a single record without affecting existing data
    """
    try:
        farm_data = get_farm_data_collection()
        if farm_data:
            # Check if this date already exists
            existing_query = farm_data.where("username", "==", username).where("Date", "==", date.isoformat()).get()
            
            if len(list(existing_query)) > 0:
                st.error(f"Data for {date} already exists!")
                return False
            
            # Create new record
            record = {
                'Date': date.isoformat(),
                'username': username,
                'A: Kebun Sendiri': int(farm_1),
                'B: Kebun DeYe': int(farm_2), 
                'C: Kebun Asan': int(farm_3),
                'D: Kebun Uncle': int(farm_4)
            }
            
            # Add to Firebase
            farm_data.add(record)
            st.success(f"Added data for {date}")
            return True
            
    except Exception as e:
        st.error(f"Error adding record: {e}")
        return False

print("FIXED THE OVERWRITE BUG!")
print("=" * 50)
print("KEY CHANGES:")
print("1. No more deleting ALL user data")
print("2. Updates existing records instead of deleting")
print("3. Only adds new records for new dates")
print("4. Prevents accidental data loss")
print("\nNow you can safely re-enter your 2025 data!")

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
            st.error(f"Error initializing Firebase: {e}")
            # Fallback to session state
            pass
    
    # Initialize session state storage
    initialize_session_storage()

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
            formatted_data[col] = formatted_data[col].apply(lambda x: f"{int(x):,}")
        
        # Create CSV in memory with clean formatting
        csv_buffer = io.StringIO()
        formatted_data.to_csv(csv_buffer, index=False)
        csv_content = csv_buffer.getvalue()
        
        return csv_content, f"✅ CSV backup created: {len(formatted_data)} records"
        
    except Exception as e:
        return None, f"❌ Backup failed: {str(e)}"

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
        
        # Create message (NO ATTACHMENT)
        message = MIMEMultipart('mixed')
        message["From"] = sender_email
        message["To"] = receiver_email
        message["Subject"] = f"Total Bunga {date_formatted}: {total_bunga:,} bunga, {total_bakul} bakul"
        
        # Format farm details with proper HTML line breaks
        farm_info = f"""A: Kebun Sendiri: {farm_data['A: Kebun Sendiri']:,} Bunga<br>
B: Kebun DeYe&nbsp;&nbsp;&nbsp;: {farm_data['B: Kebun DeYe']:,} Bunga<br>
C: Kebun Asan&nbsp;&nbsp;&nbsp;: {farm_data['C: Kebun Asan']:,} Bunga<br>
D: Kebun Uncle&nbsp;&nbsp;: {farm_data['D: Kebun Uncle']:,} Bunga"""
        
        # Get Malaysia time
        malaysia_tz = timezone(timedelta(hours=8))
        malaysia_time = datetime.now(malaysia_tz).strftime('%Y-%m-%d %H:%M:%S')
        
        # HTML email format
        html_body = f"""<!DOCTYPE html>
<html>
<head>
    <meta charset="UTF-8">
    <style>
        body {{ font-family: Arial, sans-serif; line-height: 1.6; }}
        .highlight {{ color: #FF0000; font-weight: bold; }}
        .farm-details {{ 
            font-family: Courier New, monospace; 
            background-color: #f5f5f5;
            padding: 10px;
            border-radius: 5px;
            margin: 10px 0;
        }}
    </style>
</head>
<body>
<p>New flower data has been added to Bunga di Kebun system.</p>

<p class="highlight">Date: {date_formatted} ({day_name})</p>

<p class="highlight">Total bunga: {total_bunga:,}</p>

<p class="highlight">Total bakul: {total_bakul}</p>

<p><strong>Farm Details:</strong></p>

<div class="farm-details">
{farm_info}
</div>

<p><strong>System Information:</strong></p>

<p>Password retrieved from: {password_source}</p>

<p>Timestamp: {malaysia_time} (Malaysia Time)</p>

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

# Revenue calculation functions
def calculate_bakul_distribution(total_bakul, distribution_percentages):
    """Calculate number of bakul per fruit size based on percentages"""
    bakul_per_size = {}
    remaining_bakul = total_bakul
    
    # Calculate bakul for each size except the last one
    for i, size in enumerate(FRUIT_SIZES[:-1]):
        percentage = distribution_percentages[size]
        bakul_count = int(total_bakul * percentage / 100)
        bakul_per_size[size] = bakul_count
        remaining_bakul -= bakul_count
    
    # Assign remaining bakul to the last size to ensure total matches
    bakul_per_size[FRUIT_SIZES[-1]] = max(0, remaining_bakul)
    
    return bakul_per_size

def calculate_revenue(buyer_allocation, prices):
    """Calculate revenue based on buyer allocation and prices"""
    revenue_summary = {}
    total_revenue = 0
    
    for buyer in BUYERS:
        buyer_revenue = 0
        if buyer in buyer_allocation:
            for size in FRUIT_SIZES:
                if size in buyer_allocation[buyer] and size in prices[buyer]:
                    bakul_count = buyer_allocation[buyer][size]
                    price = prices[buyer][size]
                    revenue = bakul_count * price
                    buyer_revenue += revenue
        
        revenue_summary[buyer] = buyer_revenue
        total_revenue += buyer_revenue
    
    return revenue_summary, total_revenue

def get_bakul_from_flower_date(flower_date, username):
    """Get total bakul from a specific flower harvest date"""
    if st.session_state.current_user_data.empty:
        return 0
    
    # Convert flower_date to datetime for comparison
    flower_date = pd.to_datetime(flower_date).date()
    
    # Find the row for this date
    data_copy = st.session_state.current_user_data.copy()
    data_copy['Date'] = pd.to_datetime(data_copy['Date']).dt.date
    
    matching_rows = data_copy[data_copy['Date'] == flower_date]
    
    if not matching_rows.empty:
        row = matching_rows.iloc[0]
        total_bunga = sum([row[col] for col in FARM_COLUMNS if col in row])
        total_bakul = int(total_bunga / 40)
        return total_bakul
    
    return 0

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
                st.error(f"Data for {date} already exists. Please edit the existing entry or choose a different date.")
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
                st.success(f"Data added and email sent {backup_status}!")
            else:
                if "Email password not found" in error_message:
                    st.warning(f"Data added but email notification could not be sent: {error_message}")
                else:
                    st.warning(f"Data added but failed to send notification: {error_message}")
                    
            return "success", None
        else:
            # If save fails, revert the change
            # Don't reload data on save failure - keep current session data
            pass
            return "error", None
            
    except Exception as e:
        st.error(f"Error adding data: {str(e)}")
        return "error", None

# Format number with thousands separator
def format_number(number):
    return f"{int(number):,}"

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
                        st.success(f"Welcome back, {username}!")
                        
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

# Revenue calculation tab
def revenue_calculation_tab():
    st.header("💰 Revenue Calculation")
    
    # Load existing revenue transactions
    user_transactions = load_revenue_data(st.session_state.username)
    
    # Create sub-tabs
    entry_tab, analysis_tab, scenarios_tab = st.tabs(["Transaction Entry", "Revenue Analysis", "Scenario Comparison"])
    
    with entry_tab:
        st.subheader("Add New Revenue Transaction")
        
        with st.form("revenue_transaction_form"):
            col1, col2 = st.columns(2)
            
            with col1:
                transaction_date = st.date_input("Transaction Date", datetime.now().date())
            
            with col2:
                bakul_source = st.radio(
                    "Bakul Source",
                    ["Enter Total Bakul Manually", "Select from Flower Date"],
                    horizontal=True
                )
            
            # Bakul input section
            if bakul_source == "Enter Total Bakul Manually":
                total_bakul = st.number_input("Total Bakul", min_value=0, value=100, step=1)
            else:
                # Show available flower dates
                if not st.session_state.current_user_data.empty:
                    available_dates = pd.to_datetime(st.session_state.current_user_data['Date']).dt.date.unique()
                    available_dates = sorted(available_dates, reverse=True)
                    
                    selected_flower_date = st.selectbox(
                        "Select Flower Harvest Date",
                        available_dates,
                        format_func=lambda x: f"{x} ({get_bakul_from_flower_date(x, st.session_state.username)} bakul)"
                    )
                    
                    total_bakul = get_bakul_from_flower_date(selected_flower_date, st.session_state.username)
                    st.info(f"Total Bakul from {selected_flower_date}: {total_bakul}")
                else:
                    st.warning("No flower data available. Please add flower data first or enter bakul manually.")
                    total_bakul = st.number_input("Total Bakul", min_value=0, value=100, step=1)
            
            # Fruit size distribution
            st.subheader("Fruit Size Distribution (%)")
            distribution_cols = st.columns(len(FRUIT_SIZES))
            distribution_percentages = {}
            
            for i, size in enumerate(FRUIT_SIZES):
                with distribution_cols[i]:
                    distribution_percentages[size] = st.number_input(
                        f"{size}",
                        min_value=0.0,
                        max_value=100.0,
                        value=float(DEFAULT_DISTRIBUTION[size]),
                        step=0.1,
                        key=f"dist_{size}"
                    )
            
            # Show total percentage
            total_percentage = sum(distribution_percentages.values())
            if total_percentage != 100:
                st.error(f"Total percentage must equal 100%. Current total: {total_percentage:.1f}%")
            else:
                st.success(f"✅ Total percentage: {total_percentage:.1f}%")
            
            # Calculate bakul per size
            if total_percentage == 100:
                bakul_per_size = calculate_bakul_distribution(total_bakul, distribution_percentages)
                
                st.subheader("Bakul Distribution by Size")
                dist_cols = st.columns(len(FRUIT_SIZES))
                for i, size in enumerate(FRUIT_SIZES):
                    with dist_cols[i]:
                        st.metric(size, f"{bakul_per_size[size]} bakul")
            
            # Buyer pricing section
            st.subheader("Buyer Pricing (RM per Bakul)")
            
            buyer_prices = {}
            selected_buyers = []
            
            # Create expandable sections for each buyer
            for buyer in BUYERS:
                with st.expander(f"💼 {buyer}", expanded=False):
                    include_buyer = st.checkbox(f"Include {buyer} in this transaction", key=f"include_{buyer}")
                    
                    if include_buyer:
                        selected_buyers.append(buyer)
                        buyer_prices[buyer] = {}
                        
                        price_cols = st.columns(len(FRUIT_SIZES))
                        for j, size in enumerate(FRUIT_SIZES):
                            with price_cols[j]:
                                buyer_prices[buyer][size] = st.number_input(
                                    f"{size}",
                                    min_value=0.00,
                                    value=10.00,
                                    step=0.01,
                                    format="%.2f",
                                    key=f"price_{buyer}_{size}"
                                )
            
            submitted = st.form_submit_button("Add Transaction", disabled=(total_percentage != 100))
            
            if submitted and total_percentage == 100 and selected_buyers:
                # Create transaction record
                transaction = {
                    'id': str(uuid.uuid4()),
                    'date': transaction_date.isoformat(),
                    'total_bakul': total_bakul,
                    'distribution_percentages': distribution_percentages,
                    'bakul_per_size': bakul_per_size,
                    'selected_buyers': selected_buyers,
                    'buyer_prices': buyer_prices,
                    'created_at': datetime.now().isoformat()
                }
                
                # Add to transactions
                user_transactions.append(transaction)
                
                # Save to database
                if save_revenue_data(user_transactions, st.session_state.username):
                    st.success("Transaction added successfully!")
                    st.rerun()
                else:
                    st.error("Failed to save transaction")
            elif submitted and not selected_buyers:
                st.error("Please select at least one buyer")
    
    with analysis_tab:
        st.subheader("Revenue Analysis")
        
        if not user_transactions:
            st.info("No revenue transactions available. Add transactions in the Transaction Entry tab.")
        else:
            # Display transactions summary
            st.subheader("Recent Transactions")
            
            for i, transaction in enumerate(reversed(user_transactions[-5:])):  # Show last 5
                with st.expander(f"Transaction {len(user_transactions)-i}: {transaction['date']}", expanded=False):
                    
                    col1, col2 = st.columns(2)
                    
                    with col1:
                        st.write(f"**Date:** {transaction['date']}")
                        st.write(f"**Total Bakul:** {transaction['total_bakul']}")
                        st.write(f"**Buyers:** {', '.join(transaction['selected_buyers'])}")
                    
                    with col2:
                        # Calculate and show revenue for this transaction
                        # First, allocate bakul to buyers (simplified - equal distribution for now)
                        buyer_allocation = {}
                        num_buyers = len(transaction['selected_buyers'])
                        
                        for buyer in transaction['selected_buyers']:
                            buyer_allocation[buyer] = {}
                            for size in FRUIT_SIZES:
                                # Simple equal distribution among buyers
                                buyer_allocation[buyer][size] = transaction['bakul_per_size'][size] // num_buyers
                        
                        revenue_summary, total_revenue = calculate_revenue(buyer_allocation, transaction['buyer_prices'])
                        
                        st.write("**Revenue Summary:**")
                        for buyer in transaction['selected_buyers']:
                            st.write(f"- {buyer}: RM {revenue_summary[buyer]:.2f}")
                        st.write(f"**Total Revenue:** RM {total_revenue:.2f}")
            
            # Overall statistics
            st.subheader("Overall Statistics")
            
            total_transactions = len(user_transactions)
            total_bakul_all = sum([t['total_bakul'] for t in user_transactions])
            
            stat_col1, stat_col2, stat_col3 = st.columns(3)
            
            with stat_col1:
                st.metric("Total Transactions", total_transactions)
            
            with stat_col2:
                st.metric("Total Bakul Processed", f"{total_bakul_all:,}")
            
            with stat_col3:
                # Calculate total revenue across all transactions
                total_revenue_all = 0
                for transaction in user_transactions:
                    buyer_allocation = {}
                    num_buyers = len(transaction['selected_buyers'])
                    
                    for buyer in transaction['selected_buyers']:
                        buyer_allocation[buyer] = {}
                        for size in FRUIT_SIZES:
                            buyer_allocation[buyer][size] = transaction['bakul_per_size'][size] // num_buyers
                    
                    _, transaction_revenue = calculate_revenue(buyer_allocation, transaction['buyer_prices'])
                    total_revenue_all += transaction_revenue
                
                st.metric("Total Revenue", f"RM {total_revenue_all:,.2f}")
    
    with scenarios_tab:
        st.subheader("Scenario Comparison")
        
        if not user_transactions:
            st.info("No transactions available for scenario analysis.")
        else:
            # Select a base transaction
            transaction_options = [f"{t['date']} (ID: {t['id'][:8]})" for t in user_transactions]
            
            selected_transaction_idx = st.selectbox(
                "Select Base Transaction for Scenario Analysis",
                range(len(transaction_options)),
                format_func=lambda x: transaction_options[x]
            )
            
            base_transaction = user_transactions[selected_transaction_idx]
            
            st.subheader("Scenario 1: Original Transaction")
            
            # Show original transaction details
            col1, col2 = st.columns(2)
            
            with col1:
                st.write("**Original Allocation & Pricing:**")
                
                # Simple equal distribution for display
                buyer_allocation_original = {}
                num_buyers = len(base_transaction['selected_buyers'])
                
                for buyer in base_transaction['selected_buyers']:
                    buyer_allocation_original[buyer] = {}
                    for size in FRUIT_SIZES:
                        buyer_allocation_original[buyer][size] = base_transaction['bakul_per_size'][size] // num_buyers
                
                # Display allocation
                for buyer in base_transaction['selected_buyers']:
                    st.write(f"**{buyer}:**")
                    for size in FRUIT_SIZES:
                        bakul_count = buyer_allocation_original[buyer][size]
                        price = base_transaction['buyer_prices'][buyer][size]
                        st.write(f"  {size}: {bakul_count} bakul × RM{price:.2f} = RM{bakul_count * price:.2f}")
            
            with col2:
                revenue_summary_1, total_revenue_1 = calculate_revenue(buyer_allocation_original, base_transaction['buyer_prices'])
                
                st.write("**Scenario 1 Revenue:**")
                for buyer in base_transaction['selected_buyers']:
                    st.write(f"- {buyer}: RM {revenue_summary_1[buyer]:.2f}")
                st.write(f"**Total: RM {total_revenue_1:.2f}**")
            
            st.markdown("---")
            
            st.subheader("Scenario 2: Modified Allocation")
            
            # Allow user to modify bakul allocation
            st.write("Modify bakul allocation for comparison:")
            
            buyer_allocation_scenario2 = {}
            
            for buyer in base_transaction['selected_buyers']:
                st.write(f"**{buyer} Allocation:**")
                buyer_allocation_scenario2[buyer] = {}
                
                alloc_cols = st.columns(len(FRUIT_SIZES))
                
                for j, size in enumerate(FRUIT_SIZES):
                    with alloc_cols[j]:
                        original_count = buyer_allocation_original[buyer][size]
                        buyer_allocation_scenario2[buyer][size] = st.number_input(
                            f"{size}",
                            min_value=0,
                            value=original_count,
                            step=1,
                            key=f"scenario2_{buyer}_{size}"
                        )
            
            # Validate total allocation
            total_allocated_scenario2 = {}
            for size in FRUIT_SIZES:
                total_allocated_scenario2[size] = sum([buyer_allocation_scenario2[buyer][size] for buyer in base_transaction['selected_buyers']])
            
            st.write("**Allocation Summary:**")
            allocation_valid = True
            for size in FRUIT_SIZES:
                available = base_transaction['bakul_per_size'][size]
                allocated = total_allocated_scenario2[size]
                
                if allocated != available:
                    st.error(f"{size}: Allocated {allocated} ≠ Available {available}")
                    allocation_valid = False
                else:
                    st.success(f"{size}: {allocated} bakul (✓)")
            
            if allocation_valid:
                # Calculate scenario 2 revenue
                revenue_summary_2, total_revenue_2 = calculate_revenue(buyer_allocation_scenario2, base_transaction['buyer_prices'])
                
                st.subheader("Scenario Comparison")
                
                comparison_data = []
                for buyer in base_transaction['selected_buyers']:
                    comparison_data.append({
                        'Buyer': buyer,
                        'Scenario 1 (RM)': revenue_summary_1[buyer],
                        'Scenario 2 (RM)': revenue_summary_2[buyer],
                        'Difference (RM)': revenue_summary_2[buyer] - revenue_summary_1[buyer],
                        'Change (%)': ((revenue_summary_2[buyer] - revenue_summary_1[buyer]) / revenue_summary_1[buyer] * 100) if revenue_summary_1[buyer] > 0 else 0
                    })
                
                # Add total row
                comparison_data.append({
                    'Buyer': 'TOTAL',
                    'Scenario 1 (RM)': total_revenue_1,
                    'Scenario 2 (RM)': total_revenue_2,
                    'Difference (RM)': total_revenue_2 - total_revenue_1,
                    'Change (%)': ((total_revenue_2 - total_revenue_1) / total_revenue_1 * 100) if total_revenue_1 > 0 else 0
                })
                
                comparison_df = pd.DataFrame(comparison_data)
                
                # Format the dataframe for display
                display_df = comparison_df.copy()
                for col in ['Scenario 1 (RM)', 'Scenario 2 (RM)', 'Difference (RM)']:
                    display_df[col] = display_df[col].apply(lambda x: f"{x:.2f}")
                display_df['Change (%)'] = display_df['Change (%)'].apply(lambda x: f"{x:+.1f}%")
                
                st.dataframe(display_df, use_container_width=True)
                
                # Visualization
                fig = go.Figure()
                
                buyers_for_chart = [row['Buyer'] for row in comparison_data[:-1]]  # Exclude total
                scenario1_values = [row['Scenario 1 (RM)'] for row in comparison_data[:-1]]
                scenario2_values = [row['Scenario 2 (RM)'] for row in comparison_data[:-1]]
                
                fig.add_trace(go.Bar(
                    name='Scenario 1',
                    x=buyers_for_chart,
                    y=scenario1_values,
                    marker_color='lightblue'
                ))
                
                fig.add_trace(go.Bar(
                    name='Scenario 2',
                    x=buyers_for_chart,
                    y=scenario2_values,
                    marker_color='darkblue'
                ))
                
                fig.update_layout(
                    title='Revenue Comparison by Buyer',
                    xaxis_title='Buyer',
                    yaxis_title='Revenue (RM)',
                    barmode='group'
                )
                
                st.plotly_chart(fig, use_container_width=True)

# Main app function - Enhanced with revenue tab
def main_app():
    st.title(f"🌷 Bunga di Kebun - Welcome, {st.session_state.username}!")
    
    # Display storage mode
    storage_color = "🟢" if "Firebase" in st.session_state.storage_mode else "🟡"
    st.caption(f"{storage_color} Storage mode: {st.session_state.storage_mode}")
    
    # Create tabs for different functions
    tab1, tab2, tab3 = st.tabs(["Data Entry", "Data Analysis", "Revenue Calculation"])
    
    # Tab 1: Data Entry (existing code)
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
                    font-
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
        message["Subject"] = f"Total Bunga {date_formatted}: {total_bunga:,} bunga, {total_bakul} bakul + CSV Backup"
        
        # FIXED: Format farm details with proper HTML line breaks
        farm_info = f"""A: Kebun Sendiri: {farm_data['A: Kebun Sendiri']:,} Bunga<br>
B: Kebun DeYe&nbsp;&nbsp;&nbsp;: {farm_data['B: Kebun DeYe']:,} Bunga<br>
C: Kebun Asan&nbsp;&nbsp;&nbsp;: {farm_data['C: Kebun Asan']:,} Bunga<br>
D: Kebun Uncle&nbsp;&nbsp;: {farm_data['D: Kebun Uncle']:,} Bunga"""
        
        # Get Malaysia time
        malaysia_tz = timezone(timedelta(hours=8))
        malaysia_time = datetime.now(malaysia_tz).strftime('%Y-%m-%d %H:%M:%S')
        
        # HTML email format (to support bold and red color)
        html_body = f"""<!DOCTYPE html>
<html>
<head>
    <meta charset="UTF-8">
    <style>
        body {{ font-family: Arial, sans-serif; line-height: 1.6; }}
        .highlight {{ color: #FF0000; font-weight: bold; }}
        .farm-details {{ 
            font-family: Courier New, monospace; 
            background-color: #f5f5f5;
            padding: 10px;
            border-radius: 5px;
            margin: 10px 0;
        }}
    </style>
</head>
<body>
<p>New flower data has been added to Bunga di Kebun system.</p>

<p class="highlight">Date: {date_formatted} ({day_name})</p>

<p class="highlight">Total bunga: {total_bunga:,}</p>

<p class="highlight">Total bakul: {total_bakul}</p>

<p><strong>Farm Details:</strong></p>

<div class="farm-details">
{farm_info}
</div>

<p><strong>System Information:</strong></p>

<p>Password retrieved from: {password_source}</p>

<p>Timestamp: {malaysia_time} (Malaysia Time)</p>

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
            filename = f"bunga_backup_{username}_{date_formatted}.csv"
            csv_attachment.add_header(
                'Content-Disposition',
                f'attachment; filename="{filename}"'
            )
            
            # Attach the CSV to the email
            message.attach(csv_attachment)
        
        # Send email
        with smtplib.SMTP_SSL("smtp.gmail.com", 465) as server:
            server.login(sender_email, password)
            server.send_message(message)
        
        st.success(f"Email sent with formatted CSV backup! {backup_status}")
        return True, ""
        
    except Exception as e:
        error_message = str(e)
        st.error(f"Email error: {error_message}")
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
