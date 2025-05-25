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
import hashlib
import json
import os
import firebase_admin
from firebase_admin import credentials, firestore
import uuid

# Set page config
st.set_page_config(
    page_title="Bunga di Kebun",
    page_icon="🌷",
    layout="wide"
)

# Define farm names and columns
FARM_COLUMNS = ['A: Kebun Sendiri', 'B: Kebun DeYe', 'C: Kebun Asan', 'D: Kebun Uncle']
OLD_FARM_COLUMNS = ['Farm A', 'Farm B', 'Farm C', 'Farm D']

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

# REPLACE your current save_data() function with this fixed version:

def save_data(df, username):
    farm_data = get_farm_data_collection()
    if farm_data:
        # Firebase storage - FIXED VERSION
        try:
            # Instead of deleting ALL user data, we'll update/insert individual records
            
            # Get existing records for comparison
            existing_docs = farm_data.where("username", "==", username).get()
            existing_dates = {}
            
            # Build a map of existing dates and their document IDs
            for doc in existing_docs:
                doc_data = doc.to_dict()
                if 'Date' in doc_data:
                    doc_date = pd.to_datetime(doc_data['Date']).date()
                    existing_dates[doc_date] = doc.id
            
            # Process each record in the new DataFrame
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
                
                # Ensure all values are JSON serializable
                for key, value in record.items():
                    if pd.isna(value):
                        record[key] = 0
                    elif isinstance(value, (np.integer, np.floating)):
                        record[key] = int(value) if isinstance(value, np.integer) else float(value)
                
                # Check if this date already exists
                if record_date in existing_dates:
                    # UPDATE existing record instead of deleting
                    doc_id = existing_dates[record_date]
                    farm_data.document(doc_id).set(record)
                    print(f"Updated existing record for {record_date}")
                else:
                    # ADD new record
                    farm_data.add(record)
                    print(f"Added new record for {record_date}")
            
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

def send_email_notification_with_csv_backup(date, farm_data, username):
    """
    Send email notification with CSV backup attachment
    This protects your data by including a full backup with every entry
    """
    try:
        # Email settings
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
        
        # Create message
        message = MIMEMultipart('mixed')
        message["From"] = sender_email
        message["To"] = receiver_email
        message["Subject"] = f"Total Bunga {date_formatted}: {total_bunga:,} bunga, {total_bakul} bakul + CSV Backup"
        
        # Create the email body
        farm_info = ""
        max_farm_name_length = max(len(farm) for farm in farm_data.keys())
        
        for farm, value in farm_data.items():
            padded_name = farm.ljust(max_farm_name_length)
            farm_info += f"{padded_name}: {value:,} Bunga\n"
        
        # Get Malaysia time
        malaysia_tz = timezone(timedelta(hours=8))
        malaysia_time = datetime.now(malaysia_tz).strftime('%Y-%m-%d %H:%M:%S')
        
        # HTML email body
        html_body = f"""
        <html>
        <head>
            <style>
                body {{ font-family: Arial, sans-serif; line-height: 1.6; }}
                .important {{ color: #FF0000; font-weight: bold; }}
                .farm-details {{ font-family: Courier New, monospace; margin: 15px 0; }}
                .footer {{ color: #666; font-size: 12px; margin-top: 30px; }}
                .system-info {{ background-color: #f5f5f5; padding: 10px; border-radius: 5px; margin: 20px 0; }}
                .backup-notice {{ background-color: #e8f5e8; padding: 15px; border-radius: 5px; margin: 15px 0; border-left: 4px solid #4CAF50; }}
            </style>
        </head>
        <body>
            <p>New flower data has been added to Bunga di Kebun system.</p>
            
            <p class="important">Date: {date_formatted} ({day_name})</p>
            <p class="important">Total bunga: {total_bunga:,}</p>
            <p class="important">Total bakul: {total_bakul}</p>
            
            <div class="farm-details">
                <p><strong>Farm Details:</strong></p>
                <pre>{farm_info}</pre>
            </div>
            
            <div class="backup-notice">
                <p><strong>🛡️ DATA BACKUP INCLUDED</strong></p>
                <p>This email includes a CSV backup of all your data as of {date_formatted}.</p>
                <p>The attachment contains your complete dataset for user: {username}</p>
                <p><strong>File:</strong> bunga_backup_{username}_{date_formatted}.csv</p>
            </div>
            
            <div class="system-info">
                <p><strong>System Information:</strong></p>
                <p>Password retrieved from: {password_source}</p>
                <p>Timestamp: {malaysia_time} (Malaysia Time)</p>
                <p>Backup created for user: {username}</p>
            </div>
            
            <p class="footer">This is an automated notification from Bunga di Kebun System with data backup.</p>
        </body>
        </html>
        """
        
        # Plain text version
        text_body = f"""
        Total Bunga {date_formatted}: {total_bunga:,} bunga, {total_bakul} bakul
        
        Date: {date_formatted} ({day_name})
        Total bunga: {total_bunga:,}
        Total bakul: {total_bakul}
        
        Farm Details:
        {farm_info}
        
        🛡️ DATA BACKUP INCLUDED
        This email includes a CSV backup of all your data as of {date_formatted}.
        File: bunga_backup_{username}_{date_formatted}.csv
        
        -----------------------------
        System Information:
        Password retrieved from: {password_source}
        Timestamp: {malaysia_time} (Malaysia Time)
        Backup created for user: {username}
        -----------------------------
        
        This is an automated notification from Bunga di Kebun System with data backup.
        """
        
        # Attach both versions
        message.attach(MIMEText(text_body, "plain"))
        message.attach(MIMEText(html_body, "html"))
        
        # CREATE CSV BACKUP
        try:
            # Load the current user's complete dataset
            current_data = load_data(username)
            
            if not current_data.empty:
                # Create CSV in memory
                csv_buffer = io.StringIO()
                current_data.to_csv(csv_buffer, index=False)
                csv_content = csv_buffer.getvalue()
                
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
                
                backup_status = f"✅ CSV backup created: {len(current_data)} records"
            else:
                backup_status = "⚠️ No data available for backup"
                
        except Exception as backup_error:
            backup_status = f"❌ Backup failed: {str(backup_error)}"
        
        # Send email
        with smtplib.SMTP_SSL("smtp.gmail.com", 465) as server:
            server.login(sender_email, password)
            server.send_message(message)
        
        st.success(f"Email sent with CSV backup! {backup_status}")
        return True, ""
        
    except Exception as e:
        error_message = str(e)
        st.error(f"Email error: {error_message}")
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
        
            # Try to send email
            success, error_message = send_email_notification_with_csv_backup(date, farm_data, st.session_state.username)
            if success:
                st.success("Data added and notification email sent!")
            else:
                if "Email password not found" in error_message:
                    st.warning(f"Data added but email notification could not be sent: {error_message}")
                else:
                    st.warning(f"Data added but failed to send notification: {error_message}")
                    
            return "success", None
        else:
            # If save fails, revert the change
            st.session_state.current_user_data = load_data(st.session_state.username)
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
                        st.session_state.logged_in = True
                        st.session_state.username = username
                        st.session_state.role = role
                        st.session_state.current_user_data = load_data(username)
                        st.success(f"Welcome back, {username}!")
                        st.session_state.needs_rerun = True
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

# Main app function - Keeping existing logic but with better error handling
def main_app():
    st.title(f"🌷 Bunga di Kebun - Welcome, {st.session_state.username}!")
    
    # Display storage mode
    storage_color = "🟢" if "Firebase" in st.session_state.storage_mode else "🟡"
    st.caption(f"{storage_color} Storage mode: {st.session_state.storage_mode}")
    
    # Create tabs for different functions
    tab1, tab2 = st.tabs(["Data Entry", "Data Analysis"])
    
    # Tab 1: Data Entry
    with tab1:
        st.header("Add New Data")
        
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
            st.markdown(f"""
            <div class="date-info">
                <b>Date:</b> <span class="red-data">{date_formatted} ({day_name})</span>
            </div>
            """, unsafe_allow_html=True)
            
            # Farm details section
            st.markdown("<b>Farm Details:</b>", unsafe_allow_html=True)
            
            # Display each farm on its own line
            for farm, value in farm_data.items():
                # Shorten farm name to save space
                short_name = farm.split(":")[0] + ":" + farm.split(":")[1].replace("Kebun ", "")
                st.markdown(f"<div class='farm-row'>{short_name} {format_number(value)}</div>", unsafe_allow_html=True)
            
            # Total Bunga and Total Bakul in blue
            st.markdown(f"""
            <div class="stats-item">
                <b>Total Bunga:</b> <span class="blue-data">{format_number(total_bunga)}</span>
            </div>
            <div class="stats-item">
                <b>Total Bakul:</b> <span class="blue-data">{format_number(total_bakul)}</span>
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
                        st.success(f"Data for {date_formatted} added successfully!")
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
                    farm_1 = st.number_input(f"{FARM_COLUMNS[0]} (Bunga)", min_value=0, value=0, step=1)
                
                with col2:
                    farm_2 = st.number_input(f"{FARM_COLUMNS[1]} (Bunga)", min_value=0, value=0, step=1)
                    
                with col3:
                    farm_3 = st.number_input(f"{FARM_COLUMNS[2]} (Bunga)", min_value=0, value=0, step=1)
                    
                with col4:
                    farm_4 = st.number_input(f"{FARM_COLUMNS[3]} (Bunga)", min_value=0, value=0, step=1)
                
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
                file_name=f"{st.session_state.username}_bunga_data_export.csv",
                mime="text/csv"
            )
        else:
            st.info("No data available. Add data using the form above.")
    
    # Tab 2: Data Analysis - Keeping existing implementation
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
                st.markdown(f"""
                <div style="background-color: #ffeeee; padding: 15px; border-radius: 5px; margin-bottom: 20px;">
                    <h1 style="color: #ff0000; font-weight: bold; font-size: 2.5em; text-align: center;">
                        Total Bunga: {format_number(total_bunga)}
                    </h1>
                </div>
                """, unsafe_allow_html=True)
                
                st.markdown(f"""
                <div style="background-color: #eeeeff; padding: 15px; border-radius: 5px; margin-bottom: 20px;">
                    <h1 style="color: #0000ff; font-weight: bold; font-size: 2.5em; text-align: center;">
                        Total Bakul: {format_number(total_bakul)}
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
                    st.markdown(f"<h4 style='color: {bunga_color};'>Total Bunga</h4>", unsafe_allow_html=True)
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
                    st.markdown(f"<h4 style='color: {bakul_color};'>Total Bakul</h4>", unsafe_allow_html=True)
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
                    st.markdown(f"<h4 style='color: {bunga_color};'>Daily Bunga</h4>", unsafe_allow_html=True)
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
                    st.markdown(f"<h4 style='color: {bakul_color};'>Daily Bakul</h4>", unsafe_allow_html=True)
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
                    file_name=f"{st.session_state.username}_bunga_data_{start_date}_to_{end_date}.csv",
                    mime="text/csv"
                )
            else:
                st.info("Please select both start and end dates.")

# Sidebar options - Keeping existing implementation with minor fixes
def sidebar_options():
    st.sidebar.header(f"User: {st.session_state.username}")
    
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
            
            st.sidebar.text(f"Current values for {selected_date}:")
            current_row = st.session_state.current_user_data.iloc[date_idx]
            
            formatted_values = []
            for col in FARM_COLUMNS:
                formatted_values.append(f"{col}: {format_number(current_row[col])}")
            
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
                            st.sidebar.success(f"Record for {selected_date} updated!")
                            st.session_state.needs_rerun = True
            
            # Delete option
            delete_key = f"delete_{selected_date}"
            if delete_key not in st.session_state:
                st.session_state[delete_key] = False
            
            if st.sidebar.button(f"Delete record for {selected_date}"):
                st.session_state[delete_key] = True
            
            if st.session_state[delete_key]:
                confirm = st.sidebar.checkbox("I confirm I want to delete this record", key=f"confirm_{selected_date}")
                if confirm:
                    # Drop the row
                    st.session_state.current_user_data = st.session_state.current_user_data.drop(date_idx).reset_index(drop=True)
                    
                    # Save to database
                    if save_data(st.session_state.current_user_data, st.session_state.username):
                        st.sidebar.success(f"Record for {selected_date} deleted!")
                        st.session_state.needs_rerun = True
                        st.session_state[delete_key] = False
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
                st.sidebar.error(f"CSV must contain columns: {required_cols_str}")
        except Exception as e:
            st.sidebar.error(f"Error importing data: {e}")

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
    st.sidebar.info(f"{storage_color} Data Storage Mode: {st.session_state.storage_mode}")
    
    if st.session_state.storage_mode == "Session State":
        st.sidebar.warning("Data is stored in browser session only. For permanent storage, download your data regularly.")

    # Footer
    st.sidebar.markdown("---")
    st.sidebar.markdown("🌷 Bunga di Kebun - Firebase Storage v1.0")
    st.sidebar.text(f"User: {st.session_state.username} ({st.session_state.role})")

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
            st.error(f"Firebase connection test failed: {e}")
    
    st.session_state.storage_mode = "Session State"

# Main application logic
if st.session_state.storage_mode == "Checking...":
    check_storage_mode()

if not st.session_state.logged_in:
    login_page()
else:
    main_app()
    sidebar_options()

# Trigger rerun if needed
if st.session_state.needs_rerun:
    st.session_state.needs_rerun = False
    st.rerun()
