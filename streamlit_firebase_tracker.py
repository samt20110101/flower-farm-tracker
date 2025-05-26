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
    """Parse various date string formats into a datetime object."""
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
        "january": 1, "jan": 1, "february": 2, "feb": 2, "march": 3, "mar": 3,
        "april": 4, "apr": 4, "may": 5, "june": 6, "jun": 6,
        "july": 7, "jul": 7, "august": 8, "aug": 8, "september": 9, "sep": 9,
        "october": 10, "oct": 10, "november": 11, "nov": 11, "december": 12, "dec": 12
    }
    
    # Try month name formats
    for month_name, month_num in month_map.items():
        pattern_1 = rf'\b{month_name}\s+(\d{{1,2}})(?:st|nd|rd|th)?\b'
        match = re.search(pattern_1, date_str.lower())
        if match:
            day = int(match.group(1))
            return datetime(current_year, month_num, day)
        
        pattern_2 = rf'\b(\d{{1,2}})(?:st|nd|rd|th)?\s+{month_name}\b'
        match = re.search(pattern_2, date_str.lower())
        if match:
            day = int(match.group(1))
            return datetime(current_year, month_num, day)
    
    # Try standard date formats
    formats = ['%Y-%m-%d', '%d/%m/%Y', '%m/%d/%Y', '%d-%m-%Y', '%m-%d-%Y',
               '%d/%m/%y', '%m/%d/%y', '%d-%m-%y', '%m-%d-%y']
    
    for fmt in formats:
        try:
            return datetime.strptime(date_str, fmt)
        except ValueError:
            continue
    
    return None

# Firebase connection
def connect_to_firebase():
    try:
        if not firebase_admin._apps:
            if 'firebase_credentials' in st.secrets:
                firebase_secrets = dict(st.secrets["firebase_credentials"])
                if 'private_key' in firebase_secrets:
                    firebase_secrets['private_key'] = firebase_secrets['private_key'].replace('\\n', '\n')
                
                cred = credentials.Certificate(firebase_secrets)
                firebase_admin.initialize_app(cred)
                
                db = firestore.client()
                test_collection = db.collection('test')
                test_collection.limit(1).get()
                return db
            else:
                st.error("Firebase credentials not found in secrets")
                initialize_session_storage()
                return None
        else:
            return firestore.client()
    except Exception as e:
        st.error(f"Firebase connection error: {str(e)}")
        st.error("Falling back to session storage...")
        initialize_session_storage()
        return None

def initialize_session_storage():
    if 'users' not in st.session_state:
        st.session_state.users = {
            "admin": {"password": hashlib.sha256("admin".encode()).hexdigest(), "role": "admin"}
        }
    if 'farm_data' not in st.session_state:
        st.session_state.farm_data = {}

def get_users_collection():
    db = connect_to_firebase()
    if db:
        try:
            users = db.collection('users')
            try:
                users.limit(1).get()
            except:
                pass
            return users
        except Exception as e:
            st.error(f"Error accessing users collection: {e}")
            return None
    return None

def get_farm_data_collection():
    db = connect_to_firebase()
    if db:
        try:
            farm_data = db.collection('farm_data')
            try:
                farm_data.limit(1).get()
            except:
                pass
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

def hash_password(password):
    return hashlib.sha256(password.encode()).hexdigest()

def add_user(username, password, role="user"):
    users = get_users_collection()
    if users:
        try:
            user_docs = users.where("username", "==", username).limit(1).get()
            if len(list(user_docs)) > 0:
                return False
            
            user_data = {
                "username": username,
                "password": hash_password(password),
                "role": role,
                "created_at": firestore.SERVER_TIMESTAMP
            }
            users.document(username).set(user_data)
            return True
        except Exception as e:
            st.error(f"Error adding user to Firebase: {e}")
            pass
    
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
        try:
            user_doc = users.document(username).get()
            if user_doc.exists:
                user_data = user_doc.to_dict()
                if user_data and user_data["password"] == hash_password(password):
                    return user_data["role"]
            return None
        except Exception as e:
            st.error(f"Error verifying user from Firebase: {e}")
            pass
    
    if 'users' not in st.session_state:
        initialize_session_storage()
    
    if username in st.session_state.users and st.session_state.users[username]["password"] == hash_password(password):
        return st.session_state.users[username]["role"]
    return None

def load_data(username):
    farm_data = get_farm_data_collection()
    if farm_data:
        try:
            user_data_docs = farm_data.where("username", "==", username).get()
            
            if not user_data_docs:
                return pd.DataFrame(columns=['Date'] + FARM_COLUMNS)
            
            records = []
            for doc in user_data_docs:
                doc_data = doc.to_dict()
                if doc_data:
                    records.append(doc_data)
            
            if not records:
                return pd.DataFrame(columns=['Date'] + FARM_COLUMNS)
            
            df = pd.DataFrame(records)
            
            if 'document_id' in df.columns:
                df = df.drop('document_id', axis=1)
            if 'username' in df.columns:
                df = df.drop('username', axis=1)
            if 'Date' in df.columns:
                df['Date'] = pd.to_datetime(df['Date'])
            
            for old_col, new_col in zip(OLD_FARM_COLUMNS, FARM_COLUMNS):
                if old_col in df.columns and new_col not in df.columns:
                    df[new_col] = df[old_col]
                    df = df.drop(old_col, axis=1)
            
            for col in FARM_COLUMNS:
                if col not in df.columns:
                    df[col] = 0
            
            return df
        except Exception as e:
            st.error(f"Error loading data from Firebase: {e}")
            pass
    
    if 'farm_data' not in st.session_state:
        initialize_session_storage()
    
    if username in st.session_state.farm_data:
        df = pd.DataFrame(st.session_state.farm_data[username])
        
        for old_col, new_col in zip(OLD_FARM_COLUMNS, FARM_COLUMNS):
            if old_col in df.columns and new_col not in df.columns:
                df[new_col] = df[old_col]
                df = df.drop(old_col, axis=1)
        
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
    
    if 'revenue_transactions' not in st.session_state:
        st.session_state.revenue_transactions = []
    
    return [t for t in st.session_state.revenue_transactions if t.get('username') == username]

def save_revenue_data(transactions, username):
    """Save revenue transaction data"""
    revenue_data = get_revenue_data_collection()
    if revenue_data:
        try:
            existing_docs = revenue_data.where("username", "==", username).get()
            for doc in existing_docs:
                doc.reference.delete()
            
            for transaction in transactions:
                transaction['username'] = username
                revenue_data.add(transaction)
            return True
        except Exception as e:
            st.error(f"Error saving revenue data to Firebase: {e}")
            pass
    
    if 'revenue_transactions' not in st.session_state:
        st.session_state.revenue_transactions = []
    
    st.session_state.revenue_transactions = [
        t for t in st.session_state.revenue_transactions if t.get('username') != username
    ]
    
    for transaction in transactions:
        transaction['username'] = username
        st.session_state.revenue_transactions.append(transaction)
    
    return True

def save_data(df, username):
    farm_data = get_farm_data_collection()
    if farm_data:
        try:
            existing_docs = farm_data.where("username", "==", username).get()
            existing_dates = {}
            
            for doc in existing_docs:
                doc_data = doc.to_dict()
                if 'Date' in doc_data:
                    doc_date = pd.to_datetime(doc_data['Date']).date()
                    existing_dates[doc_date] = doc.id
            
            current_dates = set()
            records = df.to_dict('records')
            
            for record in records:
                record['username'] = username
                
                if 'Date' in record:
                    if isinstance(record['Date'], pd.Timestamp):
                        record_date = record['Date'].date()
                        record['Date'] = record['Date'].isoformat()
                    else:
                        record_date = pd.to_datetime(record['Date']).date()
                    
                    current_dates.add(record_date)
                
                for key, value in record.items():
                    if pd.isna(value):
                        record[key] = 0
                    elif isinstance(value, (np.integer, np.floating)):
                        record[key] = int(value) if isinstance(value, np.integer) else float(value)
                
                if record_date in existing_dates:
                    doc_id = existing_dates[record_date]
                    farm_data.document(doc_id).set(record)
                else:
                    farm_data.add(record)
            
            dates_to_delete = set(existing_dates.keys()) - current_dates
            
            if dates_to_delete:
                for date_to_delete in dates_to_delete:
                    if date_to_delete in existing_dates:
                        doc_id = existing_dates[date_to_delete]
                        farm_data.document(doc_id).delete()
            
            return True
        except Exception as e:
            st.error(f"Error saving data to Firebase: {e}")
            pass
    
    if 'farm_data' not in st.session_state:
        initialize_session_storage()
    
    st.session_state.farm_data[username] = df.to_dict('records')
    return True

def create_formatted_csv_backup(username):
    """Create a properly formatted CSV backup"""
    try:
        current_data = st.session_state.current_user_data.copy()
        
        if current_data.empty:
            return None, "No data available for backup"
        
        formatted_data = current_data.copy()
        formatted_data['Date'] = pd.to_datetime(formatted_data['Date'])
        formatted_data = formatted_data.sort_values('Date', ascending=False).reset_index(drop=True)
        
        formatted_data['Total Bunga'] = (
            formatted_data['A: Kebun Sendiri'] + 
            formatted_data['B: Kebun DeYe'] + 
            formatted_data['C: Kebun Asan'] + 
            formatted_data['D: Kebun Uncle']
        )
        formatted_data['Total Bakul'] = (formatted_data['Total Bunga'] / 40).round().astype(int)
        
        final_columns = ['Date', 'A: Kebun Sendiri', 'B: Kebun DeYe', 'C: Kebun Asan', 'D: Kebun Uncle', 'Total Bunga', 'Total Bakul']
        formatted_data = formatted_data[final_columns]
        formatted_data['Date'] = formatted_data['Date'].dt.strftime('%Y-%m-%d')
        
        numeric_columns = ['A: Kebun Sendiri', 'B: Kebun DeYe', 'C: Kebun Asan', 'D: Kebun Uncle', 'Total Bunga', 'Total Bakul']
        for col in numeric_columns:
            formatted_data[col] = formatted_data[col].apply(lambda x: f"{int(x):,}")
        
        csv_buffer = io.StringIO()
        formatted_data.to_csv(csv_buffer, index=False)
        csv_content = csv_buffer.getvalue()
        
        return csv_content, f"✅ CSV backup created: {len(formatted_data)} records"
        
    except Exception as e:
        return None, f"❌ Backup failed: {str(e)}"

def send_email_notification_with_csv_backup(date, farm_data, username):
    """Send email notification with CSV backup attachment"""
    try:
        sender_email = "hqtong2013@gmail.com"
        receiver_email = "powchooyeo88@gmail.com"
        
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
        
        total_bunga = sum(farm_data.values())
        total_bakul = int(total_bunga / 40)
        
        if isinstance(date, str):
            try:
                date_obj = datetime.strptime(date, '%Y-%m-%d')
            except:
                date_obj = datetime.strptime(str(date), '%Y-%m-%d')
        else:
            date_obj = date
            
        day_name = date_obj.strftime('%A')
        date_formatted = date_obj.strftime('%Y-%m-%d')
        
        message = MIMEMultipart('mixed')
        message["From"] = sender_email
        message["To"] = receiver_email
        message["Subject"] = f"Total Bunga {date_formatted}: {total_bunga:,} bunga, {total_bakul} bakul + CSV Backup"
        
        farm_info = f"""A: Kebun Sendiri: {farm_data['A: Kebun Sendiri']:,} Bunga<br>
B: Kebun DeYe&nbsp;&nbsp;&nbsp;: {farm_data['B: Kebun DeYe']:,} Bunga<br>
C: Kebun Asan&nbsp;&nbsp;&nbsp;: {farm_data['C: Kebun Asan']:,} Bunga<br>
D: Kebun Uncle&nbsp;&nbsp;: {farm_data['D: Kebun Uncle']:,} Bunga"""
        
        malaysia_tz = timezone(timedelta(hours=8))
        malaysia_time = datetime.now(malaysia_tz).strftime('%Y-%m-%d %H:%M:%S')
        
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
        
        message.attach(MIMEText(html_body, "html"))
        
        csv_content, backup_status = create_formatted_csv_backup(username)
        
        if csv_content:
            csv_attachment = MIMEBase('application', 'octet-stream')
            csv_attachment.set_payload(csv_content.encode('utf-8'))
            encoders.encode_base64(csv_attachment)
            
            filename = f"bunga_backup_{username}_{date_formatted}.csv"
            csv_attachment.add_header('Content-Disposition', f'attachment; filename="{filename}"')
            message.attach(csv_attachment)
        
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
    """Send email notification WITHOUT CSV backup attachment"""
    try:
        sender_email = "hqtong2013@gmail.com"
        receiver_email = "hq_tong@hotmail.com"
        
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
        
        total_bunga = sum(farm_data.values())
        total_bakul = int(total_bunga / 40)
        
        if isinstance(date, str):
            try:
                date_obj = datetime.strptime(date, '%Y-%m-%d')
            except:
                date_obj = datetime.strptime(str(date), '%Y-%m-%d')
        else:
            date_obj = date
            
        day_name = date_obj.strftime('%A')
        date_formatted = date_obj.strftime('%Y-%m-%d')
        
        message = MIMEMultipart('mixed')
        message["From"] = sender_email
        message["To"] = receiver_email
        message["Subject"] = f"Total Bunga {date_formatted}: {total_bunga:,} bunga, {total_bakul} bakul"
        
        farm_info = f"""A: Kebun Sendiri: {farm_data['A: Kebun Sendiri']:,} Bunga<br>
B: Kebun DeYe&nbsp;&nbsp;&nbsp;: {farm_data['B: Kebun DeYe']:,} Bunga<br>
C: Kebun Asan&nbsp;&nbsp;&nbsp;: {farm_data['C: Kebun Asan']:,} Bunga<br>
D: Kebun Uncle&nbsp;&nbsp;: {farm_data['D: Kebun Uncle']:,} Bunga"""
        
        malaysia_tz = timezone(timedelta(hours=8))
        malaysia_time = datetime.now(malaysia_tz).strftime('%Y-%m-%d %H:%M:%S')
        
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
        
        message.attach(MIMEText(html_body, "html"))
        
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
    
    for i, size in enumerate(FRUIT_SIZES[:-1]):
        percentage = distribution_percentages[size]
        bakul_count = int(total_bakul * percentage / 100)
        bakul_per_size[size] = bakul_count
        remaining_bakul -= bakul_count
    
    bakul_per_size[FRUIT_SIZES[-1]] = max(0, remaining_bakul)
    return bakul_per_size

def get_bakul_from_flower_date(flower_date, username):
    """Get total bakul from a specific flower harvest date"""
    if st.session_state.current_user_data.empty:
        return 0
    
    flower_date = pd.to_datetime(flower_date).date()
    data_copy = st.session_state.current_user_data.copy()
    data_copy['Date'] = pd.to_datetime(data_copy['Date']).dt.date
    
    matching_rows = data_copy[data_copy['Date'] == flower_date]
    
    if not matching_rows.empty:
        row = matching_rows.iloc[0]
        total_bunga = sum([row[col] for col in FARM_COLUMNS if col in row])
        total_bakul = int(total_bunga / 40)
        return total_bakul
    
    return 0

def revenue_estimate_tab():
    st.header("💰 Revenue Estimate")
    
    user_transactions = load_revenue_data(st.session_state.username)
    
    price_entry_tab, scenarios_tab = st.tabs(["Price Entry", "Scenario Comparison"])
    
    with price_entry_tab:
        st.subheader("Revenue Estimation Calculator")
        
        with st.form("revenue_estimate_form"):
            # Date and Total Bakul input
            col1, col2 = st.columns(2)
            
            with col1:
                estimate_date = st.date_input("Estimate Date", datetime.now().date())
            
            with col2:
                total_bakul = st.number_input("Total Bakul", min_value=0, value=100, step=1)
            
            # Fruit Size Distribution - Combined % and Bakul display
            st.subheader("Fruit Size Distribution")
            
            # Create columns for each fruit size
            dist_cols = st.columns(len(FRUIT_SIZES) + 1)  # +1 for totals column
            
            distribution_percentages = {}
            
            # Headers
            for i, size in enumerate(FRUIT_SIZES):
                with dist_cols[i]:
                    st.write(f"**{size}**")
            with dist_cols[-1]:
                st.write("**Total**")
            
            # Percentage inputs
            st.write("**Percentage (%)**")
            perc_cols = st.columns(len(FRUIT_SIZES) + 1)
            
            for i, size in enumerate(FRUIT_SIZES):
                with perc_cols[i]:
                    distribution_percentages[size] = st.number_input(
                        f"% {size}",
                        min_value=0.0,
                        max_value=100.0,
                        value=float(DEFAULT_DISTRIBUTION[size]),
                        step=0.1,
                        key=f"dist_{size}",
                        label_visibility="collapsed"
                    )
            
            total_percentage = sum(distribution_percentages.values())
            with perc_cols[-1]:
                if total_percentage == 100:
                    st.success(f"✅ {total_percentage:.1f}%")
                else:
                    st.error(f"❌ {total_percentage:.1f}%")
            
            # Bakul distribution display
            if total_percentage == 100:
                bakul_per_size = calculate_bakul_distribution(total_bakul, distribution_percentages)
                
                st.write("**Bakul Distribution**")
                bakul_cols = st.columns(len(FRUIT_SIZES) + 1)
                
                total_bakul_display = 0
                for i, size in enumerate(FRUIT_SIZES):
                    with bakul_cols[i]:
                        st.info(f"{bakul_per_size[size]} bakul")
                        total_bakul_display += bakul_per_size[size]
                
                with bakul_cols[-1]:
                    st.info(f"**{total_bakul_display} bakul**")
            
            # Buyer Pricing Section - Streamlined
            st.subheader("Buyer Pricing (RM per kg)")
            
            buyer_prices = {}
            selected_buyers = []
            
            # Store all prices first, then determine selected buyers
            for buyer in BUYERS:
                buyer_prices[buyer] = {}
                
                st.write(f"**💼 {buyer}**")
                
                # Create two-panel layout for each buyer
                buyer_cols = st.columns(2)
                
                with buyer_cols[0]:
                    # Fruit sizes column
                    st.write("**Fruit Size**")
                    for size in FRUIT_SIZES:
                        st.write(size)
                
                with buyer_cols[1]:
                    # Prices column
                    st.write("**Price (RM/kg)**")
                    for size in FRUIT_SIZES:
                        buyer_prices[buyer][size] = st.number_input(
                            f"{buyer}_{size}_price",
                            min_value=0.00,
                            value=2.50,
                            step=0.01,
                            format="%.2f",
                            key=f"price_{buyer}_{size}",
                            label_visibility="collapsed"
                        )
                
                # Include checkbox for this buyer
                include_buyer = st.checkbox(f"Include {buyer} in calculation", key=f"include_{buyer}")
                if include_buyer:
                    selected_buyers.append(buyer)
                
                st.markdown("---")
            
            # Calculate and display results
            if total_percentage == 100 and selected_buyers:
                st.subheader("Revenue Estimate Results")
                
                # Calculate revenue (1 bakul = 15kg)
                total_revenue = 0
                results_data = []
                
                for buyer in selected_buyers:
                    buyer_revenue = 0
                    for size in FRUIT_SIZES:
                        bakul_count = bakul_per_size[size]
                        kg_total = bakul_count * 15  # 1 bakul = 15kg
                        price_per_kg = buyer_prices[buyer][size]
                        revenue = kg_total * price_per_kg
                        buyer_revenue += revenue
                    
                    total_revenue += buyer_revenue
                    results_data.append({
                        'Buyer': buyer,
                        'Revenue (RM)': f"{buyer_revenue:,.2f}"
                    })
                
                # Display results table
                results_df = pd.DataFrame(results_data)
                st.dataframe(results_df, use_container_width=True, hide_index=True)
                
                # Total revenue display
                st.markdown(f"""
                <div style="background-color: #e6ffe6; padding: 15px; border-radius: 5px; margin: 10px 0;">
                    <h2 style="color: #006600; text-align: center; margin: 0;">
                        Total Estimated Revenue: RM {total_revenue:,.2f}
                    </h2>
                </div>
                """, unsafe_allow_html=True)
            
            # Submit button
            submitted = st.form_submit_button("Save Estimate", disabled=(total_percentage != 100 or not selected_buyers))
            
            if submitted and total_percentage == 100 and selected_buyers:
                estimate = {
                    'id': str(uuid.uuid4()),
                    'date': estimate_date.isoformat(),
                    'total_bakul': total_bakul,
                    'distribution_percentages': distribution_percentages,
                    'bakul_per_size': bakul_per_size,
                    'selected_buyers': selected_buyers,
                    'buyer_prices': buyer_prices,
                    'total_revenue': total_revenue,
                    'created_at': datetime.now().isoformat()
                }
                
                user_transactions.append(estimate)
                
                if save_revenue_data(user_transactions, st.session_state.username):
                    st.success("Revenue estimate saved successfully!")
                    st.rerun()
                else:
                    st.error("Failed to save estimate")
            elif submitted and not selected_buyers:
                st.error("Please select at least one buyer")
            elif submitted and total_percentage != 100:
                st.error("Percentage distribution must total 100%")
    
    with scenarios_tab:
        st.subheader("Scenario Comparison")
        
        if not user_transactions:
            st.info("No estimates available for scenario analysis.")
        else:
            estimate_options = [f"{t['date']} (ID: {t['id'][:8]})" for t in user_transactions]
            
            selected_estimate_idx = st.selectbox(
                "Select Base Estimate for Scenario Analysis",
                range(len(estimate_options)),
                format_func=lambda x: estimate_options[x]
            )
            
            base_estimate = user_transactions[selected_estimate_idx]
            
            st.subheader("Scenario 1: Original Estimate")
            
            col1, col2 = st.columns(2)
            
            with col1:
                st.write("**Original Pricing (RM/kg):**")
                
                for buyer in base_estimate['selected_buyers']:
                    st.write(f"**{buyer}:**")
                    for size in FRUIT_SIZES:
                        price = base_estimate['buyer_prices'][buyer][size]
                        bakul_count = base_estimate['bakul_per_size'][size]
                        kg_total = bakul_count * 15  # 1 bakul = 15kg
                        revenue = kg_total * price
                        st.write(f"  {size}: {bakul_count} bakul × 15kg × RM{price:.2f} = RM{revenue:.2f}")
            
            with col2:
                # Calculate original revenue
                total_revenue_1 = 0
                revenue_summary_1 = {}
                
                for buyer in base_estimate['selected_buyers']:
                    buyer_revenue = 0
                    for size in FRUIT_SIZES:
                        bakul_count = base_estimate['bakul_per_size'][size]
                        kg_total = bakul_count * 15
                        price = base_estimate['buyer_prices'][buyer][size]
                        revenue = kg_total * price
                        buyer_revenue += revenue
                    
                    revenue_summary_1[buyer] = buyer_revenue
                    total_revenue_1 += buyer_revenue
                
                st.write("**Scenario 1 Revenue:**")
                for buyer in base_estimate['selected_buyers']:
                    st.write(f"- {buyer}: RM {revenue_summary_1[buyer]:,.2f}")
                st.write(f"**Total: RM {total_revenue_1:,.2f}**")
            
            st.markdown("---")
            
            st.subheader("Scenario 2: Modified Pricing")
            
            st.write("Modify prices (RM/kg) for comparison:")
            
            new_prices = {}
            
            for buyer in base_estimate['selected_buyers']:
                st.write(f"**{buyer} Pricing:**")
                new_prices[buyer] = {}
                
                # Create two-panel layout for pricing modification
                price_cols = st.columns(2)
                
                with price_cols[0]:
                    st.write("**Fruit Size**")
                    for size in FRUIT_SIZES:
                        st.write(size)
                
                with price_cols[1]:
                    st.write("**Price (RM/kg)**")
                    for size in FRUIT_SIZES:
                        original_price = base_estimate['buyer_prices'][buyer][size]
                        new_prices[buyer][size] = st.number_input(
                            f"scenario2_{buyer}_{size}",
                            min_value=0.00,
                            value=original_price,
                            step=0.01,
                            format="%.2f",
                            key=f"scenario2_{buyer}_{size}",
                            label_visibility="collapsed"
                        )
                
                st.markdown("---")
            
            # Calculate scenario 2 revenue
            total_revenue_2 = 0
            revenue_summary_2 = {}
            
            for buyer in base_estimate['selected_buyers']:
                buyer_revenue = 0
                for size in FRUIT_SIZES:
                    bakul_count = base_estimate['bakul_per_size'][size]
                    kg_total = bakul_count * 15
                    price = new_prices[buyer][size]
                    revenue = kg_total * price
                    buyer_revenue += revenue
                
                revenue_summary_2[buyer] = buyer_revenue
                total_revenue_2 += buyer_revenue
            
            st.subheader("Scenario Comparison")
            
            comparison_data = []
            for buyer in base_estimate['selected_buyers']:
                comparison_data.append({
                    'Buyer': buyer,
                    'Scenario 1 (RM)': f"{revenue_summary_1[buyer]:,.2f}",
                    'Scenario 2 (RM)': f"{revenue_summary_2[buyer]:,.2f}",
                    'Difference (RM)': f"{revenue_summary_2[buyer] - revenue_summary_1[buyer]:+,.2f}",
                    'Change (%)': f"{((revenue_summary_2[buyer] - revenue_summary_1[buyer]) / revenue_summary_1[buyer] * 100):+.1f}%" if revenue_summary_1[buyer] > 0 else "0.0%"
                })
            
            # Add total row
            comparison_data.append({
                'Buyer': 'TOTAL',
                'Scenario 1 (RM)': f"{total_revenue_1:,.2f}",
                'Scenario 2 (RM)': f"{total_revenue_2:,.2f}",
                'Difference (RM)': f"{total_revenue_2 - total_revenue_1:+,.2f}",
                'Change (%)': f"{((total_revenue_2 - total_revenue_1) / total_revenue_1 * 100):+.1f}%" if total_revenue_1 > 0 else "0.0%"
            })
            
            comparison_df = pd.DataFrame(comparison_data)
            st.dataframe(comparison_df, use_container_width=True, hide_index=True)
            
            # Visualization
            fig = go.Figure()
            
            buyers_for_chart = [row['Buyer'] for row in comparison_data[:-1]]  # Exclude total
            scenario1_values = [revenue_summary_1[buyer] for buyer in base_estimate['selected_buyers']]
            scenario2_values = [revenue_summary_2[buyer] for buyer in base_estimate['selected_buyers']]
            
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

def add_data(date, farm_1, farm_2, farm_3, farm_4, confirmed=False):
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
        new_row = pd.DataFrame({
            'Date': [pd.Timestamp(date)],
            FARM_COLUMNS[0]: [int(farm_1)],
            FARM_COLUMNS[1]: [int(farm_2)],
            FARM_COLUMNS[2]: [int(farm_3)],
            FARM_COLUMNS[3]: [int(farm_4)]
        })
        
        if not st.session_state.current_user_data.empty:
            existing_dates = pd.to_datetime(st.session_state.current_user_data['Date']).dt.date
            new_date = pd.Timestamp(date).date()
            if new_date in existing_dates.values:
                st.error(f"Data for {date} already exists. Please edit the existing entry or choose a different date.")
                return "error", None
        
        st.session_state.current_user_data = pd.concat([st.session_state.current_user_data, new_row], ignore_index=True)
        st.session_state.current_user_data = st.session_state.current_user_data.sort_values(by='Date').reset_index(drop=True)
        
        if save_data(st.session_state.current_user_data, st.session_state.username):
            st.session_state.needs_rerun = True
            farm_data = {
                FARM_COLUMNS[0]: farm_1,
                FARM_COLUMNS[1]: farm_2,
                FARM_COLUMNS[2]: farm_3,
                FARM_COLUMNS[3]: farm_4
            }
        
            if st.session_state.csv_backup_enabled:
                success, error_message = send_email_notification_with_csv_backup(date, farm_data, st.session_state.username)
                backup_status = "with CSV backup"
            else:
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
            pass
            return "error", None
            
    except Exception as e:
        st.error(f"Error adding data: {str(e)}")
        return "error", None

def format_number(number):
    return f"{int(number):,}"

def login_page():
    st.title("🌷 Bunga di Kebun - Login")
    
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

def main_app():
    st.title(f"🌷 Bunga di Kebun - Welcome, {st.session_state.username}!")
    
    storage_color = "🟢" if "Firebase" in st.session_state.storage_mode else "🟡"
    st.caption(f"{storage_color} Storage mode: {st.session_state.storage_mode}")
    
    tab1, tab2, tab3 = st.tabs(["Data Entry", "Data Analysis", "Revenue Estimate"])
    
    with tab1:
        st.header("Add New Data")
        
        col_toggle, col_status = st.columns([1, 2])
        
        with col_toggle:
            csv_enabled = st.toggle(
                "📊 CSV Backup", 
                value=st.session_state.csv_backup_enabled,
                help="Toggle CSV backup attachment in emails"
            )
            st.session_state.csv_backup_enabled = csv_enabled
        
        with col_status:
            if st.session_state.csv_backup_enabled:
                st.success("✅ CSV backup ENABLED (slower saves)")
            else:
                st.warning("⚠️ CSV backup DISABLED (faster saves)")
        
        st.markdown("---")
        
        if 'confirm_data' not in st.session_state:
            st.session_state.confirm_data = False
            st.session_state.data_to_confirm = None
            
        if st.session_state.confirm_data and st.session_state.data_to_confirm:
            data = st.session_state.data_to_confirm
            date = data['date']
            farm_data = data['farm_data']
            
            total_bunga = sum(farm_data.values())
            total_bakul = int(total_bunga / 40)
            
            if isinstance(date, str):
                try:
                    date_obj = datetime.strptime(date, '%Y-%m-%d')
                except:
                    date_obj = datetime.strptime(str(date), '%Y-%m-%d')
            else:
                date_obj = date
            
            day_name = date_obj.strftime('%A')
            date_formatted = date_obj.strftime('%Y-%m-%d')
            
            st.warning("⚠️ Please Confirm Before Save")
            
            st.markdown(f"**Date:** {date_formatted} ({day_name})")
            st.markdown("**Farm Details:**")
            
            for farm, value in farm_data.items():
                short_name = farm.split(":")[0] + ":" + farm.split(":")[1].replace("Kebun ", "")
                st.markdown(f"- {short_name} {format_number(value)}")
            
            st.markdown(f"**Total Bunga:** {format_number(total_bunga)}")
            st.markdown(f"**Total Bakul:** {format_number(total_bakul)}")
            
            button_col1, button_col2 = st.columns(2)
            
            with button_col1:
                if st.button("✅ CONFIRM & SAVE", key="confirm_save"):
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
                        st.session_state.confirm_data = False
                        st.session_state.data_to_confirm = None
                        st.rerun()
            
            with button_col2:
                if st.button("❌ CANCEL", key="cancel_save"):
                    st.session_state.confirm_data = False
                    st.session_state.data_to_confirm = None
                    st.rerun()
        
        if not st.session_state.confirm_data:
            with st.form("data_entry_form", clear_on_submit=False):
                today = datetime.now().date()
                date = st.date_input("Select Date", today)
                
                col1, col2, col3, col4 = st.columns(4)
                
                with col1:
                    farm_1 = st.number_input(f"{FARM_COLUMNS[0]} (Bunga)", min_value=0, value=0, step=1)
                
                with col2:
                    farm_2 = st.number_input(f"{FARM_COLUMNS[1]} (Bunga)", min_value=0, value=0, step=1)
                    
                with col3:
                    farm_3 = st.number_input(f"{FARM_COLUMNS[2]} (Bunga)", min_value=0, value=0, step=1)
                    
                with col4:
                    farm_4 = st.number_input(f"{FARM_COLUMNS[3]} (Bunga)", min_value=0, value=0, step=1)
                
                submitted = st.form_submit_button("Review Data")
                
                if submitted:
                    result, data = add_data(date, farm_1, farm_2, farm_3, farm_4)
                    
                    if result == "confirm":
                        st.session_state.confirm_data = True
                        st.session_state.data_to_confirm = data
                        st.rerun()
        
        st.header("Current Data")
        
        if not st.session_state.current_user_data.empty:
            display_df = st.session_state.current_user_data.copy()
            display_df = display_df[['Date'] + FARM_COLUMNS]
            
            if 'Date' in display_df.columns:
                display_df['Date'] = pd.to_datetime(display_df['Date']).dt.date
            
            for col in FARM_COLUMNS:
                if col in display_df.columns:
                    display_df[col] = display_df[col].apply(format_number)
            
            display_df.index = display_df.index + 1
            st.dataframe(display_df, use_container_width=True)
            
            csv = st.session_state.current_user_data.to_csv(index=False).encode('utf-8')
            st.download_button(
                label="Download Data as CSV",
                data=csv,
                file_name=f"{st.session_state.username}_bunga_data_export.csv",
                mime="text/csv"
            )
        else:
            st.info("No data available. Add data using the form above.")
    
    with tab2:
        st.header("Bunga Production Analysis")
        
        if st.session_state.current_user_data.empty:
            st.info("No data available for analysis. Please add data in the Data Entry tab.")
        else:
            analysis_df = st.session_state.current_user_data.copy()
            
            analysis_cols = ['Date'] + FARM_COLUMNS
            all_cols = set(analysis_df.columns)
            for col in all_cols:
                if col not in analysis_cols:
                    analysis_df = analysis_df.drop(col, axis=1)
            
            st.subheader("Filter by Date Range")
            
            analysis_df['Date'] = pd.to_datetime(analysis_df['Date'])
            min_date = analysis_df['Date'].min().date()
            max_date = analysis_df['Date'].max().date()
            
            date_range = st.date_input(
                "Select date range",
                value=(min_date, max_date),
                min_value=min_date,
                max_value=max_date
            )
            
            if len(date_range) == 2:
                start_date, end_date = date_range
                filtered_df = analysis_df[
                    (analysis_df['Date'] >= pd.Timestamp(start_date)) & 
                    (analysis_df['Date'] <= pd.Timestamp(end_date))
                ]
                
                total_bunga = int(filtered_df[FARM_COLUMNS].sum().sum())
                total_bakul = int(total_bunga / 40)
                
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
                
                st.subheader("Filtered Data")
                
                filtered_display = filtered_df.copy()
                filtered_display['Day'] = filtered_display['Date'].dt.strftime('%A')
                filtered_display['Total Bunga'] = filtered_display[FARM_COLUMNS].sum(axis=1).astype(int)
                filtered_display['Date'] = filtered_display['Date'].dt.date
                
                filtered_display = filtered_display[['Date', 'Day', 'Total Bunga'] + FARM_COLUMNS]
                
                filtered_display['Total Bunga'] = filtered_display['Total Bunga'].apply(format_number)
                for col in FARM_COLUMNS:
                    filtered_display[col] = filtered_display[col].apply(format_number)
                
                filtered_display.index = filtered_display.index + 1
                st.dataframe(filtered_display, use_container_width=True)
                
                st.subheader("Farm Totals")
                
                summary = pd.DataFrame({
                    'Farm': FARM_COLUMNS,
                    'Total': [int(filtered_df[col].sum()) for col in FARM_COLUMNS]
                })
                
                total_row = pd.DataFrame({
                    'Farm': ['Total All Farms'],
                    'Total': [int(filtered_df[FARM_COLUMNS].sum().sum())]
                })
                summary = pd.concat([summary, total_row], ignore_index=True)
                
                summary['Total'] = summary['Total'].apply(format_number)
                summary.index = summary.index + 1
                st.dataframe(summary, use_container_width=True)
                
                st.subheader("Visualizations")
                
                farm_totals = pd.DataFrame({
                    'Farm': FARM_COLUMNS,
                    'Total Bunga': [int(filtered_df[col].sum()) for col in FARM_COLUMNS]
                })
                
                farm_totals['Total Bakul'] = (farm_totals['Total Bunga'] / 40).apply(lambda x: round(x, 1))
                
                farm_colors = px.colors.qualitative.Set3[:len(FARM_COLUMNS)]
                bunga_color = "#ff0000"
                bakul_color = "#0000ff"
                
                chart_type = st.radio("Select Chart Type", ["Bar Chart", "Pie Chart"], horizontal=True)
                
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
                        xaxis=dict(title="Date", tickformat="%Y-%m-%d"),
                        yaxis=dict(title="Total Bunga", tickformat=","),
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
                        xaxis=dict(title="Date", tickformat="%Y-%m-%d"),
                        yaxis=dict(title="Total Bakul"),
                        title={'text': "Daily Bakul Production", 'font': {'color': bakul_color}}
                    )
                    fig_daily_bakul.update_traces(
                        hovertemplate="Date: %{x|%Y-%m-%d}<br>Total: %{y:.1f} Bakul<extra></extra>",
                        marker=dict(color=bakul_color, opacity=0.8)
                    )
                    st.plotly_chart(fig_daily_bakul, use_container_width=True)
                
                csv = filtered_df.to_csv(index=False).encode('utf-8')
                st.download_button(
                    label="Download Filtered Data as CSV",
                    data=csv,
                    file_name=f"{st.session_state.username}_bunga_data_{start_date}_to_{end_date}.csv",
                    mime="text/csv"
                )
            else:
                st.info("Please select both start and end dates.")
    
    with tab3:
        revenue_estimate_tab()

def sidebar_options():
    st.sidebar.header(f"User: {st.session_state.username}")
    
    if st.sidebar.button("Logout"):
        st.session_state.logged_in = False
        st.session_state.username = ""
        st.session_state.role = ""
        st.session_state.current_user_data = pd.DataFrame(columns=['Date'] + FARM_COLUMNS)
        st.session_state.needs_rerun = True
        return

    st.sidebar.header("Data Management")

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
                        for i, col in enumerate(FARM_COLUMNS):
                            st.session_state.current_user_data.at[date_idx, col] = edit_values[i]
                        
                        if save_data(st.session_state.current_user_data, st.session_state.username):
                            st.sidebar.success(f"Record for {selected_date} updated!")
                            st.session_state.needs_rerun = True
            
            delete_key = f"delete_{selected_date}"
            if delete_key not in st.session_state:
                st.session_state[delete_key] = False
            
            if st.sidebar.button(f"Delete record for {selected_date}"):
                st.session_state[delete_key] = True
            
            if st.session_state[delete_key]:
                confirm = st.sidebar.checkbox("I confirm I want to delete this record", key=f"confirm_{selected_date}")
                if confirm:
                    st.session_state.current_user_data = st.session_state.current_user_data.drop(date_idx).reset_index(drop=True)
                    
                    if save_data(st.session_state.current_user_data, st.session_state.username):
                        st.sidebar.success(f"Record for {selected_date} deleted!")
                        st.session_state.needs_rerun = True
                        st.session_state[delete_key] = False
                        st.rerun()
                else:
                    if st.sidebar.button("Cancel deletion"):
                        st.session_state[delete_key] = False

    st.sidebar.subheader("Import Data")
    uploaded_file = st.sidebar.file_uploader("Upload existing data (CSV)", type="csv")
    if uploaded_file is not None:
        try:
            uploaded_df = pd.read_csv(uploaded_file)
            
            required_cols = ['Date']
            has_old_cols = all(col in uploaded_df.columns for col in OLD_FARM_COLUMNS)
            has_new_cols = all(col in uploaded_df.columns for col in FARM_COLUMNS)
            
            if 'Date' in uploaded_df.columns and (has_old_cols or has_new_cols):
                uploaded_df['Date'] = pd.to_datetime(uploaded_df['Date'])
                
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

    st.sidebar.markdown("---")
    st.sidebar.subheader("Storage Information")
    storage_color = "🟢" if "Firebase" in st.session_state.storage_mode else "🟡"
    st.sidebar.info(f"{storage_color} Data Storage Mode: {st.session_state.storage_mode}")
    
    if st.session_state.storage_mode == "Session State":
        st.sidebar.warning("Data is stored in browser session only. For permanent storage, download your data regularly.")

    st.sidebar.markdown("---")
    st.sidebar.markdown("🌷 Bunga di Kebun - Enhanced v2.0")
    st.sidebar.text(f"User: {st.session_state.username} ({st.session_state.role})")

def check_storage_mode():
    db = connect_to_firebase()
    if db:
        try:
            users = db.collection('users')
            try:
                list(users.limit(1).get())
            except:
                pass
            
            st.session_state.storage_mode = "Firebase Database"
            return
        except Exception as e:
            st.error(f"Firebase connection test failed: {e}")
    
    st.session_state.storage_mode = "Session State"

def initialize_app():
    users = get_users_collection()
    if users:
        try:
            admin_doc = users.document("admin").get()
            if not admin_doc.exists:
                add_user("admin", "admin", "admin")
            return
        except Exception as e:
            st.error(f"Error initializing Firebase: {e}")
            pass
    
    initialize_session_storage()

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
