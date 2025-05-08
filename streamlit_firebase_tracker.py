import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import google.generativeai as genai
import re
from typing import List, Dict, Any, Union, Optional
from datetime import datetime, timedelta, timezone  # Add timezone here

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
# Temporary debugging for secrets


# Set page config
st.set_page_config(
    page_title="Bunga di Kebun",
    page_icon="🌷",
    layout="wide"
)
# Define farm names and columns
FARM_COLUMNS = ['Kebun Sendiri', 'Kebun DeYe', 'Kebun Uncle', 'Kebun Asan']
OLD_FARM_COLUMNS = ['Farm A', 'Farm B', 'Farm C', 'Farm D']
def parse_date_string(date_str: str, current_year: int = None) -> Optional[datetime]:
    """
    Parse various date string formats into a datetime object.
    Handles formats like:
    - "May 1"
    - "1 May"
    - "01/05/2023"
    - "2023-05-01"
    - Natural language like "today", "yesterday"
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

# Firebase connection - simplified direct approach
def connect_to_firebase():
    try:
        # Check if Firebase is already initialized
        if not firebase_admin._apps:
            # Use Streamlit secrets directly - simplified
            if 'firebase_credentials' in st.secrets:
                cred = credentials.Certificate(dict(st.secrets["firebase_credentials"]))
                firebase_admin.initialize_app(cred)
                return firestore.client()
            else:
                st.error("Firebase credentials not found in secrets")
                initialize_session_storage()
                return None
        else:
            # Return existing Firestore client if Firebase is already initialized
            return firestore.client()
    except Exception as e:
        st.error(f"Firebase connection error: {e}")
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

# Firebase collection access with fallback
def get_users_collection():
    db = connect_to_firebase()
    if db:
        try:
            # Try to access the users collection
            users = db.collection('users')
            # Test by getting a document
            users.limit(1).get()
            return users
        except Exception as e:
            # If collection access fails, use session state
            return None
    return None

def get_farm_data_collection():
    db = connect_to_firebase()
    if db:
        try:
            # Try to access the farm_data collection
            farm_data = db.collection('farm_data')
            # Test by getting a document
            farm_data.limit(1).get()
            return farm_data
        except Exception as e:
            # If collection access fails, use session state
            return None
    return None

# Password hashing
def hash_password(password):
    return hashlib.sha256(password.encode()).hexdigest()

# User management with fallbacks
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
            result = users.document(username).set(user_data)
            return True
        except Exception as e:
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
            # Fallback to session state
            pass
    
    # Session state storage
    if 'users' not in st.session_state:
        initialize_session_storage()
        
    if username in st.session_state.users and st.session_state.users[username]["password"] == hash_password(password):
        return st.session_state.users[username]["role"]
    return None

# Data functions with fallbacks
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
                records.append(doc.to_dict())
            
            df = pd.DataFrame(records)
            
            # Drop Firebase document ID if present
            if 'document_id' in df.columns:
                df = df.drop('document_id', axis=1)
            
            # Drop username field for display
            if 'username' in df.columns:
                df = df.drop('username', axis=1)
            
            # Ensure Date is datetime
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

def save_data(df, username):
    farm_data = get_farm_data_collection()
    if farm_data:
        # Firebase storage
        try:
            # Delete all existing records for this user
            batch = firestore.client().batch()
            docs_to_delete = farm_data.where("username", "==", username).get()
            for doc in docs_to_delete:
                batch.delete(doc.reference)
            batch.commit()
            
            # Prepare data for Firebase
            records = df.to_dict('records')
            
            # Use batch write for better performance
            batch = firestore.client().batch()
            for record in records:
                # Add username to each record
                record['username'] = username
                
                # Convert pandas Timestamp to string for Firebase
                if 'Date' in record and isinstance(record['Date'], pd.Timestamp):
                    record['Date'] = record['Date'].isoformat()
                
                # Create a new document with auto-generated ID
                doc_ref = farm_data.document()
                batch.set(doc_ref, record)
            
            # Commit the batch
            batch.commit()
            return True
        except Exception as e:
            # Fallback to session state
            pass
    
    # Session state storage
    if 'farm_data' not in st.session_state:
        initialize_session_storage()
        
    # Store the DataFrame as a list of dictionaries
    st.session_state.farm_data[username] = df.to_dict('records')
    return True

# Initialize app
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
            # Fallback to session state
            pass
    
    # Initialize session state storage
    initialize_session_storage()
# Initialize Gemini API
def initialize_gemini():
    try:
        # Hardcoded API key for testing purposes
        api_key = "AIzaSyBCq1-Nr9jhBbaLUWz4nm_8As8kdKvKqek"
        genai.configure(api_key=api_key)
        return True
    except Exception as e:
        st.error(f"Error initializing Gemini API: {str(e)}")
        return False

# Query parsing function for the QA system
# Improved date pattern recognition in parse_query function
def parse_query(query: str) -> Dict[str, Any]:
    """Parse a natural language query about flower data into structured parameters with improved date parsing."""
    params = {
        "date_range": None,
        "farms": [],
        "query_type": "unknown",
        "original_query": query
    }
    
    # Enhanced date patterns with better handling for month names
    month_names = r'(?:january|february|march|april|may|june|july|august|september|october|november|december|jan|feb|mar|apr|jun|jul|aug|sep|oct|nov|dec)'
    
    # Date range patterns - adding specific pattern for "from 1 april to 5 may" format
    date_range_patterns = [
        # NEW PATTERN: "from [day] [month] to [day] [month]" format (crucial for "from 1 april to 5 may")
        rf'(?:from)?\s+(\d{{1,2}})(?:st|nd|rd|th)?\s+({month_names})\s+(?:to|until|and|-)\s+(\d{{1,2}})(?:st|nd|rd|th)?\s+({month_names})\b',
        
        # Month with day range: "May 1 to 4"
        rf'\b({month_names})\s+(\d{{1,2}})(?:st|nd|rd|th)?\s+(?:to|until|and|-)\s+(\d{{1,2}})(?:st|nd|rd|th)?\b',
        
        # Day range with month: "1 to 4 May" or "from 1 to 4 May"
        rf'(?:from\s+)?(\d{{1,2}})(?:st|nd|rd|th)?\s+(?:to|until|and|-)\s+(\d{{1,2}})(?:st|nd|rd|th)?\s+({month_names})\b',
        
        # Full date range with different months: "May 1 to June 4"
        rf'\b({month_names})\s+(\d{{1,2}})(?:st|nd|rd|th)?\s+(?:to|until|and|-)\s+({month_names})\s+(\d{{1,2}})(?:st|nd|rd|th)?\b',
        
        # Numeric date ranges - e.g., "01/05 to 04/05"
        r'(\d{1,2}[-/]\d{1,2}(?:[-/]\d{2,4})?)\s+(?:to|until|and|-)\s+(\d{1,2}[-/]\d{1,2}(?:[-/]\d{2,4})?)'
    ]
    
    # First check for date ranges with month
    for pattern in date_range_patterns:
        range_match = re.search(pattern, query, re.IGNORECASE)
        if range_match:
            groups = range_match.groups()
            
            # NEW HANDLING FOR "from [day] [month] to [day] [month]" format 
            if len(groups) == 4 and re.match(r'\d+', groups[0]) and re.match(month_names, groups[1], re.IGNORECASE):
                # This matches "from 1 april to 5 may" pattern
                day1, month1, day2, month2 = groups
                params["date_range"] = [f"{day1} {month1}", f"{day2} {month2}"]
                break
                
            # Different patterns need different handling
            elif len(groups) == 3:  # Either "May 1 to 4" or "1 to 4 May"
                if re.match(month_names, groups[0], re.IGNORECASE):
                    # "May 1 to 4" pattern
                    month, start_day, end_day = groups
                    params["date_range"] = [f"{month} {start_day}", f"{month} {end_day}"]
                else:
                    # "1 to 4 May" pattern
                    start_day, end_day, month = groups
                    params["date_range"] = [f"{start_day} {month}", f"{end_day} {month}"]
            elif len(groups) == 4 and re.match(month_names, groups[0], re.IGNORECASE):  
                # "May 1 to June 4" pattern
                month1, day1, month2, day2 = groups
                params["date_range"] = [f"{month1} {day1}", f"{month2} {day2}"]
            else:
                # Numeric date ranges
                params["date_range"] = list(groups)
            break
    
    # If no date range, check for single dates
    if params["date_range"] is None:
        # Single date patterns
        date_patterns = [
            # Month name with day
            rf'\b({month_names})\s+(\d{{1,2}})(?:st|nd|rd|th)?\b',
            # Day with month name
            rf'\b(\d{{1,2}})(?:st|nd|rd|th)?\s+({month_names})\b',
            # Numeric date format
            r'\b\d{1,2}[-/]\d{1,2}(?:[-/]\d{2,4})?\b'
        ]
        
        for pattern in date_patterns:
            matches = re.findall(pattern, query, re.IGNORECASE)
            if matches:
                if isinstance(matches[0], tuple):
                    # For tuple results like ("May", "1")
                    if re.match(month_names, matches[0][0], re.IGNORECASE):
                        params["date_range"] = [f"{matches[0][0]} {matches[0][1]}"]
                    else:
                        params["date_range"] = [f"{matches[0][0]} {matches[0][1]}"]
                else:
                    # For single string matches
                    params["date_range"] = [matches[0]]
                break
    
    # Additional logic for natural language date references
    if params["date_range"] is None:
        if re.search(r'\b(?:today|now|current)\b', query, re.IGNORECASE):
            params["date_range"] = ["today"]
        elif re.search(r'\byesterday\b', query, re.IGNORECASE):
            params["date_range"] = ["yesterday"]
        elif re.search(r'\blast\s+week\b', query, re.IGNORECASE):
            params["date_range"] = ["last week"]
        elif re.search(r'\bthis\s+month\b', query, re.IGNORECASE):
            params["date_range"] = ["this month"]
        elif re.search(month_names, query, re.IGNORECASE):
            # If only a month is mentioned, assume the entire month
            month_match = re.search(month_names, query, re.IGNORECASE)
            if month_match:
                month = month_match.group(0)
                params["date_range"] = [f"{month} month"]
    
    # Look for farm names
    for farm in FARM_COLUMNS:
        # Improved regex to catch more variations of farm names
        farm_pattern = re.compile(r'(?:' + re.escape(farm) + r'|' + farm.split()[1] + r')\b', re.IGNORECASE)
        if farm_pattern.search(query):
            params["farms"].append(farm)
    
    # Determine query type with improved detection
    if re.search(r'\b(?:how\s+many|total|count|sum|is\s+there)\b', query, re.IGNORECASE):
        params["query_type"] = "count"
    elif re.search(r'\b(?:average|mean|avg)\b', query, re.IGNORECASE):
        params["query_type"] = "average"
    elif re.search(r'\b(?:compare|comparison|difference|vs|versus)\b', query, re.IGNORECASE):
        params["query_type"] = "comparison"
    elif re.search(r'\b(?:highest|most|best|top|maximum|max)\b', query, re.IGNORECASE):
        params["query_type"] = "maximum"
    elif re.search(r'\b(?:lowest|least|worst|minimum|min)\b', query, re.IGNORECASE):
        params["query_type"] = "minimum"
    
    return params
def parse_date_string(date_str: str, current_year: int = None) -> Optional[datetime]:
    """
    Parse various date string formats into a datetime object.
    Handles formats like:
    - "May 1"
    - "1 May"
    - "01/05/2023"
    - "2023-05-01"
    - Natural language like "today", "yesterday"
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

def execute_query(params: Dict[str, Any], data: pd.DataFrame) -> Dict[str, Any]:
    """Execute a parsed query against the flower data with improved date filtering."""
    if data.empty:
        return {"error": "No data available in the system", "result": None}
    
    # Make a copy of the original data for reference
    original_data = data.copy()
    
    # Handle date filtering with proper date parsing
    filtered_data = data.copy()
    date_filter_applied = False
    
    # Ensure Date column is datetime
    if 'Date' in filtered_data.columns:
        filtered_data['Date'] = pd.to_datetime(filtered_data['Date'])
    
    # Add month and year columns for easier filtering
    filtered_data['Month'] = filtered_data['Date'].dt.month
    filtered_data['Year'] = filtered_data['Date'].dt.year
    filtered_data['Month_Name'] = filtered_data['Date'].dt.strftime('%B').str.lower()
    filtered_data['Day'] = filtered_data['Date'].dt.day
    
    # Debug info
    print(f"Query parameters: {params}")
    
    # Handle date filtering
    if params["date_range"]:
        try:
            current_year = datetime.now().year
            
            # Handle natural language date references
            if len(params["date_range"]) == 1:
                date_str = params["date_range"][0].lower()
                
                if date_str == "today":
                    # Get today's date
                    today = datetime.now().date()
                    filtered_data = filtered_data[filtered_data['Date'].dt.date == today]
                    date_filter_applied = True
                
                elif date_str == "yesterday":
                    # Get yesterday's date
                    yesterday = (datetime.now() - timedelta(days=1)).date()
                    filtered_data = filtered_data[filtered_data['Date'].dt.date == yesterday]
                    date_filter_applied = True
                
                elif date_str == "last week":
                    # Get date range for last week
                    today = datetime.now().date()
                    start_of_last_week = (today - timedelta(days=today.weekday() + 7))
                    end_of_last_week = start_of_last_week + timedelta(days=6)
                    filtered_data = filtered_data[
                        (filtered_data['Date'].dt.date >= start_of_last_week) & 
                        (filtered_data['Date'].dt.date <= end_of_last_week)
                    ]
                    date_filter_applied = True
                    
                elif date_str == "this month":
                    # Get date range for current month
                    today = datetime.now().date()
                    start_of_month = today.replace(day=1)
                    filtered_data = filtered_data[
                        (filtered_data['Date'].dt.date >= start_of_month) & 
                        (filtered_data['Date'].dt.date <= today)
                    ]
                    date_filter_applied = True
                
                elif "month" in date_str:
                    # Handle single month query like "May month"
                    month_match = re.search(r'(\w+)\s+month', date_str)
                    if month_match:
                        month_name = month_match.group(1).lower()
                        month_map = {
                            "january": 1, "jan": 1, "february": 2, "feb": 2,
                            "march": 3, "mar": 3, "april": 4, "apr": 4,
                            "may": 5, "june": 6, "jun": 6,
                            "july": 7, "jul": 7, "august": 8, "aug": 8,
                            "september": 9, "sep": 9, "october": 10, "oct": 10,
                            "november": 11, "nov": 11, "december": 12, "dec": 12
                        }
                        if month_name in month_map:
                            month_num = month_map[month_name]
                            filtered_data = filtered_data[filtered_data['Month'] == month_num]
                            date_filter_applied = True
                
                else:
                    # Try to parse specific date
                    parsed_date = parse_date_string(date_str, current_year)
                    if parsed_date:
                        filtered_data = filtered_data[filtered_data['Date'].dt.date == parsed_date.date()]
                        date_filter_applied = True
            
            # Handle date range
            elif len(params["date_range"]) == 2:
                start_date_str, end_date_str = params["date_range"]
                
                # Parse both dates - with extra debugging
                print(f"Parsing date range: {start_date_str} to {end_date_str}")
                
                # Add explicit debugging for date parsing
                start_date = parse_date_string(start_date_str, current_year)
                end_date = parse_date_string(end_date_str, current_year)
                
                print(f"Parsed dates: {start_date} to {end_date}")
                
                if start_date and end_date:
                    # Use the date range to filter the data
                    filtered_data = filtered_data[
                        (filtered_data['Date'].dt.date >= start_date.date()) & 
                        (filtered_data['Date'].dt.date <= end_date.date())
                    ]
                    date_filter_applied = True
                    
                    # Debug after filtering
                    print(f"Filtered data shape: {filtered_data.shape}")
                    print(f"Date range applied: {start_date.date()} to {end_date.date()}")
                else:
                    # Try alternative parsing for formats like "1 april" to "5 may"
                    month_names_pattern = r'(?:january|february|march|april|may|june|july|august|september|october|november|december|jan|feb|mar|apr|jun|jul|aug|sep|oct|nov|dec)'
                    
                    # Month-to-number mapping
                    month_map = {
                        "january": 1, "jan": 1, "february": 2, "feb": 2,
                        "march": 3, "mar": 3, "april": 4, "apr": 4,
                        "may": 5, "june": 6, "jun": 6,
                        "july": 7, "jul": 7, "august": 8, "aug": 8,
                        "september": 9, "sep": 9, "october": 10, "oct": 10,
                        "november": 11, "nov": 11, "december": 12, "dec": 12
                    }
                    
                    # Try to extract day and month from the strings
                    day1_match = re.search(r'(\d+)', start_date_str)
                    month1_match = re.search(month_names_pattern, start_date_str, re.IGNORECASE)
                    
                    day2_match = re.search(r'(\d+)', end_date_str)
                    month2_match = re.search(month_names_pattern, end_date_str, re.IGNORECASE)
                    
                    if day1_match and month1_match and day2_match and month2_match:
                        day1 = int(day1_match.group(1))
                        month1_name = month1_match.group(0).lower()
                        
                        day2 = int(day2_match.group(1))
                        month2_name = month2_match.group(0).lower()
                        
                        if month1_name in month_map and month2_name in month_map:
                            month1_num = month_map[month1_name]
                            month2_num = month_map[month2_name]
                            
                            # Create proper datetime objects
                            start_date = datetime(current_year, month1_num, day1)
                            end_date = datetime(current_year, month2_num, day2)
                            
                            # Use the date range to filter the data
                            filtered_data = filtered_data[
                                (filtered_data['Date'].dt.date >= start_date.date()) & 
                                (filtered_data['Date'].dt.date <= end_date.date())
                            ]
                            date_filter_applied = True
                            
                            # Debug after filtering
                            print(f"Alternative parsing - Filtered data shape: {filtered_data.shape}")
                            print(f"Alternative parsing - Date range applied: {start_date.date()} to {end_date.date()}")
        
        except Exception as e:
            return {"error": f"Error processing date filter: {str(e)}", "result": None}
    
    # Debug filtered data
    if date_filter_applied:
        print(f"Date filter applied, remaining rows: {len(filtered_data)}")
        if not filtered_data.empty:
            print(f"Date range in filtered data: {filtered_data['Date'].min()} to {filtered_data['Date'].max()}")
    
    # If no data after filtering
    if filtered_data.empty:
        filter_desc = ""
        if date_filter_applied:
            date_range_display = []
            if params["date_range"]:
                if len(params["date_range"]) == 1:
                    date_range_display.append(params["date_range"][0])
                elif len(params["date_range"]) == 2:
                    date_range_display.append(f"{params['date_range'][0]} to {params['date_range'][1]}")
            filter_desc = f" for {', '.join(date_range_display)}"
            
        return {"error": f"No data found{filter_desc}. Available dates: {', '.join([d.strftime('%Y-%m-%d') for d in original_data['Date'].dt.date.unique()])}", "result": None}
    
    # Handle farm filtering
    farm_columns = FARM_COLUMNS
    if params["farms"]:
        farm_columns = [f for f in params["farms"] if f in FARM_COLUMNS]
    
    # If no valid farm columns specified, use all farms
    if not farm_columns:
        farm_columns = FARM_COLUMNS
    
    # Calculate results based on query type
    result = {}
    
    # Set query_type to "count" if it's "unknown" - this is a common default for date range queries
    if params["query_type"] == "unknown" and params["date_range"]:
        params["query_type"] = "count"
    
    if params["query_type"] == "count":
        # Total sum for specified farms
        for farm in farm_columns:
            result[farm] = int(filtered_data[farm].sum())
        result["total"] = sum(result.values())
        result["bakul"] = int(result["total"] / 40)  # Calculate bakul (40 flowers per bakul)
    
    elif params["query_type"] == "average":
        # Average for specified farms
        for farm in farm_columns:
            result[farm] = int(filtered_data[farm].mean())
        
        # Overall average per day
        daily_totals = filtered_data[farm_columns].sum(axis=1)
        result["daily_average"] = int(daily_totals.mean())
    
    elif params["query_type"] == "comparison":
        # Compare farms over the period
        for farm in farm_columns:
            result[farm] = int(filtered_data[farm].sum())
        
        # Add percentage distribution
        total = sum(result.values())
        if total > 0:
            result["percentages"] = {farm: round((result[farm] / total) * 100, 1) for farm in farm_columns}
    
    elif params["query_type"] == "maximum":
        # Find day with maximum production
        daily_totals = filtered_data[farm_columns].sum(axis=1)
        max_day_idx = daily_totals.idxmax()
        max_day = filtered_data.iloc[max_day_idx]
        
        result["max_date"] = max_day["Date"].date().isoformat()
        result["max_total"] = int(daily_totals[max_day_idx])
        
        # Get farm breakdown for max day
        for farm in farm_columns:
            result[farm] = int(max_day[farm])
    
    elif params["query_type"] == "minimum":
        # Find day with minimum production
        daily_totals = filtered_data[farm_columns].sum(axis=1)
        min_day_idx = daily_totals.idxmin()
        min_day = filtered_data.iloc[min_day_idx]
        
        result["min_date"] = min_day["Date"].date().isoformat()
        result["min_total"] = int(daily_totals[min_day_idx])
        
        # Get farm breakdown for min day
        for farm in farm_columns:
            result[farm] = int(min_day[farm])
    
    else:
        # Default: return totals
        for farm in farm_columns:
            result[farm] = int(filtered_data[farm].sum())
        result["total"] = sum(result.values())
        result["bakul"] = int(result["total"] / 40)
    
    # Add date range information to result with improved clarity
    if params["date_range"]:
        if len(params["date_range"]) == 1:
            # Single date or special reference
            date_str = params["date_range"][0]
            if date_str in ["today", "yesterday", "last week", "this month"]:
                result["query_date"] = date_str
            elif "month" in date_str.lower():
                # Extract just the month name
                month_match = re.search(r'(\w+)\s+month', date_str.lower())
                if month_match:
                    result["query_month"] = month_match.group(1)
            else:
                result["query_date"] = date_str
        elif len(params["date_range"]) == 2:
            result["query_date_range"] = params["date_range"]
    
    # Add number of days in filtered data
    result["days_count"] = len(filtered_data)
    
    # Add the actual dates included in the result for verification
    result["actual_dates"] = [d.date().isoformat() for d in filtered_data['Date']]
    
    # Check if we have farm information
    result["farms_queried"] = farm_columns
    
    # Save original query for reference
    result["original_query"] = params.get("original_query", "")
    
    return {"error": None, "result": result}
def generate_answer(query: str, query_params: Dict[str, Any], query_result: Dict[str, Any]) -> str:
    """Generate a natural language answer to the flower query using Gemini AI with enhanced data analysis."""
    # Check if Gemini is available
    gemini_available = initialize_gemini()
    
    if not gemini_available:
        # Fallback to rule-based response generation
        return generate_simple_answer(query, query_params, query_result)
    
    try:
        # Handle errors in the query result directly
        if query_result.get("error"):
            return f"Sorry, I couldn't answer that question: {query_result['error']}"
        
        result = query_result.get("result", {})
        if not result:
            return "Sorry, I couldn't find any relevant data to answer your question."
        
        # Extract date information from the query result
        date_info = ""
        date_filter_type = ""
        
        if "query_date" in result:
            if result["query_date"] in ["today", "yesterday", "last week", "this month"]:
                date_info = result["query_date"]
                date_filter_type = "special"
            else:
                date_info = result["query_date"]
                date_filter_type = "single_date"
        elif "query_month" in result:
            date_info = result["query_month"]
            date_filter_type = "month"
        elif "query_date_range" in result:
            date_info = f"{result['query_date_range'][0]} to {result['query_date_range'][1]}"
            date_filter_type = "range"
        
        # Get the actual dates that were included in the calculation
        actual_dates = result.get("actual_dates", [])
        
        # Improved prompt for Gemini with explicit date information
        prompt = f"""
        You are a helpful assistant for a flower farm tracking application called "Bunga di Kebun" in Malaysia.
        
        The application tracks flower ("Bunga") production across four farms (kebun):
        - Kebun Sendiri
        - Kebun DeYe
        - Kebun Uncle
        - Kebun Asan
        
        A user has asked: "{query}"
        
        CRITICAL INFORMATION ABOUT THE DATA FILTERING:
        
        The query has been interpreted to ask about flower data {date_info_description(date_filter_type, date_info)}.
        
        The actual dates included in the calculation are:
        {', '.join(actual_dates)}
        
        There were {len(actual_dates)} day(s) included in this data.
        
        Here are the specific results:
        {json.dumps(result, indent=2)}
        
        INSTRUCTIONS FOR GENERATING THE ANSWER:
        
        1. Be extremely precise about the date range in your answer. Mention EXACTLY which dates were included.
        
        2. For a total count query, include:
           - Total Bunga (flowers) across all farms: {result.get('total', 0)}
           - Total Bakul (baskets, 1 Bakul = 40 Bunga): {result.get('bakul', 0)}
           - Breakdown by farm for each of the farms that were queried
        
        3. Format numbers with thousand separators (e.g., "1,234" not "1234")
        
        4. Use Malay words "Bunga" for flower and "Bakul" for basket
        
        5. Keep your answer concise and factual
        
        Your answer should be accurate, helpful, and directly address the query using the exact data provided.
        
        Answer:
        """
        
        # Get response from Gemini
        model = genai.GenerativeModel('gemini-1.5-pro')
        response = model.generate_content(prompt)
        
        # Return the response text with verification
        answer = response.text
        
        # Add a verification line showing the exact dates used
        if actual_dates:
            verification = f"\n\nVerification: This answer is based on data from {len(actual_dates)} date(s): {', '.join(actual_dates)}"
            answer += verification
        
        return answer
    
    except Exception as e:
        # If Gemini fails, use simple response
        return generate_simple_answer(query, query_params, query_result) + f"\n\nNote: Gemini AI response generation failed: {str(e)}"

def date_info_description(filter_type, date_info):
    """Create a natural language description of the date filter for the Gemini prompt."""
    if filter_type == "special":
        return f"for {date_info}"
    elif filter_type == "single_date":
        return f"for the specific date of {date_info}"
    elif filter_type == "month":
        return f"for the month of {date_info}"
    elif filter_type == "range":
        return f"for the date range from {date_info}"
    else:
        return "for the specified time period"

def generate_simple_answer(query: str, query_params: Dict[str, Any], query_result: Dict[str, Any]) -> str:
    """Generate a simple rule-based answer when Gemini is not available."""
    if query_result.get("error"):
        return f"Sorry, I couldn't answer that question: {query_result['error']}"
    
    result = query_result.get("result", {})
    if not result:
        return "Sorry, I couldn't find any relevant data to answer your question."
    
    # Generate answer based on query type
    query_type = query_params.get("query_type", "unknown")
    
    # Use actual dates for more precise response
    actual_dates = result.get("actual_dates", [])
    dates_text = ""
    
    if actual_dates:
        if len(actual_dates) == 1:
            dates_text = f"on {actual_dates[0]}"
        elif len(actual_dates) > 1:
            dates_text = f"from {actual_dates[0]} to {actual_dates[-1]}"
    else:
        # Fallback to query date if no actual dates
        if "query_date" in result:
            dates_text = f"on {result['query_date']}"
        elif "query_month" in result:
            dates_text = f"in {result['query_month']}"
        elif "query_date_range" in result:
            dates_text = f"from {result['query_date_range'][0]} to {result['query_date_range'][1]}"
        else:
            dates_text = "for the requested period"
    
    farms_info = ", ".join(result.get("farms_queried", []))
    
    if query_type == "count":
        # Total count response
        if len(result.get("farms_queried", [])) == 1:
            # Single farm
            farm = result.get("farms_queried")[0]
            return f"Based on the data {dates_text}, the total number of Bunga from {farm} is {format_number(result[farm])}. This is equivalent to {format_number(result.get('bakul', 0))} Bakul."
        else:
            # Multiple farms
            farm_details = ". ".join([f"{farm}: {format_number(result[farm])}" for farm in result.get("farms_queried", []) if farm in result])
            return f"Based on the data {dates_text}, the total Bunga is {format_number(result.get('total', 0))}, which is {format_number(result.get('bakul', 0))} Bakul. Breakdown by farm: {farm_details}."
    
    elif query_type == "average":
        # Average response
        if len(result.get("farms_queried", [])) == 1:
            # Single farm
            farm = result.get("farms_queried")[0]
            return f"Based on the data {dates_text}, the average daily Bunga production for {farm} is {format_number(result[farm])}."
        else:
            # Multiple farms
            farm_details = ". ".join([f"{farm}: {format_number(result[farm])}" for farm in result.get("farms_queried", []) if farm in result])
            return f"Based on the data {dates_text}, the average daily Bunga production is {format_number(result.get('daily_average', 0))}. Breakdown by farm: {farm_details}."
    
    elif query_type == "comparison":
        # Comparison response
        farm_details = []
        percentages = result.get("percentages", {})
        
        for farm in result.get("farms_queried", []):
            if farm in result:
                percent = percentages.get(farm, 0) if percentages else 0
                farm_details.append(f"{farm}: {format_number(result[farm])} Bunga ({percent}%)")
                
        comparisons = ", ".join(farm_details)
        return f"Based on the data {dates_text}, here is the comparison of Bunga production: {comparisons}"
    
    elif query_type == "maximum":
        # Maximum response
        max_date = result.get("max_date", "")
        max_total = result.get("max_total", 0)
        
        farm_details = ". ".join([f"{farm}: {format_number(result[farm])}" for farm in result.get("farms_queried", []) if farm in result])
        
        return f"Based on the data {dates_text}, the day with maximum Bunga production was {max_date} with a total of {format_number(max_total)} Bunga. Breakdown by farm: {farm_details}."
    
    elif query_type == "minimum":
        # Minimum response
        min_date = result.get("min_date", "")
        min_total = result.get("min_total", 0)
        
        farm_details = ". ".join([f"{farm}: {format_number(result[farm])}" for farm in result.get("farms_queried", []) if farm in result])
        
        return f"Based on the data {dates_text}, the day with minimum Bunga production was {min_date} with a total of {format_number(min_total)} Bunga. Breakdown by farm: {farm_details}."
    
    else:
        # Default response - just return totals based on available data
        if len(result.get("farms_queried", [])) == 1:
            # Single farm
            farm = result.get("farms_queried")[0]
            return f"Based on the data {dates_text}, the total Bunga from {farm} is {format_number(result[farm])}. This is equivalent to approximately {format_number(result.get('bakul', 0))} Bakul."
        else:
            # Multiple farms
            farm_details = ". ".join([f"{farm}: {format_number(result[farm])}" for farm in result.get("farms_queried", []) if farm in result])
            return f"Based on the data {dates_text}, the total Bunga is {format_number(result.get('total', 0))}, which is approximately {format_number(result.get('bakul', 0))} Bakul. Breakdown by farm: {farm_details}."

def format_number(number):
    """Format a number with thousand separators."""
    return f"{int(number):,}"
def verify_answer(answer, query_result):
    """Checks if the answer is consistent with the actual data"""
    
    # Check if there's an error in the result
    if query_result.get("error"):
        return answer
    
    result = query_result.get("result", {})
    if not result:
        return answer
        
    # Get the actual dates from the result
    actual_dates = result.get("actual_dates", [])
    
    # Add verification text to the answer
    verification = f"\n\nVerification: This answer is based on data from {len(actual_dates)} date(s): {', '.join(actual_dates)}"
    
    return answer + verification
# Q&A tab function
# Add this function just before your qa_tab function
def verify_answer(answer, query_result):
    """Checks if the answer is consistent with the actual data"""
    
    # Check if there's an error in the result
    if query_result.get("error"):
        return answer
    
    result = query_result.get("result", {})
    if not result:
        return answer
        
    # Get the actual dates from the result
    actual_dates = result.get("actual_dates", [])
    
    # Add verification text to the answer
    verification = f"\n\nVerification: This answer is based on data from {len(actual_dates)} date(s): {', '.join(actual_dates)}"
    
    return answer + verification

# Q&A tab function
# Add this function just before your qa_tab function
def send_email_notification(date, farm_data):
    """Send email notification with secure password handling and improved formatting"""
    try:
        # Email settings
        sender_email = "hqtong2013@gmail.com"
        receiver_email = "hq_tong@hotmail.com"
        
        # Try different ways to get the password
        password_source = "hardcoded fallback"
        try:
            # Try as top-level secret
            password = st.secrets["email_password"]
            password_source = "top-level secret"
        except (KeyError, TypeError):
            try:
                # Try inside general section
                password = st.secrets["general"]["email_password"]
                password_source = "general section secret"
            except (KeyError, TypeError):
                # Fallback to hardcoded (only as last resort)
                password = "ukwdxxrccukpihqj"
                password_source = "hardcoded fallback"
        
        # Calculate totals
        total_bunga = sum(farm_data.values())
        total_bakul = int(total_bunga / 40)
        
        # Format date with day name - convert string date to datetime if needed
        if isinstance(date, str):
            date_obj = datetime.strptime(date, '%Y-%m-%d')
        else:
            date_obj = date
            
        day_name = date_obj.strftime('%A')
        date_formatted = date_obj.strftime('%Y-%m-%d')
        
        # Create message
        message = MIMEMultipart('alternative')  # Support both plain text and HTML
        message["From"] = sender_email
        message["To"] = receiver_email
        
        # New subject line format
        message["Subject"] = f"Total Bunga {date_formatted}: {total_bunga:,} bunga, {total_bakul} bakul"
        
        # Format farm data with proper alignment
        farm_info = ""
        max_farm_name_length = max(len(farm) for farm in farm_data.keys())
        
        for farm, value in farm_data.items():
            # Pad farm name for alignment
            padded_name = farm.ljust(max_farm_name_length)
            farm_info += f"{padded_name}: {value:,} Bunga\n"
        
        # Get Malaysia time (UTC+8)
        malaysia_tz = timezone(timedelta(hours=8))
        malaysia_time = datetime.now(malaysia_tz).strftime('%Y-%m-%d %H:%M:%S')
        
        # Plain text email body
        text_body = f"""
        Total Bunga {date_formatted}: {total_bunga:,} bunga, {total_bakul} bakul
        
        Date: {date_formatted} ({day_name})
        Total bunga: {total_bunga:,}
        Total bakul: {total_bakul}
        
        Farm Details:
        {farm_info}
        
        -----------------------------
        System Information:
        Password retrieved from: {password_source}
        Timestamp: {malaysia_time} (Malaysia Time)
        -----------------------------
        
        This is an automated notification from Bunga di Kebun System.
        """
        
        # HTML email body with requested formatting
        html_body = f"""
        <html>
        <head>
            <style>
                body {{ font-family: Arial, sans-serif; line-height: 1.6; }}
                .header {{ font-size: 18px; font-weight: bold; margin-bottom: 20px; }}
                .important {{ color: #FF0000; font-weight: bold; }}
                .farm-details {{ font-family: Courier New, monospace; margin: 15px 0; }}
                .footer {{ color: #666; font-size: 12px; margin-top: 30px; }}
                .system-info {{ background-color: #f5f5f5; padding: 10px; border-radius: 5px; margin: 20px 0; }}
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
            
            <div class="system-info">
                <p><strong>System Information:</strong></p>
                <p>Password retrieved from: {password_source}</p>
                <p>Timestamp: {malaysia_time} (Malaysia Time)</p>
            </div>
            
            <p class="footer">This is an automated notification from Bunga di Kebun System.</p>
        </body>
        </html>
        """
        
        # Attach both plain text and HTML versions
        message.attach(MIMEText(text_body, "plain"))
        message.attach(MIMEText(html_body, "html"))
        
        # Send email
        with smtplib.SMTP_SSL("smtp.gmail.com", 465) as server:
            server.login(sender_email, password)
            server.send_message(message)
        
        st.success(f"Email sent. Password approach: {password_source}")
        return True
        
    except Exception as e:
        st.error(f"Email error: {str(e)}")
        return False

def smart_filter_data(data: pd.DataFrame, query: str) -> pd.DataFrame:
    """Filter data based on query before processing"""
    
    # Make a copy to avoid modifying the original
    filtered = data.copy()
def qa_tab(data: pd.DataFrame):
    """Display the Q&A tab for natural language queries about flower data"""
    st.header("Ask Questions About Your Flower Data")
    
    # Check if data is available
    if data.empty:
        st.info("No data available for questions. Please add data in the Data Entry tab first.")
        return
    
    # Add some example questions
    with st.expander("Example questions you can ask"):
        st.markdown("""
        Here are some examples of questions you can ask:
        
        - How many Bunga were collected yesterday?
        - What was the total production from 01/04/2023 to 15/04/2023?
        - What is the average daily production for Kebun Sendiri?
        - Which farm had the highest production last week?
        - How many Bakul did we collect from Kebun Uncle this month?
        - Compare the production between Kebun DeYe and Kebun Asan for April 2023
        - What was our best production day?
        """)
    
    # Question input
    query = st.text_input("Type your question here:", placeholder="e.g., How many Bunga did we collect last week?")
    
    # Process query when submitted
    if query:
        with st.spinner("Finding the answer..."):
            # Pre-filter data based on the query
            filtered_data = smart_filter_data(data, query)
            
            # Only continue if there's data after filtering
            if filtered_data.empty:
                st.warning("No data found for your specific query. Please try a different question.")
                return
            # END OF NEW CODE
            # Check if Gemini API is initialized
            gemini_available = initialize_gemini()
            
            if not gemini_available:
                st.warning("Gemini AI is not available. Using basic answer generation.")
            
            # Create a progress bar for query processing
            progress_bar = st.progress(0)
            
            # Step 1: Parse the query
            progress_bar.progress(25)
            query_params = parse_query(query)
            
            # Step 2: Execute the query against the data
            progress_bar.progress(50)
            query_result = execute_query(query_params, data)
            
            # Step 3: Generate answer
            progress_bar.progress(75)
            answer = generate_answer(query, query_params, query_result)
            
            # Add verification information
            answer_with_verification = verify_answer(answer, query_result)
                        
            # Complete progress
            progress_bar.progress(100)
                        
            # Display the answer
            st.markdown("### Answer")
            st.markdown(f"""
            <div style="background-color: #f0f7ff; padding: 15px; border-radius: 10px; margin-bottom: 20px;">
                <p style="font-size: 1.1em;">{answer_with_verification}</p>
            </div>
            """, unsafe_allow_html=True)

            # Clear progress bar
            progress_bar.empty()
            
            # Show detailed query interpretation (for debugging or curious users)
            with st.expander("See query details"):
                st.json({
                    "Query": query,
                    "Interpreted as": query_params,
                    "Result data": query_result["result"] if not query_result.get("error") else None,
                    "Error": query_result.get("error")
                })
                
# Initialize session state variables
if 'logged_in' not in st.session_state:
    st.session_state.logged_in = False

if 'username' not in st.session_state:
    st.session_state.username = ""

if 'role' not in st.session_state:
    st.session_state.role = ""

if 'current_user_data' not in st.session_state:
    st.session_state.current_user_data = pd.DataFrame(columns=['Date'] + FARM_COLUMNS)

if 'storage_mode' not in st.session_state:
    st.session_state.storage_mode = "Checking..."

# Add a flag for rerunning the app
if 'needs_rerun' not in st.session_state:
    st.session_state.needs_rerun = False

# Initialize the app when needed
if 'app_initialized' not in st.session_state:
    initialize_app()
    st.session_state.app_initialized = True

# Function to add data for the current user
def add_data(date, farm_1, farm_2, farm_3, farm_4):
    # Create a new row
    new_row = pd.DataFrame({
        'Date': [pd.Timestamp(date)],
        FARM_COLUMNS[0]: [farm_1],
        FARM_COLUMNS[1]: [farm_2],
        FARM_COLUMNS[2]: [farm_3],
        FARM_COLUMNS[3]: [farm_4]
    })
    
    # Check if date already exists
    if not st.session_state.current_user_data.empty and any(pd.Timestamp(date) == d for d in st.session_state.current_user_data['Date'].values):
        st.error(f"Data for {date} already exists. Please edit the existing entry or choose a different date.")
        return False
    
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
    try:
        send_email_notification(date, farm_data)
        st.success("Data added and notification email sent!")
    except Exception as e:
        st.warning(f"Data added but failed to send notification: {str(e)}")    
        return True
    else:
        # If save fails, revert the change
        st.session_state.current_user_data = load_data(st.session_state.username)
        return False

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
                    users = get_users_collection()
                    if users:
                        try:
                            user_docs = users.where("username", "==", new_username).limit(1).get()
                            if len(list(user_docs)) > 0:
                                st.error("Username already exists")
                            else:
                                if add_user(new_username, new_password):
                                    st.success("Registration successful! You can now login.")
                                else:
                                    st.error("Error during registration")
                        except Exception as e:
                            st.error(f"Error checking username: {e}")
                    else:
                        if new_username in st.session_state.users:
                            st.error("Username already exists")
                        else:
                            if add_user(new_username, new_password):
                                st.success("Registration successful! You can now login.")
                            else:
                                st.error("Error during registration")

    # Add a note about the default admin account
    st.markdown("---")
    st.info("First time? Use username: 'admin' and password: 'admin' to login, then create your own account.")

# Format number with thousands separator
def format_number(number):
    return f"{int(number):,}"

# Main app function
def main_app():
    st.title(f"🌷 Bunga di Kebun - Welcome, {st.session_state.username}!")
    
    # Display storage mode
    st.caption(f"Storage mode: {st.session_state.storage_mode}")
    
    # Create tabs for different functions
    tab1, tab2, tab3 = st.tabs(["Data Entry", "Data Analysis", "Ask Questions"])

    # Define the Q&A tab early, even though we'll use it later
    if tab3:  # This condition will always be true, but it creates a separate scope
        with tab3:
            qa_tab(st.session_state.current_user_data)
    
    # Tab 1: Data Entry
    with tab1:
        st.header("Add New Data")
        
        # Form for data entry
        with st.form("data_entry_form", clear_on_submit=True):
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
            submitted = st.form_submit_button("Add Data")
            
            if submitted:
                if add_data(date, farm_1, farm_2, farm_3, farm_4):
                    st.success(f"Data for {date} added successfully!")
        
        # Add some sample data for testing if data is empty
        if st.button("Add Sample Data") and len(st.session_state.current_user_data) == 0:
            # Create some sample dates (last 10 days)
            dates = [(datetime.now() - timedelta(days=i)).date() for i in range(10, 0, -1)]
            
            # Add random data for each date
            for date in dates:
                add_data(
                    date,
                    np.random.randint(50, 200),  # Farm 1
                    np.random.randint(30, 150),  # Farm 2
                    np.random.randint(70, 250),  # Farm 3
                    np.random.randint(40, 180)   # Farm 4
                )
            st.success("Sample data added successfully!")
            st.session_state.needs_rerun = True
        
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
    
    # Tab 2: Data Analysis
    with tab2:
        st.header("Bunga Production Analysis")
        
        if st.session_state.current_user_data.empty:
            st.info("No data available for analysis. Please add data in the Data Entry tab.")
        else:
            # Use the data already in datetime format
            analysis_df = st.session_state.current_user_data.copy()
            
            # Keep only necessary columns - remove any old farm columns if they exist
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
                total_bunga = int(filtered_df[FARM_COLUMNS].sum().sum())  # Round to integer
                
                # Calculate total bakul (divide total bunga by 40)
                total_bakul = int(total_bunga / 40)
                
                # Display total bunga prominently with red color, bold, and larger font
                st.markdown(f"""
                <div style="background-color: #ffeeee; padding: 15px; border-radius: 5px; margin-bottom: 20px;">
                    <h1 style="color: #ff0000; font-weight: bold; font-size: 2.5em; text-align: center;">
                        Total Bunga: {format_number(total_bunga)}
                    </h1>
                </div>
                """, unsafe_allow_html=True)
                
                # Display total bakul prominently with blue color, bold, and larger font
                st.markdown(f"""
                <div style="background-color: #eeeeff; padding: 15px; border-radius: 5px; margin-bottom: 20px;">
                    <h1 style="color: #0000ff; font-weight: bold; font-size: 2.5em; text-align: center;">
                        Total Bakul: {format_number(total_bakul)}
                    </h1>
                </div>
                """, unsafe_allow_html=True)
                
                # Show filtered data
                st.subheader("Filtered Data")
                
                # Reorganize columns to show Date first, then Farm columns
                filtered_display = filtered_df.copy()
                
                # Add day of week and calculate total bunga for each row
                filtered_display['Day'] = filtered_display['Date'].dt.strftime('%A')
                filtered_display['Total Bunga'] = filtered_display[FARM_COLUMNS].sum(axis=1).astype(int)
                filtered_display['Date'] = filtered_display['Date'].dt.date
                
                # Reorder columns with Date, Day, Total Bunga, and then Farm columns
                filtered_display = filtered_display[['Date', 'Day', 'Total Bunga'] + FARM_COLUMNS]
                
                # Format numbers with thousand separators
                filtered_display['Total Bunga'] = filtered_display['Total Bunga'].apply(format_number)
                for col in FARM_COLUMNS:
                    filtered_display[col] = filtered_display[col].apply(format_number)
                
                # Add row numbers starting from 1
                filtered_display.index = filtered_display.index + 1
                
                # Add CSS for styling the dataframe
                st.markdown("""
                <style>
                /* Custom CSS to style the table */
                .dataframe-container .stDataFrame {
                    font-size: 14px !important;
                }
                
                /* Reduce width of numerical columns */
                .dataframe-container .stDataFrame td {
                    max-width: 100px !important;
                    padding: 2px 8px !important;
                    white-space: nowrap !important;
                }
                
                /* Style for Total Bunga column */
                .dataframe-container .stDataFrame td:nth-child(4) {
                    color: #ff0000 !important;
                    font-weight: bold !important;
                }
                
                /* Style for header row */
                .dataframe-container .stDataFrame th {
                    padding: 4px 8px !important;
                    font-weight: bold !important;
                    text-align: center !important;
                    white-space: nowrap !important;
                }
                </style>
                """, unsafe_allow_html=True)
                
                # Display the dataframe
                st.markdown('<div class="dataframe-container">', unsafe_allow_html=True)
                st.dataframe(
                    filtered_display,
                    use_container_width=True,
                    column_config={
                        "_index": st.column_config.Column(
                            "No.",
                            width="small"
                        ),
                        "Date": st.column_config.DateColumn(
                            "Date",
                            width="small"
                        ),
                        "Day": st.column_config.TextColumn(
                            "Day",
                            width="small"
                        ),
                        "Total Bunga": st.column_config.TextColumn(
                            "Total Bunga",
                            width="small"
                        ),
                        FARM_COLUMNS[0]: st.column_config.TextColumn(
                            FARM_COLUMNS[0],
                            width="small"
                        ),
                        FARM_COLUMNS[1]: st.column_config.TextColumn(
                            FARM_COLUMNS[1],
                            width="small"
                        ),
                        FARM_COLUMNS[2]: st.column_config.TextColumn(
                            FARM_COLUMNS[2],
                            width="small"
                        ),
                        FARM_COLUMNS[3]: st.column_config.TextColumn(
                            FARM_COLUMNS[3],
                            width="small"
                        ),
                    },
                    hide_index=False
                )
                st.markdown('</div>', unsafe_allow_html=True)
                
                # Farm summary statistics - only totals as requested
                st.subheader("Farm Totals")
                
                # Create summary dataframe with just totals
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
                
                # Format numbers with thousand separators
                summary['Total'] = summary['Total'].apply(format_number)
                
                # Add row numbers starting from 1
                summary.index = summary.index + 1
                
                # Add CSS for styling the summary table
                st.markdown("""
                <style>
                /* Style for Total All Farms row */
                .summary-table .stDataFrame tr:last-child td {
                    color: #ff0000 !important;
                    font-weight: bold !important;
                }
                </style>
                """, unsafe_allow_html=True)
                
                # Display summary statistics
                st.markdown('<div class="summary-table">', unsafe_allow_html=True)
                st.dataframe(
                    summary,
                    use_container_width=True,
                    column_config={
                        "_index": st.column_config.Column(
                            "No.",
                            width="small"
                        ),
                        "Farm": st.column_config.TextColumn(
                            "Farm",
                            width="medium"
                        ),
                        "Total": st.column_config.TextColumn(
                            "Total",
                            width="small"
                        )
                    },
                    hide_index=False
                )
                st.markdown('</div>', unsafe_allow_html=True)
                
                # Create visualizations
                st.subheader("Visualizations")
                
                # Farm comparison visualization
                farm_totals = pd.DataFrame({
                    'Farm': FARM_COLUMNS,
                    'Total Bunga': [int(filtered_df[col].sum()) for col in FARM_COLUMNS]
                })
                
                chart_type = st.radio("Select Chart Type", ["Bar Chart", "Pie Chart"], horizontal=True)
                
                col1, col2 = st.columns(2)
                
                with col1:
                    # Total bunga by farm
                    st.subheader("Total Bunga by Farm")
                    if chart_type == "Bar Chart":
                        fig = px.bar(
                            farm_totals,
                            x='Farm',
                            y='Total Bunga',
                            color='Farm',
                            title="Total Bunga Production by Farm",
                            color_discrete_sequence=px.colors.qualitative.Set3
                        )
                        # Format y-axis tick labels with thousands separators
                        fig.update_layout(
                            yaxis=dict(
                                tickformat=",",
                            )
                        )
                        st.plotly_chart(fig, use_container_width=True)
                    else:
                        fig = px.pie(
                            farm_totals,
                            values='Total Bunga',
                            names='Farm',
                            title="Bunga Production Distribution",
                            color='Farm',
                            color_discrete_sequence=px.colors.qualitative.Set3
                        )
                        # Format hover text with thousands separators
                        fig.update_traces(
                            texttemplate="%{value:,}",
                            hovertemplate="%{label}: %{value:,} Bunga<extra></extra>"
                        )
                        st.plotly_chart(fig, use_container_width=True)
                
                with col2:
                    # Overall totals
                    st.subheader("Daily Production")
                    
                    # Calculate daily totals
                    daily_totals = filtered_df.copy()
                    # Add day name to the date for x-axis
                    daily_totals['Day'] = daily_totals['Date'].dt.strftime('%A')
                    daily_totals['Date_Display'] = daily_totals['Date'].dt.strftime('%Y-%m-%d (%A)')
                    daily_totals['Total'] = daily_totals[FARM_COLUMNS].sum(axis=1)
                    
                    # Create line chart for daily totals
                    fig = px.line(
                        daily_totals,
                        x='Date',
                        y='Total',
                        title="Daily Total Bunga Production",
                        markers=True
                    )
                    # Format axis and hover text
                    fig.update_layout(
                        xaxis=dict(
                            title="Date",
                            tickformat="%Y-%m-%d",
                            tickmode="array",
                            tickvals=daily_totals['Date'],
                            ticktext=daily_totals['Date_Display']
                        ),
                        yaxis=dict(
                            title="Total Bunga",
                            tickformat=",",
                        )
                    )
                    # Format hover text with thousands separators
                    fig.update_traces(
                        hovertemplate="Date: %{x|%Y-%m-%d} (%{text})<br>Total: %{y:,} Bunga<extra></extra>",
                        text=daily_totals['Day']
                    )
                    st.plotly_chart(fig, use_container_width=True)
                
                # Show daily production by farm
                st.subheader("Daily Production by Farm")
                
                # Melt the dataframe to get it in the right format for the chart
                melted_df = pd.melt(
                    filtered_df,
                    id_vars=['Date'],
                    value_vars=FARM_COLUMNS,
                    var_name='Farm',
                    value_name='Bunga'
                )
                
                # Add day name for display
                melted_df['Day'] = melted_df['Date'].dt.strftime('%A')
                melted_df['Date_Display'] = melted_df['Date'].dt.strftime('%Y-%m-%d (%A)')
                
                # Create the line chart
                fig = px.line(
                    melted_df,
                    x='Date',
                    y='Bunga',
                    color='Farm',
                    title="Daily Bunga Production by Farm",
                    markers=True,
                    color_discrete_sequence=px.colors.qualitative.Set3
                )
                # Format axis and hover text
                fig.update_layout(
                    xaxis=dict(
                        title="Date",
                        tickformat="%Y-%m-%d",
                        tickmode="array",
                        tickvals=melted_df['Date'].unique(),
                        ticktext=[d.strftime('%Y-%m-%d (%A)') for d in melted_df['Date'].unique()]
                    ),
                    yaxis=dict(
                        title="Bunga",
                        tickformat=",",
                    )
                )
                # Format hover text with thousands separators
                fig.update_traces(
                    hovertemplate="Date: %{x|%Y-%m-%d} (%{text})<br>Bunga: %{y:,}<extra></extra>",
                    text=melted_df['Day']
                )
                st.plotly_chart(fig, use_container_width=True)
                
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

# Add data editing capabilities and sidebar options
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
        
        # Option to edit/delete by selecting a date
        if not st.session_state.current_user_data.empty:
            # Ensure Date column is datetime
            if 'Date' in st.session_state.current_user_data.columns:
                st.session_state.current_user_data['Date'] = pd.to_datetime(st.session_state.current_user_data['Date'])
                # Extract dates for selection
                dates = st.session_state.current_user_data['Date'].dt.date.unique()
                selected_date = st.sidebar.selectbox("Select date to edit/delete:", dates)
                
                # Get the row index for the selected date
                date_idx = st.session_state.current_user_data[st.session_state.current_user_data['Date'].dt.date == selected_date].index[0]
                
                # Display current values
                st.sidebar.text(f"Current values for {selected_date}:")
                current_row = st.session_state.current_user_data.iloc[date_idx]
                
                # Format values with thousand separators for display
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
                
                # Delete option - Fixed with session state for persistence
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
                            st.session_state[delete_key] = False  # Reset the state after operation
                    else:
                        if st.sidebar.button("Cancel deletion"):
                            st.session_state[delete_key] = False

    # Upload CSV file
    st.sidebar.subheader("Import Data")
    uploaded_file = st.sidebar.file_uploader("Upload existing data (CSV)", type="csv")
    if uploaded_file is not None:
        try:
            # Read the CSV file
            uploaded_df = pd.read_csv(uploaded_file)
            
            # Check if the required columns exist - try both old and new column names
            required_cols = ['Date']
            has_old_cols = all(col in uploaded_df.columns for col in OLD_FARM_COLUMNS)
            has_new_cols = all(col in uploaded_df.columns for col in FARM_COLUMNS)
            
            if 'Date' in uploaded_df.columns and (has_old_cols or has_new_cols):
                # Convert Date to datetime
                uploaded_df['Date'] = pd.to_datetime(uploaded_df['Date'])
                
                # Convert old column names to new ones if needed
                if has_old_cols and not has_new_cols:
                    for old_col, new_col in zip(OLD_FARM_COLUMNS, FARM_COLUMNS):
                        uploaded_df[new_col] = uploaded_df[old_col]
                        uploaded_df = uploaded_df.drop(old_col, axis=1)
                
                # Allow the user to choose whether to replace or append
                action = st.sidebar.radio("Select action", ["Replace current data", "Append to current data"])
                
                if st.sidebar.button("Confirm Import"):
                    if action == "Replace current data":
                        st.session_state.current_user_data = uploaded_df
                    else:
                        # Append and remove duplicates based on Date
                        combined = pd.concat([st.session_state.current_user_data, uploaded_df])
                        st.session_state.current_user_data = combined.drop_duplicates(subset=['Date']).sort_values(by='Date').reset_index(drop=True)
                    
                    # Save to database
                    if save_data(st.session_state.current_user_data, st.session_state.username):
                        st.sidebar.success("Data imported successfully!")
                        st.session_state.needs_rerun = True
            else:
                required_cols_str = ", ".join(['Date'] + FARM_COLUMNS)
                st.sidebar.error(f"CSV must contain columns: {required_cols_str} OR {', '.join(['Date'] + OLD_FARM_COLUMNS)}")
        except Exception as e:
            st.sidebar.error(f"Error importing data: {e}")

    # Clear all data button - Fixed with session state for persistence
    st.sidebar.subheader("Clear Data")
    if 'show_clear_confirm' not in st.session_state:
        st.session_state.show_clear_confirm = False

    if st.sidebar.button("Clear All Data"):
        st.session_state.show_clear_confirm = True

    if st.session_state.show_clear_confirm:
        confirm = st.sidebar.checkbox("I confirm I want to delete all data", key="confirm_clear_all")
        if confirm:
            # Create empty DataFrame
            st.session_state.current_user_data = pd.DataFrame(columns=['Date'] + FARM_COLUMNS)
            
            # Save to database
            if save_data(st.session_state.current_user_data, st.session_state.username):
                st.sidebar.success("All data cleared!")
                st.session_state.needs_rerun = True
                st.session_state.show_clear_confirm = False  # Reset after operation
        else:
            if st.sidebar.button("Cancel clear"):
                st.session_state.show_clear_confirm = False

    # Storage info
    st.sidebar.markdown("---")
    st.sidebar.subheader("Storage Information")
    st.sidebar.info(f"Data Storage Mode: {st.session_state.storage_mode}")
    
    if st.session_state.storage_mode == "Session State":
        st.sidebar.warning("Data is stored in browser session only. For permanent storage, download your data regularly.")

    # Footer
    st.sidebar.markdown("---")
    st.sidebar.markdown("🌷 Bunga di Kebun - Firebase Storage v1.0")
    st.sidebar.text(f"User: {st.session_state.username} ({st.session_state.role})")

# Determine storage mode at startup
def check_storage_mode():
    db = connect_to_firebase()
    if db:
        try:
            # Quick test of Firebase connection
            users = db.collection('users')
            users.limit(1).get()
            st.session_state.storage_mode = "Firebase Database"
            st.success("Firebase connection established!")
            return
        except Exception as e:
            st.error(f"Firebase connection test failed: {e}")
    
    st.session_state.storage_mode = "Session State"
    st.warning("Using Session State storage - data will not persist between sessions")

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
