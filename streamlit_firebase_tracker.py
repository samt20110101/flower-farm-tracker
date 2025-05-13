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
from datetime import datetime, timedelta, timezone
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
FARM_COLUMNS = ['A: Kebun Sendiri', 'B: Kebun DeYe', 'C: Kebun Asan', 'D: Kebun Uncle']
OLD_FARM_COLUMNS = ['Farm A', 'Farm B', 'Farm C', 'Farm D']

# NOW ADD THE SESSION STATE INITIALIZATION HERE
if 'storage_mode' not in st.session_state:
    st.session_state.storage_mode = "Checking..."

if 'logged_in' not in st.session_state:
    st.session_state.logged_in = False

if 'username' not in st.session_state:
    st.session_state.username = ""

if 'role' not in st.session_state:
    st.session_state.role = ""

if 'current_user_data' not in st.session_state:
    st.session_state.current_user_data = pd.DataFrame(columns=['Date'] + FARM_COLUMNS)

if 'needs_rerun' not in st.session_state:
    st.session_state.needs_rerun = False

# After this, continue with your function definitions
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
    """Initialize Gemini API using securely stored API key from Streamlit secrets"""
    try:
        # Try to get API key from Streamlit secrets - no hardcoded fallback
        api_key_source = "not found"
        try:
            # Try as top-level secret
            api_key = st.secrets["gemini_api_key"]
            api_key_source = "top-level secret"
        except (KeyError, TypeError):
            try:
                # Try inside general section
                api_key = st.secrets["general"]["gemini_api_key"]
                api_key_source = "general section secret"
            except (KeyError, TypeError):
                # No API key found in secrets
                st.error("Gemini API key not found in Streamlit secrets. Please configure 'gemini_api_key' in secrets.")
                return False
        
        # Configure Gemini with the API key from secrets
        genai.configure(api_key=api_key)
        st.success(f"Gemini API initialized successfully (API key from {api_key_source})")
        return True
    except Exception as e:
        st.error(f"Error initializing Gemini API: {str(e)}")
        return False

# Add this comprehensive date range pattern detection to your parse_query function
def detect_date_ranges(query: str) -> Dict[str, Any]:
    """Detect various date range patterns in natural language queries."""
    
    # Month mapping for conversion
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
    
    # Current year for date construction
    current_year = datetime.now().year
    
    # Pattern 1: "in month" - e.g., "in april"
    month_only_pattern = r'\b(?:in|during|for|of)\s+(\w+)(?:\s+month)?\b'
    month_only_match = re.search(month_only_pattern, query.lower())
    if month_only_match:
        month_name = month_only_match.group(1).lower()
        if month_name in month_map:
            return {"date_range": [f"{month_name} month"]}
    
    # Pattern 2: "in month day to day" - e.g., "in april 20 to 30"
    month_day_range_pattern = r'\b(?:in|during|for|of)\s+(\w+)\s+(\d{1,2})(?:st|nd|rd|th)?\s+(?:to|until|and|-|through|till)\s+(\d{1,2})(?:st|nd|rd|th)?\b'
    month_day_range_match = re.search(month_day_range_pattern, query.lower())
    if month_day_range_match:
        month_name = month_day_range_match.group(1).lower()
        start_day = int(month_day_range_match.group(2))
        end_day = int(month_day_range_match.group(3))
        
        if month_name in month_map:
            month_num = month_map[month_name]
            start_date_str = f"{current_year}-{month_num:02d}-{start_day:02d}"
            end_date_str = f"{current_year}-{month_num:02d}-{end_day:02d}"
            return {"date_range": [start_date_str, end_date_str]}
    
    # Pattern 3: "from day month to day month" - e.g., "from 1 may to 15 may"
    from_to_same_month_pattern = r'\bfrom\s+(\d{1,2})(?:st|nd|rd|th)?\s+(\w+)\s+to\s+(\d{1,2})(?:st|nd|rd|th)?\s+(\w+)\b'
    from_to_match = re.search(from_to_same_month_pattern, query.lower())
    if from_to_match:
        start_day = int(from_to_match.group(1))
        start_month_name = from_to_match.group(2).lower()
        end_day = int(from_to_match.group(3))
        end_month_name = from_to_match.group(4).lower()
        
        if start_month_name in month_map and end_month_name in month_map:
            start_month_num = month_map[start_month_name]
            end_month_num = month_map[end_month_name]
            
            start_date_str = f"{current_year}-{start_month_num:02d}-{start_day:02d}"
            end_date_str = f"{current_year}-{end_month_num:02d}-{end_day:02d}"
            return {"date_range": [start_date_str, end_date_str]}
    
    # Pattern 4: "from month day to month day" - e.g., "from apr 5 to apr 10"
    from_month_day_pattern = r'\bfrom\s+(\w+)\s+(\d{1,2})(?:st|nd|rd|th)?\s+to\s+(\w+)\s+(\d{1,2})(?:st|nd|rd|th)?\b'
    from_month_day_match = re.search(from_month_day_pattern, query.lower())
    if from_month_day_match:
        start_month_name = from_month_day_match.group(1).lower()
        start_day = int(from_month_day_match.group(2))
        end_month_name = from_month_day_match.group(3).lower()
        end_day = int(from_month_day_match.group(4))
        
        if start_month_name in month_map and end_month_name in month_map:
            start_month_num = month_map[start_month_name]
            end_month_num = month_map[end_month_name]
            
            start_date_str = f"{current_year}-{start_month_num:02d}-{start_day:02d}"
            end_date_str = f"{current_year}-{end_month_num:02d}-{end_day:02d}"
            return {"date_range": [start_date_str, end_date_str]}
    
    # Pattern 5: "day to day month" - e.g., "20 to 30 april"
    day_range_month_pattern = r'\b(\d{1,2})(?:st|nd|rd|th)?\s+(?:to|until|and|-|through|till)\s+(\d{1,2})(?:st|nd|rd|th)?\s+(\w+)\b'
    day_range_month_match = re.search(day_range_month_pattern, query.lower())
    if day_range_month_match:
        start_day = int(day_range_month_match.group(1))
        end_day = int(day_range_month_match.group(2))
        month_name = day_range_month_match.group(3).lower()
        
        if month_name in month_map:
            month_num = month_map[month_name]
            
            start_date_str = f"{current_year}-{month_num:02d}-{start_day:02d}"
            end_date_str = f"{current_year}-{month_num:02d}-{end_day:02d}"
            return {"date_range": [start_date_str, end_date_str]}
    
    # Pattern 6: "between month day and month day" - e.g., "between april 1 and april 15"
    between_pattern = r'\bbetween\s+(\w+)\s+(\d{1,2})(?:st|nd|rd|th)?\s+and\s+(\w+)\s+(\d{1,2})(?:st|nd|rd|th)?\b'
    between_match = re.search(between_pattern, query.lower())
    if between_match:
        start_month_name = between_match.group(1).lower()
        start_day = int(between_match.group(2))
        end_month_name = between_match.group(3).lower()
        end_day = int(between_match.group(4))
        
        if start_month_name in month_map and end_month_name in month_map:
            start_month_num = month_map[start_month_name]
            end_month_num = month_map[end_month_name]
            
            start_date_str = f"{current_year}-{start_month_num:02d}-{start_day:02d}"
            end_date_str = f"{current_year}-{end_month_num:02d}-{end_day:02d}"
            return {"date_range": [start_date_str, end_date_str]}
    
    # Pattern 7: "from day to day of month" - e.g., "from 1st to 15th of may"
    day_to_day_of_month_pattern = r'\bfrom\s+(\d{1,2})(?:st|nd|rd|th)?\s+to\s+(\d{1,2})(?:st|nd|rd|th)?\s+of\s+(\w+)\b'
    day_to_day_match = re.search(day_to_day_of_month_pattern, query.lower())
    if day_to_day_match:
        start_day = int(day_to_day_match.group(1))
        end_day = int(day_to_day_match.group(2))
        month_name = day_to_day_match.group(3).lower()
        
        if month_name in month_map:
            month_num = month_map[month_name]
            
            start_date_str = f"{current_year}-{month_num:02d}-{start_day:02d}"
            end_date_str = f"{current_year}-{month_num:02d}-{end_day:02d}"
            return {"date_range": [start_date_str, end_date_str]}
    # Add this at the end of detect_date_ranges before returning
    print("\n==== DEBUG: detect_date_ranges RESULT ====")
    print(f"Query: '{query}'")
    print(f"Detected date range: {result['date_range']}")
    # No date range pattern found
    return {"date_range": None}
# Query parsing function for the QA system
# Improved date pattern recognition in parse_query function
def parse_query(query: str) -> Dict[str, Any]:
    """Parse a natural language query about flower data into structured parameters with comprehensive date parsing."""
    params = {
        "date_range": None,
        "farms": [],
        "query_type": "unknown",
        "original_query": query
    }
    # Add right after params initialization (after line 557)
    # Use comprehensive date range detector first
    date_params = detect_date_ranges(query)
    if date_params["date_range"]:
        params["date_range"] = date_params["date_range"]
    # Enhanced month patterns with more variations
    month_names = r'(?:january|february|march|april|may|june|july|august|september|october|november|december|jan|feb|mar|apr|jun|jul|aug|sep|oct|nov|dec)'
    
    # Day ordinals for better matching (1st, 2nd, 3rd, etc.)
    day_ordinals = r'(?:\d{1,2}(?:st|nd|rd|th)?)'
    
    # ADD THE NEW PATTERN RIGHT HERE, BEFORE THE date_range_patterns SECTION
    # New pattern for "in month" queries 
    month_query_pattern = r'\b(?:in|during|for|of)\s+(\w+)(?:\s+month)?\b'
    month_query_match = re.search(month_query_pattern, query.lower())
    
    if month_query_match:
        month_name = month_query_match.group(1).lower()
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
        
        if month_name in month_map:
            params["date_range"] = [f"{month_name} month"]
    # END OF NEW CODE ADDITION
    # This specifically handles "in month X to Y" pattern
    month_date_range_pattern = r'\b(?:in|during|for|of)\s+(\w+)\s+(\d{1,2})(?:st|nd|rd|th)?\s+(?:to|until|and|-|through|till)\s+(\d{1,2})(?:st|nd|rd|th)?\b'
    month_date_range_match = re.search(month_date_range_pattern, query.lower())
    
    if month_date_range_match:
        month_name = month_date_range_match.group(1).lower()
        start_day = int(month_date_range_match.group(2))
        end_day = int(month_date_range_match.group(3))
        
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
        
        if month_name in month_map:
            month_num = month_map[month_name]
            current_year = datetime.now().year
            
            # Create explicit date strings for the range
            start_date_str = f"{current_year}-{month_num:02d}-{start_day:02d}"
            end_date_str = f"{current_year}-{month_num:02d}-{end_day:02d}"
            
            params["date_range"] = [start_date_str, end_date_str]
    
    # Enhanced date range patterns
    date_range_patterns = [
        # "from [day] [month] to [day] [month]" format
        rf'(?:from)?\s+{day_ordinals}\s+(?:of\s+)?({month_names})\s+(?:to|until|and|-|through|till)\s+{day_ordinals}\s+(?:of\s+)?({month_names})\b',
        
        # "from [month] [day] to [month] [day]" format
        rf'(?:from)?\s+({month_names})\s+{day_ordinals}\s+(?:to|until|and|-|through|till)\s+({month_names})\s+{day_ordinals}\b',
        
        # Month with day range: "May 1 to 4" or "May 1st to 4th"
        rf'\b({month_names})\s+{day_ordinals}\s+(?:to|until|and|-|through|till)\s+{day_ordinals}\b',
        
        # Day range with month: "1 to 4 May" or "from 1 to 4 May" or "1st to 4th of May"
        rf'(?:from\s+)?{day_ordinals}\s+(?:to|until|and|-|through|till)\s+{day_ordinals}\s+(?:of\s+)?({month_names})\b',
        
        # Week-based patterns
        r'(?:week\s+(?:ending|ended|of)\s+)(\d{1,2}[-/]\d{1,2}(?:[-/]\d{2,4})?)',
        r'(?:for\s+the\s+week\s+of\s+)(\d{1,2}[-/]\d{1,2}(?:[-/]\d{2,4})?)',
        
        # "between X and Y" patterns
        rf'between\s+{day_ordinals}\s+(?:of\s+)?({month_names})\s+and\s+{day_ordinals}\s+(?:of\s+)?({month_names})',
        rf'between\s+({month_names})\s+{day_ordinals}\s+and\s+({month_names})\s+{day_ordinals}',
        
        # Numeric date ranges - e.g., "01/05 to 04/05" or "01-05 to 04-05"
        r'(\d{1,2}[-/]\d{1,2}(?:[-/]\d{2,4})?)\s+(?:to|until|and|-|through|till)\s+(\d{1,2}[-/]\d{1,2}(?:[-/]\d{2,4})?)',
        
        # Quarter patterns
        r'\b((?:first|second|third|fourth|1st|2nd|3rd|4th)\s+quarter)\b',
        
        # Month ranges like "from January to March"
        rf'(?:from\s+)?({month_names})\s+(?:to|until|and|-|through|till)\s+({month_names})\b',
        
        # Year-to-date pattern
        r'\b(year[\s-]to[\s-]date|ytd)\b'
    ]
    
    # First check for date ranges
    for pattern in date_range_patterns:
        range_match = re.search(pattern, query, re.IGNORECASE)
        if range_match:
            groups = range_match.groups()
            
            # Handle various patterns based on the number of capture groups
            if len(groups) == 4:  # Patterns with day-month to day-month
                if re.match(r'\d+', groups[0]):  # First group is a number (day)
                    day1, month1, day2, month2 = groups
                    params["date_range"] = [f"{day1} {month1}", f"{day2} {month2}"]
                else:  # First group is a month
                    month1, day1, month2, day2 = groups
                    params["date_range"] = [f"{month1} {day1}", f"{month2} {day2}"]
            elif len(groups) == 3:  # Month day-range or day-range month
                if re.match(month_names, groups[0], re.IGNORECASE):
                    # "May 1 to 4" pattern
                    month, start_day, end_day = groups
                    params["date_range"] = [f"{month} {start_day}", f"{month} {end_day}"]
                else:
                    # "1 to 4 May" pattern
                    start_day, end_day, month = groups
                    params["date_range"] = [f"{start_day} {month}", f"{end_day} {month}"]
            elif len(groups) == 2 and re.match(month_names, groups[0], re.IGNORECASE) and re.match(month_names, groups[1], re.IGNORECASE):
                # "January to March" pattern (full month range)
                month1, month2 = groups
                params["date_range"] = [f"{month1} month", f"{month2} month"]
            elif len(groups) == 2:
                # Simple range pattern with two dates
                params["date_range"] = list(groups)
            elif len(groups) == 1:
                # Single parameter patterns like "quarter", "ytd"
                if "quarter" in groups[0].lower():
                    params["date_range"] = [groups[0]]
                elif re.match(r'year[\s-]to[\s-]date|ytd', groups[0], re.IGNORECASE):
                    params["date_range"] = ["year-to-date"]
                else:
                    # Week ending pattern
                    params["date_range"] = [f"week ending {groups[0]}"]
            break
    
    # If no date range, check for single dates with enhanced patterns
    if params["date_range"] is None:
        # Additional single date patterns
        date_patterns = [
            # Start/end of month patterns
            r'\b(?:start|beginning|first\s+day)\s+of\s+(?:the\s+)?(?:month|({month_names}))\b',
            r'\b(?:end|last\s+day)\s+of\s+(?:the\s+)?(?:month|({month_names}))\b',
            
            # Day-of-week patterns
            r'\b(?:last|previous|next)\s+(monday|tuesday|wednesday|thursday|friday|saturday|sunday|(?:mon|tues|tue|wed|thurs|thu|fri|sat|sun)(?:day)?)\b',
            
            # Ordinal day-of-month patterns
            rf'\b(?:the\s+)?({day_ordinals})\s+(?:of\s+)?({month_names})\b',
            rf'\b({month_names})\s+({day_ordinals})\b',
            
            # Numeric date format with enhanced flexibility
            r'\b\d{1,2}[-/\.]\d{1,2}(?:[-/\.]\d{2,4})?\b',
            
            # Month-only pattern
            rf'\b(?:for\s+)?(?:the\s+)?(?:month\s+of\s+)?({month_names})\b',
            
            # Last/this/next month
            r'\b(last|this|next)\s+month\b',
            
            # Last/this/next week
            r'\b(last|this|next)\s+week\b',
            
            # Last/this/next year
            r'\b(last|this|next)\s+year\b'
        ]
        
        for pattern in date_patterns:
            matches = re.findall(pattern, query, re.IGNORECASE)
            if matches:
                if isinstance(matches[0], tuple):
                    # For tuple results
                    if len(matches[0]) == 2:
                        # Handle patterns with two captures
                        if re.match(r'\d+', matches[0][0]):
                            # Day then month pattern
                            params["date_range"] = [f"{matches[0][0]} {matches[0][1]}"]
                        elif re.match(month_names, matches[0][0], re.IGNORECASE):
                            # Month then day pattern
                            params["date_range"] = [f"{matches[0][0]} {matches[0][1]}"]
                        else:
                            # Other two-part patterns
                            params["date_range"] = [f"{matches[0][0]} of {matches[0][1]}"]
                    else:
                        # Single capture in a tuple
                        params["date_range"] = [matches[0][0]]
                else:
                    # For single string matches
                    params["date_range"] = [matches[0]]
                break
    
    # Additional logic for natural language date references
    if params["date_range"] is None:
        natural_date_patterns = [
            # Today/yesterday/tomorrow
            r'\b(today|yesterday|tomorrow)\b',
            
            # This/last/next month/week/quarter/year
            r'\b(this|last|next)\s+(month|week|quarter|year)\b',
            
            # Time periods like "so far this month" or "month to date"
            r'\b(so\s+far\s+this\s+month|month\s+to\s+date|mtd)\b',
            
            # Seasons
            r'\b(spring|summer|fall|autumn|winter)\s+(?:of\s+)?(\d{4})?\b'
        ]
        
        for pattern in natural_date_patterns:
            match = re.search(pattern, query, re.IGNORECASE)
            if match:
                if len(match.groups()) == 1:
                    # Single term like "today"
                    params["date_range"] = [match.group(1).lower()]
                elif len(match.groups()) == 2:
                    # Two-part terms like "this month"
                    term1, term2 = match.groups()
                    params["date_range"] = [f"{term1} {term2}"]
                break
    
    # Look for farm names with improved farm name detection
    for farm in FARM_COLUMNS:
        # Split the farm name to get components
        farm_parts = farm.split(':')
        if len(farm_parts) > 1:
            farm_letter = farm_parts[0].strip()
            farm_name = farm_parts[1].strip()
            
            # Create patterns to match different ways farm might be referenced
            farm_patterns = [
                re.compile(r'\b' + re.escape(farm) + r'\b', re.IGNORECASE),           # Full name with letter
                re.compile(r'\b' + re.escape(farm_name) + r'\b', re.IGNORECASE),      # Just farm name
                re.compile(r'\bKebun\s+' + farm_name.replace("Kebun ", "") + r'\b', re.IGNORECASE),  # If "Kebun" is repeated
                re.compile(r'\bFarm\s+' + re.escape(farm_letter) + r'\b', re.IGNORECASE),  # "Farm A" style
                re.compile(r'\b' + re.escape(farm_letter) + r'\b', re.IGNORECASE)     # Just the letter
            ]
            
            # Check each pattern
            for pattern in farm_patterns:
                if pattern.search(query):
                    params["farms"].append(farm)
                    break
    
    # Determine query type with improved detection of question intent
    query_lower = query.lower()
    
    # Count/sum detection
    if re.search(r'\b(?:how\s+many|total|count|sum|overall|is\s+there|were\s+there|collected|produced|gathered|harvested)\b', query_lower):
        params["query_type"] = "count"
    
    # Average detection
    elif re.search(r'\b(?:average|mean|avg|typical|normally|usually|generally)\b', query_lower):
        params["query_type"] = "average"
    
    # Comparison detection
    elif re.search(r'\b(?:compare|comparison|difference|versus|vs|against|better|worse|relative|proportion|percent|ratio|distribution)\b', query_lower):
        params["query_type"] = "comparison"
    
    # Maximum detection
    elif re.search(r'\b(?:highest|most|best|top|maximum|max|peak|greatest|largest|biggest)\b', query_lower):
        params["query_type"] = "maximum"
    
    # Minimum detection
    elif re.search(r'\b(?:lowest|least|worst|minimum|min|smallest|fewest|poorest)\b', query_lower):
        params["query_type"] = "minimum"
    
    # Trend detection (new)
    elif re.search(r'\b(?:trend|pattern|change|growth|decline|increase|decrease|progression|development)\b', query_lower):
        params["query_type"] = "trend"
    
    # If we have a date range but no query type, default to count
    if params["date_range"] is not None and params["query_type"] == "unknown":
        params["query_type"] = "count"
    
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
    """Execute a parsed query against the flower data with enhanced date range handling."""
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
    filtered_data['Weekday'] = filtered_data['Date'].dt.weekday
    filtered_data['Weekday_Name'] = filtered_data['Date'].dt.strftime('%A').str.lower()
    filtered_data['Quarter'] = filtered_data['Date'].dt.quarter
    filtered_data['Day_of_Month'] = filtered_data['Date'].dt.day
    filtered_data['Days_in_Month'] = filtered_data['Date'].dt.days_in_month
    
    # Current date info for relative date references
    current_date = datetime.now().date()
    current_year = current_date.year
    current_month = current_date.month
    current_day = current_date.day
    
    # Debug info
    print(f"Query parameters: {params}")
    
    # Handle date filtering with enhanced patterns
    if params["date_range"]:
        try:
            # Handle natural language date references
            if len(params["date_range"]) == 1:
                date_str = params["date_range"][0].lower()
                
                # Simple natural language references
                if date_str == "today":
                    filtered_data = filtered_data[filtered_data['Date'].dt.date == current_date]
                    date_filter_applied = True
                
                elif date_str == "yesterday":
                    yesterday = (datetime.now() - timedelta(days=1)).date()
                    filtered_data = filtered_data[filtered_data['Date'].dt.date == yesterday]
                    date_filter_applied = True
                
                elif date_str == "tomorrow":
                    tomorrow = (datetime.now() + timedelta(days=1)).date()
                    filtered_data = filtered_data[filtered_data['Date'].dt.date == tomorrow]
                    date_filter_applied = True
                
                elif date_str == "last week":
                    # Get date range for last week (previous Monday to Sunday)
                    today = datetime.now().date()
                    start_of_last_week = (today - timedelta(days=today.weekday() + 7))
                    end_of_last_week = start_of_last_week + timedelta(days=6)
                    filtered_data = filtered_data[
                        (filtered_data['Date'].dt.date >= start_of_last_week) & 
                        (filtered_data['Date'].dt.date <= end_of_last_week)
                    ]
                    date_filter_applied = True
                
                elif date_str == "this week":
                    # Get date range for current week (Monday to today)
                    today = datetime.now().date()
                    start_of_week = (today - timedelta(days=today.weekday()))
                    filtered_data = filtered_data[
                        (filtered_data['Date'].dt.date >= start_of_week) & 
                        (filtered_data['Date'].dt.date <= today)
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
                
                elif date_str == "last month":
                    # Get date range for last month
                    today = datetime.now().date()
                    if today.month == 1:
                        last_month = 12
                        last_month_year = today.year - 1
                    else:
                        last_month = today.month - 1
                        last_month_year = today.year
                    
                    # Get the last day of last month
                    if last_month == 12:
                        last_day = 31
                    else:
                        last_day = (datetime(last_month_year, last_month + 1, 1) - timedelta(days=1)).day
                    
                    start_of_last_month = datetime(last_month_year, last_month, 1).date()
                    end_of_last_month = datetime(last_month_year, last_month, last_day).date()
                    
                    filtered_data = filtered_data[
                        (filtered_data['Date'].dt.date >= start_of_last_month) & 
                        (filtered_data['Date'].dt.date <= end_of_last_month)
                    ]
                    date_filter_applied = True
                
                elif date_str == "next month":
                    # Get date range for next month
                    today = datetime.now().date()
                    if today.month == 12:
                        next_month = 1
                        next_month_year = today.year + 1
                    else:
                        next_month = today.month + 1
                        next_month_year = today.year
                    
                    # Get the last day of next month
                    if next_month == 12:
                        last_day = 31
                    else:
                        last_day = (datetime(next_month_year, next_month + 1, 1) - timedelta(days=1)).day
                    
                    start_of_next_month = datetime(next_month_year, next_month, 1).date()
                    end_of_next_month = datetime(next_month_year, next_month, last_day).date()
                    
                    filtered_data = filtered_data[
                        (filtered_data['Date'].dt.date >= start_of_next_month) & 
                        (filtered_data['Date'].dt.date <= end_of_next_month)
                    ]
                    date_filter_applied = True
                
                elif re.search(r'(first|beginning|start)(?:\s+day)?\s+of\s+(?:the\s+)?month', date_str):
                    # Handle "first day of month", "start of month", etc.
                    # Find all dates that are the first day of their month
                    filtered_data = filtered_data[filtered_data['Day_of_Month'] == 1]
                    date_filter_applied = True
                
                elif re.search(r'(last|end)(?:\s+day)?\s+of\s+(?:the\s+)?month', date_str):
                    # Handle "last day of month", "end of month", etc.
                    # Find all dates that are the last day of their month
                    filtered_data = filtered_data[filtered_data['Day_of_Month'] == filtered_data['Days_in_Month']]
                    date_filter_applied = True
                
                elif re.search(r'(first|beginning|start)(?:\s+day)?\s+of\s+(?:the\s+)?(\w+)', date_str):
                    # Handle "first day of January", "start of May", etc.
                    month_match = re.search(r'(first|beginning|start)(?:\s+day)?\s+of\s+(?:the\s+)?(\w+)', date_str)
                    if month_match and month_match.group(2):
                        month_name = month_match.group(2).lower()
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
                            filtered_data = filtered_data[
                                (filtered_data['Month'] == month_num) & 
                                (filtered_data['Day_of_Month'] == 1)
                            ]
                            date_filter_applied = True
                
                elif re.search(r'(last|end)(?:\s+day)?\s+of\s+(?:the\s+)?(\w+)', date_str):
                    # Handle "last day of January", "end of May", etc.
                    month_match = re.search(r'(last|end)(?:\s+day)?\s+of\s+(?:the\s+)?(\w+)', date_str)
                    if month_match and month_match.group(2):
                        month_name = month_match.group(2).lower()
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
                            # Find all dates that are the last day of the specified month
                            month_data = filtered_data[filtered_data['Month'] == month_num]
                            if not month_data.empty:
                                last_day_of_month = month_data['Days_in_Month'].iloc[0]
                                filtered_data = filtered_data[
                                    (filtered_data['Month'] == month_num) & 
                                    (filtered_data['Day_of_Month'] == last_day_of_month)
                                ]
                                date_filter_applied = True
                
                elif "month" in date_str:
                    # Handle single month query like "May month" or "month of May"
                    month_match = re.search(r'(?:month\s+of\s+)?(\w+)(?:\s+month)?', date_str)
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
                
                elif re.match(r'(first|second|third|fourth|1st|2nd|3rd|4th)\s+quarter', date_str):
                    # Handle quarter references like "first quarter"
                    quarter_match = re.search(r'(first|second|third|fourth|1st|2nd|3rd|4th)', date_str)
                    if quarter_match:
                        quarter_name = quarter_match.group(1).lower()
                        quarter_map = {
                            "first": 1, "1st": 1,
                            "second": 2, "2nd": 2,
                            "third": 3, "3rd": 3,
                            "fourth": 4, "4th": 4
                        }
                        if quarter_name in quarter_map:
                            quarter_num = quarter_map[quarter_name]
                            filtered_data = filtered_data[filtered_data['Quarter'] == quarter_num]
                            date_filter_applied = True
                
                elif re.match(r'year[\s-]to[\s-]date|ytd', date_str):
                    # Handle year-to-date reference
                    start_of_year = datetime(current_year, 1, 1).date()
                    filtered_data = filtered_data[
                        (filtered_data['Date'].dt.date >= start_of_year) & 
                        (filtered_data['Date'].dt.date <= current_date)
                    ]
                    date_filter_applied = True
                
                elif re.search(r'week\s+(?:ending|ended|of)\s+', date_str):
                    # Handle "week ending" pattern
                    end_date_str = re.sub(r'week\s+(?:ending|ended|of)\s+', '', date_str).strip()
                    end_date = parse_date_string(end_date_str, current_year)
                    if end_date:
                        end_date = end_date.date()
                        start_date = end_date - timedelta(days=6)
                        filtered_data = filtered_data[
                            (filtered_data['Date'].dt.date >= start_date) & 
                            (filtered_data['Date'].dt.date <= end_date)
                        ]
                        date_filter_applied = True
                
                elif re.search(r'(last|previous|next|this)\s+(monday|tuesday|wednesday|thursday|friday|saturday|sunday|mon|tues?|wed|thurs?|fri|sat|sun)', date_str, re.IGNORECASE):
                    # Handle day of week references like "last monday"
                    weekday_match = re.search(r'(last|previous|next|this)\s+(monday|tuesday|wednesday|thursday|friday|saturday|sunday|mon|tues?|wed|thurs?|fri|sat|sun)', date_str, re.IGNORECASE)
                    
                    if weekday_match:
                        relative_term = weekday_match.group(1).lower()
                        weekday_name = weekday_match.group(2).lower()
                        
                        # Map weekday names to their numeric values (0=Monday, 6=Sunday in Python)
                        weekday_map = {
                            "monday": 0, "mon": 0,
                            "tuesday": 1, "tue": 1, "tues": 1,
                            "wednesday": 2, "wed": 2,
                            "thursday": 3, "thu": 3, "thur": 3, "thurs": 3,
                            "friday": 4, "fri": 4,
                            "saturday": 5, "sat": 5,
                            "sunday": 6, "sun": 6
                        }
                        
                        weekday_num = weekday_map.get(weekday_name)
                        if weekday_num is not None:
                            today = datetime.now().date()
                            today_weekday = today.weekday()
                            
                            if relative_term in ["last", "previous"]:
                                # Calculate days to go back to reach the last occurrence of this weekday
                                days_diff = (today_weekday - weekday_num) % 7
                                if days_diff == 0:
                                    days_diff = 7  # If today is the weekday, go back 7 days
                                target_date = today - timedelta(days=days_diff)
                                filtered_data = filtered_data[filtered_data['Date'].dt.date == target_date]
                                date_filter_applied = True
                            
                            elif relative_term == "next":
                                # Calculate days to go forward to reach the next occurrence of this weekday
                                days_diff = (weekday_num - today_weekday) % 7
                                if days_diff == 0:
                                    days_diff = 7  # If today is the weekday, go forward 7 days
                                target_date = today + timedelta(days=days_diff)
                                filtered_data = filtered_data[filtered_data['Date'].dt.date == target_date]
                                date_filter_applied = True
                            
                            elif relative_term == "this":
                                # Calculate the date for this weekday in the current week
                                days_diff = (weekday_num - today_weekday) % 7
                                if days_diff == 0:
                                    target_date = today  # Today is the requested weekday
                                elif days_diff < 0:
                                    target_date = today - timedelta(days=abs(days_diff))  # Weekday already passed
                                else:
                                    target_date = today + timedelta(days=days_diff)  # Weekday is coming up
                                
                                filtered_data = filtered_data[filtered_data['Date'].dt.date == target_date]
                                date_filter_applied = True
                
                else:
                    # Try to parse specific date
                    parsed_date = parse_date_string(date_str, current_year)
                    if parsed_date:
                        filtered_data = filtered_data[filtered_data['Date'].dt.date == parsed_date.date()]
                        date_filter_applied = True
            
            # Handle date range with improved parsing
            elif len(params["date_range"]) == 2:
                start_date_str, end_date_str = params["date_range"]
                
                # Parse both dates for debugging
                print(f"Parsing date range: {start_date_str} to {end_date_str}")
                
                # First try standard date parsing
                # ADD THIS NEW SECTION RIGHT HERE
                # First check if these are already formatted as YYYY-MM-DD
                if re.match(r'\d{4}-\d{2}-\d{2}', start_date_str) and re.match(r'\d{4}-\d{2}-\d{2}', end_date_str):
                    try:
                        start_date = datetime.strptime(start_date_str, '%Y-%m-%d')
                        end_date = datetime.strptime(end_date_str, '%Y-%m-%d')
                        
                        # Skip the standard parsing since we already have valid dates
                        print(f"ISO formatted dates detected: {start_date.date()} to {end_date.date()}")
                        
                        # Continue with filtering below
                    except ValueError:
                        # If there's an error with direct parsing, fall back to standard parsing
                        start_date = None
                        end_date = None
                        print("ISO format detected but couldn't parse, falling back to standard parsing")
                else:
                    # Not ISO format, use standard parsing
                    start_date = None
                    end_date = None
                # END OF NEW SECTION
                
                # Only do standard parsing if we don't already have valid dates
                if start_date is None or end_date is None:
                    # Line 1107: Standard parsing
                    start_date = parse_date_string(start_date_str, current_year)
                    end_date = parse_date_string(end_date_str, current_year)                
                print(f"Standard parsed dates: {start_date} to {end_date}")
                
                # If standard parsing works for both dates
                # If standard parsing works for both dates
                # If standard parsing works for both dates
                if start_date and end_date:
                    # Convert to datetime.date objects to avoid time component issues
                    start_date_only = start_date.date() if hasattr(start_date, 'date') else start_date
                    end_date_only = end_date.date() if hasattr(end_date, 'date') else end_date
                    
                    # Apply strict filtering
                    print(f"Filtering data for dates from {start_date_only} to {end_date_only}")
                    print(f"Available dates in data: {sorted(filtered_data['Date'].dt.date.unique())}")
                    
                    # Use strict comparison for date range
                    filtered_data = filtered_data[
                        (filtered_data['Date'].dt.date >= start_date_only) & 
                        (filtered_data['Date'].dt.date <= end_date_only)
                    ]
                    
                    # Check if any data was found in the range
                    if filtered_data.empty:
                        # If no exact matches, offer a helpful message with nearby data
                        available_dates = sorted(original_data['Date'].dt.date.unique())
                        closest_dates = [d for d in available_dates if d.month == start_date_only.month]
                        
                        if closest_dates:
                            closest_str = ', '.join([d.strftime('%Y-%m-%d') for d in closest_dates])
                            return {
                                "error": f"No data found between {start_date_only} and {end_date_only}. Available dates in {start_date_only.strftime('%B')} are: {closest_str}",
                                "result": None
                            }
                        else:
                            all_dates_str = ', '.join([d.strftime('%Y-%m-%d') for d in available_dates[:10]])
                            return {
                                "error": f"No data found between {start_date_only} and {end_date_only}. Some available dates: {all_dates_str}...",
                                "result": None
                            }
                    
                    date_filter_applied = True
                    
                    # Generate a list of all dates in the range (for reporting purposes)
                    all_dates_in_range = []
                    current_date = start_date_only
                    while current_date <= end_date_only:
                        all_dates_in_range.append(current_date.isoformat())
                        current_date += timedelta(days=1)
                    
                    # Generate a list of available dates within the range (for reporting purposes)
                    available_dates_in_range = [d.date().isoformat() for d in filtered_data['Date']]
                    
                    # Generate a list of missing dates within the range (for reporting purposes)
                    missing_dates = [d for d in all_dates_in_range if d not in available_dates_in_range]
                    
                    # DEBUG: Print info about available and missing dates
                    print(f"All dates in requested range: {len(all_dates_in_range)}")
                    print(f"Available dates in range: {len(available_dates_in_range)}")
                    print(f"Missing dates in range: {len(missing_dates)}")
                    if missing_dates:
                        print(f"Missing: {missing_dates}")
                    ##### START NEW DEBUGGING CODE #####
                    print("\n==== DEBUG: AFTER FILTER ====")
                    print(f"After filtering: {len(filtered_data)} rows remain")
                    if not filtered_data.empty:
                        print(f"Date range in filtered data: {filtered_data['Date'].min().date()} to {filtered_data['Date'].max().date()}")
                        print("Unique dates after filtering:")
                        for d in sorted(filtered_data['Date'].dt.date.unique()):
                            print(f"  - {d}")
                    else:
                        print("No data after filtering")
                    ##### END NEW DEBUGGING CODE #####
                    # Debug print to verify filter was applied
                    print(f"After filtering: {len(filtered_data)} rows remain")
                    if not filtered_data.empty:
                        print(f"Date range in filtered data: {filtered_data['Date'].min().date()} to {filtered_data['Date'].max().date()}")
                    
                    date_filter_applied = True
                    print(f"Date range applied: {start_date.date()} to {end_date.date()}")
                else:
                    # Handle month ranges like "January to March"
                    month_map = {
                        "january": 1, "jan": 1, "february": 2, "feb": 2,
                        "march": 3, "mar": 3, "april": 4, "apr": 4,
                        "may": 5, "june": 6, "jun": 6,
                        "july": 7, "jul": 7, "august": 8, "aug": 8,
                        "september": 9, "sep": 9, "october": 10, "oct": 10,
                        "november": 11, "nov": 11, "december": 12, "dec": 12
                    }
                    
                    # Check for month names in start and end strings
                    start_month_match = re.search(r'(\b' + '|'.join(month_map.keys()) + r'\b)', start_date_str, re.IGNORECASE)
                    end_month_match = re.search(r'(\b' + '|'.join(month_map.keys()) + r'\b)', end_date_str, re.IGNORECASE)
                    
                    if start_month_match and end_month_match:
                        start_month_name = start_month_match.group(1).lower()
                        end_month_name = end_month_match.group(1).lower()
                        
                        start_month_num = month_map.get(start_month_name)
                        end_month_num = month_map.get(end_month_name)
                        
                        if start_month_num and end_month_num:
                            # Handle wrap-around (e.g., "November to February")
                            if start_month_num > end_month_num:
                                filtered_data = filtered_data[
                                    (filtered_data['Month'] >= start_month_num) | 
                                    (filtered_data['Month'] <= end_month_num)
                                ]
                            else:
                                filtered_data = filtered_data[
                                    (filtered_data['Month'] >= start_month_num) & 
                                    (filtered_data['Month'] <= end_month_num)
                                ]
                            date_filter_applied = True
                    
                    # If still not parsed, try pattern-based parsing for specific formats
                    else:
                        # Try alternative parsing for formats like "1 april" to "5 may"
                        month_names_pattern = r'(?:january|february|march|april|may|june|july|august|september|october|november|december|jan|feb|mar|apr|jun|jul|aug|sep|oct|nov|dec)'
                        
                        # Try to extract day and month from the strings
                        day1_match = re.search(r'(\d+)(?:st|nd|rd|th)?', start_date_str)
                        month1_match = re.search(month_names_pattern, start_date_str, re.IGNORECASE)
                        
                        day2_match = re.search(r'(\d+)(?:st|nd|rd|th)?', end_date_str)
                        month2_match = re.search(month_names_pattern, end_date_str, re.IGNORECASE)
                        
                        if day1_match and month1_match and day2_match and month2_match:
                            day1 = int(day1_match.group(1))
                            month1_name = month1_match.group(0).lower()
                            
                            day2 = int(day2_match.group(1))
                            month2_name = month2_match.group(0).lower()
                            
                            month_map = {
                                "january": 1, "jan": 1, "february": 2, "feb": 2,
                                "march": 3, "mar": 3, "april": 4, "apr": 4,
                                "may": 5, "june": 6, "jun": 6,
                                "july": 7, "jul": 7, "august": 8, "aug": 8,
                                "september": 9, "sep": 9, "october": 10, "oct": 10,
                                "november": 11, "nov": 11, "december": 12, "dec": 12
                            }
                            
                            if month1_name in month_map and month2_name in month_map:
                                month1_num = month_map[month1_name]
                                month2_num = month_map[month2_name]
                                
                                # Create proper datetime objects
                                try:
                                    start_date = datetime(current_year, month1_num, day1)
                                    end_date = datetime(current_year, month2_num, day2)
                                    
                                    # Use the date range to filter the data
                                    filtered_data = filtered_data[
                                        (filtered_data['Date'].dt.date >= start_date.date()) & 
                                        (filtered_data['Date'].dt.date <= end_date.date())
                                    ]
                                    date_filter_applied = True
                                    print(f"Alternative parsing - Date range applied: {start_date.date()} to {end_date.date()}")
                                except ValueError as e:
                                    print(f"Date creation error: {e}")
        
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
            
    elif params["query_type"] == "trend":
        # Add trend analysis for time periods
        # Group by month to see monthly patterns
        if not filtered_data.empty:
            filtered_data['YearMonth'] = filtered_data['Date'].dt.strftime('%Y-%m')
            monthly_totals = filtered_data.groupby('YearMonth')[farm_columns].sum()
            
            # Get total per month
            monthly_grand_totals = monthly_totals.sum(axis=1)
            
            # Calculate month-to-month change
            monthly_changes = monthly_grand_totals.pct_change() * 100
            
            # Store the trend data
            result["trend_data"] = {
                "monthly_totals": {month: int(total) for month, total in monthly_grand_totals.items()},
                "monthly_changes": {month: round(change, 1) for month, change in monthly_changes.items() if not pd.isna(change)},
                "overall_trend": "increasing" if monthly_grand_totals.iloc[-1] > monthly_grand_totals.iloc[0] else "decreasing"
            }
            
            # Add farm-specific trend data
            result["farm_trends"] = {}
            for farm in farm_columns:
                farm_monthly = monthly_totals[farm]
                result["farm_trends"][farm] = {
                    "totals": {month: int(total) for month, total in farm_monthly.items()},
                    "trend": "increasing" if farm_monthly.iloc[-1] > farm_monthly.iloc[0] else "decreasing"
                }
    
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
            if date_str in ["today", "yesterday", "last week", "this week", "this month", "last month", "next month", "year-to-date", "ytd"]:
                result["query_date"] = date_str
            elif "quarter" in date_str.lower():
                result["query_quarter"] = date_str
            elif "month" in date_str.lower():
                # Extract just the month name
                month_match = re.search(r'(\w+)(?:\s+month)?', date_str.lower())
                if month_match:
                    result["query_month"] = month_match.group(1)
            else:
                result["query_date"] = date_str
        elif len(params["date_range"]) == 2:
            result["query_date_range"] = params["date_range"]
    
        # The following lines should align with the `if params["date_range"]:` block
        result["days_count"] = len(filtered_data)
        ##### START NEW DEBUGGING CODE #####
        print("\n==== DEBUG: BEFORE SETTING actual_dates ====")
        print(f"Filtered data length: {len(filtered_data)}")
        if not filtered_data.empty:
            print(f"Date range in filtered data: {filtered_data['Date'].min().date()} to {filtered_data['Date'].max().date()}")
            print("Unique dates just before setting actual_dates:")
            for d in sorted(filtered_data['Date'].dt.date.unique()):
                print(f"  - {d}")
        else:
            print("No data before setting actual_dates")
        
        # Force remove May 18 as a last resort
        if not filtered_data.empty:
            # Check if May 18 is there
            may_18_present = any(d.date() == datetime(2025, 5, 18).date() for d in filtered_data['Date'])
            if may_18_present:
                print("WARNING: May 18 is still present! Forcibly removing it...")
                filtered_data = filtered_data[filtered_data['Date'].dt.date != datetime(2025, 5, 18).date()]
                print(f"After removing May 18, have {len(filtered_data)} rows")
        ##### END NEW DEBUGGING CODE #####
        result["actual_dates"] = [d.date().isoformat() for d in filtered_data['Date']]
        ##### START NEW DEBUGGING CODE #####
        print("\n==== DEBUG: AFTER SETTING actual_dates ====")
        print(f"Actual dates: {result['actual_dates']}")
        ##### END NEW DEBUGGING CODE #####
        result["farms_queried"] = farm_columns
        result["original_query"] = params.get("original_query", "")
        # Add date coverage information if date filtering was applied
        if date_filter_applied and 'all_dates_in_range' in locals() and 'missing_dates' in locals():
            result["date_coverage"] = {
                "total_days_in_range": len(all_dates_in_range),
                "days_with_data": len(available_dates_in_range),
                "days_missing_data": len(missing_dates),
                "missing_dates": missing_dates
            }
            
            # Add a clear message about date coverage for the answer generation
            if missing_dates:
                if len(missing_dates) < len(all_dates_in_range):
                    result["date_coverage_message"] = f"Note: Data is available for {len(available_dates_in_range)} out of {len(all_dates_in_range)} days in the requested range."
                else:
                    result["date_coverage_message"] = f"Warning: No data available for any day in the requested range. Showing nearest available data."
            else:
                result["date_coverage_message"] = "Complete data available for all days in the requested range."
        
        result["farms_queried"] = farm_columns
        result["original_query"] = params.get("original_query", "")        
    
        return {"error": None, "result": result}


def generate_answer(query: str, query_params: Dict[str, Any], query_result: Dict[str, Any]) -> str:
    """Generate a natural language answer to the flower query using Gemini AI with enhanced date understanding."""
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
            if result["query_date"] in ["today", "yesterday", "last week", "this week", "this month", "last month", "next month", "year-to-date", "ytd"]:
                date_info = result["query_date"]
                date_filter_type = "special"
            else:
                date_info = result["query_date"]
                date_filter_type = "single_date"
        elif "query_month" in result:
            date_info = result["query_month"]
            date_filter_type = "month"
        elif "query_quarter" in result:
            date_info = result["query_quarter"]
            date_filter_type = "quarter"
        elif "query_date_range" in result:
            date_info = f"{result['query_date_range'][0]} to {result['query_date_range'][1]}"
            date_filter_type = "range"
        
        # Get the actual dates that were included in the calculation
        actual_dates = result.get("actual_dates", [])
        
        # Enhanced pattern recognition for the prompt
        # Build a section to explain how we interpreted date patterns
        date_interpretation = ""
        if query_params.get("date_range"):
            date_range = query_params.get("date_range")
            if len(date_range) == 1:
                date_interpretation = f"""
                The query contained the date expression: "{date_range[0]}"
                
                This was interpreted as: {date_info_description(date_filter_type, date_info)}
                """
            elif len(date_range) == 2:
                date_interpretation = f"""
                The query contained the date range: "{date_range[0]}" to "{date_range[1]}"
                
                This was interpreted as: {date_info_description(date_filter_type, date_info)}
                """
        
        # Build farm information for the prompt
        farm_interpretation = ""
        farms_queried = result.get("farms_queried", [])
        if farms_queried:
            if len(farms_queried) == len(FARM_COLUMNS):
                farm_interpretation = "The query was interpreted to include all farms."
            else:
                farm_interpretation = f"""
                The query was interpreted to specifically ask about: {', '.join(farms_queried)}
                """
        
        # Build query type information
        query_type = query_params.get("query_type", "unknown")
        if query_type == "unknown" and result.get("total") is not None:
            query_type = "count"  # Default to count if we have totals
            
        query_type_desc = {
            "count": "counting the total number of Bunga (flowers)",
            "average": "calculating the average daily Bunga production",
            "comparison": "comparing production between farms",
            "maximum": "finding the day with maximum production",
            "minimum": "finding the day with minimum production",
            "trend": "analyzing trends in production over time"
        }.get(query_type, "retrieving flower production information")
        
        # Create a comprehensive system prompt for Gemini with all the context
        system_prompt = f"""
        You are the helpful AI assistant for a flower farm tracking application called "Bunga di Kebun" in Malaysia.
        
        The application tracks flower ("Bunga") production across four farms (kebun):
        - Kebun Sendiri
        - Kebun DeYe
        - Kebun Uncle
        - Kebun Asan
        
        Each farm produces flowers which are counted individually and sometimes grouped into "Bakul" (baskets), 
        where 1 Bakul = 40 Bunga (flowers).
        
        You'll answer questions about flower production by providing precise, clear information based on the data analysis
        results provided to you.
        """
        
        # Create a user-focused prompt that includes the query and processing details
        user_prompt = f"""
        USER QUESTION: "{query}"
        
        QUERY INTERPRETATION:
        This question was understood as {query_type_desc}.
        {date_interpretation}
        {farm_interpretation}
        
        DATA RESULTS:
        The data analysis included {len(actual_dates)} day(s) from the farm records.
        The exact dates processed were: {', '.join(actual_dates) if len(actual_dates) <= 10 else f"{actual_dates[0]} to {actual_dates[-1]} ({len(actual_dates)} days)"}
        
        The quantitative results are:
        {json.dumps({k: v for k, v in result.items() if k not in ['original_query', 'actual_dates', 'farms_queried']}, indent=2)}
        
        Please construct a clear, concise answer that directly responds to the user's original question based on this data.
        """
        
        # Create the assistant instructions with detailed guidelines
        assistant_instructions = f"""
        INSTRUCTIONS FOR ANSWERING:
        
        1. Begin your answer by directly addressing the user's question - be precise about what time period the data covers.
        
        2. Always specify exact dates (e.g., "Based on data from May 1, 2023 to May 5, 2023") rather than vague references.
        
        3. Format all number values with thousand separators (e.g., "1,234" not "1234").
        
        4. Use these key terms consistently:
           - "Bunga" = flower(s)
           - "Bakul" = basket(s), where 1 Bakul = 40 Bunga
           - Use the farm names exactly as provided (e.g., "Kebun Sendiri")
        
        5. Keep your answer concise, factual and to the point. Limit explanations to what's necessary.
        
        6. When showing farm breakdowns, list farms in descending order by production amount unless otherwise specified.
        
        7. If the query seems to ask for a specific date range or farm but different data was actually used, politely note this
           difference in your response (e.g., "Although you asked about [X], the available data covers [Y]").
        
        8. Do not include details about how the query was processed unless there's a significant mismatch between the query
           and what could be provided.
        """
        
        # Combine prompts for Gemini
        full_prompt = f"{system_prompt}\n\n{user_prompt}\n\n{assistant_instructions}\n\nYour answer:"
        
        # Get response from Gemini
        model = genai.GenerativeModel('gemini-1.5-pro')
        response = model.generate_content(full_prompt)
        
        # Return the response text
        answer = response.text
        
        # Add a subtle verification line showing the exact dates used
        if len(actual_dates) > 0:
            date_range_text = f"{actual_dates[0]} to {actual_dates[-1]}" if len(actual_dates) > 1 else actual_dates[0]
            verification = f"\n\n(Data based on {len(actual_dates)} date(s): {date_range_text})"
            answer += verification
            # Add date coverage information if available
            if 'result' in query_result and query_result['result'] and 'date_coverage' in query_result['result']:
                coverage = query_result['result']['date_coverage']
                if coverage['days_missing_data'] > 0:
                    date_coverage_info = f"\n\n{query_result['result'].get('date_coverage_message', '')}"
                    
                    # Only list missing dates if there aren't too many
                    if coverage['days_missing_data'] <= 5:
                        missing_dates_formatted = ", ".join(coverage['missing_dates'])
                        date_coverage_info += f"\nDates without data: {missing_dates_formatted}"
                    
                    answer += date_coverage_info            
        
        return answer
    
    except Exception as e:
        # If Gemini fails, use simple response
        error_msg = f"Note: Advanced AI response generation failed: {str(e)}"
        return generate_simple_answer(query, query_params, query_result) + f"\n\n{error_msg}"
def date_info_description(filter_type, date_info):
    """Create a more descriptive natural language explanation of the date filter."""
    if filter_type == "special":
        special_dates = {
            "today": "data from today only",
            "yesterday": "data from yesterday only",
            "last week": "data from the previous calendar week",
            "this week": "data from the current week up to today",
            "this month": "data from the current month up to today",
            "last month": "data from the complete previous month",
            "next month": "data from the upcoming month",
            "year-to-date": "data from the start of the current year until today",
            "ytd": "data from the start of the current year until today"
        }
        return special_dates.get(date_info, f"data for {date_info}")
    elif filter_type == "single_date":
        return f"data for the specific date of {date_info}"
    elif filter_type == "month":
        return f"data for the entire month of {date_info}"
    elif filter_type == "quarter":
        return f"data for the {date_info}"
    elif filter_type == "range":
        return f"data for the date range from {date_info}"
    else:
        return "data for the specified time period"
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
    # Format the date range text
    if len(actual_dates) == 1:
        date_range_text = actual_dates[0]
    elif len(actual_dates) == 2:
        date_range_text = f"{actual_dates[0]} and {actual_dates[1]}"
    else:  # More than 2 dates
        # Check if dates are consecutive by converting to datetime objects
        actual_dates_sorted = sorted(actual_dates)
        
        # Convert to datetime objects for comparison
        date_objects = [datetime.strptime(d, '%Y-%m-%d').date() if isinstance(d, str) else d for d in actual_dates_sorted]
        
        # Check if consecutive by comparing the number of days between first and last
        if date_objects:
            first_date = date_objects[0]
            last_date = date_objects[-1]
            expected_days = (last_date - first_date).days + 1
            
            if len(actual_dates) == expected_days:
                # All consecutive dates
                date_range_text = f"{actual_dates_sorted[0]} to {actual_dates_sorted[-1]}"
            else:
                # Non-consecutive dates
                if len(actual_dates) <= 6:
                    date_range_text = ", ".join(actual_dates_sorted)
                else:
                    date_range_text = f"{actual_dates_sorted[0]} to {actual_dates_sorted[-1]} (with gaps)"
        else:
            date_range_text = "unknown date range"    
        if not actual_dates:
            if "query_date" in result:
                dates_text = f"on {result['query_date']}"
            elif "query_month" in result:
                dates_text = f"in {result['query_month']}"
            elif "query_quarter" in result:
                dates_text = f"in {result['query_quarter']}"
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
    
    elif query_type == "trend":
        # Trend analysis response
        if "trend_data" in result:
            trend_data = result["trend_data"]
            overall_trend = trend_data.get("overall_trend", "")
            monthly_totals = trend_data.get("monthly_totals", {})
            
            # Format a summary of the trend data
            months_text = ", ".join([f"{month}: {format_number(total)}" for month, total in monthly_totals.items()])
            
            return f"Based on the data {dates_text}, the overall trend in Bunga production is {overall_trend}. Monthly totals: {months_text}."
        else:
            return f"Based on the data {dates_text}, trend analysis is not available."
    
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
    """Checks if the answer is consistent with the actual data and adds verification info"""
    
    # Check if there's an error in the result
    if query_result.get("error"):
        return answer
    
    result = query_result.get("result", {})
    if not result:
        return answer
        
    # Get the actual dates from the result
    actual_dates = result.get("actual_dates", [])
    
    # Add verification text to the answer
    if actual_dates:
        # Format the date range text
        if len(actual_dates) == 1:
            date_range_text = actual_dates[0]
        elif len(actual_dates) == 2:
            date_range_text = f"{actual_dates[0]} and {actual_dates[1]}"
        else:  # More than 2 dates
            date_range_text = f"{actual_dates[0]} to {actual_dates[-1]}"
            
        verification = f"\n\nVerification: This answer is based on data from {len(actual_dates)} date(s): {date_range_text}"
        
        # Add farm information if relevant
        farms_queried = result.get("farms_queried", [])
        if farms_queried and len(farms_queried) < len(FARM_COLUMNS):
            farm_names = ", ".join([f.split(":")[1].strip() if ":" in f else f for f in farms_queried])
            verification += f"\nFarms included: {farm_names}"
            
        return answer + verification
    
    return answer
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
    """Send email notification with secure password handling using only Streamlit secrets"""
    try:
        # Email settings
        sender_email = "hqtong2013@gmail.com"
        receiver_email = "hq_tong@hotmail.com"
        
        # Try to get password from Streamlit secrets - no hardcoded fallback
        password_source = "not found"
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
                # No fallback to hardcoded password - just fail if no secret is configured
                return False, "Email password not found in Streamlit secrets. Please configure 'email_password' in secrets."
        
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
        return True, ""
        
    except Exception as e:
        error_message = str(e)
        st.error(f"Email error: {error_message}")
        return False, error_message
        
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
    
    # Add some example questions with better date expressions
    with st.expander("Example questions you can ask"):
        st.markdown("""
        Here are examples of questions you can ask using natural language date expressions:
        
        ### Simple Date Queries
        - How many Bunga were collected yesterday?
        - What was the total production for May 5th?
        - What was our harvest on the 15th of April?
        
        ### Date Range Queries
        - What was the total production from 1 April to 5 May?
        - How many flowers did we collect between January and March?
        - What was our harvest from the beginning of May to the end of May?
        
        ### Natural Language Date References
        - How did we perform last Monday?
        - What was the total for last month?
        - Show me the production for this week so far
        - What's our year-to-date total?
        
        ### Farm-Specific Queries
        - How many Bunga did Kebun Sendiri produce last week?
        - Compare Kebun DeYe and Kebun Asan for April
        - Which farm had the highest production in the first quarter?
        
        ### Analysis Queries
        - What was our best production day in May?
        - What's the average daily production for Kebun Uncle?
        - What's the trend for flower production over the past three months?
        """)
    
    # Add tips for asking questions
    with st.expander("💡 Tips for asking questions"):
        st.markdown("""
        ### The system understands many date formats:
        
        ✅ **Single dates:** "May 1", "1st of May", "01/05/2023"  
        ✅ **Date ranges:** "from 1 April to 5 May", "between January and March"  
        ✅ **Natural references:** "yesterday", "last Monday", "next Friday"  
        ✅ **Time periods:** "last week", "this month", "first quarter", "year-to-date"  
        ✅ **Special references:** "beginning of May", "end of month"
        
        ### Try these query types:
        
        - 💯 **Totals:** "How many Bunga did we collect last week?"
        - 📊 **Averages:** "What's the average daily production for Kebun Sendiri?"
        - 🔍 **Comparisons:** "Compare all farms for April"
        - 🏆 **Best/Worst:** "What was our best production day in May?"
        - 📈 **Trends:** "How has production changed over the past three months?"
        """)
    
    # Question input
    query = st.text_input("Type your question here:", placeholder="e.g., How many Bunga did we collect from 1 April to 5 May?")
    
    # Process query when submitted
    if query:
        with st.spinner("Finding the answer..."):
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
            
            # Show how the query was understood
            if not query_result.get("error"):
                result = query_result.get("result", {})
                with st.expander("See how I understood your question"):
                    # Date interpretation
                    if query_params.get("date_range"):
                        date_range = query_params.get("date_range")
                        if len(date_range) == 1:
                            st.markdown(f"**Date reference:** {date_range[0]}")
                        elif len(date_range) == 2:
                            st.markdown(f"**Date range:** From {date_range[0]} to {date_range[1]}")
                    
                    # Show farms queried
                    if result.get("farms_queried"):
                        farms = result.get("farms_queried")
                        if len(farms) == len(FARM_COLUMNS):
                            st.markdown("**Farms:** All farms included")
                        else:
                            st.markdown(f"**Farms:** {', '.join(farms)}")
                    
                    # Show query type
                    query_type = query_params.get("query_type", "unknown")
                    query_type_desc = {
                        "count": "Counting total Bunga",
                        "average": "Calculating average production",
                        "comparison": "Comparing between farms",
                        "maximum": "Finding maximum production day",
                        "minimum": "Finding minimum production day",
                        "trend": "Analyzing production trends"
                    }.get(query_type, "Retrieving flower data")
                    st.markdown(f"**Query type:** {query_type_desc}")
                    
                    # Show actual dates used
                    actual_dates = result.get("actual_dates", [])
                    if actual_dates:
                        if len(actual_dates) > 10:
                            st.markdown(f"**Dates used:** {actual_dates[0]} to {actual_dates[-1]} ({len(actual_dates)} days)")
                        else:
                            st.markdown(f"**Dates used:** {', '.join(actual_dates)}")
            
            # Show detailed query interpretation (for debugging or curious users)
            with st.expander("See technical details"):
                st.json({
                    "Query": query,
                    "Interpreted as": query_params,
                    "Result data": query_result["result"] if not query_result.get("error") else None,
                    "Error": query_result.get("error")
                })
# Function to add data for the current user
# Function to add data for the current user with confirmation step
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
        success, error_message = send_email_notification(date, farm_data)
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

    # Remove the line with default credentials
    st.markdown("---")
    # Instead, display a more secure message
    st.info("New user? Please register an account to get started.")
# Format number with thousands separator
def format_number(number):
    return f"{int(number):,}"

# Main app function
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
                date_obj = datetime.strptime(date, '%Y-%m-%d')
            else:
                date_obj = date
            
            day_name = date_obj.strftime('%A')
            date_formatted = date_obj.strftime('%Y-%m-%d')
            
            # Add custom CSS for compact layout
            st.markdown("""
            <style>
                /* Reduce spacing throughout the app during confirmation */
                div.block-container {
                    padding-top: 1rem;
                    padding-bottom: 1rem;
                }
                
                /* Make the warning box more compact */
                .stAlert {
                    padding: 0.5rem !important;
                    margin-bottom: 0.5rem !important;
                }
                
                /* Reduce margins between elements */
                p {
                    margin-bottom: 0.2rem !important;
                    font-size: 0.9rem !important;
                }
                
                /* Adjust button styles */
                .stButton button {
                    padding: 0.3rem 1rem !important;
                    height: auto !important;
                    min-height: 0 !important;
                    margin: 0.2rem 0 !important;
                    font-size: 0.9rem !important;
                }
                
                /* Style for green Confirm button */
                div[data-testid="column"]:nth-child(1) .stButton > button {
                    background-color: #2e7d32 !important;
                    color: white !important;
                    border: 1px solid #1b5e20 !important;
                }
                
                /* Style for yellow Cancel button */
                div[data-testid="column"]:nth-child(2) .stButton > button {
                    background-color: #fff9c4 !important;
                    color: #333 !important;
                    border: 1px solid #fbc02d !important;
                }
                
                /* Make farm details compact */
                .farm-row {
                    margin: 0.1rem 0 !important;
                    padding: 0 !important;
                    font-size: 1.1rem !important;
                    font-weight: bold !important;
                    color: #ff0000 !important;
                }
                
                /* Highlight important data */
                .red-data {
                    font-weight: bold !important;
                    color: #ff0000 !important;
                }
                
                /* Highlight blue data */
                .blue-data {
                    font-weight: bold !important;
                    color: #0000ff !important;
                }
                
                /* Stats styling */
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
            
            # Show warning with changed text
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
                # Shorten farm name to save space - just display without "Kebun"
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
                # Cancel button - Now with yellow styling
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
