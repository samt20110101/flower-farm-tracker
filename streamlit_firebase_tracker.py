import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import google.generativeai as genai
import re
from typing import List, Dict, Any, Union, Optional
from datetime import datetime, timedelta, timezone  # Add timezone here
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
    
    # Date range patterns - adding specific pattern for "from [day] [month] to [day] [month]" format
    date_range_patterns = [
        # NEW PATTERN: "from [day] [month] to [day] [month]" format (crucial for "from 1 april to 5 may")
        rf'(?:from)?\s*(\d{{1,2}})(?:st|nd|rd|th)?\s+({month_names})\s+(?:to|until|and|-)\s+(\d{{1,2}})(?:st|nd|rd|th)?\s+({month_names})\b',
        
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
def generate_gemini_response(query: str, df: pd.DataFrame) -> str:
    """
    Generate a natural language response to a query about flower data using the Gemini API.
    """
    if not genai.is_configured():
        st.error("Gemini API is not initialized. Please check your API key and network connection.")
        return "Gemini API error: Not initialized."

    # Parse the query to extract relevant information
    query_params = parse_query(query)

    # Prepare the prompt for Gemini with context and instructions
    prompt = f"""
    You are a helpful assistant providing information about flower data from different farms.
    Here is the data in a pandas DataFrame format:
    
    {df.head().to_markdown(index=False, numalign="left", stralign="left")}
    
    Important Instructions:
    -   The 'Date' column represents the date of the flower data.
    -   The other columns (e.g., 'A: Kebun Sendiri', 'B: Kebun DeYe', etc.) represent the number of flowers from different farms.
    -   When calculating totals, averages, or making comparisons, always consider the data within the given date range, if provided.
    -   If the query asks for a total or count, and a date range is provided, calculate the total or count only for that period.
    -   If the query asks for an average, and a date range is provided, calculate the average only for that period.
    -   If the query involves comparing farms, and a date range is provided, perform the comparison within that date range.
    -   If the query asks for the highest or lowest value within a date range, return only the highest and lowest value within the given date range.
    -   If no date range is provided, use all available data.
    -   If the query is ambiguous, ask for clarification.
    -   Do not make up data or extrapolate beyond the provided data.
    -   If a farm is not mentioned, provide the data from all farms
    
    
    Here are some examples of how to use the data:
    
    Example 1:
    Query: "How many flowers in total from 1 to 15 April?"
    
    Thought Process:
    1.  The query asks for the total number of flowers within a specific date range.
    2.  Extract the date range: "1 to 15 April".
    3.  Filter the data to include only the dates from '1 April' to '15 April'.
    4.  Calculate the sum of flowers from all farms ('A: Kebun Sendiri', 'B: Kebun DeYe', 'C: Kebun Asan', and 'D: Kebun Uncle') within the filtered date range.
    5.  Return the total number of flowers.
    
    
    Example 2:
    Query: "Average flowers in Farm A in May"
    
    Thought Process:
    1. The query asks for the average number of flowers for a specific farm within a specific date range
    2. Extract the date range: "May"
    3. Extract the farm name: "Farm A" which is "A: Kebun Sendiri"
    4. Filter the data to include only the dates in 'May'.
    5. Calculate the average number of flowers for 'A: Kebun Sendiri' within the filtered date range
    6. Return the average.
    
    
    Example 3:
    Query: "Compare the number of flowers between Farm A and Farm B"
    
    Thought process:
    1. The query asks for a comparison of the number of flowers between two specific farms.
    2. Extract the farm names: "Farm A" and "Farm B" which are "A: Kebun Sendiri" and "B: Kebun DeYe"
    3. Calculate the total number of flowers for "A: Kebun Sendiri"
    4. Calculate the total number of flowers for "B: Kebun DeYe"
    5. Compare the two totals and return the comparison.
    
    
    Now, answer the following query:
    
    Query: {query}
    """

    try:
        # Generate the response using Gemini
        model = genai.GenerativeModel('gemini-pro')
        response = model.generate_content(prompt)
        return response.text
    except Exception as e:
        st.error(f"Error generating response from Gemini: {e}")
        return f"Gemini API error: {e}"

def main():
    """Main function to run the Streamlit application."""
    initialize_app()
    if not initialize_gemini():
        st.stop()

    # Initialize session state
    if 'username' not in st.session_state:
        st.session_state.username = None
        st.session_state.role = None
        st.session_state.logged_in = False
        st.session_state.storage_mode = "Checking..." # Add storage mode to session state

    # --- Remainder of your Streamlit application code ---
    # Login section
    if not st.session_state.logged_in:
        st.title("Login")
        username = st.text_input("Username")
        password = st.text_input("Password", type="password")
        login_button = st.button("Log In")

        if login_button:
            role = verify_user(username, password)
            if role:
                st.session_state.logged_in = True
                st.session_state.username = username
                st.session_state.role = role
                st.success("Logged in successfully!")
                st.rerun()  # Rerun to show the main app
            else:
                st.error("Invalid username or password")
    else:
        # Main application
        st.title("Bunga di Kebun Dashboard")
        st.sidebar.title("Settings")

        # Load data
        df = load_data(st.session_state.username)

        # User role check for data input
        if st.session_state.role == "admin":
            st.subheader("Input Data Bunga")
            uploaded_file = st.file_uploader("Upload CSV File", type=["csv"])
            if uploaded_file is not None:
                try:
                    input_df = pd.read_csv(uploaded_file)
                    input_df['Date'] = pd.to_datetime(input_df['Date'])  # Ensure Date is datetime
                    
                    # Convert old farm column names to new ones if needed
                    for old_col, new_col in zip(OLD_FARM_COLUMNS, FARM_COLUMNS):
                        if old_col in input_df.columns and new_col not in input_df.columns:
                            input_df[new_col] = input_df[old_col]
                            input_df = input_df.drop(old_col, axis=1)
                    
                    # Ensure all farm columns exist
                    for col in FARM_COLUMNS:
                        if col not in input_df.columns:
                            input_df[col] = 0
                    
                    # Check for missing columns
                    required_columns = ['Date'] + FARM_COLUMNS
                    missing_columns = [col for col in required_columns if col not in input_df.columns]
                    if missing_columns:
                        st.error(f"Uploaded CSV is missing the following columns: {', '.join(missing_columns)}")
                    else:
                         # Merge the new data with the existing data
                        df = pd.concat([df, input_df[required_columns]], ignore_index=True)
                        save_data(df, st.session_state.username)
                        st.success("Data uploaded and saved successfully!")
                        st.rerun()
                except Exception as e:
                    st.error(f"Error reading or processing the uploaded file: {e}")
            
            date_input = st.date_input("Date", key="date_input")
            farm_inputs = {}
            for farm in FARM_COLUMNS:
                farm_inputs[farm] = st.number_input(farm, value=0, key=f"num_input_{farm}")
            
            save_button = st.button("Save Data")
            if save_button:
                new_data = pd.DataFrame([{
                    'Date': date_input,
                    **farm_inputs
                }])
                
                # Convert old farm column names to new ones if needed
                for old_col, new_col in zip(OLD_FARM_COLUMNS, FARM_COLUMNS):
                    if old_col in new_data.columns and new_col not in new_data.columns:
                        new_data[new_col] = new_data[old_col]
                        new_data = new_data.drop(old_col, axis=1)
                
                # Ensure all farm columns exist
                for col in FARM_COLUMNS:
                    if col not in new_data.columns:
                        new_data[col] = 0
                
                df = pd.concat([df, new_data], ignore_index=True)
                save_data(df, st.session_state.username)
                st.success("Data saved successfully!")
                st.rerun()
        
        # Display data
        st.subheader("Data Bunga")
        st.dataframe(df)
        
        # QA Section
        st.subheader("Ask a Question about the Flower Data")
        query = st.text_input("Enter your question:")
        if query:
            response = generate_gemini_response(query, df)
            st.write("Gemini's Response:", response)
        
        # Logout
        if st.button("Logout"):
            st.session_state.logged_in = False
            st.session_state.username = None
            st.session_state.role = None
            st.rerun()

if __name__ == "__main__":
    main()

