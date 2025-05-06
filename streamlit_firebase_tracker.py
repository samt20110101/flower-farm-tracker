import google.generativeai as genai
import re
from typing import List, Dict, Any, Union, Optional

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
FARM_COLUMNS = ['Kebun Sendiri', 'Kebun DeYe', 'Kebun Uncle', 'Kebun Asan']
OLD_FARM_COLUMNS = ['Farm A', 'Farm B', 'Farm C', 'Farm D']

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
def parse_query(query: str) -> Dict[str, Any]:
    """Parse a natural language query about flower data into structured parameters."""
    params = {
        "date_range": None,
        "farms": [],
        "query_type": "unknown"
    }
    
    # Look for date patterns
    date_patterns = [
        # Single date pattern (e.g., "March 15, 2023", "2023-03-15", "15/03/2023")
        r'(\d{1,2}[-/]\d{1,2}[-/]\d{2,4}|\d{4}[-/]\d{1,2}[-/]\d{1,2}|\w+ \d{1,2},? \d{4})',
        # Date range pattern with "to", "until", "between", "-"
        r'(?:between|from)?\s*(\d{1,2}[-/]\d{1,2}[-/]\d{2,4}|\d{4}[-/]\d{1,2}[-/]\d{1,2}|\w+ \d{1,2},? \d{4})\s*(?:to|until|and|-)\s*(\d{1,2}[-/]\d{1,2}[-/]\d{2,4}|\d{4}[-/]\d{1,2}[-/]\d{1,2}|\w+ \d{1,2},? \d{4})'
    ]
    
    # Check for date ranges first
    range_match = re.search(date_patterns[1], query, re.IGNORECASE)
    if range_match:
        start_date, end_date = range_match.groups()
        params["date_range"] = [start_date, end_date]
    else:
        # Check for single dates
        single_date_matches = re.findall(date_patterns[0], query, re.IGNORECASE)
        if single_date_matches:
            params["date_range"] = [single_date_matches[0]]
    
    # Look for farm names
    for farm in FARM_COLUMNS:
        farm_pattern = re.compile(r'(?:' + re.escape(farm) + r'|' + farm.split()[1] + r')\b', re.IGNORECASE)
        if farm_pattern.search(query):
            params["farms"].append(farm)
    
    # Determine query type
    if re.search(r'\b(?:how\s+many|total|count|sum)\b', query, re.IGNORECASE):
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

# Execute flower data query and return results
def execute_query(params: Dict[str, Any], data: pd.DataFrame) -> Dict[str, Any]:
    """Execute a parsed query against the flower data."""
    if data.empty:
        return {"error": "No data available", "result": None}
    
    # Handle date filtering
    filtered_data = data.copy()
    
    if params["date_range"]:
        try:
            # Try to parse dates from strings
            if len(params["date_range"]) == 1:
                # Single date query
                date_str = params["date_range"][0]
                try:
                    # Try multiple date formats
                    for fmt in ['%d/%m/%Y', '%Y-%m-%d', '%d-%m-%Y', '%Y/%m/%d', '%B %d, %Y', '%B %d %Y']:
                        try:
                            query_date = pd.to_datetime(date_str, format=fmt)
                            break
                        except:
                            continue
                    
                    # Filter to exact date
                    filtered_data = filtered_data[filtered_data['Date'].dt.date == query_date.date()]
                except:
                    return {"error": f"Could not parse date: {date_str}", "result": None}
            
            elif len(params["date_range"]) == 2:
                # Date range query
                start_str, end_str = params["date_range"]
                try:
                    # Try multiple date formats for start date
                    for fmt in ['%d/%m/%Y', '%Y-%m-%d', '%d-%m-%Y', '%Y/%m/%d', '%B %d, %Y', '%B %d %Y']:
                        try:
                            start_date = pd.to_datetime(start_str, format=fmt)
                            break
                        except:
                            continue
                    
                    # Try multiple date formats for end date
                    for fmt in ['%d/%m/%Y', '%Y-%m-%d', '%d-%m-%Y', '%Y/%m/%d', '%B %d, %Y', '%B %d %Y']:
                        try:
                            end_date = pd.to_datetime(end_str, format=fmt)
                            break
                        except:
                            continue
                    
                    # Filter between dates
                    filtered_data = filtered_data[
                        (filtered_data['Date'].dt.date >= start_date.date()) & 
                        (filtered_data['Date'].dt.date <= end_date.date())
                    ]
                except:
                    return {"error": f"Could not parse date range: {start_str} to {end_str}", "result": None}
        except Exception as e:
            return {"error": f"Error processing date filter: {str(e)}", "result": None}
    
    # If no data after filtering by date
    if filtered_data.empty:
        return {"error": "No data found for the specified date(s)", "result": None}
    
    # Handle farm filtering
    farm_columns = FARM_COLUMNS
    if params["farms"]:
        farm_columns = [f for f in params["farms"] if f in FARM_COLUMNS]
    
    # If no valid farm columns specified, use all farms
    if not farm_columns:
        farm_columns = FARM_COLUMNS
    
    # Calculate results based on query type
    result = {}
    
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
    
    # Add date range information to result
    if params["date_range"]:
        if len(params["date_range"]) == 1:
            result["query_date"] = params["date_range"][0]
        elif len(params["date_range"]) == 2:
            result["query_date_range"] = params["date_range"]
    
    # Add number of days in filtered data
    result["days_count"] = len(filtered_data)
    
    # Check if we have farm information
    result["farms_queried"] = farm_columns
    
    return {"error": None, "result": result}
# Generate answer using Gemini AI
def generate_answer(query: str, query_params: Dict[str, Any], query_result: Dict[str, Any]) -> str:
    """Generate a natural language answer to the flower query using Gemini AI."""
    # Check if Gemini is available
    gemini_available = initialize_gemini()
    
    if not gemini_available:
        # Fallback to rule-based response generation
        return generate_simple_answer(query, query_params, query_result)
    
    try:
        # Prepare context for Gemini
        context = {
            "original_query": query,
            "query_parameters": query_params,
            "query_results": query_result,
            "farm_names": FARM_COLUMNS
        }
        
        # Convert context to JSON string
        context_json = json.dumps(context, default=str)
        
        # Prepare prompt for Gemini
        prompt = f"""
        You are a helpful assistant for a flower farm tracking application called "Bunga di Kebun" in Malaysia.
        
        The application tracks flower ("Bunga") production across four farms (kebun):
        - Kebun Sendiri
        - Kebun DeYe
        - Kebun Uncle
        - Kebun Asan
        
        A user has asked: "{query}"
        
        Here's the data extracted from their query and the results:
        ```json
        {context_json}
        ```
        
        Please provide a natural, conversational response to their question based on this data. 
        Important rules:
        1. Be precise with numbers and use thousand separators for readability
        2. If the query_result contains an "error" that's not null, explain the error in simple terms
        3. Use Malay words "Bunga" for flower and "Bakul" for basket in your response
        4. Remember that 40 Bunga = 1 Bakul
        5. Keep your answer concise but complete, focusing on directly answering the question
        6. Format numbers with thousand separators (e.g., "12,345" not "12345")
        7. Do not explain the technical query details unless asked
        8. Be conversational and helpful in your tone
        
        Answer:
        """
        
        # Get response from Gemini
        model = genai.GenerativeModel('gemini-1.5-pro')
        response = model.generate_content(prompt)
        
        # Return the response text
        return response.text
    
    except Exception as e:
        # If Gemini fails, use simple response
        return generate_simple_answer(query, query_params, query_result) + f"\n\nNote: Gemini AI response generation failed: {str(e)}"

# Generate simple rule-based answer as fallback
def generate_simple_answer(query: str, query_params: Dict[str, Any], query_result: Dict[str, Any]) -> str:
    """Generate a simple rule-based answer when Gemini is not available."""
    if query_result.get("error"):
        return f"Sorry, I couldn't answer that question: {query_result['error']}"
    
    result = query_result.get("result", {})
    if not result:
        return "Sorry, I couldn't find any relevant data to answer your question."
    
    # Generate answer based on query type
    query_type = query_params.get("query_type", "unknown")
    
    # Format date range for response
    date_info = ""
    if "query_date" in result:
        date_info = f"on {result['query_date']}"
    elif "query_date_range" in result:
        date_info = f"from {result['query_date_range'][0]} to {result['query_date_range'][1]}"
    
    farms_info = ", ".join(result.get("farms_queried", []))
    
    if query_type == "count":
        # Total count response
        if len(result.get("farms_queried", [])) == 1:
            # Single farm
            farm = result.get("farms_queried")[0]
            return f"The total number of Bunga from {farm} {date_info} is {format_number(result[farm])}. This is equivalent to {format_number(result.get('bakul', 0))} Bakul."
        else:
            # Multiple farms
            farm_details = ". ".join([f"{farm}: {format_number(result[farm])}" for farm in result.get("farms_queried", []) if farm in result])
            return f"The total Bunga {date_info} is {format_number(result.get('total', 0))}, which is {format_number(result.get('bakul', 0))} Bakul. Breakdown by farm: {farm_details}."
    
    elif query_type == "average":
        # Average response
        if len(result.get("farms_queried", [])) == 1:
            # Single farm
            farm = result.get("farms_queried")[0]
            return f"The average daily Bunga production for {farm} {date_info} is {format_number(result[farm])}."
        else:
            # Multiple farms
            farm_details = ". ".join([f"{farm}: {format_number(result[farm])}" for farm in result.get("farms_queried", []) if farm in result])
            return f"The average daily Bunga production {date_info} is {format_number(result.get('daily_average', 0))}. Breakdown by farm: {farm_details}."
    
    elif query_type == "comparison":
        # Comparison response
        farm_details = []
        percentages = result.get("percentages", {})
        
        for farm in result.get("farms_queried", []):
            if farm in result:
                percent = percentages.get(farm, 0) if percentages else 0
                farm_details.append(f"{farm}: {format_number(result[farm])} Bunga ({percent}%)")
                
        comparisons = ", ".join(farm_details)
        return f"Comparison of Bunga production {date_info}: {comparisons}"
    
    elif query_type == "maximum":
        # Maximum response
        max_date = result.get("max_date", "")
        max_total = result.get("max_total", 0)
        
        farm_details = ". ".join([f"{farm}: {format_number(result[farm])}" for farm in result.get("farms_queried", []) if farm in result])
        
        return f"The day with maximum Bunga production {date_info} was {max_date} with a total of {format_number(max_total)} Bunga. Breakdown by farm: {farm_details}."
    
    elif query_type == "minimum":
        # Minimum response
        min_date = result.get("min_date", "")
        min_total = result.get("min_total", 0)
        
        farm_details = ". ".join([f"{farm}: {format_number(result[farm])}" for farm in result.get("farms_queried", []) if farm in result])
        
        return f"The day with minimum Bunga production {date_info} was {min_date} with a total of {format_number(min_total)} Bunga. Breakdown by farm: {farm_details}."
    
    else:
        # Default response - just return totals
        if len(result.get("farms_queried", [])) == 1:
            # Single farm
            farm = result.get("farms_queried")[0]
            return f"The total Bunga from {farm} {date_info} is {format_number(result[farm])}. This is equivalent to approximately {format_number(result.get('bakul', 0))} Bakul."
        else:
            # Multiple farms
            farm_details = ". ".join([f"{farm}: {format_number(result[farm])}" for farm in result.get("farms_queried", []) if farm in result])
            return f"The total Bunga {date_info} is {format_number(result.get('total', 0))}, which is approximately {format_number(result.get('bakul', 0))} Bakul. Breakdown by farm: {farm_details}."

# Q&A tab function
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
            
            # Complete progress
            progress_bar.progress(100)
            
            # Display the answer
            st.markdown("### Answer")
            st.markdown(f"""
            <div style="background-color: #f0f7ff; padding: 15px; border-radius: 10px; margin-bottom: 20px;">
                <p style="font-size: 1.1em;">{answer}</p>
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
