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
    page_title="Flower Farm Tracker",
    page_icon="🌷",
    layout="wide"
)

# Firebase connection - simplified direct approach
def connect_to_firebase():
    try:
        # Check if Firebase is already initialized
        if not firebase_admin._apps:
            # 1. Try using Streamlit secrets directly
            if 'firebase_credentials' in st.secrets:
                try:
                    cred = credentials.Certificate(st.secrets["firebase_credentials"])
                    firebase_admin.initialize_app(cred)
                    return firestore.client()
                except Exception as e:
                    st.error(f"Error with Firebase credentials from secrets: {e}")
                    
            # If we get here, secrets approach failed - use session storage
            initialize_session_storage()
            return None
        else:
            # Return existing Firestore client if Firebase is already initialized
            return firestore.client()
    except Exception as e:
        st.error(f"Firebase connection error: {e}")
        # Fall back to session state storage
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
                return pd.DataFrame(columns=['Date', 'Farm A', 'Farm B', 'Farm C', 'Farm D'])
            
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
            
            return df
        except Exception as e:
            # Fallback to session state
            pass
    
    # Session state storage
    if 'farm_data' not in st.session_state:
        initialize_session_storage()
        
    if username in st.session_state.farm_data:
        df = pd.DataFrame(st.session_state.farm_data[username])
        if not df.empty and 'Date' in df.columns:
            df['Date'] = pd.to_datetime(df['Date'])
        return df
    return pd.DataFrame(columns=['Date', 'Farm A', 'Farm B', 'Farm C', 'Farm D'])

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

# Initialize session state variables
if 'logged_in' not in st.session_state:
    st.session_state.logged_in = False

if 'username' not in st.session_state:
    st.session_state.username = ""

if 'role' not in st.session_state:
    st.session_state.role = ""

if 'current_user_data' not in st.session_state:
    st.session_state.current_user_data = pd.DataFrame(columns=['Date', 'Farm A', 'Farm B', 'Farm C', 'Farm D'])

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
def add_data(date, farm_a, farm_b, farm_c, farm_d):
    # Create a new row
    new_row = pd.DataFrame({
        'Date': [pd.Timestamp(date)],
        'Farm A': [farm_a],
        'Farm B': [farm_b],
        'Farm C': [farm_c],
        'Farm D': [farm_d]
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
    st.title("🌷 Flower Farm Tracker - Login")
    
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

# Main app function
def main_app():
    st.title(f"🌷 Flower Farm Tracker - Welcome, {st.session_state.username}!")
    
    # Display storage mode
    st.caption(f"Storage mode: {st.session_state.storage_mode}")
    
    # Create tabs for different functions
    tab1, tab2 = st.tabs(["Data Entry", "Data Analysis"])
    
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
                farm_a = st.number_input("Farm A (Flowers)", min_value=0, value=0, step=1)
            
            with col2:
                farm_b = st.number_input("Farm B (Flowers)", min_value=0, value=0, step=1)
                
            with col3:
                farm_c = st.number_input("Farm C (Flowers)", min_value=0, value=0, step=1)
                
            with col4:
                farm_d = st.number_input("Farm D (Flowers)", min_value=0, value=0, step=1)
            
            # Submit button
            submitted = st.form_submit_button("Add Data")
            
            if submitted:
                if add_data(date, farm_a, farm_b, farm_c, farm_d):
                    st.success(f"Data for {date} added successfully!")
        
        # Add some sample data for testing if data is empty
        if st.button("Add Sample Data") and len(st.session_state.current_user_data) == 0:
            # Create some sample dates (last 10 days)
            dates = [(datetime.now() - timedelta(days=i)).date() for i in range(10, 0, -1)]
            
            # Add random data for each date
            for date in dates:
                add_data(
                    date,
                    np.random.randint(50, 200),  # Farm A
                    np.random.randint(30, 150),  # Farm B
                    np.random.randint(70, 250),  # Farm C
                    np.random.randint(40, 180)   # Farm D
                )
            st.success("Sample data added successfully!")
            st.session_state.needs_rerun = True
        
        # Display the current data
        st.header("Current Data")
        
        if not st.session_state.current_user_data.empty:
            # Format the date column to display only the date part
            display_df = st.session_state.current_user_data.copy()
            if 'Date' in display_df.columns:
                display_df['Date'] = pd.to_datetime(display_df['Date']).dt.date
            st.dataframe(display_df, use_container_width=True)
            
            # Allow downloading the data
            csv = display_df.to_csv(index=False).encode('utf-8')
            st.download_button(
                label="Download Data as CSV",
                data=csv,
                file_name=f"{st.session_state.username}_flower_data_export.csv",
                mime="text/csv"
            )
        else:
            st.info("No data available. Add data using the form above.")
    
    # Tab 2: Data Analysis
    with tab2:
        st.header("Flower Production Analysis")
        
        if st.session_state.current_user_data.empty:
            st.info("No data available for analysis. Please add data in the Data Entry tab.")
        else:
            # Use the data already in datetime format
            analysis_df = st.session_state.current_user_data.copy()
            
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
                
                # Show filtered data
                st.subheader("Filtered Data")
                filtered_display = filtered_df.copy()
                filtered_display['Date'] = filtered_display['Date'].dt.date
                st.dataframe(filtered_display, use_container_width=True)
                
                # Summary statistics for each farm
                st.subheader("Farm Summary Statistics")
                
                # Calculate statistics
                farm_cols = ['Farm A', 'Farm B', 'Farm C', 'Farm D']
                
                # Create summary dataframe
                summary = pd.DataFrame({
                    'Farm': farm_cols,
                    'Total': [filtered_df[col].sum() for col in farm_cols],
                    'Average': [filtered_df[col].mean() for col in farm_cols],
                    'Minimum': [filtered_df[col].min() for col in farm_cols],
                    'Maximum': [filtered_df[col].max() for col in farm_cols]
                })
                
                # Add total row
                total_row = pd.DataFrame({
                    'Farm': ['Total All Farms'],
                    'Total': [filtered_df[farm_cols].sum().sum()],
                    'Average': [filtered_df[farm_cols].sum(axis=1).mean()],
                    'Minimum': [filtered_df[farm_cols].sum(axis=1).min()],
                    'Maximum': [filtered_df[farm_cols].sum(axis=1).max()]
                })
                summary = pd.concat([summary, total_row], ignore_index=True)
                
                # Display summary statistics
                st.dataframe(summary, use_container_width=True)
                
                # Create visualizations
                st.subheader("Visualizations")
                
                # Farm comparison visualization
                farm_totals = pd.DataFrame({
                    'Farm': farm_cols,
                    'Total Flowers': [filtered_df[col].sum() for col in farm_cols]
                })
                
                chart_type = st.radio("Select Chart Type", ["Bar Chart", "Pie Chart"], horizontal=True)
                
                col1, col2 = st.columns(2)
                
                with col1:
                    # Total flowers by farm
                    st.subheader("Total Flowers by Farm")
                    if chart_type == "Bar Chart":
                        fig = px.bar(
                            farm_totals,
                            x='Farm',
                            y='Total Flowers',
                            color='Farm',
                            title="Total Flower Production by Farm",
                            color_discrete_sequence=px.colors.qualitative.Set3
                        )
                        st.plotly_chart(fig, use_container_width=True)
                    else:
                        fig = px.pie(
                            farm_totals,
                            values='Total Flowers',
                            names='Farm',
                            title="Flower Production Distribution",
                            color='Farm',
                            color_discrete_sequence=px.colors.qualitative.Set3
                        )
                        st.plotly_chart(fig, use_container_width=True)
                
                with col2:
                    # Overall totals
                    st.subheader("Daily Production")
                    
                    # Calculate daily totals
                    daily_totals = filtered_df.copy()
                    daily_totals['Total'] = daily_totals[farm_cols].sum(axis=1)
                    
                    # Create line chart for daily totals
                    fig = px.line(
                        daily_totals,
                        x='Date',
                        y='Total',
                        title="Daily Total Flower Production",
                        markers=True
                    )
                    st.plotly_chart(fig, use_container_width=True)
                
                # Show daily production by farm
                st.subheader("Daily Production by Farm")
                
                # Melt the dataframe to get it in the right format for the chart
                melted_df = pd.melt(
                    filtered_df,
                    id_vars=['Date'],
                    value_vars=farm_cols,
                    var_name='Farm',
                    value_name='Flowers'
                )
                
                # Create the line chart
                fig = px.line(
                    melted_df,
                    x='Date',
                    y='Flowers',
                    color='Farm',
                    title="Daily Flower Production by Farm",
                    markers=True,
                    color_discrete_sequence=px.colors.qualitative.Set3
                )
                st.plotly_chart(fig, use_container_width=True)
                
                # Option to download filtered data
                csv = filtered_display.to_csv(index=False).encode('utf-8')
                st.download_button(
                    label="Download Filtered Data as CSV",
                    data=csv,
                    file_name=f"{st.session_state.username}_flower_data_{start_date}_to_{end_date}.csv",
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
        st.session_state.current_user_data = pd.DataFrame(columns=['Date', 'Farm A', 'Farm B', 'Farm C', 'Farm D'])
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
                
                # Edit form
                with st.sidebar.expander("Edit this record"):
                    with st.form("edit_form"):
                        edit_a = st.number_input("Farm A", value=int(current_row['Farm A']), min_value=0)
                        edit_b = st.number_input("Farm B", value=int(current_row['Farm B']), min_value=0)
                        edit_c = st.number_input("Farm C", value=int(current_row['Farm C']), min_value=0)
                        edit_d = st.number_input("Farm D", value=int(current_row['Farm D']), min_value=0)
                        
                        if st.form_submit_button("Update Record"):
                            # Update the values
                            st.session_state.current_user_data.at[date_idx, 'Farm A'] = edit_a
                            st.session_state.current_user_data.at[date_idx, 'Farm B'] = edit_b
                            st.session_state.current_user_data.at[date_idx, 'Farm C'] = edit_c
                            st.session_state.current_user_data.at[date_idx, 'Farm D'] = edit_d
                            
                            # Save to database
                            if save_data(st.session_state.current_user_data, st.session_state.username):
                                st.sidebar.success(f"Record for {selected_date} updated!")
                                st.session_state.needs_rerun = True
                
                # Delete option
                if st.sidebar.button(f"Delete record for {selected_date}"):
                    confirm = st.sidebar.checkbox("I confirm I want to delete this record")
                    if confirm:
                        # Drop the row
                        st.session_state.current_user_data = st.session_state.current_user_data.drop(date_idx).reset_index(drop=True)
                        
                        # Save to database
                        if save_data(st.session_state.current_user_data, st.session_state.username):
                            st.sidebar.success(f"Record for {selected_date} deleted!")
                            st.session_state.needs_rerun = True

    # Upload CSV file
    st.sidebar.subheader("Import Data")
    uploaded_file = st.sidebar.file_uploader("Upload existing data (CSV)", type="csv")
    if uploaded_file is not None:
        try:
            # Read the CSV file
            uploaded_df = pd.read_csv(uploaded_file)
            
            # Check if the required columns exist
            required_cols = ['Date', 'Farm A', 'Farm B', 'Farm C', 'Farm D']
            if all(col in uploaded_df.columns for col in required_cols):
                # Convert Date to datetime
                uploaded_df['Date'] = pd.to_datetime(uploaded_df['Date'])
                
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
                st.sidebar.error(f"CSV must contain columns: {', '.join(required_cols)}")
        except Exception as e:
            st.sidebar.error(f"Error importing data: {e}")

    # Clear all data button
    st.sidebar.subheader("Clear Data")
    if st.sidebar.button("Clear All Data"):
        confirm = st.sidebar.checkbox("I confirm I want to delete all data")
        if confirm:
            # Create empty DataFrame
            st.session_state.current_user_data = pd.DataFrame(columns=['Date', 'Farm A', 'Farm B', 'Farm C', 'Farm D'])
            
            # Save to database
            if save_data(st.session_state.current_user_data, st.session_state.username):
                st.sidebar.success("All data cleared!")
                st.session_state.needs_rerun = True

    # Storage info
    st.sidebar.markdown("---")
    st.sidebar.subheader("Storage Information")
    st.sidebar.info(f"Data Storage Mode: {st.session_state.storage_mode}")
    
    if st.session_state.storage_mode == "Session State":
        st.sidebar.warning("Data is stored in browser session only. For permanent storage, download your data regularly.")

    # Footer
    st.sidebar.markdown("---")
    st.sidebar.markdown("🌷 Flower Farm Tracker - Firebase Storage v1.0")
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
