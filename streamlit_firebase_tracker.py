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

def revenue_estimate_tab():
    st.header("💰 Revenue Estimate")
    
    user_transactions = load_revenue_data(st.session_state.username)
    
    price_entry_tab, scenarios_tab = st.tabs(["Price Entry", "Scenario Comparison"])
    
    with price_entry_tab:
        st.subheader("Revenue Estimation Calculator")
        
        # STEP 1: Buyer Selection (OUTSIDE the form)
        st.subheader("Step 1: Select Buyers")
        
        buyer_selection_cols = st.columns(len(BUYERS))
        selected_buyers = []
        
        for i, buyer in enumerate(BUYERS):
            with buyer_selection_cols[i]:
                if st.checkbox(f"Include {buyer}", key=f"select_{buyer}"):
                    selected_buyers.append(buyer)
        
        # Show selection status
        if selected_buyers:
            st.success(f"✅ Selected buyers: {', '.join(selected_buyers)}")
        else:
            st.warning("⚠️ Please select at least one buyer")
        
        st.markdown("---")
        
        # STEP 2: Form with pricing and calculations
        with st.form("revenue_estimate_form"):
            # Date and Total Bakul input
            col1, col2 = st.columns(2)
            
            with col1:
                estimate_date = st.date_input("Estimate Date", datetime.now().date())
            
            with col2:
                total_bakul = st.number_input("Total Bakul", min_value=0, value=100, step=1)
            
            # Fruit Size Distribution
            st.subheader("Fruit Size Distribution")
            
            dist_cols = st.columns(len(FRUIT_SIZES) + 1)
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
                if abs(total_percentage - 100.0) < 0.1:
                    st.success(f"✅ {total_percentage:.1f}%")
                else:
                    st.error(f"❌ {total_percentage:.1f}%")
            
            # Bakul distribution display
            bakul_per_size = {}
            if abs(total_percentage - 100.0) < 0.1:
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
            
            # Buyer Distribution Section
            buyer_distribution = {}
            buyer_bakul_allocation = {}
            total_buyer_percentage = 0
            
            if selected_buyers and bakul_per_size:
                st.subheader("Buyer Distribution (%)")
                
                # Create columns for buyer distribution
                buyer_dist_cols = st.columns(len(selected_buyers) + 1)
                
                # Headers
                for i, buyer in enumerate(selected_buyers):
                    with buyer_dist_cols[i]:
                        st.write(f"**{buyer}**")
                with buyer_dist_cols[-1]:
                    st.write("**Total**")
                
                # Percentage inputs for buyer distribution
                st.write("**Percentage of total bakul (%)**")
                buyer_perc_cols = st.columns(len(selected_buyers) + 1)
                
                default_buyer_percentage = 100.0 / len(selected_buyers) if selected_buyers else 0
                
                for i, buyer in enumerate(selected_buyers):
                    with buyer_perc_cols[i]:
                        buyer_distribution[buyer] = st.number_input(
                            f"% {buyer}",
                            min_value=0.0,
                            max_value=100.0,
                            value=default_buyer_percentage,
                            step=0.1,
                            key=f"buyer_dist_{buyer}",
                            label_visibility="collapsed"
                        )
                
                total_buyer_percentage = sum(buyer_distribution.values())
                with buyer_perc_cols[-1]:
                    if abs(total_buyer_percentage - 100.0) < 0.1:
                        st.success(f"✅ {total_buyer_percentage:.1f}%")
                    else:
                        st.error(f"❌ {total_buyer_percentage:.1f}%")
                
                # Calculate bakul allocation per buyer
                if abs(total_buyer_percentage - 100.0) < 0.1:
                    for buyer in selected_buyers:
                        buyer_bakul_allocation[buyer] = {}
                        for size in FRUIT_SIZES:
                            buyer_bakul_count = int(bakul_per_size[size] * buyer_distribution[buyer] / 100)
                            buyer_bakul_allocation[buyer][size] = buyer_bakul_count
                    
                    # Display buyer bakul allocation
                    st.write("**Bakul Allocation by Buyer**")
                    
                    allocation_data = []
                    for size in FRUIT_SIZES:
                        row = {'Fruit Size': size}
                        total_allocated = 0
                        for buyer in selected_buyers:
                            bakul_count = buyer_bakul_allocation[buyer][size]
                            row[buyer] = f"{bakul_count} bakul"
                            total_allocated += bakul_count
                        row['Total'] = f"{total_allocated} bakul"
                        allocation_data.append(row)
                    
                    allocation_df = pd.DataFrame(allocation_data)
                    st.dataframe(allocation_df, use_container_width=True, hide_index=True)
            
            # Buyer Pricing Section
            buyer_prices = {}
            if selected_buyers:
                st.subheader("Buyer Pricing (RM per kg)")
                
                for buyer in selected_buyers:
                    buyer_prices[buyer] = {}
                    
                    st.write(f"**💼 {buyer}**")
                    
                    buyer_cols = st.columns(2)
                    
                    with buyer_cols[0]:
                        st.write("**Fruit Size**")
                        for size in FRUIT_SIZES:
                            st.write(size)
                    
                    with buyer_cols[1]:
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
                    
                    st.markdown("---")
                
                # Calculate revenue
                total_revenue = 0
                results_data = []
                
                if (abs(total_percentage - 100.0) < 0.1 and 
                    abs(total_buyer_percentage - 100.0) < 0.1 and 
                    bakul_per_size and buyer_bakul_allocation):
                    
                    for buyer in selected_buyers:
                        buyer_revenue = 0
                        buyer_details = []
                        
                        for size in FRUIT_SIZES:
                            bakul_count = buyer_bakul_allocation[buyer][size]
                            kg_total = bakul_count * 15  # 1 bakul = 15kg
                            price_per_kg = buyer_prices[buyer][size]
                            revenue = kg_total * price_per_kg
                            buyer_revenue += revenue
                            
                            buyer_details.append(f"{size}: {bakul_count} bakul × 15kg × RM{price_per_kg:.2f} = RM{revenue:.2f}")
                        
                        total_revenue += buyer_revenue
                        results_data.append({
                            'Buyer': buyer,
                            'Revenue (RM)': f"{buyer_revenue:,.2f}",
                            'Details': buyer_details
                        })
                    
                    # Display results
                    st.subheader("Revenue Estimate Results")
                    
                    for result in results_data:
                        st.write(f"**{result['Buyer']} - RM {result['Revenue']}**")
                        for detail in result['Details']:
                            st.write(f"  • {detail}")
                        st.write("")
                    
                    # Total revenue display
                    st.markdown(f"""
                    <div style="background-color: #e6ffe6; padding: 15px; border-radius: 5px; margin: 10px 0;">
                        <h2 style="color: #006600; text-align: center; margin: 0;">
                            Total Estimated Revenue: RM {total_revenue:,.2f}
                        </h2>
                    </div>
                    """, unsafe_allow_html=True)
            else:
                st.info("👆 Please select buyers above to see pricing options")
            
            # System Status
            st.subheader("System Status")
            
            status_col1, status_col2, status_col3, status_col4 = st.columns(4)
            
            with status_col1:
                fruit_percentage_valid = abs(total_percentage - 100.0) < 0.1
                if fruit_percentage_valid:
                    st.success(f"✅ Fruit %: {total_percentage:.1f}%")
                else:
                    st.error(f"❌ Fruit %: {total_percentage:.1f}%")
            
            with status_col2:
                buyer_percentage_valid = abs(total_buyer_percentage - 100.0) < 0.1 if total_buyer_percentage > 0 else False
                if buyer_percentage_valid:
                    st.success(f"✅ Buyer %: {total_buyer_percentage:.1f}%")
                elif selected_buyers:
                    st.error(f"❌ Buyer %: {total_buyer_percentage:.1f}%")
                else:
                    st.info("Buyer % pending")
            
            with status_col3:
                if selected_buyers:
                    st.success(f"✅ Buyers: {len(selected_buyers)}")
                else:
                    st.error("❌ No buyers")
            
            with status_col4:
                if fruit_percentage_valid and buyer_percentage_valid and selected_buyers:
                    st.success("✅ Ready to save")
                else:
                    st.error("❌ Cannot save yet")
            
            # Submit button
            fruit_percentage_valid = abs(total_percentage - 100.0) < 0.1
            buyer_percentage_valid = abs(total_buyer_percentage - 100.0) < 0.1 if total_buyer_percentage > 0 else False
            can_submit = fruit_percentage_valid and buyer_percentage_valid and len(selected_buyers) > 0
            
            submitted = st.form_submit_button("Save Estimate", disabled=not can_submit)
            
            if submitted:
                if not fruit_percentage_valid:
                    st.error(f"❌ Fruit size distribution must total 100%. Current total: {total_percentage:.1f}%")
                elif not buyer_percentage_valid:
                    st.error(f"❌ Buyer distribution must total 100%. Current total: {total_buyer_percentage:.1f}%")
                elif not selected_buyers:
                    st.error("❌ Please select at least one buyer")
                else:
                    estimate = {
                        'id': str(uuid.uuid4()),
                        'date': estimate_date.isoformat(),
                        'total_bakul': total_bakul,
                        'distribution_percentages': distribution_percentages,
                        'bakul_per_size': bakul_per_size,
                        'selected_buyers': selected_buyers,
                        'buyer_distribution': buyer_distribution,
                        'buyer_bakul_allocation': buyer_bakul_allocation,
                        'buyer_prices': buyer_prices,
                        'total_revenue': total_revenue,
                        'created_at': datetime.now().isoformat()
                    }
                    
                    user_transactions.append(estimate)
                    
                    if save_revenue_data(user_transactions, st.session_state.username):
                        st.success("✅ Revenue estimate saved successfully!")
                        st.rerun()
                    else:
                        st.error("❌ Failed to save estimate")
    
    with scenarios_tab:
        st.subheader("Scenario Comparison")
        
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
            
            st.subheader("Scenario 1: Original Distribution")
            
            col1, col2 = st.columns(2)
            
            with col1:
                st.write("**Original Buyer Distribution:**")
                
                for buyer in base_estimate['selected_buyers']:
                    buyer_percentage = base_estimate['buyer_distribution'][buyer]
                    st.write(f"**{buyer}: {buyer_percentage:.1f}%**")
                    
                    for size in FRUIT_SIZES:
                        bakul_count = base_estimate['buyer_bakul_allocation'][buyer][size]
                        price = base_estimate['buyer_prices'][buyer][size]
                        kg_total = bakul_count * 15
                        revenue = kg_total * price
                        st.write(f"  {size}: {bakul_count} bakul × 15kg × RM{price:.2f} = RM{revenue:.2f}")
                    st.write("")
            
            with col2:
                total_revenue_1 = base_estimate['total_revenue']
                
                st.write("**Scenario 1 Revenue:**")
                
                for buyer in base_estimate['selected_buyers']:
                    buyer_revenue = 0
                    for size in FRUIT_SIZES:
                        bakul_count = base_estimate['buyer_bakul_allocation'][buyer][size]
                        kg_total = bakul_count * 15
                        price = base_estimate['buyer_prices'][buyer][size]
                        revenue = kg_total * price
                        buyer_revenue += revenue
                    
                    st.write(f"- {buyer}: RM {buyer_revenue:,.2f}")
                
                st.write(f"**Total: RM {total_revenue_1:,.2f}**")
            
            st.markdown("---")
            
            st.subheader("Scenario 2: Modified Buyer Distribution")
            
            st.write("Modify buyer distribution percentages for comparison:")
            
            new_buyer_distribution = {}
            
            st.write("**New Buyer Distribution (%)**")
            
            buyer_mod_cols = st.columns(len(base_estimate['selected_buyers']) + 1)
            
            # Headers
            for i, buyer in enumerate(base_estimate['selected_buyers']):
                with buyer_mod_cols[i]:
                    st.write(f"**{buyer}**")
            with buyer_mod_cols[-1]:
                st.write("**Total**")
            
            # Percentage inputs
            buyer_mod_perc_cols = st.columns(len(base_estimate['selected_buyers']) + 1)
            
            for i, buyer in enumerate(base_estimate['selected_buyers']):
                with buyer_mod_perc_cols[i]:
                    original_percentage = base_estimate['buyer_distribution'][buyer]
                    new_buyer_distribution[buyer] = st.number_input(
                        f"scenario2_buyer_{buyer}",
                        min_value=0.0,
                        max_value=100.0,
                        value=original_percentage,
                        step=0.1,
                        key=f"scenario2_buyer_{buyer}",
                        label_visibility="collapsed"
                    )
            
            total_new_buyer_percentage = sum(new_buyer_distribution.values())
            with buyer_mod_perc_cols[-1]:
                if abs(total_new_buyer_percentage - 100.0) < 0.1:
                    st.success(f"✅ {total_new_buyer_percentage:.1f}%")
                else:
                    st.error(f"❌ {total_new_buyer_percentage:.1f}%")
            
            # Calculate new bakul allocation if percentages are valid
            if abs(total_new_buyer_percentage - 100.0) < 0.1:
                new_buyer_bakul_allocation = {}
                for buyer in base_estimate['selected_buyers']:
                    new_buyer_bakul_allocation[buyer] = {}
                    for size in FRUIT_SIZES:
                        original_size_bakul = base_estimate['bakul_per_size'][size]
                        new_bakul_count = int(original_size_bakul * new_buyer_distribution[buyer] / 100)
                        new_buyer_bakul_allocation[buyer][size] = new_bakul_count
                
                # Calculate scenario 2 revenue
                total_revenue_2 = 0
                
                for buyer in base_estimate['selected_buyers']:
                    buyer_revenue = 0
                    for size in FRUIT_SIZES:
                        bakul_count = new_buyer_bakul_allocation[buyer][size]
                        kg_total = bakul_count * 15
                        price = base_estimate['buyer_prices'][buyer][size]
                        revenue = kg_total * price
                        buyer_revenue += revenue
                    
                    total_revenue_2 += buyer_revenue
                
                st.write("**New Bakul Allocation:**")
                
                new_allocation_data = []
                for size in FRUIT_SIZES:
                    row = {'Fruit Size': size}
                    total_allocated = 0
                    for buyer in base_estimate['selected_buyers']:
                        bakul_count = new_buyer_bakul_allocation[buyer][size]
                        row[buyer] = f"{bakul_count} bakul"
                        total_allocated += bakul_count
                    row['Total'] = f"{total_allocated} bakul"
                    new_allocation_data.append(row)
                
                new_allocation_df = pd.DataFrame(new_allocation_data)
                st.dataframe(new_allocation_df, use_container_width=True, hide_index=True)
                
                st.subheader("Scenario Comparison")
                
                comparison_data = []
                for buyer in base_estimate['selected_buyers']:
                    revenue_1 = 0
                    revenue_2 = 0
                    
                    for size in FRUIT_SIZES:
                        bakul_1 = base_estimate['buyer_bakul_allocation'][buyer][size]
                        price = base_estimate['buyer_prices'][buyer][size]
                        revenue_1 += bakul_1 * 15 * price
                        
                        bakul_2 = new_buyer_bakul_allocation[buyer][size]
                        revenue_2 += bakul_2 * 15 * price
                    
                    comparison_data.append({
                        'Buyer': buyer,
                        'Scenario 1 (RM)': f"{revenue_1:,.2f}",
                        'Scenario 2 (RM)': f"{revenue_2:,.2f}",
                        'Difference (RM)': f"{revenue_2 - revenue_1:+,.2f}",
                        'Change (%)': f"{((revenue_2 - revenue_1) / revenue_1 * 100):+.1f}%" if revenue_1 > 0 else "0.0%",
                        'Distribution 1 (%)': f"{base_estimate['buyer_distribution'][buyer]:.1f}%",
                        'Distribution 2 (%)': f"{new_buyer_distribution[buyer]:.1f}%"
                    })
                
                comparison_data.append({
                    'Buyer': 'TOTAL',
                    'Scenario 1 (RM)': f"{total_revenue_1:,.2f}",
                    'Scenario 2 (RM)': f"{total_revenue_2:,.2f}",
                    'Difference (RM)': f"{total_revenue_2 - total_revenue_1:+,.2f}",
                    'Change (%)': f"{((total_revenue_2 - total_revenue_1) / total_revenue_1 * 100):+.1f}%" if total_revenue_1 > 0 else "0.0%",
                    'Distribution 1 (%)': "100.0%",
                    'Distribution 2 (%)': f"{total_new_buyer_percentage:.1f}%"
                })
                
                comparison_df = pd.DataFrame(comparison_data)
                st.dataframe(comparison_df, use_container_width=True, hide_index=True)
                
                # Visualization
                fig = go.Figure()
                
                buyers_for_chart = [row['Buyer'] for row in comparison_data[:-1]]
                scenario1_values = []
                scenario2_values = []
                
                for buyer in base_estimate['selected_buyers']:
                    revenue_1 = 0
                    for size in FRUIT_SIZES:
                        bakul_1 = base_estimate['buyer_bakul_allocation'][buyer][size]
                        price = base_estimate['buyer_prices'][buyer][size]
                        revenue_1 += bakul_1 * 15 * price
                    scenario1_values.append(revenue_1)
                    
                    revenue_2 = 0
                    for size in FRUIT_SIZES:
                        bakul_2 = new_buyer_bakul_allocation[buyer][size]
                        price = base_estimate['buyer_prices'][buyer][size]
                        revenue_2 += bakul_2 * 15 * price
                    scenario2_values.append(revenue_2)
                
                fig.add_trace(go.Bar(
                    name='Scenario 1 (Original Distribution)',
                    x=buyers_for_chart,
                    y=scenario1_values,
                    marker_color='lightblue'
                ))
                
                fig.add_trace(go.Bar(
                    name='Scenario 2 (Modified Distribution)',
                    x=buyers_for_chart,
                    y=scenario2_values,
                    marker_color='darkblue'
                ))
                
                fig.update_layout(
                    title='Revenue Comparison by Buyer Distribution',
                    xaxis_title='Buyer',
                    yaxis_title='Revenue (RM)',
                    barmode='group'
                )
                
                st.plotly_chart(fig, use_container_width=True)
                
                # Distribution comparison chart
                fig2 = go.Figure()
                
                fig2.add_trace(go.Bar(
                    name='Original Distribution (%)',
                    x=buyers_for_chart,
                    y=[base_estimate['buyer_distribution'][buyer] for buyer in base_estimate['selected_buyers']],
                    marker_color='lightgreen'
                ))
                
                fig2.add_trace(go.Bar(
                    name='Modified Distribution (%)',
                    x=buyers_for_chart,
                    y=[new_buyer_distribution[buyer] for buyer in base_estimate['selected_buyers']],
                    marker_color='darkgreen'
                ))
                
                fig2.update_layout(
                    title='Buyer Distribution Comparison',
                    xaxis_title='Buyer',
                    yaxis_title='Distribution (%)',
                    barmode='group'
                )
                
                st.plotly_chart(fig2, use_container_width=True)
            
            else:
                st.warning(f"⚠️ Buyer distribution must total 100%. Current total: {total_new_buyer_percentage:.1f}%")

def main_app():
    st.title(f"🌷 Bunga di Kebun - Welcome, {st.session_state.username}!")
    
    storage_color = "🟢" if "Firebase" in st.session_state.storage_mode else "🟡"
    st.caption(f"{storage_color} Storage mode: {st.session_state.storage_mode}")
    
    tab1, tab2, tab3 = st.tabs(["Data Entry", "Data Analysis", "Revenue Estimate"])
    
    with tab1:
        st.header("Add New Data")
        st.info("This is a placeholder for the Data Entry tab")
    
    with tab2:
        st.header("Data Analysis")
        st.info("This is a placeholder for the Data Analysis tab")
    
    with tab3:
        revenue_estimate_tab()

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

def check_storage_mode():
    st.session_state.storage_mode = "Session State"

def initialize_app():
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
