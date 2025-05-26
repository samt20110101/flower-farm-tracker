import streamlit as st
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import plotly.express as px
import plotly.graph_objects as go
import hashlib
import json
import os
import uuid
import re
from typing import List, Dict, Any, Union, Optional

# Try to import Firebase (optional)
try:
    import firebase_admin
    from firebase_admin import credentials, firestore
    FIREBASE_AVAILABLE = True
except ImportError:
    FIREBASE_AVAILABLE = False
    st.warning("Firebase not available - using session storage only")

# Constants
FARM_COLUMNS = ['A: Kebun Sendiri', 'B: Kebun DeYe', 'C: Kebun Asan', 'D: Kebun Uncle']
OLD_FARM_COLUMNS = ['Farm A', 'Farm B', 'Farm C', 'Farm D']
BUYERS = ['Green', 'Kedah', 'YY', 'Lukut', 'PD']
FRUIT_SIZES = ['>600g', '>500g', '>400g', '>300g', 'Reject']
DEFAULT_DISTRIBUTION = {'>600g': 10, '>500g': 20, '>400g': 30, '>300g': 30, 'Reject': 10}
BAKUL_TO_KG = 15  # 1 bakul = 15kg

# Page Configuration
st.set_page_config(
    page_title="Bunga di Kebun",
    page_icon="🌷",
    layout="wide"
)

# Session State Initialization
def initialize_session_state():
    """Initialize all session state variables"""
    defaults = {
        'logged_in': False,
        'username': "",
        'role': "",
        'storage_mode': "Session State",
        'current_user_data': pd.DataFrame(columns=['Date'] + FARM_COLUMNS),
        'csv_backup_enabled': True,
        'revenue_transactions': [],
        'users': {"admin": {"password": hashlib.sha256("admin".encode()).hexdigest(), "role": "admin"}},
        'farm_data': {}
    }
    
    for key, value in defaults.items():
        if key not in st.session_state:
            st.session_state[key] = value

# Firebase Functions
class FirebaseManager:
    def __init__(self):
        self.db = None
        self.connected = False
        self._connect()
    
    def _connect(self):
        """Attempt to connect to Firebase"""
        if not FIREBASE_AVAILABLE:
            return False
        
        try:
            if not firebase_admin._apps:
                if 'firebase_credentials' in st.secrets:
                    firebase_secrets = dict(st.secrets["firebase_credentials"])
                    if 'private_key' in firebase_secrets:
                        firebase_secrets['private_key'] = firebase_secrets['private_key'].replace('\\n', '\n')
                    
                    cred = credentials.Certificate(firebase_secrets)
                    firebase_admin.initialize_app(cred)
            
            self.db = firestore.client()
            # Test connection
            self.db.collection('test').limit(1).get()
            self.connected = True
            st.session_state.storage_mode = "Firebase"
            return True
            
        except Exception as e:
            st.session_state.storage_mode = "Session State"
            self.connected = False
            return False
    
    def get_collection(self, collection_name):
        """Get a Firebase collection if connected"""
        if self.connected and self.db:
            try:
                return self.db.collection(collection_name)
            except Exception:
                return None
        return None

# Initialize Firebase Manager
firebase_manager = FirebaseManager()

# Authentication Functions
def hash_password(password: str) -> str:
    """Hash a password using SHA256"""
    return hashlib.sha256(password.encode()).hexdigest()

def add_user(username: str, password: str, role: str = "user") -> bool:
    """Add a new user to the system"""
    # Try Firebase first
    users_collection = firebase_manager.get_collection('users')
    if users_collection:
        try:
            # Check if user exists
            existing_user = users_collection.document(username).get()
            if existing_user.exists:
                return False
            
            # Add new user
            user_data = {
                "username": username,
                "password": hash_password(password),
                "role": role,
                "created_at": firestore.SERVER_TIMESTAMP
            }
            users_collection.document(username).set(user_data)
            return True
        except Exception as e:
            st.error(f"Error adding user to Firebase: {e}")
    
    # Fallback to session state
    if username in st.session_state.users:
        return False
    
    st.session_state.users[username] = {
        "password": hash_password(password),
        "role": role
    }
    return True

def verify_user(username: str, password: str) -> Optional[str]:
    """Verify user credentials and return role if valid"""
    hashed_password = hash_password(password)
    
    # Try Firebase first
    users_collection = firebase_manager.get_collection('users')
    if users_collection:
        try:
            user_doc = users_collection.document(username).get()
            if user_doc.exists:
                user_data = user_doc.to_dict()
                if user_data and user_data["password"] == hashed_password:
                    return user_data["role"]
        except Exception as e:
            st.error(f"Error verifying user from Firebase: {e}")
    
    # Fallback to session state
    if username in st.session_state.users:
        if st.session_state.users[username]["password"] == hashed_password:
            return st.session_state.users[username]["role"]
    
    return None

# Data Management Functions
def load_farm_data(username: str) -> pd.DataFrame:
    """Load farm data for a specific user"""
    # Try Firebase first
    farm_data_collection = firebase_manager.get_collection('farm_data')
    if farm_data_collection:
        try:
            user_data_docs = farm_data_collection.where("username", "==", username).get()
            
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
            
            # Clean up columns
            for col in ['document_id', 'username']:
                if col in df.columns:
                    df = df.drop(col, axis=1)
            
            # Convert date column
            if 'Date' in df.columns:
                df['Date'] = pd.to_datetime(df['Date'])
            
            # Handle old column names
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
            st.error(f"Error loading farm data from Firebase: {e}")
    
    # Fallback to session state
    if username in st.session_state.farm_data:
        df = pd.DataFrame(st.session_state.farm_data[username])
        
        # Handle old column names
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

def save_farm_data(df: pd.DataFrame, username: str) -> bool:
    """Save farm data for a specific user"""
    # Try Firebase first
    farm_data_collection = firebase_manager.get_collection('farm_data')
    if farm_data_collection:
        try:
            # Get existing documents
            existing_docs = farm_data_collection.where("username", "==", username).get()
            existing_dates = {}
            
            for doc in existing_docs:
                doc_data = doc.to_dict()
                if 'Date' in doc_data:
                    doc_date = pd.to_datetime(doc_data['Date']).date()
                    existing_dates[doc_date] = doc.id
            
            # Process current data
            current_dates = set()
            records = df.to_dict('records')
            
            for record in records:
                record['username'] = username
                
                # Handle date conversion
                if 'Date' in record:
                    if isinstance(record['Date'], pd.Timestamp):
                        record_date = record['Date'].date()
                        record['Date'] = record['Date'].isoformat()
                    else:
                        record_date = pd.to_datetime(record['Date']).date()
                    
                    current_dates.add(record_date)
                
                # Clean up NaN values
                for key, value in record.items():
                    if pd.isna(value):
                        record[key] = 0
                    elif isinstance(value, (np.integer, np.floating)):
                        record[key] = int(value) if isinstance(value, np.integer) else float(value)
                
                # Update or create document
                if record_date in existing_dates:
                    doc_id = existing_dates[record_date]
                    farm_data_collection.document(doc_id).set(record)
                else:
                    farm_data_collection.add(record)
            
            # Delete removed dates
            dates_to_delete = set(existing_dates.keys()) - current_dates
            for date_to_delete in dates_to_delete:
                if date_to_delete in existing_dates:
                    doc_id = existing_dates[date_to_delete]
                    farm_data_collection.document(doc_id).delete()
            
            return True
            
        except Exception as e:
            st.error(f"Error saving farm data to Firebase: {e}")
    
    # Fallback to session state
    st.session_state.farm_data[username] = df.to_dict('records')
    return True

def load_revenue_data(username: str) -> List[Dict]:
    """Load revenue transaction data for a user"""
    # Try Firebase first
    revenue_collection = firebase_manager.get_collection('revenue_data')
    if revenue_collection:
        try:
            user_revenue_docs = revenue_collection.where("username", "==", username).get()
            transactions = []
            for doc in user_revenue_docs:
                doc_data = doc.to_dict()
                if doc_data:
                    transactions.append(doc_data)
            return transactions
        except Exception as e:
            st.error(f"Error loading revenue data from Firebase: {e}")
    
    # Fallback to session state
    return [t for t in st.session_state.revenue_transactions if t.get('username') == username]

def save_revenue_data(transactions: List[Dict], username: str) -> bool:
    """Save revenue transaction data"""
    # Try Firebase first
    revenue_collection = firebase_manager.get_collection('revenue_data')
    if revenue_collection:
        try:
            # Delete existing data for user
            existing_docs = revenue_collection.where("username", "==", username).get()
            for doc in existing_docs:
                doc.reference.delete()
            
            # Add new data
            for transaction in transactions:
                transaction['username'] = username
                revenue_collection.add(transaction)
            return True
        except Exception as e:
            st.error(f"Error saving revenue data to Firebase: {e}")
    
    # Fallback to session state
    # Remove existing transactions for this user
    st.session_state.revenue_transactions = [
        t for t in st.session_state.revenue_transactions if t.get('username') != username
    ]
    
    # Add new transactions
    for transaction in transactions:
        transaction['username'] = username
        st.session_state.revenue_transactions.append(transaction)
    
    return True

# Revenue Calculation Functions
def calculate_bakul_distribution(total_bakul: int, distribution_percentages: Dict[str, float]) -> Dict[str, int]:
    """Calculate number of bakul per fruit size based on percentages"""
    bakul_per_size = {}
    remaining_bakul = total_bakul
    
    # Calculate for all sizes except the last one
    for i, size in enumerate(FRUIT_SIZES[:-1]):
        percentage = distribution_percentages[size]
        bakul_count = int(total_bakul * percentage / 100)
        bakul_per_size[size] = bakul_count
        remaining_bakul -= bakul_count
    
    # Assign remaining bakul to the last size
    bakul_per_size[FRUIT_SIZES[-1]] = max(0, remaining_bakul)
    return bakul_per_size

def validate_estimate_data(estimate: Dict) -> List[str]:
    """Validate estimate data structure and return list of missing fields"""
    required_keys = [
        'selected_buyers', 'buyer_distribution', 'buyer_bakul_allocation',
        'buyer_prices', 'bakul_per_size', 'total_revenue'
    ]
    
    missing_keys = []
    for key in required_keys:
        if key not in estimate:
            missing_keys.append(key)
        elif estimate[key] is None:
            missing_keys.append(f"{key} (null)")
    
    return missing_keys

# UI Components
def login_page():
    """Display the login/registration page"""
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

def data_entry_tab():
    """Data entry interface"""
    st.header("📊 Farm Data Entry")
    
    # Load existing data
    user_data = load_farm_data(st.session_state.username)
    
    col1, col2 = st.columns([1, 2])
    
    with col1:
        st.subheader("Add New Entry")
        
        with st.form("data_entry_form"):
            entry_date = st.date_input("Date", datetime.now().date())
            
            # Farm data inputs
            farm_values = {}
            for farm in FARM_COLUMNS:
                farm_values[farm] = st.number_input(
                    f"{farm}",
                    min_value=0,
                    value=0,
                    step=1
                )
            
            submitted = st.form_submit_button("Add Entry")
            
            if submitted:
                # Check if date already exists
                if not user_data.empty and entry_date in user_data['Date'].dt.date.values:
                    st.warning("Entry for this date already exists. It will be updated.")
                
                # Create new entry
                new_entry = {'Date': entry_date}
                new_entry.update(farm_values)
                
                # Update dataframe
                if user_data.empty or entry_date not in user_data['Date'].dt.date.values:
                    new_df = pd.concat([user_data, pd.DataFrame([new_entry])], ignore_index=True)
                else:
                    # Update existing entry
                    mask = user_data['Date'].dt.date == entry_date
                    for farm, value in farm_values.items():
                        user_data.loc[mask, farm] = value
                    new_df = user_data
                
                # Save data
                if save_farm_data(new_df, st.session_state.username):
                    st.success("Entry saved successfully!")
                    st.rerun()
                else:
                    st.error("Failed to save entry")
    
    with col2:
        st.subheader("Recent Entries")
        
        if not user_data.empty:
            # Sort by date descending
            display_data = user_data.sort_values('Date', ascending=False).head(10)
            st.dataframe(display_data, use_container_width=True, hide_index=True)
            
            # Delete functionality
            if len(user_data) > 0:
                st.subheader("Delete Entry")
                date_options = user_data['Date'].dt.date.tolist()
                selected_date = st.selectbox("Select date to delete", date_options)
                
                if st.button("Delete Entry", type="secondary"):
                    updated_data = user_data[user_data['Date'].dt.date != selected_date]
                    if save_farm_data(updated_data, st.session_state.username):
                        st.success("Entry deleted successfully!")
                        st.rerun()
                    else:
                        st.error("Failed to delete entry")
        else:
            st.info("No data entries yet. Add your first entry using the form on the left.")

def data_analysis_tab():
    """Data analysis and visualization interface"""
    st.header("📈 Data Analysis")
    
    user_data = load_farm_data(st.session_state.username)
    
    if user_data.empty:
        st.info("No data available for analysis. Please add some entries first.")
        return
    
    # Date range selector
    col1, col2 = st.columns(2)
    with col1:
        start_date = st.date_input("Start Date", user_data['Date'].min().date())
    with col2:
        end_date = st.date_input("End Date", user_data['Date'].max().date())
    
    # Filter data
    mask = (user_data['Date'].dt.date >= start_date) & (user_data['Date'].dt.date <= end_date)
    filtered_data = user_data.loc[mask]
    
    if filtered_data.empty:
        st.warning("No data in selected date range.")
        return
    
    # Summary statistics
    st.subheader("Summary Statistics")
    
    col1, col2, col3, col4 = st.columns(4)
    
    total_entries = len(filtered_data)
    total_production = filtered_data[FARM_COLUMNS].sum().sum()
    avg_daily = total_production / total_entries if total_entries > 0 else 0
    best_day = filtered_data[FARM_COLUMNS].sum(axis=1).max()
    
    with col1:
        st.metric("Total Entries", total_entries)
    with col2:
        st.metric("Total Production", f"{total_production:,.0f}")
    with col3:
        st.metric("Daily Average", f"{avg_daily:.1f}")
    with col4:
        st.metric("Best Day", f"{best_day:.0f}")
    
    # Charts
    st.subheader("Production Trends")
    
    # Line chart for daily totals
    daily_totals = filtered_data.copy()
    daily_totals['Total'] = daily_totals[FARM_COLUMNS].sum(axis=1)
    
    fig_line = px.line(
        daily_totals, 
        x='Date', 
        y='Total',
        title='Daily Total Production',
        markers=True
    )
    st.plotly_chart(fig_line, use_container_width=True)
    
    # Stacked area chart by farm
    melted_data = filtered_data.melt(
        id_vars=['Date'], 
        value_vars=FARM_COLUMNS,
        var_name='Farm', 
        value_name='Production'
    )
    
    fig_area = px.area(
        melted_data,
        x='Date',
        y='Production',
        color='Farm',
        title='Production by Farm Over Time'
    )
    st.plotly_chart(fig_area, use_container_width=True)
    
    # Farm comparison
    st.subheader("Farm Performance Comparison")
    
    farm_totals = filtered_data[FARM_COLUMNS].sum().sort_values(ascending=True)
    
    fig_bar = px.bar(
        x=farm_totals.values,
        y=farm_totals.index,
        orientation='h',
        title='Total Production by Farm',
        labels={'x': 'Total Production', 'y': 'Farm'}
    )
    st.plotly_chart(fig_bar, use_container_width=True)

def revenue_estimate_tab():
    """Revenue estimation interface"""
    st.header("💰 Revenue Estimate")
    
    user_transactions = load_revenue_data(st.session_state.username)
    
    price_entry_tab, scenarios_tab, history_tab = st.tabs(["Price Entry", "Scenario Comparison", "History"])
    
    with price_entry_tab:
        st.subheader("Revenue Estimation Calculator")
        
        # STEP 1: Buyer Selection
        st.subheader("Step 1: Select Buyers")
        
        buyer_selection_cols = st.columns(len(BUYERS))
        selected_buyers = []
        
        for i, buyer in enumerate(BUYERS):
            with buyer_selection_cols[i]:
                if st.checkbox(f"Include {buyer}", key=f"select_{buyer}"):
                    selected_buyers.append(buyer)
        
        if selected_buyers:
            st.success(f"✅ Selected buyers: {', '.join(selected_buyers)}")
        else:
            st.warning("⚠️ Please select at least one buyer")
        
        st.markdown("---")
        
        # STEP 2: Revenue Calculation Form
        with st.form("revenue_estimate_form"):
            # Basic inputs
            col1, col2 = st.columns(2)
            
            with col1:
                estimate_date = st.date_input("Estimate Date", datetime.now().date())
            with col2:
                total_bakul = st.number_input("Total Bakul", min_value=0, value=100, step=1)
            
            # Fruit Size Distribution
            st.subheader("Fruit Size Distribution")
            
            distribution_percentages = {}
            dist_cols = st.columns(len(FRUIT_SIZES))
            
            for i, size in enumerate(FRUIT_SIZES):
                with dist_cols[i]:
                    distribution_percentages[size] = st.number_input(
                        f"{size} (%)",
                        min_value=0.0,
                        max_value=100.0,
                        value=float(DEFAULT_DISTRIBUTION[size]),
                        step=0.1,
                        key=f"dist_{size}"
                    )
            
            total_percentage = sum(distribution_percentages.values())
            
            if abs(total_percentage - 100.0) > 0.1:
                st.error(f"❌ Fruit size distribution must total 100%. Current total: {total_percentage:.1f}%")
            else:
                st.success(f"✅ Fruit size distribution: {total_percentage:.1f}%")
            
            # Calculate bakul distribution
            bakul_per_size = {}
            if abs(total_percentage - 100.0) < 0.1:
                bakul_per_size = calculate_bakul_distribution(total_bakul, distribution_percentages)
                
                st.write("**Bakul Distribution:**")
                bakul_display_cols = st.columns(len(FRUIT_SIZES))
                for i, size in enumerate(FRUIT_SIZES):
                    with bakul_display_cols[i]:
                        st.info(f"{size}: {bakul_per_size[size]} bakul")
            
            # Buyer Distribution
            buyer_distribution = {}
            buyer_bakul_allocation = {}
            total_buyer_percentage = 0
            
            if selected_buyers and bakul_per_size:
                st.subheader("Buyer Distribution")
                
                default_buyer_percentage = 100.0 / len(selected_buyers) if selected_buyers else 0
                buyer_dist_cols = st.columns(len(selected_buyers))
                
                for i, buyer in enumerate(selected_buyers):
                    with buyer_dist_cols[i]:
                        buyer_distribution[buyer] = st.number_input(
                            f"{buyer} (%)",
                            min_value=0.0,
                            max_value=100.0,
                            value=default_buyer_percentage,
                            step=0.1,
                            key=f"buyer_dist_{buyer}"
                        )
                
                total_buyer_percentage = sum(buyer_distribution.values())
                
                if abs(total_buyer_percentage - 100.0) > 0.1:
                    st.error(f"❌ Buyer distribution must total 100%. Current total: {total_buyer_percentage:.1f}%")
                else:
                    st.success(f"✅ Buyer distribution: {total_buyer_percentage:.1f}%")
                    
                    # Calculate buyer bakul allocation
                    for buyer in selected_buyers:
                        buyer_bakul_allocation[buyer] = {}
                        for size in FRUIT_SIZES:
                            buyer_bakul_count = int(bakul_per_size[size] * buyer_distribution[buyer] / 100)
                            buyer_bakul_allocation[buyer][size] = buyer_bakul_count
            
            # Pricing Section
            buyer_prices = {}
            if selected_buyers:
                st.subheader("Pricing (RM per kg)")
                
                for buyer in selected_buyers:
                    buyer_prices[buyer] = {}
                    st.write(f"**💼 {buyer}**")
                    
                    price_cols = st.columns(len(FRUIT_SIZES))
                    for i, size in enumerate(FRUIT_SIZES):
                        with price_cols[i]:
                            buyer_prices[buyer][size] = st.number_input(
                                f"{size}",
                                min_value=0.00,
                                value=2.50,
                                step=0.01,
                                format="%.2f",
                                key=f"price_{buyer}_{size}"
                            )
            
            # Calculate revenue
            total_revenue = 0
            revenue_breakdown = {}
            
            if (abs(total_percentage - 100.0) < 0.1 and 
                abs(total_buyer_percentage - 100.0) < 0.1 and 
                bakul_per_size and buyer_bakul_allocation):
                
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
                
                # Display revenue breakdown
                st.subheader("Revenue Breakdown")
                
                for buyer in selected_buyers:
                    buyer_total = sum(revenue_breakdown[buyer][size]['revenue'] for size in FRUIT_SIZES)
                    st.write(f"**{buyer} - RM {buyer_total:,.2f}**")
                    
                    for size in FRUIT_SIZES:
                        details = revenue_breakdown[buyer][size]
                        st.write(f"  • {size}: {details['bakul']} bakul × {BAKUL_TO_KG}kg × RM{details['price']:.2f} = RM{details['revenue']:.2f}")
                
                # Total revenue display
                st.success(f"💰 Total Estimated Revenue: RM {total_revenue:,.2f}")
                
                # Alternative styled display
                col_rev = st.columns([1, 2, 1])
                with col_rev[1]:
                    st.markdown("### 🎯 Revenue Summary")
                    st.metric(
                        label="Total Estimated Revenue", 
                        value=f"RM {total_revenue:,.2f}",
                        help="Based on current buyer distribution and pricing"
                    )
            
            # Form validation
            fruit_percentage_valid = abs(total_percentage - 100.0) < 0.1
            buyer_percentage_valid = abs(total_buyer_percentage - 100.0) < 0.1 if total_buyer_percentage > 0 else False
            can_submit = fruit_percentage_valid and buyer_percentage_valid and len(selected_buyers) > 0
            
            submitted = st.form_submit_button("Save Estimate", disabled=not can_submit)
            
            if submitted:
                if can_submit:
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
                        'revenue_breakdown': revenue_breakdown,
                        'total_revenue': total_revenue,
                        'created_at': datetime.now().isoformat()
                    }
                    
                    user_transactions.append(estimate)
                    
                    if save_revenue_data(user_transactions, st.session_state.username):
                        st.success("✅ Revenue estimate saved successfully!")
                        st.rerun()
                    else:
                        st.error("❌ Failed to save estimate")
                else:
                    if not fruit_percentage_valid:
                        st.error(f"❌ Fruit size distribution must total 100%. Current total: {total_percentage:.1f}%")
                    elif not buyer_percentage_valid:
                        st.error(f"❌ Buyer distribution must total 100%. Current total: {total_buyer_percentage:.1f}%")
                    elif not selected_buyers:
                        st.error("❌ Please select at least one buyer")
    
    with scenarios_tab:
        st.subheader("Scenario Comparison")
        
        if not user_transactions:
            st.info("No estimates available for scenario analysis. Please create an estimate first.")
            return
        
        # Select base estimate
        estimate_options = [f"{t['date']} - {t['id'][:8]}" for t in user_transactions]
        
        selected_estimate_idx = st.selectbox(
            "Select Base Estimate for Scenario Analysis",
            range(len(estimate_options)),
            format_func=lambda x: estimate_options[x]
        )
        
        base_estimate = user_transactions[selected_estimate_idx]
        
        # Validate estimate data
        missing_keys = validate_estimate_data(base_estimate)
        
        if missing_keys:
            st.error(f"❌ Selected estimate is missing required data: {', '.join(missing_keys)}")
            st.info("This estimate might be from an older version. Please create a new estimate for scenario analysis.")
            return
        
        # Display original scenario
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
                    kg_total = bakul_count * BAKUL_TO_KG
                    revenue = kg_total * price
                    st.write(f"  {size}: {bakul_count} bakul × {BAKUL_TO_KG}kg × RM{price:.2f} = RM{revenue:.2f}")
                st.write("")
        
        with col2:
            total_revenue_1 = base_estimate['total_revenue']
            
            st.write("**Scenario 1 Revenue:**")
            
            for buyer in base_estimate['selected_buyers']:
                buyer_revenue = 0
                for size in FRUIT_SIZES:
                    bakul_count = base_estimate['buyer_bakul_allocation'][buyer][size]
                    kg_total = bakul_count * BAKUL_TO_KG
                    price = base_estimate['buyer_prices'][buyer][size]
                    revenue = kg_total * price
                    buyer_revenue += revenue
                
                st.write(f"- {buyer}: RM {buyer_revenue:,.2f}")
            
            st.write(f"**Total: RM {total_revenue_1:,.2f}**")
        
        st.markdown("---")
        
        # Modified scenario
        st.subheader("Scenario 2: Modified Buyer Distribution")
        
        st.write("Modify buyer distribution percentages for comparison:")
        
        new_buyer_distribution = {}
        
        # Buyer distribution modification
        buyer_mod_cols = st.columns(len(base_estimate['selected_buyers']))
        
        for i, buyer in enumerate(base_estimate['selected_buyers']):
            with buyer_mod_cols[i]:
                original_percentage = base_estimate['buyer_distribution'][buyer]
                new_buyer_distribution[buyer] = st.number_input(
                    f"{buyer} (%)",
                    min_value=0.0,
                    max_value=100.0,
                    value=original_percentage,
                    step=0.1,
                    key=f"scenario2_buyer_{buyer}"
                )
        
        total_new_buyer_percentage = sum(new_buyer_distribution.values())
        
        if abs(total_new_buyer_percentage - 100.0) > 0.1:
            st.error(f"❌ Buyer distribution must total 100%. Current total: {total_new_buyer_percentage:.1f}%")
        else:
            st.success(f"✅ New buyer distribution: {total_new_buyer_percentage:.1f}%")
            
            # Calculate new allocation
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
                    kg_total = bakul_count * BAKUL_TO_KG
                    price = base_estimate['buyer_prices'][buyer][size]
                    revenue = kg_total * price
                    buyer_revenue += revenue
                
                total_revenue_2 += buyer_revenue
            
            # Display new allocation
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
            
            # Scenario comparison
            st.subheader("Scenario Comparison")
            
            comparison_data = []
            for buyer in base_estimate['selected_buyers']:
                revenue_1 = 0
                revenue_2 = 0
                
                for size in FRUIT_SIZES:
                    bakul_1 = base_estimate['buyer_bakul_allocation'][buyer][size]
                    price = base_estimate['buyer_prices'][buyer][size]
                    revenue_1 += bakul_1 * BAKUL_TO_KG * price
                    
                    bakul_2 = new_buyer_bakul_allocation[buyer][size]
                    revenue_2 += bakul_2 * BAKUL_TO_KG * price
                
                comparison_data.append({
                    'Buyer': buyer,
                    'Scenario 1 (RM)': f"{revenue_1:,.2f}",
                    'Scenario 2 (RM)': f"{revenue_2:,.2f}",
                    'Difference (RM)': f"{revenue_2 - revenue_1:+,.2f}",
                    'Change (%)': f"{((revenue_2 - revenue_1) / revenue_1 * 100):+.1f}%" if revenue_1 > 0 else "0.0%",
                    'Original (%)': f"{base_estimate['buyer_distribution'][buyer]:.1f}%",
                    'Modified (%)': f"{new_buyer_distribution[buyer]:.1f}%"
                })
            
            # Add total row
            comparison_data.append({
                'Buyer': 'TOTAL',
                'Scenario 1 (RM)': f"{total_revenue_1:,.2f}",
                'Scenario 2 (RM)': f"{total_revenue_2:,.2f}",
                'Difference (RM)': f"{total_revenue_2 - total_revenue_1:+,.2f}",
                'Change (%)': f"{((total_revenue_2 - total_revenue_1) / total_revenue_1 * 100):+.1f}%" if total_revenue_1 > 0 else "0.0%",
                'Original (%)': "100.0%",
                'Modified (%)': f"{total_new_buyer_percentage:.1f}%"
            })
            
            comparison_df = pd.DataFrame(comparison_data)
            st.dataframe(comparison_df, use_container_width=True, hide_index=True)
            
            # Visualization
            buyers_for_chart = base_estimate['selected_buyers']
            scenario1_values = []
            scenario2_values = []
            
            for buyer in buyers_for_chart:
                revenue_1 = sum(
                    base_estimate['buyer_bakul_allocation'][buyer][size] * BAKUL_TO_KG * base_estimate['buyer_prices'][buyer][size]
                    for size in FRUIT_SIZES
                )
                scenario1_values.append(revenue_1)
                
                revenue_2 = sum(
                    new_buyer_bakul_allocation[buyer][size] * BAKUL_TO_KG * base_estimate['buyer_prices'][buyer][size]
                    for size in FRUIT_SIZES
                )
                scenario2_values.append(revenue_2)
            
            # Revenue comparison chart
            fig = go.Figure()
            
            fig.add_trace(go.Bar(
                name='Scenario 1 (Original)',
                x=buyers_for_chart,
                y=scenario1_values,
                marker_color='lightblue'
            ))
            
            fig.add_trace(go.Bar(
                name='Scenario 2 (Modified)',
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
                y=[base_estimate['buyer_distribution'][buyer] for buyer in buyers_for_chart],
                marker_color='lightgreen'
            ))
            
            fig2.add_trace(go.Bar(
                name='Modified Distribution (%)',
                x=buyers_for_chart,
                y=[new_buyer_distribution[buyer] for buyer in buyers_for_chart],
                marker_color='darkgreen'
            ))
            
            fig2.update_layout(
                title='Buyer Distribution Comparison',
                xaxis_title='Buyer',
                yaxis_title='Distribution (%)',
                barmode='group'
            )
            
            st.plotly_chart(fig2, use_container_width=True)
    
    with history_tab:
        st.subheader("Revenue Estimate History")
        
        if not user_transactions:
            st.info("No revenue estimates found. Create your first estimate in the Price Entry tab.")
            return
        
        # Sort transactions by date (newest first)
        sorted_transactions = sorted(user_transactions, key=lambda x: x['date'], reverse=True)
        
        # Display summary table
        summary_data = []
        for transaction in sorted_transactions:
            # Format revenue safely
            revenue_formatted = "{:,.2f}".format(transaction['total_revenue'])
            
            summary_data.append({
                'Date': transaction['date'],
                'ID': transaction['id'][:8],
                'Total Bakul': transaction['total_bakul'],
                'Buyers': ', '.join(transaction['selected_buyers']),
                'Total Revenue (RM)': revenue_formatted,
                'Created': transaction.get('created_at', 'Unknown')[:10]
            })
        
        summary_df = pd.DataFrame(summary_data)
        st.dataframe(summary_df, use_container_width=True, hide_index=True)
        
        # Detailed view selector
        st.subheader("Detailed View")
        
        selected_transaction_idx = st.selectbox(
            "Select estimate to view details",
            range(len(sorted_transactions)),
            format_func=lambda x: f"{sorted_transactions[x]['date']} - {sorted_transactions[x]['id'][:8]}"
        )
        
        selected_transaction = sorted_transactions[selected_transaction_idx]
        
        # Validate transaction data
        missing_keys = validate_estimate_data(selected_transaction)
        
        if missing_keys:
            st.error(f"❌ Selected estimate is missing data: {', '.join(missing_keys)}")
            st.info("This estimate might be from an older version.")
        else:
            # Display detailed breakdown
            col1, col2 = st.columns(2)
            
            with col1:
                st.write("**Estimate Details:**")
                st.write(f"- Date: {selected_transaction['date']}")
                st.write(f"- Total Bakul: {selected_transaction['total_bakul']}")
                
                # Format revenue safely
                revenue_text = "- Total Revenue: RM {:,.2f}".format(selected_transaction['total_revenue'])
                st.write(revenue_text)
                
                st.write(f"- Buyers: {', '.join(selected_transaction['selected_buyers'])}")
                
                st.write("**Fruit Size Distribution:**")
                for size, percentage in selected_transaction['distribution_percentages'].items():
                    bakul_count = selected_transaction['bakul_per_size'][size]
                    st.write(f"- {size}: {percentage:.1f}% ({bakul_count} bakul)")
            
            with col2:
                st.write("**Revenue Breakdown by Buyer:**")
                
                for buyer in selected_transaction['selected_buyers']:
                    buyer_total = 0
                    st.write(f"**{buyer}:**")
                    
                    for size in FRUIT_SIZES:
                        bakul_count = selected_transaction['buyer_bakul_allocation'][buyer][size]
                        price = selected_transaction['buyer_prices'][buyer][size]
                        revenue = bakul_count * BAKUL_TO_KG * price
                        buyer_total += revenue
                        
                        # Format revenue safely
                        revenue_text = "  {}: {} bakul × RM{:.2f} = RM{:.2f}".format(
                            size, bakul_count, price, revenue
                        )
                        st.write(revenue_text)
                    
                    # Format buyer total safely
                    buyer_total_text = "  **Subtotal: RM{:.2f}**".format(buyer_total)
                    st.write(buyer_total_text)
                    st.write("")
        
        # Delete functionality
        st.subheader("Delete Estimate")
        if st.button(f"🗑️ Delete Selected Estimate", type="secondary"):
            updated_transactions = [t for t in user_transactions if t['id'] != selected_transaction['id']]
            if save_revenue_data(updated_transactions, st.session_state.username):
                st.success("Estimate deleted successfully!")
                st.rerun()
            else:
                st.error("Failed to delete estimate")

def main_app():
    """Main application interface"""
    st.title(f"🌷 Bunga di Kebun - Welcome, {st.session_state.username}!")
    
    # Storage mode indicator
    storage_color = "🟢" if "Firebase" in st.session_state.storage_mode else "🟡"
    st.caption(f"{storage_color} Storage mode: {st.session_state.storage_mode}")
    
    # Logout button
    col1, col2 = st.columns([6, 1])
    with col2:
        if st.button("Logout"):
            for key in ['logged_in', 'username', 'role']:
                if key in st.session_state:
                    del st.session_state[key]
            st.rerun()
    
    # Main tabs
    tab1, tab2, tab3 = st.tabs(["📊 Data Entry", "📈 Data Analysis", "💰 Revenue Estimate"])
    
    with tab1:
        data_entry_tab()
    
    with tab2:
        data_analysis_tab()
    
    with tab3:
        revenue_estimate_tab()

# Main Application Logic
def main():
    """Main application entry point"""
    # Initialize session state
    initialize_session_state()
    
    # Check if user is logged in
    if not st.session_state.logged_in:
        login_page()
    else:
        main_app()

# Run the application
if __name__ == "__main__":
    main()
