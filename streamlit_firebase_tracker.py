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

# Revenue estimation constants
BUYERS = ['Green', 'Kedah', 'YY', 'Lukut', 'PD']
FRUIT_SIZES = ['>600g', '>500g', '>400g', '>300g', 'Reject']
DEFAULT_DISTRIBUTION = {'>600g': 10, '>500g': 20, '>400g': 30, '>300g': 30, 'Reject': 10}
BAKUL_TO_KG = 15  # 1 bakul = 15kg

# NEW: Harvest tracking constants
HARVEST_FRUIT_SIZES = ['>600g', '>500g', '>400g', '>300g', 'Reject']

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
if 'harvest_data' not in st.session_state:
    st.session_state.harvest_data = []

# Helper functions for safe formatting
def format_currency(amount):
    """Safely format currency without f-strings"""
    return "RM {:,.2f}".format(amount)

def format_percentage(value):
    """Safely format percentage without f-strings"""
    return "{:.1f}%".format(value)

def format_number(number):
    return "{:,}".format(int(number))

# Firebase connection - Fixed version
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
        st.error("Firebase connection error: " + str(e))
        st.error("Falling back to session storage...")
        initialize_session_storage()
        return None

def initialize_session_storage():
    if 'users' not in st.session_state:
        st.session_state.users = {
            "admin": {
                "password": hashlib.sha256("admin".encode()).hexdigest(),
                "role": "admin"
            }
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
            st.error("Error accessing users collection: " + str(e))
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
            st.error("Error accessing farm_data collection: " + str(e))
            return None
    return None

def get_revenue_data_collection():
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
            st.error("Error accessing revenue_data collection: " + str(e))
            return None
    return None

def get_harvest_data_collection():
    db = connect_to_firebase()
    if db:
        try:
            harvest_data = db.collection('harvest_data')
            try:
                harvest_data.limit(1).get()
            except:
                pass
            return harvest_data
        except Exception as e:
            st.error("Error accessing harvest_data collection: " + str(e))
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
            st.error("Error adding user to Firebase: " + str(e))
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
            st.error("Error verifying user from Firebase: " + str(e))
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
            st.error("Error loading data from Firebase: " + str(e))
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
            st.error("Error saving data to Firebase: " + str(e))
            pass
    
    if 'farm_data' not in st.session_state:
        initialize_session_storage()
        
    st.session_state.farm_data[username] = df.to_dict('records')
    return True

def load_harvest_data(username):
    harvest_data = get_harvest_data_collection()
    if harvest_data:
        try:
            user_harvest_docs = harvest_data.where("username", "==", username).get()
            harvests = []
            for doc in user_harvest_docs:
                doc_data = doc.to_dict()
                if doc_data:
                    harvests.append(doc_data)
            return harvests
        except Exception as e:
            st.error("Error loading harvest data from Firebase: " + str(e))
    
    if 'harvest_data' not in st.session_state:
        st.session_state.harvest_data = []
    return [h for h in st.session_state.harvest_data if h.get('username') == username]

def save_harvest_data(harvests, username):
    harvest_data = get_harvest_data_collection()
    if harvest_data:
        try:
            existing_docs = harvest_data.where("username", "==", username).get()
            for doc in existing_docs:
                doc.reference.delete()
            
            for harvest in harvests:
                harvest['username'] = username
                harvest_data.add(harvest)
            return True
        except Exception as e:
            st.error("Error saving harvest data to Firebase: " + str(e))
    
    if 'harvest_data' not in st.session_state:
        st.session_state.harvest_data = []
    
    st.session_state.harvest_data = [
        h for h in st.session_state.harvest_data if h.get('username') != username
    ]
    
    for harvest in harvests:
        harvest['username'] = username
        st.session_state.harvest_data.append(harvest)
    
    return True

def load_revenue_data(username):
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
            st.error("Error loading revenue data from Firebase: " + str(e))
    
    return [t for t in st.session_state.revenue_transactions if t.get('username') == username]

def save_revenue_data(transactions, username):
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
            st.error("Error saving revenue data to Firebase: " + str(e))
    
    st.session_state.revenue_transactions = [
        t for t in st.session_state.revenue_transactions if t.get('username') != username
    ]
    
    for transaction in transactions:
        transaction['username'] = username
        st.session_state.revenue_transactions.append(transaction)
    
    return True

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
                st.error("Data for " + str(date) + " already exists. Please edit the existing entry or choose a different date.")
                return "error", None
        
        st.session_state.current_user_data = pd.concat([st.session_state.current_user_data, new_row], ignore_index=True)
        st.session_state.current_user_data = st.session_state.current_user_data.sort_values(by='Date').reset_index(drop=True)
        
        if save_data(st.session_state.current_user_data, st.session_state.username):
            st.session_state.needs_rerun = True
            return "success", None
        else:
            return "error", None
            
    except Exception as e:
        st.error("Error adding data: " + str(e))
        return "error", None

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
                        
                        st.success("Welcome back, " + username + "!")
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

def revenue_estimate_tab():
    st.header("💰 Revenue Estimate")
    st.info("Revenue estimation functionality is available. This is a simplified placeholder - add your full revenue estimation code here.")

def harvest_tracking_tab():
    st.header("🥭 Harvest Tracking")
    
    today = datetime.now().date()
    start_date = today - timedelta(days=35)
    end_date = today - timedelta(days=27)
    
    st.info(f"📅 Showing flowers planted between {start_date} and {end_date} (35-27 days ago)")
    
    if st.session_state.current_user_data.empty:
        st.warning("No flower data available. Please add flower data in the Data Entry tab first.")
        return
    
    flower_df = st.session_state.current_user_data.copy()
    flower_df['Date'] = pd.to_datetime(flower_df['Date'])
    
    filtered_flowers = flower_df[
        (flower_df['Date'].dt.date >= start_date) & 
        (flower_df['Date'].dt.date <= end_date)
    ]
    
    if filtered_flowers.empty:
        st.warning(f"No flower data found between {start_date} and {end_date}.")
        return
    
    filtered_flowers = filtered_flowers.sort_values('Date', ascending=False)
    user_harvests = load_harvest_data(st.session_state.username)
    
    entry_tab, history_tab = st.tabs(["🌱 Harvest Entry", "📊 Harvest History"])
    
    with entry_tab:
        st.subheader("Select Flower Planting Date")
        
        flower_options = []
        flower_date_map = {}
        
        for idx, row in filtered_flowers.iterrows():
            plant_date = row['Date'].date()
            total_bunga = sum([row[col] for col in FARM_COLUMNS])
            total_bakul = int(total_bunga / 40)
            
            existing_harvests = [h for h in user_harvests if h.get('flower_date') == plant_date.isoformat()]
            harvest_count = len(existing_harvests)
            
            total_harvested_bakul = 0
            is_marked_completed = False
            
            for harvest in existing_harvests:
                if 'equivalent_bakul' in harvest:
                    total_harvested_bakul += harvest.get('equivalent_bakul', 0)
                else:
                    total_harvested_bakul += harvest.get('total_harvest_bakul', 0)
                
                if harvest.get('marked_completed', False):
                    is_marked_completed = True
            
            remaining_bakul = max(0, total_bakul - total_harvested_bakul)
            
            if harvest_count > 0:
                if is_marked_completed:
                    status = f" ✅ ({harvest_count} harvest{'s' if harvest_count > 1 else ''}, marked completed)"
                elif remaining_bakul > 0:
                    status = f" 🔄 ({harvest_count} harvest{'s' if harvest_count > 1 else ''}, {remaining_bakul:.1f} bakul remaining)"
                else:
                    status = f" ✅ ({harvest_count} harvest{'s' if harvest_count > 1 else ''}, completed)"
            else:
                status = " 🌱 (ready for harvest)"
            
            option_text = f"{plant_date} - {total_bunga:,} bunga ({total_bakul} bakul){status}"
            flower_options.append(option_text)
            flower_date_map[option_text] = {
                'date': plant_date,
                'total_bunga': total_bunga,
                'total_bakul': total_bakul,
                'remaining_bakul': remaining_bakul,
                'harvest_count': harvest_count,
                'total_harvested_bakul': total_harvested_bakul,
                'is_marked_completed': is_marked_completed,
                'row_data': row
            }
        
        if not flower_options:
            st.warning("No flower data available for harvest tracking.")
            return
        
        selected_flower = st.selectbox(
            "Choose flower planting date (newest first):",
            flower_options,
            help="Select a date when flowers were planted to record harvest data"
        )
        
        if selected_flower:
            flower_info = flower_date_map[selected_flower]
            plant_date = flower_info['date']
            total_bunga = flower_info['total_bunga']
            total_bakul = flower_info['total_bakul']
            remaining_bakul = flower_info['remaining_bakul']
            harvest_count = flower_info['harvest_count']
            total_harvested_bakul = flower_info['total_harvested_bakul']
            is_marked_completed = flower_info['is_marked_completed']
            row_data = flower_info['row_data']
            
            st.subheader(f"🌸 Flower Details for {plant_date}")
            
            if is_marked_completed:
                st.success("✅ **This flower batch has been marked as completed**")
            
            col1, col2, col3 = st.columns(3)
            with col1:
                st.metric("Total Bunga", f"{total_bunga:,}")
                st.metric("Expected Bakul", total_bakul)
            with col2:
                st.metric("Harvested Bakul", f"{total_harvested_bakul:.1f}")
                st.metric("Remaining Bakul", f"{remaining_bakul:.1f}")
                if total_bakul > 0:
                    completion_pct = (total_harvested_bakul / total_bakul) * 100
                    st.metric("Completion", f"{completion_pct:.1f}%")
            with col3:
                st.write("**Farm Breakdown:**")
                for farm_col in FARM_COLUMNS:
                    st.write(f"• {farm_col}: {row_data[farm_col]:,} bunga")
            
            if harvest_count > 0:
                existing_harvests = [h for h in user_harvests if h.get('flower_date') == plant_date.isoformat()]
                existing_harvests = sorted(existing_harvests, key=lambda x: x.get('harvest_date', ''), reverse=True)
                
                st.subheader(f"📋 Previous Harvests ({harvest_count})")
                
                for i, harvest in enumerate(existing_harvests):
                    harvest_date = harvest.get('harvest_date', 'Unknown')
                    harvest_bakul = harvest.get('total_harvest_bakul', 0)
                    additional_kg = harvest.get('total_additional_kg', 0)
                    days_diff = harvest.get('days_to_harvest', 0)
                    efficiency = harvest.get('harvest_efficiency', 0)
                    marked_completed = harvest.get('marked_completed', False)
                    
                    if additional_kg > 0:
                        harvest_amount = f"{harvest_bakul} bakul + {additional_kg:.1f} kg"
                    else:
                        harvest_amount = f"{harvest_bakul} bakul"
                    
                    completion_indicator = " 🏁" if marked_completed else ""
                    
                    with st.expander(f"Harvest #{i+1}: {harvest_date} - {harvest_amount}{completion_indicator}"):
                        col_h1, col_h2 = st.columns(2)
                        with col_h1:
                            st.write(f"• Date: {harvest_date}")
                            st.write(f"• Days after planting: {days_diff}")
                            st.write(f"• Amount: {harvest_amount}")
                            st.write(f"• Efficiency: {efficiency:.1f}%")
                            if marked_completed:
                                st.write("• **Status:** Marked as completed")
                        with col_h2:
                            bakul_distribution = harvest.get('harvest_bakul_distribution', {})
                            kg_distribution = harvest.get('harvest_kg_distribution', {})
                            
                            if bakul_distribution:
                                st.write("**Size Distribution:**")
                                for size in HARVEST_FRUIT_SIZES:
                                    bakul_count = bakul_distribution.get(size, 0)
                                    kg_count = kg_distribution.get(size, 0) if kg_distribution else 0
                                    
                                    if bakul_count > 0 or kg_count > 0:
                                        if kg_count > 0:
                                            st.write(f"• {size}: {bakul_count} bakul + {kg_count:.1f} kg")
                                        else:
                                            st.write(f"• {size}: {bakul_count} bakul")
                            
                            notes = harvest.get('notes', '')
                            if notes:
                                st.write(f"**Notes:** {notes}")
            
            st.markdown("---")
            
            st.subheader("📝 Record New Harvest")
            
            if is_marked_completed:
                st.info("ℹ️ **This flower batch is marked as completed.** You can still add correction harvests if needed.")
            elif remaining_bakul > 50:
                st.info(f"💡 **Large Batch Tip:** You have {remaining_bakul:.1f} bakul remaining. Consider harvesting over multiple days for better quality control.")
            elif remaining_bakul == 0:
                st.warning("⚠️ **All bakul harvested:** This flower planting has been fully harvested. You can still add corrections if needed.")
            
            with st.form("harvest_entry_form"):
                st.write("**Harvest Information:**")
                
                harvest_date = st.date_input(
                    "Harvest Date",
                    value=today,
                    help="Date when fruits were harvested"
                )
                
                if harvest_date:
                    days_diff = (harvest_date - plant_date).days
                    if days_diff < 0:
                        st.error("❌ Harvest date cannot be before planting date!")
                    else:
                        st.info(f"🗓️ Harvested {days_diff} days after planting")
                
                if remaining_bakul > 0 and not is_marked_completed:
                    st.write(f"**Remaining bakul to harvest:** {remaining_bakul:.1f}")
                    if remaining_bakul > 20:
                        st.info("💡 **Tip:** For large batches, consider harvesting 15-25 bakul per day for optimal quality.")
                elif is_marked_completed:
                    st.info("ℹ️ This batch is marked as completed. Any additional harvest will be recorded as correction/bonus harvest.")
                
                st.write("**Harvest by Fruit Size (Bakul + Kg):**")
                st.caption("💡 Enter bakul (full baskets) and additional kg (max 14.9kg per size)")
                
                bakul_inputs = {}
                kg_inputs = {}
                
                for size in HARVEST_FRUIT_SIZES:
                    st.write(f"**{size}:**")
                    size_col1, size_col2 = st.columns([1, 1])
                    
                    with size_col1:
                        bakul_inputs[size] = st.number_input(
                            f"Bakul",
                            min_value=0,
                            value=0,
                            step=1,
                            key=f"harvest_bakul_{size}_{plant_date}_{harvest_count}",
                            help=f"Number of full bakul (15kg each) for {size}"
                        )
                    
                    with size_col2:
                        kg_inputs[size] = st.number_input(
                            f"Additional Kg",
                            min_value=0.0,
                            max_value=14.9,
                            value=0.0,
                            step=0.1,
                            format="%.1f",
                            key=f"harvest_kg_{size}_{plant_date}_{harvest_count}",
                            help=f"Additional kg for {size} (partial bakul)"
                        )
                
                total_harvest_bakul = sum(bakul_inputs.values())
                total_additional_kg = sum(kg_inputs.values())
                total_harvest_kg = (total_harvest_bakul * 15) + total_additional_kg
                equivalent_bakul = total_harvest_kg / 15
                
                if equivalent_bakul > remaining_bakul and remaining_bakul > 0:
                    over_amount = equivalent_bakul - remaining_bakul
                    st.warning(f"⚠️ **Over-harvest warning:** You're trying to harvest {equivalent_bakul:.1f} equivalent bakul, but only {remaining_bakul} remain. This is {over_amount:.1f} bakul over the expected amount.")
                
                if total_harvest_bakul > 0 or total_additional_kg > 0:
                    st.write("**Harvest Summary:**")
                    col_summary1, col_summary2, col_summary3, col_summary4 = st.columns(4)
                    
                    with col_summary1:
                        if total_additional_kg > 0:
                            st.metric("This Harvest", f"{total_harvest_bakul} bakul + {total_additional_kg:.1f} kg")
                        else:
                            st.metric("This Harvest", f"{total_harvest_bakul} bakul")
                    with col_summary2:
                        st.metric("Total Weight", f"{total_harvest_kg:.1f} kg")
                    with col_summary3:
                        new_total_harvested_equiv = total_harvested_bakul + equivalent_bakul
                        overall_efficiency = (new_total_harvested_equiv / total_bakul * 100) if total_bakul > 0 else 0
                        st.metric("Overall Progress", f"{overall_efficiency:.1f}%")
                    with col_summary4:
                        new_remaining = max(0, total_bakul - new_total_harvested_equiv)
                        st.metric("Will Remain", f"{new_remaining:.1f} bakul")
                
                mark_completed = st.checkbox(
                    "🏁 Mark this flower batch as completed after this harvest",
                    help="Check this if you want to mark this flower batch as fully harvested, even if remaining bakul > 0"
                )
                
                if mark_completed and remaining_bakul > 0:
                    remaining_after = max(0, total_bakul - (total_harvested_bakul + equivalent_bakul))
                    st.info(f"ℹ️ **Completion Note:** This will mark the flower batch as completed. Approximately {remaining_after:.1f} bakul will be marked as not harvested due to low efficiency or other factors.")
                
                notes = st.text_area(
                    "Notes (optional)",
                    placeholder="Add notes about harvest conditions, quality, weather, completion reasons, etc.",
                    key=f"harvest_notes_{plant_date}_{harvest_count}"
                )
                
                submitted = st.form_submit_button("💾 Save Harvest Record")
                
                if submitted:
                    if harvest_date < plant_date:
                        st.error("❌ Harvest date cannot be before planting date!")
                    elif total_harvest_bakul == 0 and total_additional_kg == 0:
                        st.error("❌ Please enter at least some harvest data!")
                    else:
                        harvest_record = {
                            'id': f"{plant_date.isoformat()}_{harvest_date.isoformat()}_{int(datetime.now().timestamp())}",
                            'flower_date': plant_date.isoformat(),
                            'harvest_date': harvest_date.isoformat(),
                            'days_to_harvest': (harvest_date - plant_date).days,
                            'flower_total_bunga': total_bunga,
                            'flower_total_bakul': total_bakul,
                            'flower_farm_breakdown': {col: int(row_data[col]) for col in FARM_COLUMNS},
                            'harvest_bakul_distribution': bakul_inputs,
                            'harvest_kg_distribution': kg_inputs,
                            'total_harvest_bakul': total_harvest_bakul,
                            'total_additional_kg': total_additional_kg,
                            'total_harvest_kg': total_harvest_kg,
                            'equivalent_bakul': equivalent_bakul,
                            'harvest_efficiency': (equivalent_bakul / total_bakul * 100) if total_bakul > 0 else 0,
                            'harvest_number': harvest_count + 1,
                            'cumulative_harvested': total_harvested_bakul + equivalent_bakul,
                            'remaining_after_harvest': max(0, total_bakul - (total_harvested_bakul + equivalent_bakul)),
                            'marked_completed': mark_completed,
                            'notes': notes.strip() if notes else "",
                            'created_at': datetime.now().isoformat()
                        }
                        
                        user_harvests.append(harvest_record)
                        
                        if save_harvest_data(user_harvests, st.session_state.username):
                            new_remaining = max(0, total_bakul - (total_harvested_bakul + equivalent_bakul))
                            
                            if mark_completed:
                                st.success(f"🎉 Harvest completed and marked as finished! {total_harvest_bakul} bakul + {total_additional_kg:.1f} kg harvested.")
                            elif new_remaining > 0:
                                if total_additional_kg > 0:
                                    st.success(f"✅ Harvest #{harvest_count + 1} saved! {total_harvest_bakul} bakul + {total_additional_kg:.1f} kg harvested. {new_remaining:.1f} bakul remaining.")
                                else:
                                    st.success(f"✅ Harvest #{harvest_count + 1} saved! {total_harvest_bakul} bakul harvested. {new_remaining:.1f} bakul remaining.")
                            else:
                                st.success(f"🎉 Final harvest saved! {total_harvest_bakul} bakul + {total_additional_kg:.1f} kg harvested. All bakul completed!")
                            st.rerun()
                        else:
                            st.error("❌ Failed to save harvest record")
    
    with history_tab:
        st.subheader("📊 Harvest History")
        
        if not user_harvests:
            st.info("No harvest records found. Add your first harvest in the Harvest Entry tab.")
            return
        
        sorted_harvests = sorted(
            user_harvests,
            key=lambda x: x.get('harvest_date', '1900-01-01'),
            reverse=True
        )
        
        st.subheader("🌸 Harvest Summary by Flower Date")
        
        flower_date_summary = {}
        for harvest in user_harvests:
            flower_date = harvest.get('flower_date', 'Unknown')
            if flower_date not in flower_date_summary:
                flower_date_summary[flower_date] = {
                    'flower_date': flower_date,
                    'expected_bakul': harvest.get('flower_total_bakul', 0),
                    'total_bunga': harvest.get('flower_total_bunga', 0),
                    'total_harvested_bakul': 0,
                    'harvest_count': 0,
                    'first_harvest_date': harvest.get('harvest_date', ''),
                    'last_harvest_date': harvest.get('harvest_date', ''),
                    'days_to_first_harvest': harvest.get('days_to_harvest', 0),
                    'farm_breakdown': harvest.get('flower_farm_breakdown', {}),
                    'is_marked_completed': False
                }
            
            if 'equivalent_bakul' in harvest:
                flower_date_summary[flower_date]['total_harvested_bakul'] += harvest.get('equivalent_bakul', 0)
            else:
                flower_date_summary[flower_date]['total_harvested_bakul'] += harvest.get('total_harvest_bakul', 0)
            
            flower_date_summary[flower_date]['harvest_count'] += 1
            
            if harvest.get('marked_completed', False):
                flower_date_summary[flower_date]['is_marked_completed'] = True
            
            harvest_date = harvest.get('harvest_date', '')
            if harvest_date < flower_date_summary[flower_date]['first_harvest_date']:
                flower_date_summary[flower_date]['first_harvest_date'] = harvest_date
                flower_date_summary[flower_date]['days_to_first_harvest'] = harvest.get('days_to_harvest', 0)
            if harvest_date > flower_date_summary[flower_date]['last_harvest_date']:
                flower_date_summary[flower_date]['last_harvest_date'] = harvest_date
        
        summary_list = list(flower_date_summary.values())
        summary_list = sorted(summary_list, key=lambda x: x['flower_date'], reverse=True)
        
        summary_table_data = []
        for summary in summary_list:
            expected_bakul = summary['expected_bakul']
            harvested_bakul = summary['total_harvested_bakul']
            efficiency = (harvested_bakul / expected_bakul * 100) if expected_bakul > 0 else 0
            is_marked_completed = summary['is_marked_completed']
            
            if harvested_bakul == 0:
                status = "🌱 Not Started"
            elif is_marked_completed:
                status = "✅ Completed (Marked)"
            elif harvested_bakul >= expected_bakul:
                status = "✅ Completed"
            else:
                remaining = expected_bakul - harvested_bakul
                status = f"🔄 In Progress ({remaining:.1f} remaining)"
            
            if summary['harvest_count'] == 1:
                harvest_period = summary['first_harvest_date']
            else:
                harvest_period = f"{summary['first_harvest_date']} to {summary['last_harvest_date']}"
            
            summary_table_data.append({
                'Flower Date': summary['flower_date'],
                'Expected Bakul': expected_bakul,
                'Harvested Bakul': f"{harvested_bakul:.1f}",
                'Efficiency (%)': f"{efficiency:.1f}%",
                'Status': status,
                'Harvest Count': summary['harvest_count'],
                'Harvest Period': harvest_period,
                'Days to First Harvest': summary['days_to_first_harvest']
            })
        
        if summary_table_data:
            summary_df = pd.DataFrame(summary_table_data)
            st.dataframe(summary_df, use_container_width=True, hide_index=True)

def main_app():
    st.title("🌷 Bunga di Kebun - Welcome, " + st.session_state.username + "!")
    
    storage_color = "🟢" if "Firebase" in st.session_state.storage_mode else "🟡"
    st.caption(storage_color + " Storage mode: " + st.session_state.storage_mode)
    
    tab1, tab2, tab3, tab4 = st.tabs(["📝 Data Entry", "📊 Data Analysis", "💰 Revenue Estimate", "🥭 Harvest Tracking"])
    
    with tab1:
        st.header("Add New Data")
        
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
            
            st.write(f"**Date:** {date_formatted} ({day_name})")
            st.write(f"**Total Bunga:** {format_number(total_bunga)}")
            st.write(f"**Total Bakul:** {format_number(total_bakul)}")
            
            st.write("**Farm Details:**")
            for farm, value in farm_data.items():
                st.write(f"• {farm}: {format_number(value)} bunga")
            
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
                        st.success("Data for " + date_formatted + " added successfully!")
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
                    farm_1 = st.number_input("" + FARM_COLUMNS[0] + " (Bunga)", min_value=0, value=0, step=1)
                
                with col2:
                    farm_2 = st.number_input("" + FARM_COLUMNS[1] + " (Bunga)", min_value=0, value=0, step=1)
                    
                with col3:
                    farm_3 = st.number_input("" + FARM_COLUMNS[2] + " (Bunga)", min_value=0, value=0, step=1)
                    
                with col4:
                    farm_4 = st.number_input("" + FARM_COLUMNS[3] + " (Bunga)", min_value=0, value=0, step=1)
                
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
                file_name=st.session_state.username + "_bunga_data_export.csv",
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
            
            total_bunga = int(analysis_df[FARM_COLUMNS].sum().sum())
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
            
            display_df = analysis_df.copy()
            display_df['Date'] = pd.to_datetime(display_df['Date']).dt.date
            display_df['Total Bunga'] = display_df[FARM_COLUMNS].sum(axis=1).astype(int)
            
            for col in FARM_COLUMNS + ['Total Bunga']:
                if col in display_df.columns:
                    display_df[col] = display_df[col].apply(format_number)
            
            st.dataframe(display_df, use_container_width=True, hide_index=True)
    
    with tab3:
        revenue_estimate_tab()
    
    with tab4:
        harvest_tracking_tab()

def sidebar_options():
    st.sidebar.header("User: " + st.session_state.username)
    
    if st.sidebar.button("Logout"):
        st.session_state.logged_in = False
        st.session_state.username = ""
        st.session_state.role = ""
        st.session_state.current_user_data = pd.DataFrame(columns=['Date'] + FARM_COLUMNS)
        st.session_state.needs_rerun = True
        return

    st.sidebar.markdown("---")
    st.sidebar.subheader("Storage Information")
    storage_color = "🟢" if "Firebase" in st.session_state.storage_mode else "🟡"
    st.sidebar.info(storage_color + " Data Storage Mode: " + st.session_state.storage_mode)

    st.sidebar.markdown("---")
    st.sidebar.markdown("🌷 Bunga di Kebun - v2.0 with Harvest Tracking")
    st.sidebar.text("User: " + st.session_state.username + " (" + st.session_state.role + ")")

def initialize_app():
    users = get_users_collection()
    if users:
        try:
            admin_doc = users.document("admin").get()
            if not admin_doc.exists:
                add_user("admin", "admin", "admin")
            return
        except Exception as e:
            st.error("Error initializing Firebase: " + str(e))
            pass
    
    initialize_session_storage()

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
            st.error("Firebase connection test failed: " + str(e))
    
    st.session_state.storage_mode = "Session State"

initialize_app()

if st.session_state.storage_mode == "Checking...":
    check_storage_mode()

if not st.session_state.logged_in:
    login_page()
else:
    main_app()
    sidebar_options()
