"""
DATA CLEANING EXERCISE
=====================
Retrieve, explore, and clean an e-commerce customer orders dataset
"""
from datetime import datetime
import pandas as pd
import numpy as np
import re

print("=" * 70)
print("DATA CLEANING EXERCISE - E-COMMERCE CUSTOMER ORDERS")
print("=" * 70)
print(f"Started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")

# ============================================================================
# STEP 1: RETRIEVE DATA FROM WEB SOURCE (Aquí: archivo local)
# ============================================================================

df = pd.read_csv("exercise.csv", engine="python", sep=",", on_bad_lines="warn")

print("STEP 1: DATA RETRIEVED\n")

# ============================================================================
# STEP 2: INITIAL EXPLORATION
# ============================================================================

print("STEP 2: EXPLORATION")
print(df.head())
print("\nInfo:")
print(df.info())
print("\nDescription:")
print(df.describe(include="all"))
print()

# ============================================================================
# STEP 3: IDENTIFY QUALITY ISSUES
# ============================================================================
print("STEP 3: IDENTIFY QUALITY ISSUES (AUTO-DETECTED)\n")

clean = df.copy()
issues = {}

# 1. Missing values
issues["missing_values"] = clean.isna().sum()

# 2. Duplicated rows
issues["duplicated_rows"] = clean.duplicated().sum()

# 3. Inconsistent country values
issues["country_values"] = clean["Country"].astype(str).str.lower().value_counts()

# 4. Invalid emails (simple check: must contain @ and .)
invalid_emails = clean[
    (~clean["Email"].fillna("").str.contains("@")) |
    (~clean["Email"].fillna("").str.contains("."))
]
issues["invalid_emails"] = invalid_emails["Email"]

# 5. Invalid phones (simple check: remove spaces and - → must be digits)
phones = clean["Phone"].fillna("").astype(str).str.replace(" ", "").str.replace("-", "")
invalid_phones = clean[~phones.str.isdigit()]
issues["invalid_phones"] = invalid_phones["Phone"]

# 6. Invalid quantities (negative or zero)
issues["invalid_quantities"] = clean[clean["Quantity"] <= 0]["Quantity"]

# 7. Invalid ages (not numbers or outside 0–120)
def invalid_age(x):
    try:
        x = int(float(x))
        return x < 0 or x > 120
    except:
        return True

issues["invalid_ages"] = clean[clean["CustomerAge"].apply(invalid_age)]["CustomerAge"]

# 8. Invalid dates (simple try/except)
def invalid_date(x):
    try:
        pd.to_datetime(x)
        return False
    except:
        return True

issues["invalid_dates"] = clean[clean["OrderDate"].apply(invalid_date)]["OrderDate"]

# 9. Invalid prices (missing or <= 0)
issues["invalid_prices"] = clean[(clean["Price"].isna()) | (clean["Price"] <= 0)]["Price"]

for name, value in issues.items():
    print(f"--- {name.upper()} ---")

    if isinstance(value, (pd.DataFrame, pd.Series)):
        print(value.head(10))
    else:
        print(value)

    print()



# ============================================================================
# STEP 4: DATA CLEANING
# ============================================================================

print("STEP 4: DATA CLEANING\n")

# Clean names
clean["CustomerName"] = clean["CustomerName"].str.title().str.strip()

# Clean email
clean["Email"] = clean["Email"].str.strip().str.lower()

# Clean country
clean["Country"] = clean["Country"].str.strip().str.title()

country_map = {
    "Usa": "USA",
    "Us": "USA",
    "United States": "USA",
    "Gb": "UK",
    "United Kingdom": "UK",
    "Uk": "UK"
}
clean["Country"] = clean["Country"].replace(country_map)

# Clean phones
def clean_phone(x):
    if pd.isna(x):
        return np.nan
    phone_str = str(x).replace(" ", "").replace("-", "")
    if phone_str.isdigit() and 7 <= len(phone_str) <= 15:
        return phone_str
    return np.nan

clean["Phone"] = clean["Phone"].apply(clean_phone)

# Fix dates
clean["OrderDate"] = pd.to_datetime(clean["OrderDate"], errors="coerce")

# Fix quantities
clean["Quantity"] = clean["Quantity"].apply(lambda x: np.nan if x <= 0 else x)

# Fix prices
clean["Price"] = pd.to_numeric(clean["Price"], errors="coerce")
clean["Price"] = clean["Price"].apply(lambda x: np.nan if pd.notna(x) and x <= 0 else x)

# Fix age
def fix_age(a):
    if pd.isna(a):
        return np.nan
    a_str = str(a).strip().lower()
    if a_str in ("unknown", "", "nan"):
        return np.nan
    try:
        a_num = int(float(a_str))
    except:
        return np.nan
    if a_num < 0 or a_num > 120:
        return np.nan
    return a_num

clean["CustomerAge"] = clean["CustomerAge"].apply(fix_age)

# Remove duplicates
clean = clean.drop_duplicates()

# ============================================================================
# STEP 5: FINAL VALIDATION
# ============================================================================

print("STEP 5: VALIDATION\n")

print("Data types and non-null counts:")
print(clean.info())
print("\n" + "="*70)
print("Summary of missing values:")
print(clean.isna().sum())
print("\n" + "="*70)

# ============================================================================
# STEP 6: SAVE CLEANED DATA
# ============================================================================

clean.to_csv("cleaned_orders.csv", index=False)
print("Saved cleaned dataset as cleaned_orders.csv\n")

# ============================================================================
# SUMMARY
# ============================================================================

print("SUMMARY:")
print(f"Original rows: {len(df)}")
print(f"Clean rows:    {len(clean)}")
print(f"Completed at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
