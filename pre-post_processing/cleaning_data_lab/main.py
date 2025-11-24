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

df = pd.read_csv("exercise.csv", engine="python", sep=",", quotechar='"', on_bad_lines="warn")

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
missing = clean.isna().sum()
issues["missing_values"] = missing[missing > 0]

# 2. Duplicated rows
issues["duplicated_rows"] = clean.duplicated().sum()

# 3. Inconsistent country values
issues["country_values"] = clean["Country"].str.lower().value_counts()

# 4. Invalid emails (regex)
email_pattern = r"^[\w\.-]+@[\w\.-]+\.\w+$"
invalid_emails = clean[~clean["Email"].fillna("").str.match(email_pattern)]
issues["invalid_emails"] = invalid_emails[["Email"]]

# 5. Invalid phones (must be 7–15 digits)
invalid_phones = clean[
    ~clean["Phone"].fillna("").str.replace(r"\D", "", regex=True).str.len().between(7, 15)
]
issues["invalid_phones"] = invalid_phones[["Phone"]]

# 6. Invalid quantities
issues["invalid_quantities"] = clean[clean["Quantity"] <= 0][["Quantity"]]

# 7. Invalid ages
def age_invalid(a):
    try:
        a_num = int(float(a))
        return a_num < 0 or a_num > 120
    except:
        return True

issues["invalid_ages"] = clean[clean["CustomerAge"].apply(age_invalid)][["CustomerAge"]]

# 8. Invalid dates
def invalid_date(x):
    try:
        pd.to_datetime(x, errors="raise")
        return False
    except:
        return True

issues["invalid_dates"] = clean[clean["OrderDate"].apply(invalid_date)][["OrderDate"]]

# 9. Invalid prices
issues["invalid_prices"] = clean[(clean["Price"].isna()) | (clean["Price"] <= 0)][["Price"]]


for name, details in issues.items():
    print(f"--- {name.upper()} ---")

    if isinstance(details, (pd.DataFrame, pd.Series)):
        print(details.head(10))
    else:
        print(details)

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
clean["Phone"] = clean["Phone"].astype(str).str.replace(" ", "").str.replace("-", "")
clean["Phone"] = clean["Phone"].apply(
    lambda x: x if x.isdigit() and 7 <= len(x) <= 15 else np.nan
)

# Fix dates
clean["OrderDate"] = pd.to_datetime(clean["OrderDate"], errors="coerce")

# Fix quantities
clean["Quantity"] = clean["Quantity"].apply(lambda x: np.nan if x <= 0 else x)

# Fix prices
clean["Price"] = pd.to_numeric(clean["Price"], errors="coerce")
clean["Price"] = clean["Price"].apply(lambda x: np.nan if x is not None and x <= 0 else x)

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

print(clean.info())
print("\nSummary of missing values:")
print(clean.isna().sum())
print()

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
