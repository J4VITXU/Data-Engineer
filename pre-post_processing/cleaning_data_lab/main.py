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

df = pd.read_csv("exercise.csv", engine="python", on_bad_lines="skip")

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

print("STEP 3: IDENTIFY QUALITY ISSUES")

issues = [
    "• Duplicated rows",
    "• Inconsistent country names (USA, usa, united states, US…) ",
    "• Invalid emails",
    "• Invalid phone numbers",
    "• Invalid or negative quantities",
    "• Age values impossible (negative, 999, 'unknown')",
    "• Price values missing",
    "• Mixed date formats"
]

for issue in issues:
    print(issue)

print()

# ============================================================================
# STEP 4: DATA CLEANING
# ============================================================================

print("STEP 4: CLEANING...")

clean = df.copy()

# ---- Clean spacing issues ----
clean["CustomerName"] = clean["CustomerName"].str.title().str.strip()
clean["Email"] = clean["Email"].str.strip().str.lower()
clean["Country"] = clean["Country"].str.strip().str.title()
clean["Phone"] = clean["Phone"].astype(str).str.strip()

# ---- Standardize country names ----
country_map = {
    "Usa": "USA",
    "Us": "USA",
    "United States": "USA",
    "Gb": "UK",
    "United Kingdom": "UK"
}
clean["Country"] = clean["Country"].replace(country_map)

# ---- Fix email format ----
def valid_email(x):
    if pd.isna(x): return np.nan
    pattern = r"^[\w\.-]+@[\w\.-]+\.\w+$"
    return x if re.match(pattern, x) else np.nan

clean["Email"] = clean["Email"].apply(valid_email)

# ---- Fix phone numbers ----
clean["Phone"] = clean["Phone"].apply(lambda x: np.nan if "invalid" in str(x).lower() else x)
clean["Phone"] = clean["Phone"].str.replace(" ", "").str.replace("-", "")

# ---- Fix dates ----
clean["OrderDate"] = pd.to_datetime(
    clean["OrderDate"],
    errors="coerce",
    dayfirst=False
)

# ---- Fix quantities ----
clean["Quantity"] = clean["Quantity"].apply(lambda x: np.nan if x <= 0 else x)

# ---- Fix price ----
clean["Price"] = pd.to_numeric(clean["Price"], errors="coerce")

# ---- Fix age ----
def fix_age(a):
    # Si es NaN → devolver NaN
    if pd.isna(a):
        return np.nan
    
    # Convertir todo a string
    a_str = str(a).strip().lower()

    # Caso "unknown"
    if a_str == "unknown" or a_str == "":
        return np.nan

    # Intentar convertir a número
    try:
        a_num = int(float(a_str))
    except:
        return np.nan

    # Rango válido
    if a_num < 0 or a_num > 120:
        return np.nan

    return a_num


clean["CustomerAge"] = clean["CustomerAge"].apply(fix_age)

# ---- Remove duplicates ----
clean = clean.drop_duplicates()

print("Cleaning completed.\n")

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
