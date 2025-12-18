# Exercise Correction: Data Cleaning Lab
**Student:** Javier Gallardo  
**Exercise:** E-commerce Customer Orders Data Cleaning  
**Date:** December 12, 2025

---

## Overall Assessment

**Grade: 6.8/10** 

This is an overall fine data cleaning exercise that demonstrates good understanding of data quality issues and systematic cleaning approaches. The code shows comprehensive issue detection, though there are areas where the implementation could be more robust and complete.

However a critical failure is to use a local file when the requirements of the exercise added the restriction of downloading via the request API.

---

## Strengths

### 1. **Excellent Issue Detection Framework**
-  Comprehensive automated issue detection in Step 3.
- Systematic identification of 9 different data quality problems:
  - Missing values
  - Duplicated rows
  - Inconsistent country values
  - Invalid emails
  - Invalid phones
  - Invalid quantities (negative/zero)
  - Invalid ages
  - Invalid dates
  - Invalid prices
- Good diagnostic output showing specific problematic records.

### 2. **Good Code Structure** 
- Clear separation of steps with headers.
- Logical flow from exploration → detection → cleaning → validation.
- Good use of `.copy()` to preserve original data.
- Comprehensive summary at the end.

### 3. **Robust Validation Functions** 
- Smart custom validation functions (`invalid_age`, `invalid_date`).
- Good use of try-except for error handling in validation.
- Appropriate use of `errors="coerce"` for type conversion.

### 4. **Comprehensive Data Exploration** 
- Good use of `.info()`, `.describe(include="all")`.
- Clear initial data overview.
- Shows understanding of data profiling.

### 5. **Advanced Cleaning Techniques** 
- Custom `clean_phone()` function with length validation (7-15 digits).
- Custom `fix_age()` function handling multiple edge cases.
- Country mapping for standardization.

---

## Issues Found

### 1. **CRITICAL: Local File Instead of Web Source** (-2 points)
**Severity:** High (doesn't meet exercise requirements)

```python
# Current implementation
df = pd.read_csv("exercise.csv", engine="python", sep=",", on_bad_lines="warn")
```

**Expected:** The exercise should retrieve data from the web source:
```python
# Should be:
import requests
import io

url = "https://raw.githubusercontent.com/victorbrub/data-engineering-class/refs/heads/main/pre-post_processing/exercise.csv"
response = requests.get(url, timeout=10)
df = pd.read_csv(io.StringIO(response.text), sep=',', on_bad_lines='warn')
```

**Impact:** 
- Missing `requests` library in requirements.txt.
- Doesn't demonstrate HTTP request handling.
- File dependency instead of true data retrieval.

---

### 2. **Incomplete Missing Value Handling** (-0.8 points)

**Issue:** Invalid values are converted to NaN, but no imputation strategy is applied.

**Result (from output.log):**
```
Summary of missing values:
OrderDate     70  (37% missing!)
Quantity      44  (23% missing!)
CustomerAge   98  (52% missing!)
```

**Recommendation:** Add imputation strategies:
```python
# After cleaning, add imputation
# For numeric fields - use median
clean["Quantity"] = clean["Quantity"].fillna(clean["Quantity"].median())
clean["Price"] = clean["Price"].fillna(clean["Price"].median())
clean["CustomerAge"] = clean["CustomerAge"].fillna(clean["CustomerAge"].median())

# For dates - use mode or forward fill
mode_date = clean["OrderDate"].mode()[0] if len(clean["OrderDate"].mode()) > 0 else pd.Timestamp.now()
clean["OrderDate"] = clean["OrderDate"].fillna(mode_date)

# For categorical - use placeholder
clean["Email"] = clean["Email"].fillna("unknown@email.com")
clean["Phone"] = clean["Phone"].fillna("UNKNOWN")
```

---

### 3. **No Before/After Comparison** (-0.5 points)

**Issue:** Excellent issue detection in Step 3, but no re-validation after cleaning to show improvement.

**Recommendation:** Add quality metrics comparison:
```python
# After Step 5
print("\nQUALITY IMPROVEMENT METRICS:")
print("="*70)
print(f"Missing values (before): {df.isna().sum().sum()}")
print(f"Missing values (after):  {clean.isna().sum().sum()}")
print(f"Invalid ages (before):   {len(issues['invalid_ages'])}")
print(f"Invalid ages (after):    {clean['CustomerAge'].isna().sum()}")
print(f"Invalid quantities (before): {len(issues['invalid_quantities'])}")
print(f"Invalid quantities (after):  {clean['Quantity'].isna().sum()}")
```

---

### 4. **Email Validation Logic Issue** (-0.4 points)

**Issue:** Email validation in detection is too simplistic:
```python
invalid_emails = clean[
    (~clean["Email"].fillna("").str.contains("@")) |
    (~clean["Email"].fillna("").str.contains("."))
]
```

This marks valid emails as invalid (e.g., needs both @ AND ., but logic is OR).

**Also:** No cleaning applied to invalid emails (only lowercasing and stripping).

**Better approach:**
```python
# Detection
def is_valid_email(email):
    if pd.isna(email):
        return False
    pattern = r'^[\w\.-]+@[\w\.-]+\.\w+$'
    return bool(re.match(pattern, email))

invalid_emails = clean[~clean["Email"].apply(is_valid_email)]

# Cleaning
def validate_email(email):
    if pd.isna(email):
        return np.nan
    pattern = r'^[\w\.-]+@[\w\.-]+\.\w+$'
    return email if re.match(pattern, email) else np.nan

clean["Email"] = clean["Email"].apply(validate_email)
```

---

### 5. **ParserWarning on Line 6** (-0.3 points)

From output.log:
```
ParserWarning: Skipping line 6: Expected 10 fields in line 6, saw 11
```

**Issue:** While `on_bad_lines="warn"` handles it, data is being silently lost.

**Better approach:**
```python
# Show more detail about skipped lines
df = pd.read_csv(
    "exercise.csv", 
    engine="python", 
    sep=",", 
    on_bad_lines="warn"
)
print(f" Warning: Some malformed lines were skipped")
```

---

### 6. **Quantity Cleaning Issue** (-0.2 points)

```python
clean["Quantity"] = clean["Quantity"].apply(lambda x: np.nan if x <= 0 else x)
```

This converts Quantity to float (due to NaN), but quantities should remain integers:

```python
# Better
clean.loc[clean["Quantity"] <= 0, "Quantity"] = np.nan
clean["Quantity"] = clean["Quantity"].astype("Int64")  # Nullable integer
```

---

## Suggestions for Improvement

### 1. **Add Data Quality Dimensions**
Following industry standards (like DAMA), create quality metrics:
```python
def calculate_quality_metrics(df):
    total = len(df)
    return {
        "Completeness": 100 * (1 - df.isna().sum().sum() / (total * len(df.columns))),
        "Validity": 100 * df["Email"].apply(is_valid_email).mean(),
        "Uniqueness": 100 * (~df["OrderID"].duplicated()).mean(),
        "Accuracy": 100 * df["CustomerAge"].between(0, 120).mean()
    }

print("BEFORE:", calculate_quality_metrics(df))
print("AFTER:", calculate_quality_metrics(clean))
```

### 2. **Improve Phone Cleaning Function**
```python
def clean_phone(x):
    if pd.isna(x):
        return np.nan
    # Remove all non-digit characters
    phone_str = re.sub(r'\D', '', str(x))
    # Valid if 7-15 digits
    if 7 <= len(phone_str) <= 15:
        return phone_str
    return np.nan
```

### 3. **Add Statistical Summary**
```python
print("\nNUMERICAL SUMMARY:")
print(clean[["CustomerAge", "Quantity", "Price"]].describe())

print("\nCATEGORICAL SUMMARY:")
print(f"Countries: {clean['Country'].value_counts().to_dict()}")
print(f"Order Status: {clean['OrderStatus'].value_counts().to_dict()}")
```

### 4. **Add Duplicate Detection Details**
```python
# Before drop_duplicates()
if clean.duplicated().sum() > 0:
    print(f"⚠️  Found {clean.duplicated().sum()} duplicate rows")
    duplicates = clean[clean.duplicated(keep=False)].sort_values("OrderID")
    print(duplicates[["OrderID", "CustomerName", "Email"]])
```

---

