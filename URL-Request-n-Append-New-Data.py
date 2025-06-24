# About the task 
# Following a recent event, we collected feedback from attendees. Use that data to update our CRM contact log for each attendee, and update any records that are missing data or are outdated.
# Use the CRM Data API to collect the latest CRM data.
# Use the Form Submission API to collect the event feedback form submission data.
# Create a new CSV file to update the CRM given the feedback data provided from the event.
# The CSV file must be named crm_update.csv.
# The CSV file must have the following columns: id, first, last, email, phone, last contact date, last contact text, all contact text.
# Submit your solution through the "Submit New Work" button.
# You can submit your solution as many times as you like. We will only mark the latest submission.

import requests
import pandas as pd
import re
from pandas import json_normalize
from IPython.display import display
import os
from pathlib import Path
from io import StringIO

token = "f89e2abb2c0502214bbcec06e392a0c0"
headers = {
    "Authorization": f"Bearer {token}"
}

crm_API = "https://it-hiring.blackbird.vc/api/data/crm"
crm_response = requests.get(crm_API, headers=headers)

if crm_response.status_code == 200:
    from io import StringIO
    crm_data = pd.read_csv(StringIO(crm_response.text))
    print("CRM data obtained")
    display(crm_data.head())
else:
    print(f"Failed to fetch CRM data: {crm_response.status_code}")

form_API = "https://it-hiring.blackbird.vc//api/data/form-submissions"
form_response = requests.get(form_API, headers=headers)

if form_response.status_code == 200:
    form_data = pd.json_normalize(form_response.json()) 
    print("Form submission data obtained")
    
    form_data_exploded = form_data.explode("data")
    form_expanded = pd.json_normalize(form_data_exploded["data"])
    
    def clean_name(name):
        if pd.isna(name):
            return ""
        name = name.strip().lower()
        name = re.sub(r'\s+', ' ', name)
        if len(name.split()) < 2:
            return ""
        return name

    if "name" in form_expanded.columns:
        form_expanded["n_name"] = form_expanded["name"].apply(clean_name)

        if "phone" in form_expanded.columns:
            form_expanded["n_phone"] = form_expanded["phone"].astype(str).str.replace(r"^\+", "", regex=True)

        pd.set_option("display.max_columns", None)
        display(form_expanded[["name", "n_name", "phone", "n_phone"]].head(20))
    else:
        print("No 'name' field found in expanded data")

else:
    print(f"Failed to fetch form submissions: {form_response.status_code}")

output_dir = Path.home() / "Documents" / "Interviews"
output_dir.mkdir(parents=True, exist_ok=True)

excel_path = output_dir / "blackbird_raw_data.xlsx"

with pd.ExcelWriter(excel_path) as writer:
    crm_data.to_excel(writer, sheet_name='CRM_Data', index=False)
    form_expanded.to_excel(writer, sheet_name='Form_Submissions', index=False)

print(f"File saved at: {excel_path}")

excel_path

xls = pd.ExcelFile(excel_path)

crm_df = xls.parse("CRM_Data")
form_df = xls.parse("Form_Submissions")

output_path = output_dir / "crm_cleaned.xlsx"

with pd.ExcelWriter(output_path, engine="openpyxl") as writer:
    crm_df.to_excel(writer, sheet_name="CRM_Data", index=False)
    form_expanded.to_excel(writer, sheet_name="Form_Submissions_Expanded", index=False)

import sqlite3
from IPython.display import FileLink

conn = sqlite3.connect(":memory:")

excel_path = output_dir / "crm_cleaned.xlsx"

crm_df = pd.read_excel(excel_path, sheet_name="CRM_Data")
form_df = pd.read_excel(excel_path, sheet_name="Form_Submissions_Expanded")

conn = sqlite3.connect(":memory:")
crm_df.to_sql("crm", conn, index=False, if_exists="replace")
form_df.to_sql("form", conn, index=False, if_exists="replace")

sql = """
-- First CTE: join by phone
WITH first_join AS (
    SELECT 
        crm.id,
        crm.first,
        crm.last,
        COALESCE(form.email, crm.email) AS email,
        crm.phone,
        COALESCE(form.timestamp, crm."last contact date") AS last_contact_date,
        COALESCE(form.message, crm."last contact text") AS last_contact_text,
        crm."all contact text" AS all_contact_text,    
        form.n_name AS name
    FROM crm
    LEFT JOIN form ON crm.phone = form.n_phone
),

-- Second CTE: join by email
second_join AS (
    SELECT 
        fj.id,
        fj.first,
        fj.last,
        fj.email,
        COALESCE(form.n_phone, fj.phone) AS phone,
        COALESCE(form.timestamp, fj.last_contact_date) AS last_contact_date,
        COALESCE(form.message, fj.last_contact_text) AS last_contact_text,
        fj.all_contact_text,
        COALESCE(form.n_name, fj.name) AS name,
        LOWER(fj.first || ' ' || fj.last) AS fullname
    FROM first_join AS fj
    LEFT JOIN form ON fj.email = form.email
),

-- Third CTE: join by fullname
final_merge AS (
    SELECT
        sj.id,
        sj.first,  
        sj.last,
        sj.email,
        COALESCE(form.n_phone, sj.phone) AS phone,
        COALESCE(form.timestamp, sj.last_contact_date) AS last_contact_date,
        COALESCE(form.message, sj.last_contact_text) AS last_contact_text,
        sj.all_contact_text,
        sj.name,
        sj.fullname
    FROM second_join AS sj
    LEFT JOIN form ON sj.fullname = form.n_name
),

-- New records from the event
new_records AS (
    SELECT 
        NULL AS id,
        SUBSTR(f.n_name, 1, INSTR(f.n_name, ' ') - 1) AS first,
        SUBSTR(f.n_name, INSTR(f.n_name, ' ') + 1) AS last,
        f.email,
        f.n_phone AS phone,
        f.timestamp AS last_contact_date,
        f.message AS last_contact_text,
        f.timestamp || ' - ' || f.message AS all_contact_text,
        f.n_name AS name,
        f.n_name AS fullname
    FROM form f
    WHERE NOT EXISTS (
        SELECT 1
        FROM final_merge fm
        WHERE fm.last_contact_date = f.timestamp
          AND fm.last_contact_text = f.message
    )
),

-- Final CTE to prepend latest message only if not already in all_contact_text
final_output AS (
    SELECT
        id,
        CASE 
            WHEN name IS NOT NULL AND name != fullname THEN SUBSTR(name, 1, INSTR(name, ' ') - 1)
            ELSE first
        END AS first,
        
        CASE 
            WHEN name IS NOT NULL AND name != fullname THEN SUBSTR(name, INSTR(name, ' ') + 1)
            ELSE last
        END AS last,
        LOWER(email) AS email,
        phone,
        last_contact_date as 'last contact date',
        last_contact_text as 'last contact text',
        CASE
            WHEN SUBSTR(all_contact_text, 1, 10) != last_contact_date THEN
                last_contact_date || ' - ' || last_contact_text || CHAR(10) || CHAR(10) || ' ' || all_contact_text
            ELSE
                all_contact_text
        END AS 'all contact text',
        name,
        fullname
    FROM final_merge

    UNION ALL

    SELECT * FROM new_records
)

-- Final output
SELECT * FROM final_output;

"""

result_df = pd.read_sql_query(sql, conn)

result_df['first'] = result_df['first'].str.capitalize()
result_df['last'] = result_df['last'].str.capitalize()
result_df.drop(columns=['name', 'fullname'], inplace=True)

pd.set_option('display.max_columns', None)
display(result_df.head(20))

csv_output_path = output_dir / "crm_update.csv"
result_df.to_csv(csv_output_path, index=False)

print(f"CSV file saved to: {csv_output_path}")
