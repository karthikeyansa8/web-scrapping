import re

def extract_dates(affiliation_period):
    # Regular expression to match the 'From : dd/mm/yyyy To : dd/mm/yyyy' format
    date_pattern = r"From\s*:\s*(\d{2}/\d{2}/\d{4})\s*To\s*:\s*(\d{2}/\d{2}/\d{4})"
    
    # Search for the pattern in the input string
    match = re.search(date_pattern, affiliation_period)
    
    if match:
        # Extract the 'from' and 'to' dates from the regex groups
        affiliation_from = match.group(1)  # 'From' date
        affiliation_to = match.group(2)    # 'To' date
        return affiliation_from, affiliation_to
    else:
        return None, None

# Example usage:
affiliation_period = "From : 01/04/2025 To : 31/03/2030"
affiliation_from, affiliation_to = extract_dates(affiliation_period)

print(f"Affiliation From: {affiliation_from}")
print(f"Affiliation To: {affiliation_to}")
