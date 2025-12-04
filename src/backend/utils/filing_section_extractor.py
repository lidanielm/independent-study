"""
Utilities for extracting sections from SEC filing documents (10-K, 10-Q, etc.).
"""

import re

def extract_sections(text):
    sections = {}
    
    # Normalize text - replace newlines with spaces for regex matching
    normalized_text = text.replace('\n', ' ').replace('\r', ' ')
    
    # Extract Risk Factors (Item 1A)
    risk_pattern = re.compile(
        r'ITEM\s+1A\.?\s*RISK\s+FACTORS(.*?)(?=ITEM\s+1B|ITEM\s+2|ITEM\s+7|ITEM\s+8|$)',
        re.IGNORECASE | re.DOTALL
    )
    risk_match = risk_pattern.search(normalized_text)
    if risk_match:
        sections["Risk Factors"] = risk_match.group(1).strip()
    
    # Extract Management's Discussion and Analysis (Item 7)
    mda_pattern = re.compile(
        r"ITEM\s+7\.?\s*MANAGEMENT['']?S?\s+DISCUSSION\s+AND\s+ANALYSIS(.*?)(?=ITEM\s+7A|ITEM\s+8|$)",
        re.IGNORECASE | re.DOTALL
    )
    mda_match = mda_pattern.search(normalized_text)
    if mda_match:
        sections["MD&A"] = mda_match.group(1).strip()
    
    # Extract Quantitative and Qualitative Disclosures (Item 7A)
    qqd_pattern = re.compile(
        r'ITEM\s+7A\.?\s*QUANTITATIVE\s+AND\s+QUALITATIVE\s+DISCLOSURES(.*?)(?=ITEM\s+8|$)',
        re.IGNORECASE | re.DOTALL
    )
    qqd_match = qqd_pattern.search(normalized_text)
    if qqd_match:
        sections["Quantitative Disclosures"] = qqd_match.group(1).strip()
    
    # Extract Financial Statements (Item 8)
    financial_pattern = re.compile(
        r'ITEM\s+8\.?\s*FINANCIAL\s+STATEMENTS(.*?)(?=ITEM\s+9|ITEM\s+10|$)',
        re.IGNORECASE | re.DOTALL
    )
    financial_match = financial_pattern.search(normalized_text)
    if financial_match:
        sections["Financial Statements"] = financial_match.group(1).strip()
    
    # Extract Controls and Procedures (Item 9A)
    controls_pattern = re.compile(
        r'ITEM\s+9A\.?\s*CONTROLS\s+AND\s+PROCEDURES(.*?)(?=ITEM\s+10|ITEM\s+15|$)',
        re.IGNORECASE | re.DOTALL
    )
    controls_match = controls_pattern.search(normalized_text)
    if controls_match:
        sections["Controls and Procedures"] = controls_match.group(1).strip()
    
    # Extract Business Description (Item 1)
    business_pattern = re.compile(
        r'ITEM\s+1\.?\s*BUSINESS(.*?)(?=ITEM\s+1A|ITEM\s+2|$)',
        re.IGNORECASE | re.DOTALL
    )
    business_match = business_pattern.search(normalized_text)
    if business_match:
        sections["Business"] = business_match.group(1).strip()
    
    return sections

def extract_mda(text):
    sections = extract_sections(text)
    return sections.get("MD&A", text)

def extract_risk_factors(text):
    sections = extract_sections(text)
    return sections.get("Risk Factors", "")

