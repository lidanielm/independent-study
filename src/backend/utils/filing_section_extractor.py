"""
Utilities for extracting sections from SEC filing documents (10-K, 10-Q, etc.).
"""

import re

ITEM_WORD = r"(?:ITEM|I\\s*T\\s*E\\s*M)"

def _best_section_match(pattern: re.Pattern, text: str, min_len: int = 800) -> str:
    """
    Inline XBRL filings often contain an early Table of Contents entry like:
      'ITEM 1A. RISK FACTORS 13'
    which matches naive regex first and yields junk.

    We therefore consider ALL matches and return the *longest* captured section
    (optionally requiring a minimum length to avoid TOC hits).
    """
    best = ""
    for m in pattern.finditer(text):
        candidate = (m.group(1) or "").strip()
        if len(candidate) > len(best):
            best = candidate
    if best and len(best) >= min_len:
        return best
    # If nothing meets min length, still return the best we found (could be short)
    return best

def extract_sections(text):
    sections = {}
    
    if not text or not isinstance(text, str):
        return sections
    
    # Normalize text - replace newlines with spaces for regex matching
    # Also handle HTML artifacts that might remain
    normalized_text = text.replace('\n', ' ').replace('\r', ' ').replace('\t', ' ')
    # Remove excessive whitespace
    normalized_text = re.sub(r'\s+', ' ', normalized_text)
    
    # Extract Risk Factors (Item 1A)
    risk_pattern = re.compile(
        rf'{ITEM_WORD}\s+1A\.?\s*RISK\s+FACTORS(.*?)(?={ITEM_WORD}\s+1B|{ITEM_WORD}\s+2|{ITEM_WORD}\s+7|{ITEM_WORD}\s+8|$)',
        re.IGNORECASE | re.DOTALL
    )
    risk = _best_section_match(risk_pattern, normalized_text)
    # Some filers (e.g., Intel) use a non-traditional 10-K format where the section is titled
    # "Risk Factors and Other Key Information" and may not include "Item 1A" headings in body text.
    if not risk or len(risk) < 1200:
        risk_alt_pattern = re.compile(
            r'RISK\s+FACTORS(?:\s+AND\s+OTHER\s+KEY\s+INFORMATION)?(.*?)(?=MANAGEMENT[’\\\']?S\s+DISCUSSION|RESULTS\s+OF\s+OPERATIONS|FINANCIAL\s+STATEMENTS|PART\s+II|$)',
            re.IGNORECASE | re.DOTALL
        )
        risk = _best_section_match(risk_alt_pattern, normalized_text, min_len=1200)
    if risk:
        sections["Risk Factors"] = risk
    
    # Extract Management's Discussion and Analysis (Item 7)
    mda_pattern = re.compile(
        rf"{ITEM_WORD}\s+7\.?\s*MANAGEMENT['']?S?\s+DISCUSSION\s+AND\s+ANALYSIS(.*?)(?={ITEM_WORD}\s+7A|{ITEM_WORD}\s+8|$)",
        re.IGNORECASE | re.DOTALL
    )
    mda = _best_section_match(mda_pattern, normalized_text)
    if mda:
        sections["MD&A"] = mda
    
    # Extract Quantitative and Qualitative Disclosures (Item 7A)
    qqd_pattern = re.compile(
        rf'{ITEM_WORD}\s+7A\.?\s*QUANTITATIVE\s+AND\s+QUALITATIVE\s+DISCLOSURES(.*?)(?={ITEM_WORD}\s+8|$)',
        re.IGNORECASE | re.DOTALL
    )
    qqd = _best_section_match(qqd_pattern, normalized_text, min_len=300)
    if qqd:
        sections["Quantitative Disclosures"] = qqd
    
    # Extract Financial Statements (Item 8)
    financial_pattern = re.compile(
        rf'{ITEM_WORD}\s+8\.?\s*FINANCIAL\s+STATEMENTS(.*?)(?={ITEM_WORD}\s+9|{ITEM_WORD}\s+10|$)',
        re.IGNORECASE | re.DOTALL
    )
    financial = _best_section_match(financial_pattern, normalized_text, min_len=500)
    if financial:
        sections["Financial Statements"] = financial
    
    # Extract Controls and Procedures (Item 9A)
    controls_pattern = re.compile(
        rf'{ITEM_WORD}\s+9A\.?\s*CONTROLS\s+AND\s+PROCEDURES(.*?)(?={ITEM_WORD}\s+10|{ITEM_WORD}\s+15|$)',
        re.IGNORECASE | re.DOTALL
    )
    controls = _best_section_match(controls_pattern, normalized_text, min_len=200)
    if controls:
        sections["Controls and Procedures"] = controls
    
    # Extract Business Description (Item 1)
    business_pattern = re.compile(
        rf'{ITEM_WORD}\s+1\.?\s*BUSINESS(.*?)(?={ITEM_WORD}\s+1A|{ITEM_WORD}\s+2|$)',
        re.IGNORECASE | re.DOTALL
    )
    business = _best_section_match(business_pattern, normalized_text)
    if not business or len(business) < 1200:
        business_alt_pattern = re.compile(
            r'\bBUSINESS\b(.*?)(?=RISK\s+FACTORS|MANAGEMENT[’\\\']?S\s+DISCUSSION|FINANCIAL\s+STATEMENTS|PART\s+II|$)',
            re.IGNORECASE | re.DOTALL
        )
        business = _best_section_match(business_alt_pattern, normalized_text, min_len=1200)
    if business:
        sections["Business"] = business
    
    return sections

def extract_mda(text):
    sections = extract_sections(text)
    return sections.get("MD&A", text)

def extract_risk_factors(text):
    sections = extract_sections(text)
    return sections.get("Risk Factors", "")

