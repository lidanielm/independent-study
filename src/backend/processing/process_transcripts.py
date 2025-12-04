import re
import pandas as pd
import os
import sys
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from utils.sentiment_model import sentiment
from utils.nlp import get_embedding


def split_speakers(text):
    """
    Split transcript text by speaker.
    
    Args:
        text: Full transcript text
    
    Returns:
        List of tuples: [(speaker, snippet), ...]
    """
    # Try multiple patterns to match different transcript formats
    patterns = [
        r"^([A-Z][A-Za-z ]+?):\s*",  # Pattern for lines starting with speaker (multiline)
        r"([A-Z][A-Za-z ]+?):\s*",   # General pattern
    ]
    
    # Split by lines first to handle line-based speaker format
    lines = text.split('\n')
    segments = []
    current_speaker = None
    current_text = []
    
    for line in lines:
        line = line.strip()
        if not line:
            continue
        
        # Check if line starts with a speaker pattern
        speaker_match = None
        for pattern in patterns:
            match = re.match(pattern, line)
            if match:
                speaker_match = match.group(1).strip()
                break
        
        if speaker_match:
            # Save previous segment
            if current_speaker and current_text:
                segments.append((current_speaker, ' '.join(current_text)))
            # Start new segment
            current_speaker = speaker_match
            # Get text after speaker name
            text_part = line[len(speaker_match)+1:].strip()  # +1 for colon
            current_text = [text_part] if text_part else []
        else:
            # Continuation of current speaker's text
            if current_speaker:
                current_text.append(line)
            else:
                # No speaker identified yet, treat as continuation or new unknown speaker
                if current_text:
                    current_text.append(line)
                else:
                    current_speaker = "Unknown"
                    current_text = [line]
    
    # Add final segment
    if current_speaker and current_text:
        segments.append((current_speaker, ' '.join(current_text)))
    
    # If no segments found, return entire text as one segment
    if not segments:
        return [("Unknown", text)]
    
    return segments


def process_transcript_text(text):
    """
    Process a transcript text by splitting into segments and computing sentiment/embeddings.
    
    Args:
        text: Full transcript text
    
    Returns:
        List of dictionaries with speaker, text, sentiment, and embedding
    """
    rows = []
    
    # Try to split by speakers first
    speaker_segments = split_speakers(text)
    
    # If we got meaningful speaker segments, use them
    if len(speaker_segments) > 1 and speaker_segments[0][0] != "Unknown":
        for speaker, snippet in speaker_segments:
            if snippet.strip():  # Only process non-empty segments
                rows.append({
                    "speaker": speaker.strip(),
                    "text": snippet.strip(),
                    "sentiment": sentiment(snippet.strip()),
                    "embedding": get_embedding(snippet.strip())
                })
    else:
        # No speaker labels found, split by paragraphs instead
        paragraphs = [p.strip() for p in text.split('\n\n') if p.strip()]
        
        # Process in chunks (combine small paragraphs, split large ones)
        current_chunk = []
        chunk_size = 0
        max_chunk_size = 1000  # characters
        
        for para in paragraphs:
            if chunk_size + len(para) > max_chunk_size and current_chunk:
                # Save current chunk
                chunk_text = ' '.join(current_chunk)
                rows.append({
                    "speaker": "Transcript",
                    "text": chunk_text,
                    "sentiment": sentiment(chunk_text),
                    "embedding": get_embedding(chunk_text)
                })
                current_chunk = [para]
                chunk_size = len(para)
            else:
                current_chunk.append(para)
                chunk_size += len(para) + 1
        
        # Add final chunk
        if current_chunk:
            chunk_text = ' '.join(current_chunk)
            rows.append({
                "speaker": "Transcript",
                "text": chunk_text,
                "sentiment": sentiment(chunk_text),
                "embedding": get_embedding(chunk_text)
            })
    
    return rows


def process_transcript_file(input_path, output_path=None):
    """
    Process a transcript file (txt or parquet) by splitting into segments and computing features.
    
    Args:
        input_path: Path to raw transcript file (.txt or .parquet)
        output_path: Path to save processed transcript (default: same as input in processed directory)
    
    Returns:
        DataFrame with processed transcript segments
    """
    # Check if it's a text file or parquet
    if str(input_path).endswith('.txt'):
        # Read text file directly
        with open(input_path, "r", encoding="utf-8") as f:
            text = f.read()
    else:
        # Try to read as parquet
        try:
            df = pd.read_parquet(input_path)
            # Get transcript text (assuming first row or 'text' column)
            if "text" in df.columns:
                text = df["text"].iloc[0] if len(df) > 0 else ""
            else:
                text = str(df.iloc[0, 0]) if len(df) > 0 else ""
        except:
            # If parquet read fails, try as text
            with open(input_path, "r", encoding="utf-8") as f:
                text = f.read()
    
    if not text or not text.strip():
        print(f"Warning: Empty transcript file: {input_path}")
        return pd.DataFrame()
    
    # Process transcript
    rows = process_transcript_text(text)
    
    if not rows:
        print(f"Warning: No segments extracted from transcript: {input_path}")
        return pd.DataFrame()
    
    result_df = pd.DataFrame(rows)
    
    # Set output path if not provided
    if output_path is None:
        filename = os.path.basename(input_path).replace(".txt", ".parquet")
        output_path = os.path.join("data/processed/transcripts", filename)
    
    # Save processed data
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    result_df.to_parquet(output_path, index=False)
    
    return result_df


def process_transcript_from_text(text, output_path=None):
    """
    Process transcript text directly (not from a file).
    
    Args:
        text: Transcript text string
        output_path: Optional path to save processed transcript
    
    Returns:
        DataFrame with processed transcript segments
    """
    rows = process_transcript_text(text)
    result_df = pd.DataFrame(rows)
    
    if output_path:
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        result_df.to_parquet(output_path, index=False)
    
    return result_df
