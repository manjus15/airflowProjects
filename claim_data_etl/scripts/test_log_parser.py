import os
import re
import csv
import pandas as pd
import sys
from datetime import datetime
import json

# Add parent directory to path for importing LogProcessor
parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if parent_dir not in sys.path:
    sys.path.append(parent_dir)

try:
    from scripts.log_processor import LogProcessor
except ImportError:
    # Define a simplified log pattern for testing
    class LogProcessor:
        def __init__(self, log_file_path):
            self.log_file_path = log_file_path
            self.log_pattern = re.compile(r'(\d{4}-\d{2}-\d{2}\s\d{2}:\d{2}:\d{2})\s+(\w+)\s+\[([^\]]+)\]\s+(.*)')
        
        def extract_sample(self, max_entries=20):
            """Extract a sample of log entries for testing"""
            entries = []
            current_entry = None
            current_details = []
            
            try:
                with open(self.log_file_path, 'r') as log_file:
                    for line in log_file:
                        match = self.log_pattern.match(line.strip())
                        
                        if match:
                            # If there's a previous entry, add it to the list
                            if current_entry:
                                if current_details:
                                    current_entry['details'] = '\n'.join(current_details)
                                entries.append(current_entry)
                                current_details = []
                                
                                # Stop if we've reached the maximum number of entries
                                if len(entries) >= max_entries:
                                    break
                            
                            # Start a new log entry
                            timestamp, log_level, component, message = match.groups()
                            current_entry = {
                                'timestamp': timestamp,
                                'log_level': log_level,
                                'component': component,
                                'message': message,
                                'details': ''
                            }
                        elif current_entry:
                            # Continuation of the previous entry (details)
                            current_details.append(line.strip())
                
                # Add the last entry if it exists
                if current_entry and len(entries) < max_entries:
                    if current_details:
                        current_entry['details'] = '\n'.join(current_details)
                    entries.append(current_entry)
                
                return entries
            
            except Exception as e:
                print(f"Error reading log file: {str(e)}")
                return []

# Main execution
if __name__ == "__main__":
    # Get the log file path
    airflow_home = os.path.expanduser('~/airflow')
    log_file_path = os.path.join(airflow_home, 'hms.log-17-03-2025-3.log')
    
    if not os.path.exists(log_file_path):
        print(f"Log file not found at {log_file_path}")
        sys.exit(1)
    
    # Initialize the log processor
    processor = LogProcessor(log_file_path)
    
    # Extract a sample of log entries
    print(f"Reading log file: {log_file_path}")
    print(f"File size: {os.path.getsize(log_file_path) / (1024*1024):.2f} MB")
    
    # Get a sample of log entries
    entries = processor.extract_sample(max_entries=20)
    
    # Print the sample entries
    print("\n=== SAMPLE LOG ENTRIES ===\n")
    for i, entry in enumerate(entries):
        print(f"Entry {i+1}:")
        print(f"  Timestamp: {entry['timestamp']}")
        print(f"  Log Level: {entry['log_level']}")
        print(f"  Component: {entry['component']}")
        print(f"  Message: {entry['message']}")
        if entry['details']:
            print(f"  Details: {entry['details'][:100]}..." if len(entry['details']) > 100 else f"  Details: {entry['details']}")
        print()
    
    # Print statistics
    log_levels = {}
    components = {}
    
    for entry in entries:
        level = entry['log_level']
        component = entry['component']
        
        log_levels[level] = log_levels.get(level, 0) + 1
        components[component] = components.get(component, 0) + 1
    
    print("\n=== LOG LEVEL DISTRIBUTION ===\n")
    for level, count in log_levels.items():
        print(f"{level}: {count} entries")
    
    print("\n=== COMPONENT DISTRIBUTION ===\n")
    for component, count in components.items():
        print(f"{component}: {count} entries")
    
    # Now try to detect the actual log pattern
    print("\n=== PATTERN DETECTION ===\n")
    
    # Check the first few lines of the file to try to determine the pattern
    with open(log_file_path, 'r') as f:
        first_lines = [f.readline().strip() for _ in range(10)]
    
    print("First few lines of the log file:")
    for i, line in enumerate(first_lines):
        print(f"{i+1}: {line[:100]}..." if len(line) > 100 else f"{i+1}: {line}")
    
    print("\nTry updating the log pattern in scripts/log_processor.py if the current pattern doesn't match your log format.") 