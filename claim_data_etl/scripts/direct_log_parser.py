import os
import re
import sys
from datetime import datetime

# Get the log file path
airflow_home = os.path.expanduser('~/airflow')
log_file_path = os.path.join(airflow_home, 'hms.log-17-03-2025-3.log')

if not os.path.exists(log_file_path):
    print(f"Log file not found at {log_file_path}")
    sys.exit(1)

# Update the pattern based on the actual log format seen
log_pattern = re.compile(r'(\d{4}-\d{2}-\d{2}\s\d{2}:\d{2}:\d{2})\s+\[([^\]]+)\]\s+\[([^\]]+)\]\s+(\w+)\s+([^\s]+)\s+\(([^)]+)\)\s+:(.*)')
entries = []
current_entry = None
current_details = []
max_entries = 20

# Print file information
print(f"Reading log file: {log_file_path}")
print(f"File size: {os.path.getsize(log_file_path) / (1024*1024):.2f} MB")

# Extract a sample of log entries
with open(log_file_path, 'r') as log_file:
    line_count = 0
    for line in log_file:
        line_count += 1
        match = log_pattern.match(line.strip())
        
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
            timestamp, ip, user, log_level, function, file_info, message = match.groups()
            current_entry = {
                'timestamp': timestamp,
                'ip': ip,
                'user': user,
                'log_level': log_level,
                'function': function,
                'file_info': file_info,
                'message': message.strip(),
                'details': ''
            }
        elif line.strip() and current_entry:
            # Continuation of the previous entry (details)
            current_details.append(line.strip())
        elif line_count <= 100 and line.strip():
            # For the first 100 lines, print non-matching lines to help debug the pattern
            print(f"Non-matching line {line_count}: {line.strip()}")
    
    # Add the last entry if it exists
    if current_entry and len(entries) < max_entries:
        if current_details:
            current_entry['details'] = '\n'.join(current_details)
        entries.append(current_entry)
    
    print(f"Total lines processed: {line_count}")
    print(f"Total entries found: {len(entries)}")

# Print the sample entries
print("\n=== SAMPLE LOG ENTRIES ===\n")
if entries:
    for i, entry in enumerate(entries):
        print(f"Entry {i+1}:")
        print(f"  Timestamp: {entry['timestamp']}")
        print(f"  IP: {entry['ip']}")
        print(f"  User: {entry['user']}")
        print(f"  Log Level: {entry['log_level']}")
        print(f"  Function: {entry['function']}")
        print(f"  File Info: {entry['file_info']}")
        print(f"  Message: {entry['message']}")
        if entry['details']:
            print(f"  Details: {entry['details'][:100]}..." if len(entry['details']) > 100 else f"  Details: {entry['details']}")
        print()
else:
    print("No log entries were successfully parsed. The log pattern might need adjustment.")

# Try another pattern based on the first few lines
print("\n=== TRYING ALTERNATIVE PATTERN ===\n")

alt_pattern = re.compile(r'(\d{4}-\d{2}-\d{2}\s\d{2}:\d{2}:\d{2})\s+\[([^\]]+)\]\s+\[([^\]]+)\]\s+(\w+)\s+(.*)')
alt_entries = []
with open(log_file_path, 'r') as f:
    for i in range(20):  # Try the first 20 lines
        line = f.readline().strip()
        match = alt_pattern.match(line)
        if match:
            timestamp, ip, user, log_level, message = match.groups()
            entry = {
                'timestamp': timestamp,
                'ip': ip,
                'user': user,
                'log_level': log_level,
                'message': message
            }
            alt_entries.append(entry)
            print(f"Matched entry {i+1}:")
            print(f"  Timestamp: {entry['timestamp']}")
            print(f"  IP: {entry['ip']}")
            print(f"  User: {entry['user']}")
            print(f"  Log Level: {entry['log_level']}")
            print(f"  Message: {entry['message']}")
            print()

# Try one more pattern based on observation
print("\n=== ANALYZING RAW LOG LINES ===\n")

# Check the first few lines of the file to understand the format
with open(log_file_path, 'r') as f:
    first_lines = [f.readline().strip() for _ in range(20)]

print("First 20 lines of the log file:")
for i, line in enumerate(first_lines):
    print(f"{i+1}: {line}")

# Suggest a pattern
print("\n=== SUGGESTED LOG PATTERN ===\n")
suggested_pattern = r'(\d{4}-\d{2}-\d{2}\s\d{2}:\d{2}:\d{2})\s+\[([^\]]+)\]\s+\[([^\]]+)\]\s+(\w+)\s+(.*)'
print(f"Based on the log format, try using this pattern:")
print(suggested_pattern)

# Print statistics
log_levels = {}
components = {}

for entry in entries:
    level = entry['log_level']
    component = entry['function']
    
    log_levels[level] = log_levels.get(level, 0) + 1
    components[component] = components.get(component, 0) + 1

print("\n=== LOG LEVEL DISTRIBUTION ===\n")
for level, count in log_levels.items():
    print(f"{level}: {count} entries")

print("\n=== COMPONENT DISTRIBUTION ===\n")
for component, count in components.items():
    print(f"{component}: {count} entries")

# Try to detect the actual log pattern
print("\n=== PATTERN DETECTION ===\n")

# Check the first few lines of the file to understand the format
with open(log_file_path, 'r') as f:
    first_lines = [f.readline().strip() for _ in range(10)]

print("First few lines of the log file:")
for i, line in enumerate(first_lines):
    print(f"{i+1}: {line[:100]}..." if len(line) > 100 else f"{i+1}: {line}")

# Alternative patterns to try
print("\n=== ALTERNATIVE PATTERNS TO TRY ===\n")

alternative_patterns = [
    r'^(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2},\d{3})\s+(\w+)\s+\[([^\]]+)\]\s+(.*)$',  # Common log4j pattern
    r'^(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z)\s+(\w+)\s+(.*)$',  # ISO timestamp format
    r'^(\d{2}/\d{2}/\d{4} \d{2}:\d{2}:\d{2})\s+(\w+)\s+(.*)$',  # MM/DD/YYYY format
]

# Test alternative patterns on the first few lines
for i, pattern in enumerate(alternative_patterns):
    regex = re.compile(pattern)
    matches = 0
    
    for line in first_lines:
        if regex.match(line):
            matches += 1
    
    print(f"Pattern {i+1}: {pattern}")
    print(f"  Matches: {matches} of {len(first_lines)} lines")
    print() 