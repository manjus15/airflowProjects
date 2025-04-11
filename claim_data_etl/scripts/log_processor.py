import os
import re
import csv
import pandas as pd
import logging
from typing import Dict, List, Tuple, Optional

logger = logging.getLogger(__name__)

class LogProcessor:
    """
    Utility class for processing HMS log files
    """
    
    def __init__(self, log_file_path: str):
        """
        Initialize the log processor
        
        Args:
            log_file_path: Path to the log file
        """
        self.log_file_path = log_file_path
        self.output_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'processed_data')
        
        # Create output directory if it doesn't exist
        if not os.path.exists(self.output_dir):
            os.makedirs(self.output_dir, exist_ok=True)
        
        # Updated log pattern based on the actual log format
        self.log_pattern = re.compile(r'(\d{4}-\d{2}-\d{2}\s\d{2}:\d{2}:\d{2})\s+\[([^\]]+)\]\s+\[([^\]]+)\]\s+(\w+)\s+(.*)')
    
    def extract(self) -> str:
        """
        Extract structured data from log file
        
        Returns:
            Path to the extracted CSV file
        """
        output_csv = os.path.join(self.output_dir, 'processed_log_data.csv')
        
        # Define CSV structure based on the updated log format
        fieldnames = ['timestamp', 'ip', 'username', 'log_level', 'message', 'details']
        
        # Process the log file
        try:
            with open(self.log_file_path, 'r') as log_file, open(output_csv, 'w', newline='') as csv_file:
                csv_writer = csv.DictWriter(csv_file, fieldnames=fieldnames)
                csv_writer.writeheader()
                
                # Process line by line
                current_log_entry = {}
                current_details = []
                entry_count = 0
                
                for line in log_file:
                    match = self.log_pattern.match(line.strip())
                    
                    if match:
                        # If there's a previous entry, write it
                        if current_log_entry:
                            if current_details:
                                current_log_entry['details'] = '\n'.join(current_details)
                            csv_writer.writerow(current_log_entry)
                            entry_count += 1
                            current_details = []
                        
                        # Start a new log entry with the updated format
                        timestamp, ip, username, log_level, message = match.groups()
                        current_log_entry = {
                            'timestamp': timestamp,
                            'ip': ip,
                            'username': username,
                            'log_level': log_level,
                            'message': message.strip(),
                            'details': ''
                        }
                    elif line.strip() and current_log_entry:
                        # Continuation of the previous entry (details)
                        current_details.append(line.strip())
                
                # Write the last entry if exists
                if current_log_entry:
                    if current_details:
                        current_log_entry['details'] = '\n'.join(current_details)
                    csv_writer.writerow(current_log_entry)
                    entry_count += 1
            
            logger.info(f"Extracted {entry_count} log entries to {output_csv}")
            return output_csv
        
        except Exception as e:
            logger.error(f"Error extracting log data: {str(e)}")
            raise
    
    def transform(self, csv_file_path: str) -> Tuple[str, Dict[str, str]]:
        """
        Transform the extracted log data for analysis
        
        Args:
            csv_file_path: Path to the extracted CSV file
        
        Returns:
            Tuple of transformed CSV path and a dictionary with additional analysis file paths
        """
        try:
            # Read the CSV file
            df = pd.read_csv(csv_file_path)
            
            # Extract method/function name and file info from message
            df['function'] = df['message'].apply(lambda x: self._extract_function_name(x))
            df['file_info'] = df['message'].apply(lambda x: self._extract_file_info(x))
            df['clean_message'] = df['message'].apply(lambda x: self._extract_clean_message(x))
            
            # Convert timestamp to datetime
            df['timestamp'] = pd.to_datetime(df['timestamp'])
            
            # Add additional columns for analysis
            df['date'] = df['timestamp'].dt.date
            df['hour'] = df['timestamp'].dt.hour
            df['minute'] = df['timestamp'].dt.minute
            
            # Calculate time differences between consecutive logs
            df['next_timestamp'] = df['timestamp'].shift(-1)
            df['time_diff_seconds'] = (df['next_timestamp'] - df['timestamp']).dt.total_seconds()
            
            # Count occurrences of different log levels by component
            level_counts = {}
            for level in df['log_level'].unique():
                level_counts[f"{level.lower()}_counts"] = os.path.join(
                    self.output_dir, f"{level.lower()}_counts.csv")
                level_df = df[df['log_level'] == level].groupby('function').size().reset_index(name=f'{level.lower()}_count')
                level_df.to_csv(level_counts[f"{level.lower()}_counts"], index=False)
            
            # Create hourly summary
            hourly_summary = df.groupby(['date', 'hour']).agg({
                'log_level': 'count',
                'time_diff_seconds': ['mean', 'max']
            }).reset_index()
            hourly_summary.columns = ['date', 'hour', 'log_count', 'avg_time_diff', 'max_time_diff']
            hourly_summary_path = os.path.join(self.output_dir, 'hourly_summary.csv')
            hourly_summary.to_csv(hourly_summary_path, index=False)
            level_counts['hourly_summary'] = hourly_summary_path
            
            # Generate IP-based summary
            ip_summary = df.groupby('ip').size().reset_index(name='count')
            ip_summary_path = os.path.join(self.output_dir, 'ip_summary.csv')
            ip_summary.to_csv(ip_summary_path, index=False)
            level_counts['ip_summary'] = ip_summary_path
            
            # Generate user-based summary
            username_summary = df.groupby('username').size().reset_index(name='count')
            username_summary_path = os.path.join(self.output_dir, 'username_summary.csv')
            username_summary.to_csv(username_summary_path, index=False)
            level_counts['username_summary'] = username_summary_path
            
            # Save the transformed data
            transformed_csv = os.path.join(self.output_dir, 'transformed_log_data.csv')
            df = df.drop(columns=['next_timestamp'])  # Remove temporary column
            df.to_csv(transformed_csv, index=False)
            
            logger.info(f"Transformed log data saved to {transformed_csv}")
            return transformed_csv, level_counts
        
        except Exception as e:
            logger.error(f"Error transforming log data: {str(e)}")
            raise
    
    def _extract_function_name(self, message: str) -> str:
        """Extract function name from message"""
        try:
            # First check for the pattern: function_name (file_info) : message
            pattern = r'^(\S+)\s+\(([^)]+)\)\s+:'
            match = re.match(pattern, message)
            if match:
                return match.group(1)
            return 'unknown'
        except:
            return 'unknown'
    
    def _extract_file_info(self, message: str) -> str:
        """Extract file info from message"""
        try:
            pattern = r'^(\S+)\s+\(([^)]+)\)\s+:'
            match = re.match(pattern, message)
            if match:
                return match.group(2)
            return ''
        except:
            return ''
    
    def _extract_clean_message(self, message: str) -> str:
        """Extract clean message by removing function and file info"""
        try:
            pattern = r'^(\S+)\s+\(([^)]+)\)\s+:\s*(.*)'
            match = re.match(pattern, message)
            if match:
                return match.group(3)
            return message
        except:
            return message
    
    def generate_stats(self, transformed_csv: str) -> str:
        """
        Generate statistical summary of the log data
        
        Args:
            transformed_csv: Path to the transformed CSV file
        
        Returns:
            Path to the statistics CSV file
        """
        try:
            # Read the transformed data
            df = pd.read_csv(transformed_csv)
            
            # Generate statistics
            stats = {
                'total_logs': len(df),
                'date_range': [df['date'].min(), df['date'].max()],
                'log_levels': df['log_level'].value_counts().to_dict(),
                'top_functions': df['function'].value_counts().head(10).to_dict(),
                'top_usernames': df['username'].value_counts().head(10).to_dict(),
                'top_ips': df['ip'].value_counts().head(10).to_dict(),
                'avg_logs_per_hour': df.groupby(['date', 'hour']).size().mean(),
                'max_logs_per_hour': df.groupby(['date', 'hour']).size().max()
            }
            
            # Convert to DataFrame for easier handling in Superset
            stats_df = pd.DataFrame([{
                'metric': key,
                'value': str(value)
            } for key, value in stats.items()])
            
            # Save to CSV
            stats_csv = os.path.join(self.output_dir, 'log_stats.csv')
            stats_df.to_csv(stats_csv, index=False)
            
            logger.info(f"Statistics generated and saved to {stats_csv}")
            return stats_csv
        
        except Exception as e:
            logger.error(f"Error generating statistics: {str(e)}")
            raise


if __name__ == "__main__":
    # Example usage
    log_file = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'hms.log-17-03-2025-3.log')
    processor = LogProcessor(log_file)
    
    # Extract
    csv_path = processor.extract()
    
    # Transform
    transformed_csv, analysis_files = processor.transform(csv_path)
    
    # Generate statistics
    stats_csv = processor.generate_stats(transformed_csv)
    
    print(f"Log processing complete. Results saved to {processor.output_dir}") 