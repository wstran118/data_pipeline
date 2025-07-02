import csv
import sqlite3
import logging
import os
import json
import yaml
from datetime import datetime
import pandas as pd
from typing import List, Dict, Optional
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from time import perf_counter

# Configure logging
logging.basicConfig(
    filename='data_pipeline.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

@dataclass
class PipelineConfig:
    """Pipeline configuration settings."""
    input_file: str
    output_db: str
    batch_size: int = 1000
    max_workers: int = 4
    supported_formats: List[str] = None
    validation_rules: Dict = None

    @classmethod
    def from_yaml(cls, config_path: str) -> 'PipelineConfig':
        """Load configuration from YAML file."""
        try:
            with open(config_path, 'r') as f:
                config = yaml.safe_load(f)
            return cls(**config)
        except Exception as e:
            logging.error(f"Failed to load config: {str(e)}")
            raise

class DataPipeline:
    def __init__(self, config: PipelineConfig):
        """Initialize pipeline with configuration."""
        self.config = config
        self.logger = logging.getLogger(__name__)
        self.metrics = {'processed': 0, 'valid': 0, 'invalid': 0, 'execution_time': 0}
        self.start_time = perf_counter()

    def extract_data(self) -> List[Dict]:
        """Extract data from input file based on format."""
        try:
            self.logger.info(f"Extracting data from {self.config.input_file}")
            file_extension = os.path.splitext(self.config.input_file)[1].lower()
            
            if file_extension == '.csv':
                df = pd.read_csv(self.config.input_file)
            elif file_extension == '.json':
                with open(self.config.input_file, 'r') as f:
                    df = pd.DataFrame(json.load(f))
            else:
                raise ValueError(f"Unsupported file format: {file_extension}")

            return df.to_dict('records')
        except Exception as e:
            self.logger.error(f"Error extracting data: {str(e)}")
            raise

    def validate_data_batch(self, data: List[Dict]) -> List[Dict]:
        """Validate a batch of data records."""
        validated_data = []
        
        for record in data:
            try:
                # Apply validation rules from config
                for field, rules in self.config.validation_rules.items():
                    if field not in record:
                        self.logger.warning(f"Missing field {field} in record: {record}")
                        self.metrics['invalid'] += 1
                        continue
                    
                    value = record[field]
                    if 'type' in rules:
                        if rules['type'] == 'float' and not isinstance(value, (int, float)):
                            self.logger.warning(f"Invalid type for {field} in record: {record}")
                            self.metrics['invalid'] += 1
                            continue
                        if rules['type'] == 'date':
                            try:
                                datetime.strptime(value, '%Y-%m-%d')
                            except ValueError:
                                self.logger.warning(f"Invalid date format in record: {record}")
                                self.metrics['invalid'] += 1
                                continue
                    if 'min' in rules and value < rules['min']:
                        self.logger.warning(f"Value below minimum for {field} in record: {record}")
                        self.metrics['invalid'] += 1
                        continue

                validated_data.append(record)
                self.metrics['valid'] += 1
            except Exception as e:
                self.logger.error(f"Error validating record {record}: {str(e)}")
                self.metrics['invalid'] += 1
                continue

        return validated_data

    def transform_data_batch(self, data: List[Dict]) -> List[Dict]:
        """Transform a batch of data records."""
        transformed_data = []
        
        for record in data:
            try:
                transformed_record = record.copy()
                # Apply transformations
                transformed_record['amount_with_tax'] = round(record['amount'] * 1.1, 2)
                transformed_record['name'] = record['name'].title()
                transformed_record['processed_timestamp'] = datetime.now().isoformat()
                transformed_data.append(transformed_record)
            except Exception as e:
                self.logger.error(f"Error transforming record {record}: {str(e)}")
                continue

        return transformed_data

    def load_data(self, data: List[Dict]):
        """Load data into SQLite database."""
        try:
            conn = sqlite3.connect(self.config.output_db)
            cursor = conn.cursor()

            # Create table with additional fields
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS transactions (
                    id INTEGER PRIMARY KEY,
                    name TEXT,
                    amount REAL,
                    date TEXT,
                    amount_with_tax REAL,
                    processed_timestamp TEXT
                )
            ''')

            # Batch insert
            for i in range(0, len(data), self.config.batch_size):
                batch = data[i:i + self.config.batch_size]
                cursor.executemany('''
                    INSERT OR REPLACE INTO transactions 
                    (id, name, amount, date, amount_with_tax, processed_timestamp)
                    VALUES (?, ?, ?, ?, ?, ?)
                ''', [
                    (
                        record['id'],
                        record['name'],
                        record['amount'],
                        record['date'],
                        record['amount_with_tax'],
                        record['processed_timestamp']
                    ) for record in batch
                ])

            conn.commit()
            self.logger.info(f"Loaded {len(data)} records into database")
        except sqlite3.Error as e:
            self.logger.error(f"Database error: {str(e)}")
            raise
        finally:
            conn.close()

    def process_batch(self, batch: List[Dict]) -> List[Dict]:
        """Process a single batch through validation and transformation."""
        validated = self.validate_data_batch(batch)
        transformed = self.transform_data_batch(validated)
        return transformed

    def run_pipeline(self):
        """Execute the complete data pipeline with parallel processing."""
        self.logger.info("Starting advanced data pipeline")
        try:
            # Extract
            raw_data = self.extract_data()
            self.metrics['processed'] = len(raw_data)
            
            # Process in parallel
            transformed_data = []
            batches = [raw_data[i:i + self.config.batch_size] 
                      for i in range(0, len(raw_data), self.config.batch_size)]
            
            with ThreadPoolExecutor(max_workers=self.config.max_workers) as executor:
                future_to_batch = {executor.submit(self.process_batch, batch): batch 
                                 for batch in batches}
                for future in as_completed(future_to_batch):
                    transformed_data.extend(future.result())

            # Load
            self.load_data(transformed_data)
            
            # Calculate and log metrics
            self.metrics['execution_time'] = perf_counter() - self.start_time
            self.logger.info(f"Pipeline metrics: {json.dumps(self.metrics, indent=2)}")
            
            self.logger.info("Data pipeline completed successfully")
            return True
        except Exception as e:
            self.logger.error(f"Pipeline failed: {str(e)}")
            return False

def main():
    # Sample configuration
    config_data = {
        'input_file': 'input_data.csv',
        'output_db': 'transactions.db',
        'batch_size': 1000,
        'max_workers': 4,
        'supported_formats': ['csv', 'json'],
        'validation_rules': {
            'id': {'type': 'int'},
            'name': {'type': 'str'},
            'amount': {'type': 'float', 'min': 0.0},
            'date': {'type': 'date'}
        }
    }

    # Create sample config file
    config_path = 'pipeline_config.yaml'
    if not os.path.exists(config_path):
        with open(config_path, 'w') as f:
            yaml.dump(config_data, f)

    # Create sample input data
    input_file = config_data['input_file']
    if not os.path.exists(input_file):
        sample_data = [
            {'id': 1, 'name': 'john doe', 'amount': 100.0, 'date': '2025-01-01'},
            {'id': 2, 'name': 'jane smith', 'amount': 200.0, 'date': '2025-01-02'},
            {'id': 3, 'name': 'bob johnson', 'amount': -50.0, 'date': '2025-01-03'},
            {'id': 4, 'name': 'alice brown', 'amount': 150.0, 'date': 'invalid-date'},
            {'id': 5, 'name': 'charlie davis', 'amount': 300.0, 'date': '2025-01-04'}
        ]
        with open(input_file, 'w', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=['id', 'name', 'amount', 'date'])
            writer.writeheader()
            writer.writerows(sample_data)

    # Initialize and run pipeline
    config = PipelineConfig.from_yaml(config_path)
    pipeline = DataPipeline(config)
    pipeline.run_pipeline()

if __name__ == '__main__':
    main()