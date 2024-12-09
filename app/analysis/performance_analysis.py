import pandas as pd
import numpy as np
from typing import Dict, Any, List
import matplotlib.pyplot as plt
import seaborn as sns
from datetime import datetime
import os
import json

class PerformanceAnalyzer:
    """Analyzes and saves performance data from benchmarks"""
    
    def __init__(self, performance_data=None):
        """Initialize the analyzer with optional performance data"""
        self.df = None
        if performance_data:
            self.create_dataframe(performance_data)
        
    def create_dataframe(self, data: List[Dict[str, Any]]) -> pd.DataFrame:
        """
        Create a pandas DataFrame from the raw performance data
        
        Args:
            data: List of dictionaries containing performance metrics
                Each dict should have:
                - payment_type: str
                - router_type: str
                - database: str
                - db_persistence_time_ms: float
                - kafka_time_ms: float
                - total_time_ms: float
        
        Returns:
            pd.DataFrame: Processed performance data
        """
        self.df = pd.DataFrame(data)
        return self.df
    
    def get_distribution_by_payment_and_db(self) -> Dict[str, Any]:
        """
        Compute distribution statistics grouped by payment type and database
        
        Returns:
            Dict containing:
            - summary_stats: Basic statistics for each metric
            - percentiles: Key percentiles (25, 50, 75, 90, 95, 99)
        """
        if self.df is None:
            raise ValueError("DataFrame not initialized. Call create_dataframe first.")
            
        metrics = ['db_persistence_time_ms', 'kafka_time_ms', 'total_time_ms']
        groups = ['payment_type', 'database']
        
        summary = {}
        
        # Basic statistics
        summary['summary_stats'] = self.df.groupby(groups)[metrics].agg([
            'count', 'mean', 'std', 'min', 'max'
        ]).round(2)
        
        # Percentiles
        summary['percentiles'] = self.df.groupby(groups)[metrics].agg([
            lambda x: np.percentile(x, 25),
            lambda x: np.percentile(x, 50),
            lambda x: np.percentile(x, 75),
            lambda x: np.percentile(x, 90),
            lambda x: np.percentile(x, 95),
            lambda x: np.percentile(x, 99)
        ]).round(2)
        
        return summary
    
    def get_distribution_by_payment_router_and_db(self) -> Dict[str, Any]:
        """
        Compute distribution statistics grouped by payment type, router type, and database
        
        Returns:
            Dict containing:
            - summary_stats: Basic statistics for each metric
            - percentiles: Key percentiles (25, 50, 75, 90, 95, 99)
        """
        if self.df is None:
            raise ValueError("DataFrame not initialized. Call create_dataframe first.")
            
        metrics = ['db_persistence_time_ms', 'kafka_time_ms', 'total_time_ms']
        groups = ['payment_type', 'router_type', 'database']
        
        summary = {}
        
        # Basic statistics
        summary['summary_stats'] = self.df.groupby(groups)[metrics].agg([
            'count', 'mean', 'std', 'min', 'max'
        ]).round(2)
        
        # Percentiles
        summary['percentiles'] = self.df.groupby(groups)[metrics].agg([
            lambda x: np.percentile(x, 25),
            lambda x: np.percentile(x, 50),
            lambda x: np.percentile(x, 75),
            lambda x: np.percentile(x, 90),
            lambda x: np.percentile(x, 95),
            lambda x: np.percentile(x, 99)
        ]).round(2)
        
        return summary
    
    def save_results(self, output_dir: str, timestamp: str = None) -> Dict[str, str]:
        """Save results in multiple formats"""
        if self.df is None:
            raise ValueError("No data to save. Call create_dataframe first.")
            
        # Create timestamp if not provided
        if timestamp is None:
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            
        # Create output directory if it doesn't exist
        os.makedirs(output_dir, exist_ok=True)
        
        # Save as CSV
        csv_path = os.path.join(output_dir, f'performance_data_{timestamp}.csv')
        self.df.to_csv(csv_path, index=False)
        
        # Save as JSON with stats
        stats = {
            'summary': {
                'total_payments': len(self.df),
                'avg_processing_time_ms': self.df['total_time_ms'].mean(),
                'p95_processing_time_ms': self.df['total_time_ms'].quantile(0.95),
                'avg_db_time_ms': self.df['db_persistence_time_ms'].mean(),
                'avg_kafka_time_ms': self.df['kafka_time_ms'].mean()
            },
            'by_payment_type': {},
            'by_database': {},
            'timestamp': timestamp
        }
        
        # Add stats by payment type
        for ptype in self.df['payment_type'].unique():
            type_data = self.df[self.df['payment_type'] == ptype]
            stats['by_payment_type'][ptype] = {
                'count': len(type_data),
                'avg_processing_time_ms': type_data['total_time_ms'].mean(),
                'p95_processing_time_ms': type_data['total_time_ms'].quantile(0.95)
            }
            
        # Add stats by database
        for db in self.df['database'].unique():
            db_data = self.df[self.df['database'] == db]
            stats['by_database'][db] = {
                'count': len(db_data),
                'avg_persistence_time_ms': db_data['db_persistence_time_ms'].mean(),
                'p95_persistence_time_ms': db_data['db_persistence_time_ms'].quantile(0.95)
            }
            
        stats_path = os.path.join(output_dir, f'performance_stats_{timestamp}.json')
        with open(stats_path, 'w') as f:
            json.dump(stats, f, indent=2)
            
        return {
            'csv_path': csv_path,
            'stats_path': stats_path
        }
    
    def generate_summary_report(self, output_file: str):
        """
        Generate a comprehensive summary report in HTML format
        
        Args:
            output_file: Path to save the HTML report
        """
        if self.df is None:
            raise ValueError("DataFrame not initialized. Call create_dataframe first.")
        
        # Get summaries
        db_summary = self.get_distribution_by_payment_and_db()
        router_summary = self.get_distribution_by_payment_router_and_db()
        
        # Create HTML report
        html_content = f"""
        <!DOCTYPE html>
        <html>
        <head>
            <title>Performance Analysis Report</title>
            <link href="https://cdn.jsdelivr.net/npm/bootstrap@5.1.3/dist/css/bootstrap.min.css" rel="stylesheet">
            <style>
                body {{ padding: 20px; }}
                .card {{ margin-bottom: 20px; }}
            </style>
        </head>
        <body>
            <div class="container">
                <h1>Performance Analysis Report</h1>
                <p class="lead">Generated on {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</p>
                
                <div class="card">
                    <div class="card-header">
                        <h5>Dataset Overview</h5>
                    </div>
                    <div class="card-body">
                        <p>Total Records: {len(self.df)}</p>
                        <p>Payment Types: {', '.join(sorted(self.df['payment_type'].unique()))}</p>
                        <p>Databases: {', '.join(sorted(self.df['database'].unique()))}</p>
                        <p>Router Types: {', '.join(sorted(self.df['router_type'].unique()))}</p>
                    </div>
                </div>
                
                <div class="card">
                    <div class="card-header">
                        <h5>Distribution by Payment Type and Database</h5>
                    </div>
                    <div class="card-body">
                        <h6>Summary Statistics</h6>
                        {db_summary['summary_stats'].to_html(classes='table table-striped')}
                        
                        <h6 class="mt-4">Percentiles</h6>
                        {db_summary['percentiles'].to_html(classes='table table-striped')}
                    </div>
                </div>
                
                <div class="card">
                    <div class="card-header">
                        <h5>Distribution by Payment Type, Router Type, and Database</h5>
                    </div>
                    <div class="card-body">
                        <h6>Summary Statistics</h6>
                        {router_summary['summary_stats'].to_html(classes='table table-striped')}
                        
                        <h6 class="mt-4">Percentiles</h6>
                        {router_summary['percentiles'].to_html(classes='table table-striped')}
                    </div>
                </div>
            </div>
        </body>
        </html>
        """
        
        with open(output_file, 'w') as f:
            f.write(html_content)
