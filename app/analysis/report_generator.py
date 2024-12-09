import pandas as pd
import plotly.graph_objects as go
import plotly.express as px
from plotly.subplots import make_subplots
from datetime import datetime
import json
from typing import Dict, Any, List
import numpy as np
import os
import glob

class ReportGenerator:
    def __init__(self, performance_data: List[Dict[str, Any]], metrics_report: Dict[str, Any]):
        self.df = pd.DataFrame(performance_data)
        self.metrics = metrics_report
        
    def generate_introduction(self) -> str:
        """Generate the introduction section of the report"""
        intro = """
        <div class="section">
            <h2>Polymorphic Payment Processing System</h2>
            <p>This benchmark report analyzes the performance of our polymorphic payment processing system, which handles multiple payment types through a unified interface while maintaining type safety and specific business rules for each payment type.</p>
            
            <h3>Payment Types</h3>
            <ul>
                <li><strong>SWIFT</strong>: International wire transfers using the SWIFT network</li>
                <li><strong>ACH</strong>: Automated Clearing House transfers for domestic US payments</li>
                <li><strong>SEPA</strong>: Single Euro Payments Area transfers for European payments</li>
                <li><strong>WIRE</strong>: Traditional wire transfers</li>
                <li><strong>CRYPTO</strong>: Cryptocurrency transactions</li>
                <li><strong>RTP</strong>: Real-Time Payments</li>
            </ul>
            
            <h3>Polymorphic Model Architecture</h3>
            <p>Our system uses a polymorphic data model that:</p>
            <ul>
                <li>Maintains type safety through Pydantic models</li>
                <li>Supports different validation rules per payment type</li>
                <li>Enables efficient routing based on payment characteristics</li>
                <li>Provides flexible storage strategies across multiple databases</li>
            </ul>
        </div>
        """
        return intro
    
    def generate_distribution_summary(self) -> str:
        """Generate summary of payment type and database distribution with timing statistics"""
        # Calculate payment type statistics
        payment_stats_db = self.df.groupby('payment_type').agg({
            'persistence_time': ['count', 'mean', 'median', 'std', 'min', 'max', lambda x: np.percentile(x, 95)]
        }).round(2)
        
        payment_stats_router = self.df.groupby('payment_type').agg({
            'router_processing_time': ['count', 'mean', 'median', 'std', 'min', 'max', lambda x: np.percentile(x, 95)]
        }).round(2)
        
        payment_stats_total = self.df.groupby('payment_type').agg({
            'total_time_ms': ['count', 'mean', 'median', 'std', 'min', 'max', lambda x: np.percentile(x, 95)]
        }).round(2)
        
        # Rename columns for clarity
        payment_stats_db.columns = [
            'Count', 'Avg DB Time', 'Median DB Time', 'Std DB Time', 'Min DB Time', 'Max DB Time', 'P95 DB Time'
        ]
        
        payment_stats_router.columns = [
            'Count', 'Avg Router Time', 'Median Router Time', 'Std Router Time', 'Min Router Time', 'Max Router Time', 'P95 Router Time'
        ]
        
        payment_stats_total.columns = [
            'Count', 'Avg Total Time', 'Median Total Time', 'Std Total Time', 'Min Total Time', 'Max Total Time', 'P95 Total Time'
        ]
        
        # Calculate database statistics
        db_stats = self.df.groupby('database_type').agg({
            'total_time_ms': ['count', 'mean', 'median', 'std', 'min', 'max', lambda x: np.percentile(x, 95)],
            'persistence_time': ['mean', 'median', 'std', 'min', 'max', lambda x: np.percentile(x, 95)]
        }).round(2)
        
        # Rename columns for clarity
        db_stats.columns = [
            'Count', 'Avg Total Time', 'Median Total Time', 'Std Total Time', 'Min Total Time', 'Max Total Time', 'P95 Total Time',
            'Avg DB Time', 'Median DB Time', 'Std DB Time', 'Min DB Time', 'Max DB Time', 'P95 DB Time'
        ]
        
        # Filter out outliers for plotting
        df_filtered = self.df.copy()
        for col in ['total_time_ms', 'persistence_time', 'router_processing_time']:
            df_filtered.loc[df_filtered[col] > 100, col] = None
        
        # Create distribution plots
        fig_payments = go.Figure()
        for col in ['total_time_ms', 'persistence_time', 'router_processing_time']:
            fig_payments.add_trace(go.Box(
                y=df_filtered[col],
                x=df_filtered['payment_type'],
                name=col.replace('_', ' ').title(),
                boxmean=True
            ))
        fig_payments.update_layout(
            title="Payment Type Timing Distribution (≤ 100ms)",
            xaxis_title="Payment Type",
            yaxis_title="Time (ms)",
            boxmode='group',
            yaxis_range=[0, 100]
        )
        
        fig_db = go.Figure()
        for col in ['total_time_ms', 'persistence_time']:
            fig_db.add_trace(go.Box(
                y=df_filtered[col],
                x=df_filtered['database_type'],
                name=col.replace('_', ' ').title(),
                boxmean=True
            ))
        fig_db.update_layout(
            title="Database Timing Distribution (≤ 100ms)",
            xaxis_title="Database Type",
            yaxis_title="Time (ms)",
            boxmode='group',
            yaxis_range=[0, 100]
        )
        
        # Calculate percentage of outliers
        outlier_stats = {}
        for col in ['total_time_ms', 'persistence_time', 'router_processing_time']:
            total = len(self.df[col].dropna())
            outliers = len(self.df[self.df[col] > 100][col].dropna())
            pct = (outliers / total * 100) if total > 0 else 0
            outlier_stats[col] = f"{outliers:,} ({pct:.1f}%)"
        
        outlier_info = f"""
        <div class="alert alert-info">
            <strong>Note:</strong> Outliers (>100ms) excluded from plots:
            <ul>
                <li>Total Time: {outlier_stats['total_time_ms']} measurements</li>
                <li>DB Time: {outlier_stats['persistence_time']} measurements</li>
                <li>Router Time: {outlier_stats['router_processing_time']} measurements</li>
            </ul>
        </div>
        """
        
        summary = f"""
        <div class="section">
            <h2>Distribution Analysis</h2>
            {outlier_info}
            
            <div class="row">
                <div class="col-12">
                    <h3>Payment Type Distribution</h3>
                    <h4>Database Time</h4>
                    <div class="table-responsive">
                        {payment_stats_db.to_html(classes='table table-striped table-sm', 
                                            float_format=lambda x: f'{x:,.2f}' if pd.notnull(x) else '')}
                    </div>
                    <h4>Router Processing Time</h4>
                    <div class="table-responsive">
                        {payment_stats_router.to_html(classes='table table-striped table-sm',
                                            float_format=lambda x: f'{x:,.2f}' if pd.notnull(x) else '')}
                    </div>
                    <h4>Total Time</h4>
                    <div class="table-responsive">
                        {payment_stats_total.to_html(classes='table table-striped table-sm',
                                            float_format=lambda x: f'{x:,.2f}' if pd.notnull(x) else '')}
                    </div>
                    <div class="plot-container">
                        {fig_payments.to_html(full_html=False, include_plotlyjs=False)}
                    </div>
                </div>
            </div>
            
            <div class="row mt-4">
                <div class="col-12">
                    <h3>Database Distribution</h3>
                    <div class="table-responsive">
                        {db_stats.to_html(classes='table table-striped table-sm',
                                        float_format=lambda x: f'{x:,.2f}' if pd.notnull(x) else '')}
                    </div>
                    <div class="plot-container">
                        {fig_db.to_html(full_html=False, include_plotlyjs=False)}
                    </div>
                </div>
            </div>
        </div>
        """
        return summary
    
    def generate_summary_section(self) -> str:
        """Generate the summary section with key metrics"""
        # Calculate summary statistics
        total_payments = len(self.df)
        avg_processing_time = self.df['total_time_ms'].mean()
        p95_processing_time = self.df['total_time_ms'].quantile(0.95)
        
        # Create summary by payment type
        payment_type_summary = self.df.groupby('payment_type').agg({
            'total_time_ms': ['count', 'mean', 'median', lambda x: np.percentile(x, 95)],
            'persistence_time': ['mean', 'median', lambda x: np.percentile(x, 95)],
            'router_processing_time': ['mean', 'median', lambda x: np.percentile(x, 95)]
        }).round(2)
        
        payment_type_table = payment_type_summary.to_html(
            classes='table table-striped',
            float_format=lambda x: '{:.2f}'.format(x) if pd.notnull(x) else ''
        )
        
        # Create summary by router type
        router_summary = self.df.groupby('router_type').agg({
            'total_time_ms': ['count', 'mean', 'median', lambda x: np.percentile(x, 95)],
            'router_processing_time': ['mean', 'median', lambda x: np.percentile(x, 95)]
        }).round(2)
        
        router_type_table = router_summary.to_html(
            classes='table table-striped',
            float_format=lambda x: '{:.2f}'.format(x) if pd.notnull(x) else ''
        )
        
        summary = f"""
        <div class="section">
            <h2>Performance Summary</h2>
            
            <div class="summary-stats">
                <div class="stat-box">
                    <h4>Total Payments</h4>
                    <p>{total_payments:,}</p>
                </div>
                <div class="stat-box">
                    <h4>Avg Processing Time</h4>
                    <p>{avg_processing_time:.2f} ms</p>
                </div>
                <div class="stat-box">
                    <h4>P95 Processing Time</h4>
                    <p>{p95_processing_time:.2f} ms</p>
                </div>
            </div>
            
            <h3>Payment Type Performance</h3>
            {payment_type_table}
            
            <h3>Router Type Performance</h3>
            {router_type_table}
        </div>
        """
        return summary
    
    def _create_persistence_time_plot(self) -> str:
        """Create plot for persistence time across databases"""
        # Create box plot for persistence times by database type
        fig = go.Figure()

        for db_type in self.df['database_type'].unique():
            db_data = self.df[self.df['database_type'] == db_type]['persistence_time']
            
            fig.add_trace(go.Box(
                y=db_data,
                name=db_type,
                boxpoints='outliers',
                jitter=0.3,
                pointpos=-1.8
            ))
        
        fig.update_layout(
            title="Database Persistence Times",
            xaxis_title="",
            yaxis_title="Persistence Time (seconds)",
            showlegend=True
        )
        return fig.to_html(full_html=False, include_plotlyjs='cdn')
    
    def _create_processing_time_plot(self) -> str:
        """Create plot for total processing time by payment type"""
        fig = go.Figure()

        for payment_type in self.df['payment_type'].unique():
            type_data = self.df[self.df['payment_type'] == payment_type]['total_time_ms']
            
            fig.add_trace(go.Box(
                y=type_data,
                name=payment_type,
                boxpoints='outliers',
                jitter=0.3,
                pointpos=-1.8
            ))
        
        fig.update_layout(
            title="Total Processing Time by Payment Type",
            xaxis_title="",
            yaxis_title="Processing Time (ms)",
            showlegend=True
        )
        return fig.to_html(full_html=False, include_plotlyjs=False)
    
    def _create_router_performance_plot(self) -> str:
        """Create plot for router performance"""
        fig = go.Figure()

        for router_type in self.df['router_type'].unique():
            router_data = self.df[self.df['router_type'] == router_type]['router_processing_time']
            
            fig.add_trace(go.Box(
                y=router_data,
                name=router_type,
                boxpoints='outliers',
                jitter=0.3,
                pointpos=-1.8
            ))
        
        fig.update_layout(
            title="Router Performance",
            xaxis_title="",
            yaxis_title="Processing Time (seconds)",
            showlegend=True
        )
        return fig.to_html(full_html=False, include_plotlyjs=False)
    
    def create_interactive_plots(self) -> str:
        """Create interactive plots using plotly"""
        persistence_time_plot = self._create_persistence_time_plot()
        processing_time_plot = self._create_processing_time_plot()
        router_performance_plot = self._create_router_performance_plot()
        
        return f"""
        <div class="plot-section">
            <div class="plot-container">
                {persistence_time_plot}
            </div>
            
            <div class="plot-container">
                {processing_time_plot}
            </div>
            
            <div class="plot-container">
                {router_performance_plot}
            </div>
        </div>
        """
    
    def cleanup_old_results(self, results_dir: str, max_files: int = 3):
        """Cleanup old result files keeping only the most recent ones"""
        # Clean up CSV files
        csv_files = glob.glob(os.path.join(results_dir, "performance_data_*.csv"))
        csv_files.sort(key=os.path.getmtime, reverse=True)
        for old_file in csv_files[max_files:]:
            os.remove(old_file)
            
        # Clean up HTML files
        html_files = glob.glob(os.path.join(results_dir, "report_*.html"))
        html_files.sort(key=os.path.getmtime, reverse=True)
        for old_file in html_files[max_files:]:
            os.remove(old_file)
    
    def generate_html_report(self, output_path: str):
        """Generate a complete HTML report with all sections"""
        css = """
        <style>
            body {
                font-family: Arial, sans-serif;
                line-height: 1.6;
                margin: 0;
                padding: 20px;
                background-color: #f5f5f5;
            }
            .container {
                max-width: 1200px;
                margin: 0 auto;
                background-color: white;
                padding: 20px;
                border-radius: 8px;
                box-shadow: 0 2px 4px rgba(0,0,0,0.1);
            }
            .section {
                margin-bottom: 30px;
                padding: 20px;
                background-color: white;
                border-radius: 8px;
            }
            .summary-stats {
                display: grid;
                grid-template-columns: repeat(auto-fit, minmax(250px, 1fr));
                gap: 20px;
                margin: 20px 0;
            }
            .stat-box {
                background-color: #f8f9fa;
                padding: 15px;
                border-radius: 8px;
                text-align: center;
            }
            .plot-container {
                margin: 20px 0;
                padding: 15px;
                background-color: white;
                border-radius: 8px;
                box-shadow: 0 1px 3px rgba(0,0,0,0.1);
            }
            table {
                width: 100%;
                border-collapse: collapse;
                margin: 15px 0;
                background-color: white;
            }
            th, td {
                padding: 12px;
                text-align: left;
                border-bottom: 1px solid #ddd;
            }
            th {
                background-color: #f8f9fa;
                font-weight: bold;
            }
            tr:nth-child(even) {
                background-color: #f8f9fa;
            }
            h2 {
                color: #2c3e50;
                border-bottom: 2px solid #eee;
                padding-bottom: 10px;
            }
            h3 {
                color: #34495e;
                margin-top: 25px;
            }
            .analysis-text {
                margin-top: 20px;
                padding: 15px;
                background-color: #f8f9fa;
                border-radius: 8px;
            }
            .plot-section {
                display: flex;
                flex-direction: column;
                gap: 30px;
            }
            @media print {
                body {
                    background-color: white;
                }
                .container {
                    box-shadow: none;
                    max-width: none;
                }
            }
        </style>
        """
        
        # Generate timestamp
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        
        # Combine all sections
        html_content = f"""
        <!DOCTYPE html>
        <html>
        <head>
            <title>Payment Processing Benchmark Report</title>
            <script src="https://cdn.plot.ly/plotly-latest.min.js"></script>
            {css}
        </head>
        <body>
            <div class="container">
                <h1>Payment Processing Benchmark Report</h1>
                <p>Generated on: {timestamp}</p>
                
                {self.generate_introduction()}
                {self.generate_distribution_summary()}
                {self.generate_summary_section()}
                {self.create_interactive_plots()}
            </div>
        </body>
        </html>
        """
        
        # Write to file
        with open(output_path, 'w') as f:
            f.write(html_content)
