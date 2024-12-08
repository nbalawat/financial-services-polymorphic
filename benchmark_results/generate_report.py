import json
import os
from datetime import datetime
import glob
from collections import defaultdict
import plotly.graph_objects as go

def load_benchmark_results():
    """Load all benchmark results from JSON files"""
    results = []
    result_files = []
    
    # Get all benchmark result files
    for filename in os.listdir('.'):
        if filename.startswith('benchmark_results_') and filename.endswith('.json'):
            filepath = os.path.join('.', filename)
            result_files.append((os.path.getmtime(filepath), filepath))
    
    # Sort by modification time (newest first) and keep only last 4
    result_files.sort(reverse=True)
    result_files = result_files[:4]
    
    # Load the results
    for _, filepath in result_files:
        with open(filepath, 'r') as f:
            data = json.load(f)
            # Extract timestamp parts from filename
            parts = filepath.split('_')[2:4]  # ['20241208', '092944.json']
            timestamp = parts[0] + parts[1].split('.')[0]  # '20241208092944'
            dt = datetime.strptime(timestamp, '%Y%m%d%H%M%S')
            results.append({
                'timestamp': dt,
                'data': data
            })
    
    return results

def generate_html(results):
    html = """
    <!DOCTYPE html>
    <html>
    <head>
        <title>Payment System Benchmark Results</title>
        <link href="https://cdn.jsdelivr.net/npm/bootstrap@5.1.3/dist/css/bootstrap.min.css" rel="stylesheet">
        <script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
        <script src="https://cdn.plot.ly/plotly-latest.min.js"></script>
        <style>
            body { padding: 20px; }
            .card { margin-bottom: 20px; }
            .table-sm td, .table-sm th { padding: 0.3rem; }
            .plot-container { 
                height: 800px;  
                width: 100%;
                margin-bottom: 40px;  
            }
            .overview-section { 
                background-color: #f8f9fa;
                padding: 20px;
                border-radius: 5px;
                margin-bottom: 30px;
            }
            .section-header {
                border-bottom: 2px solid #dee2e6;
                padding-bottom: 10px;
                margin-bottom: 20px;
            }
            .highlight-box {
                background-color: #e9ecef;
                border-left: 4px solid #6c757d;
                padding: 15px;
                margin: 20px 0;
                border-radius: 0 4px 4px 0;
            }
        </style>
    </head>
    <body>
        <div class="container">
            <h1 class="mb-4">Payment System Benchmark Results</h1>

            <!-- Overview Section -->
            <div class="overview-section">
                <h2 class="section-header">System Overview</h2>

                <div class="highlight-box">
                    <h4>Polymorphic Data Model</h4>
                    <p>This benchmark evaluates a sophisticated polymorphic data model designed for handling diverse payment types across multiple databases. The model:</p>
                    <ul>
                        <li><strong>Adapts to Different Payment Schemas</strong>: Each payment type (ACH, WIRE, SEPA, etc.) has unique fields and validation requirements</li>
                        <li><strong>Maintains Consistency</strong>: Common payment attributes are standardized across all types</li>
                        <li><strong>Enables Flexibility</strong>: New payment types can be added without changing the core data structure</li>
                    </ul>
                </div>
                
                <h4>Payment Types</h4>
                <p>The system processes various types of payments, each with different characteristics and requirements:</p>
                <ul>
                    <li><strong>ACH (Automated Clearing House)</strong>: Domestic bank-to-bank transfers in the US
                        <ul>
                            <li>Normal ACH: Standard transfers with regular processing</li>
                            <li>High-value ACH: Larger amounts with additional verification</li>
                        </ul>
                    </li>
                    <li><strong>WIRE</strong>: Real-time, irrevocable bank transfers
                        <ul>
                            <li>Normal Wire: Standard wire transfers</li>
                            <li>High-value Wire: Large-value transfers with enhanced security</li>
                        </ul>
                    </li>
                    <li><strong>SEPA (Single Euro Payments Area)</strong>: European payment network
                        <ul>
                            <li>Normal SEPA: Regular European transfers</li>
                            <li>High-value SEPA: Large-value European transfers</li>
                        </ul>
                    </li>
                    <li><strong>RTP (Real-Time Payments)</strong>: Instant payment network
                        <ul>
                            <li>Normal RTP: Instant payments with standard limits</li>
                        </ul>
                    </li>
                    <li><strong>SWIFT</strong>: International bank transfers
                        <ul>
                            <li>Normal SWIFT: Standard international transfers</li>
                            <li>High-value SWIFT: Large international transfers</li>
                        </ul>
                    </li>
                    <li><strong>CRYPTO</strong>: Cryptocurrency transactions
                        <ul>
                            <li>Major Crypto: Major cryptocurrency transfers</li>
                            <li>Alt Crypto: Alternative cryptocurrency transfers</li>
                        </ul>
                    </li>
                </ul>

                <h4>Polymorphic Implementation Details</h4>
                <p>The data model is implemented with the following key features:</p>
                <div class="row">
                    <div class="col-md-6">
                        <h5>Core Components</h5>
                        <ul>
                            <li><strong>Base Payment Model</strong>: Contains common fields across all payment types
                                <ul>
                                    <li>Transaction ID</li>
                                    <li>Amount and Currency</li>
                                    <li>Timestamp and Status</li>
                                    <li>Source and Destination</li>
                                </ul>
                            </li>
                            <li><strong>Type-Specific Extensions</strong>: Additional fields for each payment type
                                <ul>
                                    <li>ACH: Routing numbers, account types</li>
                                    <li>WIRE: Bank codes, intermediary details</li>
                                    <li>SEPA: IBAN, BIC codes</li>
                                    <li>CRYPTO: Wallet addresses, network fees</li>
                                </ul>
                            </li>
                        </ul>
                    </div>
                    <div class="col-md-6">
                        <h5>Database Implementation</h5>
                        <ul>
                            <li><strong>PostgreSQL</strong>: 
                                <ul>
                                    <li>Table inheritance for payment types</li>
                                    <li>JSON fields for flexible metadata</li>
                                    <li>Strong consistency guarantees</li>
                                </ul>
                            </li>
                            <li><strong>MongoDB</strong>:
                                <ul>
                                    <li>Dynamic schemas for each type</li>
                                    <li>Embedded documents for related data</li>
                                    <li>Flexible querying capabilities</li>
                                </ul>
                            </li>
                            <li><strong>Redis</strong>:
                                <ul>
                                    <li>Hash structures for payment data</li>
                                    <li>Sets for quick lookups</li>
                                    <li>Cache for frequent queries</li>
                                </ul>
                            </li>
                        </ul>
                    </div>
                </div>

                <h4>System Architecture</h4>
                <p>The payment system utilizes a multi-database architecture for optimal performance and reliability:</p>
                <ul>
                    <li><strong>PostgreSQL</strong>: Primary database for transaction records and payment details</li>
                    <li><strong>MongoDB</strong>: Document store for flexible payment metadata and routing information</li>
                    <li><strong>Redis</strong>: High-speed cache for recent payments and routing decisions</li>
                    <li><strong>Kafka</strong>: Message broker for asynchronous processing and event streaming</li>
                </ul>

                <h4>Routing Logic</h4>
                <p>Payments are routed based on several factors:</p>
                <ul>
                    <li>Payment type and amount (normal vs high-value)</li>
                    <li>Geographic region and currency</li>
                    <li>Processing speed requirements (instant vs standard)</li>
                    <li>Risk level and security requirements</li>
                </ul>

                <h4>Benchmarking Methodology</h4>
                <p>The benchmark simulates real-world payment processing scenarios with the following characteristics:</p>
                <ul>
                    <li><strong>Load Generation</strong>:
                        <ul>
                            <li>Concurrent payment requests using asyncio</li>
                            <li>Random distribution of payment types and amounts</li>
                            <li>Realistic payload sizes and data structures</li>
                        </ul>
                    </li>
                    <li><strong>Measurement Points</strong>:
                        <ul>
                            <li>Database operations (write/read latencies)</li>
                            <li>Message queue processing times</li>
                            <li>End-to-end payment processing duration</li>
                            <li>Per-route and per-database latency metrics</li>
                        </ul>
                    </li>
                    <li><strong>Performance Metrics</strong>:
                        <ul>
                            <li>Average latency (mean processing time)</li>
                            <li>Median latency (50th percentile)</li>
                            <li>P95 latency (95th percentile)</li>
                            <li>Throughput (payments/second)</li>
                            <li>Error rates and types</li>
                        </ul>
                    </li>
                    <li><strong>Test Environment</strong>:
                        <ul>
                            <li>Isolated database instances for consistent results</li>
                            <li>Clean state before each benchmark run</li>
                            <li>Controlled network conditions</li>
                            <li>Monitoring of system resources (CPU, memory, I/O)</li>
                        </ul>
                    </li>
                </ul>

                <h4>Benchmark Parameters</h4>
                <div class="row">
                    <div class="col-md-6">
                        <table class="table table-sm table-bordered">
                            <tbody>
                                <tr>
                                    <th>Total Payments per Run</th>
                                    <td>500</td>
                                </tr>
                                <tr>
                                    <th>Concurrent Connections</th>
                                    <td>50</td>
                                </tr>
                                <tr>
                                    <th>Payment Mix</th>
                                    <td>Random distribution across all types</td>
                                </tr>
                                <tr>
                                    <th>Database State</th>
                                    <td>Fresh instance per run</td>
                                </tr>
                            </tbody>
                        </table>
                    </div>
                </div>
            </div>

            <!-- Benchmark Results Section -->
            <h2 class="section-header">Benchmark Results</h2>
            
            <h3 class="mb-3">Recent Benchmark Runs</h3>
    """
    
    # Summary cards for each benchmark
    html += '<div class="row">'
    for result in results:
        summary = result['data']['summary']
        html += f"""
            <div class="col-md-6 mb-4">
                <div class="card">
                    <div class="card-header">
                        <h5 class="card-title mb-0">Benchmark Run - {result['timestamp'].strftime('%Y-%m-%d %H:%M:%S')}</h5>
                    </div>
                    <div class="card-body">
                        <ul class="list-group list-group-flush">
                            <li class="list-group-item">Total Payments: {summary['total_payments']}</li>
                            <li class="list-group-item">Total Duration: {summary['total_duration']:.2f}s</li>
                            <li class="list-group-item">Average Rate: {summary['average_rate']:.2f} payments/sec</li>
                            <li class="list-group-item">Error Count: {summary['error_count']}</li>
                        </ul>
                    </div>
                </div>
            </div>
        """
    html += '</div>'

    # Latest benchmark details
    latest = results[0]['data']
    
    # Prepare data for box plots
    payment_data = {
        'postgres': defaultdict(list),
        'mongodb': defaultdict(list),
        'redis': defaultdict(list),
        'kafka': defaultdict(list)
    }

    routing_data = {
        'postgres': defaultdict(list),
        'mongodb': defaultdict(list),
        'redis': defaultdict(list),
        'kafka': defaultdict(list)
    }

    # Extract metrics for each database
    for db_name in ['postgres', 'mongodb', 'redis']:
        db_metrics = latest['grid_metrics']['databases'][db_name]
        for payment_type, metrics in db_metrics.items():
            # Get all latency values for this payment type
            latencies = []
            
            # Get data from current result
            try:
                if 'avg_time' in metrics:
                    latencies.append(metrics['avg_time'] * 1000)  # Convert to ms
            except (KeyError, TypeError):
                pass

            # Get data from previous results
            for result in results[1:]:  # Skip current result as we already have it
                try:
                    prev_metrics = result['data']['grid_metrics']['databases'][db_name][payment_type]
                    if 'avg_time' in prev_metrics:
                        latencies.append(prev_metrics['avg_time'] * 1000)
                except (KeyError, TypeError):
                    continue

            if latencies:
                payment_data[db_name][payment_type].extend(latencies)
                print(f"{db_name} - {payment_type}: {len(latencies)} values")

            # Get routing data from the current result
            try:
                # Get routing data for each route type
                route_types = ['normal', 'high-value']
                for route_type in route_types:
                    key = f"{route_type}-{payment_type.lower()}"
                    if 'avg_time' in metrics:
                        routing_data[db_name][key].append(metrics['avg_time'] * 1000)
                    
                    # Get routing data from previous results
                    for result in results[1:]:
                        try:
                            prev_metrics = result['data']['grid_metrics']['databases'][db_name][payment_type]
                            if 'avg_time' in prev_metrics:
                                routing_data[db_name][key].append(prev_metrics['avg_time'] * 1000)
                        except (KeyError, TypeError):
                            continue
                
                # Special handling for CRYPTO type
                if payment_type == 'CRYPTO':
                    for route_type in ['major', 'alt']:
                        key = f"{route_type}-crypto"
                        if 'avg_time' in metrics:
                            routing_data[db_name][key].append(metrics['avg_time'] * 1000)
                        
                        # Get data from previous results
                        for result in results[1:]:
                            try:
                                prev_metrics = result['data']['grid_metrics']['databases'][db_name][payment_type]
                                if 'avg_time' in prev_metrics:
                                    routing_data[db_name][key].append(prev_metrics['avg_time'] * 1000)
                            except (KeyError, TypeError):
                                continue
            except (KeyError, TypeError) as e:
                print(f"Error processing routing data for {db_name}/{payment_type}: {str(e)}")
                continue

    # Handle Kafka metrics separately
    kafka_metrics = latest['grid_metrics'].get('kafka', {})
    for route, metrics in kafka_metrics.items():
        try:
            payment_type = route.split('-')[-1].upper()
            if 'avg_time' in metrics:
                latency = metrics['avg_time'] * 1000  # Convert to ms
                
                # Add to both datasets
                payment_data['kafka'][payment_type].append(latency)
                routing_data['kafka'][route].append(latency)
            
            # Get data from previous results
            for result in results[1:]:
                try:
                    prev_metrics = result['data']['grid_metrics']['kafka'][route]
                    if 'avg_time' in prev_metrics:
                        prev_latency = prev_metrics['avg_time'] * 1000
                        payment_data['kafka'][payment_type].append(prev_latency)
                        routing_data['kafka'][route].append(prev_latency)
                except KeyError:
                    continue
        except (KeyError, TypeError) as e:
            print(f"Error processing Kafka data for {route}: {str(e)}")
            continue

    # Debug print
    print("\nData summary:")
    for db_name, payments in payment_data.items():
        print(f"\n{db_name}:")
        for payment_type, values in payments.items():
            print(f"  {payment_type}: {len(values)} values")

    print("\nRouting data summary:")
    for db_name, routes in routing_data.items():
        print(f"\n{db_name}:")
        for route, values in routes.items():
            print(f"  {route}: {len(values)} values")

    # Convert data to Plotly format
    plot_data = {}
    routing_plot_data = {}
    summary_stats = {}  # For comparison table
    
    for db_name, db_data in payment_data.items():
        plot_data[db_name] = []
        summary_stats[db_name] = {}
        
        for payment_type, values in db_data.items():
            if values:
                # Calculate statistics
                mean = sum(values) / len(values)
                sorted_values = sorted(values)
                median = sorted_values[len(values)//2]
                p95 = sorted_values[int(len(values)*0.95)]
                
                # Store stats for comparison table
                summary_stats[db_name][payment_type] = {
                    'mean': mean,
                    'median': median,
                    'p95': p95,
                    'count': len(values)
                }
                
                # Add box plot with hover text
                plot_data[db_name].append({
                    'type': 'box',
                    'name': payment_type,
                    'y': values,
                    'boxpoints': 'all',
                    'jitter': 0.3,
                    'pointpos': -1.8,
                    'boxmean': True,
                    'width': 0.5,
                    'hovertemplate': 
                        'Payment Type: %{x}<br>' +
                        'Value: %{y:.2f}ms<br>' +
                        f'Mean: {mean:.2f}ms<br>' +
                        f'Median: {median:.2f}ms<br>' +
                        f'95th Percentile: {p95:.2f}ms<br>' +
                        '<extra></extra>'  # Hides secondary box
                })

    for db_name, db_data in routing_data.items():
        routing_plot_data[db_name] = []
        for route_key, values in db_data.items():
            if values:
                # Calculate statistics
                mean = sum(values) / len(values)
                sorted_values = sorted(values)
                median = sorted_values[len(values)//2]
                p95 = sorted_values[int(len(values)*0.95)]
                
                # Format the route name nicely
                parts = route_key.split('-')
                if len(parts) == 2:
                    route_type, payment_type = parts
                    name = f"{route_type.title()} {payment_type.upper()}"
                else:
                    name = route_key.title()
                
                routing_plot_data[db_name].append({
                    'type': 'box',
                    'name': name,
                    'y': values,
                    'boxpoints': 'all',
                    'jitter': 0.3,
                    'pointpos': -1.8,
                    'boxmean': True,
                    'width': 0.5,
                    'hovertemplate': 
                        'Route: %{x}<br>' +
                        'Value: %{y:.2f}ms<br>' +
                        f'Mean: {mean:.2f}ms<br>' +
                        f'Median: {median:.2f}ms<br>' +
                        f'95th Percentile: {p95:.2f}ms<br>' +
                        '<extra></extra>'
                })

    # Add JavaScript for box plots
    html += f"""
        <script>
            const plotData = {json.dumps(plot_data)};
            const routingPlotData = {json.dumps(routing_plot_data)};
            
            function createBoxPlot(data, elementId, title, tickangle=-45) {{
                const layout = {{
                    title: {{
                        text: title,
                        font: {{ size: 24 }}
                    }},
                    xaxis: {{
                        title: elementId === 'performancePlot' ? 'Payment Type' : 'Payment Type - Route',
                        tickangle: tickangle,
                        tickfont: {{ size: 12 }},
                        automargin: true
                    }},
                    yaxis: {{
                        title: 'Time (ms)',
                        zeroline: false,
                        gridcolor: 'lightgray',
                        tickfont: {{ size: 14 }},
                        automargin: true
                    }},
                    showlegend: false,
                    height: 700,
                    margin: {{ 
                        t: 60,
                        b: 150,  
                        l: 100,
                        r: 40
                    }},
                    boxmode: 'group',
                    plot_bgcolor: '#fff',
                    paper_bgcolor: '#fff',
                    hoverlabel: {{
                        bgcolor: 'white',
                        font: {{ size: 14 }}
                    }}
                }};
                
                // Sort the data by name for consistent ordering
                const sortedData = [...data].sort((a, b) => a.name.localeCompare(b.name));
                
                // Adjust box width based on number of boxes
                const boxWidth = Math.max(0.3, Math.min(0.8, 8 / sortedData.length));
                sortedData.forEach(box => {{
                    box.width = boxWidth;
                }});
                
                const config = {{
                    displayModeBar: true,
                    responsive: true,
                    displaylogo: false,
                    modeBarButtonsToRemove: ['lasso2d', 'select2d']
                }};
                
                Plotly.newPlot(elementId, sortedData, layout, config);
            }}
            
            // Initial plot
            document.addEventListener('DOMContentLoaded', function() {{
                const selector = document.getElementById('databaseSelector');
                
                function updatePlots() {{
                    const selectedDb = selector.value;
                    const title = `${{selectedDb.charAt(0).toUpperCase() + selectedDb.slice(1)}} Performance`;
                    createBoxPlot(plotData[selectedDb], 'performancePlot', title);
                    createBoxPlot(routingPlotData[selectedDb], 'routingPlot', `${{title}} by Route`, -90);
                }}
                
                // Create initial plots
                updatePlots();
                
                // Update plots when selection changes
                selector.addEventListener('change', updatePlots);
            }});
        </script>
    """

    # Box Plot Section
    html += """
        <div class="card">
            <div class="card-header">
                <h5 class="card-title mb-0">Performance Distribution by Database</h5>
            </div>
            <div class="card-body">
                <div class="row">
                    <div class="col-12 mb-4">  
                        <select id="databaseSelector" class="form-select" style="width: 200px;">
                            <option value="postgres">PostgreSQL</option>
                            <option value="mongodb">MongoDB</option>
                            <option value="redis">Redis</option>
                            <option value="kafka">Kafka</option>
                        </select>
                    </div>
                    <div class="col-12">
                        <h5>Performance by Payment Type</h5>
                        <div id="performancePlot" class="plot-container"></div>
                    </div>
                    <div class="col-12">
                        <h5>Performance by Payment Type and Routing</h5>
                        <div id="routingPlot" class="plot-container"></div>
                    </div>
                </div>
            </div>
        </div>
    """

    # Add Database Comparison Table
    html += """
        <div class="card mt-4">
            <div class="card-header">
                <h5 class="card-title mb-0">Database Performance Comparison</h5>
            </div>
            <div class="card-body">
                <div class="table-responsive">
                    <table class="table table-sm table-bordered">
                        <thead>
                            <tr>
                                <th>Database</th>
                                <th>Payment Type</th>
                                <th>Mean (ms)</th>
                                <th>Median (ms)</th>
                                <th>95th Percentile (ms)</th>
                                <th>Sample Count</th>
                            </tr>
                        </thead>
                        <tbody>
    """
    
    # Add rows for each database and payment type
    for db_name in sorted(summary_stats.keys()):
        first_row = True
        for payment_type in sorted(summary_stats[db_name].keys()):
            stats = summary_stats[db_name][payment_type]
            html += f"""
                <tr>
                    <td>{'<strong>' + db_name.title() + '</strong>' if first_row else ''}</td>
                    <td>{payment_type}</td>
                    <td>{stats['mean']:.2f}</td>
                    <td>{stats['median']:.2f}</td>
                    <td>{stats['p95']:.2f}</td>
                    <td>{stats['count']}</td>
                </tr>
            """
            first_row = False
    
    html += """
                        </tbody>
                    </table>
                </div>
            </div>
        </div>
    """

    # Detailed performance metrics table
    html += """
                <h6 class="mb-3">Detailed Performance Metrics</h6>
                <div class="table-responsive">
                    <table class="table table-striped table-sm">
                        <thead>
                            <tr>
                                <th>Payment Type</th>
                                <th>Database</th>
                                <th>Transactions</th>
                                <th>Avg (ms)</th>
                                <th>Median (ms)</th>
                                <th>P95 (ms)</th>
                            </tr>
                        </thead>
                        <tbody>
    """
    
    # Get transaction counts from routes
    transaction_counts = {}
    for route, data in latest['grid_metrics']['routes'].items():
        payment_type = route.split('-')[-1].upper()
        if payment_type not in transaction_counts:
            transaction_counts[payment_type] = 0
        transaction_counts[payment_type] += data['count']

    for payment_type in latest['grid_metrics']['databases']['postgres'].keys():
        tx_count = transaction_counts.get(payment_type.upper(), 0)
        for db in ['postgres', 'mongodb']:
            if db in latest['grid_metrics']['databases'] and payment_type in latest['grid_metrics']['databases'][db]:
                metrics = latest['grid_metrics']['databases'][db][payment_type]
                html += f"""
                    <tr>
                        <td>{payment_type}</td>
                        <td>{db}</td>
                        <td>{tx_count:,}</td>
                        <td>{metrics['avg_time']*1000:.2f}</td>
                        <td>{metrics['median']*1000:.2f}</td>
                        <td>{metrics['p95']*1000:.2f}</td>
                    </tr>
                """
    
    html += """
                        </tbody>
                    </table>
                </div>
            </div>
        </div>
    """

    # Routing Distribution
    html += """
        <div class="card mt-4">
            <div class="card-header">
                <h5 class="card-title mb-0">Payment Routing Distribution</h5>
            </div>
            <div class="card-body">
                <canvas id="routingChart"></canvas>
            </div>
        </div>
    """

    # Add Chart.js initialization
    routes_data = latest['grid_metrics']['routes']
    html += """
        <script>
            const ctx = document.getElementById('routingChart');
            new Chart(ctx, {
                type: 'bar',
                data: {
                    labels: [%s],
                    datasets: [{
                        label: 'Payment Count',
                        data: [%s],
                        backgroundColor: 'rgba(54, 162, 235, 0.5)',
                        borderColor: 'rgba(54, 162, 235, 1)',
                        borderWidth: 1
                    }]
                },
                options: {
                    responsive: true,
                    scales: {
                        y: {
                            beginAtZero: true
                        }
                    }
                }
            });
        </script>
    """ % (
        ','.join([f"'{route}'" for route in routes_data.keys()]),
        ','.join([str(data['count']) for data in routes_data.values()])
    )

    html += """
        </div>
    </body>
    </html>
    """
    
    return html

if __name__ == '__main__':
    results = load_benchmark_results()
    html_content = generate_html(results)
    
    with open('benchmark_report.html', 'w') as f:
        f.write(html_content)
    
    print("Report generated successfully!")
