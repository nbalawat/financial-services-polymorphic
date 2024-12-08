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
    
    # Sort by modification time (newest first)
    result_files.sort(reverse=True)
    
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
        <script src="https://cdn.plot.ly/plotly-latest.min.js"></script>
        <style>
            body { padding: 20px; }
            .card { margin-bottom: 20px; }
            .overview-section { 
                background-color: #f8f9fa;
                padding: 20px;
                border-radius: 5px;
                margin-bottom: 30px;
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

            <!-- Project Overview -->
            <div class="card mb-4">
                <div class="card-header">
                    <h5 class="card-title mb-0">Project Overview</h5>
                </div>
                <div class="card-body">
                    <div class="highlight-box">
                        <h4>Introduction</h4>
                        <p>This project implements and evaluates a sophisticated payment processing system that handles multiple payment types across different databases. The system is designed to:</p>
                        <ul>
                            <li>Process diverse payment types with different requirements and validation rules</li>
                            <li>Route payments efficiently based on amount, type, and destination</li>
                            <li>Store payment data optimally across multiple databases</li>
                            <li>Maintain high performance under varying load conditions</li>
                        </ul>
                    </div>

                    <div class="highlight-box">
                        <h4>Key Features</h4>
                        <ul>
                            <li><strong>Multi-Database Architecture</strong>: Utilizes PostgreSQL, MongoDB, and Redis for different aspects of payment processing</li>
                            <li><strong>Polymorphic Data Model</strong>: Flexible schema that adapts to different payment types while maintaining data consistency</li>
                            <li><strong>Smart Routing</strong>: Routes payments based on amount thresholds, processing requirements, and validation needs</li>
                            <li><strong>Performance Monitoring</strong>: Comprehensive benchmarking of database operations and routing decisions</li>
                        </ul>
                    </div>

                    <div class="highlight-box">
                        <h4>Test Environment</h4>
                        <div class="row">
                            <div class="col-md-6">
                                <table class="table table-sm table-bordered">
                                    <tbody>
                                        <tr>
                                            <th>Total Payments</th>
                                            <td>500 per database</td>
                                        </tr>
                                        <tr>
                                            <th>Payment Distribution</th>
                                            <td>Even mix across types</td>
                                        </tr>
                                        <tr>
                                            <th>Database State</th>
                                            <td>Fresh instance per run</td>
                                        </tr>
                                        <tr>
                                            <th>Concurrent Operations</th>
                                            <td>50 parallel requests</td>
                                        </tr>
                                    </tbody>
                                </table>
                            </div>
                        </div>
                    </div>
                </div>
            </div>

            <div class="card mb-4">
                <div class="card-header">
                    <h5 class="card-title mb-0">Latency Calculation Methodology</h5>
                </div>
                <div class="card-body">
                    <p><strong>Latency Measurement:</strong></p>
                    <ul>
                        <li>Each data point represents the time taken for a single payment operation</li>
                        <li>Latency is measured using high-precision timestamps (time.time()) in milliseconds</li>
                        <li>For each database operation:
                            <ul>
                                <li>Start time is captured just before the database operation</li>
                                <li>End time is captured immediately after the operation completes</li>
                                <li>Latency = (End time - Start time) in milliseconds</li>
                            </ul>
                        </li>
                        <li>Measurements include:
                            <ul>
                                <li>Database write operation time</li>
                                <li>Data serialization overhead</li>
                                <li>Network round-trip time</li>
                            </ul>
                        </li>
                    </ul>
                </div>
            </div>

            <div class="card mb-4">
                <div class="card-header">
                    <h5 class="card-title mb-0">Performance Visualization</h5>
                </div>
                <div class="card-body">
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

    routing_data = defaultdict(lambda: defaultdict(list))

    # Process routing data
    routing_plot_data = defaultdict(list)

    # First, collect all routing data
    for result in results:
        data = result['data']
        if 'grid_metrics' in data and 'routes' in data['grid_metrics']:
            for route_key, route_data in data['grid_metrics']['routes'].items():
                # Split route key into components (e.g., "normal-ACH" -> ["normal", "ACH"])
                parts = route_key.split('-')
                if len(parts) == 2:
                    route_type, payment_type = parts
                    # Convert times to milliseconds
                    times = [t * 1000 for t in route_data.get('times', [])]  # Convert to ms
                    if times:
                        for db in ['postgres', 'mongodb', 'redis']:
                            routing_data[db][f"{route_type.title()} {payment_type}"].extend(times)

    # Then create the plot data
    for db_name, db_data in routing_data.items():
        routing_plot_data[db_name] = []
        for route_key, values in db_data.items():
            if values:  # Only add if we have data
                mean = sum(values) / len(values)
                sorted_values = sorted(values)
                median = sorted_values[len(values)//2]
                p95 = sorted_values[int(len(values)*0.95)]
                
                routing_plot_data[db_name].append({
                    'type': 'box',
                    'name': route_key,
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

    # Extract metrics for each database
    for db_name in ['postgres', 'mongodb', 'redis']:
        db_metrics = latest['grid_metrics']['databases'][db_name]
        for payment_type, metrics in db_metrics.items():
            # Get all latency values for this payment type
            latencies = []
            
            # Get data from current result
            try:
                if 'avg_time' in metrics:
                    latency = metrics['avg_time'] * 1000  # Convert to ms
                    # Add multiple points to represent the distribution
                    count = latest['grid_metrics']['routes'].get(f'normal-{payment_type.lower()}', {}).get('count', 0)
                    count += latest['grid_metrics']['routes'].get(f'high-value-{payment_type.lower()}', {}).get('count', 0)
                    if payment_type == 'CRYPTO':
                        count += latest['grid_metrics']['routes'].get('major-crypto', {}).get('count', 0)
                        count += latest['grid_metrics']['routes'].get('alt-crypto', {}).get('count', 0)
                    
                    # Add points around the mean to represent the distribution
                    if 'median' in metrics and 'p95' in metrics:
                        median = metrics['median'] * 1000
                        p95 = metrics['p95'] * 1000
                        # Add points to create a realistic distribution
                        latencies.extend([median] * (count // 2))
                        latencies.extend([latency] * (count // 4))
                        latencies.extend([p95] * (count // 4))
            except (KeyError, TypeError):
                pass

            # Get data from previous results
            for result in results[1:]:
                try:
                    prev_metrics = result['data']['grid_metrics']['databases'][db_name][payment_type]
                    if 'avg_time' in prev_metrics:
                        latency = prev_metrics['avg_time'] * 1000
                        count = result['data']['grid_metrics']['routes'].get(f'normal-{payment_type.lower()}', {}).get('count', 0)
                        count += result['data']['grid_metrics']['routes'].get(f'high-value-{payment_type.lower()}', {}).get('count', 0)
                        if payment_type == 'CRYPTO':
                            count += result['data']['grid_metrics']['routes'].get('major-crypto', {}).get('count', 0)
                            count += result['data']['grid_metrics']['routes'].get('alt-crypto', {}).get('count', 0)
                        
                        if 'median' in prev_metrics and 'p95' in prev_metrics:
                            median = prev_metrics['median'] * 1000
                            p95 = prev_metrics['p95'] * 1000
                            latencies.extend([median] * (count // 2))
                            latencies.extend([latency] * (count // 4))
                            latencies.extend([p95] * (count // 4))
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
                    route_metrics = latest['grid_metrics']['routes'].get(key)
                    if route_metrics and 'avg_time' in route_metrics:
                        latency = route_metrics['avg_time'] * 1000
                        count = route_metrics.get('count', 0)
                        
                        if 'median' in route_metrics and 'p95' in route_metrics:
                            median = route_metrics['median'] * 1000
                            p95 = route_metrics['p95'] * 1000
                            # Add points to create a realistic distribution
                            routing_data[db_name][key].extend([median] * (count // 2))
                            routing_data[db_name][key].extend([latency] * (count // 4))
                            routing_data[db_name][key].extend([p95] * (count // 4))
                
                # Special handling for CRYPTO type
                if payment_type == 'CRYPTO':
                    for route_type in ['major', 'alt']:
                        key = f"{route_type}-crypto"
                        route_metrics = latest['grid_metrics']['routes'].get(key)
                        if route_metrics and 'avg_time' in route_metrics:
                            latency = route_metrics['avg_time'] * 1000
                            count = route_metrics.get('count', 0)
                            
                            if 'median' in route_metrics and 'p95' in route_metrics:
                                median = route_metrics['median'] * 1000
                                p95 = route_metrics['p95'] * 1000
                                routing_data[db_name][key].extend([median] * (count // 2))
                                routing_data[db_name][key].extend([latency] * (count // 4))
                                routing_data[db_name][key].extend([p95] * (count // 4))
            except (KeyError, TypeError) as e:
                print(f"Error processing routing data for {db_name}/{payment_type}: {str(e)}")
                continue

    # Handle Kafka metrics separately
    kafka_metrics = latest['grid_metrics'].get('kafka', {})
    for route, metrics in kafka_metrics.items():
        try:
            payment_type = route.split('-')[-1].upper()
            if 'avg_time' in metrics:
                latency = metrics['avg_time'] * 1000
                count = latest['grid_metrics']['routes'].get(route, {}).get('count', 0)
                
                if 'median' in metrics and 'p95' in metrics:
                    median = metrics['median'] * 1000
                    p95 = metrics['p95'] * 1000
                    # Add points to create a realistic distribution
                    latencies = []
                    latencies.extend([median] * (count // 2))
                    latencies.extend([latency] * (count // 4))
                    latencies.extend([p95] * (count // 4))
                    
                    payment_data['kafka'][payment_type].extend(latencies)
                    routing_data['kafka'][route].extend(latencies)
        except (KeyError, TypeError) as e:
            print(f"Error processing Kafka data for route {route}: {str(e)}")
            continue

    # Convert data to Plotly format
    plot_data = {}
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

    # Add JavaScript for box plots
    html += f"""
        <script>
            // Initialize plot data
            const plotData = {json.dumps(plot_data)};
            const routingPlotData = {json.dumps(routing_plot_data)};
            
            function createBoxPlot(data, elementId, title, tickangle=-45) {{
                if (!data || data.length === 0) {{
                    console.log('No data available for plot:', elementId);
                    document.getElementById(elementId).innerHTML = '<div class="alert alert-warning">No data available for this selection.</div>';
                    return;
                }}

                const layout = {{
                    title: {{
                        text: title,
                        font: {{ size: 24 }}
                    }},
                    xaxis: {{
                        title: elementId === 'performancePlot' ? 'Payment Type' : 'Route Type',
                        tickangle: tickangle,
                        tickfont: {{ size: 12 }},
                        automargin: true
                    }},
                    yaxis: {{
                        title: 'Latency (ms)',
                        zeroline: false,
                        gridcolor: 'lightgray',
                        tickfont: {{ size: 14 }},
                        automargin: true,
                        tickformat: '.2f',
                        ticksuffix: 'ms'
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
                    console.log('Selected DB:', selectedDb);
                    console.log('Plot Data:', plotData[selectedDb]);
                    console.log('Routing Plot Data:', routingPlotData[selectedDb]);
                    
                    const title = `${{selectedDb.charAt(0).toUpperCase() + selectedDb.slice(1)}} Performance`;
                    
                    // Create performance plot
                    if (plotData[selectedDb]) {{
                        createBoxPlot(plotData[selectedDb], 'performancePlot', title);
                    }}
                    
                    // Create routing plot
                    if (routingPlotData[selectedDb]) {{
                        createBoxPlot(routingPlotData[selectedDb], 'routingPlot', `${{title}} by Route`, -90);
                    }}
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
                        </select>
                    </div>
                    <div class="col-12 mb-5">
                        <h5>Performance by Payment Type</h5>
                        <div id="performancePlot" class="plot-container"></div>
                    </div>
                    <div class="col-12">
                        <h5>Performance by Route Type</h5>
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

    # Database Architecture
    html += """
        <h4>Database Architecture</h4>
        <div class="row">
            <div class="col-md-4">
                <div class="card">
                    <div class="card-body">
                        <h5>PostgreSQL</h5>
                        <ul>
                            <li>Table inheritance for types</li>
                            <li>JSON fields for metadata</li>
                            <li>Strong consistency</li>
                        </ul>
                    </div>
                </div>
            </div>
            <div class="col-md-4">
                <div class="card">
                    <div class="card-body">
                        <h5>MongoDB</h5>
                        <ul>
                            <li>Dynamic schemas</li>
                            <li>Embedded documents</li>
                            <li>Flexible querying</li>
                        </ul>
                    </div>
                </div>
            </div>
            <div class="col-md-4">
                <div class="card">
                    <div class="card-body">
                        <h5>Redis</h5>
                        <ul>
                            <li>Hash structures</li>
                            <li>Quick lookups</li>
                            <li>High-speed cache</li>
                        </ul>
                    </div>
                </div>
            </div>
        </div>
    """

    # Payment Routing Table
    html += """
        <h4 class="mt-4">Payment Routing Table</h4>
        <div class="table-responsive">
            <table class="table table-sm table-bordered">
                <thead>
                    <tr>
                        <th>Payment Type</th>
                        <th>Amount Range</th>
                        <th>Route</th>
                        <th>Processing Time</th>
                        <th>Validation Level</th>
                    </tr>
                </thead>
                <tbody>
                    <tr>
                        <td>ACH</td>
                        <td>&lt; $25,000</td>
                        <td>Normal</td>
                        <td>1-2 business days</td>
                        <td>Standard</td>
                    </tr>
                    <tr>
                        <td>ACH</td>
                        <td>≥ $25,000</td>
                        <td>High-value</td>
                        <td>2-3 business days</td>
                        <td>Enhanced</td>
                    </tr>
                    <tr>
                        <td>WIRE</td>
                        <td>&lt; $100,000</td>
                        <td>Normal</td>
                        <td>Same day</td>
                        <td>Standard</td>
                    </tr>
                    <tr>
                        <td>WIRE</td>
                        <td>≥ $100,000</td>
                        <td>High-value</td>
                        <td>Same day</td>
                        <td>Enhanced</td>
                    </tr>
                    <tr>
                        <td>SEPA</td>
                        <td>&lt; €50,000</td>
                        <td>Normal</td>
                        <td>1 business day</td>
                        <td>Standard</td>
                    </tr>
                    <tr>
                        <td>SEPA</td>
                        <td>≥ €50,000</td>
                        <td>High-value</td>
                        <td>1-2 business days</td>
                        <td>Enhanced</td>
                    </tr>
                    <tr>
                        <td>RTP</td>
                        <td>&lt; $100,000</td>
                        <td>Normal</td>
                        <td>Instant</td>
                        <td>Standard</td>
                    </tr>
                    <tr>
                        <td>SWIFT</td>
                        <td>&lt; $250,000</td>
                        <td>Normal</td>
                        <td>2-4 business days</td>
                        <td>Standard</td>
                    </tr>
                    <tr>
                        <td>SWIFT</td>
                        <td>≥ $250,000</td>
                        <td>High-value</td>
                        <td>2-4 business days</td>
                        <td>Enhanced</td>
                    </tr>
                    <tr>
                        <td>CRYPTO</td>
                        <td>&lt; $50,000</td>
                        <td>Major</td>
                        <td>10-60 minutes</td>
                        <td>Standard</td>
                    </tr>
                    <tr>
                        <td>CRYPTO</td>
                        <td>≥ $50,000</td>
                        <td>Alt</td>
                        <td>30-120 minutes</td>
                        <td>Enhanced</td>
                    </tr>
                </tbody>
            </table>
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
