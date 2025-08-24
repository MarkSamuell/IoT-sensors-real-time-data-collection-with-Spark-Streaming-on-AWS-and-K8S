import React from 'react';

const QueryVisualizer = () => {
    const [queryResults, setQueryResults] = React.useState([]);
    const [columns, setColumns] = React.useState([]);
    const [error, setError] = React.useState(null);
    const [chartConfig, setChartConfig] = React.useState({
        type: 'bar',
        xAxis: '',
        yAxes: [],
        aggregation: 'sum'
    });
    const [availableColumns, setAvailableColumns] = React.useState([]);

    // Color palette for consistent chart colors
    const COLORS = ['#0088FE', '#00C49F', '#FFBB28', '#FF8042', '#8884D8', '#82CA9D'];

    // Global function to update visualization
    window.updateQueryVisualization = (rawResults, columnList) => {
        try {
            if (!rawResults || !columnList) {
                throw new Error("No data or columns provided");
            }

            // Convert results to proper types
            const processedResults = rawResults.map(row => {
                const processedRow = {};
                columnList.forEach(col => {
                    const value = row[col];
                    // Convert numeric strings to numbers
                    if (typeof value === 'string' && !isNaN(value) && value.trim() !== '') {
                        processedRow[col] = parseFloat(value);
                    } 
                    // Convert other numeric types
                    else if (typeof value === 'number') {
                        processedRow[col] = value;
                    }
                    // Handle null/undefined
                    else if (value === null || value === undefined) {
                        processedRow[col] = null;
                    }
                    // Keep as-is for other types
                    else {
                        processedRow[col] = value;
                    }
                });
                return processedRow;
            });

            // Analyze column types
            const columnAnalysis = columnList.map(col => {
                const sampleValue = processedResults[0]?.[col];
                const isNumeric = typeof sampleValue === 'number' && !isNaN(sampleValue);
                return {
                    name: col,
                    type: typeof sampleValue,
                    isNumeric
                };
            });

            // Auto-configure chart if no configuration exists
            if (!chartConfig.xAxis) {
                const defaultXAxis = columnAnalysis.find(col => !col.isNumeric)?.name || columnList[0];
                const defaultYAxes = columnAnalysis.filter(col => col.isNumeric).map(col => col.name).slice(0, 3);
                
                setChartConfig(prev => ({
                    ...prev,
                    xAxis: defaultXAxis,
                    yAxes: defaultYAxes
                }));
            }

            setQueryResults(processedResults);
            setColumns(columnList);
            setAvailableColumns(columnAnalysis);
            setError(null);
        } catch (err) {
            console.error("Visualization error:", err);
            setError(err.message);
        }
    };

    const generateChartData = () => {
        if (!queryResults.length || !chartConfig.xAxis) return [];

        // Group data by x-axis value
        const groupedData = {};
        queryResults.forEach(row => {
            const xValue = row[chartConfig.xAxis];
            if (!groupedData[xValue]) {
                groupedData[xValue] = { [chartConfig.xAxis]: xValue };
            }

            // Aggregate y-axis values
            chartConfig.yAxes.forEach(yCol => {
                const val = row[yCol];
                if (val === null || val === undefined) return;

                switch(chartConfig.aggregation) {
                    case 'sum':
                        groupedData[xValue][yCol] = (groupedData[xValue][yCol] || 0) + val;
                        break;
                    case 'avg':
                        if (!groupedData[xValue][`${yCol}_count`]) {
                            groupedData[xValue][`${yCol}_count`] = 0;
                            groupedData[xValue][`${yCol}_sum`] = 0;
                        }
                        groupedData[xValue][`${yCol}_count`]++;
                        groupedData[xValue][`${yCol}_sum`] += val;
                        groupedData[xValue][yCol] = groupedData[xValue][`${yCol}_sum`] / groupedData[xValue][`${yCol}_count`];
                        break;
                    case 'max':
                        groupedData[xValue][yCol] = Math.max(groupedData[xValue][yCol] || -Infinity, val);
                        break;
                    case 'min':
                        groupedData[xValue][yCol] = Math.min(groupedData[xValue][yCol] || Infinity, val);
                        break;
                    case 'count':
                        groupedData[xValue][yCol] = (groupedData[xValue][yCol] || 0) + 1;
                        break;
                    default:
                        groupedData[xValue][yCol] = val;
                }
            });
        });

        return Object.values(groupedData);
    };

    const renderChart = () => {
        if (!window.Recharts) {
            return (
                <div className="alert alert-danger">
                    Recharts library failed to load. Please refresh the page.
                </div>
            );
        }

        if (error) {
            return (
                <div className="alert alert-warning">
                    Visualization error: {error}
                </div>
            );
        }

        const chartData = generateChartData();
        if (!chartData.length) {
            return (
                <div style={{
                    display: 'flex',
                    justifyContent: 'center',
                    alignItems: 'center',
                    height: '300px',
                    color: '#6c757d'
                }}>
                    No data available for visualization
                </div>
            );
        }

        const { BarChart, Bar, LineChart, Line, XAxis, YAxis, CartesianGrid, Tooltip, Legend, ResponsiveContainer } = window.Recharts;
        const ChartComponent = chartConfig.type === 'bar' ? BarChart : LineChart;
        const DataComponent = chartConfig.type === 'bar' ? Bar : Line;

        return (
            <ResponsiveContainer width="100%" height={400}>
                <ChartComponent data={chartData} margin={{ top: 20, right: 30, left: 20, bottom: 60 }}>
                    <CartesianGrid strokeDasharray="3 3" />
                    <XAxis 
                        dataKey={chartConfig.xAxis} 
                        angle={-45} 
                        textAnchor="end" 
                        height={80}
                        tick={{ fontSize: 12 }}
                    />
                    <YAxis />
                    <Tooltip />
                    <Legend />
                    {chartConfig.yAxes.map((yCol, index) => (
                        <DataComponent
                            key={yCol}
                            type="monotone"
                            dataKey={yCol}
                            name={`${yCol} (${chartConfig.aggregation})`}
                            stroke={COLORS[index % COLORS.length]}
                            fill={COLORS[index % COLORS.length]}
                            activeDot={{ r: 8 }}
                        />
                    ))}
                </ChartComponent>
            </ResponsiveContainer>
        );
    };

    const renderControls = () => {
        if (!queryResults.length) return null;

        return (
            <div style={{
                display: 'grid',
                gridTemplateColumns: 'repeat(auto-fit, minmax(200px, 1fr))',
                gap: '16px',
                marginBottom: '20px'
            }}>
                <div>
                    <label>Chart Type</label>
                    <select
                        value={chartConfig.type}
                        onChange={(e) => setChartConfig({...chartConfig, type: e.target.value})}
                        className="form-control"
                    >
                        <option value="bar">Bar Chart</option>
                        <option value="line">Line Chart</option>
                    </select>
                </div>

                <div>
                    <label>X-Axis</label>
                    <select
                        value={chartConfig.xAxis}
                        onChange={(e) => setChartConfig({...chartConfig, xAxis: e.target.value})}
                        className="form-control"
                    >
                        {availableColumns.map(col => (
                            <option key={col.name} value={col.name}>
                                {col.name} ({col.type})
                            </option>
                        ))}
                    </select>
                </div>

                <div>
                    <label>Y-Axis</label>
                    <select
                        multiple
                        value={chartConfig.yAxes}
                        onChange={(e) => {
                            const selected = Array.from(e.target.selectedOptions, opt => opt.value);
                            setChartConfig({...chartConfig, yAxes: selected});
                        }}
                        className="form-control"
                        style={{ height: 'auto' }}
                    >
                        {availableColumns
                            .filter(col => col.isNumeric)
                            .map(col => (
                                <option key={col.name} value={col.name}>
                                    {col.name} (number)
                                </option>
                            ))}
                    </select>
                </div>

                <div>
                    <label>Aggregation</label>
                    <select
                        value={chartConfig.aggregation}
                        onChange={(e) => setChartConfig({...chartConfig, aggregation: e.target.value})}
                        className="form-control"
                    >
                        <option value="sum">Sum</option>
                        <option value="avg">Average</option>
                        <option value="max">Maximum</option>
                        <option value="min">Minimum</option>
                        <option value="count">Count</option>
                    </select>
                </div>
            </div>
        );
    };

    return (
        <div style={{ padding: '20px', border: '1px solid #eee', borderRadius: '8px' }}>
            <h3 style={{ marginTop: 0 }}>Data Visualization</h3>
            {renderControls()}
            {renderChart()}
            {queryResults.length > 0 && (
                <div style={{ 
                    fontSize: '12px', 
                    color: '#666', 
                    marginTop: '10px',
                    textAlign: 'center'
                }}>
                    Showing {queryResults.length} records | X-Axis: {chartConfig.xAxis} | Y-Axis: {chartConfig.yAxes.join(', ')}
                </div>
            )}
        </div>
    );
};

// Self-initializing component
const rootElement = document.getElementById('query-visualization-root');
if (rootElement) {
    ReactDOM.render(<QueryVisualizer />, rootElement);
} else {
    console.error("Could not find visualization root element");
}

export default QueryVisualizer;