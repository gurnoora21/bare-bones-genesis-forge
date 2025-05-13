
import React, { useState, useEffect } from 'react';
import './App.css';

// API functions
async function resetSystem() {
  try {
    const response = await fetch('https://wshetxovyxtfqohhbvpg.supabase.co/functions/v1/systemReset/reset-all', {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
      },
      body: JSON.stringify({ staleThresholdMinutes: 15 })
    });
    
    const data = await response.json();
    return { success: true, data };
  } catch (error) {
    return { success: false, error: error.message };
  }
}

async function getSystemHealth() {
  try {
    const response = await fetch('https://wshetxovyxtfqohhbvpg.supabase.co/functions/v1/systemReset/health');
    const data = await response.json();
    return { success: true, data };
  } catch (error) {
    return { success: false, error: error.message };
  }
}

function App() {
  const [health, setHealth] = useState<any>(null);
  const [loading, setLoading] = useState(false);
  const [resetResult, setResetResult] = useState<any>(null);
  const [error, setError] = useState<string | null>(null);

  // Fetch health status on mount
  useEffect(() => {
    loadHealthStatus();
  }, []);

  async function loadHealthStatus() {
    setLoading(true);
    setError(null);
    
    const result = await getSystemHealth();
    
    if (result.success) {
      setHealth(result.data);
    } else {
      setError(result.error || 'Failed to load system health');
    }
    
    setLoading(false);
  }

  async function handleSystemReset() {
    if (!confirm('Are you sure you want to reset the system? This will clear all circuit breakers and stale locks.')) {
      return;
    }
    
    setLoading(true);
    setError(null);
    setResetResult(null);
    
    const result = await resetSystem();
    
    if (result.success) {
      setResetResult(result.data);
      // Reload health after reset
      await loadHealthStatus();
    } else {
      setError(result.error || 'Failed to reset system');
    }
    
    setLoading(false);
  }

  return (
    <div className="min-h-screen bg-gray-100 p-6">
      <div className="max-w-7xl mx-auto">
        <h1 className="text-3xl font-bold mb-6">Music Producer Discovery System</h1>
        
        {/* Admin Controls */}
        <div className="mb-8 p-4 bg-white shadow rounded-lg">
          <h2 className="text-xl font-semibold mb-4">System Controls</h2>
          
          <div className="flex flex-wrap gap-4">
            <button
              onClick={loadHealthStatus}
              disabled={loading}
              className="px-4 py-2 bg-blue-500 text-white rounded hover:bg-blue-600 disabled:bg-blue-300"
            >
              {loading ? 'Loading...' : 'Refresh Status'}
            </button>
            
            <button
              onClick={handleSystemReset}
              disabled={loading}
              className="px-4 py-2 bg-red-500 text-white rounded hover:bg-red-600 disabled:bg-red-300"
            >
              {loading ? 'Resetting...' : 'Reset System'}
            </button>
          </div>
          
          {error && (
            <div className="mt-4 p-3 bg-red-100 text-red-700 rounded">
              {error}
            </div>
          )}
          
          {resetResult && (
            <div className="mt-4">
              <h3 className="font-medium">Reset Results:</h3>
              <pre className="mt-2 p-3 bg-gray-100 rounded overflow-auto text-sm">
                {JSON.stringify(resetResult, null, 2)}
              </pre>
            </div>
          )}
        </div>
        
        {/* Health Status */}
        {health && (
          <div className="p-4 bg-white shadow rounded-lg">
            <h2 className="text-xl font-semibold mb-4">System Health</h2>
            
            {/* API Services */}
            <div className="mb-6">
              <h3 className="font-medium text-lg mb-2">API Services</h3>
              <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-4">
                {Object.entries(health.api_services || {}).map(([service, data]: [string, any]) => (
                  <div 
                    key={service}
                    className="p-3 rounded border"
                    style={{
                      borderColor: 
                        data.status === 'HEALTHY' ? 'green' :
                        data.status === 'DEGRADED' ? 'orange' : 
                        data.status === 'UNHEALTHY' ? 'red' : 'gray'
                    }}
                  >
                    <div className="font-medium">{service}</div>
                    <div className="text-sm flex justify-between">
                      <span>Status:</span>
                      <span 
                        style={{
                          color: 
                            data.status === 'HEALTHY' ? 'green' :
                            data.status === 'DEGRADED' ? 'orange' : 
                            data.status === 'UNHEALTHY' ? 'red' : 'gray'
                        }}
                      >
                        {data.status}
                      </span>
                    </div>
                    <div className="text-sm flex justify-between">
                      <span>Circuit:</span>
                      <span>{data.circuitState}</span>
                    </div>
                    <div className="text-sm flex justify-between">
                      <span>Failures:</span>
                      <span>{data.failureCount}</span>
                    </div>
                  </div>
                ))}
              </div>
            </div>
            
            {/* Locks */}
            <div className="mb-6">
              <h3 className="font-medium text-lg mb-2">Stale Locks</h3>
              <div className="p-3 bg-gray-50 rounded">
                <div className="flex justify-between items-center mb-2">
                  <span>Total stale locks:</span>
                  <span 
                    className="font-medium"
                    style={{ color: health.stale_locks_count > 0 ? 'red' : 'green' }}
                  >
                    {health.stale_locks_count}
                  </span>
                </div>
                
                {health.stale_locks_count > 0 && (
                  <details>
                    <summary className="cursor-pointer text-blue-500">Show details</summary>
                    <pre className="mt-2 p-2 bg-gray-100 rounded overflow-auto text-xs">
                      {JSON.stringify(health.stale_locks, null, 2)}
                    </pre>
                  </details>
                )}
              </div>
            </div>
            
            {/* Stuck Entities */}
            <div className="mb-6">
              <h3 className="font-medium text-lg mb-2">Stuck Entities</h3>
              <div className="p-3 bg-gray-50 rounded">
                <div className="flex justify-between items-center mb-2">
                  <span>Stuck entities:</span>
                  <span 
                    className="font-medium"
                    style={{ color: health.stuck_entities_count > 0 ? 'red' : 'green' }}
                  >
                    {health.stuck_entities_count}
                  </span>
                </div>
                
                {health.stuck_entities_count > 0 && (
                  <details>
                    <summary className="cursor-pointer text-blue-500">Show details</summary>
                    <pre className="mt-2 p-2 bg-gray-100 rounded overflow-auto text-xs">
                      {JSON.stringify(health.stuck_entities, null, 2)}
                    </pre>
                  </details>
                )}
              </div>
            </div>
            
            {/* Queue Health */}
            <div>
              <h3 className="font-medium text-lg mb-2">Queue Health</h3>
              <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
                {Object.entries(health.queue_health || {}).map(([queue, data]: [string, any]) => (
                  <div key={queue} className="p-3 bg-gray-50 rounded">
                    <div className="font-medium">{queue}</div>
                    
                    {data.error ? (
                      <div className="text-red-500 text-sm">{data.error}</div>
                    ) : (
                      <>
                        <div className="flex justify-between text-sm">
                          <span>Stuck messages:</span>
                          <span 
                            style={{ color: data.stuck_messages > 0 ? 'red' : 'green' }}
                          >
                            {data.stuck_messages}
                          </span>
                        </div>
                        
                        {data.stuck_messages > 0 && (
                          <details>
                            <summary className="cursor-pointer text-blue-500 text-sm">Show details</summary>
                            <pre className="mt-2 p-2 bg-gray-100 rounded overflow-auto text-xs">
                              {JSON.stringify(data.messages, null, 2)}
                            </pre>
                          </details>
                        )}
                      </>
                    )}
                  </div>
                ))}
              </div>
            </div>
            
            <div className="mt-4 text-sm text-gray-500">
              Last updated: {new Date(health.timestamp).toLocaleString()}
            </div>
          </div>
        )}
      </div>
    </div>
  );
}

export default App;
