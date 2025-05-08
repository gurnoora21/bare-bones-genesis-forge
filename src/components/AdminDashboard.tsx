
import React, { useState, useEffect } from 'react';
import { supabase } from "@/integrations/supabase/client";
import { Card, CardContent, CardDescription, CardFooter, CardHeader, CardTitle } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs";
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "@/components/ui/table";
import { useToast } from "@/hooks/use-toast";
import {
  LineChart,
  Line,
  BarChart, 
  Bar,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  Legend,
  ResponsiveContainer
} from 'recharts';

interface QueueStats {
  name: string;
  messageCount: number;
  oldestMessage: string | null;
  processingRate: number | null;
  avgProcessingTime: number | null;
  errorRate: number | null;
}

interface ProcessingStats {
  entityType: string;
  total: number;
  pending: number;
  inProgress: number;
  completed: number;
  failed: number;
  stuckCount: number;
}

interface WorkerIssue {
  id: string;
  worker_name: string;
  issue_type: string;
  message: string;
  details: any;
  created_at: string;
}

interface SystemHealth {
  queueStats: QueueStats[];
  processingStats: ProcessingStats[];
  deduplicationMetrics: Record<string, any>;
  redisKeyStats: Record<string, number>;
  workerIssues: {
    total: number;
    byWorker: Record<string, number>;
    byType: Record<string, number>;
    recent: WorkerIssue[];
  };
}

export default function AdminDashboard() {
  const [systemHealth, setSystemHealth] = useState<SystemHealth | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [activeTab, setActiveTab] = useState("queues");
  const { toast } = useToast();

  useEffect(() => {
    fetchDashboardData();
    
    // Refresh data every 30 seconds
    const intervalId = setInterval(fetchDashboardData, 30000);
    
    return () => clearInterval(intervalId);
  }, []);

  async function fetchDashboardData() {
    try {
      setLoading(true);
      
      const { data, error } = await supabase.functions.invoke('dashboardData');
      
      if (error) {
        throw error;
      }
      
      setSystemHealth(data as SystemHealth);
      setError(null);
    } catch (err: any) {
      setError(err.message || "Failed to fetch dashboard data");
      console.error("Dashboard data error:", err);
    } finally {
      setLoading(false);
    }
  }
  
  async function clearIdempotencyKeys(queueName?: string, ageMinutes?: number) {
    try {
      const params: Record<string, any> = {};
      if (queueName) params.queueName = queueName;
      if (ageMinutes) params.age = ageMinutes;
      
      const { data, error } = await supabase.functions.invoke('clearIdempotencyKeys', {
        body: params
      });
      
      if (error) {
        throw error;
      }
      
      toast({
        title: "Success",
        description: `Cleared ${data.details.keysDeleted} idempotency keys`,
      });
      
      // Refresh dashboard data
      fetchDashboardData();
    } catch (err: any) {
      toast({
        title: "Error clearing idempotency keys",
        description: err.message,
        variant: "destructive",
      });
    }
  }

  if (loading && !systemHealth) {
    return (
      <div className="flex items-center justify-center h-64">
        <div className="animate-spin rounded-full h-12 w-12 border-t-2 border-b-2 border-primary"></div>
      </div>
    );
  }

  if (error) {
    return (
      <div className="bg-red-50 border border-red-200 text-red-700 p-4 rounded-md">
        <h3 className="text-lg font-semibold">Error</h3>
        <p>{error}</p>
        <Button onClick={fetchDashboardData} variant="outline" className="mt-2">
          Try Again
        </Button>
      </div>
    );
  }

  if (!systemHealth) {
    return null;
  }

  // Transform data for charts
  const queueBarData = systemHealth.queueStats.map(q => ({
    name: q.name,
    pending: q.messageCount,
    avgTime: q.avgProcessingTime || 0,
  }));

  const processingStateData = systemHealth.processingStats.map(p => ({
    name: p.entityType,
    pending: p.pending,
    inProgress: p.inProgress,
    completed: p.completed,
    failed: p.failed,
    stuck: p.stuckCount,
  }));

  return (
    <div className="container mx-auto py-8">
      <div className="flex justify-between items-center mb-6">
        <div>
          <h1 className="text-3xl font-bold">System Health Dashboard</h1>
          <p className="text-muted-foreground">
            Monitor queues, processing status, and system health
          </p>
        </div>
        <div className="flex gap-2">
          <Button 
            onClick={() => fetchDashboardData()}
            variant="outline"
            size="sm"
          >
            Refresh
          </Button>
        </div>
      </div>
      
      <Tabs value={activeTab} onValueChange={setActiveTab} className="space-y-4">
        <TabsList>
          <TabsTrigger value="queues">Queues</TabsTrigger>
          <TabsTrigger value="processing">Processing</TabsTrigger>
          <TabsTrigger value="redis">Redis</TabsTrigger>
          <TabsTrigger value="issues">Issues</TabsTrigger>
        </TabsList>
        
        {/* Queue Stats */}
        <TabsContent value="queues" className="space-y-4">
          <div className="grid grid-cols-1 gap-6">
            <Card>
              <CardHeader>
                <CardTitle>Queue Statistics</CardTitle>
                <CardDescription>
                  Message counts and processing metrics for all queues
                </CardDescription>
              </CardHeader>
              <CardContent>
                <div className="h-80">
                  <ResponsiveContainer width="100%" height="100%">
                    <BarChart data={queueBarData}>
                      <CartesianGrid strokeDasharray="3 3" />
                      <XAxis dataKey="name" />
                      <YAxis yAxisId="left" orientation="left" stroke="#8884d8" />
                      <YAxis yAxisId="right" orientation="right" stroke="#82ca9d" />
                      <Tooltip />
                      <Legend />
                      <Bar yAxisId="left" dataKey="pending" fill="#8884d8" name="Pending Messages" />
                      <Bar yAxisId="right" dataKey="avgTime" fill="#82ca9d" name="Avg. Process Time (s)" />
                    </BarChart>
                  </ResponsiveContainer>
                </div>
                
                <Table>
                  <TableHeader>
                    <TableRow>
                      <TableHead>Queue</TableHead>
                      <TableHead>Pending</TableHead>
                      <TableHead>Proc. Rate</TableHead>
                      <TableHead>Avg Time</TableHead>
                      <TableHead>Error Rate</TableHead>
                      <TableHead>Oldest</TableHead>
                      <TableHead>Actions</TableHead>
                    </TableRow>
                  </TableHeader>
                  <TableBody>
                    {systemHealth.queueStats.map((queue) => (
                      <TableRow key={queue.name}>
                        <TableCell>{queue.name}</TableCell>
                        <TableCell>{queue.messageCount}</TableCell>
                        <TableCell>
                          {queue.processingRate !== null 
                            ? `${queue.processingRate.toFixed(2)}/min`
                            : '-'}
                        </TableCell>
                        <TableCell>
                          {queue.avgProcessingTime !== null
                            ? `${queue.avgProcessingTime.toFixed(2)}s`
                            : '-'}
                        </TableCell>
                        <TableCell>
                          {queue.errorRate !== null
                            ? `${(queue.errorRate * 100).toFixed(2)}%`
                            : '-'}
                        </TableCell>
                        <TableCell>
                          {queue.oldestMessage 
                            ? new Date(queue.oldestMessage).toLocaleString()
                            : '-'}
                        </TableCell>
                        <TableCell>
                          <Button 
                            onClick={() => clearIdempotencyKeys(queue.name)}
                            variant="outline"
                            size="sm"
                          >
                            Clear Keys
                          </Button>
                        </TableCell>
                      </TableRow>
                    ))}
                  </TableBody>
                </Table>
              </CardContent>
              <CardFooter>
                <div className="flex gap-2">
                  <Button onClick={() => clearIdempotencyKeys()}>
                    Clear All Idempotency Keys
                  </Button>
                  <Button onClick={() => clearIdempotencyKeys(undefined, 60)} variant="outline">
                    Clear Keys Older Than 1 Hour
                  </Button>
                </div>
              </CardFooter>
            </Card>
          </div>
        </TabsContent>
        
        {/* Processing Stats */}
        <TabsContent value="processing" className="space-y-4">
          <div className="grid grid-cols-1 gap-6">
            <Card>
              <CardHeader>
                <CardTitle>Processing Status</CardTitle>
                <CardDescription>
                  Entity processing state across the system
                </CardDescription>
              </CardHeader>
              <CardContent>
                <div className="h-80">
                  <ResponsiveContainer width="100%" height="100%">
                    <BarChart data={processingStateData}>
                      <CartesianGrid strokeDasharray="3 3" />
                      <XAxis dataKey="name" />
                      <YAxis />
                      <Tooltip />
                      <Legend />
                      <Bar dataKey="pending" stackId="a" fill="#8884d8" />
                      <Bar dataKey="inProgress" stackId="a" fill="#82ca9d" />
                      <Bar dataKey="completed" stackId="a" fill="#8dd1e1" />
                      <Bar dataKey="failed" stackId="a" fill="#fd8d3c" />
                      <Bar dataKey="stuck" stackId="a" fill="#e6550d" />
                    </BarChart>
                  </ResponsiveContainer>
                </div>
                
                <Table>
                  <TableHeader>
                    <TableRow>
                      <TableHead>Entity Type</TableHead>
                      <TableHead>Total</TableHead>
                      <TableHead>Pending</TableHead>
                      <TableHead>In Progress</TableHead>
                      <TableHead>Completed</TableHead>
                      <TableHead>Failed</TableHead>
                      <TableHead>Stuck</TableHead>
                    </TableRow>
                  </TableHeader>
                  <TableBody>
                    {systemHealth.processingStats.map((stats) => (
                      <TableRow key={stats.entityType}>
                        <TableCell>{stats.entityType}</TableCell>
                        <TableCell>{stats.total}</TableCell>
                        <TableCell>{stats.pending}</TableCell>
                        <TableCell>{stats.inProgress}</TableCell>
                        <TableCell>{stats.completed}</TableCell>
                        <TableCell>{stats.failed}</TableCell>
                        <TableCell>
                          {stats.stuckCount > 0 ? (
                            <span className="text-red-500">{stats.stuckCount}</span>
                          ) : (
                            stats.stuckCount
                          )}
                        </TableCell>
                      </TableRow>
                    ))}
                  </TableBody>
                </Table>
              </CardContent>
            </Card>
          </div>
        </TabsContent>
        
        {/* Redis Keys */}
        <TabsContent value="redis" className="space-y-4">
          <div className="grid grid-cols-1 gap-6">
            <Card>
              <CardHeader>
                <CardTitle>Redis Keys</CardTitle>
                <CardDescription>
                  Redis key counts by category
                </CardDescription>
              </CardHeader>
              <CardContent>
                <Table>
                  <TableHeader>
                    <TableRow>
                      <TableHead>Key Pattern</TableHead>
                      <TableHead>Count</TableHead>
                      <TableHead>Actions</TableHead>
                    </TableRow>
                  </TableHeader>
                  <TableBody>
                    {Object.entries(systemHealth.redisKeyStats)
                      .sort((a, b) => b[1] - a[1]) // Sort by count descending
                      .map(([pattern, count]) => (
                        <TableRow key={pattern}>
                          <TableCell>{pattern}</TableCell>
                          <TableCell>{count}</TableCell>
                          <TableCell>
                            {pattern.startsWith('dedup:') && (
                              <Button 
                                onClick={() => clearIdempotencyKeys(pattern.split(':')[1])}
                                variant="outline"
                                size="sm"
                              >
                                Clear
                              </Button>
                            )}
                          </TableCell>
                        </TableRow>
                      ))}
                  </TableBody>
                </Table>
              </CardContent>
            </Card>
            
            {/* Deduplication Metrics */}
            <Card>
              <CardHeader>
                <CardTitle>Deduplication Metrics</CardTitle>
                <CardDescription>
                  Effectiveness of deduplication by queue
                </CardDescription>
              </CardHeader>
              <CardContent>
                <Table>
                  <TableHeader>
                    <TableRow>
                      <TableHead>Queue</TableHead>
                      <TableHead>Total Processed</TableHead>
                      <TableHead>Deduplicated</TableHead>
                      <TableHead>Dedup Rate</TableHead>
                    </TableRow>
                  </TableHeader>
                  <TableBody>
                    {Object.entries(systemHealth.deduplicationMetrics).map(([queueName, metrics]: [string, any]) => (
                      <TableRow key={queueName}>
                        <TableCell>{queueName}</TableCell>
                        <TableCell>{metrics.totalProcessed || 0}</TableCell>
                        <TableCell>{metrics.totalDeduplicated || 0}</TableCell>
                        <TableCell>
                          {metrics.totalProcessed > 0
                            ? `${((metrics.totalDeduplicated / metrics.totalProcessed) * 100).toFixed(2)}%`
                            : '0%'}
                        </TableCell>
                      </TableRow>
                    ))}
                  </TableBody>
                </Table>
              </CardContent>
            </Card>
          </div>
        </TabsContent>
        
        {/* Worker Issues */}
        <TabsContent value="issues" className="space-y-4">
          <div className="grid grid-cols-1 gap-6">
            <Card>
              <CardHeader>
                <CardTitle>Worker Issues</CardTitle>
                <CardDescription>
                  Recent worker issues and error statistics
                </CardDescription>
              </CardHeader>
              <CardContent>
                <div className="grid grid-cols-2 gap-4 mb-6">
                  <div>
                    <h3 className="text-md font-medium mb-2">Issues by Worker</h3>
                    <div className="bg-muted p-4 rounded-md">
                      {Object.entries(systemHealth.workerIssues.byWorker)
                        .sort((a, b) => b[1] - a[1]) // Sort by count descending
                        .map(([worker, count]) => (
                          <div key={worker} className="flex justify-between py-1 border-b border-border">
                            <span>{worker}</span>
                            <span>{count}</span>
                          </div>
                        ))}
                    </div>
                  </div>
                  <div>
                    <h3 className="text-md font-medium mb-2">Issues by Type</h3>
                    <div className="bg-muted p-4 rounded-md">
                      {Object.entries(systemHealth.workerIssues.byType)
                        .sort((a, b) => b[1] - a[1]) // Sort by count descending
                        .map(([type, count]) => (
                          <div key={type} className="flex justify-between py-1 border-b border-border">
                            <span>{type}</span>
                            <span>{count}</span>
                          </div>
                        ))}
                    </div>
                  </div>
                </div>
                
                <h3 className="text-md font-medium mb-2">Recent Issues</h3>
                <Table>
                  <TableHeader>
                    <TableRow>
                      <TableHead>Time</TableHead>
                      <TableHead>Worker</TableHead>
                      <TableHead>Type</TableHead>
                      <TableHead>Message</TableHead>
                    </TableRow>
                  </TableHeader>
                  <TableBody>
                    {systemHealth.workerIssues.recent.map((issue) => (
                      <TableRow key={issue.id}>
                        <TableCell>{new Date(issue.created_at).toLocaleString()}</TableCell>
                        <TableCell>{issue.worker_name}</TableCell>
                        <TableCell>{issue.issue_type}</TableCell>
                        <TableCell>{issue.message}</TableCell>
                      </TableRow>
                    ))}
                  </TableBody>
                </Table>
              </CardContent>
            </Card>
          </div>
        </TabsContent>
      </Tabs>
    </div>
  );
}
