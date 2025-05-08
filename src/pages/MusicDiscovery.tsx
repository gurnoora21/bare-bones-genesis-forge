
import { useState } from "react";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Card, CardContent, CardDescription, CardFooter, CardHeader, CardTitle } from "@/components/ui/card";
import { Separator } from "@/components/ui/separator";
import { toast } from "sonner";
import { supabase } from "@/integrations/supabase/client";

const MusicDiscovery = () => {
  const [artistName, setArtistName] = useState("");
  const [queueName, setQueueName] = useState("artist_discovery");
  const [isLoading, setIsLoading] = useState(false);
  const [isProcessing, setIsProcessing] = useState(false);

  const startDiscovery = async () => {
    if (!artistName) {
      toast.error("Please enter an artist name");
      return;
    }

    setIsLoading(true);
    try {
      const response = await supabase.functions.invoke('startDiscovery', {
        body: { artistName }
      });

      if (response.error) {
        throw new Error(response.error.message);
      }

      toast.success(`Discovery started for ${artistName}`, {
        description: response.data.message
      });
      
      // Clear input after successful submission
      setArtistName("");
    } catch (error) {
      console.error("Error starting discovery:", error);
      toast.error("Failed to start discovery", {
        description: error.message
      });
    } finally {
      setIsLoading(false);
    }
  };

  const forceProcessQueue = async () => {
    if (!queueName) {
      toast.error("Please select a queue");
      return;
    }

    setIsProcessing(true);
    try {
      const response = await supabase.functions.invoke('forceProcessQueue', {
        body: { 
          queueName,
          batchSize: 10,
          force: true
        }
      });

      if (response.error) {
        throw new Error(response.error.message);
      }

      toast.success(`Processed queue: ${queueName}`, {
        description: response.data.message
      });
    } catch (error) {
      console.error("Error processing queue:", error);
      toast.error("Failed to process queue", {
        description: error.message
      });
    } finally {
      setIsProcessing(false);
    }
  };

  return (
    <div className="container mx-auto py-8">
      <h1 className="text-3xl font-bold mb-6">Music Producer Discovery</h1>
      
      <div className="grid gap-6 md:grid-cols-2">
        {/* Artist Discovery Card */}
        <Card>
          <CardHeader>
            <CardTitle>Start Artist Discovery</CardTitle>
            <CardDescription>
              Enter an artist name to start the discovery process for their albums, tracks, and producers.
            </CardDescription>
          </CardHeader>
          <CardContent>
            <div className="flex items-center space-x-2">
              <Input 
                placeholder="Artist name (e.g., Drake)" 
                value={artistName}
                onChange={(e) => setArtistName(e.target.value)}
              />
              <Button onClick={startDiscovery} disabled={isLoading}>
                {isLoading ? "Starting..." : "Start"}
              </Button>
            </div>
          </CardContent>
        </Card>

        {/* Queue Processing Card */}
        <Card>
          <CardHeader>
            <CardTitle>Process Queue</CardTitle>
            <CardDescription>
              Force processing of a specific queue to resolve stuck messages.
            </CardDescription>
          </CardHeader>
          <CardContent>
            <div className="flex items-center space-x-2">
              <select
                className="flex h-10 w-full rounded-md border border-input bg-background px-3 py-2 text-sm ring-offset-background file:border-0 file:bg-transparent file:text-sm file:font-medium placeholder:text-muted-foreground focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring focus-visible:ring-offset-2 disabled:cursor-not-allowed disabled:opacity-50"
                value={queueName}
                onChange={(e) => setQueueName(e.target.value)}
              >
                <option value="artist_discovery">Artist Discovery</option>
                <option value="album_discovery">Album Discovery</option>
                <option value="track_discovery">Track Discovery</option>
                <option value="producer_identification">Producer Identification</option>
                <option value="social_enrichment">Social Enrichment</option>
              </select>
              <Button onClick={forceProcessQueue} disabled={isProcessing}>
                {isProcessing ? "Processing..." : "Process"}
              </Button>
            </div>
          </CardContent>
        </Card>
      </div>

      <Separator className="my-8" />

      <div className="rounded-lg bg-slate-50 p-6 dark:bg-slate-900">
        <h2 className="text-xl font-semibold mb-4">System Information</h2>
        <p className="mb-4">
          The music producer discovery system processes data in several stages:
        </p>
        <ol className="list-decimal list-inside space-y-2 ml-4">
          <li>Artist Discovery: Finds and saves artist information from Spotify</li>
          <li>Album Discovery: Retrieves albums for the discovered artists</li>
          <li>Track Discovery: Gets track information from albums</li>
          <li>Producer Identification: Identifies producers from track credits and external data</li>
          <li>Social Enrichment: Enhances producer profiles with social media information</li>
        </ol>
      </div>
    </div>
  );
};

export default MusicDiscovery;
