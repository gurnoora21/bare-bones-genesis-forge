import { createClient } from '@supabase/supabase-js';
import { BaseWorker } from '../lib/BaseWorker.ts';
import { EnvConfig } from '../lib/EnvConfig';

interface SocialEnrichmentMessage {
  producerId: string;
  producerName: string;
}

export class SocialEnrichmentWorker extends BaseWorker<SocialEnrichmentMessage> {
  constructor() {
    super({
      queueName: 'social_enrichment',
      batchSize: 10,
      visibilityTimeout: 180,
      maxRetries: 3
    });
    
    // Configure rate limits for Instagram API (or other social media)
    this.maxRequestsPerWindow.instagram = 10;  // 10 requests per minute
    this.windowMs.instagram = 60000;           // 1 minute window
  }

  async processMessage(message: SocialEnrichmentMessage): Promise<void> {
    const { producerId, producerName } = message;

    // Get producer details
    const { data: producer, error: producerError } = await this.supabase
      .from('producers')
      .select('id, name, normalized_name')
      .eq('id', producerId)
      .single();

    if (producerError || !producer) {
      throw new Error(`Producer not found with ID: ${producerId}`);
    }

    try {
      // Ensure producer.name is a string
      if (typeof producer.name !== 'string') {
        throw new Error(`Invalid producer name for producer ${producerId}`);
      }
      
      // Try to find Instagram profile
      const instagramData = await this.findInstagramProfile(producer.name);
      
      // Update producer with social media info
      await this.supabase
        .from('producers')
        .update({
          instagram_handle: instagramData?.username || null,
          instagram_bio: instagramData?.bio || null,
          image_url: instagramData?.profile_pic_url || null,
          enriched_at: new Date().toISOString(),
          enrichment_failed: false
        })
        .eq('id', producerId);
      
      console.log(`Enriched producer ${producerName} with social data`);
    } catch (error) {
      console.error(`Error enriching producer ${producerName}:`, error);
      
      // Mark as failed but allow future retries
      await this.supabase
        .from('producers')
        .update({
          enriched_at: new Date().toISOString(),
          enrichment_failed: true
        })
        .eq('id', producerId);
      
      // Don't throw the error, just log it and mark as failed
    }
  }

  private async findInstagramProfile(producerName: string): Promise<any | null> {
    await this.waitForRateLimit('instagram');

    return this.withCircuitBreaker('instagram', async () => {
      try {
        // Instagram doesn't have an official API for this purpose,
        // so this would typically use a service like Proxycurl, SerpAPI, or similar
        
        const instagramApiKey = EnvConfig.INSTAGRAM_API_KEY;
        if (!instagramApiKey) {
          throw new Error('Instagram API key not configured');
        }
        
        // Example using a hypothetical API service
        const response = await this.cachedFetch<any>(
          `https://api.example.com/instagram/profile?username=${encodeURIComponent(producerName)}`,
          {
            headers: {
              'Authorization': `Bearer ${instagramApiKey}`
            }
          },
          86400000 // Cache for 24 hours
        );
        
        if (!response.success) {
          return null;
        }
        
        return {
          username: response.data.username,
          bio: response.data.biography,
          profile_pic_url: response.data.profile_pic_url,
          verified: response.data.is_verified
        };
      } catch (error) {
        console.warn(`Instagram lookup failed for ${producerName}:`, error);
        return null;
      }
    });
  }
}

// Node.js version of Edge function handler
export async function handleSocialEnrichment(): Promise<any> {
  try {
    const worker = new SocialEnrichmentWorker();
    const metrics = await worker.processBatch();
    
    return metrics;
  } catch (error) {
    console.error('Error in social enrichment worker:', error);
    return { error: error instanceof Error ? error.message : 'Unknown error' };
  }
}
