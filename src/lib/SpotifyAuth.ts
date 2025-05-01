
import { createClient } from '@supabase/supabase-js';
import { EnvConfig } from './EnvConfig';

export interface SpotifyToken {
  access_token: string;
  token_type: string;
  expires_at: number;
}

export class SpotifyAuth {
  private static instance: SpotifyAuth;
  private supabase: ReturnType<typeof createClient>;
  private token: SpotifyToken | null = null;

  private constructor() {
    this.supabase = createClient(
      EnvConfig.SUPABASE_URL,
      EnvConfig.SUPABASE_SERVICE_ROLE_KEY
    );
  }

  public static getInstance(): SpotifyAuth {
    if (!SpotifyAuth.instance) {
      SpotifyAuth.instance = new SpotifyAuth();
    }
    return SpotifyAuth.instance;
  }

  public async getToken(): Promise<string> {
    // Check if we have a valid token
    if (this.token && this.token.expires_at > Date.now()) {
      return this.token.access_token;
    }

    // If not, get a new token
    const clientId = EnvConfig.SPOTIFY_CLIENT_ID;
    const clientSecret = EnvConfig.SPOTIFY_CLIENT_SECRET;

    if (!clientId || !clientSecret) {
      throw new Error('Missing Spotify credentials');
    }

    const response = await fetch('https://accounts.spotify.com/api/token', {
      method: 'POST',
      headers: {
        'Content-Type': 'application/x-www-form-urlencoded',
        'Authorization': `Basic ${btoa(`${clientId}:${clientSecret}`)}`
      },
      body: 'grant_type=client_credentials'
    });

    if (!response.ok) {
      throw new Error(`Failed to get Spotify token: ${response.statusText}`);
    }

    const data = await response.json();
    
    // Store token with expiration (subtract 60 seconds as buffer)
    this.token = {
      access_token: data.access_token,
      token_type: data.token_type,
      expires_at: Date.now() + (data.expires_in * 1000) - 60000
    };

    return this.token.access_token;
  }
}
