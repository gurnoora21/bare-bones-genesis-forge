
// Environment configuration helper
// This can be replaced with actual environment variables from your hosting provider

export const EnvConfig = {
  // Supabase configuration
  SUPABASE_URL: process.env.SUPABASE_URL || 'https://your-supabase-project.supabase.co',
  SUPABASE_SERVICE_ROLE_KEY: process.env.SUPABASE_SERVICE_ROLE_KEY || 'your-service-role-key',
  
  // Spotify API credentials
  SPOTIFY_CLIENT_ID: process.env.SPOTIFY_CLIENT_ID || 'your-spotify-client-id',
  SPOTIFY_CLIENT_SECRET: process.env.SPOTIFY_CLIENT_SECRET || 'your-spotify-client-secret',
  
  // Genius API key
  GENIUS_ACCESS_TOKEN: process.env.GENIUS_ACCESS_TOKEN || 'your-genius-access-token',
  
  // Instagram API key (or other social media)
  INSTAGRAM_API_KEY: process.env.INSTAGRAM_API_KEY || 'your-instagram-api-key',
};
