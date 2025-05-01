
// Edge function adapter for Supabase

import { handleArtistDiscovery } from '../workers/artistDiscovery';
import { handleAlbumDiscovery } from '../workers/albumDiscovery';
import { handleTrackDiscovery } from '../workers/trackDiscovery';
import { handleProducerIdentification } from '../workers/producerIdentification';
import { handleSocialEnrichment } from '../workers/socialEnrichment';

// This adapter lets us test the workers locally
export const workerHandlers = {
  artistDiscovery: handleArtistDiscovery,
  albumDiscovery: handleAlbumDiscovery,
  trackDiscovery: handleTrackDiscovery,
  producerIdentification: handleProducerIdentification,
  socialEnrichment: handleSocialEnrichment
};

// When deploying to Supabase Edge Functions, you would need to create
// separate function files that import these handlers
