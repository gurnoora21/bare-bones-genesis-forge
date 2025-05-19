/**
 * Pipeline Test Script
 * 
 * This script tests the entire music discovery pipeline by:
 * 1. Starting artist discovery for a test artist
 * 2. Monitoring the progress through each queue
 * 3. Verifying that data flows correctly through artist → album → track → producer
 */

// Configuration
const SUPABASE_URL = process.env.SUPABASE_URL || 'http://localhost:54321';
const SUPABASE_KEY = process.env.SUPABASE_ANON_KEY;
const TEST_ARTIST = 'Kendrick Lamar'; // Artist to use for testing

// Import required libraries
const { createClient } = require('@supabase/supabase-js');

// Initialize Supabase client
const supabase = createClient(SUPABASE_URL, SUPABASE_KEY);

// Colors for console output
const colors = {
  reset: '\x1b[0m',
  bright: '\x1b[1m',
  dim: '\x1b[2m',
  red: '\x1b[31m',
  green: '\x1b[32m',
  yellow: '\x1b[33m',
  blue: '\x1b[34m',
  magenta: '\x1b[35m',
  cyan: '\x1b[36m'
};

// Helper function to log with timestamp and color
function log(message, color = colors.reset) {
  const timestamp = new Date().toISOString();
  console.log(`${colors.dim}[${timestamp}]${colors.reset} ${color}${message}${colors.reset}`);
}

// Helper function to wait for a specified time
function sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

// Check queue status
async function checkQueueStatus(queueName) {
  try {
    const { data, error } = await supabase.rpc('get_queue_status', {
      queue_name: queueName
    });
    
    if (error) {
      log(`Error checking queue ${queueName}: ${error.message}`, colors.red);
      return null;
    }
    
    return data;
  } catch (err) {
    log(`Exception checking queue ${queueName}: ${err.message}`, colors.red);
    return null;
  }
}

// Start artist discovery
async function startArtistDiscovery(artistName) {
  try {
    log(`Starting discovery for artist: ${artistName}`, colors.cyan);
    
    const response = await fetch(`${SUPABASE_URL}/functions/v1/startDiscovery`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'Authorization': `Bearer ${SUPABASE_KEY}`
      },
      body: JSON.stringify({
        artist_name: artistName,
        priority: 1
      })
    });
    
    if (!response.ok) {
      const errorText = await response.text();
      throw new Error(`Failed to start discovery: ${errorText}`);
    }
    
    const result = await response.json();
    log(`Discovery started successfully: ${JSON.stringify(result)}`, colors.green);
    return result;
  } catch (err) {
    log(`Failed to start artist discovery: ${err.message}`, colors.red);
    throw err;
  }
}

// Check if artist exists in database
async function checkArtistExists(artistName) {
  try {
    const { data, error } = await supabase
      .from('artists')
      .select('id, name')
      .ilike('name', artistName)
      .limit(1);
    
    if (error) {
      log(`Error checking artist: ${error.message}`, colors.red);
      return null;
    }
    
    return data && data.length > 0 ? data[0] : null;
  } catch (err) {
    log(`Exception checking artist: ${err.message}`, colors.red);
    return null;
  }
}

// Check if albums exist for artist
async function checkArtistAlbums(artistId) {
  try {
    const { data, error } = await supabase
      .from('albums')
      .select('id, title')
      .eq('artist_id', artistId);
    
    if (error) {
      log(`Error checking albums: ${error.message}`, colors.red);
      return [];
    }
    
    return data || [];
  } catch (err) {
    log(`Exception checking albums: ${err.message}`, colors.red);
    return [];
  }
}

// Check if tracks exist for album
async function checkAlbumTracks(albumId) {
  try {
    const { data, error } = await supabase
      .from('tracks')
      .select('id, title')
      .eq('album_id', albumId);
    
    if (error) {
      log(`Error checking tracks: ${error.message}`, colors.red);
      return [];
    }
    
    return data || [];
  } catch (err) {
    log(`Exception checking tracks: ${err.message}`, colors.red);
    return [];
  }
}

// Check if producers exist for track
async function checkTrackProducers(trackId) {
  try {
    const { data, error } = await supabase
      .from('track_producers')
      .select(`
        producer_id,
        producers:producer_id (
          id, name
        )
      `)
      .eq('track_id', trackId);
    
    if (error) {
      log(`Error checking producers: ${error.message}`, colors.red);
      return [];
    }
    
    return data || [];
  } catch (err) {
    log(`Exception checking producers: ${err.message}`, colors.red);
    return [];
  }
}

// Main test function
async function testPipeline() {
  log('Starting pipeline test', colors.bright + colors.blue);
  
  try {
    // Step 1: Start artist discovery
    await startArtistDiscovery(TEST_ARTIST);
    
    // Step 2: Monitor queues
    const queues = ['artist_discovery', 'album_discovery', 'track_discovery', 'producer_identification'];
    let allQueuesEmpty = false;
    let iterations = 0;
    const MAX_ITERATIONS = 30; // Maximum number of iterations to prevent infinite loop
    
    while (!allQueuesEmpty && iterations < MAX_ITERATIONS) {
      iterations++;
      log(`\n--- Checking queues (iteration ${iterations}) ---`, colors.yellow);
      
      let emptyCount = 0;
      for (const queue of queues) {
        const status = await checkQueueStatus(queue);
        
        if (status) {
          const messageCount = status.message_count || 0;
          const color = messageCount > 0 ? colors.yellow : colors.green;
          log(`Queue ${queue}: ${messageCount} messages`, color);
          
          if (messageCount === 0) {
            emptyCount++;
          }
        }
      }
      
      allQueuesEmpty = emptyCount === queues.length;
      
      if (!allQueuesEmpty) {
        log('Waiting for queues to process...', colors.dim);
        await sleep(5000); // Wait 5 seconds before checking again
      }
    }
    
    if (iterations >= MAX_ITERATIONS) {
      log('Maximum iterations reached. Some queues may still have messages.', colors.yellow);
    } else {
      log('All queues are empty. Processing complete!', colors.green);
    }
    
    // Step 3: Verify data in database
    log('\n--- Verifying data in database ---', colors.magenta);
    
    // Check artist
    const artist = await checkArtistExists(TEST_ARTIST);
    if (!artist) {
      log(`Artist "${TEST_ARTIST}" not found in database`, colors.red);
      return;
    }
    
    log(`Found artist: ${artist.name} (${artist.id})`, colors.green);
    
    // Check albums
    const albums = await checkArtistAlbums(artist.id);
    if (albums.length === 0) {
      log(`No albums found for artist ${artist.name}`, colors.red);
      return;
    }
    
    log(`Found ${albums.length} albums for artist ${artist.name}`, colors.green);
    
    // Check tracks for first album
    const firstAlbum = albums[0];
    const tracks = await checkAlbumTracks(firstAlbum.id);
    if (tracks.length === 0) {
      log(`No tracks found for album ${firstAlbum.title}`, colors.red);
      return;
    }
    
    log(`Found ${tracks.length} tracks for album ${firstAlbum.title}`, colors.green);
    
    // Check producers for first track
    const firstTrack = tracks[0];
    const producers = await checkTrackProducers(firstTrack.id);
    if (producers.length === 0) {
      log(`No producers found for track ${firstTrack.title}`, colors.yellow);
      log('Note: Not all tracks have producer information available', colors.dim);
    } else {
      const producerNames = producers.map(p => p.producers?.name).filter(Boolean);
      log(`Found ${producers.length} producers for track ${firstTrack.title}: ${producerNames.join(', ')}`, colors.green);
    }
    
    // Final result
    log('\n--- Pipeline Test Results ---', colors.bright + colors.blue);
    log('✅ Artist discovery: ' + (artist ? 'PASSED' : 'FAILED'), artist ? colors.green : colors.red);
    log('✅ Album discovery: ' + (albums.length > 0 ? 'PASSED' : 'FAILED'), albums.length > 0 ? colors.green : colors.red);
    log('✅ Track discovery: ' + (tracks.length > 0 ? 'PASSED' : 'FAILED'), tracks.length > 0 ? colors.green : colors.red);
    log('✅ Producer identification: ' + (producers.length > 0 ? 'PASSED' : 'WARNING'), producers.length > 0 ? colors.green : colors.yellow);
    
    if (artist && albums.length > 0 && tracks.length > 0) {
      log('\n🎉 Pipeline test PASSED! The data flows correctly through the system.', colors.bright + colors.green);
    } else {
      log('\n❌ Pipeline test FAILED! Some parts of the data flow are not working correctly.', colors.bright + colors.red);
    }
    
  } catch (err) {
    log(`Pipeline test failed with error: ${err.message}`, colors.red);
    console.error(err);
  }
}

// Run the test
testPipeline().catch(console.error);
