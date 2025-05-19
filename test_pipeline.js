/**
 * Test Script for Music Discovery Pipeline
 * 
 * This script tests the entire pipeline by:
 * 1. Sending an artist discovery message
 * 2. Monitoring the progress through each queue
 * 3. Verifying the data is correctly stored in the database
 */

// Replace with your Supabase URL and anon key
const SUPABASE_URL = process.env.SUPABASE_URL || 'https://your-project.supabase.co';
const SUPABASE_KEY = process.env.SUPABASE_ANON_KEY || 'your-anon-key';

// Import required libraries
const { createClient } = require('@supabase/supabase-js');

// Initialize Supabase client
const supabase = createClient(SUPABASE_URL, SUPABASE_KEY);

// Test artist to discover
const TEST_ARTIST = 'Drake';

/**
 * Send a message to a queue
 */
async function sendToQueue(queueName, message) {
  try {
    const response = await fetch(`${SUPABASE_URL}/functions/v1/sendToQueue`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'Authorization': `Bearer ${SUPABASE_KEY}`
      },
      body: JSON.stringify({
        queue_name: queueName,
        message: message
      })
    });
    
    const data = await response.json();
    console.log(`Message sent to ${queueName}:`, data);
    return data;
  } catch (error) {
    console.error(`Error sending to ${queueName}:`, error);
    throw error;
  }
}

/**
 * Check queue status
 */
async function checkQueueStatus(queueName) {
  try {
    const { data, error } = await supabase.rpc('get_queue_stats', {
      queue_name: queueName
    });
    
    if (error) throw error;
    
    console.log(`Queue ${queueName} status:`, data);
    return data;
  } catch (error) {
    console.error(`Error checking queue ${queueName}:`, error);
    return null;
  }
}

/**
 * Check database for artist
 */
async function checkArtistInDatabase(artistName) {
  try {
    const { data, error } = await supabase
      .from('artists')
      .select('id, name, spotify_id')
      .ilike('name', `%${artistName}%`)
      .limit(1);
    
    if (error) throw error;
    
    if (data && data.length > 0) {
      console.log(`Artist found in database:`, data[0]);
      return data[0];
    } else {
      console.log(`Artist not found in database: ${artistName}`);
      return null;
    }
  } catch (error) {
    console.error(`Error checking artist in database:`, error);
    return null;
  }
}

/**
 * Check albums for artist
 */
async function checkAlbumsForArtist(artistId) {
  try {
    const { data, error } = await supabase
      .from('albums')
      .select('id, name, spotify_id')
      .eq('artist_id', artistId);
    
    if (error) throw error;
    
    if (data && data.length > 0) {
      console.log(`Found ${data.length} albums for artist ID ${artistId}`);
      return data;
    } else {
      console.log(`No albums found for artist ID ${artistId}`);
      return [];
    }
  } catch (error) {
    console.error(`Error checking albums:`, error);
    return [];
  }
}

/**
 * Check tracks for album
 */
async function checkTracksForAlbum(albumId) {
  try {
    const { data, error } = await supabase
      .from('tracks')
      .select('id, name, spotify_id')
      .eq('album_id', albumId);
    
    if (error) throw error;
    
    if (data && data.length > 0) {
      console.log(`Found ${data.length} tracks for album ID ${albumId}`);
      return data;
    } else {
      console.log(`No tracks found for album ID ${albumId}`);
      return [];
    }
  } catch (error) {
    console.error(`Error checking tracks:`, error);
    return [];
  }
}

/**
 * Check producers for track
 */
async function checkProducersForTrack(trackId) {
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
    
    if (error) throw error;
    
    if (data && data.length > 0) {
      console.log(`Found ${data.length} producers for track ID ${trackId}`);
      return data;
    } else {
      console.log(`No producers found for track ID ${trackId}`);
      return [];
    }
  } catch (error) {
    console.error(`Error checking producers:`, error);
    return [];
  }
}

/**
 * Main test function
 */
async function testPipeline() {
  console.log(`Starting pipeline test for artist: ${TEST_ARTIST}`);
  
  // Step 1: Send artist discovery message
  const artistMessage = {
    artistName: TEST_ARTIST
  };
  
  await sendToQueue('artist_discovery', artistMessage);
  
  // Step 2: Wait for processing (in a real test, you'd use polling or webhooks)
  console.log('Waiting for initial processing...');
  await new Promise(resolve => setTimeout(resolve, 5000));
  
  // Step 3: Check queue statuses
  await checkQueueStatus('artist_discovery');
  await checkQueueStatus('album_discovery');
  await checkQueueStatus('track_discovery');
  await checkQueueStatus('producer_identification');
  
  // Step 4: Check database for artist
  const artist = await checkArtistInDatabase(TEST_ARTIST);
  
  if (!artist) {
    console.log('Test failed: Artist not found in database');
    return;
  }
  
  // Step 5: Wait for album processing
  console.log('Waiting for album processing...');
  await new Promise(resolve => setTimeout(resolve, 10000));
  
  // Step 6: Check albums
  const albums = await checkAlbumsForArtist(artist.id);
  
  if (albums.length === 0) {
    console.log('Test failed: No albums found for artist');
    return;
  }
  
  // Step 7: Wait for track processing
  console.log('Waiting for track processing...');
  await new Promise(resolve => setTimeout(resolve, 10000));
  
  // Step 8: Check tracks for first album
  const tracks = await checkTracksForAlbum(albums[0].id);
  
  if (tracks.length === 0) {
    console.log('Test failed: No tracks found for album');
    return;
  }
  
  // Step 9: Wait for producer processing
  console.log('Waiting for producer processing...');
  await new Promise(resolve => setTimeout(resolve, 10000));
  
  // Step 10: Check producers for first track
  const producers = await checkProducersForTrack(tracks[0].id);
  
  // Final results
  console.log('\n--- Pipeline Test Results ---');
  console.log(`Artist: ${artist.name} (ID: ${artist.id})`);
  console.log(`Albums found: ${albums.length}`);
  console.log(`Tracks found: ${tracks.length}`);
  console.log(`Producers found: ${producers.length}`);
  
  if (producers.length > 0) {
    console.log('Test PASSED: Full pipeline is working!');
  } else {
    console.log('Test INCOMPLETE: Pipeline did not complete all stages');
  }
}

// Run the test
testPipeline().catch(error => {
  console.error('Test failed with error:', error);
});
