
// Transaction Demo Edge Function
// Shows how to use the transaction manager and idempotency system for multi-step operations

import { createClient } from 'https://esm.sh/@supabase/supabase-js@2.39.7';
import { serve } from 'https://deno.land/std@0.177.0/http/server.ts';
import { getTransactionManager } from '../_shared/transactionManager.ts';
import { getIdempotencyManager } from '../_shared/idempotencyManager.ts';

// CORS headers
const corsHeaders = {
  'Access-Control-Allow-Origin': '*',
  'Access-Control-Allow-Headers': 'authorization, x-client-info, apikey, content-type',
};

// Set up Supabase client
const supabaseUrl = Deno.env.get('SUPABASE_URL') || '';
const supabaseServiceRole = Deno.env.get('SUPABASE_SERVICE_ROLE_KEY') || '';
const supabase = createClient(supabaseUrl, supabaseServiceRole);

serve(async (req) => {
  // Handle CORS preflight request
  if (req.method === 'OPTIONS') {
    return new Response(null, { headers: corsHeaders, status: 204 });
  }

  try {
    // Parse the request body
    let reqData: any = {};
    try {
      reqData = await req.json();
    } catch (e) {
      reqData = {};
    }

    const action = new URL(req.url).searchParams.get('action') || 'demo';
    const operationId = reqData.operationId || new URL(req.url).searchParams.get('operationId');

    // Initialize transaction manager
    const transactionManager = getTransactionManager(supabase);

    switch (action) {
      case 'atomic': {
        // Example of using database atomic functions
        const { artistName, popularity } = reqData;
        
        if (!artistName) {
          return new Response(
            JSON.stringify({ error: 'Artist name is required' }),
            { headers: { ...corsHeaders, 'Content-Type': 'application/json' }, status: 400 }
          );
        }

        const result = await transactionManager.atomicOperation(
          'process_artist_atomic',
          {
            p_artist_data: {
              name: artistName,
              popularity: popularity || 0,
              metadata: { source: 'transactionDemo' }
            },
            p_operation_id: operationId || `artist_${Date.now()}_${Math.random().toString(36).substring(2, 10)}`,
            p_spotify_id: null
          },
          {
            operationId: operationId,
            entityType: 'artist',
            entityId: artistName.toLowerCase().replace(/[^a-z0-9]/g, '')
          }
        );

        return new Response(
          JSON.stringify({ success: true, result }),
          { headers: { ...corsHeaders, 'Content-Type': 'application/json' } }
        );
      }

      case 'multi-step': {
        // Example of multi-step transaction with idempotency
        const { artistName, albumName } = reqData;
        
        if (!artistName || !albumName) {
          return new Response(
            JSON.stringify({ error: 'Artist and album names are required' }),
            { headers: { ...corsHeaders, 'Content-Type': 'application/json' }, status: 400 }
          );
        }

        const idempotencyOp = operationId || `multi_${artistName}_${albumName}_${Date.now()}`;
        const idempotencyManager = getIdempotencyManager(supabase);

        // Check if already processed
        const existingResult = await idempotencyManager.checkOperation({
          operationId: idempotencyOp,
          entityType: 'multi-step',
          entityId: `${artistName}_${albumName}`
        });

        if (existingResult.alreadyProcessed) {
          return new Response(
            JSON.stringify({ 
              success: true, 
              result: existingResult.result,
              cached: true,
              message: 'Retrieved from cache - operation already processed'
            }),
            { headers: { ...corsHeaders, 'Content-Type': 'application/json' } }
          );
        }

        // Execute multi-step operation in transaction
        const result = await transactionManager.transaction(async (db, txId) => {
          console.log(`Starting multi-step transaction ${txId}`);
          
          // Step 1: Create artist
          const artistResult = await db.rpc('process_artist_atomic', {
            p_artist_data: {
              name: artistName,
              metadata: { source: 'transactionDemo', txId }
            },
            p_operation_id: `${idempotencyOp}_artist`,
            p_spotify_id: null
          });

          if (!artistResult.artist_id) {
            throw new Error('Failed to create artist');
          }

          // Step 2: Create album
          const albumResult = await db.rpc('process_album_atomic', {
            p_album_data: {
              name: albumName,
              artist_id: artistResult.artist_id,
              metadata: { source: 'transactionDemo', txId }
            },
            p_operation_id: `${idempotencyOp}_album`,
            p_spotify_id: null
          });

          if (!albumResult.album_id) {
            throw new Error('Failed to create album');
          }

          // Return combined result
          return {
            artist: {
              id: artistResult.artist_id,
              name: artistName
            },
            album: {
              id: albumResult.album_id,
              name: albumName
            },
            txId
          };
        }, {
          timeout: 10000,
          retryOnConflict: true,
          maxRetries: 3,
          idempotency: {
            operationId: idempotencyOp,
            entityType: 'multi-step',
            entityId: `${artistName}_${albumName}`
          }
        });

        return new Response(
          JSON.stringify({ success: true, result }),
          { headers: { ...corsHeaders, 'Content-Type': 'application/json' } }
        );
      }

      case 'sql': {
        // Example of executing SQL with transaction safety and idempotency
        const { name, query } = reqData;
        
        if (!name) {
          return new Response(
            JSON.stringify({ error: 'Name parameter is required' }),
            { headers: { ...corsHeaders, 'Content-Type': 'application/json' }, status: 400 }
          );
        }

        // This is a safe SQL example - don't allow arbitrary SQL
        const sql = query || `
          WITH artist_insert AS (
            INSERT INTO artists (name, metadata)
            VALUES ($1, jsonb_build_object('source', 'sql_demo'))
            RETURNING id, name
          )
          SELECT id::text, name FROM artist_insert
        `;

        const result = await transactionManager.executeSql(sql, {
          operationId: operationId || `sql_${Date.now()}_${Math.random().toString(36).substring(2, 10)}`,
          entityType: 'sql_demo',
          entityId: name,
          params: { 1: name }
        });

        return new Response(
          JSON.stringify({ success: true, result }),
          { headers: { ...corsHeaders, 'Content-Type': 'application/json' } }
        );
      }

      default: {
        // Demo endpoints list
        return new Response(
          JSON.stringify({
            message: 'Transaction Manager Demo',
            availableActions: [
              {
                action: 'atomic',
                description: 'Use atomic database function',
                example: {
                  artistName: 'Artist Name',
                  popularity: 50,
                  operationId: 'optional-idempotency-key'
                }
              },
              {
                action: 'multi-step',
                description: 'Multi-step transaction with artist and album',
                example: {
                  artistName: 'Artist Name',
                  albumName: 'Album Name',
                  operationId: 'optional-idempotency-key'
                }
              },
              {
                action: 'sql',
                description: 'Execute SQL with transaction safety',
                example: {
                  name: 'Example Name',
                  operationId: 'optional-idempotency-key'
                }
              }
            ]
          }),
          { headers: { ...corsHeaders, 'Content-Type': 'application/json' } }
        );
      }
    }
  } catch (error) {
    console.error('Error in transaction demo:', error);
    
    return new Response(
      JSON.stringify({ error: error.message }),
      { headers: { ...corsHeaders, 'Content-Type': 'application/json' }, status: 500 }
    );
  }
});
