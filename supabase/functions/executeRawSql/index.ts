
import { serve } from "https://deno.land/std@0.168.0/http/server.ts";
import { createClient } from "https://esm.sh/@supabase/supabase-js@2.39.7";

const corsHeaders = {
  'Access-Control-Allow-Origin': '*',
  'Access-Control-Allow-Headers': 'authorization, x-client-info, apikey, content-type',
};

serve(async (req) => {
  // Handle CORS preflight requests
  if (req.method === 'OPTIONS') {
    return new Response(null, { headers: corsHeaders });
  }

  try {
    const requestBody = await req.json();
    const { sql_query, params = [], use_transaction = false } = requestBody;
    
    if (!sql_query) {
      throw new Error("sql_query is required");
    }
    
    const supabase = createClient(
      Deno.env.get("SUPABASE_URL")!,
      Deno.env.get("SUPABASE_SERVICE_ROLE_KEY")!
    );
    
    console.log(`Executing SQL query: ${sql_query}`);
    console.log(`With parameters: ${JSON.stringify(params)}`);
    console.log(`Using transaction: ${use_transaction}`);
    
    let result;
    
    if (use_transaction) {
      // Execute the SQL query in a transaction
      const { data, error } = await supabase.rpc('raw_sql_query', {
        sql_query: `
          BEGIN;
          ${sql_query}
          COMMIT;
        `,
        params: params
      });
      
      if (error) {
        throw error;
      }
      
      result = data;
    } else {
      // Execute the raw SQL query without transaction
      const { data, error } = await supabase.rpc('raw_sql_query', {
        sql_query: sql_query,
        params: params
      });
      
      if (error) {
        throw error;
      }
      
      result = data;
    }
    
    return new Response(
      JSON.stringify({ data: result }),
      { headers: { ...corsHeaders, 'Content-Type': 'application/json' } }
    );
    
  } catch (error) {
    console.error("Error executing SQL query:", error);
    
    return new Response(
      JSON.stringify({ error: error.message }),
      { status: 500, headers: { ...corsHeaders, 'Content-Type': 'application/json' } }
    );
  }
});
