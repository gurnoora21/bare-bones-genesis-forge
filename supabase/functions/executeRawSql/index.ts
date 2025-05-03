
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
    const { 
      sql_query,
      params = [],
      transaction = false,
      statements = []
    } = requestBody;
    
    // Validate input - either need sql_query or statements for a transaction
    if (!sql_query && (!transaction || !statements || !statements.length)) {
      throw new Error("Either sql_query or (transaction=true with statements) are required");
    }
    
    const supabase = createClient(
      Deno.env.get("SUPABASE_URL")!,
      Deno.env.get("SUPABASE_SERVICE_ROLE_KEY")!
    );
    
    let result;
    
    // Handle transaction mode
    if (transaction && statements && statements.length > 0) {
      console.log(`Executing transaction with ${statements.length} statements`);
      
      // Construct transaction SQL
      let transactionSql = "BEGIN;\n";
      
      statements.forEach((stmt: { sql: string, params?: any[] }, index: number) => {
        // For parameterized queries, we need to replace $1, $2, etc. with actual values
        // because we can't pass separate param arrays for each statement
        if (stmt.params && stmt.params.length > 0) {
          let sql = stmt.sql;
          stmt.params.forEach((param, i) => {
            const placeholder = `$${i + 1}`;
            let replacement;
            
            if (param === null) {
              replacement = 'NULL';
            } else if (typeof param === 'string') {
              replacement = `'${param.replace(/'/g, "''")}'`;
            } else if (typeof param === 'object' && param !== null) {
              replacement = `'${JSON.stringify(param).replace(/'/g, "''")}'`;
            } else {
              replacement = param;
            }
            
            sql = sql.replace(new RegExp('\\' + placeholder, 'g'), replacement);
          });
          transactionSql += sql + ";\n";
        } else {
          transactionSql += stmt.sql + ";\n";
        }
      });
      
      transactionSql += "COMMIT;";
      
      const { data, error } = await supabase.rpc('raw_sql_query', {
        sql_query: transactionSql,
        params: []
      });
      
      if (error) throw error;
      result = { transaction: true, statements: statements.length, data };
    } else {
      // Single SQL query mode
      console.log(`Executing SQL query: ${sql_query?.substring(0, 100)}...`);
      
      const { data, error } = await supabase.rpc('raw_sql_query', {
        sql_query,
        params
      });
      
      if (error) throw error;
      result = { data };
    }
    
    return new Response(
      JSON.stringify(result),
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
