export type Json =
  | string
  | number
  | boolean
  | null
  | { [key: string]: Json | undefined }
  | Json[]

export type Database = {
  public: {
    Tables: {
      albums: {
        Row: {
          artist_id: string
          cover_url: string | null
          created_at: string | null
          id: string
          metadata: Json | null
          name: string
          release_date: string | null
          spotify_id: string | null
          updated_at: string | null
        }
        Insert: {
          artist_id: string
          cover_url?: string | null
          created_at?: string | null
          id?: string
          metadata?: Json | null
          name: string
          release_date?: string | null
          spotify_id?: string | null
          updated_at?: string | null
        }
        Update: {
          artist_id?: string
          cover_url?: string | null
          created_at?: string | null
          id?: string
          metadata?: Json | null
          name?: string
          release_date?: string | null
          spotify_id?: string | null
          updated_at?: string | null
        }
        Relationships: [
          {
            foreignKeyName: "albums_artist_id_fkey"
            columns: ["artist_id"]
            isOneToOne: false
            referencedRelation: "artists"
            referencedColumns: ["id"]
          },
        ]
      }
      artists: {
        Row: {
          created_at: string | null
          followers: number | null
          id: string
          image_url: string | null
          metadata: Json | null
          name: string
          popularity: number | null
          spotify_id: string | null
          updated_at: string | null
        }
        Insert: {
          created_at?: string | null
          followers?: number | null
          id?: string
          image_url?: string | null
          metadata?: Json | null
          name: string
          popularity?: number | null
          spotify_id?: string | null
          updated_at?: string | null
        }
        Update: {
          created_at?: string | null
          followers?: number | null
          id?: string
          image_url?: string | null
          metadata?: Json | null
          name?: string
          popularity?: number | null
          spotify_id?: string | null
          updated_at?: string | null
        }
        Relationships: []
      }
      normalized_tracks: {
        Row: {
          artist_id: string
          created_at: string | null
          id: string
          normalized_name: string
          representative_track_id: string | null
          updated_at: string | null
        }
        Insert: {
          artist_id: string
          created_at?: string | null
          id?: string
          normalized_name: string
          representative_track_id?: string | null
          updated_at?: string | null
        }
        Update: {
          artist_id?: string
          created_at?: string | null
          id?: string
          normalized_name?: string
          representative_track_id?: string | null
          updated_at?: string | null
        }
        Relationships: [
          {
            foreignKeyName: "normalized_tracks_artist_id_fkey"
            columns: ["artist_id"]
            isOneToOne: false
            referencedRelation: "artists"
            referencedColumns: ["id"]
          },
          {
            foreignKeyName: "normalized_tracks_representative_track_id_fkey"
            columns: ["representative_track_id"]
            isOneToOne: false
            referencedRelation: "tracks"
            referencedColumns: ["id"]
          },
        ]
      }
      processing_locks: {
        Row: {
          acquired_at: string
          correlation_id: string | null
          entity_id: string
          entity_type: string
          id: string
          last_heartbeat: string
          metadata: Json | null
          ttl_seconds: number
          worker_id: string
        }
        Insert: {
          acquired_at?: string
          correlation_id?: string | null
          entity_id: string
          entity_type: string
          id?: string
          last_heartbeat?: string
          metadata?: Json | null
          ttl_seconds?: number
          worker_id: string
        }
        Update: {
          acquired_at?: string
          correlation_id?: string | null
          entity_id?: string
          entity_type?: string
          id?: string
          last_heartbeat?: string
          metadata?: Json | null
          ttl_seconds?: number
          worker_id?: string
        }
        Relationships: []
      }
      processing_status: {
        Row: {
          attempts: number | null
          created_at: string | null
          dead_lettered: boolean | null
          entity_id: string
          entity_type: string
          id: string
          last_error: string | null
          last_processed_at: string | null
          metadata: Json | null
          state: string
          updated_at: string | null
        }
        Insert: {
          attempts?: number | null
          created_at?: string | null
          dead_lettered?: boolean | null
          entity_id: string
          entity_type: string
          id?: string
          last_error?: string | null
          last_processed_at?: string | null
          metadata?: Json | null
          state: string
          updated_at?: string | null
        }
        Update: {
          attempts?: number | null
          created_at?: string | null
          dead_lettered?: boolean | null
          entity_id?: string
          entity_type?: string
          id?: string
          last_error?: string | null
          last_processed_at?: string | null
          metadata?: Json | null
          state?: string
          updated_at?: string | null
        }
        Relationships: []
      }
      producers: {
        Row: {
          created_at: string | null
          email: string | null
          enriched_at: string | null
          enrichment_failed: boolean | null
          id: string
          image_url: string | null
          instagram_bio: string | null
          instagram_handle: string | null
          metadata: Json | null
          name: string
          normalized_name: string
          updated_at: string | null
        }
        Insert: {
          created_at?: string | null
          email?: string | null
          enriched_at?: string | null
          enrichment_failed?: boolean | null
          id?: string
          image_url?: string | null
          instagram_bio?: string | null
          instagram_handle?: string | null
          metadata?: Json | null
          name: string
          normalized_name: string
          updated_at?: string | null
        }
        Update: {
          created_at?: string | null
          email?: string | null
          enriched_at?: string | null
          enrichment_failed?: boolean | null
          id?: string
          image_url?: string | null
          instagram_bio?: string | null
          instagram_handle?: string | null
          metadata?: Json | null
          name?: string
          normalized_name?: string
          updated_at?: string | null
        }
        Relationships: []
      }
      queue_metrics: {
        Row: {
          created_at: string | null
          details: Json | null
          error_count: number | null
          finished_at: string | null
          id: string
          operation: string
          processed_count: number | null
          queue_name: string
          started_at: string
          success_count: number | null
        }
        Insert: {
          created_at?: string | null
          details?: Json | null
          error_count?: number | null
          finished_at?: string | null
          id?: string
          operation: string
          processed_count?: number | null
          queue_name: string
          started_at: string
          success_count?: number | null
        }
        Update: {
          created_at?: string | null
          details?: Json | null
          error_count?: number | null
          finished_at?: string | null
          id?: string
          operation?: string
          processed_count?: number | null
          queue_name?: string
          started_at?: string
          success_count?: number | null
        }
        Relationships: []
      }
      settings: {
        Row: {
          created_at: string | null
          key: string
          updated_at: string | null
          value: string
        }
        Insert: {
          created_at?: string | null
          key: string
          updated_at?: string | null
          value: string
        }
        Update: {
          created_at?: string | null
          key?: string
          updated_at?: string | null
          value?: string
        }
        Relationships: []
      }
      track_producers: {
        Row: {
          confidence: number
          created_at: string | null
          id: string
          producer_id: string
          source: string
          track_id: string
        }
        Insert: {
          confidence: number
          created_at?: string | null
          id?: string
          producer_id: string
          source: string
          track_id: string
        }
        Update: {
          confidence?: number
          created_at?: string | null
          id?: string
          producer_id?: string
          source?: string
          track_id?: string
        }
        Relationships: [
          {
            foreignKeyName: "track_producers_producer_id_fkey"
            columns: ["producer_id"]
            isOneToOne: false
            referencedRelation: "producer_popularity"
            referencedColumns: ["id"]
          },
          {
            foreignKeyName: "track_producers_producer_id_fkey"
            columns: ["producer_id"]
            isOneToOne: false
            referencedRelation: "producers"
            referencedColumns: ["id"]
          },
          {
            foreignKeyName: "track_producers_track_id_fkey"
            columns: ["track_id"]
            isOneToOne: false
            referencedRelation: "tracks"
            referencedColumns: ["id"]
          },
        ]
      }
      tracks: {
        Row: {
          album_id: string
          created_at: string | null
          duration_ms: number | null
          id: string
          metadata: Json | null
          name: string
          popularity: number | null
          spotify_id: string | null
          spotify_preview_url: string | null
          updated_at: string | null
        }
        Insert: {
          album_id: string
          created_at?: string | null
          duration_ms?: number | null
          id?: string
          metadata?: Json | null
          name: string
          popularity?: number | null
          spotify_id?: string | null
          spotify_preview_url?: string | null
          updated_at?: string | null
        }
        Update: {
          album_id?: string
          created_at?: string | null
          duration_ms?: number | null
          id?: string
          metadata?: Json | null
          name?: string
          popularity?: number | null
          spotify_id?: string | null
          spotify_preview_url?: string | null
          updated_at?: string | null
        }
        Relationships: [
          {
            foreignKeyName: "tracks_album_id_fkey"
            columns: ["album_id"]
            isOneToOne: false
            referencedRelation: "albums"
            referencedColumns: ["id"]
          },
        ]
      }
      worker_issues: {
        Row: {
          created_at: string | null
          details: Json | null
          id: string
          issue_type: string
          message: string
          resolved: boolean | null
          updated_at: string | null
          worker_name: string
        }
        Insert: {
          created_at?: string | null
          details?: Json | null
          id?: string
          issue_type: string
          message: string
          resolved?: boolean | null
          updated_at?: string | null
          worker_name: string
        }
        Update: {
          created_at?: string | null
          details?: Json | null
          id?: string
          issue_type?: string
          message?: string
          resolved?: boolean | null
          updated_at?: string | null
          worker_name?: string
        }
        Relationships: []
      }
    }
    Views: {
      producer_popularity: {
        Row: {
          artist_count: number | null
          avg_track_popularity: number | null
          id: string | null
          name: string | null
          track_count: number | null
        }
        Relationships: []
      }
    }
    Functions: {
      acquire_processing_lock: {
        Args:
          | {
              p_entity_type: string
              p_entity_id: string
              p_timeout_minutes?: number
            }
          | {
              p_entity_type: string
              p_entity_id: string
              p_timeout_minutes?: number
              p_correlation_id?: string
            }
        Returns: boolean
      }
      check_worker_crons: {
        Args: Record<PropertyKey, never>
        Returns: {
          jobid: number
          schedule: string
          command: string
          nodename: string
          nodeport: number
          database: string
          username: string
          active: boolean
          next_run: string
        }[]
      }
      claim_stale_lock: {
        Args: {
          p_entity_type: string
          p_entity_id: string
          p_new_worker_id: string
          p_correlation_id?: string
          p_stale_threshold_seconds?: number
        }
        Returns: Json
      }
      cleanup_stale_locks: {
        Args: { p_stale_threshold_seconds?: number }
        Returns: {
          entity_type: string
          entity_id: string
          worker_id: string
          stale_seconds: number
        }[]
      }
      cleanup_stuck_processing_states: {
        Args: Record<PropertyKey, never>
        Returns: number
      }
      confirm_message_deletion: {
        Args: { queue_name: string; message_id: string }
        Returns: boolean
      }
      emergency_reset_message: {
        Args: {
          p_queue_name: string
          p_message_id: string
          p_is_numeric?: boolean
        }
        Returns: boolean
      }
      enhanced_delete_message: {
        Args: {
          p_queue_name: string
          p_message_id: string
          p_is_numeric?: boolean
        }
        Returns: boolean
      }
      ensure_message_deleted: {
        Args: { queue_name: string; message_id: string; max_attempts?: number }
        Returns: boolean
      }
      execute_in_transaction: {
        Args: {
          p_sql: string
          p_params?: Json
          p_operation_id?: string
          p_entity_type?: string
          p_entity_id?: string
        }
        Returns: Json
      }
      find_inconsistent_states: {
        Args: { p_entity_type?: string; p_older_than_minutes?: number }
        Returns: {
          entity_type: string
          entity_id: string
          db_state: string
          redis_state: string
          last_processed_at: string
          minutes_since_update: number
        }[]
      }
      force_release_entity_lock: {
        Args: { p_entity_type: string; p_entity_id: string }
        Returns: Json
      }
      get_all_queue_tables: {
        Args: Record<PropertyKey, never>
        Returns: {
          schema_name: string
          table_name: string
          queue_name: string
          record_count: number
        }[]
      }
      get_producer_collaborations: {
        Args: { producer_id: string }
        Returns: {
          artist_id: string
          artist_name: string
          track_count: number
        }[]
      }
      get_queue_table_name_safe: {
        Args: { p_queue_name: string }
        Returns: string
      }
      get_setting: {
        Args: { p_key: string }
        Returns: string
      }
      maintenance_clear_stale_entities: {
        Args: Record<PropertyKey, never>
        Returns: {
          entity_type: string
          entity_id: string
          state: string
          action: string
        }[]
      }
      manual_trigger_worker: {
        Args: { worker_name: string }
        Returns: Json
      }
      pg_advisory_lock_exists: {
        Args: { p_key: string }
        Returns: boolean
      }
      pg_advisory_lock_timeout: {
        Args: { p_key: string; p_timeout_ms: number }
        Returns: boolean
      }
      pg_advisory_unlock: {
        Args: { p_key: string }
        Returns: boolean
      }
      pg_delete_message: {
        Args: { queue_name: string; message_id: string }
        Returns: boolean
      }
      pg_dequeue: {
        Args: {
          queue_name: string
          batch_size?: number
          visibility_timeout?: number
        }
        Returns: Json
      }
      pg_enqueue: {
        Args: { queue_name: string; message_body: Json }
        Returns: number
      }
      pg_force_advisory_unlock_all: {
        Args: { p_key: string }
        Returns: {
          pid: number
          released: boolean
        }[]
      }
      pg_queue_status: {
        Args: { queue_name: string }
        Returns: Json
      }
      pg_release_message: {
        Args: { queue_name: string; message_id: string }
        Returns: boolean
      }
      pg_send_text: {
        Args: { queue_name: string; msg_text: string }
        Returns: number[]
      }
      pg_try_advisory_lock: {
        Args: { p_key: string }
        Returns: boolean
      }
      process_album_atomic: {
        Args: {
          p_album_data: Json
          p_operation_id: string
          p_spotify_id?: string
        }
        Returns: Json
      }
      process_artist_atomic: {
        Args: {
          p_artist_data: Json
          p_operation_id: string
          p_spotify_id?: string
        }
        Returns: Json
      }
      process_producer_atomic: {
        Args: {
          p_producer_data: Json
          p_operation_id: string
          p_normalized_name?: string
        }
        Returns: Json
      }
      process_track_atomic: {
        Args: {
          p_track_data: Json
          p_operation_id: string
          p_spotify_id?: string
        }
        Returns: Json
      }
      raw_sql_query: {
        Args: { sql_query: string; params?: Json }
        Returns: Json
      }
      record_problematic_message: {
        Args: {
          p_queue_name: string
          p_message_id: string
          p_message_body: Json
          p_error_type: string
          p_error_details?: string
        }
        Returns: undefined
      }
      release_lock: {
        Args: {
          p_entity_type: string
          p_entity_id: string
          p_worker_id?: string
        }
        Returns: boolean
      }
      release_processing_lock: {
        Args: { p_entity_type: string; p_entity_id: string }
        Returns: boolean
      }
      reset_entity_processing_state: {
        Args: {
          p_entity_type?: string
          p_older_than_minutes?: number
          p_target_states?: string[]
        }
        Returns: {
          attempts: number | null
          created_at: string | null
          dead_lettered: boolean | null
          entity_id: string
          entity_type: string
          id: string
          last_error: string | null
          last_processed_at: string | null
          metadata: Json | null
          state: string
          updated_at: string | null
        }[]
      }
      reset_stuck_message: {
        Args: { queue_name: string; message_id: string }
        Returns: boolean
      }
      search_producers: {
        Args: { search_term: string }
        Returns: {
          id: string
          name: string
          normalized_name: string
          instagram_handle: string
          track_count: number
          artist_count: number
        }[]
      }
      start_artist_discovery: {
        Args: { artist_name: string }
        Returns: number
      }
      start_bulk_artist_discovery: {
        Args: { genre?: string; min_popularity?: number; limit_count?: number }
        Returns: string[]
      }
      update_lock_heartbeat: {
        Args: {
          p_entity_type: string
          p_entity_id: string
          p_worker_id: string
          p_correlation_id?: string
        }
        Returns: boolean
      }
      validate_producer_identification_message: {
        Args: { message: Json }
        Returns: boolean
      }
    }
    Enums: {
      [_ in never]: never
    }
    CompositeTypes: {
      [_ in never]: never
    }
  }
}

type DefaultSchema = Database[Extract<keyof Database, "public">]

export type Tables<
  DefaultSchemaTableNameOrOptions extends
    | keyof (DefaultSchema["Tables"] & DefaultSchema["Views"])
    | { schema: keyof Database },
  TableName extends DefaultSchemaTableNameOrOptions extends {
    schema: keyof Database
  }
    ? keyof (Database[DefaultSchemaTableNameOrOptions["schema"]]["Tables"] &
        Database[DefaultSchemaTableNameOrOptions["schema"]]["Views"])
    : never = never,
> = DefaultSchemaTableNameOrOptions extends { schema: keyof Database }
  ? (Database[DefaultSchemaTableNameOrOptions["schema"]]["Tables"] &
      Database[DefaultSchemaTableNameOrOptions["schema"]]["Views"])[TableName] extends {
      Row: infer R
    }
    ? R
    : never
  : DefaultSchemaTableNameOrOptions extends keyof (DefaultSchema["Tables"] &
        DefaultSchema["Views"])
    ? (DefaultSchema["Tables"] &
        DefaultSchema["Views"])[DefaultSchemaTableNameOrOptions] extends {
        Row: infer R
      }
      ? R
      : never
    : never

export type TablesInsert<
  DefaultSchemaTableNameOrOptions extends
    | keyof DefaultSchema["Tables"]
    | { schema: keyof Database },
  TableName extends DefaultSchemaTableNameOrOptions extends {
    schema: keyof Database
  }
    ? keyof Database[DefaultSchemaTableNameOrOptions["schema"]]["Tables"]
    : never = never,
> = DefaultSchemaTableNameOrOptions extends { schema: keyof Database }
  ? Database[DefaultSchemaTableNameOrOptions["schema"]]["Tables"][TableName] extends {
      Insert: infer I
    }
    ? I
    : never
  : DefaultSchemaTableNameOrOptions extends keyof DefaultSchema["Tables"]
    ? DefaultSchema["Tables"][DefaultSchemaTableNameOrOptions] extends {
        Insert: infer I
      }
      ? I
      : never
    : never

export type TablesUpdate<
  DefaultSchemaTableNameOrOptions extends
    | keyof DefaultSchema["Tables"]
    | { schema: keyof Database },
  TableName extends DefaultSchemaTableNameOrOptions extends {
    schema: keyof Database
  }
    ? keyof Database[DefaultSchemaTableNameOrOptions["schema"]]["Tables"]
    : never = never,
> = DefaultSchemaTableNameOrOptions extends { schema: keyof Database }
  ? Database[DefaultSchemaTableNameOrOptions["schema"]]["Tables"][TableName] extends {
      Update: infer U
    }
    ? U
    : never
  : DefaultSchemaTableNameOrOptions extends keyof DefaultSchema["Tables"]
    ? DefaultSchema["Tables"][DefaultSchemaTableNameOrOptions] extends {
        Update: infer U
      }
      ? U
      : never
    : never

export type Enums<
  DefaultSchemaEnumNameOrOptions extends
    | keyof DefaultSchema["Enums"]
    | { schema: keyof Database },
  EnumName extends DefaultSchemaEnumNameOrOptions extends {
    schema: keyof Database
  }
    ? keyof Database[DefaultSchemaEnumNameOrOptions["schema"]]["Enums"]
    : never = never,
> = DefaultSchemaEnumNameOrOptions extends { schema: keyof Database }
  ? Database[DefaultSchemaEnumNameOrOptions["schema"]]["Enums"][EnumName]
  : DefaultSchemaEnumNameOrOptions extends keyof DefaultSchema["Enums"]
    ? DefaultSchema["Enums"][DefaultSchemaEnumNameOrOptions]
    : never

export type CompositeTypes<
  PublicCompositeTypeNameOrOptions extends
    | keyof DefaultSchema["CompositeTypes"]
    | { schema: keyof Database },
  CompositeTypeName extends PublicCompositeTypeNameOrOptions extends {
    schema: keyof Database
  }
    ? keyof Database[PublicCompositeTypeNameOrOptions["schema"]]["CompositeTypes"]
    : never = never,
> = PublicCompositeTypeNameOrOptions extends { schema: keyof Database }
  ? Database[PublicCompositeTypeNameOrOptions["schema"]]["CompositeTypes"][CompositeTypeName]
  : PublicCompositeTypeNameOrOptions extends keyof DefaultSchema["CompositeTypes"]
    ? DefaultSchema["CompositeTypes"][PublicCompositeTypeNameOrOptions]
    : never

export const Constants = {
  public: {
    Enums: {},
  },
} as const
