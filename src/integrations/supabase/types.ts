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
      get_producer_collaborations: {
        Args: { producer_id: string }
        Returns: {
          artist_id: string
          artist_name: string
          track_count: number
        }[]
      }
      manual_trigger_worker: {
        Args: { worker_name: string }
        Returns: Json
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
      pg_release_message: {
        Args: { queue_name: string; message_id: string }
        Returns: boolean
      }
      pg_send_text: {
        Args: { queue_name: string; msg_text: string }
        Returns: number[]
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
        Returns: string
      }
      start_bulk_artist_discovery: {
        Args: { genre?: string; min_popularity?: number; limit_count?: number }
        Returns: string[]
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
