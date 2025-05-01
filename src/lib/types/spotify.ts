
// Spotify API Types
export interface SpotifyError {
  status: number;
  message: string;
}

export interface SpotifyPagination {
  href: string;
  limit: number;
  next: string | null;
  offset: number;
  previous: string | null;
  total: number;
}

export interface SpotifyImage {
  url: string;
  height: number | null;
  width: number | null;
}

export interface Artist {
  id: string;
  name: string;
  type: 'artist';
  uri: string;
  href: string;
  external_urls: { spotify: string };
  popularity: number;
  genres: string[];
  images: SpotifyImage[];
  followers: { href: string | null; total: number };
}

export interface ArtistPage extends SpotifyPagination {
  items: Artist[];
}

export interface SimplifiedArtist {
  id: string;
  name: string;
  type: 'artist';
  uri: string;
  href: string;
  external_urls: { spotify: string };
}

export interface Album {
  id: string;
  name: string;
  type: 'album';
  album_type: 'album' | 'single' | 'compilation';
  uri: string;
  href: string;
  external_urls: { spotify: string };
  images: SpotifyImage[];
  release_date: string;
  release_date_precision: 'year' | 'month' | 'day';
  total_tracks: number;
  artists: SimplifiedArtist[];
  available_markets: string[];
}

export interface AlbumPage extends SpotifyPagination {
  items: Album[];
}

export interface Track {
  id: string;
  name: string;
  type: 'track';
  uri: string;
  href: string;
  external_urls: { spotify: string };
  duration_ms: number;
  explicit: boolean;
  track_number: number;
  disc_number: number;
  album: Album;
  artists: SimplifiedArtist[];
  popularity: number;
  available_markets: string[];
  preview_url: string | null;
  is_local: boolean;
}

export interface SimplifiedTrack {
  id: string;
  name: string;
  type: 'track';
  uri: string;
  href: string;
  external_urls: { spotify: string };
  duration_ms: number;
  explicit: boolean;
  track_number: number;
  disc_number: number;
  artists: SimplifiedArtist[];
  preview_url: string | null;
  is_local: boolean;
  available_markets: string[];
}

export interface TrackPage extends SpotifyPagination {
  items: SimplifiedTrack[];
}
