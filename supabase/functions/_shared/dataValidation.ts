
/**
 * Data validation system for music producer pipeline
 * Provides schema validation, data contracts, and quality checks
 */

import { ErrorCategory, ErrorSource, createEnhancedError } from "./errorHandling.ts";

// Supported validation types
export enum ValidationType {
  STRING = 'string',
  NUMBER = 'number',
  BOOLEAN = 'boolean',
  OBJECT = 'object',
  ARRAY = 'array',
  NULL = 'null',
  ANY = 'any',
  DATE = 'date',
  UUID = 'uuid',
  EMAIL = 'email',
  URL = 'url',
  ENUM = 'enum',
  SPOTIFY_ID = 'spotify_id',
  GENIUS_ID = 'genius_id'
}

// Field definition for schema validation
export interface FieldDefinition {
  type: ValidationType | ValidationType[];
  required?: boolean;
  nullable?: boolean;
  minLength?: number;
  maxLength?: number;
  minValue?: number;
  maxValue?: number;
  pattern?: RegExp | string;
  enum?: any[];
  properties?: SchemaDefinition;
  items?: FieldDefinition;
  description?: string;
}

// Schema definition for object validation
export type SchemaDefinition = Record<string, FieldDefinition>;

// Validation options
export interface ValidationOptions {
  mode?: 'strict' | 'loose';
  abortEarly?: boolean;
  additionalProperties?: boolean;
  strictRequired?: boolean;
}

// Validation error details
export interface ValidationErrorDetail {
  path: string;
  value: any;
  rule: string;
  message: string;
}

// Validation context
export interface ValidationContext {
  schema: string;
  rootValue: any;
  options: ValidationOptions;
  errors: ValidationErrorDetail[];
}

// Default validation options
export const DEFAULT_VALIDATION_OPTIONS: ValidationOptions = {
  mode: 'strict',
  abortEarly: false,
  additionalProperties: false,
  strictRequired: true
};

/**
 * Validate data against a schema definition
 */
export function validateSchema(
  data: any,
  schema: SchemaDefinition,
  schemaName: string,
  options: ValidationOptions = DEFAULT_VALIDATION_OPTIONS
): { valid: boolean; errors: ValidationErrorDetail[] } {
  // Create validation context
  const context: ValidationContext = {
    schema: schemaName,
    rootValue: data,
    options,
    errors: []
  };
  
  // Validate object against schema
  validateObject(data, schema, '', context);
  
  // Return validation result
  return {
    valid: context.errors.length === 0,
    errors: context.errors
  };
}

/**
 * Validate an object against a schema
 */
function validateObject(
  value: any,
  schema: SchemaDefinition,
  path: string,
  context: ValidationContext
): boolean {
  // Check if value is an object
  if (!value || typeof value !== 'object' || Array.isArray(value)) {
    context.errors.push({
      path: path || 'root',
      value,
      rule: 'type',
      message: `Expected object but got ${Array.isArray(value) ? 'array' : typeof value}`
    });
    return false;
  }
  
  let isValid = true;
  const { options } = context;
  
  // Check for required fields
  for (const [key, field] of Object.entries(schema)) {
    const fieldPath = path ? `${path}.${key}` : key;
    
    // Check if field is required but missing
    if (field.required && (!(key in value) || value[key] === undefined)) {
      context.errors.push({
        path: fieldPath,
        value: undefined,
        rule: 'required',
        message: `Required field '${fieldPath}' is missing`
      });
      isValid = false;
      
      // If aborting early, return immediately
      if (options.abortEarly) {
        return false;
      }
      
      // Skip further validation for this field
      continue;
    }
    
    // Skip validation if field is not present and not required
    if (!(key in value) || value[key] === undefined) {
      continue;
    }
    
    // Validate field
    const fieldValue = value[key];
    const fieldValid = validateField(fieldValue, field, fieldPath, context);
    
    // Update overall validity
    isValid = isValid && fieldValid;
    
    // If aborting early and validation failed, return immediately
    if (!fieldValid && options.abortEarly) {
      return false;
    }
  }
  
  // Check for additional properties if in strict mode
  if (options.mode === 'strict' && !options.additionalProperties) {
    const schemaKeys = Object.keys(schema);
    const dataKeys = Object.keys(value);
    
    for (const key of dataKeys) {
      if (!schemaKeys.includes(key)) {
        const fieldPath = path ? `${path}.${key}` : key;
        
        context.errors.push({
          path: fieldPath,
          value: value[key],
          rule: 'additionalProperty',
          message: `Additional property '${fieldPath}' not allowed`
        });
        
        isValid = false;
        
        // If aborting early, return immediately
        if (options.abortEarly) {
          return false;
        }
      }
    }
  }
  
  return isValid;
}

/**
 * Validate a field against its definition
 */
function validateField(
  value: any,
  field: FieldDefinition,
  path: string,
  context: ValidationContext
): boolean {
  // Handle null values
  if (value === null) {
    if (field.nullable === false) {
      context.errors.push({
        path,
        value,
        rule: 'nullable',
        message: `Field '${path}' cannot be null`
      });
      return false;
    }
    return true;
  }
  
  // Convert single type to array for consistent handling
  const types = Array.isArray(field.type) ? field.type : [field.type];
  
  // Check if value matches any of the allowed types
  for (const type of types) {
    const typeValid = validateType(value, type, field, path, context);
    if (typeValid) {
      return true;
    }
  }
  
  // If we get here, value doesn't match any allowed type
  context.errors.push({
    path,
    value,
    rule: 'type',
    message: `Field '${path}' expected to be ${types.join(' or ')} but got ${typeof value}`
  });
  
  return false;
}

/**
 * Validate a value against a specific type
 */
function validateType(
  value: any,
  type: ValidationType,
  field: FieldDefinition,
  path: string,
  context: ValidationContext
): boolean {
  switch (type) {
    case ValidationType.STRING:
      return validateString(value, field, path, context);
    case ValidationType.NUMBER:
      return validateNumber(value, field, path, context);
    case ValidationType.BOOLEAN:
      return validateBoolean(value, field, path, context);
    case ValidationType.OBJECT:
      if (field.properties) {
        return validateObject(value, field.properties, path, context);
      }
      return typeof value === 'object' && !Array.isArray(value);
    case ValidationType.ARRAY:
      return validateArray(value, field, path, context);
    case ValidationType.NULL:
      return value === null;
    case ValidationType.ANY:
      return true;
    case ValidationType.DATE:
      return validateDate(value, field, path, context);
    case ValidationType.UUID:
      return validateUuid(value, field, path, context);
    case ValidationType.EMAIL:
      return validateEmail(value, field, path, context);
    case ValidationType.URL:
      return validateUrl(value, field, path, context);
    case ValidationType.ENUM:
      return validateEnum(value, field, path, context);
    case ValidationType.SPOTIFY_ID:
      return validateSpotifyId(value, field, path, context);
    case ValidationType.GENIUS_ID:
      return validateGeniusId(value, field, path, context);
    default:
      return false;
  }
}

/**
 * Validate a string field
 */
function validateString(
  value: any,
  field: FieldDefinition,
  path: string,
  context: ValidationContext
): boolean {
  // Check type
  if (typeof value !== 'string') {
    context.errors.push({
      path,
      value,
      rule: 'type',
      message: `Field '${path}' expected to be string but got ${typeof value}`
    });
    return false;
  }
  
  let isValid = true;
  
  // Check min length
  if (field.minLength !== undefined && value.length < field.minLength) {
    context.errors.push({
      path,
      value,
      rule: 'minLength',
      message: `Field '${path}' length must be at least ${field.minLength} characters`
    });
    isValid = false;
  }
  
  // Check max length
  if (field.maxLength !== undefined && value.length > field.maxLength) {
    context.errors.push({
      path,
      value,
      rule: 'maxLength',
      message: `Field '${path}' length must not exceed ${field.maxLength} characters`
    });
    isValid = false;
  }
  
  // Check pattern
  if (field.pattern) {
    const pattern = typeof field.pattern === 'string' 
      ? new RegExp(field.pattern) 
      : field.pattern;
    
    if (!pattern.test(value)) {
      context.errors.push({
        path,
        value,
        rule: 'pattern',
        message: `Field '${path}' must match pattern ${pattern}`
      });
      isValid = false;
    }
  }
  
  return isValid;
}

/**
 * Validate a number field
 */
function validateNumber(
  value: any,
  field: FieldDefinition,
  path: string,
  context: ValidationContext
): boolean {
  // Check type
  if (typeof value !== 'number' || isNaN(value)) {
    context.errors.push({
      path,
      value,
      rule: 'type',
      message: `Field '${path}' expected to be number but got ${typeof value}`
    });
    return false;
  }
  
  let isValid = true;
  
  // Check min value
  if (field.minValue !== undefined && value < field.minValue) {
    context.errors.push({
      path,
      value,
      rule: 'minValue',
      message: `Field '${path}' must be at least ${field.minValue}`
    });
    isValid = false;
  }
  
  // Check max value
  if (field.maxValue !== undefined && value > field.maxValue) {
    context.errors.push({
      path,
      value,
      rule: 'maxValue',
      message: `Field '${path}' must not exceed ${field.maxValue}`
    });
    isValid = false;
  }
  
  return isValid;
}

/**
 * Validate a boolean field
 */
function validateBoolean(
  value: any,
  field: FieldDefinition,
  path: string,
  context: ValidationContext
): boolean {
  // Check type
  if (typeof value !== 'boolean') {
    context.errors.push({
      path,
      value,
      rule: 'type',
      message: `Field '${path}' expected to be boolean but got ${typeof value}`
    });
    return false;
  }
  
  return true;
}

/**
 * Validate an array field
 */
function validateArray(
  value: any,
  field: FieldDefinition,
  path: string,
  context: ValidationContext
): boolean {
  // Check type
  if (!Array.isArray(value)) {
    context.errors.push({
      path,
      value,
      rule: 'type',
      message: `Field '${path}' expected to be array but got ${typeof value}`
    });
    return false;
  }
  
  let isValid = true;
  
  // Check min length
  if (field.minLength !== undefined && value.length < field.minLength) {
    context.errors.push({
      path,
      value,
      rule: 'minLength',
      message: `Array '${path}' length must be at least ${field.minLength} items`
    });
    isValid = false;
  }
  
  // Check max length
  if (field.maxLength !== undefined && value.length > field.maxLength) {
    context.errors.push({
      path,
      value,
      rule: 'maxLength',
      message: `Array '${path}' length must not exceed ${field.maxLength} items`
    });
    isValid = false;
  }
  
  // Validate array items if items definition is provided
  if (field.items && isValid) {
    for (let i = 0; i < value.length; i++) {
      const itemPath = `${path}[${i}]`;
      const itemValue = value[i];
      
      // Validate item
      const itemValid = validateField(itemValue, field.items, itemPath, context);
      
      // Update overall validity
      isValid = isValid && itemValid;
      
      // If aborting early and validation failed, return immediately
      if (!itemValid && context.options.abortEarly) {
        return false;
      }
    }
  }
  
  return isValid;
}

/**
 * Validate a date field
 */
function validateDate(
  value: any,
  field: FieldDefinition,
  path: string,
  context: ValidationContext
): boolean {
  // Check if value is a valid date
  if (typeof value === 'string') {
    // Try to parse as date string
    const date = new Date(value);
    if (isNaN(date.getTime())) {
      context.errors.push({
        path,
        value,
        rule: 'type',
        message: `Field '${path}' expected to be a valid date string`
      });
      return false;
    }
    return true;
  } else if (value instanceof Date) {
    // Check if Date object is valid
    if (isNaN(value.getTime())) {
      context.errors.push({
        path,
        value,
        rule: 'type',
        message: `Field '${path}' expected to be a valid Date object`
      });
      return false;
    }
    return true;
  }
  
  context.errors.push({
    path,
    value,
    rule: 'type',
    message: `Field '${path}' expected to be a Date object or date string but got ${typeof value}`
  });
  return false;
}

/**
 * Validate a UUID field
 */
function validateUuid(
  value: any,
  field: FieldDefinition,
  path: string,
  context: ValidationContext
): boolean {
  // Check type
  if (typeof value !== 'string') {
    context.errors.push({
      path,
      value,
      rule: 'type',
      message: `Field '${path}' expected to be string but got ${typeof value}`
    });
    return false;
  }
  
  // Check UUID format
  const uuidPattern = /^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i;
  if (!uuidPattern.test(value)) {
    context.errors.push({
      path,
      value,
      rule: 'pattern',
      message: `Field '${path}' must be a valid UUID`
    });
    return false;
  }
  
  return true;
}

/**
 * Validate an email field
 */
function validateEmail(
  value: any,
  field: FieldDefinition,
  path: string,
  context: ValidationContext
): boolean {
  // Check type
  if (typeof value !== 'string') {
    context.errors.push({
      path,
      value,
      rule: 'type',
      message: `Field '${path}' expected to be string but got ${typeof value}`
    });
    return false;
  }
  
  // Check email format
  // This is a simplified pattern; real email validation is complex
  const emailPattern = /^[^\s@]+@[^\s@]+\.[^\s@]+$/;
  if (!emailPattern.test(value)) {
    context.errors.push({
      path,
      value,
      rule: 'pattern',
      message: `Field '${path}' must be a valid email address`
    });
    return false;
  }
  
  return true;
}

/**
 * Validate a URL field
 */
function validateUrl(
  value: any,
  field: FieldDefinition,
  path: string,
  context: ValidationContext
): boolean {
  // Check type
  if (typeof value !== 'string') {
    context.errors.push({
      path,
      value,
      rule: 'type',
      message: `Field '${path}' expected to be string but got ${typeof value}`
    });
    return false;
  }
  
  // Check URL format
  try {
    new URL(value);
    return true;
  } catch (error) {
    context.errors.push({
      path,
      value,
      rule: 'pattern',
      message: `Field '${path}' must be a valid URL`
    });
    return false;
  }
}

/**
 * Validate an enum field
 */
function validateEnum(
  value: any,
  field: FieldDefinition,
  path: string,
  context: ValidationContext
): boolean {
  // Check if enum values are defined
  if (!field.enum || !Array.isArray(field.enum) || field.enum.length === 0) {
    context.errors.push({
      path,
      value,
      rule: 'enum',
      message: `Field '${path}' has invalid enum definition`
    });
    return false;
  }
  
  // Check if value is in enum
  if (!field.enum.includes(value)) {
    context.errors.push({
      path,
      value,
      rule: 'enum',
      message: `Field '${path}' must be one of [${field.enum.join(', ')}]`
    });
    return false;
  }
  
  return true;
}

/**
 * Validate a Spotify ID field
 */
function validateSpotifyId(
  value: any,
  field: FieldDefinition,
  path: string,
  context: ValidationContext
): boolean {
  // Check type
  if (typeof value !== 'string') {
    context.errors.push({
      path,
      value,
      rule: 'type',
      message: `Field '${path}' expected to be string but got ${typeof value}`
    });
    return false;
  }
  
  // Check Spotify ID format (base-62 string, 22 chars)
  // Simplified: Spotify IDs aren't always 22 chars, could be shorter for legacy reasons
  if (!/^[0-9a-zA-Z]{10,30}$/.test(value)) {
    context.errors.push({
      path,
      value,
      rule: 'pattern',
      message: `Field '${path}' must be a valid Spotify ID`
    });
    return false;
  }
  
  return true;
}

/**
 * Validate a Genius ID field
 */
function validateGeniusId(
  value: any,
  field: FieldDefinition,
  path: string,
  context: ValidationContext
): boolean {
  // Check type
  if (typeof value !== 'number' && !(typeof value === 'string' && !isNaN(Number(value)))) {
    context.errors.push({
      path,
      value,
      rule: 'type',
      message: `Field '${path}' expected to be a number but got ${typeof value}`
    });
    return false;
  }
  
  // Ensure it's a positive integer
  const numValue = Number(value);
  if (!Number.isInteger(numValue) || numValue <= 0) {
    context.errors.push({
      path,
      value,
      rule: 'pattern',
      message: `Field '${path}' must be a positive integer`
    });
    return false;
  }
  
  return true;
}

/**
 * Create a validation error from validation results
 */
export function createValidationError(
  schemaName: string,
  results: { valid: boolean; errors: ValidationErrorDetail[] }
): Error {
  const enhancedError = createEnhancedError(
    `Validation failed for schema '${schemaName}' with ${results.errors.length} errors`,
    ErrorSource.VALIDATION,
    ErrorCategory.PERMANENT_VALIDATION,
    {
      schemaName,
      errors: results.errors
    }
  );
  
  return enhancedError;
}

// Define common schemas for pipeline messages

// Spotify Artist Schema
export const SPOTIFY_ARTIST_SCHEMA: SchemaDefinition = {
  id: { type: ValidationType.SPOTIFY_ID, required: true },
  name: { type: ValidationType.STRING, required: true },
  followers: {
    type: ValidationType.OBJECT,
    properties: {
      total: { type: ValidationType.NUMBER }
    }
  },
  genres: { type: ValidationType.ARRAY, items: { type: ValidationType.STRING } },
  images: {
    type: ValidationType.ARRAY,
    items: {
      type: ValidationType.OBJECT,
      properties: {
        url: { type: ValidationType.URL, required: true },
        height: { type: ValidationType.NUMBER },
        width: { type: ValidationType.NUMBER }
      }
    }
  },
  popularity: { type: ValidationType.NUMBER, minValue: 0, maxValue: 100 }
};

// Spotify Album Schema
export const SPOTIFY_ALBUM_SCHEMA: SchemaDefinition = {
  id: { type: ValidationType.SPOTIFY_ID, required: true },
  name: { type: ValidationType.STRING, required: true },
  artists: {
    type: ValidationType.ARRAY,
    items: {
      type: ValidationType.OBJECT,
      properties: {
        id: { type: ValidationType.SPOTIFY_ID, required: true },
        name: { type: ValidationType.STRING, required: true }
      }
    },
    required: true
  },
  release_date: { type: ValidationType.STRING, required: true },
  release_date_precision: { type: ValidationType.STRING },
  total_tracks: { type: ValidationType.NUMBER },
  images: {
    type: ValidationType.ARRAY,
    items: {
      type: ValidationType.OBJECT,
      properties: {
        url: { type: ValidationType.URL, required: true },
        height: { type: ValidationType.NUMBER },
        width: { type: ValidationType.NUMBER }
      }
    }
  }
};

// Spotify Track Schema
export const SPOTIFY_TRACK_SCHEMA: SchemaDefinition = {
  id: { type: ValidationType.SPOTIFY_ID, required: true },
  name: { type: ValidationType.STRING, required: true },
  artists: {
    type: ValidationType.ARRAY,
    items: {
      type: ValidationType.OBJECT,
      properties: {
        id: { type: ValidationType.SPOTIFY_ID, required: true },
        name: { type: ValidationType.STRING, required: true }
      }
    },
    required: true
  },
  album: {
    type: ValidationType.OBJECT,
    properties: {
      id: { type: ValidationType.SPOTIFY_ID, required: true },
      name: { type: ValidationType.STRING, required: true }
    }
  },
  duration_ms: { type: ValidationType.NUMBER, minValue: 0 },
  explicit: { type: ValidationType.BOOLEAN },
  popularity: { type: ValidationType.NUMBER, minValue: 0, maxValue: 100 },
  preview_url: { type: [ValidationType.URL, ValidationType.NULL] }
};

// Genius Song Schema
export const GENIUS_SONG_SCHEMA: SchemaDefinition = {
  id: { type: ValidationType.GENIUS_ID, required: true },
  title: { type: ValidationType.STRING, required: true },
  primary_artist: {
    type: ValidationType.OBJECT,
    properties: {
      id: { type: ValidationType.GENIUS_ID, required: true },
      name: { type: ValidationType.STRING, required: true }
    },
    required: true
  },
  producer_artists: {
    type: ValidationType.ARRAY,
    items: {
      type: ValidationType.OBJECT,
      properties: {
        id: { type: ValidationType.GENIUS_ID, required: true },
        name: { type: ValidationType.STRING, required: true }
      }
    }
  }
};

// Queue message schemas
export const ARTIST_DISCOVERY_MESSAGE_SCHEMA: SchemaDefinition = {
  artistId: { type: ValidationType.SPOTIFY_ID, required: false },
  artistName: { type: ValidationType.STRING, required: false },
  _idempotencyKey: { type: ValidationType.STRING, required: false }
};

export const ALBUM_DISCOVERY_MESSAGE_SCHEMA: SchemaDefinition = {
  albumId: { type: ValidationType.SPOTIFY_ID, required: false },
  artistId: { type: ValidationType.SPOTIFY_ID, required: false }
};

export const TRACK_DISCOVERY_MESSAGE_SCHEMA: SchemaDefinition = {
  trackId: { type: ValidationType.SPOTIFY_ID, required: false },
  albumId: { type: ValidationType.SPOTIFY_ID, required: false }
};

export const PRODUCER_IDENTIFICATION_MESSAGE_SCHEMA: SchemaDefinition = {
  trackId: { type: ValidationType.UUID, required: true },
  trackName: { type: ValidationType.STRING, required: true },
  artistName: { type: ValidationType.STRING, required: true }
};

export const SOCIAL_ENRICHMENT_MESSAGE_SCHEMA: SchemaDefinition = {
  producerId: { type: ValidationType.UUID, required: true },
  producerName: { type: ValidationType.STRING, required: true }
};

// Utility schema validation functions
export function validateArtistDiscoveryMessage(message: any): { valid: boolean; errors: ValidationErrorDetail[] } {
  return validateSchema(message, ARTIST_DISCOVERY_MESSAGE_SCHEMA, 'ArtistDiscoveryMessage');
}

export function validateAlbumDiscoveryMessage(message: any): { valid: boolean; errors: ValidationErrorDetail[] } {
  return validateSchema(message, ALBUM_DISCOVERY_MESSAGE_SCHEMA, 'AlbumDiscoveryMessage');
}

export function validateTrackDiscoveryMessage(message: any): { valid: boolean; errors: ValidationErrorDetail[] } {
  return validateSchema(message, TRACK_DISCOVERY_MESSAGE_SCHEMA, 'TrackDiscoveryMessage');
}

export function validateProducerIdentificationMessage(message: any): { valid: boolean; errors: ValidationErrorDetail[] } {
  return validateSchema(message, PRODUCER_IDENTIFICATION_MESSAGE_SCHEMA, 'ProducerIdentificationMessage');
}

export function validateSocialEnrichmentMessage(message: any): { valid: boolean; errors: ValidationErrorDetail[] } {
  return validateSchema(message, SOCIAL_ENRICHMENT_MESSAGE_SCHEMA, 'SocialEnrichmentMessage');
}

export function validateSpotifyArtist(artist: any): { valid: boolean; errors: ValidationErrorDetail[] } {
  return validateSchema(artist, SPOTIFY_ARTIST_SCHEMA, 'SpotifyArtist');
}

export function validateSpotifyAlbum(album: any): { valid: boolean; errors: ValidationErrorDetail[] } {
  return validateSchema(album, SPOTIFY_ALBUM_SCHEMA, 'SpotifyAlbum');
}

export function validateSpotifyTrack(track: any): { valid: boolean; errors: ValidationErrorDetail[] } {
  return validateSchema(track, SPOTIFY_TRACK_SCHEMA, 'SpotifyTrack');
}

export function validateGeniusSong(song: any): { valid: boolean; errors: ValidationErrorDetail[] } {
  return validateSchema(song, GENIUS_SONG_SCHEMA, 'GeniusSong');
}
