/**
 * Unified API response wrapper (matches backend ApiResponse<T>).
 * Backend: com.auction.common.dto.ApiResponse
 */
export interface ApiResponse<T> {
  code: number;
  message: string;
  data: T;
  timestamp?: string;
  traceId?: string;
}

/**
 * Error response shape from gateway/filters.
 * Backend: com.auction.common.dto.ErrorResponse
 */
export interface ErrorResponse {
  code: number;
  message: string;
  timestamp: string;
  traceId?: string;
}

/**
 * Generic pagination wrapper used by auction-service.
 * Backend: com.auction.auction.controller.dto.PageResponse<T>
 */
export interface PageResponse<T> {
  items: T[];
  total: number;
  page: number;
  size: number;
}

/**
 * Spring Data Page wrapper used by order & notification services.
 */
export interface SpringPage<T> {
  content: T[];
  totalElements: number;
  totalPages: number;
  size: number;
  number: number;
  first: boolean;
  last: boolean;
  empty: boolean;
}