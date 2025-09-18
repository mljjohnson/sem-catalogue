export type PageItem = {
  page_id: string;
  url: string;
  canonical_url: string;
  status_code: number;
  primary_category: string | null;
  vertical: string | null;
  template_type: string | null;
  has_coupons: boolean;
  has_promotions?: boolean;
  brand_list: string[];
  brand_positions: string | null;
  first_seen?: string | null;
  last_seen?: string | null;
  ga_sessions_14d?: number | null;
  ga_key_events_14d?: number | null;
};

export type PagesResponse = {
  items: PageItem[];
  total: number;
  limit: number;
  offset: number;
};

export type Filters = {
  search?: string;
  coupons?: boolean | null;
  promotions?: boolean | null;
  brands?: string[];
  primary_category?: string;
  vertical?: string;
  template_type?: string;
  status?: number | null;
  limit?: number;
  offset?: number;
  sort?: string;
};



