import axios from "axios";
import qs from "qs";
import type { Filters, PagesResponse } from "../types";

const API_BASE = process.env.NEXT_PUBLIC_API_URL || "http://localhost:8000/api";

export async function fetchPages(filters: Filters): Promise<PagesResponse> {
  const query = qs.stringify(
    {
      search: filters.search || undefined,
      coupons: filters.coupons ?? undefined,
      promotions: filters.promotions ?? undefined,
      brands: filters.brands && filters.brands.length ? filters.brands : undefined,
      primary_category: filters.primary_category || undefined,
      vertical: filters.vertical || undefined,
      template_type: filters.template_type || undefined,
      status: filters.status,
      limit: filters.limit ?? 50,
      offset: filters.offset ?? 0,
      sort: filters.sort || "last_seen:desc",
    },
    { arrayFormat: "repeat" }
  );

  const { data } = await axios.get<PagesResponse>(`${API_BASE}/pages?${query}`);
  return data;
}

export function buildExportCsvUrl(filters: Filters): string {
  const query = qs.stringify(
    {
      search: filters.search || undefined,
      coupons: filters.coupons ?? undefined,
      promotions: filters.promotions ?? undefined,
      brands: filters.brands && filters.brands.length ? filters.brands : undefined,
      primary_category: filters.primary_category || undefined,
      vertical: filters.vertical || undefined,
      template_type: filters.template_type || undefined,
      status: typeof filters.status === "number" ? filters.status : undefined,
      sort: filters.sort || "last_seen:desc",
    },
    { arrayFormat: "repeat" }
  );
  return `${API_BASE}/pages/export.csv?${query}`;
}



