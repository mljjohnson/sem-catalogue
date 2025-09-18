"use client";

import React from "react";
import "@ant-design/v5-patch-for-react-19";
import { Table, Tag, Input, Select, Button, Space, Typography, Segmented, Pagination, Flex, Divider, Tooltip, Card, Row, Col, ConfigProvider, Modal, List } from "antd";
import Link from "next/link";
import { Resizable } from "react-resizable";
import { CopyOutlined, ExportOutlined, LinkOutlined, ReloadOutlined, SearchOutlined } from "@ant-design/icons";
import type { ColumnsType } from "antd/es/table";
import axios from "axios";
// build query string without qs dependency
function buildQuery(params: Record<string, any>): string {
  const usp = new URLSearchParams();
  Object.entries(params).forEach(([k, v]) => {
    if (v === undefined || v === null || v === "") return;
    if (Array.isArray(v)) {
      v.forEach((vv) => {
        if (vv !== undefined && vv !== null && vv !== "") usp.append(k, String(vv));
      });
    } else {
      usp.append(k, String(v));
    }
  });
  return usp.toString();
}

type PageItem = {
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
  product_list?: string[];
  product_positions?: string | null;
  last_seen?: string | null;
  page_type?: "listing" | "single_product" | null;
};

type PagesResponse = {
  items: PageItem[];
  total: number;
  limit: number;
  offset: number;
};

type Filters = {
  search?: string;
  coupons?: boolean | null;
  promotions?: boolean | null;
  brands?: string[];
  products?: string[];
  primary_category?: string;
  vertical?: string;
  template_type?: string;
  status?: number | null;
  limit?: number;
  offset?: number;
  sort?: string;
};

const API_BASE = process.env.NEXT_PUBLIC_API_URL || "http://localhost:8000/api";

export default function HomePage() {
  const [filters, setFilters] = React.useState<Filters>({ limit: 50, offset: 0, status: 200, promotions: null, coupons: null });
  const [data, setData] = React.useState<PagesResponse>({ items: [], total: 0, limit: 50, offset: 0 });
  const [loading, setLoading] = React.useState(false);
  const [facets, setFacets] = React.useState<{ brands: string[]; primary_categories: string[]; verticals: string[] }>({ brands: [], primary_categories: [], verticals: [] });
  const [listModal, setListModal] = React.useState<{ open: boolean; title: string; items: string[]; note?: string }>( { open: false, title: "", items: [] } );

  const fetchPages = React.useCallback(async () => {
    setLoading(true);
    const query = buildQuery(
      {
        search: filters.search || undefined,
        coupons: filters.coupons ?? undefined,
        promotions: filters.promotions ?? undefined,
        brands: filters.brands && filters.brands.length ? filters.brands : undefined,
        products: filters.products && filters.products.length ? filters.products : undefined,
        primary_category: filters.primary_category || undefined,
        vertical: filters.vertical || undefined,
        template_type: filters.template_type || undefined,
        status: typeof filters.status === "number" ? filters.status : undefined,
        limit: filters.limit ?? 50,
        offset: filters.offset ?? 0,
        sort: filters.sort || "last_seen:desc",
      }
    );
    const res = await axios.get<PagesResponse>(`${API_BASE}/pages?${query}`);
    setData(res.data);
    setLoading(false);
  }, [filters]);

  React.useEffect(() => {
    fetchPages();
  }, [fetchPages]);

  // Load global facets so filters search across all rows, not just the current page
  React.useEffect(() => {
    (async () => {
      try {
        const res = await axios.get(`${API_BASE}/facets`);
        setFacets({
          brands: res.data?.brands || [],
          primary_categories: res.data?.primary_categories || [],
          verticals: res.data?.verticals || [],
        });
      } catch {
        // ignore facet load errors
      }
    })();
  }, []);

  const items = data.items || [];
  const total = data.total || 0;
  const pagePromotions = items.filter((i) => i.has_promotions).length;
  const brandOptions = (facets.brands || []).map((b) => ({ label: b, value: b }));
  const productOptions = Array.from(new Set(items.flatMap((i) => (i as any).product_list || []))).map((p) => ({ label: p, value: p }));
  const categoryOptions = (facets.primary_categories || []).map((c) => ({ label: c as string, value: c as string }));
  const verticalOptions = (facets.verticals || []).map((v) => ({ label: v as string, value: v as string }));

  const baseColumns: ColumnsType<PageItem> = [
    {
      title: "URL",
      dataIndex: "url",
      key: "url",
      sorter: true,
      width: 260,
      render: (v: string, r: PageItem) => {
        let display = v;
        try {
          const u = new URL(v);
          display = u.pathname || "/";
        } catch {}
        return (
          <Typography.Paragraph ellipsis={{ rows: 1 }} style={{ marginBottom: 0 }} title={v}>
            <a href={v} target="_blank" rel="noreferrer">{display}</a>
          </Typography.Paragraph>
        );
      },
    },
    { title: "Category", dataIndex: "primary_category", key: "primary_category", sorter: true, width: 140, align: "center" },
    { title: "Vertical", dataIndex: "vertical", key: "vertical", sorter: true, width: 120, align: "center" },
    {
      title: "Promotions",
      dataIndex: "has_promotions",
      key: "has_promotions",
      width: 100,
      align: "center",
      render: (_: any, r: PageItem) => (r.has_promotions ? (
        <Tag style={{ borderColor: "#22c55e", color: "#22c55e", background: "transparent" }}>Yes</Tag>
      ) : (
        <Tag style={{ borderColor: "#9ca3af", color: "#6b7280", background: "transparent" }}>No</Tag>
      )),
    },
    {
      title: "Brands",
      dataIndex: "brand_list",
      key: "brand_list",
      width: 120,
      align: "center",
      render: (_: any, r: PageItem) => {
        const count = r.page_type === "single_product" ? 1 : (r.brand_list?.length || 0);
        const items = r.brand_list || [];
        return (
          <Button size="small" type="primary" ghost shape="round" onClick={() => setListModal({ open: true, title: "Brands", items, note: r.page_type === "single_product" ? "Single product page" : undefined })}>{count}</Button>
        );
      },
    },
    {
      title: "Products",
      dataIndex: "product_list",
      key: "product_list",
      width: 120,
      align: "center",
      render: (_: any, r: PageItem) => {
        const count = r.page_type === "single_product" ? 1 : ((r.product_list || []).length);
        const items = r.product_list || [];
        return (
          <Button size="small" type="primary" ghost shape="round" onClick={() => setListModal({ open: true, title: "Products", items, note: r.page_type === "single_product" ? "Single product page" : undefined })}>{count}</Button>
        );
      },
    },
    { title: "Status", dataIndex: "status_code", key: "status_code", sorter: true, width: 90, align: "center" },
    { title: "Last seen", dataIndex: "last_seen", key: "last_seen", sorter: true, width: 110, align: "center" },
    {
      title: "",
      key: "view",
      width: 90,
      align: "center",
      render: (_: any, r: PageItem) => (
        <Link href={`/page/${r.page_id}`} target="_blank">
          <Button size="small" type="primary" ghost shape="round">View</Button>
        </Link>
      ),
    },
  ];

  // Resizable columns wiring
  const ResizableTitle = (props: any) => {
    const { onResize, width, children, ...restProps } = props;
    if (!width) {
      return <th {...restProps}>{children}</th>;
    }
    return (
      <Resizable
        width={width}
        height={0}
        handle={<span style={{ position: "absolute", right: 0, top: 0, bottom: 0, width: 8, cursor: "col-resize" }} />}
        onResize={onResize}
        draggableOpts={{ enableUserSelectHack: false }}
      >
        <th {...restProps}>{children}</th>
      </Resizable>
    );
  };

  const [columns, setColumns] = React.useState<any[]>(baseColumns);
  const handleResize = (index: number) => (_e: any, { size }: any) => {
    setColumns((prev) => {
      const next = [...prev];
      next[index] = { ...next[index], width: size.width };
      return next;
    });
  };
  const resizableColumns = columns.map((col, index) => ({
    ...col,
    onHeaderCell: (column: any) => ({
      width: column.width,
      onResize: handleResize(index),
      style: { position: "relative" },
    }),
  }));

  const onCopyUrls = async () => {
    const urls = items.map((x) => x.canonical_url || x.url).filter(Boolean).join("\n");
    await navigator.clipboard.writeText(urls);
  };

  const exportUrl = (() => {
    const query = buildQuery(
      {
        search: filters.search || undefined,
        coupons: filters.coupons ?? undefined,
        promotions: filters.promotions ?? undefined,
        brands: filters.brands && filters.brands.length ? filters.brands : undefined,
        products: filters.products && filters.products.length ? filters.products : undefined,
        primary_category: filters.primary_category || undefined,
        vertical: filters.vertical || undefined,
        template_type: filters.template_type || undefined,
        status: typeof filters.status === "number" ? filters.status : undefined,
        sort: filters.sort || "last_seen:desc",
      }
    );
    return `${API_BASE}/pages/export.csv?${query}`;
  })();

  return (
    <ConfigProvider
      theme={{
        token: {
          colorBgLayout: "#ffffff",
          colorPrimary: "#FF512F",
          colorBorderSecondary: "#eef0f4",
          borderRadiusLG: 12,
          padding: 12,
        },
      }}
    >
      <div style={{ padding: "60px 24px 24px", maxWidth: 1280, margin: "0 auto" }}>
        <div className="hero">
          <div className="icon-container">
            <img src="/landing-icon.svg" alt="Landing icon" />
          </div>
          <Typography.Title level={2} style={{ margin: 0 }}>SEM Landing Page Catalogue</Typography.Title>
          <Typography.Text type="secondary">Explore, filter and export SEM landing pages with promotions</Typography.Text>
        </div>

        {/* Loading bar */}
        {loading && <div className="loading-bar" />}

        {/* Filters */}
        <Card title="Advanced Filters" size="small" className="elevate fade-in" style={{ marginBottom: 12 }}>
          {/* Row 1: URL + Status + Coupons + Promotions */}
          <Row gutter={[12, 12]} align="middle" style={{ marginBottom: 6 }}>
            <Col flex="360px">
              <Input.Search
                allowClear
                placeholder="Filter by URL…"
                onSearch={(v) => setFilters((f) => ({ ...f, search: v || undefined, offset: 0 }))}
                enterButton={<Button className="gradient-btn" icon={<SearchOutlined />} />}
              />
            </Col>
            <Col>
              <Segmented className="segmented-gradient" options={[{ label: "200 only", value: 200 }, { label: "All", value: null }]} value={filters.status as any} onChange={(v) => setFilters((f) => ({ ...f, status: v as any, offset: 0 }))} />
            </Col>
            <Col>
              <Space align="center">
                <Typography.Text type="secondary">Coupons:</Typography.Text>
                <Segmented
                  className="segmented-gradient"
                  options={[{ label: "All", value: "all" }, { label: "Yes", value: "yes" }, { label: "No", value: "no" }]}
                  value={filters.coupons === undefined || filters.coupons === null ? "all" : filters.coupons ? "yes" : "no"}
                  onChange={(v) => {
                    const map: any = { all: undefined, yes: true, no: false };
                    setFilters((f) => ({ ...f, coupons: map[v as string] ?? undefined, offset: 0 }));
                  }}
                />
              </Space>
            </Col>
            <Col>
              <Space align="center">
                <Typography.Text type="secondary">Promotions:</Typography.Text>
                <Segmented
                  className="segmented-gradient"
                  options={[{ label: "All", value: "all" }, { label: "Yes", value: "yes" }, { label: "No", value: "no" }]}
                  value={filters.promotions === undefined || filters.promotions === null ? "all" : filters.promotions ? "yes" : "no"}
                  onChange={(v) => {
                    const map: any = { all: undefined, yes: true, no: false };
                    setFilters((f) => ({ ...f, promotions: map[v as string] ?? undefined, offset: 0 }));
                  }}
                />
              </Space>
            </Col>
          </Row>

          {/* Row 2: Brands + Categories + Verticals + Actions */}
          <Row gutter={[12, 12]} align="middle">
            <Col flex="260px">
              <Select
                className={filters.brands && filters.brands.length ? "gradient-border" : undefined}
                mode="multiple"
                showSearch
                allowClear
                placeholder="Search brands…"
                optionFilterProp="label"
                value={filters.brands as any}
                onChange={(v) => setFilters((f) => ({ ...f, brands: v as string[], offset: 0 }))}
                options={brandOptions}
                style={{ width: "100%" }}
              />
            </Col>
            <Col flex="260px">
              <Select
                className={filters.primary_category ? "gradient-border" : undefined}
                showSearch
                allowClear
                placeholder="Search categories…"
                optionFilterProp="label"
                value={filters.primary_category as any}
                onChange={(v) => setFilters((f) => ({ ...f, primary_category: (v as string | undefined) || undefined, offset: 0 }))}
                options={categoryOptions}
                style={{ width: "100%" }}
              />
            </Col>
            <Col flex="260px">
              <Select
                className={filters.vertical ? "gradient-border" : undefined}
                showSearch
                allowClear
                placeholder="Search verticals…"
                optionFilterProp="label"
                value={filters.vertical as any}
                onChange={(v) => setFilters((f) => ({ ...f, vertical: (v as string | undefined) || undefined, offset: 0 }))}
                options={verticalOptions}
                style={{ width: "100%" }}
              />
            </Col>
            <Col flex="auto" style={{ textAlign: "right" }}>
              <Space>
                <Button icon={<ReloadOutlined />} onClick={fetchPages} loading={loading}>Refresh</Button>
                <Button onClick={onCopyUrls} icon={<CopyOutlined />}>Copy All URLs</Button>
                <a href={exportUrl} target="_blank" rel="noreferrer">
                  <Button type="primary" className="gradient-btn" icon={<ExportOutlined />}>Export as CSV</Button>
                </a>
              </Space>
            </Col>
          </Row>
        <div style={{ marginTop: 8, color: "#888" }}>
          Showing {(filters.offset || 0) + 1}-{Math.min((filters.offset || 0) + (filters.limit || 50), total)} of {total} pages · {pagePromotions} with promotions
        </div>
        </Card>

        <Card size="small" className="elevate fade-in">
          <Table
            rowKey={(r) => r.page_id}
            columns={resizableColumns as any}
            dataSource={items}
            loading={loading}
            pagination={false}
            size="small"
            bordered
            sticky
            scroll={{ x: 1100 }}
            components={{
              header: {
                cell: ResizableTitle as any,
              },
            }}
            onChange={(pagination, _filters, sorter: any) => {
              if (Array.isArray(sorter)) return;
              const field = sorter?.field as string | undefined;
              const order = sorter?.order as "ascend" | "descend" | undefined;
              if (field && order) {
                setFilters((f) => ({ ...f, sort: `${field}:${order === "ascend" ? "asc" : "desc"}` }));
              }
            }}
          />

          <Divider style={{ margin: "12px 0" }} />
          <Pagination
            current={(filters.offset || 0) / (filters.limit || 50) + 1}
            pageSize={filters.limit}
            total={total}
            showSizeChanger
            pageSizeOptions={[25, 50, 100, 200] as any}
            onChange={(page, pageSize) => setFilters((f) => ({ ...f, offset: (page - 1) * pageSize, limit: pageSize }))}
          />
        </Card>
        <Modal
          open={listModal.open}
          title={listModal.title}
          onCancel={() => setListModal({ open: false, title: "", items: [] })}
          footer={<Button onClick={() => setListModal({ open: false, title: "", items: [] })}>Close</Button>}
        >
          {listModal.note && <Typography.Paragraph type="secondary" style={{ marginTop: 0 }}>{listModal.note}</Typography.Paragraph>}
          <List
            size="small"
            dataSource={listModal.items}
            renderItem={(it) => <List.Item style={{ padding: "6px 0" }}>{it}</List.Item>}
            locale={{ emptyText: "No items" }}
          />
        </Modal>
    </div>
    </ConfigProvider>
  );
}
