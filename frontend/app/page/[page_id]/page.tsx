"use client";

import React from "react";
import "@ant-design/v5-patch-for-react-19";
import { useParams } from "next/navigation";
import axios from "axios";
import { Card, Typography, Space, Tag, Divider, Descriptions, Table, Skeleton } from "antd";

const API_BASE = process.env.NEXT_PUBLIC_API_URL || "http://localhost:8000/api";

export default function PageDetail() {
  const params = useParams<{ page_id: string }>();
  const pageId = params?.page_id as string;
  const [data, setData] = React.useState<any>(null);
  const [loading, setLoading] = React.useState<boolean>(true);

  React.useEffect(() => {
    async function load() {
      try {
        const res = await axios.get(`${API_BASE}/ai/extracts/${pageId}`);
        setData(res.data);
      } finally {
        setLoading(false);
      }
    }
    if (pageId) load();
  }, [pageId]);

  const jsonPretty = (obj: any) => JSON.stringify(obj ?? {}, null, 2);

  const listings = (data?.data?.listings as any[]) || [];
  const otherPromos = ((data?.data?.other_promotions as any[]) || []).map((promo, idx) => ({
    ...promo,
    _uniqueKey: `promo-${idx}-${promo.description || ''}-${promo.code || ''}`,
  }));
  const hasPromotions = !!data?.data?.has_promotions;
  const hasCoupons = !!data?.data?.has_coupons;

  return (
    <div style={{ padding: "40px 24px 24px", maxWidth: 1200, margin: "0 auto" }}>
      <div style={{ marginBottom: 24 }}>
        <Typography.Title level={2} style={{ margin: 0 }}>
          Page Details — {data?.url ? (
            <Typography.Text type="secondary" style={{ fontWeight: 400, fontSize: 18 }}>
              <a href={data.url} target="_blank" rel="noreferrer">{data.url}</a>
            </Typography.Text>
          ) : null}
        </Typography.Title>
        <div className="title-accent" />
      </div>

      

      {loading ? (
        <Card className="elevate fade-in" style={{ marginBottom: 16 }}>
          <Skeleton active paragraph={{ rows: 6 }} />
        </Card>
      ) : listings.length > 0 ? (
      <Card className="elevate fade-in" title={`Listings (${listings.length})`} style={{ marginBottom: 16 }}>
        <Table
          rowKey={(r: any) => r.selector || `${r.brand_name}-${r.product_offer_name}-${r.affiliate_link || ""}`}
          dataSource={listings}
          pagination={false}
          size="small"
          tableLayout="fixed"
          scroll={{ x: true }}
          columns={[
            { title: "Brand", dataIndex: "brand_name", key: "brand_name", width: 140 },
            { title: "Product", dataIndex: "product_name", key: "product_name", width: 180, render: (v: string, r: any) => v || r.product_offer_name },
            { title: "Description", dataIndex: "description", key: "description" },
            { title: "Code", dataIndex: "code", key: "code", width: 120, render: (v: string) => (v ? <Tag color="blue">{v}</Tag> : <span />) },
            { title: "Affiliate link", dataIndex: "affiliate_link", key: "affiliate_link", width: 260, render: (v: string) => (v ? <a href={v} target="_blank" rel="noreferrer" style={{ wordBreak: "break-all", whiteSpace: "normal" }}>{v}</a> : <span />) },
            { title: "Position", dataIndex: "position", key: "position", width: 90, align: "center" },
            { title: "Has promo", dataIndex: "has_promotion", key: "has_promotion", width: 110, align: "center", render: (v: boolean) => (v ? <Tag style={{ borderColor: "#22c55e", color: "#22c55e", background: "transparent" }}>Yes</Tag> : <Tag style={{ borderColor: "#9ca3af", color: "#6b7280", background: "transparent" }}>No</Tag>) },
          ]}
        />
      </Card>
      ) : null}

      <Card className="elevate fade-in" title={`Other promotions (${otherPromos.length})`} style={{ marginBottom: 16 }}>
        <Table
          rowKey={(r: any) => r._uniqueKey}
          dataSource={otherPromos}
          pagination={false}
          size="small"
          tableLayout="fixed"
          scroll={{ x: true }}
          columns={[
            { title: "Description", dataIndex: "description", key: "description" },
            { title: "Code", dataIndex: "code", key: "code", width: 120, render: (v: string) => (v ? <Tag color="blue">{v}</Tag> : <span />) },
            { title: "Affiliate link", dataIndex: "affiliate_link", key: "affiliate_link", width: 260, render: (v: string) => (v ? <a href={v} target="_blank" rel="noreferrer" style={{ wordBreak: "break-all", whiteSpace: "normal" }}>{v}</a> : <span />) },
          ]}
        />
      </Card>
    </div>
  );
}


