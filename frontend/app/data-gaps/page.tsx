"use client";

import React, { useEffect, useState } from "react";
import { Card, Table, Tabs, Tag, Spin, Button } from "antd";
import type { ColumnsType } from "antd/es/table";
import { ArrowLeftOutlined } from "@ant-design/icons";
import Link from "next/link";
import axios from "axios";

const API_BASE = process.env.NEXT_PUBLIC_API_URL || "http://localhost:8000";

interface GapURL {
  url: string;
  sessions?: number;
  primary_category?: string;
  vertical?: string;
  page_status?: string;
  airtable_id?: string;
  status_code?: number;
  catalogued: boolean;
  duplicate_count?: number;
  page_ids?: string;
}

interface DataGapsResponse {
  airtable_not_bigquery: {
    count: number;
    urls: GapURL[];
  };
  bigquery_not_airtable: {
    count: number;
    urls: GapURL[];
  };
  summary: {
    total_gaps: number;
    at_only: number;
    bq_only: number;
  };
}

export default function DataGapsPage() {
  const [loading, setLoading] = useState(true);
  const [data, setData] = useState<DataGapsResponse | null>(null);

  useEffect(() => {
    fetchDataGaps();
  }, []);

  const fetchDataGaps = async () => {
    setLoading(true);
    try {
      const res = await axios.get<DataGapsResponse>(`${API_BASE}/data-gaps`);
      setData(res.data);
    } catch (error) {
      console.error("Failed to fetch data gaps:", error);
    } finally {
      setLoading(false);
    }
  };

  const atNotBqColumns: ColumnsType<GapURL> = [
    {
      title: "URL",
      dataIndex: "url",
      key: "url",
      render: (url: string) => (
        <a href={url} target="_blank" rel="noopener noreferrer" style={{ fontSize: "12px" }}>
          {url}
        </a>
      ),
      width: "40%",
    },
    {
      title: "Category",
      dataIndex: "primary_category",
      key: "primary_category",
      width: "15%",
    },
    {
      title: "Vertical",
      dataIndex: "vertical",
      key: "vertical",
      width: "12%",
    },
    {
      title: "Page Status",
      dataIndex: "page_status",
      key: "page_status",
      render: (status: string) => (
        <Tag color={status === "Active" ? "green" : "orange"}>{status || "—"}</Tag>
      ),
      width: "12%",
    },
    {
      title: "Status Code",
      dataIndex: "status_code",
      key: "status_code",
      render: (code: number) => (
        <Tag color={code === 200 ? "green" : code === 0 ? "blue" : "red"}>
          {code || "—"}
        </Tag>
      ),
      width: "10%",
    },
    {
      title: "Catalogued",
      dataIndex: "catalogued",
      key: "catalogued",
      render: (catalogued: boolean) => (
        <Tag color={catalogued ? "green" : "default"}>{catalogued ? "Yes" : "No"}</Tag>
      ),
      width: "10%",
    },
    {
      title: "Duplicates",
      dataIndex: "duplicate_count",
      key: "duplicate_count",
      render: (count: number, record: GapURL) => {
        if (count && count > 1) {
          return (
            <Tag color="red" title={`Page IDs: ${record.page_ids}`}>
              {count} copies
            </Tag>
          );
        }
        return <span>—</span>;
      },
      width: "11%",
    },
  ];

  const bqNotAtColumns: ColumnsType<GapURL> = [
    {
      title: "URL",
      dataIndex: "url",
      key: "url",
      render: (url: string) => (
        <a href={url} target="_blank" rel="noopener noreferrer" style={{ fontSize: "12px" }}>
          {url}
        </a>
      ),
      width: "40%",
    },
    {
      title: "Sessions",
      dataIndex: "sessions",
      key: "sessions",
      sorter: (a, b) => (a.sessions || 0) - (b.sessions || 0),
      defaultSortOrder: "descend",
      width: "10%",
    },
    {
      title: "Category",
      dataIndex: "primary_category",
      key: "primary_category",
      width: "15%",
    },
    {
      title: "Vertical",
      dataIndex: "vertical",
      key: "vertical",
      width: "12%",
    },
    {
      title: "Status Code",
      dataIndex: "status_code",
      key: "status_code",
      render: (code: number) => (
        <Tag color={code === 200 ? "green" : code === 0 ? "blue" : "red"}>
          {code || "—"}
        </Tag>
      ),
      width: "10%",
    },
    {
      title: "Catalogued",
      dataIndex: "catalogued",
      key: "catalogued",
      render: (catalogued: boolean) => (
        <Tag color={catalogued ? "green" : "default"}>{catalogued ? "Yes" : "No"}</Tag>
      ),
      width: "10%",
    },
    {
      title: "Duplicates",
      dataIndex: "duplicate_count",
      key: "duplicate_count",
      render: (count: number, record: GapURL) => {
        if (count && count > 1) {
          return (
            <Tag color="red" title={`Page IDs: ${record.page_ids}`}>
              {count} copies
            </Tag>
          );
        }
        return <span>—</span>;
      },
      width: "11%",
    },
  ];

  // Add unique keys to data
  const atNotBqData = (data?.airtable_not_bigquery.urls || []).map((item, idx) => ({
    ...item,
    _uniqueKey: `at-${idx}-${item.url}`,
  }));

  const bqNotAtData = (data?.bigquery_not_airtable.urls || []).map((item, idx) => ({
    ...item,
    _uniqueKey: `bq-${idx}-${item.url}`,
  }));

  const tabs = [
    {
      key: "at-not-bq",
      label: `In Airtable, Not in BigQuery (${data?.airtable_not_bigquery.count || 0})`,
      children: (
        <Card>
          <div style={{ marginBottom: 16, color: "#666" }}>
            <p style={{ marginBottom: 8 }}>These URLs are in Airtable but have no session data from BigQuery. They may be:</p>
            <ul style={{ marginTop: 0, marginBottom: 0 }}>
              <li>New URLs that haven't been launched yet</li>
              <li>Paused/inactive pages</li>
              <li>URLs that need to be removed from Airtable</li>
            </ul>
          </div>
          <Table
            dataSource={atNotBqData}
            columns={atNotBqColumns}
            rowKey={(r: any) => r._uniqueKey}
            pagination={{ pageSize: 50, showSizeChanger: true }}
            size="small"
          />
        </Card>
      ),
    },
    {
      key: "bq-not-at",
      label: `In BigQuery, Not in Airtable (${data?.bigquery_not_airtable.count || 0})`,
      children: (
        <Card>
          <div style={{ marginBottom: 16, color: "#666" }}>
            <p style={{ marginBottom: 8 }}>These URLs have session data from BigQuery but aren't in Airtable. They may be:</p>
            <ul style={{ marginTop: 0, marginBottom: 0 }}>
              <li>Old URLs that should be archived</li>
              <li>Test pages</li>
              <li>URLs that should be added to Airtable</li>
            </ul>
          </div>
          <Table
            dataSource={bqNotAtData}
            columns={bqNotAtColumns}
            rowKey={(r: any) => r._uniqueKey}
            pagination={{ pageSize: 50, showSizeChanger: true }}
            size="small"
          />
        </Card>
      ),
    },
  ];

  if (loading) {
    return (
      <div style={{ display: "flex", justifyContent: "center", alignItems: "center", height: "100vh" }}>
        <Spin size="large" />
      </div>
    );
  }

  return (
    <div style={{ padding: "60px 24px 24px", maxWidth: 1280, margin: "0 auto" }}>
      <div style={{ marginBottom: 24, display: "flex", alignItems: "center", gap: 16 }}>
        <Link href="/">
          <Button icon={<ArrowLeftOutlined />}>Back to Catalogue</Button>
        </Link>
        <h1 style={{ margin: 0 }}>Data Source Gaps</h1>
      </div>
      <Card style={{ marginBottom: 24 }}>
        <h3>Summary</h3>
        <div style={{ display: "flex", gap: "32px" }}>
          <div>
            <div style={{ fontSize: "32px", fontWeight: "bold", color: "#1890ff" }}>
              {data?.summary.total_gaps || 0}
            </div>
            <div style={{ color: "#666" }}>Total Gaps</div>
          </div>
          <div>
            <div style={{ fontSize: "32px", fontWeight: "bold", color: "#52c41a" }}>
              {data?.summary.at_only || 0}
            </div>
            <div style={{ color: "#666" }}>Airtable Only</div>
          </div>
          <div>
            <div style={{ fontSize: "32px", fontWeight: "bold", color: "#faad14" }}>
              {data?.summary.bq_only || 0}
            </div>
            <div style={{ color: "#666" }}>BigQuery Only</div>
          </div>
        </div>
      </Card>

      <Tabs defaultActiveKey="at-not-bq" items={tabs} />
    </div>
  );
}

