import { useEffect, useState } from 'react';
import { TrendingUp, AlertTriangle, RefreshCw, Loader2 } from 'lucide-react';
import { getCapacityForecast } from '../../lib/api';
import type { CapacityForecast as Forecast } from '../../types';

type Props = { clusterId: string };

const fmt_bytes = (b: number): string => {
  if (b == null) return '-';
  const u = ['B', 'KB', 'MB', 'GB', 'TB'];
  let n = b;
  let i = 0;
  while (n >= 1024 && i < u.length - 1) { n /= 1024; i++; }
  return `${n.toFixed(1)} ${u[i]}`;
};

const fmt_pct = (n: number): string => `${n.toFixed(1)}%`;

const fmt_days = (d: number | null | undefined): string => {
  if (d == null) return '—';
  if (d < 1) return `${(d * 24).toFixed(1)} hours`;
  if (d < 30) return `${d.toFixed(1)} days`;
  if (d < 365) return `${(d / 30).toFixed(1)} months`;
  return `${(d / 365).toFixed(1)} years`;
};

export default function CapacityForecast({ clusterId }: Props) {
  const [data, setData] = useState<Forecast | null>(null);
  const [loading, setLoading] = useState(true);

  const fetchForecast = async () => {
    setLoading(true);
    try {
      const r = await getCapacityForecast(clusterId);
      setData(r);
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => { fetchForecast(); }, [clusterId]);

  if (loading) {
    return <div className="flex items-center gap-2 text-sm text-gray-500"><Loader2 size={14} className="animate-spin" /> Loading forecast…</div>;
  }
  if (!data || !data.available) {
    return (
      <div className="bg-yellow-50 border border-yellow-200 rounded p-4 text-sm text-yellow-800 flex items-start gap-2">
        <AlertTriangle size={16} className="mt-0.5 shrink-0" />
        <div>
          <div className="font-medium">Capacity forecast unavailable</div>
          <div className="mt-1 text-yellow-700">{data?.reason ?? 'Unknown error'}</div>
        </div>
      </div>
    );
  }

  // Build a tiny inline SVG line chart (no chart lib for simplicity).
  // X axis: time (history -> forecast). Y axis: bytes used.
  const all = [...data.history, ...data.forecast];
  if (all.length === 0) return null;
  const minT = Math.min(...all.map(p => p.t));
  const maxT = Math.max(...all.map(p => p.t));
  const minY = 0;
  const maxY = Math.max(data.total_bytes, ...all.map(p => p.used_bytes));
  const W = 760;
  const H = 200;
  const padL = 40;
  const padR = 16;
  const padT = 16;
  const padB = 24;
  const xs = (t: number) => padL + ((t - minT) / (maxT - minT || 1)) * (W - padL - padR);
  const ys = (v: number) => padT + (1 - (v - minY) / (maxY - minY || 1)) * (H - padT - padB);

  const histPath = data.history.map((p, i) =>
    `${i === 0 ? 'M' : 'L'} ${xs(p.t).toFixed(1)} ${ys(p.used_bytes).toFixed(1)}`
  ).join(' ');
  const fcastPath = data.forecast.map((p, i) =>
    `${i === 0 ? 'M' : 'L'} ${xs(p.t).toFixed(1)} ${ys(p.used_bytes).toFixed(1)}`
  ).join(' ');
  const thresholdY = ys(data.total_bytes * data.full_threshold);
  const totalY = ys(data.total_bytes);
  const nowX = xs(data.history[data.history.length - 1].t);

  return (
    <div>
      <div className="flex items-center justify-between mb-3">
        <h3 className="text-sm font-semibold text-gray-700 flex items-center gap-2">
          <TrendingUp size={16} className="text-blue-600" />
          Capacity trend forecast
        </h3>
        <button
          onClick={fetchForecast}
          className="flex items-center gap-1.5 px-3 py-1.5 text-xs border rounded-lg hover:bg-gray-50"
        >
          <RefreshCw size={13} /> Refresh
        </button>
      </div>

      <div className="grid grid-cols-2 md:grid-cols-4 gap-3 mb-4">
        <Stat label="Current usage" value={`${fmt_pct(data.current_used_pct)}`} sub={`${fmt_bytes(data.current_used_bytes)} of ${fmt_bytes(data.total_bytes)}`} />
        <Stat label="Growth rate" value={fmt_bytes(data.growth_bytes_per_day) + ' / day'} sub={data.growth_bytes_per_day < 0 ? 'shrinking' : data.growth_bytes_per_day === 0 ? 'flat' : 'growing'} />
        <Stat
          label={`ETA to ${fmt_pct(data.full_threshold * 100)}`}
          value={fmt_days(data.eta_to_threshold_days ?? null)}
          sub={data.eta_to_threshold_days == null ? 'no growth detected' : 'plan capacity by then'}
          warn={data.eta_to_threshold_days != null && data.eta_to_threshold_days < 30}
        />
        <Stat label="Disk total" value={fmt_bytes(data.total_bytes)} sub="from node_exporter" />
      </div>

      <div className="border rounded-lg p-3 bg-white">
        <svg width="100%" height={H} viewBox={`0 0 ${W} ${H}`} preserveAspectRatio="none">
          {/* Threshold line */}
          <line x1={padL} x2={W - padR} y1={thresholdY} y2={thresholdY} stroke="#dc2626" strokeWidth="1" strokeDasharray="4 3" />
          <text x={W - padR} y={thresholdY - 3} textAnchor="end" fontSize="10" fill="#dc2626">{fmt_pct(data.full_threshold * 100)} of disk</text>
          {/* 100% line */}
          <line x1={padL} x2={W - padR} y1={totalY} y2={totalY} stroke="#94a3b8" strokeWidth="1" strokeDasharray="2 2" />
          <text x={W - padR} y={totalY - 3} textAnchor="end" fontSize="10" fill="#64748b">100% ({fmt_bytes(data.total_bytes)})</text>
          {/* "now" marker */}
          <line x1={nowX} x2={nowX} y1={padT} y2={H - padB} stroke="#0f172a" strokeWidth="1" strokeOpacity="0.3" />
          <text x={nowX + 3} y={padT + 10} fontSize="10" fill="#0f172a">now</text>
          {/* Historical usage */}
          <path d={histPath} fill="none" stroke="#2563eb" strokeWidth="2" />
          {/* Forecast (dashed) */}
          <path d={fcastPath} fill="none" stroke="#2563eb" strokeWidth="2" strokeDasharray="5 4" strokeOpacity="0.6" />
        </svg>
      </div>
      <div className="mt-2 text-xs text-gray-500">
        Solid line = historical usage from Prometheus. Dashed line = linear projection forward {data.forecast.length > 0 ? Math.round((data.forecast[data.forecast.length - 1].t - data.forecast[0].t) / 86400) : 0} day(s).
      </div>
    </div>
  );
}

function Stat({ label, value, sub, warn }: { label: string; value: string; sub?: string; warn?: boolean }) {
  return (
    <div className={`border rounded-lg px-3 py-2 ${warn ? 'border-red-300 bg-red-50' : 'bg-white'}`}>
      <div className="text-[11px] uppercase text-gray-500 tracking-wide">{label}</div>
      <div className={`text-base font-semibold mt-0.5 ${warn ? 'text-red-700' : 'text-gray-900'}`}>{value}</div>
      {sub && <div className="text-xs text-gray-500 mt-0.5">{sub}</div>}
    </div>
  );
}
