import { useState, useEffect } from 'react';
import {
  Shield, ShieldCheck, ShieldAlert, AlertTriangle, CheckCircle,
  XCircle, ChevronDown, ChevronUp, Loader2, Info,
} from 'lucide-react';
import axios from 'axios';
import { getAccessToken, isAdmin } from '../lib/auth';
import type { Cluster } from '../types';

const authApi = axios.create({ baseURL: '/api' });
authApi.interceptors.request.use((config) => {
  const token = getAccessToken();
  if (token) config.headers.Authorization = `Bearer ${token}`;
  return config;
});

interface Finding {
  id: string;
  category: string;
  check: string;
  severity: string;
  status: string;
  message: string;
  recommendation: string;
  details?: Record<string, unknown>;
}

interface CategorySummary {
  total: number;
  passed: number;
  failed: number;
  warnings: number;
  errors: number;
  score: number;
}

interface ScanResult {
  cluster_id: string;
  cluster_name: string;
  score: number;
  grade: string;
  total_checks: number;
  passed: number;
  failed: number;
  critical_issues: number;
  high_issues: number;
  findings: Finding[];
  summary: Record<string, CategorySummary>;
}

const SEVERITY_COLORS: Record<string, string> = {
  critical: 'bg-red-100 text-red-700 border-red-200',
  high: 'bg-orange-100 text-orange-700 border-orange-200',
  medium: 'bg-yellow-100 text-yellow-700 border-yellow-200',
  low: 'bg-blue-100 text-blue-700 border-blue-200',
};

const STATUS_ICONS: Record<string, React.ReactNode> = {
  pass: <CheckCircle size={16} className="text-green-500" />,
  fail: <XCircle size={16} className="text-red-500" />,
  warning: <AlertTriangle size={16} className="text-yellow-500" />,
  error: <Info size={16} className="text-gray-400" />,
};

const GRADE_COLORS: Record<string, string> = {
  A: 'text-green-600 bg-green-50 border-green-200',
  B: 'text-blue-600 bg-blue-50 border-blue-200',
  C: 'text-yellow-600 bg-yellow-50 border-yellow-200',
  D: 'text-orange-600 bg-orange-50 border-orange-200',
  F: 'text-red-600 bg-red-50 border-red-200',
};

export default function SecurityScan() {
  const [clusters, setClusters] = useState<Cluster[]>([]);
  const [selectedCluster, setSelectedCluster] = useState<string>('');
  const [scanning, setScanning] = useState(false);
  const [result, setResult] = useState<ScanResult | null>(null);
  const [expandedCategories, setExpandedCategories] = useState<Set<string>>(new Set());
  const [expandedFindings, setExpandedFindings] = useState<Set<string>>(new Set());
  const [filterSeverity, setFilterSeverity] = useState<string>('all');
  const [filterStatus, setFilterStatus] = useState<string>('all');
  const [error, setError] = useState('');
  const admin = isAdmin();

  useEffect(() => {
    authApi.get<Cluster[]>('/clusters').then(r => {
      const scannable = r.data.filter(c => c.state === 'running' || (c.kind === 'external' && c.state === 'connected'));
      setClusters(scannable);
      if (scannable.length > 0) setSelectedCluster(scannable[0].id);
    });
  }, []);

  const runScan = async () => {
    if (!selectedCluster) return;
    setScanning(true);
    setError('');
    setResult(null);
    try {
      const { data } = await authApi.post<ScanResult>(`/security-scan/clusters/${selectedCluster}/scan`);
      setResult(data);
      // Auto-expand all categories
      setExpandedCategories(new Set(Object.keys(data.summary)));
    } catch (err: unknown) {
      setError((err as { response?: { data?: { detail?: string } } })?.response?.data?.detail || 'Scan failed');
    } finally {
      setScanning(false);
    }
  };

  const toggleCategory = (cat: string) => {
    setExpandedCategories(prev => {
      const next = new Set(prev);
      if (next.has(cat)) next.delete(cat); else next.add(cat);
      return next;
    });
  };

  const toggleFinding = (id: string) => {
    setExpandedFindings(prev => {
      const next = new Set(prev);
      if (next.has(id)) next.delete(id); else next.add(id);
      return next;
    });
  };

  const filteredFindings = (category: string) => {
    if (!result) return [];
    return result.findings.filter(f => {
      if (f.category !== category) return false;
      if (filterSeverity !== 'all' && f.severity !== filterSeverity) return false;
      if (filterStatus !== 'all' && f.status !== filterStatus) return false;
      return true;
    });
  };

  return (
    <div className="space-y-6">
      <div className="flex items-center justify-between">
        <div>
          <h1 className="text-2xl font-bold text-gray-900 flex items-center gap-2">
            <Shield size={24} />
            Security Scanner
          </h1>
          <p className="text-gray-500 mt-1">Vulnerability Assessment & Penetration Testing for Kafka</p>
        </div>
      </div>

      {/* Cluster selector + scan button */}
      <div className="bg-white border border-gray-200 rounded-xl p-6 shadow-sm">
        <div className="flex items-center gap-4">
          <div className="flex-1">
            <label className="block text-sm font-medium text-gray-700 mb-1">Select Cluster</label>
            <select
              value={selectedCluster}
              onChange={e => setSelectedCluster(e.target.value)}
              className="w-full px-3 py-2 border border-gray-300 rounded-lg text-sm focus:ring-2 focus:ring-blue-500 focus:border-blue-500"
            >
              {clusters.length === 0 && <option value="">No scannable clusters</option>}
              {clusters.map(c => (
                <option key={c.id} value={c.id}>
                  {c.name} ({c.kind === 'external' ? 'external' : 'managed'}) - Kafka {c.kafka_version}
                </option>
              ))}
            </select>
          </div>
          <div className="pt-5">
            <button
              onClick={runScan}
              disabled={scanning || !selectedCluster || !admin}
              className="flex items-center gap-2 px-5 py-2 bg-blue-600 hover:bg-blue-700 disabled:bg-blue-400 text-white rounded-lg font-medium transition-colors"
            >
              {scanning ? (
                <Loader2 size={16} className="animate-spin" />
              ) : (
                <ShieldCheck size={16} />
              )}
              {scanning ? 'Scanning...' : 'Run Security Scan'}
            </button>
          </div>
        </div>
        {!admin && (
          <p className="text-xs text-gray-400 mt-2">Admin role required to run security scans.</p>
        )}
      </div>

      {error && (
        <div className="p-3 bg-red-50 border border-red-200 rounded-lg text-red-700 text-sm">{error}</div>
      )}

      {/* Scan Results */}
      {result && (
        <>
          {/* Score card */}
          <div className="grid grid-cols-4 gap-4">
            <div className={`border rounded-xl p-6 text-center ${GRADE_COLORS[result.grade] || 'bg-gray-50'}`}>
              <div className="text-5xl font-bold mb-1">{result.grade}</div>
              <div className="text-sm font-medium">Security Grade</div>
              <div className="text-xs mt-1 opacity-75">{result.score}% score</div>
            </div>
            <div className="bg-white border border-gray-200 rounded-xl p-6 text-center">
              <div className="text-3xl font-bold text-gray-900">{result.total_checks}</div>
              <div className="text-sm text-gray-500">Total Checks</div>
              <div className="text-xs text-gray-400 mt-1">
                <span className="text-green-600">{result.passed} passed</span>
                {' · '}
                <span className="text-red-600">{result.failed} failed</span>
              </div>
            </div>
            <div className="bg-white border border-gray-200 rounded-xl p-6 text-center">
              <div className="text-3xl font-bold text-red-600">{result.critical_issues}</div>
              <div className="text-sm text-gray-500">Critical Issues</div>
              <div className="text-xs text-gray-400 mt-1">Requires immediate attention</div>
            </div>
            <div className="bg-white border border-gray-200 rounded-xl p-6 text-center">
              <div className="text-3xl font-bold text-orange-600">{result.high_issues}</div>
              <div className="text-sm text-gray-500">High Issues</div>
              <div className="text-xs text-gray-400 mt-1">Should be addressed soon</div>
            </div>
          </div>

          {/* Filters */}
          <div className="flex items-center gap-4">
            <div className="flex items-center gap-2">
              <span className="text-sm text-gray-500">Severity:</span>
              {['all', 'critical', 'high', 'medium', 'low'].map(sev => (
                <button
                  key={sev}
                  onClick={() => setFilterSeverity(sev)}
                  className={`px-2.5 py-1 text-xs rounded-full border transition-colors ${
                    filterSeverity === sev
                      ? 'bg-blue-100 text-blue-700 border-blue-300'
                      : 'bg-white text-gray-500 border-gray-200 hover:bg-gray-50'
                  }`}
                >
                  {sev === 'all' ? 'All' : sev.charAt(0).toUpperCase() + sev.slice(1)}
                </button>
              ))}
            </div>
            <div className="flex items-center gap-2">
              <span className="text-sm text-gray-500">Status:</span>
              {['all', 'pass', 'fail', 'warning'].map(st => (
                <button
                  key={st}
                  onClick={() => setFilterStatus(st)}
                  className={`px-2.5 py-1 text-xs rounded-full border transition-colors ${
                    filterStatus === st
                      ? 'bg-blue-100 text-blue-700 border-blue-300'
                      : 'bg-white text-gray-500 border-gray-200 hover:bg-gray-50'
                  }`}
                >
                  {st === 'all' ? 'All' : st.charAt(0).toUpperCase() + st.slice(1)}
                </button>
              ))}
            </div>
          </div>

          {/* Category breakdowns */}
          <div className="space-y-4">
            {Object.entries(result.summary).map(([category, summary]) => {
              const findings = filteredFindings(category);
              const isExpanded = expandedCategories.has(category);
              return (
                <div key={category} className="bg-white border border-gray-200 rounded-xl shadow-sm overflow-hidden">
                  <button
                    onClick={() => toggleCategory(category)}
                    className="w-full flex items-center justify-between p-4 hover:bg-gray-50 transition-colors"
                  >
                    <div className="flex items-center gap-3">
                      {summary.failed > 0 ? (
                        <ShieldAlert size={20} className="text-red-500" />
                      ) : (
                        <ShieldCheck size={20} className="text-green-500" />
                      )}
                      <span className="font-semibold text-gray-900">{category}</span>
                      <span className="text-xs text-gray-400">
                        {summary.passed}/{summary.total} passed ({summary.score}%)
                      </span>
                    </div>
                    <div className="flex items-center gap-3">
                      {summary.failed > 0 && (
                        <span className="px-2 py-0.5 text-xs font-medium bg-red-100 text-red-700 rounded-full">
                          {summary.failed} failed
                        </span>
                      )}
                      {summary.warnings > 0 && (
                        <span className="px-2 py-0.5 text-xs font-medium bg-yellow-100 text-yellow-700 rounded-full">
                          {summary.warnings} warnings
                        </span>
                      )}
                      {isExpanded ? <ChevronUp size={16} /> : <ChevronDown size={16} />}
                    </div>
                  </button>

                  {isExpanded && (
                    <div className="border-t border-gray-100">
                      {findings.length === 0 ? (
                        <div className="p-4 text-sm text-gray-400 text-center">
                          No findings match current filters
                        </div>
                      ) : (
                        <div className="divide-y divide-gray-100">
                          {findings.map(finding => (
                            <div key={finding.id} className="p-4">
                              <div
                                className="flex items-start gap-3 cursor-pointer"
                                onClick={() => toggleFinding(finding.id)}
                              >
                                {STATUS_ICONS[finding.status] || STATUS_ICONS.error}
                                <div className="flex-1 min-w-0">
                                  <div className="flex items-center gap-2">
                                    <span className="text-sm font-medium text-gray-900">{finding.check}</span>
                                    <span className={`px-1.5 py-0.5 text-[10px] font-medium rounded border ${SEVERITY_COLORS[finding.severity] || 'bg-gray-100 text-gray-600'}`}>
                                      {finding.severity.toUpperCase()}
                                    </span>
                                  </div>
                                  <p className="text-xs text-gray-500 mt-0.5">{finding.message}</p>
                                </div>
                                <div className="shrink-0">
                                  {expandedFindings.has(finding.id) ? <ChevronUp size={14} className="text-gray-400" /> : <ChevronDown size={14} className="text-gray-400" />}
                                </div>
                              </div>

                              {expandedFindings.has(finding.id) && (
                                <div className="mt-3 ml-7 space-y-2">
                                  <div className="bg-blue-50 border border-blue-100 rounded-lg p-3">
                                    <p className="text-xs font-medium text-blue-700 mb-1">Recommendation</p>
                                    <p className="text-xs text-blue-600">{finding.recommendation}</p>
                                  </div>
                                  {finding.details && (
                                    <div className="bg-gray-50 border border-gray-100 rounded-lg p-3">
                                      <p className="text-xs font-medium text-gray-600 mb-1">Details</p>
                                      <pre className="text-xs text-gray-500 font-mono whitespace-pre-wrap">
                                        {JSON.stringify(finding.details, null, 2)}
                                      </pre>
                                    </div>
                                  )}
                                </div>
                              )}
                            </div>
                          ))}
                        </div>
                      )}
                    </div>
                  )}
                </div>
              );
            })}
          </div>
        </>
      )}

      {/* Empty state */}
      {!result && !scanning && (
        <div className="bg-gray-50 border border-gray-200 rounded-xl p-12 text-center">
          <Shield size={40} className="mx-auto text-gray-400 mb-4" />
          <h3 className="font-semibold text-gray-700">No Scan Results</h3>
          <p className="text-sm text-gray-500 mt-2">
            Select a running managed cluster or connected external cluster and click "Run Security Scan" to assess your Kafka cluster's security posture.
          </p>
        </div>
      )}
    </div>
  );
}
