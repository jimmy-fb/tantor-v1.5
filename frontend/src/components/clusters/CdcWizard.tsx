import { useEffect, useState } from 'react';
import { Database, ChevronRight, Loader2, X, CheckCircle2, AlertTriangle } from 'lucide-react';
import { listCdcTemplates, createCdcConnector } from '../../lib/api';
import type { CdcTemplate } from '../../types';

type Props = {
  clusterId: string;
  onClose: () => void;
  onCreated: () => void;
};

/**
 * Pre-curated CDC connector wizard. Lets the operator pick a template
 * (MySQL / Postgres / MongoDB / SQL Server via Debezium), fill in only the
 * customer-specific fields (host, db, credentials), and Tantor materializes
 * the rest of the Connect config from the template's `fixed` keys.
 *
 * APB asked for this in the requirements table — "Stream real-time database
 * changes directly into Kafka topics via CDC pipelines".
 */
export default function CdcWizard({ clusterId, onClose, onCreated }: Props) {
  const [templates, setTemplates] = useState<CdcTemplate[]>([]);
  const [picked, setPicked] = useState<CdcTemplate | null>(null);
  const [name, setName] = useState('');
  const [values, setValues] = useState<Record<string, string>>({});
  const [submitting, setSubmitting] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [created, setCreated] = useState(false);

  useEffect(() => {
    listCdcTemplates(clusterId).then(setTemplates).catch(() => setTemplates([]));
  }, [clusterId]);

  const onPick = (t: CdcTemplate) => {
    setPicked(t);
    const initial: Record<string, string> = {};
    for (const f of t.fields) {
      if (f.default) initial[f.key] = f.default;
    }
    setValues(initial);
    setName(`${t.id}-${Date.now().toString(36)}`);
    setError(null);
  };

  const submit = async () => {
    if (!picked) return;
    // Required field check
    const missing = picked.fields.filter(f => f.required && !(values[f.key] || '').trim());
    if (missing.length > 0) {
      setError(`Missing required fields: ${missing.map(f => f.label).join(', ')}`);
      return;
    }
    setSubmitting(true);
    setError(null);
    try {
      await createCdcConnector(clusterId, {
        name: name.trim(),
        template_id: picked.id,
        fields: values,
      });
      setCreated(true);
      onCreated();
    } catch (e: unknown) {
      const msg = e instanceof Error ? e.message : 'Failed to create CDC connector';
      // Try to extract the FastAPI detail
      const apiErr = (e as { response?: { data?: { detail?: string } } })?.response?.data?.detail;
      setError(apiErr || msg);
    } finally {
      setSubmitting(false);
    }
  };

  return (
    <div className="fixed inset-0 z-50 bg-black/40 flex items-center justify-center p-4">
      <div className="bg-white rounded-lg shadow-xl w-full max-w-2xl max-h-[90vh] overflow-y-auto">
        <div className="flex items-center justify-between border-b px-5 py-3">
          <div className="flex items-center gap-2">
            <Database size={18} className="text-blue-600" />
            <h3 className="text-base font-semibold">CDC quickstart — stream a database into Kafka</h3>
          </div>
          <button onClick={onClose} className="text-gray-400 hover:text-gray-700">
            <X size={18} />
          </button>
        </div>

        {created ? (
          <div className="p-8 text-center">
            <CheckCircle2 size={48} className="text-green-500 mx-auto mb-3" />
            <h4 className="text-lg font-semibold mb-1">CDC pipeline created</h4>
            <p className="text-sm text-gray-600 mb-4">
              Connector <span className="font-mono">{name}</span> is now polling source database changes
              and writing them to Kafka. Watch the Connect tab for status and lag.
            </p>
            <button onClick={onClose} className="px-4 py-2 bg-blue-600 text-white rounded-lg text-sm hover:bg-blue-700">
              Close
            </button>
          </div>
        ) : !picked ? (
          <div className="p-5">
            <p className="text-sm text-gray-600 mb-3">
              Pick a source database. Tantor uses <a href="https://debezium.io" target="_blank" rel="noopener noreferrer" className="text-blue-600 underline">Debezium</a> connectors;
              the relevant plugin must be installed on this cluster's Kafka Connect (Plugins tab).
            </p>
            <div className="space-y-2">
              {templates.length === 0 && (
                <div className="text-sm text-gray-500 italic py-4 text-center border border-dashed rounded">
                  Loading templates…
                </div>
              )}
              {templates.map(t => (
                <button
                  key={t.id}
                  onClick={() => onPick(t)}
                  className="w-full flex items-center justify-between text-left border rounded-lg p-3 hover:bg-blue-50 hover:border-blue-300"
                >
                  <div>
                    <div className="text-sm font-semibold text-gray-900">{t.name}</div>
                    <div className="text-xs text-gray-600 mt-0.5">{t.description}</div>
                  </div>
                  <ChevronRight size={18} className="text-gray-400" />
                </button>
              ))}
            </div>
          </div>
        ) : (
          <div className="p-5">
            <button onClick={() => setPicked(null)} className="text-xs text-blue-600 hover:underline mb-3">
              ← Back to templates
            </button>
            <h4 className="text-base font-semibold mb-1">{picked.name}</h4>
            <p className="text-xs text-gray-600 mb-4">{picked.description}</p>

            <label className="block text-xs font-medium text-gray-700 mb-1">Connector name</label>
            <input
              value={name}
              onChange={e => setName(e.target.value)}
              className="w-full mb-4 px-3 py-2 border rounded-lg text-sm"
              placeholder="e.g. mysql-orders-cdc"
            />

            <div className="space-y-3">
              {picked.fields.map(f => (
                <div key={f.key}>
                  <label className="block text-xs font-medium text-gray-700 mb-1">
                    {f.label}
                    {f.required && <span className="text-red-500 ml-0.5">*</span>}
                    <span className="ml-2 text-gray-400 font-mono">{f.key}</span>
                  </label>
                  <input
                    type={f.secret ? 'password' : 'text'}
                    value={values[f.key] || ''}
                    placeholder={f.placeholder || ''}
                    onChange={e => setValues({ ...values, [f.key]: e.target.value })}
                    className="w-full px-3 py-2 border rounded-lg text-sm font-mono"
                  />
                </div>
              ))}
            </div>

            {error && (
              <div className="mt-4 flex items-start gap-2 text-sm text-red-700 bg-red-50 border border-red-200 rounded p-3">
                <AlertTriangle size={16} className="mt-0.5 shrink-0" />
                <span>{error}</span>
              </div>
            )}

            <div className="mt-5 flex justify-end gap-2">
              <button
                onClick={onClose}
                className="px-4 py-2 border rounded-lg text-sm hover:bg-gray-50"
                disabled={submitting}
              >
                Cancel
              </button>
              <button
                onClick={submit}
                disabled={submitting || !name.trim()}
                className="flex items-center gap-2 px-4 py-2 bg-blue-600 text-white rounded-lg text-sm hover:bg-blue-700 disabled:opacity-50"
              >
                {submitting && <Loader2 size={16} className="animate-spin" />}
                Create CDC connector
              </button>
            </div>
          </div>
        )}
      </div>
    </div>
  );
}
