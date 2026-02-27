// API helper — auto-detect backend, fallback to demo data

const API_BASE = import.meta.env.VITE_API_URL || '';

let _backendAvailable = null;

export async function checkBackend() {
  if (_backendAvailable !== null) return _backendAvailable;
  try {
    const res = await fetch(`${API_BASE}/api/health`, { signal: AbortSignal.timeout(3000) });
    const data = await res.json();
    _backendAvailable = data.database === 'connected';
    console.log(_backendAvailable ? '✅ Backend connected with database' : '⚠️ Backend running but no database');
    return _backendAvailable;
  } catch {
    _backendAvailable = false;
    console.log('⚠️ No backend detected — using demo data');
    return false;
  }
}

export async function api(path, params = {}) {
  const qs = new URLSearchParams(params).toString();
  const url = `${API_BASE}/api/${path}${qs ? '?' + qs : ''}`;
  const res = await fetch(url);
  if (!res.ok) throw new Error(`API error: ${res.status}`);
  return res.json();
}

export async function apiPost(path, body) {
  const res = await fetch(`${API_BASE}/api/${path}`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(body),
  });
  if (!res.ok) throw new Error(`API error: ${res.status}`);
  return res.json();
}

export function isLive() {
  return _backendAvailable === true;
}
