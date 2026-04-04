// Soft gate — tracks free actions, prompts sign-up after limit
(function() {
  const LIMIT = 3;
  const KEY = 'agora_action_count';
  const DISMISSED_KEY = 'agora_gate_dismissed';

  function isLoggedIn() {
    return !!localStorage.getItem('agora_token');
  }

  function getCount() {
    return parseInt(localStorage.getItem(KEY) || '0', 10);
  }

  function showGate() {
    if (document.getElementById('agora-gate-modal')) return;
    const modal = document.createElement('div');
    modal.id = 'agora-gate-modal';
    modal.style.cssText = 'position:fixed;inset:0;background:rgba(0,0,0,0.75);z-index:9999;display:flex;align-items:center;justify-content:center;font-family:system-ui';
    modal.innerHTML = `
      <div style="background:#111827;border:1px solid #1e293b;border-radius:12px;padding:2rem;max-width:420px;width:90%;text-align:center;">
        <div style="font-size:2rem;margin-bottom:0.75rem;">📈</div>
        <h2 style="color:#f1f5f9;font-size:1.25rem;font-weight:600;margin:0 0 0.5rem">You're getting the most out of Agora</h2>
        <p style="color:#64748b;font-size:0.9rem;margin:0 0 1.5rem;line-height:1.6">Create a free account to keep your research history, unlock unlimited queries, and save your watchlist.</p>
        <a href="/auth/index.html" style="display:block;background:#10b981;color:#fff;padding:0.75rem;border-radius:6px;font-weight:600;font-size:0.95rem;text-decoration:none;margin-bottom:0.75rem;">Create free account</a>
        <button id="agora-gate-dismiss" style="background:none;border:none;color:#64748b;font-size:0.85rem;cursor:pointer;padding:0.5rem;">Maybe later</button>
      </div>`;
    document.body.appendChild(modal);
    document.getElementById('agora-gate-dismiss').onclick = function() {
      modal.remove();
      sessionStorage.setItem(DISMISSED_KEY, '1');
    };
  }

  window.agoraTrackAction = function() {
    if (isLoggedIn()) return;
    if (sessionStorage.getItem(DISMISSED_KEY)) return;
    const count = getCount() + 1;
    localStorage.setItem(KEY, count);
    if (count >= LIMIT) {
      setTimeout(showGate, 800);  // show after result renders
    }
  };
})();