const eventSource = new EventSource(`/events?since=${STATE_VERSION}`);
eventSource.onmessage = () => {
  location.reload();
};
// The dataflow dying takes this page's event stream with it. Tell the
// operator on screen instead of leaving a silently frozen page.
const CONNECTION_LOST_AFTER_MS = 4000;
let connectionLostTimer = null;
eventSource.onopen = () => {
  clearTimeout(connectionLostTimer);
  connectionLostTimer = null;
  document.getElementById("connection-lost").style.display = "none";
};
eventSource.onerror = () => {
  if (connectionLostTimer) return;
  connectionLostTimer = setTimeout(async () => {
    connectionLostTimer = null;
    if (eventSource.readyState === EventSource.OPEN) return;
    if (EMBEDDED) return; // The mode page owns session status and navigation.
    if (window.quitRequestPending) return;
    document.getElementById("connection-lost").style.display = "flex";
    // The launcher outlives the dataflow: it holds the failure detail.
    try {
      const status = await (await fetch(LAUNCHER_URL + "status",
                                        {cache: "no-store"})).json();
      if (status.session_id === LAUNCHER_SESSION_ID &&
          status.session_state === "idle" && !status.busy) {
        window.location.href = LAUNCHER_URL + "menu";
      } else if (status.session_state === "error" ||
                 (!status.session_state &&
                  (status.state === "error" || status.state === "stopped"))) {
        window.location.href = LAUNCHER_URL;
      }
    } catch (e) {
      // No launcher (started by hand): keep showing the banner.
    }
  }, CONNECTION_LOST_AFTER_MS);
};
