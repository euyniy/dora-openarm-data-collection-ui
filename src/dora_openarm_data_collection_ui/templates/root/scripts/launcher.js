const EMBEDDED = window.parent !== window;
let parentOrigin = null;
window.addEventListener("message", event => {
  if (EMBEDDED && event.source === window.parent &&
      event.data?.type === "openarm:connected" &&
      event.data?.session_id === LAUNCHER_SESSION_ID) {
    parentOrigin = event.origin;
  }
});
function requestParentEnd() {
  if (parentOrigin) {
    window.parent.postMessage({type: "openarm:end", session_id: LAUNCHER_SESSION_ID}, parentOrigin);
  }
}
// Embedded task actions must not add entries to the browser's Back stack.
async function submitAction(form) {
  if (!EMBEDDED) {
    form.submit();
    return;
  }
  if (form.id === "quit-form") {
    requestParentEnd();
    return;
  }
  if (window.actionPending) return;
  window.actionPending = true;
  try {
    const response = await fetch(form.action, {method: "POST", body: new FormData(form)});
    if (!response.ok) throw new Error("Action failed");
    location.replace(response.url);
  } catch (error) {
    window.actionPending = false;
    window.alert("操作を確認できませんでした。画面の状態を確認してください。");
  }
}
if (EMBEDDED) {
  document.addEventListener("submit", event => {
    event.preventDefault();
    event.stopImmediatePropagation();
    submitAction(event.target);
  }, true);
  document.addEventListener("click", event => {
    const link = event.target.closest("a");
    if (link && link.target !== "_blank") {
      event.preventDefault();
      requestParentEnd();
    }
  });
}
