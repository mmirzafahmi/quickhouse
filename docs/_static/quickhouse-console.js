/* quickhouse "Console" theme — live hero controls (progressive enhancement).
   Regular code blocks get their copy button from sphinx-copybutton. This wires
   the hand-authored hero: the install button, the tab strip
   (full refresh / incremental / CLI), and the code panel's "copy" label.
   The authoritative quickhouse-console.css is left untouched — anything visual
   that CSS can't express (cursor, hidden panels) is set here. */
(function () {
  "use strict";

  function flash(el, ok) {
    if (!el.hasAttribute("data-label")) el.setAttribute("data-label", el.textContent);
    el.textContent = ok ? "copied ✓" : "press ⌘/Ctrl-C";
    el.classList.add("is-copied");
    setTimeout(function () {
      el.textContent = el.getAttribute("data-label");
      el.classList.remove("is-copied");
    }, 1400);
  }

  function copy(text, el) {
    if (navigator.clipboard && navigator.clipboard.writeText) {
      navigator.clipboard.writeText(text).then(
        function () { flash(el, true); },
        function () { flash(el, false); }
      );
    } else {
      flash(el, false);
    }
  }

  function selectTab(tab) {
    var panel = tab.closest(".qh-panel");
    if (!panel) return;
    var name = tab.getAttribute("data-tab");
    panel.querySelectorAll("[data-tab]").forEach(function (t) {
      t.setAttribute("aria-selected", String(t === tab));
    });
    panel.querySelectorAll(".qh-tabpanel").forEach(function (p) {
      p.hidden = p.getAttribute("data-panel") !== name;
    });
  }

  // Returns true if the target was one of our controls (so we can preventDefault).
  function activate(target) {
    var tab = target.closest("[data-tab]");
    if (tab) { selectTab(tab); return true; }

    var btn = target.closest("[data-clipboard]");
    if (btn) { copy(btn.getAttribute("data-clipboard"), btn); return true; }

    if (target.matches(".qh-panel__tabs > span") && /copy/i.test(target.textContent)) {
      var panel = target.closest(".qh-panel");
      var pre = panel && (panel.querySelector(".qh-tabpanel:not([hidden]) pre") ||
                          panel.querySelector("pre"));
      if (pre) copy(pre.innerText, target);
      return true;
    }
    return false;
  }

  document.addEventListener("click", function (e) {
    if (activate(e.target)) e.preventDefault();
  });

  document.addEventListener("keydown", function (e) {
    if (e.key !== "Enter" && e.key !== " ") return;
    if (e.target.matches("[data-tab], .qh-panel__tabs > span, [data-clipboard]")) {
      if (activate(e.target)) e.preventDefault();
    }
  });

  // Make the hand-authored controls read as interactive (keyboard + cursor).
  document.addEventListener("DOMContentLoaded", function () {
    document
      .querySelectorAll(".qh-panel__tabs [data-tab], .qh-panel__tabs > span")
      .forEach(function (el) {
        el.style.cursor = "pointer";
        el.setAttribute("role", "button");
        if (!el.hasAttribute("tabindex")) el.setAttribute("tabindex", "0");
      });
  });
})();
