/* quickhouse "Console" theme — live hero controls + sync-modes filter
   (progressive enhancement). Regular code blocks keep sphinx-copybutton.
   The authoritative quickhouse-console.css is left untouched — anything CSS
   can't express (cursor, hidden panels/sections) is set here. */
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

  // ---- hero code panel: tab strip (full refresh / incremental / CLI) ----
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

  // ---- sync-modes cards: filter the page to the chosen mode ----
  // Each card links to a section anchor (#full-refresh, #incremental, ...).
  // Selecting a card shows that section (and its right-sidebar TOC entry) and
  // hides the sibling modes'. Sections not referenced by any card (e.g.
  // "Staging tables") are general and always stay visible.
  function modeTargets(card) {
    var href = card.getAttribute("href") || "";
    if (href.charAt(0) !== "#") return {};
    var id = decodeURIComponent(href.slice(1));
    var anchor = document.getElementById(id);
    var section = anchor ? (anchor.closest("section") || anchor) : null;
    var tocLink = document.querySelector('.toc-tree a[href="#' + id + '"]');
    return { section: section, tocItem: tocLink ? tocLink.closest("li") : null };
  }

  function selectMode(card) {
    var group = card.closest(".qh-modes");
    if (!group) return;
    group.querySelectorAll(".qh-mode").forEach(function (c) {
      var on = c === card;
      c.classList.toggle("qh-mode--current", on);
      c.setAttribute("aria-selected", String(on));
      var t = modeTargets(c);
      // toggle both `hidden` and inline display so no theme rule can override it
      if (t.section) { t.section.hidden = !on; t.section.style.display = on ? "" : "none"; }
      if (t.tocItem) { t.tocItem.hidden = !on; t.tocItem.style.display = on ? "" : "none"; }
    });
  }

  // Returns true if the target was one of our controls (so we can preventDefault).
  function activate(target) {
    var mode = target.closest(".qh-mode");
    if (mode && mode.closest(".qh-modes")) { selectMode(mode); return true; }

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
    if (e.target.matches(".qh-mode, [data-tab], .qh-panel__tabs > span, [data-clipboard]")) {
      if (activate(e.target)) e.preventDefault();
    }
  });

  document.addEventListener("DOMContentLoaded", function () {
    // hero tab strip + copy label: make them read as interactive
    document
      .querySelectorAll(".qh-panel__tabs [data-tab], .qh-panel__tabs > span")
      .forEach(function (el) {
        el.style.cursor = "pointer";
        el.setAttribute("role", "button");
        if (!el.hasAttribute("tabindex")) el.setAttribute("tabindex", "0");
      });

    // sync-modes: apply the initial filter (the pre-marked card, else the first)
    document.querySelectorAll(".qh-modes").forEach(function (group) {
      var current = group.querySelector(".qh-mode--current") || group.querySelector(".qh-mode");
      if (current) selectMode(current);
    });
  });
})();
