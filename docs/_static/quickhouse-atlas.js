/* docs/_static/quickhouse-atlas.js — hero tabs, copy buttons, nav state */
(function () {
  function ready(fn) {
    if (document.readyState !== "loading") fn();
    else document.addEventListener("DOMContentLoaded", fn);
  }

  ready(function () {
    // Landing marker — fallback for browsers without :has()
    if (document.querySelector(".qh-hero")) {
      document.body.classList.add("qh-hero-page");
    }

    // Copy buttons
    document.querySelectorAll("[data-qh-copy]").forEach(function (btn) {
      btn.addEventListener("click", function () {
        var old = btn.textContent;
        var settle = function (label, ok) {
          btn.textContent = label;
          if (ok) btn.dataset.copied = "1"; else btn.dataset.copyFailed = "1";
          setTimeout(function () {
            btn.textContent = old;
            delete btn.dataset.copied;
            delete btn.dataset.copyFailed;
          }, 1400);
        };
        if (!navigator.clipboard) { settle("copy failed", false); return; }
        navigator.clipboard.writeText(btn.getAttribute("data-qh-copy")).then(
          function () { settle("copied", true); },
          function () { settle("copy failed", false); }
        );
      });
    });

    // Hero slab tabs — click and arrow-key navigation (ARIA APG tabs pattern)
    document.querySelectorAll(".qh-slab").forEach(function (slab) {
      var tabs = Array.prototype.slice.call(slab.querySelectorAll(".qh-slab__tab"));
      var panels = Array.prototype.slice.call(slab.querySelectorAll(".qh-slab__panel"));
      function activate(i, focusTab) {
        tabs.forEach(function (t, idx) {
          var selected = idx === i;
          t.setAttribute("aria-selected", selected ? "true" : "false");
          t.tabIndex = selected ? 0 : -1;
        });
        panels.forEach(function (p) { p.removeAttribute("data-active"); });
        if (panels[i]) panels[i].setAttribute("data-active", "1");
        if (focusTab) tabs[i].focus();
      }
      tabs.forEach(function (tab, i) {
        tab.addEventListener("click", function () { activate(i, false); });
        tab.addEventListener("keydown", function (e) {
          var next;
          if (e.key === "ArrowRight" || e.key === "ArrowDown") next = (i + 1) % tabs.length;
          else if (e.key === "ArrowLeft" || e.key === "ArrowUp") next = (i - 1 + tabs.length) % tabs.length;
          else if (e.key === "Home") next = 0;
          else if (e.key === "End") next = tabs.length - 1;
          else return;
          e.preventDefault();
          activate(next, true);
        });
      });
    });

    // Highlight the current top-nav section — matched via an explicit
    // data-qh-section attribute (not by parsing href, since Sphinx's pathto()
    // renders a same-directory link as a bare filename and a self-link as "#",
    // neither of which a naive href parse can distinguish reliably).
    // A page's own filename (e.g. benchmark.html) always wins over a mere
    // ancestor-directory match (e.g. "guide"), so a page that's both nested
    // under one section and promoted to its own nav item — like
    // guide/benchmark.html — only highlights the more specific one.
    var path = window.location.pathname;
    var navLinks = Array.prototype.slice.call(document.querySelectorAll(".qh-topnav__links a[data-qh-section]"));
    var exact = navLinks.filter(function (a) {
      return new RegExp("/" + a.getAttribute("data-qh-section") + "\\.html$").test(path);
    });
    (exact.length ? exact : navLinks.filter(function (a) {
      return new RegExp("(^|/)" + a.getAttribute("data-qh-section") + "(/|\\.html$)").test(path);
    })).forEach(function (a) {
      a.classList.add("qh-current");
      a.setAttribute("aria-current", "page");
    });

    // Sync-modes selector cards (guide/sync-modes.md): filter the page down
    // to one mode's section at a time instead of just anchor-jumping to it.
    // Sphinx nests each H2 and everything under it (including H3 subsections)
    // inside one <section id="..."> element, so toggling that one element
    // per mode is enough — no manual sibling-walking needed.
    var modesNav = document.querySelector(".qh-modes");
    if (modesNav) {
      var hrefForMode = { full: "#full-refresh", incremental: "#incremental", append: "#append-bronze-landing" };
      var modeSections = {
        full: document.getElementById("full-refresh"),
        incremental: document.getElementById("incremental"),
        append: document.getElementById("append-bronze-landing"),
      };
      var cards = Array.prototype.slice.call(modesNav.querySelectorAll(".qh-mode"));
      function showMode(mode) {
        Object.keys(modeSections).forEach(function (m) {
          if (modeSections[m]) modeSections[m].style.display = m === mode ? "" : "none";
        });
        cards.forEach(function (c) {
          var isCurrent = c.getAttribute("href") === hrefForMode[mode];
          c.classList.toggle("qh-mode--current", isCurrent);
          if (isCurrent) c.setAttribute("aria-current", "page");
          else c.removeAttribute("aria-current");
        });
      }
      cards.forEach(function (card) {
        card.addEventListener("click", function (e) {
          e.preventDefault();
          var href = card.getAttribute("href");
          var mode = Object.keys(hrefForMode).filter(function (m) { return hrefForMode[m] === href; })[0];
          if (mode) showMode(mode);
        });
      });
      showMode("full");
    }

    // "/" focuses search
    document.addEventListener("keydown", function (e) {
      if (e.key === "/" && !/input|textarea/i.test(document.activeElement.tagName)) {
        var input = document.querySelector(".sidebar-search") || document.querySelector("input[name=q]");
        if (input) { e.preventDefault(); input.focus(); }
      }
    });
  });
})();
