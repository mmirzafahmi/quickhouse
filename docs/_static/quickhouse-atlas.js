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
        navigator.clipboard.writeText(btn.getAttribute("data-qh-copy")).then(function () {
          var old = btn.textContent;
          btn.textContent = "copied";
          btn.dataset.copied = "1";
          setTimeout(function () {
            btn.textContent = old;
            delete btn.dataset.copied;
          }, 1400);
        });
      });
    });

    // Hero slab tabs
    document.querySelectorAll(".qh-slab").forEach(function (slab) {
      var tabs = slab.querySelectorAll(".qh-slab__tab");
      var panels = slab.querySelectorAll(".qh-slab__panel");
      tabs.forEach(function (tab, i) {
        tab.addEventListener("click", function () {
          tabs.forEach(function (t) { t.setAttribute("aria-selected", "false"); });
          panels.forEach(function (p) { p.removeAttribute("data-active"); });
          tab.setAttribute("aria-selected", "true");
          if (panels[i]) panels[i].setAttribute("data-active", "1");
        });
      });
    });

    // Highlight the current top-nav section
    var path = window.location.pathname;
    document.querySelectorAll(".qh-topnav__links a").forEach(function (a) {
      var href = a.getAttribute("href") || "";
      var slug = href.replace(/\.\.\//g, "").replace(/\.html$/, "").split("/")[0];
      if (slug && path.indexOf("/" + slug) !== -1) a.classList.add("qh-current");
    });

    // "/" focuses search
    document.addEventListener("keydown", function (e) {
      if (e.key === "/" && !/input|textarea/i.test(document.activeElement.tagName)) {
        var input = document.querySelector(".sidebar-search") || document.querySelector("input[name=q]");
        if (input) { e.preventDefault(); input.focus(); }
      }
    });
  });
})();
