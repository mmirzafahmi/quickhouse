/* docs/_static/quickhouse-bench.js — benchmark switch, teaser carousel, bar reveal */
(function () {
  function ready(fn) {
    if (document.readyState !== "loading") fn();
    else document.addEventListener("DOMContentLoaded", fn);
  }

  ready(function () {
    // 1. Bars grow when scrolled into view (and immediately if IO is absent).
    var barGroups = document.querySelectorAll(".qh-bars");
    if (!("IntersectionObserver" in window)) {
      barGroups.forEach(function (g) { g.classList.add("is-visible"); });
    } else {
      var io = new IntersectionObserver(function (entries) {
        entries.forEach(function (e) {
          if (e.isIntersecting) { e.target.classList.add("is-visible"); io.unobserve(e.target); }
        });
      }, { threshold: 0.25 });
      barGroups.forEach(function (g) { io.observe(g); });
    }

    // 2. Benchmark destination switch (ClickHouse / BigQuery).
    document.querySelectorAll(".qh-bench").forEach(function (bench) {
      var tabs = Array.prototype.slice.call(bench.querySelectorAll(".qh-bench__switch button"));
      var panels = Array.prototype.slice.call(bench.querySelectorAll(".qh-bench__panel"));
      function activate(i, focus) {
        tabs.forEach(function (t, idx) {
          t.setAttribute("aria-selected", idx === i ? "true" : "false");
          t.tabIndex = idx === i ? 0 : -1;
        });
        panels.forEach(function (p, idx) {
          if (idx === i) {
            p.hidden = false;
            p.querySelectorAll(".qh-bars").forEach(function (g) {
              g.classList.remove("is-visible");
              void g.offsetWidth;            // restart the grow transition
              g.classList.add("is-visible");
            });
          } else {
            p.hidden = true;
          }
        });
        if (focus) tabs[i].focus();
      }
      tabs.forEach(function (tab, i) {
        tab.addEventListener("click", function () { activate(i, false); });
        tab.addEventListener("keydown", function (e) {
          var next;
          if (e.key === "ArrowRight") next = (i + 1) % tabs.length;
          else if (e.key === "ArrowLeft") next = (i - 1 + tabs.length) % tabs.length;
          else return;
          e.preventDefault();
          activate(next, true);
        });
      });
    });

    // 3. Landing teaser carousel — auto-advances, pauses on hover/focus.
    document.querySelectorAll(".qh-teaser").forEach(function (teaser) {
      var slides = Array.prototype.slice.call(teaser.querySelectorAll(".qh-teaser__slide"));
      var dots = Array.prototype.slice.call(teaser.querySelectorAll(".qh-teaser__dot"));
      var label = teaser.querySelector(".qh-teaser__label");
      var head = teaser.querySelector(".qh-teaser__title");
      var i = 0, timer = null;
      var reduce = window.matchMedia("(prefers-reduced-motion: reduce)").matches;

      function show(n) {
        i = n;
        slides.forEach(function (s, idx) {
          s.hidden = idx !== n;
          if (idx === n) {
            s.querySelectorAll(".qh-bars").forEach(function (g) {
              g.classList.remove("is-visible");
              void g.offsetWidth;
              g.classList.add("is-visible");
            });
          }
        });
        dots.forEach(function (d, idx) { d.setAttribute("aria-selected", idx === n ? "true" : "false"); });
        if (head) head.textContent = slides[n].getAttribute("data-title") || "";
        if (label) label.textContent = slides[n].getAttribute("data-note") || "";
      }
      function start() { if (!reduce && !timer) timer = setInterval(function () { show((i + 1) % slides.length); }, 4600); }
      function stop() { clearInterval(timer); timer = null; }

      dots.forEach(function (d, idx) {
        d.addEventListener("click", function () { stop(); show(idx); start(); });
      });
      teaser.addEventListener("mouseenter", stop);
      teaser.addEventListener("mouseleave", start);
      teaser.addEventListener("focusin", stop);
      teaser.addEventListener("focusout", start);

      show(0);
      start();
    });

    // 4. Breadcrumb eyebrow above the page title, derived from the sidebar
    //    caption the current page sits under (no per-page markup needed).
    (function () {
      // Descendant, not child: Furo wraps the article in .article-container,
      // so ".content > article" never matches and the crumb silently vanishes.
      var article = document.querySelector(".content article");
      if (!article || document.querySelector(".qh-hero")) return;
      var h1 = article.querySelector("h1");
      if (!h1 || h1.previousElementSibling && h1.previousElementSibling.classList.contains("qh-crumb")) return;

      var current = document.querySelector(".sidebar-tree .current-page > .reference");
      if (!current) return;

      // Walk up to the <ul> that this page's caption precedes.
      var list = current.closest("ul");
      while (list && list.parentElement && list.parentElement.tagName === "LI") list = list.parentElement.closest("ul");
      var caption = null, prev = list && list.previousElementSibling;
      while (prev) {
        if (prev.classList && prev.classList.contains("caption")) { caption = prev; break; }
        prev = prev.previousElementSibling;
      }
      if (!caption) return;

      var section = (caption.textContent || "").trim();
      var page = (current.textContent || "").trim();
      if (!section || !page) return;

      var crumb = document.createElement("div");
      crumb.className = "qh-crumb";
      crumb.textContent = section + " ";
      var sep = document.createElement("span");
      sep.textContent = "/";
      crumb.appendChild(sep);
      crumb.appendChild(document.createTextNode(" " + page));
      h1.parentNode.insertBefore(crumb, h1);
    })();
  });
})();
