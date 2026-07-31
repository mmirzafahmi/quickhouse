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
  });
})();
