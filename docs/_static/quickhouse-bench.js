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
      // Descendant, NOT child. Furo's real chain is
      //   div.content > div.article-container > article
      // so ".content > article" matches zero elements and this whole
      // function returns at the first line, silently. Verified against the
      // built HTML: "> article" = 0 matches, " article" = 1.
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

    // 5. Body code blocks get the mockup's chrome: a language bar with a copy
    //    button. Pygments emits <div class="highlight-python notranslate">
    //    <div class="highlight"><pre>; the language lives on the outer wrapper.
    (function () {
      // Descendant, NOT child. Furo's real chain is
      //   div.content > div.article-container > article
      // so ".content > article" matches zero elements and this whole
      // function returns at the first line, silently. Verified against the
      // built HTML: "> article" = 0 matches, " article" = 1.
      var article = document.querySelector(".content article");
      if (!article) return;

      var PRETTY = {
        python: "python", py: "python", pycon: "python",
        bash: "shell", console: "shell", shell: "shell", sh: "shell",
        toml: "toml", yaml: "yaml", json: "json", sql: "sql",
        text: "text", default: "text", none: "text"
      };

      article.querySelectorAll("div.highlight").forEach(function (hl) {
        if (hl.closest(".qh-codeblock") || hl.closest(".qh-slab") || hl.closest(".qh-newband")) return;

        // The language lives on the Pygments wrapper, if there is one; that
        // wrapper is also what we move into the chrome, so caption and content
        // are read from the same element.
        var outer = hl.parentElement;
        var m = outer && outer.className ? outer.className.match(/highlight-([a-z0-9+#-]+)/i) : null;
        var lang = m ? (PRETTY[m[1].toLowerCase()] || m[1].toLowerCase()) : "text";
        var host = m ? outer : hl;
        var wrap = document.createElement("div");
        wrap.className = "qh-codeblock";
        host.parentNode.insertBefore(wrap, host);

        var bar = document.createElement("div");
        bar.className = "qh-codeblock__bar";
        var label = document.createElement("span");
        label.className = "qh-codeblock__lang";
        label.textContent = lang;
        var btn = document.createElement("button");
        btn.type = "button";
        btn.className = "qh-codeblock__copy";
        btn.textContent = "copy";
        btn.setAttribute("aria-live", "polite");
        bar.appendChild(label);
        bar.appendChild(btn);

        wrap.appendChild(bar);
        wrap.appendChild(host);

        btn.addEventListener("click", function () {
          var pre = hl.querySelector("pre");
          var text = pre ? pre.innerText.replace(/\n$/, "") : "";
          var settle = function (msg, ok) {
            btn.textContent = msg;
            if (ok) btn.dataset.copied = "1";
            setTimeout(function () { btn.textContent = "copy"; delete btn.dataset.copied; }, 1400);
          };
          if (!navigator.clipboard) { settle("copy failed", false); return; }
          navigator.clipboard.writeText(text).then(
            function () { settle("copied", true); },
            function () { settle("copy failed", false); }
          );
        });
      });
    })();

    // 6. Sidebar expansion. Furo folds a nested toctree behind a hidden
    //    checkbox, so a section's own index page renders with its children
    //    collapsed. Check every checkbox on the path to the current page so
    //    the section you are inside stays open. No-op until a section
    //    actually nests — see DESIGN.md §7.
    (function () {
      var current = document.querySelector(".sidebar-tree .current-page");
      if (!current) return;
      for (var node = current; node && node !== document; node = node.parentNode) {
        if (node.tagName !== "LI") continue;
        var kids = node.children;
        for (var i = 0; i < kids.length; i++) {
          if (kids[i].classList && kids[i].classList.contains("toctree-checkbox")) {
            kids[i].checked = true;
          }
        }
      }
    })();

    // 7. Mode-card selection. Clicking marks the card and follows its anchor;
    //    a scroll-spy then moves the selection to whichever linked section is
    //    in view. Nothing is hidden — the prose stays in the document, so
    //    Ctrl-F, deep links and no-JS all keep working.
    //
    //    Only anchor cards are interactive, per DESIGN.md §4 ("Anchor
    //    children"). A grid of plain <div> cards — the benchmark's cost
    //    comparison — is a static display, and a click there must not be able
    //    to move the accent off the leading row.
    document.querySelectorAll(".qh-modes").forEach(function (grid) {
      var cards = Array.prototype.slice.call(grid.querySelectorAll("a.qh-mode"));
      if (!cards.length) return;

      function select(card) {
        cards.forEach(function (c) {
          var on = c === card;
          c.classList.toggle("qh-mode--current", on);
          if (on) c.setAttribute("aria-current", "true");
          else c.removeAttribute("aria-current");
        });
      }

      // Normalise whatever the markdown authored, so the load state and the
      // JS state agree before any interaction.
      select(cards.filter(function (c) {
        return c.classList.contains("qh-mode--current");
      })[0] || cards[0]);

      cards.forEach(function (card) {
        card.addEventListener("click", function () { select(card); });
      });

      // Spy only on cards that point at a section of this page; a grid that
      // links to other pages keeps click behaviour alone.
      var targets = cards.map(function (card) {
        var href = card.getAttribute("href") || "";
        return href.charAt(0) === "#" ? document.getElementById(href.slice(1)) : null;
      });
      if (!("IntersectionObserver" in window)) return;
      if (!targets.some(function (t) { return t; })) return;

      var spy = new IntersectionObserver(function (entries) {
        entries.forEach(function (e) {
          if (!e.isIntersecting) return;
          var i = targets.indexOf(e.target);
          if (i >= 0) select(cards[i]);
        });
      }, { rootMargin: "-25% 0px -60% 0px" });
      targets.forEach(function (t) { if (t) spy.observe(t); });
    });
  });
})();
