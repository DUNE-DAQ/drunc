/**
 * Pan/zoom viewer for inline SVG diagrams, with an optional fixed legend overlay.
 *
 * Container markup (see docs/gallery_generator.py::svg_viewer):
 *   <div class="svg-viewer" data-src="..." data-legend="...">
 *     <div class="svg-viewer-toolbar">...</div>
 *     <div class="svg-viewer-canvas"></div>
 *     <div class="svg-viewer-legend"></div>
 *   </div>
 *
 * The legend lives outside the pan/zoom transform, so it never moves.
 */
(function () {
  const MIN_SCALE = 0.2;
  const MAX_SCALE = 8;
  const ZOOM_STEP = 1.2;

  function initViewer(container) {
    if (container.dataset.svgViewerReady) {
      return;
    }
    container.dataset.svgViewerReady = "true";

    const canvas = container.querySelector(".svg-viewer-canvas");
    const legendSlot = container.querySelector(".svg-viewer-legend");
    const searchInput = container.querySelector(".svg-viewer-search");
    const searchResults = container.querySelector(".svg-viewer-search-results");
    const toolbar = container.querySelector(".svg-viewer-toolbar");
    const src = container.dataset.src;
    const legendSrc = container.dataset.legend;

    const state = { x: 0, y: 0, scale: 1 };
    let svgEl = null;
    let naturalWidth = 0;
    let naturalHeight = 0;
    let dragging = false;
    let lastX = 0;
    let lastY = 0;
    let nodeIndex = [];
    let highlightedNode = null;

    function applyTransform() {
      if (!svgEl) {
        return;
      }
      // Resize the SVG itself (true vector re-layout) rather than CSS-scaling it,
      // so zooming stays crisp instead of stretching a rasterized layer.
      svgEl.style.width = `${naturalWidth * state.scale}px`;
      svgEl.style.height = `${naturalHeight * state.scale}px`;
      svgEl.style.transform = `translate(${state.x}px, ${state.y}px)`;
    }

    function resetView() {
      state.x = 0;
      state.y = 0;
      state.scale = 1;
      applyTransform();
    }

    function zoomBy(factor, originX, originY) {
      const newScale = Math.min(MAX_SCALE, Math.max(MIN_SCALE, state.scale * factor));
      if (newScale === state.scale) {
        return;
      }
      // Keep the point under the cursor stationary while zooming.
      const rect = canvas.getBoundingClientRect();
      const cx = originX !== undefined ? originX - rect.left : rect.width / 2;
      const cy = originY !== undefined ? originY - rect.top : rect.height / 2;
      state.x = cx - ((cx - state.x) * newScale) / state.scale;
      state.y = cy - ((cy - state.y) * newScale) / state.scale;
      state.scale = newScale;
      applyTransform();
    }

    fetch(src)
      .then((response) => response.text())
      .then((svgText) => {
        canvas.innerHTML = svgText;
        svgEl = canvas.querySelector("svg");
        if (!svgEl) {
          return;
        }
        // Read the SVG's true intrinsic size before any theme CSS shrinks it.
        const viewBox = svgEl.viewBox && svgEl.viewBox.baseVal;
        naturalWidth = parseFloat(svgEl.getAttribute("width")) || (viewBox && viewBox.width) || 800;
        naturalHeight = parseFloat(svgEl.getAttribute("height")) || (viewBox && viewBox.height) || 600;
        svgEl.style.maxWidth = "none";
        svgEl.style.transformOrigin = "0 0";
        applyTransform();

        nodeIndex = Array.from(svgEl.querySelectorAll("g.node"))
          .map((node) => {
            const title = node.querySelector("title");
            return title ? { node, name: title.textContent.trim() } : null;
          })
          .filter(Boolean);
      });

    if (legendSrc && legendSlot) {
      fetch(legendSrc)
        .then((response) => response.text())
        .then((svgText) => {
          legendSlot.innerHTML = svgText;
        });
    }

    canvas.addEventListener("wheel", (event) => {
      event.preventDefault();
      const factor = event.deltaY < 0 ? ZOOM_STEP : 1 / ZOOM_STEP;
      zoomBy(factor, event.clientX, event.clientY);
    }, { passive: false });

    canvas.addEventListener("mousedown", (event) => {
      dragging = true;
      lastX = event.clientX;
      lastY = event.clientY;
      canvas.classList.add("is-grabbing");
    });

    window.addEventListener("mousemove", (event) => {
      if (!dragging) {
        return;
      }
      state.x += event.clientX - lastX;
      state.y += event.clientY - lastY;
      lastX = event.clientX;
      lastY = event.clientY;
      applyTransform();
    });

    window.addEventListener("mouseup", () => {
      dragging = false;
      canvas.classList.remove("is-grabbing");
    });

    container.querySelectorAll(".svg-viewer-toolbar button").forEach((button) => {
      button.addEventListener("click", () => {
        const action = button.dataset.action;
        if (action === "zoom-in") {
          zoomBy(ZOOM_STEP);
        } else if (action === "zoom-out") {
          zoomBy(1 / ZOOM_STEP);
        } else if (action === "reset") {
          resetView();
        }
      });
    });

    // Land a matched node clear of the toolbar/legend, whatever their actual
    // rendered size is, clamped so it stays sane on small canvases too.
    function computeFocusAnchor() {
      const canvasRect = canvas.getBoundingClientRect();
      const toolbarHeight = toolbar ? toolbar.getBoundingClientRect().height : 0;
      const legendRect = legendSlot ? legendSlot.getBoundingClientRect() : null;
      const legendVisible = legendRect && legendRect.width > 0;

      const clearY = (legendVisible ? Math.max(toolbarHeight, legendRect.height) : toolbarHeight) + 24;
      const clearX = (legendVisible ? legendRect.width : 0) + 24;

      return {
        x: Math.min(clearX, canvasRect.width * 0.4),
        y: Math.min(clearY, canvasRect.height * 0.4),
      };
    }

    function focusOnNode(match) {
      if (!svgEl) {
        return;
      }
      const nodeCTM = match.node.getScreenCTM();
      if (!nodeCTM) {
        return;
      }
      // Ask the browser exactly where the node is on screen right now (this
      // accounts for the viewBox, any Graphviz wrapper transforms, and our
      // own pan/zoom), instead of recomputing that mapping ourselves - that
      // manual math is what made snapping unreliable at some zoom levels.
      const bbox = match.node.getBBox();
      const centerPoint = svgEl.createSVGPoint();
      centerPoint.x = bbox.x + bbox.width / 2;
      centerPoint.y = bbox.y + bbox.height / 2;
      const screenPoint = centerPoint.matrixTransform(nodeCTM);

      const canvasRect = canvas.getBoundingClientRect();
      const anchor = computeFocusAnchor();
      const dx = canvasRect.left + anchor.x - screenPoint.x;
      const dy = canvasRect.top + anchor.y - screenPoint.y;

      // Leave the zoom level untouched so the jump feels gentle.
      state.x += dx;
      state.y += dy;
      applyTransform();

      if (highlightedNode) {
        highlightedNode.classList.remove("svg-viewer-highlight");
      }
      highlightedNode = match.node;
      highlightedNode.classList.add("svg-viewer-highlight");
    }

    function renderSearchResults(query) {
      searchResults.innerHTML = "";
      if (!query) {
        searchResults.hidden = true;
        return;
      }

      const lowerQuery = query.toLowerCase();
      const matches = nodeIndex
        .filter((entry) => entry.name.toLowerCase().includes(lowerQuery))
        .slice(0, 8);

      if (matches.length === 0) {
        searchResults.innerHTML = '<div class="svg-viewer-search-empty">No matches</div>';
        searchResults.hidden = false;
        return;
      }

      matches.forEach((match) => {
        const item = document.createElement("button");
        item.type = "button";
        item.className = "svg-viewer-search-item";
        item.textContent = match.name;
        item.addEventListener("click", () => {
          focusOnNode(match);
          searchResults.hidden = true;
        });
        searchResults.appendChild(item);
      });
      searchResults.hidden = false;
    }

    if (searchInput) {
      searchInput.addEventListener("input", () => {
        renderSearchResults(searchInput.value.trim());
      });

      searchInput.addEventListener("keydown", (event) => {
        if (event.key !== "Enter") {
          return;
        }
        const query = searchInput.value.trim().toLowerCase();
        const firstMatch = nodeIndex.find((entry) => entry.name.toLowerCase().includes(query));
        if (firstMatch) {
          focusOnNode(firstMatch);
          searchResults.hidden = true;
        }
      });
    }
  }

  function initAllViewers() {
    document.querySelectorAll(".svg-viewer").forEach(initViewer);
  }

  if (window.document$) {
    // MkDocs Material instant-navigation hook: re-run on every page swap.
    window.document$.subscribe(initAllViewers);
  } else {
    document.addEventListener("DOMContentLoaded", initAllViewers);
  }
})();
