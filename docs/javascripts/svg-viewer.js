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
    const src = container.dataset.src;
    const legendSrc = container.dataset.legend;

    const state = { x: 0, y: 0, scale: 1 };
    let svgEl = null;
    let naturalWidth = 0;
    let naturalHeight = 0;
    let dragging = false;
    let lastX = 0;
    let lastY = 0;

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
