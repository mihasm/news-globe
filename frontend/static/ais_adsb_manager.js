/**
 * Minimal AIS/ADSB Leaflet manager
 * - Fetches from /api/ais and /api/adsb using current Leaflet map bounds
 * - Draws DOTS (circle markers)
 * - Popups show fields directly from the returned JSON (including AIS.latest_body)
 */

class AISADSBManager {
  constructor({ apiBaseUrl = "/api" } = {}) {
    this.apiBaseUrl = apiBaseUrl;

    this.aisLayer = null;
    this.adsbLayer = null;

    this.isAISFetching = false;
    this.isADSBFetching = false;

    // Periodic update functionality
    this.aisUpdateInterval = null;
    this.adsbUpdateInterval = null;
    this.updateIntervalMs = 5000; // 5 seconds

    // Map movement handling
    this.mapMoveTimeout = null;
    this.mapMoveDebounceMs = 1000; // 1 second

    // Marker tracking for updates instead of clearing
    this.aisMarkers = new Map(); // key: unique identifier, value: marker
    this.adsbMarkers = new Map(); // key: unique identifier, value: marker

    // Active states
    this.aisActive = false;
    this.adsbActive = false;

    this._initLeafletLayers();
    this._setupMapEventHandlers();
  }

  _initLeafletLayers() {
    if (typeof L === "undefined") return;
    this.aisLayer = L.layerGroup();
    this.adsbLayer = L.layerGroup();

    if (window.map && window.map.addLayer) {
      this.aisLayer.addTo(window.map);
      this.adsbLayer.addTo(window.map);
    }
  }

  _setupMapEventHandlers() {
    if (!window.map) return;

    // Debounced map movement handler
    const handleMapMove = () => {
      if (this.mapMoveTimeout) {
        clearTimeout(this.mapMoveTimeout);
      }
      this.mapMoveTimeout = setTimeout(() => {
        this._handleMapBoundsChanged();
      }, this.mapMoveDebounceMs);
    };

    // Listen to various map movement events
    window.map.on('moveend', handleMapMove);
    window.map.on('zoomend', handleMapMove);
    window.map.on('dragend', handleMapMove);
  }

  _handleMapBoundsChanged() {
    // Update both services if they are active
    if (this.aisActive) {
      this.drawAIS();
    }
    if (this.adsbActive) {
      this.drawADSB();
    }
  }

  getCurrentMapBounds() {
    if (window.map && window.map.getBounds) {
      const b = window.map.getBounds();
      return {
        minLat: b.getSouth(),
        maxLat: b.getNorth(),
        minLon: b.getWest(),
        maxLon: b.getEast(),
      };
    }
    // fallback (world)
    return { minLat: -90, maxLat: 90, minLon: -180, maxLon: 180 };
  }

  async fetchJson(endpoint, bbox, extraParams = {}) {
    const params = new URLSearchParams({
      min_lat: bbox.minLat,
      max_lat: bbox.maxLat,
      min_lon: bbox.minLon,
      max_lon: bbox.maxLon,
      ...extraParams,
    });

    const res = await fetch(`${this.apiBaseUrl}/${endpoint}?${params.toString()}`);
    if (!res.ok) throw new Error(`${endpoint.toUpperCase()} API error: ${res.status}`);
    return await res.json();
  }

  // Handle API response format: {"count": N, "items": [...]}
  normalizeToList(data) {
    if (!data) return [];
    if (Array.isArray(data)) return data.filter(Boolean);
    if (typeof data === "object") {
      // Handle API response format: {"count": N, "items": [...]}
      if (Array.isArray(data.items)) return data.items.filter(Boolean);
      // If it's already a GeoJSON FeatureCollection, keep features
      if (Array.isArray(data.features)) return data.features.filter(Boolean);
      // Otherwise treat it as keyed object
      return Object.values(data).filter(Boolean);
    }
    return [];
  }

  clearAIS() {
    if (this.aisLayer) this.aisLayer.clearLayers();
    this.aisMarkers.clear();
  }

  clearADSB() {
    if (this.adsbLayer) this.adsbLayer.clearLayers();
    this.adsbMarkers.clear();
  }

  // Periodic update controls
  startAISUpdates() {
    this.stopAISUpdates(); // Clear any existing interval
    this.aisActive = true;
    // Re-add all previously seen markers to the layer so they reappear immediately
    for (const marker of this.aisMarkers.values()) {
      this.aisLayer.addLayer(marker);
    }
    this.drawAIS(); // Initial draw and position updates
    this.aisUpdateInterval = setInterval(() => {
      this.drawAIS();
    }, this.updateIntervalMs);
  }

  stopAISUpdates() {
    this.aisActive = false;
    if (this.aisUpdateInterval) {
      clearInterval(this.aisUpdateInterval);
      this.aisUpdateInterval = null;
    }
    // Clear markers from map but keep them in memory for potential reuse
    if (this.aisLayer) {
      this.aisLayer.clearLayers();
    }
    // Note: We keep this.aisMarkers Map intact so markers can be reused if service restarts
  }

  startADSBUpdates() {
    this.stopADSBUpdates(); // Clear any existing interval
    this.adsbActive = true;
    this.drawADSB(); // Initial draw
    this.adsbUpdateInterval = setInterval(() => {
      this.drawADSB();
    }, this.updateIntervalMs);
  }

  stopADSBUpdates() {
    this.adsbActive = false;
    if (this.adsbUpdateInterval) {
      clearInterval(this.adsbUpdateInterval);
      this.adsbUpdateInterval = null;
    }
    // Clear markers from map but keep them in memory for potential reuse
    if (this.adsbLayer) {
      this.adsbLayer.clearLayers();
    }
    // Note: We keep this.adsbMarkers Map intact so markers can be reused if service restarts
  }

  // ---- Popup helpers ----

  formatValue(v) {
    if (v === null || v === undefined || v === "") return "N/A";
    if (typeof v === "number") return Number.isFinite(v) ? String(v) : "N/A";
    if (typeof v === "boolean") return v ? "true" : "false";
    return (window.escapeHtml || ((s) => s))(v);
  }

  objectToTableRows(obj) {
    if (!obj || typeof obj !== "object") return "";
    const keys = Object.keys(obj);
    if (!keys.length) return "";
    // Stable-ish ordering: keep as-is; if you want alpha: keys.sort()
    return keys
      .map((k) => {
        const v = obj[k];
        const val =
          v && typeof v === "object" && !Array.isArray(v)
            ? `<pre style="margin:6px 0; white-space:pre-wrap;">${(window.escapeHtml || ((s) => s))(
                JSON.stringify(v, null, 2)
              )}</pre>`
            : this.formatValue(v);
        return `<tr><td style="padding:2px 6px; vertical-align:top;"><strong>${(window.escapeHtml || ((s) => s))(
          k
        )}</strong></td><td style="padding:2px 6px;">${val}</td></tr>`;
      })
      .join("");
  }

  buildAdsbPopup(item) {
    // Example ADSB item fields: id, icao, callsign, lat, lon, alt_baro_ft, speed_knots, heading_deg, ...
    const title = item.callsign || item.icao || item.id || "Aircraft";
    const coords = `(${this.formatValue(item.lat)}, ${this.formatValue(item.lon)})`;

    return `
      <div class="adsb-popup">
        <div style="font-weight:700; margin-bottom:6px;">${(window.escapeHtml || ((s) => s))(title)}</div>
        <div style="margin-bottom:6px;">Position: ${coords}</div>
        <table style="border-collapse:collapse; width:100%; font-size:12px;">
          ${this.objectToTableRows(item)}
        </table>
      </div>
    `;
  }

  buildAisPopup(item) {
    // Example AIS item: mmsi, name, first_seen, last_seen, last_message_type, latest_body{Latitude,Longitude,...}
    const body = item.latest_body && typeof item.latest_body === "object" ? item.latest_body : {};
    const title = item.name || body.Name || String(item.mmsi || body.UserID || "Vessel");
    const lat = body.Latitude ?? item.lat ?? item.Latitude ?? null;
    const lon = body.Longitude ?? item.lon ?? item.Longitude ?? null;

    return `
      <div class="ais-popup">
        <div style="font-weight:700; margin-bottom:6px;">${(window.escapeHtml || ((s) => s))(title)}</div>
        <div style="margin-bottom:6px;">Position: (${this.formatValue(lat)}, ${this.formatValue(lon)})</div>

        <div style="font-weight:700; margin:8px 0 4px;">Top-level</div>
        <table style="border-collapse:collapse; width:100%; font-size:12px;">
          ${this.objectToTableRows(
            Object.fromEntries(
              Object.entries(item).filter(([k]) => k !== "latest_body")
            )
          )}
        </table>

        <div style="font-weight:700; margin:10px 0 4px;">latest_body</div>
        <table style="border-collapse:collapse; width:100%; font-size:12px;">
          ${this.objectToTableRows(body)}
        </table>
      </div>
    `;
  }

  // ---- Marker helpers (DOTS) ----

  addDot(layer, lat, lon, popupHtml) {
    if (!layer || typeof L === "undefined") return;

    const nlat = Number(lat);
    const nlon = Number(lon);
    if (!Number.isFinite(nlat) || !Number.isFinite(nlon)) return;

    // Simple dot (no custom colors here; Leaflet defaults)
    const marker = L.circleMarker([nlat, nlon], {
      radius: 4,
      weight: 1,
      fillOpacity: 0.8,
    });

    if (popupHtml) marker.bindPopup(popupHtml);
    layer.addLayer(marker);
  }

  // ---- Public: fetch + draw ----

  async drawADSB() {
    if (!this.adsbLayer) return;
    if (this.isADSBFetching) return;

    this.isADSBFetching = true;
    try {
      // Only update map display if ADSB is currently active
      if (!this.adsbActive) {
        // If not active, skip the update entirely
        return;
      }

      this.clearADSB();

      const bbox = this.getCurrentMapBounds();
      const raw = await this.fetchJson("adsb", bbox);
      const list = this.normalizeToList(raw);

      for (const item of list) {
        // Support both your “flat item” format and GeoJSON (if server changes later)
        if (item && item.type === "Feature" && item.geometry?.coordinates) {
          const [lon, lat] = item.geometry.coordinates;
          const props = item.properties || {};
          this.addDot(this.adsbLayer, lat, lon, this.buildAdsbPopup(props.entities ? props.entities : props));
        } else {
          this.addDot(this.adsbLayer, item.lat, item.lon, this.buildAdsbPopup(item));
        }
      }
    } finally {
      this.isADSBFetching = false;
    }
  }

  async drawAIS() {
    if (!this.aisLayer) return;
    if (this.isAISFetching) return;

    this.isAISFetching = true;
    try {
      // Don't clear AIS markers - we'll update existing ones and add new ones

      const bbox = this.getCurrentMapBounds();
      // keep your old timeout param if backend supports it
      const raw = await this.fetchJson("ais", bbox, { timeout: 5 });
      const list = this.normalizeToList(raw);

      // Track which MMSIs we've seen in this update
      const seenMmsis = new Set();

      // Only update map display if AIS is currently active
      const shouldUpdateMap = this.aisActive;

      for (const item of list) {
        let lat, lon, mmsi;
        // Support both your “flat item” format and GeoJSON (if server changes later)
        if (item && item.type === "Feature" && item.geometry?.coordinates) {
          const [ilon, ilat] = item.geometry.coordinates;
          lon = ilon;
          lat = ilat;
          const props = item.properties || {};
          mmsi = props.mmsi || props.MMSI || props.id;
        } else {
          // Handle AIS vessel format: check last_position, then latest_body, then direct fields
          const lastPos = item.last_position || {};
          const body = item.latest_body || {};
          lat = lastPos.lat ?? body.Latitude ?? item.lat ?? item.Latitude;
          lon = lastPos.lon ?? body.Longitude ?? item.lon ?? item.Longitude;
          mmsi = item.mmsi || body.UserID || item.MMSI || item.id;
        }

        if (!mmsi || !Number.isFinite(lat) || !Number.isFinite(lon)) continue;

        seenMmsis.add(String(mmsi));

        // Check if we already have a marker for this MMSI
        const existingMarker = this.aisMarkers.get(String(mmsi));

        if (existingMarker) {
          // Update existing marker data
          existingMarker.setLatLng([lat, lon]);
          existingMarker.setPopupContent(this.buildAisPopup(item));
          // Only update map display if AIS is active
          if (shouldUpdateMap) {
            this.aisLayer.addLayer(existingMarker);
          }
        } else {
          // Create new marker
          const marker = L.circleMarker([lat, lon], {
            radius: 4,
            weight: 1,
            fillOpacity: 0.8,
          });
          marker.bindPopup(this.buildAisPopup(item));
          this.aisMarkers.set(String(mmsi), marker);
          // Only add to map display if AIS is active
          if (shouldUpdateMap) {
            this.aisLayer.addLayer(marker);
          }
        }
      }

      // Note: We don't remove old markers for AIS since ships that aren't updated
      // should remain visible (they might just not have new position reports)
    } finally {
      this.isAISFetching = false;
    }
  }
}

// Global instance + button functions
let aisAdsbManager = null;

document.addEventListener("DOMContentLoaded", () => {
  aisAdsbManager = new AISADSBManager({ apiBaseUrl: "/api" });
});

async function fetchAndDisplayAISData() {
  if (!aisAdsbManager) return;
  try {
    await aisAdsbManager.drawAIS();
  } catch (e) {
    console.error(e);
    alert("Error fetching AIS: " + (e?.message || e));
  }
}

async function fetchAndDisplayADSBData() {
  if (!aisAdsbManager) return;
  try {
    await aisAdsbManager.drawADSB();
  } catch (e) {
    console.error(e);
    alert("Error fetching ADSB: " + (e?.message || e));
  }
}

function clearAISData() {
  if (!aisAdsbManager) return;
  aisAdsbManager.clearAIS();
}

function clearADSBData() {
  if (!aisAdsbManager) return;
  aisAdsbManager.clearADSB();
}

window.AISADSBManager = AISADSBManager;
window.fetchAndDisplayAISData = fetchAndDisplayAISData;
window.fetchAndDisplayADSBData = fetchAndDisplayADSBData;
window.clearAISData = clearAISData;
window.clearADSBData = clearADSBData;
