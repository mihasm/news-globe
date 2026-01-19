/**
 * Map Interface - Abstraction layer for switching between 2D (Leaflet) and 3D (CesiumJS)
 */

class MapInterface {
    constructor() {
        this.mode = '2d';  // '2d' or '3d'
        this.leafletMap = null;
        this.cesiumMap = null;
        this.markers2D = [];  // Track Leaflet markers
        this.markerData = {};  // Store marker data for both modes
        
        // Selection state
        this.selectedLocationKey = null;
        this.sidebarVisible = false;
        
        // Callbacks
        this.onModeChange = null;
        this.onMarkerSelect = null;
        this.onLocationSelect = null;  // Called when a location is selected
    }
    
    /**
     * Initialize the map interface
     * @param {object} leafletMap - Existing Leaflet map instance
     * @param {string} cesiumContainerId - ID of the Cesium container element
     */
    init(leafletMap, cesiumContainerId = 'cesiumContainer') {
        this.leafletMap = leafletMap;
        this.cesiumContainerId = cesiumContainerId;
        
        // Hide Cesium container initially (2D mode default)
        const cesiumContainer = document.getElementById(cesiumContainerId);
        if (cesiumContainer) {
            cesiumContainer.style.display = 'none';
        }
        
        // Initialize LayerManager when available (will be initialized after Cesium is created)
        // LayerManager.init() will be called from _initCesium() when Cesium is created

        return this;
    }
    
    /**
     * Initialize Cesium viewer (lazy initialization)
     */
    _initCesium() {
        if (!this.cesiumMap && window.CesiumMap) {
            this.cesiumMap = new CesiumMap(this.cesiumContainerId);
            this.cesiumMap.init();
            
            // Initialize LayerManager now that both maps are available
            if (window.layerManager) {
                window.layerManager.init(this.leafletMap, this.cesiumMap, this);
                // Apply current configuration to Leaflet (2D mode is active)
                window.layerManager.applyToLeaflet();
            }
            
            // Sync UI checkboxes with CesiumMap defaults (cylinders enabled, others disabled)
            const vizCircles = document.getElementById('viz_circles');
            const vizCylinders = document.getElementById('viz_cylinders');
            const vizPolylines = document.getElementById('viz_polylines');
            if (vizCircles && vizCylinders && vizPolylines) {
                vizCircles.checked = this.cesiumMap.showCircles;
                vizCylinders.checked = this.cesiumMap.showCylinders;
                vizPolylines.checked = this.cesiumMap.showPolylines;
            }
            
            // Setup selection callback - wire to selectLocation
            this.cesiumMap.onMarkerSelect = (key, marker) => {
                // Select the location (updates sidebar) - always open sidebar on click in 3D
                this.selectLocation(key, { openSidebar: true });
                
                // Also call legacy callback if set
                if (this.onMarkerSelect) {
                    this.onMarkerSelect(key, marker);
                }
            };
            
            // Setup deselection callback
            this.cesiumMap.onMarkerDeselect = () => {
                this.deselectLocation();
            };
            
            // Sync existing locations from store to Cesium
            if (window.locationStore) {
                const locations = window.locationStore.getAllLocations();
                for (const [key, location] of Object.entries(locations)) {
                    this.cesiumMap.addMarker(key, location.lat, location.lng, {
                        html: location.items[0]?.html || '',
                        tooltip: location.locationName || key,
                        source: location.items[0]?.source,
                        count: location.items.length
                    });
                }
            }
        }
    }
    
    /**
     * Toggle between 2D and 3D modes
     * @returns {string} The new mode
     */
    toggle() {
        if (this.mode === '2d') {
            this.setMode('3d');
        } else {
            this.setMode('2d');
        }
        return this.mode;
    }
    
    /**
     * Set the map mode
     * @param {string} mode - '2d' or '3d'
     */
    setMode(mode) {
        if (mode !== '2d' && mode !== '3d') {
            console.error('Invalid mode:', mode);
            return;
        }
        
        const previousMode = this.mode;
        this.mode = mode;
        
        // Store current selection and sidebar state
        const preservedSelection = this.selectedLocationKey;
        const preservedSidebarVisible = this.sidebarVisible;
        
        // Get selected location data for flying to it
        let selectedLocationData = null;
        if (preservedSelection && window.locationStore) {
            selectedLocationData = window.locationStore.getLocation(preservedSelection);
        }
        
        if (mode === '3d') {
            // Switch to 3D
            this._initCesium();
            
            // Apply layers to Cesium (synchronize from current configuration)
            if (window.layerManager) {
                window.layerManager.applyToCesium();
            }
            
            // Determine where to fly to
            let targetLat, targetLng;
            if (selectedLocationData) {
                // Fly to selected location
                targetLat = selectedLocationData.lat;
                targetLng = selectedLocationData.lng;
            } else {
                // Use current Leaflet view
                const center = this.leafletMap.getCenter();
                targetLat = center.lat;
                targetLng = center.lng;
            }
            
            const zoom = this.leafletMap.getZoom();
            
            // Hide Leaflet, show Cesium
            document.getElementById('map').style.display = 'none';
            document.getElementById(this.cesiumContainerId).style.display = 'block';
            
            // Set Cesium view
            const height = this._zoomToHeight(zoom);
            this.cesiumMap.setView(targetLat, targetLng, height);
            this.cesiumMap.refresh();
            
            // Restore selection in 3D
            if (preservedSelection) {
                this.cesiumMap.selectedKey = preservedSelection;
            }
            
        } else {
            // Switch to 2D
            let targetLat, targetLng, zoom;
            
            if (this.cesiumMap) {
                // Get current Cesium view
                const center = this.cesiumMap.getCenter();
                const height = this.cesiumMap._getCurrentHeight();
                zoom = this._heightToZoom(height);
                
                // Use selected location if available
                if (selectedLocationData) {
                    targetLat = selectedLocationData.lat;
                    targetLng = selectedLocationData.lng;
                } else {
                    targetLat = center.lat;
                    targetLng = center.lng;
                }
                
                // Hide Cesium, show Leaflet
                document.getElementById(this.cesiumContainerId).style.display = 'none';
                document.getElementById('map').style.display = 'block';
                
                // Apply layers to Leaflet (synchronize from current configuration)
                if (window.layerManager) {
                    window.layerManager.applyToLeaflet();
                }
                
                // Set Leaflet view
                this.leafletMap.setView([targetLat, targetLng], zoom);
            } else {
                // Just show Leaflet
                document.getElementById(this.cesiumContainerId).style.display = 'none';
                document.getElementById('map').style.display = 'block';
                
                // Apply layers to Leaflet (synchronize from current configuration)
                if (window.layerManager) {
                    window.layerManager.applyToLeaflet();
                }
            }
        }
        
        // Restore sidebar state
        if (preservedSidebarVisible && window.newsSidebar) {
            // Sidebar should remain visible with the same content
            if (preservedSelection && window.locationStore) {
                // Use selectLocation to track the key for auto-refresh
                window.newsSidebar.selectLocation(preservedSelection);
            }
        }
        
        // Trigger callback
        if (this.onModeChange) {
            this.onModeChange(mode, previousMode);
        }
    }
    
    /**
     * Convert Leaflet zoom level to Cesium camera height
     * Linear conversion with zoom out factor to make Cesium appear further away
     */
    _zoomToHeight(zoom) {
        // Linear conversion: height = base / (2^zoom) * factor
        // Factor makes Cesium appear further away (higher camera) for same zoom level
        const zoomOutFactor = 4;  // Increase to zoom out more, decrease to zoom in more
        return (40000000 / Math.pow(2, zoom)) * zoomOutFactor;
    }
    
    /**
     * Convert Cesium camera height to Leaflet zoom level
     * Inverse of _zoomToHeight - must use same zoomOutFactor
     */
    _heightToZoom(height) {
        // Inverse conversion: zoom = log2(base * factor / height)
        // Must use same zoomOutFactor as _zoomToHeight for bidirectional conversion
        const zoomOutFactor = 4;  // Must match _zoomToHeight
        return Math.log2((40000000 * zoomOutFactor) / height);
    }
    
    /**
     * Get the current mode
     * @returns {string} '2d' or '3d'
     */
    getMode() {
        return this.mode;
    }
    
    /**
     * Add a marker to the map
     * @param {string} id - Unique marker ID
     * @param {number} lat - Latitude
     * @param {number} lng - Longitude
     * @param {object} data - Marker data (html, tooltip, etc.)
     * @param {object} options - Additional options
     */
    addMarker(id, lat, lng, data, options = {}) {
        // Store marker data for potential mode switching
        this.markerData[id] = {
            lat: lat,
            lng: lng,
            ...data,
            timestamp: Date.now()
        };
        
        // Only add to Cesium when in 3D mode
        // 2D markers are handled directly by map.js (not through this interface)
        if (this.mode === '3d' && this.cesiumMap) {
            this.cesiumMap.addMarker(id, lat, lng, data);
            this.cesiumMap.refresh();
        }
    }
    
    /**
     * Fly to a location
     * @param {number} lat - Latitude
     * @param {number} lng - Longitude
     * @param {number} zoom - Zoom level (for 2D) or height (for 3D)
     */
    flyTo(lat, lng, zoom = null) {
        if (this.mode === '2d') {
            if (zoom) {
                this.leafletMap.flyTo([lat, lng], zoom);
            } else {
                this.leafletMap.flyTo([lat, lng]);
            }
        } else {
            if (this.cesiumMap) {
                const height = zoom ? this._zoomToHeight(zoom) : null;
                this.cesiumMap.flyTo(lat, lng, height);
            }
        }
    }
    
    /**
     * Get the current map center
     * @returns {object} {lat, lng}
     */
    getCenter() {
        if (this.mode === '2d') {
            const center = this.leafletMap.getCenter();
            return { lat: center.lat, lng: center.lng };
        } else {
            if (this.cesiumMap) {
                return this.cesiumMap.getCenter();
            }
        }
        return { lat: 0, lng: 0 };
    }
    
    /**
     * Set visualization options (for 3D mode)
     * @param {object} options - {circles, cylinders, polylines}
     */
    setVisualization(options) {
        if (this.cesiumMap) {
            this.cesiumMap.setVisualization(options);
        }
    }
    
    /**
     * Clear all markers
     */
    clearMarkers() {
        this.markerData = {};
        
        if (this.mode === '3d' && this.cesiumMap) {
            this.cesiumMap.clearMarkers();
        }
        // Note: 2D markers are managed by the existing markers cluster group
    }
    
    /**
     * Refresh the map visualization
     */
    refresh() {
        if (this.mode === '3d' && this.cesiumMap) {
            this.cesiumMap.refresh();
        }
        // 2D map doesn't need explicit refresh
    }
    
    /**
     * Select a location and update sidebar
     * @param {string} locationKey - The location key to select
     * @param {object} options - { openSidebar: boolean, flyTo: boolean }
     */
    selectLocation(locationKey, options = {}) {
        const { openSidebar = true, flyTo = false } = options;
        
        this.selectedLocationKey = locationKey;
        
        // Get location data from store
        const locationData = window.locationStore ? window.locationStore.getLocation(locationKey) : null;
        
        if (!locationData) {
            console.warn('No location data found for key:', locationKey);
            return;
        }
        
        // Update sidebar if enabled
        if (openSidebar && window.newsSidebar) {
            // Use selectLocation to track the key for auto-refresh
            window.newsSidebar.selectLocation(locationKey);
            this.sidebarVisible = true;
            
            // Sync sidebar button state
            if (window.syncSidebarButtonState) {
                window.syncSidebarButtonState(true);
            }
        }
        
        // Fly to location if requested
        if (flyTo) {
            this.flyTo(locationData.lat, locationData.lng);
        }
        
        // Highlight in 3D mode
        if (this.mode === '3d' && this.cesiumMap) {
            this.cesiumMap.selectedKey = locationKey;
        }
        
        // Trigger callback
        if (this.onLocationSelect) {
            this.onLocationSelect(locationKey, locationData);
        }
    }
    
    /**
     * Deselect the current location
     */
    deselectLocation() {
        this.selectedLocationKey = null;
        
        if (this.mode === '3d' && this.cesiumMap) {
            this.cesiumMap.selectedKey = null;
        }
    }
    
    /**
     * Get the currently selected location key
     * @returns {string|null}
     */
    getSelectedLocation() {
        return this.selectedLocationKey;
    }
    
    /**
     * Toggle sidebar visibility
     */
    toggleSidebar() {
        if (window.newsSidebar) {
            window.newsSidebar.toggle();
            this.sidebarVisible = window.newsSidebar.isVisible;
        }
    }
    
    /**
     * Show sidebar with current selection
     */
    showSidebar() {
        if (window.newsSidebar) {
            if (this.selectedLocationKey) {
                // Use selectLocation to track the key for auto-refresh
                window.newsSidebar.selectLocation(this.selectedLocationKey);
            }
            window.newsSidebar.show();
            this.sidebarVisible = true;
            
            // Sync sidebar button state
            if (window.syncSidebarButtonState) {
                window.syncSidebarButtonState(true);
            }
        }
    }
    
    /**
     * Hide sidebar
     */
    hideSidebar() {
        if (window.newsSidebar) {
            window.newsSidebar.hide();
            this.sidebarVisible = false;
            
            // Sync sidebar button state
            if (window.syncSidebarButtonState) {
                window.syncSidebarButtonState(false);
            }
        }
    }
}

// Create global instance
window.mapInterface = new MapInterface();
