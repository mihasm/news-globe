/**
 * LayerManager - Unified layer management for Leaflet and Cesium
 * Maintains layer state and applies it to both 2D and 3D maps
 */

class LayerManager {
    constructor() {
        // Layer configuration state (will be updated from NewsGlobeConfig when available)
        this.config = {
            baseLayer: window.NewsGlobeConfig?.defaultBaseLayer || 'osm',  // Default base layer
            weatherLayer: 'none'            // Weather overlay
        };
        
        // References to map instances (set via init)
        this.leafletMap = null;
        this.cesiumMap = null;
        this.mapInterface = null;
        
        // Leaflet layer references
        this.leafletBaseLayer = null;
        this.leafletWeatherLayer = null;
        
        // Cesium layer references (managed by CesiumMap)
        // We'll call methods on CesiumMap instead of managing directly
    }
    
    /**
     * Initialize the layer manager with map references
     * @param {object} leafletMap - Leaflet map instance (required)
     * @param {object} cesiumMap - Cesium map instance (optional, can be set later)
     * @param {object} mapInterface - MapInterface instance (optional, can be set later)
     */
    init(leafletMap, cesiumMap, mapInterface) {
        if (leafletMap) {
            this.leafletMap = leafletMap;
        }
        if (cesiumMap) {
            this.cesiumMap = cesiumMap;
        }
        if (mapInterface) {
            this.mapInterface = mapInterface;
        }

        // Update config from NewsGlobeConfig if available
        if (window.NewsGlobeConfig && window.NewsGlobeConfig.defaultBaseLayer) {
            this.config.baseLayer = window.NewsGlobeConfig.defaultBaseLayer;
        }

        // Apply initial layers
        this._applyToCurrentMap();
    }
    
    /**
     * Set base layer
     * @param {string} layerId - Layer identifier (e.g., 'mapbox/dark-v10', 'ion-world')
     */
    setBaseLayer(layerId) {
        this.config.baseLayer = layerId;
        this._applyToCurrentMap();
    }
    
    /**
     * Set weather layer
     * @param {string} layerId - Weather layer identifier ('none', 'clouds_new', etc.)
     */
    setWeatherLayer(layerId) {
        this.config.weatherLayer = layerId;
        this._applyToCurrentMap();
    }
    
    
    /**
     * Get current configuration
     */
    getConfig() {
        return { ...this.config };
    }
    
    /**
     * Apply configuration to the currently active map
     */
    _applyToCurrentMap() {
        // If mapInterface is available, use it to determine mode
        if (this.mapInterface) {
            const mode = this.mapInterface.getMode();
            if (mode === '2d') {
                this.applyToLeaflet();
            } else {
                this.applyToCesium();
            }
        } else {
            // At startup, mapInterface might not be set yet - apply to Leaflet by default
            // (since we start in 2D mode)
            if (this.leafletMap) {
                this.applyToLeaflet();
            }
        }
    }
    
    /**
     * Apply layers to Leaflet map
     */
    applyToLeaflet() {
        if (!this.leafletMap) {
            console.warn("LayerManager: Leaflet map not set");
            return;
        }
        
        // Apply base layer
        this._applyBaseLayerToLeaflet();
        
        // Apply weather layer
        this._applyWeatherLayerToLeaflet();
    }
    
    /**
     * Apply layers to Cesium map
     */
    applyToCesium() {
        if (!this.cesiumMap) return;
        
        // Apply base layer
        this._applyBaseLayerToCesium();
        
        // Apply weather layer
        this._applyWeatherLayerToCesium();
    }
    
    /**
     * Apply base layer to Leaflet
     */
    _applyBaseLayerToLeaflet() {
        console.log('LayerManager: _applyBaseLayerToLeaflet called with layerId:', this.config.baseLayer);
        if (!this.leafletMap) {
            console.log('LayerManager: No leafletMap available');
            return;
        }
        
        // Remove existing base layer
        if (this.leafletBaseLayer) {
            this.leafletMap.removeLayer(this.leafletBaseLayer);
            this.leafletBaseLayer = null;
        }
        
        // Update legacy variable
        window.current_base_layer = null;
        
        const layerId = this.config.baseLayer;
        
        // Handle Mapbox styles
        if (layerId && layerId.includes('mapbox')) {
            const mapboxToken = window.NewsGlobeConfig?.mapboxToken;
            if (!mapboxToken || mapboxToken === 'YOUR_MAPBOX_ACCESS_TOKEN_HERE') {
                console.warn('Mapbox token not configured, falling back to OpenStreetMap');
                // Fallback to OpenStreetMap
                this.leafletBaseLayer = L.tileLayer('https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png', {
                    maxZoom: 19,
                    attribution: '&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a> contributors'
                });
                this.leafletMap.addLayer(this.leafletBaseLayer);
                window.current_base_layer = this.leafletBaseLayer;
                // Update config to reflect the fallback
                this.config.baseLayer = 'osm';
                // Update UI selector to show fallback
                const baseLayerSelect = document.getElementById('base_layer_select') || document.getElementById('map_style_select');
                if (baseLayerSelect) {
                    baseLayerSelect.value = 'osm';
                }
                return;
            }
            this.leafletBaseLayer = L.tileLayer('https://api.mapbox.com/styles/v1/{id}/tiles/{z}/{x}/{y}?access_token={accessToken}', {
                maxNativeZoom: 18,
                maxZoom: 25,
                id: layerId,
                tileSize: 512,
                zoomOffset: -1,
                accessToken: mapboxToken
            });
            this.leafletMap.addLayer(this.leafletBaseLayer);
            window.current_base_layer = this.leafletBaseLayer;
        }
        // Handle OpenStreetMap (works in both 2D and 3D)
        else if (layerId === 'osm') {
            this.leafletBaseLayer = L.tileLayer('https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png', {
                maxZoom: 19,
                attribution: '&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a> contributors'
            });
            this.leafletMap.addLayer(this.leafletBaseLayer);
            window.current_base_layer = this.leafletBaseLayer;
        }
        // Handle Cesium-only providers (ion-world, ion-sentinel, natural-earth)
        // These don't work in Leaflet, so fallback to OpenStreetMap
        else if (layerId === 'ion-world' || layerId === 'ion-sentinel' || layerId === 'natural-earth') {
            console.warn(`Layer "${layerId}" is only available in 3D mode. Falling back to OpenStreetMap.`);
            // Fallback to OpenStreetMap
            this.leafletBaseLayer = L.tileLayer('https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png', {
                maxZoom: 19,
                attribution: '&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a> contributors'
            });
            this.leafletMap.addLayer(this.leafletBaseLayer);
            window.current_base_layer = this.leafletBaseLayer;
            // Update config to reflect the fallback (so UI stays in sync)
            this.config.baseLayer = 'osm';
            // Update UI selector to show fallback
            const baseLayerSelect = document.getElementById('base_layer_select') || document.getElementById('map_style_select');
            if (baseLayerSelect) {
                baseLayerSelect.value = 'osm';
            }
        }
    }
    
    /**
     * Apply weather layer to Leaflet
     */
    _applyWeatherLayerToLeaflet() {
        if (!this.leafletMap) return;
        
        // Remove existing weather layer
        if (this.leafletWeatherLayer) {
            this.leafletMap.removeLayer(this.leafletWeatherLayer);
            this.leafletWeatherLayer = null;
        }
        
        // Update legacy variable
        window.current_openweather_layer = null;
        
        // Add new weather layer if not 'none'
        if (this.config.weatherLayer && !this.config.weatherLayer.includes('none')) {
            const openweatherToken = window.NewsGlobeConfig?.openweathermapApiKey;
            if (!openweatherToken) {
                console.warn('OpenWeatherMap API key not configured. Weather layers will not be available.');
                return;
            }
            this.leafletWeatherLayer = L.tileLayer('https://tile.openweathermap.org/map/{id}/{z}/{x}/{y}.png?appid={accessToken}', {
                id: this.config.weatherLayer,
                accessToken: openweatherToken
            });
            this.leafletMap.addLayer(this.leafletWeatherLayer);
            // Update legacy variable for backwards compatibility
            window.current_openweather_layer = this.leafletWeatherLayer;
        }
    }
    
    
    
    
    
    /**
     * Apply base layer to Cesium
     */
    _applyBaseLayerToCesium() {
        if (!this.cesiumMap) return;
        
        // Use CesiumMap's setBaseLayer method
        if (this.cesiumMap.setBaseLayer) {
            this.cesiumMap.setBaseLayer(this.config.baseLayer);
        } else {
            // Fallback to old method for non-Mapbox layers
            if (!this.config.baseLayer.includes('mapbox')) {
                this.cesiumMap.setImageryProvider(this.config.baseLayer);
            }
        }
    }
    
    /**
     * Apply weather layer to Cesium
     */
    _applyWeatherLayerToCesium() {
        if (!this.cesiumMap) return;
        
        // Use CesiumMap's setWeatherLayer method
        if (this.cesiumMap.setWeatherLayer) {
            this.cesiumMap.setWeatherLayer(this.config.weatherLayer);
        }
    }
    
}

// Create global instance
window.layerManager = new LayerManager();
