/**
 * CesiumJS 3D Globe Module
 * Adapted from News Globe for Tweetdeck Parser
 */

// Cesium Ion access token - loaded from configuration
// Note: This will be set after config loads in the CesiumMap constructor

/**
 * CesiumMap class - wrapper for CesiumJS viewer
 */
class CesiumMap {
    constructor(containerId = 'cesiumContainer') {
        this.containerId = containerId;
        this.viewer = null;
        this.primitives = new Cesium.PrimitiveCollection();
        this.markers = {};  // Store markers by id
        this.selectedKey = null;
        
        // Visualization settings - default to cylinders only
        this.showCircles = false;
        this.showCylinders = true;
        this.showPolylines = false;
        
        // Color gradient for recency (pink = old, red = new)
        this.rainbow = new Rainbow();
        this.rainbow.setSpectrum("#FFC0CB", "#990000");
        
        // Layer management
        this.baseLayer = null;  // Reference to base imagery layer
        this.weatherLayer = null;  // Reference to weather overlay layer
    }
    
    /**
     * Initialize the Cesium viewer
     */
    init() {
        // Set Cesium Ion access token from configuration
        if (window.NewsGlobeConfig && window.NewsGlobeConfig.cesiumIonToken) {
            Cesium.Ion.defaultAccessToken = window.NewsGlobeConfig.cesiumIonToken;
        } else {
            console.warn('Cesium Ion token not configured. Cesium functionality may be limited.');
        }

        this.viewer = new Cesium.Viewer(this.containerId, {
            // Disable default UI elements - controlled via layers panel
            baseLayerPicker: false,
            sceneModePicker: false,
            geocoder: false,
            navigationHelpButton: false,
            vrButton: false,
            infoBox: false,
            timeline: false,
            animation: false,
            homeButton: false,
            // Other settings
            contextOptions: {
                alpha: true
            },
            navigationInstructionsInitiallyVisible: false,
            shadows: false,
            skyAtmosphere: false
        });
        
        // Set default imagery provider (Bing Aerial)
        this._setDefaultImagery();
        
        // Clean up sky elements for dark background
        this.viewer.scene.skyBox.destroy();
        this.viewer.scene.skyBox = undefined;
        this.viewer.scene.sun.destroy();
        this.viewer.scene.sun = undefined;
        this.viewer.scene.moon.destroy();
        this.viewer.scene.moon = undefined;
        this.viewer.scene.fog.enabled = false;
        
        // Atmosphere settings
        this.viewer.scene.globe.atmosphereHueShift = -1.0;
        this.viewer.scene.globe.atmosphereSaturationShift = 0.0;
        this.viewer.scene.globe.atmosphereBrightnessShift = 0.0;
        this.viewer.scene.globe.showGroundAtmosphere = false;
        
        // Transparent background
        this.viewer.scene.backgroundColor = new Cesium.Color(0, 0, 0, 0);
        
        // Hide credits
        this.viewer.cesiumWidget.creditContainer.style.display = 'none';
        
        // Setup click handler
        this._setupClickHandler();
        
        // Setup keyboard controls
        this._setupKeyboardControls();
        
        // Add primitives collection to scene
        this.viewer.scene.primitives.add(this.primitives);

        // Listen for data updates from DataManager (via locationStore)
        this._setupDataListeners();

        console.log("CesiumJS viewer initialized");
        return this;
    }

    /**
     * Setup listeners for data updates from DataManager
     */
    _setupDataListeners() {
        // Wait for locationStore to be available
        const setupListeners = () => {
            if (window.locationStore) {
                // Listen for data updates from DataManager (emitted via locationStore)
                window.locationStore.on('dataRestored', () => {
                    console.log('CesiumMap: Data updated, refreshing markers');
                    this.refresh();
                });
            } else {
                // Retry after a short delay
                setTimeout(setupListeners, 100);
            }
        };

        setupListeners();
    }

    /**
     * Set default imagery provider
     */
    _setDefaultImagery() {
        // Use Cesium Ion World Imagery as default (includes Bing Maps via Ion token)
        this.setImageryProvider('ion-world');
    }
    
    /**
     * Change the imagery provider
     * @param {string} providerId - The provider ID
     */
    setImageryProvider(providerId) {
        const imageryLayers = this.viewer.imageryLayers;
        const self = this;
        
        // Remove existing base layer
        if (this.baseLayer) {
            imageryLayers.remove(this.baseLayer);
            this.baseLayer = null;
        }
        
        switch (providerId) {
            case 'ion-world':
                // Cesium Ion World Imagery (Bing Maps via Ion) - default
                Cesium.IonImageryProvider.fromAssetId(2).then(function(provider) {
                    self.baseLayer = imageryLayers.addImageryProvider(provider, 0);
                });
                break;
            case 'ion-sentinel':
                // Sentinel-2 imagery via Ion
                Cesium.IonImageryProvider.fromAssetId(3954).then(function(provider) {
                    self.baseLayer = imageryLayers.addImageryProvider(provider, 0);
                });
                break;
            case 'osm':
                // OpenStreetMap - no key required
                this.baseLayer = imageryLayers.addImageryProvider(
                    new Cesium.OpenStreetMapImageryProvider({
                        url: 'https://a.tile.openstreetmap.org/'
                    }), 
                    0
                );
                break;
            case 'natural-earth':
                // Natural Earth II - bundled with Cesium
                Cesium.TileMapServiceImageryProvider.fromUrl(
                    Cesium.buildModuleUrl('Assets/Textures/NaturalEarthII')
                ).then(function(provider) {
                    self.baseLayer = imageryLayers.addImageryProvider(provider, 0);
                });
                break;
            default:
                // Default to Ion World Imagery
                Cesium.IonImageryProvider.fromAssetId(2).then(function(provider) {
                    self.baseLayer = imageryLayers.addImageryProvider(provider, 0);
                });
        }
    }
    
    /**
     * Set base layer (supports Mapbox and Cesium providers)
     * @param {string} layerId - Layer identifier (Mapbox style ID or Cesium provider ID)
     */
    setBaseLayer(layerId) {
        const imageryLayers = this.viewer.imageryLayers;
        const self = this;
        
        // Store overlay layers before removing base layers
        const weatherLayerRef = this.weatherLayer;
        
        // Remove ALL imagery layers (Cesium Viewer creates a default one that might not be tracked)
        // This ensures we remove the default base layer that was created on init
        while (imageryLayers.length > 0) {
            imageryLayers.remove(imageryLayers.get(0));
        }
        
        // Reset all layer references
        this.baseLayer = null;
        this.weatherLayer = null;
        
        // Check if it's a Mapbox style
        if (layerId && layerId.includes('mapbox')) {
            const mapboxToken = window.NewsGlobeConfig?.mapboxToken;
            if (!mapboxToken || mapboxToken === 'YOUR_MAPBOX_ACCESS_TOKEN_HERE') {
                console.warn('Mapbox token not configured. Please set window.NewsGlobeConfig.mapboxToken in config.js');
                return;
            }
            // Mapbox style URL format: https://api.mapbox.com/styles/v1/{username}/{style_id}/tiles/{tileSize}/{z}/{x}/{y}
            // layerId format is "mapbox/dark-v10" (username/style_id)
            const url = `https://api.mapbox.com/styles/v1/${layerId}/tiles/512/{z}/{x}/{y}?access_token=${mapboxToken}`;
            
            this.baseLayer = imageryLayers.addImageryProvider(
                new Cesium.UrlTemplateImageryProvider({
                    url: url,
                    tileWidth: 512,
                    tileHeight: 512,
                    maximumLevel: 18
                }),
                0
            );
        } else {
            // Use standard setImageryProvider for non-Mapbox layers
            this.setImageryProvider(layerId || 'ion-world');
        }
        
        // Re-add overlay layers after base layer (they will be re-added by their respective set methods if needed)
        // But for now, we'll let the LayerManager handle re-adding them via setWeatherLayer
    }
    
    /**
     * Set weather layer overlay
     * @param {string} layerId - Weather layer identifier ('none', 'clouds_new', etc.)
     */
    setWeatherLayer(layerId) {
        const imageryLayers = this.viewer.imageryLayers;
        
        // Remove existing weather layer
        if (this.weatherLayer) {
            imageryLayers.remove(this.weatherLayer);
            this.weatherLayer = null;
        }
        
        // Add weather layer if not 'none'
        if (layerId && !layerId.includes('none')) {
            const openweatherToken = window.NewsGlobeConfig?.openweathermapApiKey;
            if (!openweatherToken) {
                console.warn('OpenWeatherMap API key not configured. Weather layers will not be available.');
                return;
            }
            const url = `https://tile.openweathermap.org/map/${layerId}/{z}/{x}/{y}.png?appid=${openweatherToken}`;
            
            // Add weather layer above base layer
            const insertIndex = imageryLayers.length;
            this.weatherLayer = imageryLayers.addImageryProvider(
                new Cesium.UrlTemplateImageryProvider({
                    url: url,
                    tileWidth: 256,
                    tileHeight: 256,
                    maximumLevel: 19
                }),
                insertIndex
            );
        }
    }
    
    
    
    
    
    /**
     * Set the camera view
     */
    setView(lat, lng, height = 10000000) {
        this.viewer.camera.setView({
            destination: Cesium.Cartesian3.fromDegrees(lng, lat, height)
        });
    }
    
    /**
     * Fly to a location
     */
    flyTo(lat, lng, height = null) {
        if (height === null) {
            height = this._getCurrentHeight();
        }
        
        this.viewer.camera.flyTo({
            destination: Cesium.Cartesian3.fromDegrees(lng, lat, height),
            duration: 1.0
        });
    }
    
    /**
     * Get current camera height
     */
    _getCurrentHeight() {
        const ellipsoid = this.viewer.scene.globe.ellipsoid;
        return ellipsoid.cartesianToCartographic(this.viewer.camera.position).height;
    }
    
    /**
     * Get current camera center
     */
    getCenter() {
        const ellipsoid = this.viewer.scene.globe.ellipsoid;
        const cartographic = ellipsoid.cartesianToCartographic(this.viewer.camera.position);
        return {
            lat: Cesium.Math.toDegrees(cartographic.latitude),
            lng: Cesium.Math.toDegrees(cartographic.longitude)
        };
    }
    
    /**
     * Add a marker/data point to the map
     */
    addMarker(id, lat, lng, data) {
        // Store marker data
        // Use count from data if provided (for initial load), otherwise increment
        const newCount = data.count !== undefined 
            ? data.count 
            : (this.markers[id]?.count || 0) + 1;
        
        this.markers[id] = {
            lat: lat,
            lng: lng,
            data: data,
            count: newCount,
            lastUpdate: Date.now()
        };
    }
    
    /**
     * Clear all markers
     */
    clearMarkers() {
        this.markers = {};
        this._clearPrimitives();
    }
    
    /**
     * Refresh the visualization based on current markers
     */
    refresh() {
        this._clearPrimitives();
        this._drawMarkers();
    }
    
    /**
     * Clear all primitives from scene
     */
    _clearPrimitives() {
        this.primitives.removeAll();
    }
    
    /**
     * Draw all markers as 3D primitives
     */
    _drawMarkers() {
        const height = this._getCurrentHeight();
        const circleInstances = [];
        const cylinderInstances = [];
        const polylines = new Cesium.PolylineCollection();

        // Get locations from location store for filtering and color calculation
        const locations = window.locationStore ? window.locationStore.getAllLocations() : {};

        for (const [key, marker] of Object.entries(this.markers)) {
            const { lat, lng } = marker;

            // Get location data from store if available
            const location = locations[key];
            let filteredItems = [];
            let totalCount = marker.count || 0;

            if (location && location.items) {
                // Handle both individual items and cluster items
                const hasClusters = location.items.some(item => item.type === 'cluster');

                if (hasClusters) {
                    // Process cluster items
                    for (const item of location.items) {
                        if (item.type === 'cluster' && item.cluster_items) {
                            // Filter cluster items by time if needed
                            let clusterFilteredItems = item.cluster_items;
                            if (window.timeFilterPanel) {
                                clusterFilteredItems = window.timeFilterPanel.filterItems(item.cluster_items);
                            }

                            if (clusterFilteredItems.length > 0) {
                                filteredItems.push({
                                    ...item,
                                    cluster_filtered_count: clusterFilteredItems.length
                                });
                                totalCount += clusterFilteredItems.length;
                            }
                        }
                    }
                } else {
                    // Process individual items
                    filteredItems = location.items;
                    if (window.timeFilterPanel) {
                        filteredItems = window.timeFilterPanel.filterItems(location.items);
                    }
                    totalCount = filteredItems.length;
                }
            }

            const count = totalCount;

            // Skip if no items after filtering
            if (count === 0) {
                continue;
            }
            
            // Calculate color based on time filter settings
            let colorHex = '#990000';  // Default red
            if (window.timeFilterPanel && location) {
                // For clusters, find the latest item across all cluster items
                let latestItem = null;
                let latestTime = 0;

                if (location && location.items && location.items.some(item => item.type === 'cluster')) {
                    // Handle cluster items
                    for (const clusterItem of location.items) {
                        if (clusterItem.type === 'cluster' && clusterItem.cluster_items) {
                            for (const subItem of clusterItem.cluster_items) {
                                if (subItem.published_at) {
                                    const itemTime = new Date(subItem.published_at).getTime();
                                    if (itemTime > latestTime) {
                                        latestTime = itemTime;
                                        latestItem = subItem;
                                    }
                                }
                            }
                        }
                    }
                } else if (filteredItems.length > 0) {
                    // Handle individual items
                    latestItem = filteredItems.reduce((latest, item) => {
                        if (!item.published_at) return latest;
                        const itemTime = new Date(item.published_at).getTime();
                        if (!latest || itemTime > new Date(latest.published_at).getTime()) {
                            return item;
                        }
                        return latest;
                    }, null);
                }

                if (latestItem) {
                    colorHex = window.timeFilterPanel.getColorForItem(latestItem);
                } else {
                    colorHex = window.timeFilterPanel.getColorForLocation(location);
                }
            } else {
                // Fallback to old method if time filter panel not available
                const now = Date.now();
                const oneWeekAgo = now - (7 * 24 * 60 * 60 * 1000);
                const lastUpdate = marker.lastUpdate || now;
                const timePercentage = Math.max(0, Math.min(1, (lastUpdate - oneWeekAgo) / (now - oneWeekAgo)));
                colorHex = "#" + this.rainbow.colorAt(timePercentage * 100);
            }
            
            const color = new Cesium.Color.fromCssColorString(colorHex);
            
            // Create circle
            if (this.showCircles) {
                const circle = this._createCircle(lat, lng, key, count, color, height);
                circleInstances.push(circle);
            }
            
            // Create cylinder
            if (this.showCylinders) {
                const cylinderHeight = 100000 * count;
                const cylinder = this._createCylinder(lat, lng, key, count, cylinderHeight, color, height);
                cylinderInstances.push(cylinder);
            }
            
            // Create polyline
            if (this.showPolylines) {
                const lineHeight = 100000 * count;
                polylines.add({
                    positions: [
                        Cesium.Cartesian3.fromDegrees(lng, lat, 0),
                        Cesium.Cartesian3.fromDegrees(lng, lat, lineHeight + 10000)
                    ],
                    width: 10,
                    id: key,
                    material: new Cesium.Material.fromType('Color', {
                        color: color
                    })
                });
            }
        }
        
        // Add circle primitives
        if (circleInstances.length > 0) {
            const circlePrimitive = new Cesium.Primitive({
                geometryInstances: circleInstances,
                appearance: new Cesium.PerInstanceColorAppearance({})
            });
            this.primitives.add(circlePrimitive);
        }
        
        // Add cylinder primitives
        if (cylinderInstances.length > 0) {
            const cylinderPrimitive = new Cesium.Primitive({
                geometryInstances: cylinderInstances,
                appearance: new Cesium.PerInstanceColorAppearance({
                    translucent: false
                })
            });
            this.primitives.add(cylinderPrimitive);
        }
        
        // Add polylines
        if (polylines.length > 0) {
            this.primitives.add(polylines);
        }
    }
    
    /**
     * Create a circle geometry instance
     */
    _createCircle(lat, lng, id, count, color, height) {
        const colorWithAlpha = color.clone();
        colorWithAlpha.alpha = 0.8;
        
        const radius = this._erf(height / 3e6) * Math.log(count + 1.5) * 110000;
        
        return new Cesium.GeometryInstance({
            geometry: new Cesium.CircleGeometry({
                center: Cesium.Cartesian3.fromDegrees(lng, lat),
                radius: radius,
                vertexFormat: Cesium.PerInstanceColorAppearance.VERTEX_FORMAT
            }),
            attributes: {
                color: Cesium.ColorGeometryInstanceAttribute.fromColor(colorWithAlpha)
            },
            id: id
        });
    }
    
    /**
     * Create a cylinder geometry instance
     */
    _createCylinder(lat, lng, id, count, cylinderHeight, color, cameraHeight) {
        const colorWithAlpha = color.clone();
        colorWithAlpha.alpha = 1.0;
        
        const radius = this._erf(cameraHeight / 3e6) * Math.log(count + 1.5) * 20000;
        
        if (cylinderHeight > cameraHeight) {
            cylinderHeight = cameraHeight - 10000;
        }
        
        const geometry = new Cesium.CylinderGeometry({
            topRadius: radius,
            bottomRadius: radius,
            length: cylinderHeight,
            vertexFormat: Cesium.PerInstanceColorAppearance.VERTEX_FORMAT
        });
        
        const modelMatrix = Cesium.Matrix4.multiplyByTranslation(
            Cesium.Transforms.eastNorthUpToFixedFrame(Cesium.Cartesian3.fromDegrees(lng, lat)),
            new Cesium.Cartesian3(0.0, 0.0, cylinderHeight * 0.5),
            new Cesium.Matrix4()
        );
        
        return new Cesium.GeometryInstance({
            geometry: geometry,
            modelMatrix: modelMatrix,
            attributes: {
                color: Cesium.ColorGeometryInstanceAttribute.fromColor(colorWithAlpha)
            },
            id: id
        });
    }
    
    /**
     * Error function approximation for radius scaling
     */
    _erf(x) {
        const a1 = 0.254829592;
        const a2 = -0.284496736;
        const a3 = 1.421413741;
        const a4 = -1.453152027;
        const a5 = 1.061405429;
        const p = 0.3275911;
        
        const sign = x < 0 ? -1 : 1;
        x = Math.abs(x);
        
        const t = 1.0 / (1.0 + p * x);
        const y = 1.0 - (((((a5 * t + a4) * t) + a3) * t + a2) * t + a1) * t * Math.exp(-x * x);
        
        return sign * y;
    }
    
    /**
     * Setup click handler for marker selection
     */
    _setupClickHandler() {
        const handler = new Cesium.ScreenSpaceEventHandler(this.viewer.scene.canvas);
        
        handler.setInputAction((movement) => {
            const pickedObject = this.viewer.scene.pick(movement.position);
            
            if (pickedObject && pickedObject.id) {
                this.selectedKey = pickedObject.id;
                const marker = this.markers[pickedObject.id];
                
                if (marker) {
                    // Trigger selection event
                    if (this.onMarkerSelect) {
                        this.onMarkerSelect(pickedObject.id, marker);
                    }
                }
            } else {
                this.selectedKey = null;
                if (this.onMarkerDeselect) {
                    this.onMarkerDeselect();
                }
            }
        }, Cesium.ScreenSpaceEventType.LEFT_CLICK);
        
        // Disable double-click tracking
        handler.setInputAction(() => {
            this.viewer.trackedEntity = undefined;
        }, Cesium.ScreenSpaceEventType.LEFT_DOUBLE_CLICK);
    }
    
    /**
     * Setup keyboard controls
     */
    _setupKeyboardControls() {
        const keyPressed = {};
        
        document.addEventListener('keydown', (e) => {
            keyPressed[e.keyCode] = true;
            this._handleKeys(keyPressed);
        });
        
        document.addEventListener('keyup', (e) => {
            keyPressed[e.keyCode] = false;
        });
    }
    
    /**
     * Handle keyboard input
     */
    _handleKeys(keyPressed) {
        const height = this._getCurrentHeight();
        let horizontalDegrees = 10.0;
        let verticalDegrees = 10.0;
        
        const viewRect = this.viewer.camera.computeViewRectangle();
        if (Cesium.defined(viewRect)) {
            horizontalDegrees *= Cesium.Math.toDegrees(viewRect.east - viewRect.west) / 360.0;
            verticalDegrees *= Cesium.Math.toDegrees(viewRect.north - viewRect.south) / 180.0;
        }
        
        // Shift + Up/Down for zoom
        if (keyPressed[16] && keyPressed[38]) {
            this.viewer.camera.zoomIn(0.5 * height);
        } else if (keyPressed[16] && keyPressed[40]) {
            this.viewer.camera.zoomOut(0.5 * height);
        }
        // Arrow keys for rotation
        else if (keyPressed[39]) {
            this.viewer.camera.rotateRight(Cesium.Math.toRadians(horizontalDegrees));
        } else if (keyPressed[37]) {
            this.viewer.camera.rotateLeft(Cesium.Math.toRadians(horizontalDegrees));
        } else if (keyPressed[38]) {
            this.viewer.camera.rotateDown(Cesium.Math.toRadians(verticalDegrees));
        } else if (keyPressed[40]) {
            this.viewer.camera.rotateUp(Cesium.Math.toRadians(verticalDegrees));
        }
    }
    
    /**
     * Set visualization options
     */
    setVisualization(options) {
        if ('circles' in options) this.showCircles = options.circles;
        if ('cylinders' in options) this.showCylinders = options.cylinders;
        if ('polylines' in options) this.showPolylines = options.polylines;
        
        this.refresh();
    }
    
    /**
     * Destroy the viewer
     */
    destroy() {
        if (this.viewer) {
            this.viewer.destroy();
            this.viewer = null;
        }
    }
    
    /**
     * Show the Cesium container
     */
    show() {
        const container = document.getElementById(this.containerId);
        if (container) {
            container.style.display = 'block';
        }
    }
    
    /**
     * Hide the Cesium container
     */
    hide() {
        const container = document.getElementById(this.containerId);
        if (container) {
            container.style.display = 'none';
        }
    }
}

// Export for use
window.CesiumMap = CesiumMap;

// Global function to change Cesium imagery (called from layers panel)
window.changeCesiumImagery = function(providerId) {
    if (window.mapInterface && window.mapInterface.cesiumMap) {
        window.mapInterface.cesiumMap.setImageryProvider(providerId);
    }
};
