// Configuration for News Globe frontend
// This file loads configuration from the API
(function() {
    // Default configuration
    window.NewsGlobeConfig = {
        // Mapbox access token - loaded from API
        mapboxToken: null,

        // Cesium Ion access token - loaded from API
        cesiumIonToken: null,

        // OpenWeatherMap API key - loaded from API
        openweathermapApiKey: null,

        // API configuration
        apiBaseUrl: '/api',

        // Default map settings (will be updated based on token availability)
        defaultBaseLayer: 'osm', // Default to OSM, will be updated to mapbox/dark-v10 if token is available
        defaultMapMode: '2d', // '2d' or '3d'

        // Visualization settings
        defaultVisualization: {
            circles: false,
            cylinders: true,
            polylines: false
        }
    };

    // Load configuration from API
    fetch('/api/config')
        .then(response => response.json())
        .then(config => {
            window.NewsGlobeConfig.mapboxToken = config.mapboxToken || null;
            window.NewsGlobeConfig.cesiumIonToken = config.cesiumIonToken || null;
            window.NewsGlobeConfig.openweathermapApiKey = config.openweathermapApiKey || null;

            // Set default base layer based on Mapbox token availability
            if (window.NewsGlobeConfig.mapboxToken && window.NewsGlobeConfig.mapboxToken !== 'YOUR_MAPBOX_ACCESS_TOKEN_HERE') {
                window.NewsGlobeConfig.defaultBaseLayer = 'mapbox/dark-v10';
                console.log('Configuration loaded from API. Mapbox token configured, using Mapbox Dark style');
            } else {
                window.NewsGlobeConfig.defaultBaseLayer = 'osm';
                console.log('Configuration loaded from API. Mapbox token not configured, using OpenStreetMap');
            }

            // Update LayerManager config and apply the new base layer
            if (window.layerManager) {
                console.log('Config loaded, setting base layer to:', window.NewsGlobeConfig.defaultBaseLayer);
                window.layerManager.config.baseLayer = window.NewsGlobeConfig.defaultBaseLayer;
                // Use setBaseLayer method which handles the application properly
                window.layerManager.setBaseLayer(window.NewsGlobeConfig.defaultBaseLayer);
                console.log('LayerManager config updated, base layer is now:', window.layerManager.config.baseLayer);
            } else {
                console.warn('LayerManager not available when config loaded');
            }

            // Update UI selector to match the default base layer
            const baseLayerSelect = document.getElementById('base_layer_select');
            if (baseLayerSelect) {
                baseLayerSelect.value = window.NewsGlobeConfig.defaultBaseLayer;
            }
        })
        .catch(error => {
            console.warn('Failed to load configuration from API, will use OpenStreetMap as fallback:', error);
            window.NewsGlobeConfig.mapboxToken = null;
            window.NewsGlobeConfig.defaultBaseLayer = 'osm';
        });
})();