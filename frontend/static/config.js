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

        // Default map settings
        defaultBaseLayer: 'mapbox/dark-v10',
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
            console.log('Configuration loaded from API. Mapbox token:', window.NewsGlobeConfig.mapboxToken ? 'configured' : 'not configured (using OSM fallback)');
        })
        .catch(error => {
            console.warn('Failed to load configuration from API, will use OpenStreetMap as fallback:', error);
            window.NewsGlobeConfig.mapboxToken = null;
        });
})();