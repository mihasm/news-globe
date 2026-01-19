
// Legacy variables for backwards compatibility (used by other code)
window.current_selected_tile = undefined;
window.prev_selected_tile = undefined;
window.current_base_layer = undefined;
window.current_openweather_layer = undefined;

/**
 * Change tiles - now uses LayerManager for unified layer management
 */
function change_tiles() {
    if (!window.layerManager) {
        console.warn("LayerManager not initialized");
        return;
    }
    
    // Get values from UI - try unified selector first, fallback to legacy selectors
    const baseLayerSelect = document.getElementById("base_layer_select") || document.getElementById("map_style_select");
    const weatherSelect = document.getElementById("openweather_select");

    if (!baseLayerSelect || !weatherSelect) {
        console.warn("Layer selectors not found");
        return;
    }
    
    // Update base layer
    const selectedBaseLayer = baseLayerSelect.value;
    if (window.current_selected_tile !== selectedBaseLayer) {
        window.prev_selected_tile = window.current_selected_tile;
        window.current_selected_tile = selectedBaseLayer;
        window.layerManager.setBaseLayer(selectedBaseLayer);
    }
    
    // Update weather layer
    const selectedWeatherLayer = weatherSelect.value;
    window.layerManager.setWeatherLayer(selectedWeatherLayer);
}


