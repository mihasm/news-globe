function remove_loading_circle() {
    getElementByXpath('//div[@class="loading-overlay"]').remove();
}

remove_loading_circle();

$(".popup").each(function(){
	$(this).resizable().draggable();
	$(this).css("position","absolute");
})

function close_layers() {
    document.getElementById('layers_panel').style.display = "none";
    button_layers_panel.checked=0;
}



function close_time_filter_panel() {
    document.getElementById('time_filter_panel').style.display = "none";
    button_time_filter_panel.checked=0;
}

function close_admin_panel() {
    document.getElementById('admin_panel').style.display = "none";
    button_admin_panel.checked = 0;
}

/* BUTTONS */


var show_planes_bool = 0;


var button_layers_panel = document.getElementById('button_layers_panel');
document.getElementById("layers_panel").style.display = "none";
button_layers_panel.addEventListener('change', (event) => {
    if (button_layers_panel.checked) {
        document.getElementById("layers_panel").style.display = "";
    } else {
        document.getElementById("layers_panel").style.display = "none";
    }
});

var button_time_filter_panel = document.getElementById('button_time_filter');
var time_filter_panel = document.getElementById("time_filter_panel");
if (button_time_filter_panel && time_filter_panel) {
    time_filter_panel.style.display = "none";
    button_time_filter_panel.addEventListener('change', (event) => {
        if (button_time_filter_panel.checked) {
            time_filter_panel.style.display = "";
        } else {
            time_filter_panel.style.display = "none";
        }
    });
}

var button_admin_panel = document.getElementById('button_admin_panel');
var admin_panel = document.getElementById("admin_panel");
if (button_admin_panel && admin_panel) {
    admin_panel.style.display = "none";
    button_admin_panel.addEventListener('change', (event) => {
        if (button_admin_panel.checked) {
            admin_panel.style.display = "";
            updateAdminStats();
        } else {
            admin_panel.style.display = "none";
        }
    });
}

// Admin panel functions
function updateAdminStats() {
    fetch('/api/stats')
        .then(response => response.json())
        .then(data => {
            const normalizedItemsEl = document.getElementById('admin_normalized_items_count');
            if (normalizedItemsEl) {
                normalizedItemsEl.textContent = data.normalized_items_count || 0;
            }
            const clusteredItemsEl = document.getElementById('admin_clustered_items_count');
            if (clusteredItemsEl) {
                clusteredItemsEl.textContent = data.clustered_items_count || 0;
            }
            const clustersEl = document.getElementById('admin_clusters_count');
            if (clustersEl) {
                clustersEl.textContent = data.clusters_count || 0;
            }
        })
        .catch(error => {
            console.error('Error fetching stats:', error);
            const normalizedItemsEl = document.getElementById('admin_normalized_items_count');
            if (normalizedItemsEl) {
                normalizedItemsEl.textContent = 'Error';
            }
            const clusteredItemsEl = document.getElementById('admin_clustered_items_count');
            if (clusteredItemsEl) {
                clusteredItemsEl.textContent = 'Error';
            }
            const clustersEl = document.getElementById('admin_clusters_count');
            if (clustersEl) {
                clustersEl.textContent = 'Error';
            }
        });
}

// Confirmation popup functions
function showConfirmationPopup(message, onConfirm, onCancel = null) {
    const popup = document.getElementById('confirmation_popup');
    const messageEl = document.getElementById('confirmation_message');
    const confirmBtn = document.getElementById('confirmation_confirm');
    const cancelBtn = document.getElementById('confirmation_cancel');

    messageEl.textContent = message;
    popup.style.display = 'block';

    const handleConfirm = () => {
        popup.style.display = 'none';
        confirmBtn.removeEventListener('click', handleConfirm);
        cancelBtn.removeEventListener('click', handleCancel);
        overlay.removeEventListener('click', handleCancel);
        onConfirm();
    };

    const handleCancel = () => {
        popup.style.display = 'none';
        confirmBtn.removeEventListener('click', handleConfirm);
        cancelBtn.removeEventListener('click', handleCancel);
        overlay.removeEventListener('click', handleCancel);
        if (onCancel) onCancel();
    };

    confirmBtn.addEventListener('click', handleConfirm);
    cancelBtn.addEventListener('click', handleCancel);

    // Allow clicking overlay to cancel
    const overlay = popup.querySelector('.confirmation-popup-overlay');
    overlay.addEventListener('click', handleCancel);

    // Focus the cancel button by default
    cancelBtn.focus();
}

function deleteAllData() {
    const btn = document.getElementById('admin_delete_all_btn');
    const statusEl = document.getElementById('admin_status_message');

    showConfirmationPopup(
        'Are you sure you want to delete ALL data from the database? This will permanently delete all normalized items and all clusters. This action cannot be undone.',
        () => {
            // User confirmed - proceed with deletion
            performDeleteAll(btn, statusEl);
        }
    );
}

function performDeleteAll(btn, statusEl) {
    // Disable button and show loading state
    if (btn) {
        btn.disabled = true;
        btn.textContent = 'Deleting...';
    }

    if (statusEl) {
        statusEl.style.display = 'none';
    }

    fetch('/api/delete-all', {
        method: 'DELETE'
    })
        .then(response => response.json())
        .then(data => {
            if (data.status === 'success') {
                // Show success message
                if (statusEl) {
                    statusEl.style.display = 'block';
                    statusEl.style.backgroundColor = '#4caf50';
                    statusEl.style.color = 'white';
                    statusEl.textContent = `Successfully deleted all data: ${data.normalized_items_deleted || 0} normalized items deleted, and ${data.clusters_deleted} clusters deleted`;
                }

                // Update stats
                updateAdminStats();

                // Refresh map and sidebar if they exist

                // Clear all cluster-related state
                if (window.markers && window.displayedClusters) {
                    for (let [clusterId, clusterData] of window.displayedClusters.entries()) {
                        if (clusterData && clusterData.marker) {
                            window.markers.removeLayer(clusterData.marker);
                        }
                    }
                }

                // Clear cluster tracking maps
                if (window.displayedClusters) {
                    window.displayedClusters.clear();
                }
                if (window.displayedItemsByCluster) {
                    window.displayedItemsByCluster.clear();
                }

                // Clear sidebar cluster state
                if (window.newsSidebar && window.newsSidebar.displayedClusters) {
                    window.newsSidebar.displayedClusters.clear();
                }

                // Clear location store if it exists
                if (window.locationStore) {
                    window.locationStore.clear();
                }
            } else {
                throw new Error(data.error || 'Unknown error');
            }
        })
        .catch(error => {
            console.error('Error deleting all data:', error);
            if (statusEl) {
                statusEl.style.display = 'block';
                statusEl.style.backgroundColor = '#f44336';
                statusEl.style.color = 'white';
                statusEl.textContent = `Error: ${error.message}`;
            }
        })
        .finally(() => {
            // Re-enable button
            if (btn) {
                btn.disabled = false;
                btn.textContent = 'Delete All Data';
            }
        });
}







// Sidebar toggle
window.sidebarEnabled = false;
var button_sidebar = document.getElementById('button_sidebar');
if (button_sidebar) {
    button_sidebar.addEventListener('change', (event) => {
        if (button_sidebar.checked) {
            window.sidebarEnabled = true;
            // Show sidebar if there's a selected location
            if (window.mapInterface) {
                window.mapInterface.showSidebar();
            }
        } else {
            window.sidebarEnabled = false;
            // Hide sidebar
            if (window.mapInterface) {
                window.mapInterface.hideSidebar();
            }
        }
    });
}







/* STATUS CONTAINER LOGIC */

// Track when data was last received
let lastDataReceived = null;
let serverOnline = false;
const DATA_WINDOW_MS = 30000; // 30 seconds

// Status color functions
function redStatus() {
    document.getElementById("status_container").style.color = "red";
}

function greenStatus() {
    document.getElementById("status_container").style.color = "green";
}

function orangeStatus() {
    document.getElementById("status_container").style.color = "orange";
}

// Mark that data was received
function markDataReceived() {
    lastDataReceived = Date.now();
    serverOnline = true;
    updateStatus();
}

// Mark that server request failed (server offline)
function markServerOffline() {
    serverOnline = false;
    updateStatus();
}

// Mark that server request succeeded (server online)
function markServerOnline() {
    serverOnline = true;
    updateStatus();
}

// Update status based on conditions
function updateStatus() {
    const now = Date.now();
    const timeSinceLastData = lastDataReceived ? (now - lastDataReceived) : Infinity;
    
    if (!serverOnline) {
        // Server is offline
        redStatus();
    } else if (timeSinceLastData <= DATA_WINDOW_MS) {
        // Server is online AND data received in last 30 seconds
        greenStatus();
    } else {
        // Server is online BUT no data in last 30 seconds
        orangeStatus();
    }
}

// Make functions globally accessible
window.markDataReceived = markDataReceived;
window.markServerOnline = markServerOnline;
window.markServerOffline = markServerOffline;
window.updateStatus = updateStatus;

// Periodic status check (every 2 seconds)
setInterval(function() {
    updateStatus();
}, 2000);

// Initialize status to red (server not checked yet)
redStatus();


/* KEYBOARD SHORTCUTS*/

document.addEventListener("keypress", function onEvent(event) {
    if(event.target.nodeName !== 'INPUT' && event.target.nodeName !== 'TEXTAREA') {
        // No keyboard shortcuts defined
    }

});

/* LAYERS PANEL CONTROLS */


// Weather Radar toggle - controls rainviewer visibility
var layer_weather_radar = document.getElementById('layer_weather_radar');
if (layer_weather_radar) {
    layer_weather_radar.addEventListener('change', function(event) {
        if (typeof toggleWeatherRadar === 'function') {
            toggleWeatherRadar(layer_weather_radar.checked);
        }
    });
}

// Marker mode radio buttons
var marker_mode_cluster = document.getElementById('marker_mode_cluster');
var marker_mode_circles = document.getElementById('marker_mode_circles');

if (marker_mode_cluster) {
    marker_mode_cluster.addEventListener('change', function(event) {
        if (marker_mode_cluster.checked && typeof setCircleMode === 'function') {
            setCircleMode(false);
        }
    });
}

if (marker_mode_circles) {
    marker_mode_circles.addEventListener('change', function(event) {
        if (marker_mode_circles.checked && typeof setCircleMode === 'function') {
            setCircleMode(true);
        }
    });
}

// Unified base layer selector - now uses LayerManager (replaces separate 2D/3D selectors)
var base_layer_select = document.getElementById('base_layer_select');
if (base_layer_select) {
    base_layer_select.addEventListener('change', function(event) {
        if (window.layerManager) {
            window.layerManager.setBaseLayer(base_layer_select.value);
        }
    });
}

// Legacy Cesium imagery selector - kept for backwards compatibility but hidden
var cesium_imagery_select = document.getElementById('cesium_imagery_select');
if (cesium_imagery_select) {
    cesium_imagery_select.addEventListener('change', function(event) {
        if (window.layerManager) {
            window.layerManager.setBaseLayer(cesium_imagery_select.value);
        } else if (typeof changeCesiumImagery === 'function') {
            // Fallback to old method if LayerManager not available
            changeCesiumImagery(cesium_imagery_select.value);
        }
    });
}

// 2D/3D Mode-aware section visibility
function updateLayersPanelForMode(mode) {
    var section3dViz = document.getElementById('section_3d_viz');
    var section2dViz = document.getElementById('section_2d_viz');
    var sectionOverlays = document.getElementById('section_overlays');
    var rowBaseLayer = document.getElementById('row_base_layer');
    var row2dBase = document.getElementById('row_2d_base');
    var row3dBase = document.getElementById('row_3d_base');
    var rowWeather = document.getElementById('row_weather');
    
    // Menu buttons that should be hidden in 3D mode
    
    // Filter base layer options based on mode
    var baseLayerSelect = document.getElementById('base_layer_select');
    if (baseLayerSelect) {
        var options = baseLayerSelect.querySelectorAll('option');
        options.forEach(function(option) {
            var value = option.value;
            // Cesium-only providers (ion-world, ion-sentinel, natural-earth) - hide in 2D
            if (mode === '2d' && (value === 'ion-world' || value === 'ion-sentinel' || value === 'natural-earth')) {
                option.style.display = 'none';
                option.disabled = true;
            }
            // Show all options in 3D (all are compatible)
            else if (mode === '3d') {
                option.style.display = '';
                option.disabled = false;
            }
            // Show Mapbox and OSM in 2D (they work in both modes)
            else {
                option.style.display = '';
                option.disabled = false;
            }
        });
        
        // If current selection is incompatible, change to a compatible one
        if (mode === '2d' && baseLayerSelect.value && 
            (baseLayerSelect.value === 'ion-world' || baseLayerSelect.value === 'ion-sentinel' || baseLayerSelect.value === 'natural-earth')) {
            baseLayerSelect.value = 'mapbox/dark-v10'; // Default to dark mapbox style
            // Trigger change to update the layer
            if (typeof change_tiles === 'function') {
                change_tiles();
            }
        }
    }
    
    if (mode === '3d') {
        // Show 3D sections, hide 2D-specific sections
        if (section3dViz) section3dViz.style.display = '';
        if (section2dViz) section2dViz.style.display = 'none';
        if (sectionOverlays) sectionOverlays.style.display = 'none';
        // Show unified base layer selector, hide legacy selectors
        if (rowBaseLayer) rowBaseLayer.style.display = '';
        if (row2dBase) row2dBase.style.display = 'none';
        if (row3dBase) row3dBase.style.display = 'none';
        // Show weather in both modes (unified layer management)
        if (rowWeather) rowWeather.style.display = '';

        // Hide 2D-only menu buttons (hide parent label element)
    } else {
        // Show 2D sections, hide 3D-specific sections
        if (section3dViz) section3dViz.style.display = 'none';
        if (section2dViz) section2dViz.style.display = '';
        if (sectionOverlays) sectionOverlays.style.display = '';
        // Show unified base layer selector, hide legacy selectors
        if (rowBaseLayer) rowBaseLayer.style.display = '';
        if (row2dBase) row2dBase.style.display = 'none';
        if (row3dBase) row3dBase.style.display = 'none';
        // Show weather in both modes (unified layer management)
        if (rowWeather) rowWeather.style.display = '';
    }
}

// Make function globally accessible
window.updateLayersPanelForMode = updateLayersPanelForMode;

// Listen for map mode changes
document.addEventListener('DOMContentLoaded', function() {
    // Hook into map mode toggle
    var button_map_mode = document.getElementById('button_map_mode');
    if (button_map_mode) {
        button_map_mode.addEventListener('change', function() {
            var mode = button_map_mode.checked ? '3d' : '2d';
            updateLayersPanelForMode(mode);
        });
    }
    
    // Initialize to 2D mode
    updateLayersPanelForMode('2d');
    
    // Initialize time filter panel controls
    initializeTimeFilterPanel();
});

/* TIME FILTER PANEL CONTROLS */

function initializeTimeFilterPanel() {
    // Wait for time filter panel to be available
    const initInterval = setInterval(() => {
        if (window.timeFilterPanel) {
            clearInterval(initInterval);
            setupTimeFilterControls();
        }
    }, 100);
}

function setupTimeFilterControls() {
    const timeFilterPanel = window.timeFilterPanel;
    
    // Get initial time range
    const range = timeFilterPanel.getTimeRange();
    
    // Update datetime inputs
    const fromInput = document.getElementById('time_filter_from');
    const toInput = document.getElementById('time_filter_to');
    
    
    if (fromInput) {
        fromInput.value = window.formatDateTimeLocal(range.from);
        fromInput.addEventListener('change', function() {
            const fromTime = new Date(this.value).getTime();
            const toTime = timeFilterPanel.getTimeRange().to;
            if (fromTime < toTime) {
                timeFilterPanel.setTimeRange(fromTime, toTime);
                // timeFilterManager will handle the refresh automatically
            }
        });
        fromInput.addEventListener('keydown', function(e) {
            if (e.key === 'Enter') {
                const fromTime = new Date(this.value).getTime();
                const toTime = timeFilterPanel.getTimeRange().to;
                if (fromTime < toTime) {
                    timeFilterPanel.setTimeRange(fromTime, toTime);
                    // Update slider handles to match the new time range
                    if (window.timeFilterManager) {
                        window.timeFilterManager.syncUIState();
                    }
                }
            }
        });
    }
    
    if (toInput) {
        toInput.value = window.formatDateTimeLocal(range.to);
        toInput.addEventListener('change', function() {
            const fromTime = timeFilterPanel.getTimeRange().from;
            const toTime = new Date(this.value).getTime();
            if (toTime > fromTime) {
                timeFilterPanel.setTimeRange(fromTime, toTime);
                // timeFilterManager will handle the refresh automatically
            }
        });
        toInput.addEventListener('keydown', function(e) {
            if (e.key === 'Enter') {
                const fromTime = timeFilterPanel.getTimeRange().from;
                const toTime = new Date(this.value).getTime();
                if (toTime > fromTime) {
                    timeFilterPanel.setTimeRange(fromTime, toTime);
                    // Update slider handles to match the new time range
                    if (window.timeFilterManager) {
                        window.timeFilterManager.syncUIState();
                    }
                }
            }
        });
    }
    
    // Color pickers
    const colorFromInput = document.getElementById('time_filter_color_from');
    const colorToInput = document.getElementById('time_filter_color_to');
    const colors = timeFilterPanel.getColorGradient();
    
    if (colorFromInput) {
        colorFromInput.value = colors.from;
        colorFromInput.addEventListener('change', function() {
            const colorTo = timeFilterPanel.getColorGradient().to;
            timeFilterPanel.setColorGradient(this.value, colorTo);
            // timeFilterManager will handle the refresh automatically
        });
    }
    
    if (colorToInput) {
        colorToInput.value = colors.to;
        colorToInput.addEventListener('change', function() {
            const colorFrom = timeFilterPanel.getColorGradient().from;
            timeFilterPanel.setColorGradient(colorFrom, this.value);
            // timeFilterManager will handle the refresh automatically
        });
    }
    
    // Checkboxes
    const filterEnabledCheckbox = document.getElementById('time_filter_enabled');
    const colorCodingCheckbox = document.getElementById('time_filter_color_coding');
    
    if (filterEnabledCheckbox) {
        filterEnabledCheckbox.checked = timeFilterPanel.isFilterEnabled();
        filterEnabledCheckbox.addEventListener('change', function() {
            timeFilterPanel.setFilterEnabled(this.checked);
            // timeFilterManager will handle the refresh automatically
        });
    }
    
    if (colorCodingCheckbox) {
        colorCodingCheckbox.checked = timeFilterPanel.isColorCodingEnabled();
        colorCodingCheckbox.addEventListener('change', function() {
            timeFilterPanel.setColorCodingEnabled(this.checked);
            // timeFilterManager will handle the refresh automatically
        });
    }
    
    
    // Time filter callbacks are now handled by timeFilterManager
    // The manager coordinates all filtering operations consistently
}

// refreshAllVisualizations function removed - now handled by timeFilterManager
