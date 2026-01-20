
var pulse_icon = L.icon({
    iconUrl: 'pulsating_dot.gif',
    iconSize: [20, 20]
});
var static_icon = L.icon({
    iconUrl: 'static_dot.gif',
    iconSize: [20,20]
});

var map = L.map('map',{
    maxZoom:25,
    minZoom:0,
    zoomControl:false,
    renderer: L.canvas(),
}).setView([46, 15], 4);

// Make map globally accessible
window.map = map;


L.control.mousePosition().addTo(map);

// Weather radar (Rainviewer) - controlled via layers panel
var rainviewerControl = L.control.rainviewer({ 
    position: 'bottomright',
    nextButtonText: '>',
    playStopButtonText: 'Play/Stop',
    prevButtonText: '<',
    positionSliderLabelText: "Hour:",
    opacitySliderLabelText: "Opacity:",
    animationInterval: 500,
    opacity: 0.5
}).addTo(map);

// Rainviewer control is now always visible in bottom right


// Initialize LayerManager early if available (before change_tiles)
if (window.layerManager && window.map) {
    window.layerManager.init(window.map, null, null);
}

change_tiles();

map.keyboard.enable();

// Clear location store on page load - fetch fresh data from backend
if (window.locationStore) {
    window.locationStore.clear();
}

var markers = L.markerClusterGroup({
    removeOutsideVisibleBounds: false,
    zoomToBoundsOnClick: true,
    maxClusterRadius:10,
    closePopupOnClick:false,
    spiderfyOnMaxZoom: true,
    animate: false,
    iconCreateFunction: function(cluster) {
        pulsating = false;
        var markers = cluster.getAllChildMarkers();
        var n = 0;
        for (var i = 0; i < markers.length; i++) {
            c = markers[i];
            if (c.alert_on == true) {
                pulsating = true;
                break;
            }
        }

        // Count clusters by location_key - get all location_keys from markers in this cluster
        var locationKeys = new Set();
        for (var i = 0; i < markers.length; i++) {
            if (markers[i].locationKey) {
                locationKeys.add(markers[i].locationKey);
            }
        }

        // If all markers have the same location_key, count all markers with that location_key across the entire map
        // Otherwise, just count markers in this cluster group
        if (locationKeys.size === 1 && markers.length > 0) {
            try {
                var locationKey = Array.from(locationKeys)[0];
                // Count all markers on the map with this location_key
                // Access the MarkerClusterGroup via the cluster's parent or use the global variable
                // The cluster object has a reference to the parent group
                var markerGroup = cluster._group || cluster._parentGroup;
                if (markerGroup && markerGroup.getLayers) {
                    var allMarkers = markerGroup.getLayers();
                    count = 0;
                    for (var i = 0; i < allMarkers.length; i++) {
                        if (allMarkers[i].locationKey === locationKey) {
                            count++;
                        }
                    }
                } else {
                    // Fallback: count markers in this cluster only
                    count = cluster.getChildCount();
                }
                // If count is 0 (shouldn't happen), fall back to cluster count
                if (count === 0) {
                    count = cluster.getChildCount();
                }
            } catch (e) {
                // If there's any error, fall back to cluster count
                console.warn('Error counting by location_key, using cluster count:', e);
                count = cluster.getChildCount();
            }
        } else {
            // Multiple location_keys in this cluster, just count markers in this group
            count = cluster.getChildCount();
        }

        // Always show cluster icon, even for single items

        margin_left="2px";
        if (markers.length >= 10) {
            margin_left="-1px";
        }

        if (markers.length >= 100) {
            count = "99+";
        }
        if (pulsating == true) {
            return L.divIcon({ html: '<img src="pulsating_dot.gif" class="custom_cluster_icon"> \
                <b style="position:absolute;margin-left:'+margin_left+';margin-top:-3px;color:white;">' +
                count + '</b> ' });
        } else {
            return L.divIcon({ html: '<img src="static_dot.gif" class="custom_cluster_icon"> \
                <b style="position:absolute;margin-left:'+margin_left+';margin-top:-3px;color:white;">' +
                count + '</b> ' });
        }
    }
})

// Make markers globally accessible
window.markers = markers;

cluster_selected = null;

markers.on('clusterclick', function (a) {
    cluster_selected = a.layer;
});

map.on('click', function(e) {        
    cluster_selected = null;
});

markers.on('clustermouseover', function (e) {
    cluster_selected = e.layer;
    cluster_selected.spiderfy();
});

map.options.maxZoom = 25;

/* MARKER MANAGEMENT */

// Track displayed items by ID to avoid re-adding (database handles deduplication)
var displayedItemIds = new Set();
// Track clusters and their items for dynamic updates
var displayedClusters = new Map(); // clusterId -> { marker, clusterData, itemIds: Set }
var displayedItemsByCluster = new Map(); // clusterId -> Set of item IDs
// Track which clusters have expanded balloons
var expandedClusters = new Set(); // clusterId -> boolean

// Make cluster tracking maps globally accessible
window.displayedClusters = displayedClusters;
window.displayedItemsByCluster = displayedItemsByCluster;
window.expandedClusters = expandedClusters;

// Global in-memory cache of all clusters (without time filter)
// This allows instant filtering without waiting for AJAX
window.clusterCache = [];
var clusterCacheUpdateInterval = null;

var refresh_items_timeout;

var currentMapRefreshRequest = null; // Track current map refresh request

function refresh_all_items() {
    // Cancel any existing request (kept for compatibility)
    if (currentMapRefreshRequest) {
        // console.log('refresh_all_items: Cancelling previous request');
        // Since we're not using AJAX anymore, just clear the reference
        currentMapRefreshRequest = null;
    }

    // Get data from DataManager instead of AJAX
    if (!window.dataManager) {
        console.warn('DataManager not available for map refresh');
        // Retry after 5 seconds
        clearTimeout(refresh_items_timeout);
        refresh_items_timeout = setTimeout(function(){refresh_all_items()}, 5000);
        return;
    }

    // Build filter parameters for DataManager
    var filters = {};

    // Add time filter parameters if enabled (use centralized manager)
    if (window.timeFilterManager && window.timeFilterManager.isFilterActive()) {
        const timeParams = window.timeFilterManager.getTimeFilterParams();
        if (timeParams.time_from) filters.timeFrom = timeParams.time_from;
        if (timeParams.time_to) filters.timeTo = timeParams.time_to;
    }

    // Layer filtering is now handled by DataManager based on checkbox states

    // Get clusters from DataManager
    const clusters = window.dataManager.getClusters(filters);

    // Fast path for circle mode: update locationStore only, then refresh circles incrementally
    if (circleModeEnabled) {
        syncLocationStoreFromClusters(clusters);
        refreshDirtyCircles();
        // Clear request reference and schedule next poll
        currentMapRefreshRequest = null;
        clearTimeout(refresh_items_timeout);
        refresh_items_timeout = setTimeout(function(){refresh_all_items()}, 5000);
        return;
    }

    // Process clusters directly (simulating AJAX success callback)
    {
        // Mark server as online (simulate since DataManager handles connectivity)
        if (typeof markServerOnline === 'function') {
            markServerOnline();
        } else if (typeof window.markServerOnline === 'function') {
            window.markServerOnline();
        } else {
            console.error("markServerOnline function not found!");
        }

        if (clusters && clusters.length > 0) {
                // Track which clusters should exist (from current response)
                var clustersInResponse = new Set();
                var skippedCount = 0;
                var addedCount = 0;
                var updatedCount = 0;
                
                // Process clusters - iterate over clusters and add/update markers
                for (let cluster of clusters) {
                    var clusterId = cluster.cluster_id;
                    clustersInResponse.add(clusterId);
                    
                    // Get cluster location from cluster data (DataManager format)
                    var clusterLat = cluster.representative_lat;
                    var clusterLng = cluster.representative_lon;
                    var clusterLocationKey = cluster.location_key;
                    var clusterLocationName = cluster.representative_location_name;
                    var clusterItems = cluster.items || [];
                    
                    var existingCluster = displayedClusters.get(clusterId);
                    
                    // Create cluster marker data from cluster JSON
                    //console.log('Map: Creating cluster marker for cluster', clusterId, 'with location_key:', clusterLocationKey, 'location_name:', clusterLocationName);
                    var clusterMarkerData = {
                        lat: clusterLat,
                        lng: clusterLng,
                        location_key: clusterLocationKey,
                        location_name: clusterLocationName,
                        type: 'cluster',
                        cluster_id: clusterId,
                        title: cluster.title,
                        item_count: cluster.item_count,
                        items: clusterItems,
                        createdAt: cluster.last_seen_at || cluster.first_seen_at
                    };
                    
                    if (existingCluster && existingCluster.marker) {
                        // Cluster marker already exists - just update it
                        existingCluster.clusterData = clusterMarkerData;
                        updatedCount++;
                        
                        // Update marker tooltip and popup
                        var updatedContent = generateClusterHtml(clusterMarkerData);
                        var updatedTooltip = `${cluster.title || 'Cluster'} (${cluster.item_count} items)`;
                        
                        existingCluster.marker.bindTooltip(updatedTooltip, {
                            permanent: false,
                            autoPan: false,
                            className: 'marker_tooltip',
                        });
                        
                        if (existingCluster.marker.popup_store) {
                            existingCluster.marker.popup_store.setContent(updatedContent);
                        }
                        
                        existingCluster.marker.clusterItemCount = cluster.item_count;
                        existingCluster.marker.locationKey = clusterLocationKey;
                        
                        // Update location store
                        if (window.locationStore && clusterLocationKey) {
                            window.locationStore.updateItem(clusterLocationKey, {
                                type: 'cluster',
                                cluster_id: clusterId,
                                cluster_title: cluster.title,
                                cluster_item_count: cluster.item_count,
                                cluster_items: clusterItems,
                                html: updatedContent,
                                tooltip: updatedTooltip,
                                locationName: clusterLocationName
                            });
                        }
                    } else {
                        // New cluster - create marker only if it doesn't exist on map
                        // Check if marker already exists on map by clusterId
                        var markerExists = false;
                        var allLayers = markers.getLayers();
                        for (var layer of allLayers) {
                            var checkMarkers = layer.getAllChildMarkers ? layer.getAllChildMarkers() : [layer];
                            for (var m of checkMarkers) {
                                if (m.clusterId === clusterId) {
                                    markerExists = true;
                                    // Update the existing marker
                                    var updatedContent = generateClusterHtml(clusterMarkerData);
                                    var updatedTooltip = `${cluster.title || 'Cluster'} (${cluster.item_count} items)`;
                                    m.bindTooltip(updatedTooltip, {
                                        permanent: false,
                                        autoPan: false,
                                        className: 'marker_tooltip',
                                    });
                                    if (m.popup_store) {
                                        m.popup_store.setContent(updatedContent);
                                    }
                                    m.clusterItemCount = cluster.item_count;
                                    m.locationKey = clusterLocationKey;
                                    
                                    // Track it
                                    var itemIds = new Set();
                                    for (let item of clusterItems) {
                                        var itemId = item.id || (item.type === 'tweet' ? item.tweet_id : item.url);
                                        itemIds.add(itemId);
                                    }
                                    displayedClusters.set(clusterId, {
                                        marker: m,
                                        clusterData: clusterMarkerData,
                                        itemIds: itemIds
                                    });
                                    displayedItemsByCluster.set(clusterId, itemIds);
                                    break;
                                }
                            }
                            if (markerExists) break;
                        }
                        
                        if (!markerExists) {
                            // Really new - add it
                            var itemIds = new Set();
                            for (let item of clusterItems) {
                                var itemId = item.id || (item.type === 'tweet' ? item.tweet_id : item.url);
                                itemIds.add(itemId);
                            }
                            
                            var marker = addClusterMarkerAndReturn(clusterMarkerData);
                            if (marker) {
                                displayedClusters.set(clusterId, {
                                    marker: marker,
                                    clusterData: clusterMarkerData,
                                    itemIds: itemIds
                                });
                                displayedItemsByCluster.set(clusterId, itemIds);
                                addedCount++;
                            }
                        }
                    }
                }
                
                // Remove clusters that are no longer in the response
                var removedCount = 0;
                for (let [clusterId, clusterData] of displayedClusters.entries()) {
                    if (!clustersInResponse.has(clusterId)) {
                        // Remove marker from map
                        if (clusterData.marker) {
                            markers.removeLayer(clusterData.marker);
                        }
                        // Remove from tracking
                        displayedClusters.delete(clusterId);
                        displayedItemsByCluster.delete(clusterId);
                        removedCount++;
                    }
                }
                
                // Mark that data was received
                if (typeof markDataReceived === 'function') {
                    markDataReceived();
                } else if (typeof window.markDataReceived === 'function') {
                    window.markDataReceived();
                }

            }

            // Clear request reference
            currentMapRefreshRequest = null;

            clearTimeout(refresh_items_timeout);
            refresh_items_timeout = setTimeout(function(){refresh_all_items()}, 5000);  // Poll every 5 seconds
        }
    }

// Sync locationStore from clusters (fast path for circle mode)
function syncLocationStoreFromClusters(clusters) {
    if (!window.locationStore) return;

    // Track which locations should exist
    const locationKeysInClusters = new Set();

    for (let cluster of clusters) {
        const locationKey = cluster.location_key;
        if (!locationKey) continue;

        locationKeysInClusters.add(locationKey);

        const clusterData = {
            type: 'cluster',
            cluster_id: cluster.cluster_id,
            cluster_title: cluster.title,
            cluster_item_count: cluster.item_count,
            cluster_items: cluster.items || [],
            locationName: cluster.representative_location_name
        };

        const existingLocation = window.locationStore.getLocation(locationKey);
        if (existingLocation) {
            window.locationStore.updateItem(locationKey, clusterData);
        } else {
            window.locationStore.addItem(locationKey, cluster.representative_lat, cluster.representative_lon, clusterData, cluster.representative_location_name);
        }
    }

    // Note: LocationStore is a read-only wrapper around DataManager.
    // DataManager handles location removal automatically, so we don't need to
    // explicitly remove locations from LocationStore.
}

// Function to fetch all clusters and store in cache (without time filter)
function updateClusterCache() {
    // DataManager handles all data fetching and caching now
    // Just ensure clusterCache is updated from DataManager data
    if (window.dataManager) {
        // Get all clusters without time/RSS filter (for cache)
        const allClusters = window.dataManager.getClusters();
        window.clusterCache = allClusters;
        //console.log('Cluster cache updated from DataManager:', window.clusterCache.length, 'clusters');

        // Note: We don't call refreshMarkerClusters here anymore since TimeFilterManager
        // now handles map updates through refresh_all_items() which applies current filters
    } else {
        console.warn('DataManager not available for cluster cache update');
    }
}

// Start fetching all items (tweets and news combined)
refresh_all_items();

// Initialize cluster cache and set up periodic updates (every 10 seconds)
updateClusterCache();
if (clusterCacheUpdateInterval) {
    clearInterval(clusterCacheUpdateInterval);
}
clusterCacheUpdateInterval = setInterval(updateClusterCache, 10000); // Update every 10 seconds

// Time filter callbacks are now handled by timeFilterManager
// The manager coordinates all filtering operations consistently

function addmarker(data, options) {
    // Extract fields from raw data object
    var lat = data.lat;
    var lng = data.lng;
    var source = data.source || 'unknown';
    var type = data.type || 'news';
    var locationName = data.location_name || data.location;
    var createdAt = data.published_at || data.createdAt;
    
    // Use location_key from backend (normalized lowercase)
    var locationKey = data.location_key;
    
    // Generate HTML from raw data
    var content_popup = type === 'news' 
        ? generateNewsHtml(data) 
        : generateTweetHtml(data);
    var content_tooltip = data.text || data.title || '';
    
    // Extract keywords from text (hashtags and mentions)
    var keywords = [];
    var textForKeywords = data.text || data.title || '';
    var hashtagMatches = textForKeywords.match(/#\w+/g);
    if (hashtagMatches) {
        keywords.push(...hashtagMatches.map(h => h.toLowerCase()));
    }
    var mentionMatches = textForKeywords.match(/@\w+/g);
    if (mentionMatches) {
        keywords.push(...mentionMatches.slice(0, 3));
    }
    
    // Add item to location store using location_key from backend
    // This is shared between 2D and 3D modes
    // Skip if options.skipLocationStore is true (e.g., when re-adding from refreshMarkerClusters)
    if (window.locationStore && locationKey && (!options || !options.skipLocationStore)) {
        window.locationStore.addItem(locationKey, lat, lng, {
            // Raw data fields for sidebar
            text: data.text,
            title: data.title,
            authorName: data.authorName,
            authorScreenName: data.authorScreenName,
            author: data.author,
            url: data.url,
            createdAt: createdAt,
            description: data.description,
            media: data.media,
            quotedTweet: data.quotedTweet,
            type: type,
            // Generated content for balloon
            html: content_popup,
            tooltip: content_tooltip,
            source: source,
            keywords: keywords,
            locationName: locationName
        }, locationName);
    }
    
    // Check current mode - only process for the active mode
    var currentMode = window.mapInterface ? window.mapInterface.getMode() : '2d';
    
    if (currentMode === '3d') {
        // In 3D mode: add to Cesium only, skip all 2D processing
        if (window.mapInterface) {
            window.mapInterface.addMarker(locationKey || `${lat},${lng}`, lat, lng, {
                html: content_popup,
                tooltip: content_tooltip,
                source: source
            });
        }
        return; // Skip all 2D marker/popup creation
    }
    
    // 2D mode processing below
    
    // Skip adding individual markers when circle mode is enabled
    // Circle mode only shows aggregated circles, not individual markers
    if (circleModeEnabled) {
        // In circle mode, circles are refreshed by the location store events
        // We don't need to add individual markers
        return;
    }

    var marker = L.marker([lat,lng],{icon: pulse_icon});
    marker.locationKey = locationKey;  // Store location key for selection
    marker.itemCreatedAt = createdAt;  // Store item's createdAt for filtering
    marker.itemUrl = data.url;  // Store item URL for identification
    marker.itemText = data.text || data.title;  // Store item text for identification
    
    
    marker.bindTooltip(content_tooltip,{
        permanent: false,
        autoPan:false,
        className: 'marker_tooltip',
    });

    var popup = L.popup({
        autoClose:false,
        autoPan:false,
        closePopupOnClick:false,
    })
    .setLatLng([lat,lng])
    .setContent(content_popup);

    marker.popup_store = popup;

    marker.on('click', function() {
        popup.openOn(window.map);
        marker.setIcon(static_icon);
        marker.alert_on = false;
        markers.refreshClusters(marker);
        
        // Notify map interface of selection (for sidebar)
        if (window.mapInterface && marker.locationKey) {
            window.mapInterface.selectLocation(marker.locationKey, { openSidebar: window.sidebarEnabled });
        }
    });
    

    marker.alert_on = true;

    setTimeout(function() {
        marker.alert_on = false;
        marker.setIcon(static_icon);
        markers.refreshClusters(marker);
    }, 5000);

    markers.addLayer(marker);
    map.addLayer(markers);

    if (cluster_selected != null && typeof cluster_selected.spiderfy === 'function') {
        try {
            cluster_selected.spiderfy();
            setTimeout(function(){
                if (cluster_selected != null && typeof cluster_selected.spiderfy === 'function') {
                    cluster_selected.spiderfy();
                }
            }, 20);
        } catch (e) {
            // Cluster reference is stale, clear it
            cluster_selected = null;
        }
    }
}

/**
 * Format all tweet and news times in balloons using Luxon
 * Converts ISO timestamps to relative time ("2 hours ago")
 */
function formatBalloonTimes() {
    // Check if Luxon formatting function is available
    if (typeof window.formatRelativeTime !== 'function') {
        console.warn('formatRelativeTime function not available, skipping time formatting');
        return;
    }
    
    // Find all elements with data-timestamp attribute (tweets and news)
    // Selectors: .tweet-time[data-timestamp] for tweets, .news-time[data-timestamp] for news
    document.querySelectorAll('.tweet-time[data-timestamp], .news-time[data-timestamp]').forEach(function(el) {
        var timestamp = el.getAttribute('data-timestamp');
        if (timestamp && timestamp.length > 0) {
            try {
                // Use global Luxon formatting function
                var relativeTime = window.formatRelativeTime(timestamp);
                if (relativeTime) {
                    el.textContent = relativeTime;
                }
            } catch (e) {
                console.warn('Error formatting time:', e);
            }
        }
    });
}

// Make function globally available
window.formatBalloonTimes = formatBalloonTimes;

/**
 * Toggle cluster items display in balloon
 * @param {string} clusterId - Cluster ID
 * @param {HTMLElement} button - Toggle button element
 */
function toggleClusterItems(clusterId, button) {
    const itemsContainer = document.getElementById(`cluster-items-${clusterId}`);
    const icon = button.querySelector('i');

    if (itemsContainer.style.display === 'none') {
        itemsContainer.style.display = 'block';
        button.classList.add('expanded');
        icon.classList.remove('fa-chevron-down');
        icon.classList.add('fa-chevron-up');
        expandedClusters.add(clusterId);
    } else {
        itemsContainer.style.display = 'none';
        button.classList.remove('expanded');
        icon.classList.remove('fa-chevron-up');
        icon.classList.add('fa-chevron-down');
        expandedClusters.delete(clusterId);
    }
}

// Make function globally available
window.toggleClusterItems = toggleClusterItems;

/**
 * Open cluster location in sidebar
 * @param {string} locationKey - Location key for the cluster
 */
function openClusterInSidebar(locationKey) {
    if (!locationKey) {
        console.warn('No location key provided for sidebar');
        return;
    }

    if (window.newsSidebar) {
        window.newsSidebar.selectLocation(locationKey);
    } else if (window.mapInterface) {
        window.mapInterface.selectLocation(locationKey, { openSidebar: true });
    } else {
        console.warn('No sidebar or mapInterface available');
    }
}

// Make function globally available
window.openClusterInSidebar = openClusterInSidebar;

/**
 * Generate HTML for cluster balloon
 * @param {object} clusterData - Cluster data object
 * @returns {string} HTML string for balloon content
 */
function generateClusterHtml(clusterData) {
    const clusterTitle = clusterData.title || clusterData.cluster_title || 'Untitled Cluster';
    const items = clusterData.items || clusterData.cluster_items || [];
    const itemCount = clusterData.item_count || clusterData.cluster_item_count || items.length;
    const locationName = clusterData.representative_location_name || clusterData.location_name || '';
    const clusterId = clusterData.cluster_id;
    const isExpanded = expandedClusters.has(clusterId);

    // Normalize source string for matching
    const normSource = (s) => String(s || '').trim().toLowerCase();

    // Font Awesome icon mapping (brands where possible, solid otherwise)
    // Fallback is a generic news icon.
    const SOURCE_ICON = {
        gdelt:   { prefix: 'fa-solid',  name: 'globe' },
        gdacs:   { prefix: 'fa-solid',  name: 'triangle-exclamation' },
        usgs:    { prefix: 'fa-solid',  name: 'mountain' },
        telegram:{ prefix: 'fa-brands', name: 'telegram' },
        mastodon:{ prefix: 'fa-brands', name: 'mastodon' },
        rss:     { prefix: 'fa-solid',  name: 'rss' },
        news:    { prefix: 'fa-solid',  name: 'newspaper' }, // generic
    };

    const getIconHtml = (source) => {
        const key = normSource(source);
        const icon = SOURCE_ICON[key] || SOURCE_ICON.news;
        return `<i class="${icon.prefix} fa-${icon.name}"></i>`;
    };

    const getLinkClass = (source) => {
        const key = normSource(source);
        return 'news';
    };

    let html = `
        <div class="cluster-popup">
            <div class="cluster-popup-header">
                <h3 class="cluster-popup-title">${escapeHtml(clusterTitle)}</h3>
                <div class="cluster-popup-meta">
                    <span class="cluster-item-count">${itemCount} item${itemCount !== 1 ? 's' : ''}</span>
                    ${locationName ? `<span class="cluster-location">${escapeHtml(locationName)}</span>` : ''}
                    <button class="cluster-sidebar-button" onclick="window.openClusterInSidebar('${clusterData.location_key || ''}')" title="Open in sidebar">
                        <i class="fa-solid fa-list"></i>
                    </button>
                    <button class="cluster-expand-toggle ${isExpanded ? 'expanded' : ''}" onclick="window.toggleClusterItems('${clusterId}', this)">
                        <i class="fa-solid fa-${isExpanded ? 'chevron-up' : 'chevron-down'}"></i>
                    </button>
                </div>
            </div>
            <div class="cluster-popup-items" id="cluster-items-${clusterId}" style="display: ${isExpanded ? 'block' : 'none'};">
    `;

    const previewItems = items.slice(0, 5);
    for (const item of previewItems) {
        const url = item.url || '#';

        let sourceName = null;
        if (item.title !== null && item.title !== undefined) {
            sourceName = (item.source || 'unknown') + ": " + String(item.title).substring(0, 50);
        } else if (item.text !== null && item.text !== undefined) {
            sourceName = (item.source || 'unknown') + ": " + String(item.text).substring(0, 50);
        } else {
            sourceName = (item.source || 'unknown') + ": " + "no data";
        }

        html += `
            <div class="cluster-item-link">
                <a href="${url}" target="_blank" class="cluster-source-link ${getLinkClass(item.source)}">
                    ${getIconHtml(item.source)}
                    ${escapeHtml(sourceName)}
                </a>
                <span class="cluster-item-time" data-timestamp="${item.published_at || ''}"></span>
            </div>
        `;
    }

    if (items.length > 3) {
        html += `<div class="cluster-more-items">+ ${items.length - 3} more items</div>`;
    }

    html += `
            </div>
        </div>
    `;

    return html;
}

/**
 * Helper function to escape HTML
 */
function escapeHtml(text) {
    if (!text) return '';
    const div = document.createElement('div');
    div.textContent = text;
    return div.innerHTML;
}

// Make globally available
window.escapeHtml = escapeHtml;

/**
 * Add a cluster marker to the map and return the marker
 * @param {object} clusterData - Cluster data with location and cluster info
 * @returns {L.Marker} The created marker
 */
function addClusterMarkerAndReturn(clusterData) {
    return addClusterMarker(clusterData);
}

/**
 * Add a cluster marker to the map
 * @param {object} clusterData - Cluster data with location and cluster info
 * @returns {L.Marker} The created marker (for tracking)
 */
function addClusterMarker(clusterData) {
    var lat = clusterData.lat;
    var lng = clusterData.lng;
    var locationKey = clusterData.location_key;
    var locationName = clusterData.location_name;
    var clusterId = clusterData.cluster_id;
    var clusterTitle = clusterData.title || clusterData.cluster_title || 'Cluster';
    var itemCount = clusterData.item_count || clusterData.cluster_item_count || 0;
    
    // Generate cluster popup content
    var content_popup = generateClusterHtml(clusterData);
    var content_tooltip = `${clusterTitle} (${itemCount} items)`;
    
    // Add cluster to location store (for sidebar)
    if (window.locationStore && locationKey) {
        var location = window.locationStore.getLocation(locationKey);
        var clusterExists = location && location.items && location.items.some(function(item) {
            return item.type === 'cluster' && item.cluster_id === clusterId;
        });
        
        var clusterItemData = {
            type: 'cluster',
            cluster_id: clusterId,
            cluster_title: clusterTitle,
            cluster_item_count: itemCount,
            cluster_items: clusterData.cluster_items || [],
            html: content_popup,
            tooltip: content_tooltip,
            locationName: locationName
        };
        
        if (clusterExists) {
            window.locationStore.updateItem(locationKey, clusterItemData);
        } else {
            window.locationStore.addItem(locationKey, lat, lng, clusterItemData, locationName);
        }
    }
    
    // Check current mode
    var currentMode = window.mapInterface ? window.mapInterface.getMode() : '2d';

    if (currentMode === '3d') {
        // In 3D mode: add to Cesium only
        if (window.mapInterface) {
            window.mapInterface.addMarker(locationKey || `${lat},${lng}`, lat, lng, {
                html: content_popup,
                tooltip: content_tooltip,
                source: 'cluster'
            });
        }
        return;
    }
    
    // 2D mode processing
    if (circleModeEnabled) {
        // In circle mode, circles are refreshed by location store events
        return;
    }
    
    var marker = L.marker([lat, lng], {icon: pulse_icon});
    marker.locationKey = locationKey;
    marker.clusterId = clusterId;
    marker.clusterItemCount = itemCount;
    
    marker.bindTooltip(content_tooltip, {
        permanent: false,
        autoPan: false,
        className: 'marker_tooltip',
    });
    
    var popup = L.popup({
        autoClose: false,
        autoPan: false,
        closePopupOnClick: false,
    })
    .setLatLng([lat, lng])
    .setContent(content_popup);
    
    marker.popup_store = popup;
    
    marker.on('click', function() {
        popup.openOn(window.map);
        marker.setIcon(static_icon);
        marker.alert_on = false;
        markers.refreshClusters(marker);
        
        // Open sidebar with this location's clusters
        if (window.mapInterface && marker.locationKey) {
            window.mapInterface.selectLocation(marker.locationKey, { openSidebar: window.sidebarEnabled });
        }
    });
    
    
    marker.alert_on = true;
    
    setTimeout(function() {
        marker.alert_on = false;
        marker.setIcon(static_icon);
        markers.refreshClusters(marker);
    }, 5000);
    
    markers.addLayer(marker);
    map.addLayer(markers);

    if (cluster_selected != null && typeof cluster_selected.spiderfy === 'function') {
        try {
            cluster_selected.spiderfy();
            setTimeout(function() {
                if (cluster_selected != null && typeof cluster_selected.spiderfy === 'function') {
                    cluster_selected.spiderfy();
                }
            }, 20);
        } catch (e) {
            // Cluster reference is stale, clear it
            cluster_selected = null;
        }
    }

    return marker; // Return marker for tracking
}

/**
 * Generate HTML for news balloon from raw news data
 * @param {object} news - Raw news data object
 * @returns {string} HTML string for balloon content
 */
function generateNewsHtml(news) {
    const timeAgo = typeof window.formatRelativeTime === 'function' && news.published_at
        ? window.formatRelativeTime(news.published_at)
        : '';
    
    // Escape HTML to prevent XSS
    
    const description = news.description 
        ? escapeHtml(news.description.substring(0, 200)) + (news.description.length > 200 ? '...' : '')
        : '';
    
    // Add data-timestamp attribute for periodic updates
    const timestampAttr = news.published_at ? ` data-timestamp="${escapeHtml(news.published_at)}"` : '';
    
    return `
        <div class="news-popup">
            <div class="news-source">${escapeHtml(news.author || 'Unknown')}</div>
            <a href="${news.url}" target="_blank" class="news-title">${escapeHtml(news.title || 'No title')}</a>
            <div class="news-description">${description}</div>
            <div class="news-time"${timestampAttr}>${timeAgo}</div>
        </div>
    `;
}

// Stub function for removed Twitter functionality
function generateTweetHtml(data) {
    return `
        <div class="news-item">
            <div class="news-title">Twitter content removed</div>
            <div class="news-text">Twitter scraping functionality has been disabled.</div>
        </div>
    `;
}

// Make HTML generators globally available
window.generateTweetHtml = generateTweetHtml;
window.generateNewsHtml = generateNewsHtml;

// Global flag for sidebar state
window.sidebarEnabled = false;

// ============================================
// 2D Circle Mode Layer
// ============================================

// Create a layer group for circle markers
var circleMarkersLayer = L.layerGroup();
var circleModeEnabled = false;

// Circle tracking for incremental updates
const circlesByKey = new Map();
const dirtyKeys = new Set();
let circleRefreshScheduled = false;

// Rainbow gradient for circle colors (pink = old, red = new)
var circleRainbow = new Rainbow();
circleRainbow.setSpectrum("#FFC0CB", "#990000");

/**
 * Toggle between marker cluster mode and circle mode in 2D
 */
function setCircleMode(enabled) {
    circleModeEnabled = enabled;
    
    if (enabled) {
        // Hide marker clusters, show circle layer
        map.removeLayer(markers);
        map.addLayer(circleMarkersLayer);
        // Mark all locations as dirty for initial circle creation
        if (window.locationStore) {
            const locations = window.locationStore.getAllLocations();
            for (const key of Object.keys(locations)) {
                dirtyKeys.add(key);
            }
            refreshDirtyCircles();
        }
    } else {
        // Hide circle layer, show marker clusters
        map.removeLayer(circleMarkersLayer);
        map.addLayer(markers);
    }
}

// Make circleModeEnabled globally accessible
window.isCircleModeEnabled = function() {
    return circleModeEnabled;
};

// Make circle count globally accessible for testing
window.getCircleCount = function() {
    return circleMarkersLayer.getLayers().length;
};

/**
 * Refresh circle markers based on location store
 */
function refreshCircleMarkers() {
    console.time('refreshCircleMarkers');
    if (!circleModeEnabled || !window.locationStore) {
        // console.log('refreshCircleMarkers: Skipping - circleModeEnabled:', circleModeEnabled, 'locationStore:', !!window.locationStore);
        console.timeEnd('refreshCircleMarkers');
        return;
    }

    // console.log('refreshCircleMarkers: Starting refresh');

    // Clear existing circles
    circleMarkersLayer.clearLayers();

    const locations = window.locationStore.getAllLocations();

    for (const [key, location] of Object.entries(locations)) {
        const { lat, lng, items, locationName } = location;

        // Handle both individual items and cluster items
        let filteredItems = [];
        let totalCount = 0;

        // Check if this location has clusters or individual items
        const hasClusters = items.some(item => item.type === 'cluster');
        const hasIndividualItems = items.some(item => item.type !== 'cluster');

        if (hasClusters) {
            // Process cluster items
            for (const item of items) {
                if (item.type === 'cluster' && item.cluster_items) {
                    // Filter cluster items by time if needed
                    let clusterFilteredItems = item.cluster_items;
                    if (window.timeFilterManager && window.timeFilterManager.isFilterActive()) {
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
            // Process individual items (legacy behavior)
            filteredItems = items;
            if (window.timeFilterManager && window.timeFilterManager.isFilterActive()) {
                filteredItems = window.timeFilterPanel.filterItems(items);
            }
            totalCount = filteredItems.length;
        }

        // Skip locations with no items after filtering
        if (totalCount === 0) {
            continue;
        }

        const count = totalCount;
        
        // Calculate color based on time filter settings
        let colorHex = '#990000';  // Default red
        if (window.timeFilterManager && window.timeFilterManager.isFilterActive()) {
            // For clusters, find the latest item across all cluster items
            let latestItem = null;
            let latestTime = 0;

            if (hasClusters) {
                for (const clusterItem of filteredItems) {
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
            } else {
                // Individual items - use existing logic
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
            // Fallback to old method if time filter not active
            const now = Date.now();
            const oneWeekAgo = now - (7 * 24 * 60 * 60 * 1000);
            const lastUpdate = location.lastUpdate || now;
            const timePercentage = Math.max(0, Math.min(1, (lastUpdate - oneWeekAgo) / (now - oneWeekAgo)));
            colorHex = "#" + circleRainbow.colorAt(timePercentage * 100);
        }
        
        // Calculate radius using EXACT same formula as Cesium circles
        // Formula: this._erf(height / 3e6) * Math.log(count + 1.5) * 110000
        // Use L.circle with radius in meters (not L.circleMarker with pixels)
        // This makes circles maintain fixed geographic size like in Cesium
        
        // Use a fixed reference height for 2D (equivalent to typical Cesium view)
        // This ensures circles maintain consistent size regardless of zoom
        const height = 5e6;  // Fixed reference height in meters
        
        // Error function approximation (EXACT same as Cesium _erf method)
        const erf = (x) => {
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
        };
        
        // Calculate radius in meters using EXACT same formula as Cesium (no min/max bounds)
        const radius = erf(height / 3e6) * Math.log(count + 1.5) * 110000;
        
        // Create circle with radius in meters (not circleMarker with pixels)
        // L.circle uses meters and maintains geographic size like Cesium
        const circle = L.circle([lat, lng], {
            radius: radius,
            fillColor: colorHex,
            color: colorHex,
            weight: 2,
            opacity: 0.9,
            fillOpacity: 0.6
        });
        
        // Store location key for selection
        circle.locationKey = key;
        
        // Add tooltip with location name and count
        const tooltipText = locationName ? `${locationName} (${count} items)` : `${count} items`;
        circle.bindTooltip(tooltipText, {
            permanent: false,
            direction: 'top',
            className: 'marker_tooltip'
        });
        
        // Click handler - open sidebar and sync button
        circle.on('click', function() {
            if (window.mapInterface && circle.locationKey) {
                window.mapInterface.selectLocation(circle.locationKey, { openSidebar: true });
                
                // Sync sidebar button state
                syncSidebarButtonState(true);
                
                // Also fly to location
                map.setView([lat, lng], map.getZoom());
            }
        });
        
        circleMarkersLayer.addLayer(circle);
    }

    console.log('refreshCircleMarkers: Processed', Object.keys(locations).length, 'locations');
    // console.log('refreshCircleMarkers: Completed - added', Object.keys(locations).length, 'circles');
    console.timeEnd('refreshCircleMarkers');
}

// Incremental circle refresh - only update changed circles
function refreshDirtyCircles() {
    console.time('refreshDirtyCircles');
    if (!circleModeEnabled || !window.locationStore) {
        console.timeEnd('refreshDirtyCircles');
        return;
    }

    const locations = window.locationStore.getAllLocations();
    const locationKeys = new Set(Object.keys(locations));

    // Process dirty keys
    for (const key of dirtyKeys) {
        const location = locations[key];

        if (!location) {
            // Location was removed - remove circle if it exists
            const existingCircle = circlesByKey.get(key);
            if (existingCircle) {
                circleMarkersLayer.removeLayer(existingCircle);
                circlesByKey.delete(key);
            }
            continue;
        }

        const { lat, lng, items, locationName } = location;

        // Calculate aggregated data for this location
        const aggregated = calculateLocationAggregates(location);

        if (aggregated.totalCount === 0) {
            // No items after filtering - remove circle if it exists
            const existingCircle = circlesByKey.get(key);
            if (existingCircle) {
                circleMarkersLayer.removeLayer(existingCircle);
                circlesByKey.delete(key);
            }
            continue;
        }

        // Create or update circle
        const existingCircle = circlesByKey.get(key);
        if (existingCircle) {
            // Update existing circle
            updateCircle(existingCircle, aggregated, lat, lng, locationName, key);
        } else {
            // Create new circle
            const circle = createCircle(aggregated, lat, lng, locationName, key);
            circlesByKey.set(key, circle);
            circleMarkersLayer.addLayer(circle);
        }
    }

    // Remove circles for locations that no longer exist
    for (const [key, circle] of circlesByKey.entries()) {
        if (!locationKeys.has(key)) {
            circleMarkersLayer.removeLayer(circle);
            circlesByKey.delete(key);
        }
    }

    console.log('refreshDirtyCircles: Processed', dirtyKeys.size, 'dirty locations out of', locationKeys.size, 'total locations');
    // Clear dirty keys
    dirtyKeys.clear();
    circleRefreshScheduled = false;
    console.timeEnd('refreshDirtyCircles');
}

// Calculate aggregated data for a location (cached values would be better but this works)
function calculateLocationAggregates(location) {
    const { items } = location;
    let totalCount = 0;
    let latestTime = 0;
    let latestItem = null;

    // LocationStore.getAllLocations() returns flattened items array
    // Filter by time if needed
    let filteredItems = items;
    if (window.timeFilterManager && window.timeFilterManager.isFilterActive()) {
        filteredItems = window.timeFilterPanel.filterItems(items);
    }

    // Process filtered items
    for (const item of filteredItems) {
        if (item.published_at) {
            const itemTime = new Date(item.published_at).getTime();
            if (itemTime > latestTime) {
                latestTime = itemTime;
                latestItem = item;
            }
        }
    }
    totalCount = filteredItems.length;

    return { totalCount, latestTime, latestItem };
}

// Update existing circle properties
function updateCircle(circle, aggregated, lat, lng, locationName, key) {
    const { totalCount, latestItem } = aggregated;

    // Calculate color
    let colorHex = '#990000'; // Default red
    if (window.timeFilterManager && window.timeFilterManager.isFilterActive() && latestItem) {
        colorHex = window.timeFilterPanel.getColorForItem(latestItem);
    }

    // Calculate radius using same formula as before
    const height = 5e6;
    const erf = (x) => {
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
    };
    const radius = erf(height / 3e6) * Math.log(totalCount + 1.5) * 110000;

    // Update circle properties
    circle.setLatLng([lat, lng]);
    circle.setRadius(radius);
    circle.setStyle({
        fillColor: colorHex,
        color: colorHex
    });

    // Update tooltip
    const tooltipText = locationName ? `${locationName} (${totalCount} items)` : `${totalCount} items`;
    circle.setTooltipContent(tooltipText);
}

// Create new circle
function createCircle(aggregated, lat, lng, locationName, key) {
    const { totalCount, latestItem } = aggregated;

    // Calculate color
    let colorHex = '#990000'; // Default red
    if (window.timeFilterManager && window.timeFilterManager.isFilterActive() && latestItem) {
        colorHex = window.timeFilterPanel.getColorForItem(latestItem);
    }

    // Calculate radius
    const height = 5e6;
    const erf = (x) => {
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
    };
    const radius = erf(height / 3e6) * Math.log(totalCount + 1.5) * 110000;

    // Create circle
    const circle = L.circle([lat, lng], {
        radius: radius,
        fillColor: colorHex,
        color: colorHex,
        weight: 2,
        opacity: 0.9,
        fillOpacity: 0.6
    });

    // Store location key for selection
    circle.locationKey = key;

    // Add tooltip
    const tooltipText = locationName ? `${locationName} (${totalCount} items)` : `${totalCount} items`;
    circle.bindTooltip(tooltipText, {
        permanent: false,
        direction: 'top',
        className: 'marker_tooltip'
    });

    // Click handler
    circle.on('click', function() {
        if (window.mapInterface && circle.locationKey) {
            window.mapInterface.selectLocation(circle.locationKey, { openSidebar: true });
            syncSidebarButtonState(true);
            map.setView([lat, lng], map.getZoom());
        }
    });

    return circle;
}

// Schedule circle refresh (coalesces multiple updates)
function scheduleCircleRefresh(key) {
    if (key) {
        dirtyKeys.add(key);
    }

    if (!circleRefreshScheduled) {
        circleRefreshScheduled = true;
        requestAnimationFrame(() => refreshDirtyCircles());
    }
}

// Make function globally accessible
window.refreshCircleMarkers = refreshCircleMarkers;

/**
 * Sync sidebar button checked state with actual sidebar visibility
 */
function syncSidebarButtonState(visible) {
    var button_sidebar = document.getElementById('button_sidebar');
    if (button_sidebar) {
        button_sidebar.checked = visible;
        window.sidebarEnabled = visible;
    }
}

// Make sync function globally accessible
window.syncSidebarButtonState = syncSidebarButtonState;

/**
 * Refresh marker clusters by filtering based on time range
 * This function is called when the time filter changes
 * Rebuilds markers from cache to handle both adding and removing
 */
function refreshMarkerClusters() {
    if (!markers || circleModeEnabled || !window.clusterCache) return;

    // console.log('refreshMarkerClusters: Starting refresh');

    // Get all markers on map
    var allMarkers = [];
    var allLayers = markers.getLayers();
    for (var layer of allLayers) {
        if (layer.getAllChildMarkers) {
            allMarkers.push(...layer.getAllChildMarkers());
        } else {
            allMarkers.push(layer);
        }
    }

    // Determine which clusters should be visible based on time filter
    var validClusterIds = new Set();
    var timeFilterActive = window.timeFilterManager && window.timeFilterManager.isFilterActive();

    for (var cluster of window.clusterCache) {
        var clusterId = cluster.cluster_id;
        var clusterItems = cluster.items || [];

        if (timeFilterActive) {
            // Check if cluster has any items that pass the time filter
            var filtered = window.timeFilterPanel.filterItems(clusterItems);
            if (filtered.length > 0) {
                validClusterIds.add(clusterId);
            }
        } else if (clusterItems.length > 0) {
            // No time filter, show all clusters with items
            validClusterIds.add(clusterId);
        }
    }

    // console.log('refreshMarkerClusters: Valid cluster IDs:', validClusterIds.size, 'out of', window.clusterCache.length);

    // Track current markers by clusterId
    var currentMarkerIds = new Set();
    for (var marker of allMarkers) {
        if (marker.clusterId) {
            currentMarkerIds.add(marker.clusterId);
        }
    }

    // Remove markers for clusters that don't pass filter
    var removed = 0;
    for (var marker of allMarkers) {
        if (marker.clusterId && !validClusterIds.has(marker.clusterId)) {
            try {
                //console.log('refreshMarkerClusters: Removing marker for cluster:', marker.clusterId);
                markers.removeLayer(marker);
                removed++;
            } catch (e) {
                console.warn('refreshMarkerClusters: Error removing marker:', e);
            }
        }
    }

    // Add markers for clusters that pass filter but don't have markers yet
    var added = 0;
    for (var cluster of window.clusterCache) {
        var clusterId = cluster.cluster_id;

        if (validClusterIds.has(clusterId) && !currentMarkerIds.has(clusterId)) {
            // Create marker data from cluster (DataManager format)
            var clusterLat = cluster.representative_lat;
            var clusterLng = cluster.representative_lon;
            var clusterLocationKey = cluster.location_key;
            var clusterLocationName = cluster.representative_location_name;
            var clusterItems = cluster.items || [];

            // Skip if no coordinates (location_key is optional)
            if (!clusterLat || !clusterLng) {
                console.warn('refreshMarkerClusters: Skipping cluster without coordinates:', clusterId);
                continue;
            }

            // Create cluster marker data
            var clusterMarkerData = {
                lat: clusterLat,
                lng: clusterLng,
                location_key: clusterLocationKey,
                location_name: clusterLocationName,
                type: 'cluster',
                cluster_id: clusterId,
                title: cluster.title,
                item_count: cluster.item_count,
                items: clusterItems,
                createdAt: cluster.last_seen_at || cluster.first_seen_at
            };

            //console.log('refreshMarkerClusters: Adding marker for cluster:', clusterId);
            var marker = addClusterMarkerAndReturn(clusterMarkerData);
            if (marker) {
                added++;
            }
        }
    }

    //console.log('refreshMarkerClusters: Completed - added:', added, 'removed:', removed);
}

// Make function globally accessible
window.refreshMarkerClusters = refreshMarkerClusters;

// Set up circle mode toggle handler
document.addEventListener('DOMContentLoaded', function() {
    var viz2dCircles = document.getElementById('viz_2d_circles');
    if (viz2dCircles) {
        viz2dCircles.addEventListener('change', function() {
            setCircleMode(viz2dCircles.checked);
        });
    }
    
        // Listen to location store updates to refresh circles incrementally
        if (window.locationStore) {
            window.locationStore.on('locationAdded', function(event) {
                if (circleModeEnabled && event.key) {
                    scheduleCircleRefresh(event.key);
                }
            });
            window.locationStore.on('locationUpdated', function(event) {
                if (circleModeEnabled && event.key) {
                    scheduleCircleRefresh(event.key);
                }
            });

        // Listen for DataManager updates to refresh cluster cache and map
        if (window.dataManager) {
            window.dataManager.on('dataUpdated', function() {
                updateClusterCache();
                // Trigger immediate map refresh when new data arrives
                refresh_all_items();
            });
        }
    }
});
