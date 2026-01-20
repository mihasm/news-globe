/**
 * Sidebar - Display location-grouped news/tweets
 * Adapted from News Globe's NewsContainer
 */

class NewsSidebar {
    constructor(containerId = 'news_sidebar') {
        this.containerId = containerId;
        this.container = null;
        this.isVisible = false;
        this.currentLocation = null;
        this.currentLocationKey = null;  // Track current location key for auto-refresh
        this.articles = [];
        this.displayedItemIds = new Set();  // Track displayed items
        this.displayedClusters = new Map();  // Track displayed clusters: clusterId -> { element, clusterData, itemIds: Set }
        this.refreshTimer = null;  // Debounce timer for auto-refresh calls

        this._createContainer();
        this._setupAutoRefresh();
    }
    
    /**
     * Setup auto-refresh when DataManager is updated
     */
    _setupAutoRefresh() {
        // Wait for DataManager to be available
        const setupListeners = () => {
            if (window.dataManager) {
                // Listen for data updates from DataManager
                window.dataManager.on('dataUpdated', () => {
                    if (this.isVisible && this.currentLocationKey) {
                        this._scheduleRefresh();
                    }
                });
            } else {
                // Retry after a short delay
                setTimeout(setupListeners, 100);
            }
        };

        setupListeners();
    }
    
    /**
     * Schedule a refresh with debouncing to prevent rapid-fire calls
     */
    _scheduleRefresh() {
        // Clear any existing refresh timer
        if (this.refreshTimer) {
            clearTimeout(this.refreshTimer);
            this.refreshTimer = null;
        }
        
        // Schedule refresh after a short delay (debounce)
        this.refreshTimer = setTimeout(() => {
            this.refreshTimer = null;
            this._refreshCurrentLocation();
        }, 500); // 500ms debounce - adjust as needed
    }
    
    /**
     * Refresh the sidebar with current location data (auto-refresh only - no show/hide)
     * Now uses DataManager instead of direct AJAX calls
     */
    _refreshCurrentLocation() {
        if (!this.currentLocationKey) return;

        // Store locationKey at start
        const refreshLocationKey = this.currentLocationKey;

        // Get data from DataManager instead of AJAX
        if (!window.dataManager) {
            console.warn('DataManager not available for sidebar refresh');
            return;
        }

        // Build filter parameters for DataManager
        const filters = {
            limit: 10000
        };

        // Add time filter parameters if enabled (use centralized manager)
        if (window.timeFilterManager && window.timeFilterManager.isFilterActive()) {
            const timeParams = window.timeFilterManager.getTimeFilterParams();
            if (timeParams.time_from) filters.timeFrom = timeParams.time_from;
            if (timeParams.time_to) filters.timeTo = timeParams.time_to;
        }

        // Get clusters for this location from DataManager
        const clusters = window.dataManager.getClustersForLocation(refreshLocationKey, filters);

        // Get location name - priority: cluster's location_name, then locationStore, then location_key
        var locationName = null;
        if (clusters.length > 0) {
            locationName = clusters[0].location_name;
        }
        if (!locationName && window.locationStore) {
            var loc = window.locationStore.getLocation(refreshLocationKey);
            if (loc) {
                locationName = loc.locationName;
            }
        }
        if (!locationName) {
            locationName = refreshLocationKey;
        }

        // Update the sidebar with the clusters
        this.updateClusters(clusters, locationName, refreshLocationKey, refreshLocationKey);
    }
    
    _createContainer() {
        // Check if container already exists
        if (document.getElementById(this.containerId)) {
            this.container = document.getElementById(this.containerId);
            return;
        }
        
        // Create sidebar container
        const sidebar = document.createElement('div');
        sidebar.id = this.containerId;
        sidebar.className = 'news-sidebar';
        sidebar.innerHTML = `
            <div class="sidebar-location">
                <i class="fa-solid fa-location-dot"></i>
                <span class="sidebar-location-text">Location</span>
                <button class="sidebar-close" onclick="window.newsSidebar.hide()">×</button>
            </div>
            <div class="sidebar-content"></div>
        `;
        
        document.body.appendChild(sidebar);
        this.container = sidebar;
        
        // Add styles
        this._addStyles();
    }
    
    _addStyles() {
        if (document.getElementById('sidebar-styles')) return;
        
        const style = document.createElement('style');
        style.id = 'sidebar-styles';
        style.textContent = `
            .news-sidebar {
                position: fixed;
                right: 0;
                top: 0;
                width: 350px;
                height: 100%;
                background: rgba(0, 0, 0, 0.95);
                color: white;
                z-index: 1000;
                display: none;
                flex-direction: column;
                box-shadow: -2px 0 10px rgba(0, 0, 0, 0.5);
                font-family: Arial, sans-serif;
            }
            
            .news-sidebar.visible {
                display: flex;
            }
            
            .sidebar-location {
                background: #1a1a1a;
                padding: 12px 15px;
                font-size: 14px;
                font-weight: bold;
                border-bottom: 1px solid #333;
                display: flex;
                align-items: center;
            }
            
            .sidebar-location i {
                margin-right: 8px;
                color: #00c8ff;
            }
            
            .sidebar-location-text {
                flex: 1;
            }
            
            .sidebar-close {
                background: none;
                border: none;
                color: white;
                font-size: 24px;
                cursor: pointer;
                padding: 0 5px;
                margin-left: auto;
            }
            
            .sidebar-close:hover {
                color: #ff4444;
            }
            
            .sidebar-content {
                flex: 1;
                overflow-y: auto;
                padding: 10px;
            }
            
            .sidebar-item {
                background: #1a1a1a;
                border-radius: 8px;
                padding: 12px;
                margin-bottom: 10px;
                border-left: 3px solid #00c8ff;
            }
            
            
            /* Source-specific border colors */

            .sidebar-item.source-mastodon {
                border-left-color: #6364ff;
            }

            .sidebar-item.source-gdelt {
                border-left-color: #ff6b35;
            }

            .sidebar-item.source-telegram {
                border-left-color: #0088cc;
            }

            .sidebar-item.source-adsb {
                border-left-color: #00aa88;
            }

            .sidebar-item.source-ais {
                border-left-color: #0066cc;
            }

            .sidebar-item.source-usgs {
                border-left-color: #cc3333;
            }

            .sidebar-item.source-gdacs {
                border-left-color: #ff8800;
            }

            .sidebar-item.source-news {
                border-left-color: #666666;
            }

            .sidebar-item-header {
                display: flex;
                justify-content: space-between;
                align-items: center;
                margin-bottom: 6px;
            }

            .sidebar-item-source {
                font-size: 11px;
                text-transform: uppercase;
                font-weight: bold;
            }

            /* Source-specific text colors */

            .sidebar-item-source.mastodon {
                color: #6364ff;
            }

            .sidebar-item-source.gdelt {
                color: #ff6b35;
            }

            .sidebar-item-source.telegram {
                color: #0088cc;
            }

            .sidebar-item-source.adsb {
                color: #00aa88;
            }

            .sidebar-item-source.ais {
                color: #0066cc;
            }

            .sidebar-item-source.usgs {
                color: #cc3333;
            }

            .sidebar-item-source.gdacs {
                color: #ff8800;
            }

            .sidebar-item-source.news {
                color: #666666;
            }
            
            .sidebar-item-author {
                color: #888;
                font-size: 11px;
            }
            
            .sidebar-item-title {
                color: white;
                text-decoration: none;
                font-size: 14px;
                font-weight: bold;
                line-height: 1.3;
                display: block;
                margin-bottom: 8px;
            }
            
            .sidebar-item-title:hover {
                color: #00c8ff;
            }
            
            .sidebar-item-description {
                color: #aaa;
                font-size: 12px;
                line-height: 1.4;
                margin-bottom: 8px;
                display: block;
                text-decoration: none;
            }
            
            a.sidebar-item-description:hover {
                color: #ccc;
            }
            
            .sidebar-item-keywords {
                display: flex;
                flex-wrap: wrap;
                gap: 4px;
                margin-bottom: 8px;
            }
            
            .sidebar-keyword {
                background: #333;
                color: #00c8ff;
                font-size: 10px;
                padding: 2px 6px;
                border-radius: 3px;
            }
            
            .sidebar-keyword.hashtag {
                background: #1a3a4a;
            }
            
            .sidebar-keyword.mention {
                background: #2a2a4a;
                color: #a0a0ff;
            }
            
            .sidebar-item-time {
                color: #666;
                font-size: 10px;
            }
            
            .sidebar-item-footer {
                display: flex;
                justify-content: space-between;
                align-items: center;
                margin-top: 8px;
            }
            
            .sidebar-item-expand {
                background: none;
                border: 1px solid #444;
                color: #888;
                width: 24px;
                height: 24px;
                border-radius: 4px;
                cursor: pointer;
                display: flex;
                align-items: center;
                justify-content: center;
                padding: 0;
            }
            
            .sidebar-item-expand:hover {
                border-color: #00c8ff;
                color: #00c8ff;
            }
            
            .sidebar-item-expand i {
                font-size: 10px;
            }
            
            .sidebar-item-body {
                margin-bottom: 0;
            }
            
            .sidebar-item-expanded {
                animation: expandIn 0.2s ease;
            }
            
            @keyframes expandIn {
                from {
                    opacity: 0;
                    max-height: 0;
                }
                to {
                    opacity: 1;
                    max-height: 500px;
                }
            }
            
            .sidebar-item.expanded {
                border-left-color: #00c8ff;
            }
            
            /* Sidebar media grid - similar to balloon-media-holder */
            .sidebar-media-holder {
                display: grid;
                width: 100%;
                max-width: 300px;
                height: auto;
                overflow: hidden;
                margin-top: 10px;
                box-sizing: border-box;
                gap: 2px;
                border-radius: 6px;
            }

            .sidebar-media-holder.grid-1 {
                grid-template-columns: 1fr;
            }

            .sidebar-media-holder.grid-2 {
                grid-template-columns: 1fr 1fr;
            }

            .sidebar-media-holder.grid-3 {
                grid-template-columns: 1fr 1fr 1fr;
            }

            .sidebar-media-holder.grid-3x2 {
                grid-template-columns: 1fr 1fr 1fr;
                grid-template-rows: auto auto;
            }

            .sidebar-media-holder img,
            .sidebar-media-holder video {
                width: 100%;
                height: 100%;
                object-fit: cover;
                display: block;
                border-radius: 4px;
            }

            .sidebar-media-holder.grid-1 img,
            .sidebar-media-holder.grid-1 video {
                max-height: 150px;
            }

            .sidebar-media-holder.grid-2 img,
            .sidebar-media-holder.grid-2 video {
                max-height: 150px;
            }

            .sidebar-media-holder.grid-3 img,
            .sidebar-media-holder.grid-3 video {
                max-height: 150px;
            }

            .sidebar-media-holder.grid-3x2 img,
            .sidebar-media-holder.grid-3x2 video {
                max-height: 75px;
            }

            .sidebar-media-holder a {
                display: block;
            }
            
            /* Quote indicator in collapsed view */
            .sidebar-quote-indicator {
                display: block;
                color: #666;
                font-size: 10px;
                margin-top: 4px;
                font-style: italic;
            }
            
            /* Quoted tweet styling */
            .sidebar-quoted-tweet {
                margin-top: 10px;
                padding: 10px;
                background: #1a1a1a;
                border: 1px solid #333;
                border-left: 3px solid #555;
                border-radius: 6px;
            }
            
            .sidebar-quoted-header {
                display: flex;
                gap: 6px;
                margin-bottom: 6px;
                font-size: 11px;
            }
            
            .sidebar-quoted-author {
                color: #fff;
                font-weight: 600;
            }
            
            .sidebar-quoted-screen-name {
                color: #666;
            }
            
            .sidebar-quoted-text {
                color: #aaa;
                font-size: 12px;
                line-height: 1.4;
                text-decoration: none;
                display: block;
            }
            
            .sidebar-quoted-text:hover {
                color: #ccc;
            }
            
            .sidebar-quoted-tweet .sidebar-media-holder {
                max-width: 100%;
                margin-top: 8px;
            }
            
            .sidebar-thematic {
                margin-bottom: 15px;
            }
            
            .sidebar-thematic-header {
                background: #333;
                padding: 8px 12px;
                border-radius: 5px;
                margin-bottom: 8px;
                cursor: pointer;
                display: flex;
                justify-content: space-between;
                align-items: center;
            }
            
            .sidebar-thematic-header:hover {
                background: #444;
            }
            
            .sidebar-thematic-keywords {
                color: #00c8ff;
                font-size: 12px;
            }
            
            .sidebar-thematic-count {
                color: #888;
                font-size: 11px;
            }
            
            .sidebar-thematic-items {
                padding-left: 10px;
            }
            
            .sidebar-thematic-items.collapsed {
                display: none;
            }
            
            .sidebar-empty {
                color: #aaa;
                text-align: center;
                padding: 40px 20px;
            }
            
            .sidebar-empty i {
                font-size: 48px;
                margin-bottom: 15px;
                display: block;
            }
            
            /* Cluster grouping styles */
            .sidebar-cluster {
                background: #1a1a1a;
                border-radius: 8px;
                padding: 0;
                margin-bottom: 15px;
                border-left: 3px solid #00c8ff;
            }
            
            .sidebar-cluster-header {
                padding: 12px 15px;
                cursor: pointer;
                display: flex;
                justify-content: space-between;
                align-items: center;
                background: #222;
                border-radius: 8px 8px 0 0;
            }
            
            .sidebar-cluster-header:hover {
                background: #2a2a2a;
            }
            
            .sidebar-cluster-info {
                flex: 1;
                display: flex;
                flex-direction: column;
                gap: 4px;
            }
            
            .sidebar-cluster-title {
                color: white;
                font-size: 12px;
                font-weight: normal;
                line-height: 1.4;
            }
            
            .sidebar-cluster-count {
                color: #888;
                font-size: 11px;
            }
            
            .sidebar-cluster-toggle {
                background: rgba(255, 255, 255, 0.1);
                border: 1px solid #444;
                color: #888;
                cursor: pointer;
                padding: 4px;
                border-radius: 3px;
                display: flex;
                align-items: center;
                justify-content: center;
                transition: all 0.2s ease;
                width: 24px;
                height: 24px;
            }

            .sidebar-cluster-toggle:hover {
                background: rgba(0, 200, 255, 0.2);
                border-color: #00c8ff;
                color: #00c8ff;
            }
            
            .sidebar-cluster-toggle i {
                font-size: 12px;
            }
            
            .sidebar-cluster-items {
                padding: 10px;
                max-height: none;
                overflow: visible;
            }
            
            .sidebar-cluster-items.collapsed {
                max-height: 0;
                padding: 0 10px;
                overflow: hidden;
                opacity: 0;
            }
            
            .sidebar-cluster.collapsed {
                border-left-color: #555;
            }
            
            .sidebar-cluster-items .sidebar-item {
                margin-bottom: 8px;
                background: #252525;
            }
            
            .sidebar-cluster-items .sidebar-item:last-child {
                margin-bottom: 0;
            }
            
            /* Responsive */
            @media (max-width: 768px) {
                .news-sidebar {
                    width: 100%;
                    height: 40%;
                    top: auto;
                    bottom: 0;
                    right: 0;
                }
            }
        `;
        
        document.head.appendChild(style);
    }
    
    show() {
        if (this.container) {
            this.container.classList.add('visible');
        } else {
            console.error('Sidebar: container not found');
        }
        this.isVisible = true;
        document.body.classList.add('sidebar-visible');
        
        // Trigger map resize after transition
        setTimeout(() => {
            if (window.map && typeof window.map.invalidateSize === 'function') {
                window.map.invalidateSize();
            }
            if (window.mapInterface && window.mapInterface.cesiumMap && window.mapInterface.cesiumMap.viewer) {
                window.mapInterface.cesiumMap.viewer.resize();
            }
        }, 300); // Wait for CSS transition to complete
        
        // Sync sidebar button state
        if (window.syncSidebarButtonState) {
            window.syncSidebarButtonState(true);
        }
    }
    
    hide() {
        this.container.classList.remove('visible');
        this.isVisible = false;
        document.body.classList.remove('sidebar-visible');
        
        // Trigger map resize after transition
        setTimeout(() => {
            if (window.map && typeof window.map.invalidateSize === 'function') {
                window.map.invalidateSize();
            }
            if (window.mapInterface && window.mapInterface.cesiumMap && window.mapInterface.cesiumMap.viewer) {
                window.mapInterface.cesiumMap.viewer.resize();
            }
        }, 300); // Wait for CSS transition to complete
        
        // Sync sidebar button state
        if (window.syncSidebarButtonState) {
            window.syncSidebarButtonState(false);
        }
    }
    
    toggle() {
        if (this.isVisible) {
            this.hide();
        } else {
            this.show();
        }
    }
    
    /**
     * Select a location by key and display its items (not clusters)
     * @param {string} locationKey - The location key
     */
    selectLocation(locationKey) {
        // Show sidebar immediately (user-triggered, simple)
        this.show();

        // Store the previous locationKey before updating
        const previousLocationKey = this.currentLocationKey;
        this.currentLocationKey = locationKey;

        // ONLY display clusters - no individual items allowed
        let clusters = [];
        let locationName = locationKey;

        if (window.dataManager) {
            // Build filter parameters for DataManager
            const filters = {
                limit: 10000  // Fetch all clusters for this location
            };

            // Add time filter parameters if enabled (use centralized manager)
            if (window.timeFilterManager && window.timeFilterManager.isFilterActive()) {
                const timeParams = window.timeFilterManager.getTimeFilterParams();
                if (timeParams.time_from) filters.timeFrom = timeParams.time_from;
                if (timeParams.time_to) filters.timeTo = timeParams.time_to;
            }

            // Get clusters for this location from DataManager
            clusters = window.dataManager.getClustersForLocation(locationKey, filters);

            // Get location name from clusters if available
            if (clusters.length > 0) {
                locationName = clusters[0].location_name || locationName;
            }
        }

        // Always display clusters (even if empty) - NO individual items allowed
        this.updateClusters(clusters, locationName, locationKey, previousLocationKey);
    }
    
    /**
     * Update sidebar with articles for a location
     * @param {Array} articles - List of articles/tweets
     * @param {string} location - Location name
     */
    update(articles, location) {
        const isNewLocation = this.currentLocation !== location;
        
        // Filter articles by time range if filter is enabled
        let filteredArticles = articles;
        if (window.timeFilterPanel && articles) {
            filteredArticles = window.timeFilterPanel.filterItems(articles);
        }
        
        this.articles = filteredArticles;
        this.currentLocation = location;
        
        // Update location header with count in parentheses (show filtered count)
        const locationTextEl = this.container.querySelector('.sidebar-location-text');
        const itemCount = filteredArticles ? filteredArticles.length : 0;
        const totalCount = articles ? articles.length : 0;
        if (totalCount !== itemCount && window.timeFilterPanel && window.timeFilterPanel.isFilterEnabled()) {
            locationTextEl.textContent = `${location || 'Unknown Location'} (${itemCount}/${totalCount})`;
        } else {
            locationTextEl.textContent = `${location || 'Unknown Location'} (${itemCount})`;
        }
        
        // Update content
        const contentEl = this.container.querySelector('.sidebar-content');
        
        if (!filteredArticles || filteredArticles.length === 0) {
            contentEl.innerHTML = `
                <div class="sidebar-empty">
                    <i class="fa-solid fa-inbox"></i>
                    <p>No items for this location${window.timeFilterManager && window.timeFilterManager.isFilterActive() ? ' (filtered)' : ''}</p>
                </div>
            `;
            this.displayedItemIds.clear();
            this.show();
            return;
        }
        
        // Sort articles by date (newest first)
        const sortedArticles = this._sortByDate(filteredArticles);
        
        // If new location, clear everything
        if (isNewLocation) {
            this.displayedItemIds.clear();
            contentEl.innerHTML = '';
        }
        
        // Create a map of existing DOM elements by item ID
        const existingElements = new Map();
        const existingItems = contentEl.querySelectorAll('.sidebar-item');
        for (const item of existingItems) {
            // Try to get item ID from data attribute
            let itemId = item.dataset.itemId;
            
            // If no data attribute, try to match by URL
            if (!itemId) {
                const urlLink = item.querySelector('.sidebar-item-title, .sidebar-item-description');
                if (urlLink && urlLink.href) {
                    // Find matching article by URL
                    for (const article of sortedArticles) {
                        if (article.url === urlLink.href) {
                            itemId = this._getItemId(article);
                            // Set data attribute for future lookups
                            item.dataset.itemId = itemId;
                            break;
                        }
                    }
                }
            }
            
            if (itemId) {
                existingElements.set(itemId, item);
            }
        }
        
        // Get set of current article IDs
        const currentIds = new Set(sortedArticles.map(a => this._getItemId(a)));
        
        // Track which items are new
        const newItemIds = new Set();
        for (const article of sortedArticles) {
            const itemId = this._getItemId(article);
            if (!this.displayedItemIds.has(itemId)) {
                newItemIds.add(itemId);
            }
        }
        
        // Incremental update: reuse existing elements, only add/remove what's changed
        const currentItemIds = new Set(sortedArticles.map(a => this._getItemId(a)));

        // Remove items that no longer exist
        for (const itemId of this.displayedItemIds) {
            if (!currentItemIds.has(itemId)) {
                const itemEl = existingElements.get(itemId);
                if (itemEl && itemEl.parentNode) {
                    itemEl.remove();
                }
                this.displayedItemIds.delete(itemId);
            }
        }

        // Add new items that don't exist yet
        for (const article of sortedArticles) {
            const itemId = this._getItemId(article);
            if (!existingElements.has(itemId)) {
                const itemEl = this._createArticleItem(article, null); // No cluster context available here
                itemEl.dataset.itemId = itemId;
                contentEl.appendChild(itemEl);
                this.displayedItemIds.add(itemId);
            }
        }
        
        // Remove stale IDs from tracking
        for (const id of this.displayedItemIds) {
            if (!currentIds.has(id)) {
                this.displayedItemIds.delete(id);
            }
        }
        
        this.show();
    }
    
    /**
     * Update sidebar with clusters for a location
     * @param {Array} clusters - List of clusters with items
     * @param {string} location - Location name
     * @param {string} locationKey - Location key
     * @param {string} previousLocationKey - Previous location key (for detecting location changes)
     */
    updateClusters(clusters, location, locationKey, previousLocationKey) {
        // Detect location change by comparing locationKey (not location name)
        const isNewLocation = previousLocationKey !== locationKey;

        // Filter clusters by time range if filter is enabled (use centralized manager)
        let filteredClusters = clusters;
        if (window.timeFilterManager && window.timeFilterManager.isFilterActive() && clusters) {
            filteredClusters = clusters.filter(cluster => {
                if (!cluster.items || cluster.items.length === 0) {
                    return false;
                }
                const hasValidItems = cluster.items.some(item => {
                    return window.timeFilterPanel.filterItems([item]).length > 0;
                });
                return hasValidItems;
            });
        }

        this.articles = filteredClusters.flatMap(c => c.items || []);
        this.currentLocation = location;

        // Update content
        const contentEl = this.container.querySelector('.sidebar-content');

        // Store expanded cluster IDs before clearing (preserve user state)
        const expandedClusterIds = new Set();
        if (!isNewLocation) {
            for (const [clusterId, clusterData] of this.displayedClusters.entries()) {
                const clusterEl = clusterData.element;
                if (clusterEl && !clusterEl.classList.contains('collapsed')) {
                    expandedClusterIds.add(clusterId);
                }
            }
        }

        // Only clear state when location changes (not on time filter updates)
        if (isNewLocation) {
            this.displayedItemIds.clear();
            this.displayedClusters.clear();
            contentEl.innerHTML = '';
        }
        
        if (!filteredClusters || filteredClusters.length === 0) {
            const filterNote = window.timeFilterManager && window.timeFilterManager.isFilterActive() ? ' (time filter active)' : '';
            contentEl.innerHTML = `
                <div class="sidebar-empty">
                    <i class="fa-solid fa-inbox"></i>
                    <p>No news clusters found for "${location || locationKey}"${filterNote}</p>
                    <p style="font-size: 12px; margin-top: 10px;">Try selecting a location with recent news activity, or adjust your time filter.</p>
                </div>
            `;
            // Update header
            const locationTextEl = this.container.querySelector('.sidebar-location-text');
            locationTextEl.textContent = `${location || 'Unknown Location'} (0 items)`;
            return;
        }
        
        // Sort clusters by earliest item datetime (latest cluster at top)
        const sortedClusters = [...filteredClusters].sort((a, b) => {
            const dateA = this._getEarliestItemDatetime(a);
            const dateB = this._getEarliestItemDatetime(b);
            // Handle empty strings (clusters with no items) - put them last
            if (!dateA && !dateB) return 0;
            if (!dateA) return 1; // A goes after B
            if (!dateB) return -1; // B goes after A
            // Compare ISO strings directly (ISO format is lexicographically sortable)
            return dateB.localeCompare(dateA); // Descending: latest cluster (with most recent earliest item) at top
        });
        
        // Track all item IDs in this update
        const allItemIds = new Set();
        let hasNewClusters = false;
        
        // Track which clusters are in the current response
        const responseClusterIds = new Set(sortedClusters.map(c => c.cluster_id));

        // Process all clusters - update existing or add new
        for (const cluster of sortedClusters) {
            const clusterId = cluster.cluster_id;
            const existingCluster = this.displayedClusters.get(clusterId);

            // Track item IDs for this cluster
            const clusterItemIds = new Set();
            if (cluster.items) {
                for (const item of cluster.items) {
                    const itemId = this._getItemId(item);
                    clusterItemIds.add(itemId);
                    allItemIds.add(itemId);
                }
            }

            if (existingCluster) {
                // Cluster exists - check if it needs updating
                const currentItemIds = existingCluster.itemIds;
                const needsUpdate = !this._setsEqual(currentItemIds, clusterItemIds) ||
                                   JSON.stringify(existingCluster.clusterData) !== JSON.stringify(cluster);

                if (needsUpdate) {
                    existingCluster.clusterData = cluster;
                    existingCluster.itemIds = clusterItemIds;

                    // Update the cluster element dynamically
                    this._updateClusterGroup(existingCluster.element, cluster, cluster.items || []);
                }
            } else {
                // New cluster - create element
                const clusterEl = this._createClusterGroup(cluster);
                contentEl.appendChild(clusterEl);

                this.displayedClusters.set(clusterId, {
                    element: clusterEl,
                    clusterData: cluster,
                    itemIds: clusterItemIds
                });

                hasNewClusters = true;
            }
        }

        // Remove clusters that are no longer in the response (filtered out by time)
        const clustersToRemove = [];
        for (const [clusterId, clusterData] of this.displayedClusters.entries()) {
            if (!responseClusterIds.has(clusterId)) {
                // Remove from DOM
                if (clusterData.element && clusterData.element.parentNode) {
                    clusterData.element.remove();
                }
                clustersToRemove.push(clusterId);
            }
        }

        // Clean up tracking for removed clusters
        for (const clusterId of clustersToRemove) {
            this.displayedClusters.delete(clusterId);
        }

        // Update displayed item IDs to match current response
        this.displayedItemIds.clear();
        for (const id of allItemIds) {
            this.displayedItemIds.add(id);
        }
        
        // Incremental update: only add new clusters, remove old ones, preserve existing order
        const currentClusterIds = new Set(sortedClusters.map(c => c.cluster_id));
        const existingClusterIds = new Set(this.displayedClusters.keys());

        // Remove clusters that no longer exist
        for (const clusterId of existingClusterIds) {
            if (!currentClusterIds.has(clusterId)) {
                const clusterData = this.displayedClusters.get(clusterId);
                if (clusterData && clusterData.element && clusterData.element.parentNode) {
                    clusterData.element.remove();
                }
                this.displayedClusters.delete(clusterId);
            }
        }

        // Add new clusters that don't exist yet
        for (const cluster of sortedClusters) {
            const clusterId = cluster.cluster_id;
            if (!existingClusterIds.has(clusterId)) {
                const clusterEl = this._createClusterGroup(cluster);
                contentEl.appendChild(clusterEl);
                this.displayedClusters.set(clusterId, {
                    element: clusterEl,
                    clusterData: cluster,
                    itemIds: new Set(cluster.items?.map(item => this._getItemId(item)) || [])
                });
            }
        }

        // Restore expanded state for clusters that were previously expanded
        for (const clusterId of expandedClusterIds) {
            const clusterData = this.displayedClusters.get(clusterId);
            if (clusterData && clusterData.element) {
                const itemsContainer = clusterData.element.querySelector('.sidebar-cluster-items');
                const icon = clusterData.element.querySelector('.sidebar-cluster-toggle i');
                if (itemsContainer && icon) {
                    itemsContainer.classList.remove('collapsed');
                    clusterData.element.classList.remove('collapsed');
                    icon.className = 'fa-solid fa-chevron-up';
                }
            }
        }
        
        // Count clusters actually displayed in the DOM by counting cluster elements
        const displayedClusterCount = contentEl.querySelectorAll('.sidebar-cluster').length;
        
        // Update header with actual count of displayed clusters (matches what's actually in the DOM)
        const locationTextEl = this.container.querySelector('.sidebar-location-text');
        locationTextEl.textContent = `${location || 'Unknown Location'} (${displayedClusterCount} items)`;
    }
    
    /**
     * Get the latest item datetime from a cluster
     * @param {object} cluster - Cluster object with items array
     * @returns {Date} Latest item datetime
     */
    _getLatestItemDatetime(cluster) {
        if (!cluster.items || cluster.items.length === 0) {
            return new Date(0);
        }
        
        let latestDate = new Date(0);
        for (const item of cluster.items) {
            const itemDate = new Date(item.published_at || item.createdAt || 0);
            if (itemDate > latestDate) {
                latestDate = itemDate;
            }
        }
        
        return latestDate;
    }
    
    /**
     * Get the earliest item datetime from a cluster (returns ISO string)
     * @param {object} cluster - Cluster object with items array
     * @returns {string} Earliest item datetime as ISO string
     */
    _getEarliestItemDatetime(cluster) {
        if (!cluster.items || cluster.items.length === 0) {
            return ''; // Empty string sorts first, but we want these at the end
        }
        
        let earliestDate = null;
        for (const item of cluster.items) {
            const itemDate = item.published_at || item.createdAt || '';
            if (itemDate && (!earliestDate || itemDate < earliestDate)) {
                earliestDate = itemDate;
            }
        }
        
        return earliestDate || ''; // Return ISO string directly
    }
    
    /**
     * Create a cluster group element
     */
    _createClusterGroup(cluster) {
        const group = document.createElement('div');
        group.className = 'sidebar-cluster';
        group.dataset.clusterId = cluster.cluster_id;
        
        const clusterTitle = cluster.title || (cluster.items && cluster.items.length > 0 
            ? (cluster.items[0].text || cluster.items[0].title || 'Untitled cluster').substring(0, 100)
            : 'Untitled cluster');
        const itemCount = cluster.item_count || (cluster.items ? cluster.items.length : 0);
        
        // Display cluster author (from first item)
        const clusterAuthor = (cluster.items && cluster.items.length > 0)
            ? (cluster.items[0].authorName || cluster.items[0].author || 'Unknown')
            : 'Unknown';
        const authorStyle = clusterAuthor === 'Unknown' ? 'color: #f44;' : 'color: #f44;';
        
        // Sort items within cluster by date (oldest first)
        const sortedItems = cluster.items ? this._sortByDateAscending(cluster.items) : [];
        
        group.innerHTML = `
            <div class="sidebar-cluster-header" onclick="window.newsSidebar._toggleClusterExpand(this)">
                <div class="sidebar-cluster-info">
                    <span class="sidebar-cluster-title">${(window.escapeHtml || ((s) => s))(clusterTitle)}</span>
                    <span class="sidebar-cluster-count">${itemCount} item${itemCount !== 1 ? 's' : ''} </span>
                </div>
                <button class="sidebar-cluster-toggle" onclick="event.stopPropagation(); window.newsSidebar._toggleClusterExpand(this.closest('.sidebar-cluster-header'))" title="Click to expand/collapse items in this cluster">
                    <i class="fa-solid fa-chevron-down"></i>
                </button>
            </div>
            <div class="sidebar-cluster-items"></div>
        `;
        
        const itemsContainer = group.querySelector('.sidebar-cluster-items');

        // Add items to cluster (items are now included in cluster data)
        for (const article of sortedItems) {
            const itemId = this._getItemId(article);
            const itemEl = this._createArticleItem(article, itemCount);
            itemEl.dataset.itemId = itemId;
            itemsContainer.appendChild(itemEl);
        }

        // Start collapsed by default - users can click to expand and see individual items
        itemsContainer.classList.add('collapsed');
        group.classList.add('collapsed');
        
        return group;
    }
    
    /**
     * Update an existing cluster group dynamically
     * @param {HTMLElement} clusterEl - Existing cluster element
     * @param {object} cluster - Updated cluster data
     * @param {Array} allItems - All items that should be displayed
     */
    _updateClusterGroup(clusterEl, cluster, allItems) {
        // Update item count
        const countEl = clusterEl.querySelector('.sidebar-cluster-count');
        if (countEl) {
            const itemCount = cluster.item_count || (allItems.length || 0);
            countEl.textContent = `${itemCount} item${itemCount !== 1 ? 's' : ''}`;
        }

        const itemsContainer = clusterEl.querySelector('.sidebar-cluster-items');
        if (!itemsContainer) return;

        // Sort items by date (oldest first)
        const sortedItems = this._sortByDateAscending(allItems);

        // Get current item elements and their IDs
        const existingItems = new Map();
        const currentItemElements = itemsContainer.querySelectorAll('.sidebar-item');
        for (const itemEl of currentItemElements) {
            const itemId = itemEl.dataset.itemId;
            if (itemId) {
                existingItems.set(itemId, itemEl);
            }
        }

        // Get set of new item IDs
        const newItemIds = new Set(sortedItems.map(item => this._getItemId(item)));

        // Remove items that are no longer in the cluster
        for (const [itemId, itemEl] of existingItems.entries()) {
            if (!newItemIds.has(itemId)) {
                itemEl.remove();
            }
        }

        // Create document fragment for new ordering
        const fragment = document.createDocumentFragment();
        let insertPosition = 0;

        // Process items in sorted order
        for (const article of sortedItems) {
            const itemId = this._getItemId(article);
            let itemEl = existingItems.get(itemId);

            if (itemEl) {
                // Item exists - reuse it and remove from existing map
                itemEl.remove();
                existingItems.delete(itemId);
            } else {
                // New item - create it (no cluster context available here)
                itemEl = this._createArticleItem(article, null);
                itemEl.dataset.itemId = itemId;
            }

            fragment.appendChild(itemEl);
        }

        // Clear container and append new order
        itemsContainer.innerHTML = '';
        itemsContainer.appendChild(fragment);
    }
    
    /**
     * Toggle cluster expansion
     */
    _toggleClusterExpand(header) {
        const cluster = header.closest('.sidebar-cluster');
        const itemsContainer = cluster.querySelector('.sidebar-cluster-items');
        const icon = header.querySelector('.sidebar-cluster-toggle i');

        if (itemsContainer.classList.contains('collapsed')) {
            itemsContainer.classList.remove('collapsed');
            cluster.classList.remove('collapsed');
            icon.className = 'fa-solid fa-chevron-up';
        } else {
            itemsContainer.classList.add('collapsed');
            cluster.classList.add('collapsed');
            icon.className = 'fa-solid fa-chevron-down';
        }
    }
    
    _getItemId(article) {
        // Use tweet_id or URL from database (id field from API)
        return article.id || article.url || `${article.published_at}-${article.text?.substring(0, 50)}`;
    }
    
    _sortByDate(articles) {
        return [...articles].sort((a, b) => {
            const dateA = new Date(a.published_at || a.createdAt || 0);
            const dateB = new Date(b.published_at || b.createdAt || 0);
            return dateB - dateA; // Newest first
        });
    }
    
    _sortByDateAscending(articles) {
        return [...articles].sort((a, b) => {
            const dateA = new Date(a.published_at || a.createdAt || 0);
            const dateB = new Date(b.published_at || b.createdAt || 0);
            return dateA - dateB; // Oldest first
        });
    }
    
    _groupBySource(articles) {
        const groups = {};
        
        for (const article of articles) {
            // Normalize source: News has 'news' or 'rss'
            let source = article.source || 'unknown';
            if (source === 'rss') {
                source = 'news';
            }
            
            if (!groups[source]) {
                groups[source] = [];
            }
            groups[source].push(article);
        }
        
        return groups;
    }
    
    _groupByThematic(articles) {
        const groups = {};
        
        for (const article of articles) {
            const thematicNum = article.thematic_number || 0;
            if (!groups[thematicNum]) {
                groups[thematicNum] = [];
            }
            groups[thematicNum].push(article);
        }
        
        return groups;
    }
    
    _createThematicGroup(thematicNum, items) {
        const group = document.createElement('div');
        group.className = 'sidebar-thematic';
        
        // Get keywords from first item's matching keywords
        const keywords = items[0]?.matching?.slice(0, 3)?.join(', ') || `Group ${thematicNum}`;
        
        group.innerHTML = `
            <div class="sidebar-thematic-header" onclick="this.nextElementSibling.classList.toggle('collapsed')">
                <span class="sidebar-thematic-keywords">${keywords}</span>
                <span class="sidebar-thematic-count">${items.length} items</span>
            </div>
            <div class="sidebar-thematic-items"></div>
        `;
        
        const itemsContainer = group.querySelector('.sidebar-thematic-items');
        for (const article of items) {
            const itemEl = this._createArticleItem(article, null); // No cluster context for thematic groups
            itemsContainer.appendChild(itemEl);
        }
        
        return group;
    }
    
    _createArticleItem(article, clusterItemCount = null) {
        const item = document.createElement('div');
        const truncateLength = 150;

        // Use actual source name for display
        let sourceType = article.source || 'unknown';
        // Normalize legacy source types if needed
        if (sourceType === 'rss') {
            sourceType = 'news';
        }
        item.className = `sidebar-item source-${sourceType}`;
        
        // Use raw fields directly - authorName for tweets, author for news
        // For retweets, show "retweeter › @original_author" format
        const hasRetweetedTweet = article.retweetedTweet && article.retweetedTweet.authorScreenName;
        let author = '';
        if (hasRetweetedTweet) {
            const retweeter = this._truncateText(article.authorName || 'Unknown', 15);
            const originalAuthor = article.retweetedTweet.authorScreenName;
            author = `${retweeter} › @${originalAuthor}`;
        } else {
            author = this._truncateText(article.authorName || article.author || 'Unknown', 25);
        }
        const url = article.url || '#';
        
        // Get full and truncated content
        let title = article.title || '';
        let fullContent = article.text || '';
        let truncatedContent = fullContent.substring(0, 150);
        
        // Check if there's a quoted tweet
        const hasQuotedTweet = article.quotedTweet && article.quotedTweet.text;

        const needsExpand = fullContent.length > truncateLength ||
            (article.media_urls && article.media_urls.length > 0) ||
            hasQuotedTweet;
        
        // Use actual publication time (published_at from backend), not scrape time
        const createdAt = article.published_at || article.createdAt;

        // Format time using Luxon for relative display
        let timeDisplay = '';
        if (createdAt && typeof window.formatRelativeTime === 'function') {
            timeDisplay = window.formatRelativeTime(createdAt);
        } else if (createdAt) {
            // Fallback if Luxon not available
            try {
                const date = new Date(createdAt);
                if (!isNaN(date.getTime())) {
                    const now = new Date();
                    const diffMs = now - date;
                    const diffMins = Math.floor(diffMs / 60000);
                    const diffHours = Math.floor(diffMs / 3600000);

                    if (diffMins < 1) {
                        timeDisplay = 'Just now';
                    } else if (diffMins < 60) {
                        timeDisplay = `${diffMins}m ago`;
                    } else if (diffHours < 24) {
                        timeDisplay = `${diffHours}h ago`;
                    } else {
                        timeDisplay = date.toLocaleDateString();
                    }
                }
            } catch (e) {
                // Ignore date parsing errors
            }
        }
        
        // Add data-timestamp attribute for periodic updates
        // createdAt is already a safe ISO string, so we just need to escape quotes for HTML attribute
        const timestampAttr = createdAt ? ` data-timestamp="${String(createdAt).replace(/"/g, '&quot;')}"` : '';
        
        // Source type label - use actual source name
        const sourceLabel = sourceType.toUpperCase();
        const sourceClass = sourceType;
        
        // Generate media HTML for expanded view
        const mediaHtml = this._generateMediaHtml(article.media_urls);
        
        // Generate quoted tweet HTML for expanded view
        const quotedTweetHtml = this._generateQuotedTweetHtml(article.quotedTweet);
        
        // Build expand button if needed
        const expandButton = needsExpand ? `
            <button class="sidebar-item-expand" onclick="window.newsSidebar._toggleExpand(this)">
                <i class="fa-solid fa-chevron-down"></i>
            </button>
        ` : '';
        
        item.innerHTML = `
            <div class="sidebar-item-header">
                <span class="sidebar-item-source ${sourceClass}">${sourceLabel}</span>
                ${author && author !== 'Unknown' ? `<span class="sidebar-item-author">@${(window.escapeHtml || ((s) => s))(author)}</span>` : ''}
            </div>
            ${title ? `<a href="${url}" target="_blank" class="sidebar-item-title">${(window.escapeHtml || ((s) => s))(title)}</a>` : ''}
            <div class="sidebar-item-body">
                <a href="${url}" target="_blank" class="sidebar-item-description sidebar-item-collapsed">${(window.escapeHtml || ((s) => s))(truncatedContent)}${needsExpand && fullContent.length > truncateLength ? '...' : ''}</a>
                ${hasQuotedTweet ? '<span class="sidebar-quote-indicator">▸ Quote</span>' : ''}
                <div class="sidebar-item-expanded" style="display: none;">
                    <a href="${url}" target="_blank" class="sidebar-item-description">${(window.escapeHtml || ((s) => s))(fullContent)}</a>
                    ${quotedTweetHtml}
                    ${mediaHtml}
                </div>
            </div>
            <div class="sidebar-item-footer">
                ${timeDisplay ? `<span class="sidebar-item-time"${timestampAttr}>${timeDisplay}</span>` : ''}
                ${expandButton}
            </div>
        `;
        
        return item;
    }
    
    _toggleExpand(button) {
        const item = button.closest('.sidebar-item');
        const collapsed = item.querySelector('.sidebar-item-collapsed');
        const expanded = item.querySelector('.sidebar-item-expanded');
        const icon = button.querySelector('i');

        if (expanded.style.display === 'none') {
            // Expand
            collapsed.style.display = 'none';
            expanded.style.display = 'block';
            icon.className = 'fa-solid fa-chevron-up';
            item.classList.add('expanded');
        } else {
            // Collapse
            collapsed.style.display = 'block';
            expanded.style.display = 'none';
            icon.className = 'fa-solid fa-chevron-down';
            item.classList.remove('expanded');
        }
    }
    
    _generateMediaHtml(media_urls) {
        if (!Array.isArray(media_urls) || media_urls.length === 0) {
            return '';
        }

        // First: classify and keep only supported media
        const mediaItems = media_urls
            .map(url => {
                if (/\.(jpg|jpeg|png|gif|webp|bmp|svg)$/i.test(url)) {
                    return { type: 'image', url };
                }
                if (/\.(mp4|mov|avi|webm|ogv|m4v)$/i.test(url)) {
                    return { type: 'video', url };
                }
                return null;
            })
            .filter(Boolean);

        if (mediaItems.length === 0) {
            return '';
        }

        // Second: determine grid class based on VALID media count
        const mediaCount = mediaItems.length;
        let gridClass;
        if (mediaCount === 1) {
            gridClass = 'grid-1';
        } else if (mediaCount === 2) {
            gridClass = 'grid-2';
        } else if (mediaCount === 3) {
            gridClass = 'grid-3';
        } else {
            gridClass = 'grid-3x2';
        }

        // Third: render HTML
        let html = `<div class="sidebar-media-holder ${gridClass}">`;

        for (const item of mediaItems) {
            if (item.type === 'image') {
                html += `<a href="${item.url}" target="_blank"><img src="${item.url}" alt="Media"></a>`;
            } else if (item.type === 'video') {
                const proxyUrl = `http://localhost:6390/proxy?url=${encodeURIComponent(item.url)}`;
                html += `<video controls><source src="${proxyUrl}" type="video/mp4"></video>`;
            }
        }

        html += '</div>';
        return html;
    }
    
    
    _generateQuotedTweetHtml(quotedTweet) {
        if (!quotedTweet || !quotedTweet.text) {
            return '';
        }
        
        const author = quotedTweet.authorName || quotedTweet.authorScreenName || 'Unknown';
        const screenName = quotedTweet.authorScreenName || '';
        const url = quotedTweet.url || '#';
        const text = (window.escapeHtml || ((s) => s))(quotedTweet.text);
        
        // Generate media HTML for quoted tweet
        const quotedMediaHtml = this._generateMediaHtml(quotedTweet.media_urls);
        
        return `
            <div class="sidebar-quoted-tweet">
                <div class="sidebar-quoted-header">
                    <span class="sidebar-quoted-author">${(window.escapeHtml || ((s) => s))(author)}</span>
                    ${screenName ? `<span class="sidebar-quoted-screen-name">@${(window.escapeHtml || ((s) => s))(screenName)}</span>` : ''}
                </div>
                <a href="${url}" target="_blank" class="sidebar-quoted-text">${text}</a>
                ${quotedMediaHtml}
            </div>
        `;
    }
    
    
    _truncateText(text, maxLength) {
        if (!text || text.length <= maxLength) return text;
        return text.substring(0, maxLength) + '...';
    }

    /**
     * Check if two Sets contain the same elements
     * @param {Set} setA - First set
     * @param {Set} setB - Second set
     * @returns {boolean} True if sets are equal
     */
    _setsEqual(setA, setB) {
        if (setA.size !== setB.size) return false;
        for (const item of setA) {
            if (!setB.has(item)) return false;
        }
        return true;
    }
    
    /**
     * Update all sidebar time displays using Luxon
     * Converts ISO timestamps to relative time ("2 hours ago")
     */
    updateSidebarTimes() {
        // Check if Luxon formatting function is available
        if (typeof window.formatRelativeTime !== 'function') {
            return;
        }
        
        // Find all sidebar time elements with data-timestamp attribute
        if (!this.container) return;
        
        const timeElements = this.container.querySelectorAll('.sidebar-item-time[data-timestamp]');
        timeElements.forEach(function(el) {
            const timestamp = el.getAttribute('data-timestamp');
            if (timestamp && timestamp.length > 0) {
                try {
                    // Use global Luxon formatting function
                    const relativeTime = window.formatRelativeTime(timestamp);
                    if (relativeTime) {
                        el.textContent = relativeTime;
                    }
                } catch (e) {
                    console.warn('Error formatting sidebar time:', e);
                }
            }
        });
    }
}

// Create global instance
window.newsSidebar = new NewsSidebar();
