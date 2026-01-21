/**
 * Unified Data Manager - Central data service for tweets and news items
 *
 * Features:
 * - Periodically fetches all data for the last 7 days
 * - In-memory storage of tweets and news items
 * - Provides filtered query functions for different components
 * - Handles time filtering on frontend (for sliders)
 * - Replaces separate AJAX calls in sidebar, map, and cesium components
 */

class DataManager {
    constructor() {
        this.data = {
            clusters: [],
            items: [], // Flat list of all items for easy querying
            lastFetch: null,
            isFetching: false
        };

        // Backend data fetch interval
        this.refreshInterval = 15 * 1000; // 15 seconds
        this.refreshTimer = null;

        // Frontend update cycle - separate from data fetching
        this.frontendUpdateInterval = 5 * 1000; // 5 seconds - update UI every 5 seconds
        this.frontendUpdateTimer = null;

        // Event listeners for components that need data updates
        this.listeners = {
            'dataUpdated': [], // Now triggered by frontend update cycle, not data fetching
            'fetchStarted': [],
            'fetchCompleted': [],
            'fetchError': []
        };

        // Start periodic data fetching
        this.startPeriodicFetch();
    }

    /**
     * Start periodic background data fetching and frontend updates
     */
    startPeriodicFetch() {
        // Initial fetch
        this.fetchData();

        // Set up periodic backend data fetch (15 seconds)
        this.refreshTimer = setInterval(() => {
            this.fetchData();
        }, this.refreshInterval);

        // Set up separate periodic frontend updates (5 seconds, independent of data fetching)
        this.frontendUpdateTimer = setInterval(() => {
            this._triggerFrontendUpdate();
        }, this.frontendUpdateInterval);
    }

    /**
     * Stop periodic data fetching and frontend updates
     */
    stopPeriodicFetch() {
        if (this.refreshTimer) {
            clearInterval(this.refreshTimer);
            this.refreshTimer = null;
        }
        if (this.frontendUpdateTimer) {
            clearInterval(this.frontendUpdateTimer);
            this.frontendUpdateTimer = null;
        }
    }

    /**
     * Fetch all data for the last 7 days from backend
     */
    async fetchData() {
        if (this.isFetching) {
            console.log('DataManager: Fetch already in progress, skipping');
            return;
        }

        this.isFetching = true;
        this._emit('fetchStarted');

        try {
            // Calculate time range (last 7 days)
            const endTime = new Date();
            const startTime = new Date(endTime.getTime() - (1 * 24 * 60 * 60 * 1000)); // one day

            // Fetch clusters from the new API
            const clusters = await this._fetchClusters(startTime, endTime);

            // Process clusters only (no immediate UI update)
            this._processFetchedData([], clusters);
            this.lastFetch = new Date();
            this._emit('fetchCompleted', clusters.length);

        } catch (error) {
            console.error('DataManager: Fetch error:', error);
            this._emit('fetchError', error);
        } finally {
            this.isFetching = false;
        }
    }

    /**
     * Fetch clusters from the new API
     */
    async _fetchClusters(startTime, endTime) {
        // Fetch clusters globally - no bbox filtering needed
        const params = {
            since: startTime.toISOString()
        };

        const queryString = Object.keys(params)
            .map(key => key + '=' + encodeURIComponent(params[key]))
            .join('&');
        const url = '/api/clusters?' + queryString;

        try {
            const response = await $.ajax({
                method: "GET",
                url: url
            });

            if (response && response.features) {
                // Convert GeoJSON features to our internal cluster format
                return response.features.map(feature => ({
                    cluster_id: feature.properties.cluster_id,
                    item_count: feature.properties.item_count,
                    title: feature.properties.title,
                    summary: feature.properties.summary,
                    representative_lat: feature.geometry ? feature.geometry.coordinates[1] : feature.properties.representative_lat,
                    representative_lon: feature.geometry ? feature.geometry.coordinates[0] : feature.properties.representative_lon,
                    representative_location_name: feature.properties.representative_location_name,
                    // Use location_key from API, fallback to generated key if not provided
                    location_key: feature.properties.location_key ||
                        (feature.properties.representative_location_name ?
                            feature.properties.representative_location_name.toLowerCase().replace(/[^a-z0-9]/g, '') :
                            `${feature.geometry.coordinates[1].toFixed(2)}_${feature.geometry.coordinates[0].toFixed(2)}`),
                    first_seen_at: feature.properties.first_seen_at,
                    last_seen_at: feature.properties.last_seen_at,
                    created_at: feature.properties.created_at,
                    updated_at: feature.properties.updated_at,
                    items: feature.properties.items || []  // Include items from the API response
                }));
            }
        } catch (error) {
            console.warn('DataManager: Failed to fetch clusters:', error);
        }

        return [];
    }

    /**
     * Process fetched data into internal storage
     * Note: No longer emits dataUpdated immediately - UI updates are handled by separate cycle
     */
    _processFetchedData(items, clusters) {
        // Store individual items
        this.data.items = items;

        // Clusters already have their items from the API response, so just store them as-is
        this.data.clusters = clusters || [];

        // Data fetching is now silent - UI updates happen on separate periodic cycle
        // No immediate dataUpdated emission here
    }

    /**
     * Get all clusters (for map display)
     * @param {Object} filters - Optional filters {timeFrom, timeTo}
     * @returns {Array} Filtered clusters
     */
    getClusters(filters = {}) {
        let clusters = [...this.data.clusters];

        return clusters;
    }

    /**
     * Get clusters for a specific location (for sidebar)
     * @param {string} locationKey - Location key to filter by
     * @param {Object} filters - Optional filters {timeFrom, timeTo, limit}
     * @returns {Array} Filtered clusters for the location
     */
    getClustersForLocation(locationKey, filters = {}) {
        let clusters = this.data.clusters.filter(cluster =>
            cluster.location_key === locationKey
        );


        // Apply limit if specified
        if (filters.limit) {
            clusters = clusters.slice(0, filters.limit);
        }

        return clusters;
    }


    /**
     * Get all items (flat list) - useful for various queries
     * @param {Object} filters - Optional filters {locationKey, timeFrom, timeTo, type}
     * @returns {Array} Filtered items
     */
    getItems(filters = {}) {
        let items = [...this.data.items];

        // Apply filters
        if (filters.locationKey) {
            items = items.filter(item => item.clusterLocationKey === filters.locationKey);
        }

        if (filters.type) {
            items = items.filter(item => item.type === filters.type);
        }


        return items;
    }

    /**
     * Get locations with their item counts
     * @param {Object} filters - Optional filters {timeFrom, timeTo}
     * @returns {Object} Location data with counts
     */
    getLocations(filters = {}) {
        const locations = {};

        this.data.clusters.forEach(cluster => {

            const key = cluster.location_key;
            if (!locations[key]) {
                locations[key] = {
                    lat: cluster.representative_lat,
                    lng: cluster.representative_lon,
                    locationName: cluster.representative_location_name,
                    itemCount: 0,
                    clusters: []
                };
            }

            locations[key].clusters.push(cluster);
            locations[key].itemCount += cluster.items ? cluster.items.length : 0;
        });

        return locations;
    }



    /**
     * Force a data refresh (background operation, doesn't trigger immediate UI update)
     */
    refresh() {
        return this.fetchData();
    }

    /**
     * Force immediate UI refresh using current data
     */
    refreshUI() {
        this.triggerFrontendUpdate();
    }

    /**
     * Get data statistics
     */
    getStats() {
        return {
            clusterCount: this.data.clusters.length,
            itemCount: this.data.items.length,
            lastFetch: this.lastFetch,
            isFetching: this.isFetching
        };
    }

    /**
     * Event system for components to listen to data changes
     */
    on(event, callback) {
        if (this.listeners[event]) {
            this.listeners[event].push(callback);
        }
    }

    off(event, callback) {
        if (this.listeners[event]) {
            const index = this.listeners[event].indexOf(callback);
            if (index > -1) {
                this.listeners[event].splice(index, 1);
            }
        }
    }

    _emit(event, ...args) {
        if (this.listeners[event]) {
            this.listeners[event].forEach(callback => {
                try {
                    callback(...args);
                } catch (error) {
                    console.error(`DataManager: Error in ${event} listener:`, error);
                }
            });
        }
    }

    /**
     * Trigger frontend update cycle - independent of data fetching
     * This periodically refreshes UI components using current data
     */
    _triggerFrontendUpdate() {
        // Emit dataUpdated event to trigger UI refresh using current data
        // This happens independently of when data was last fetched
        this._emit('dataUpdated');
    }

    /**
     * Force immediate frontend update (for manual refresh)
     */
    triggerFrontendUpdate() {
        this._triggerFrontendUpdate();
    }
}

// Create global instance
window.dataManager = new DataManager();