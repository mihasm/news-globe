/**
 * Location Store - Wrapper around DataManager for location-based queries
 * Used by both 2D (Leaflet) and 3D (Cesium) map views
 *
 * Now acts as an interface to the DataManager, providing location-grouped access
 * Maintains backward compatibility with existing code
 */

class LocationStore {
    constructor() {
        // Wait for DataManager to be available
        this._waitForDataManager();

        // Event listeners (for backward compatibility)
        this.listeners = {
            'locationAdded': [],
            'locationUpdated': [],
            'itemAdded': [],
            'dataRestored': []
        };
    }

    /**
     * Wait for DataManager to be available and set up listeners
     */
    _waitForDataManager() {
        const setupDataManager = () => {
            if (window.dataManager) {
                // Listen to DataManager updates
                window.dataManager.on('dataUpdated', () => {
                    this._emit('dataRestored'); // Signal that data has been updated
                });
            } else {
                // Retry after a short delay
                setTimeout(setupDataManager, 100);
            }
        };

        setupDataManager();
    }
    
    /**
     * Add an item to a location (deprecated - DataManager handles data now)
     * Kept for backward compatibility but now just emits events
     * @param {string} locationKey - The location key from backend (normalized lowercase)
     * @param {number} lat - Latitude
     * @param {number} lng - Longitude
     * @param {object} item - The item data
     * @param {string} locationName - Location name for display
     * @returns {string} The location key used
     */
    addItem(locationKey, lat, lng, item, locationName = null) {
        // Skip items without a location key
        if (!locationKey) {
            return null;
        }

        // DataManager handles storage now, just emit events for compatibility
        const locationData = this.getLocation(locationKey);
        const isNewLocation = !locationData;

        // Emit events for backward compatibility
        if (isNewLocation) {
            const newLocation = {
                lat: lat,
                lng: lng,
                locationName: locationName,
                items: [item],
                lastUpdate: Date.now()
            };
            this._emit('locationAdded', locationKey, newLocation);
        } else {
            this._emit('locationUpdated', locationKey, locationData);
        }
        this._emit('itemAdded', locationKey, item);

        return locationKey;
    }
    
    /**
     * Get all items for a location
     * @param {string} locationKey
     * @returns {object|null} Location data with items, or null if not found
     */
    getLocation(locationKey) {
        if (!window.dataManager) return null;

        const locations = window.dataManager.getLocations();
        const location = locations[locationKey];

        if (!location) return null;

        // Transform to expected format
        return {
            lat: location.lat,
            lng: location.lng,
            locationName: location.locationName,
            items: location.clusters.flatMap(cluster => cluster.items || []),
            lastUpdate: Date.now()
        };
    }

    /**
     * Get items for a location
     * @param {string} locationKey
     * @returns {Array} Array of items, or empty array if not found
     */
    getItems(locationKey) {
        const location = this.getLocation(locationKey);
        return location ? location.items : [];
    }

    /**
     * Get all location keys
     * @returns {Array} Array of location keys
     */
    getAllLocationKeys() {
        if (!window.dataManager) return [];
        return Object.keys(window.dataManager.getLocations());
    }

    /**
     * Get all locations with their data
     * @returns {object} All locations
     */
    getAllLocations() {
        if (!window.dataManager) return {};

        const locations = window.dataManager.getLocations();
        const result = {};

        for (const [key, location] of Object.entries(locations)) {
            result[key] = {
                lat: location.lat,
                lng: location.lng,
                locationName: location.locationName,
                items: location.clusters.flatMap(cluster => cluster.items || []),
                lastUpdate: Date.now()
            };
        }

        return result;
    }

    /**
     * Get location count
     * @returns {number}
     */
    getLocationCount() {
        return this.getAllLocationKeys().length;
    }

    /**
     * Get total item count across all locations
     * @returns {number}
     */
    getTotalItemCount() {
        if (!window.dataManager) return 0;

        const locations = window.dataManager.getLocations();
        return Object.values(locations).reduce((sum, loc) => sum + loc.itemCount, 0);
    }

    /**
     * Get item count for a specific location
     * @param {string} locationKey
     * @returns {number}
     */
    getItemCount(locationKey) {
        const location = this.getLocation(locationKey);
        return location ? location.items.length : 0;
    }
    
    /**
     * Update/replace an item in a location (deprecated - DataManager handles updates)
     * Kept for backward compatibility
     * @param {string} locationKey - The location key
     * @param {object} itemData - The item data to update/replace with
     * @returns {boolean} True if updated, false if location doesn't exist
     */
    updateItem(locationKey, itemData) {
        // DataManager handles updates, just emit event for compatibility
        const location = this.getLocation(locationKey);
        if (location) {
            this._emit('locationUpdated', locationKey, location);
            return true;
        }
        return false;
    }

    /**
     * Clear all locations (deprecated - DataManager handles clearing)
     * Kept for backward compatibility
     */
    clear() {
        // DataManager handles data clearing, just emit event
        this._emit('dataRestored');
    }
    
    /**
     * Get the location name for a location key
     * @param {string} locationKey
     * @returns {string|null}
     */
    getLocationName(locationKey) {
        const location = this.getLocation(locationKey);
        return location ? location.locationName : null;
    }
    
    /**
     * Subscribe to events
     * @param {string} event - Event name ('locationAdded', 'locationUpdated', 'itemAdded')
     * @param {function} callback - Callback function
     */
    on(event, callback) {
        if (this.listeners[event]) {
            this.listeners[event].push(callback);
        }
    }
    
    /**
     * Unsubscribe from events
     * @param {string} event - Event name
     * @param {function} callback - Callback function to remove
     */
    off(event, callback) {
        if (this.listeners[event]) {
            this.listeners[event] = this.listeners[event].filter(cb => cb !== callback);
        }
    }
    
    /**
     * Emit an event
     * @private
     */
    _emit(event, ...args) {
        if (this.listeners[event]) {
            for (const callback of this.listeners[event]) {
                try {
                    callback(...args);
                } catch (e) {
                    console.error(`Error in LocationStore event handler for '${event}':`, e);
                }
            }
        }
    }
    
    /**
     * Extract keywords from HTML content (kept for backward compatibility)
     * Looks for common patterns in tweet/news HTML
     * @param {string} html - HTML content
     * @returns {Array} Array of keywords
     */
    extractKeywords(html) {
        const keywords = [];

        // Extract hashtags
        const hashtagMatches = html.match(/#\w+/g);
        if (hashtagMatches) {
            keywords.push(...hashtagMatches.map(h => h.toLowerCase()));
        }

        // Extract mentions
        const mentionMatches = html.match(/@\w+/g);
        if (mentionMatches) {
            keywords.push(...mentionMatches.slice(0, 3));  // Limit mentions
        }

        // Look for data attributes with keywords
        const keywordAttrMatch = html.match(/data-keywords="([^"]+)"/);
        if (keywordAttrMatch) {
            keywords.push(...keywordAttrMatch[1].split(',').map(k => k.trim()));
        }

        // Deduplicate
        return [...new Set(keywords)];
    }
}

// Create global instance
window.locationStore = new LocationStore();

// Also export the class for potential extension
window.LocationStore = LocationStore;
