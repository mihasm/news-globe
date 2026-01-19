/**
 * Time Filter Panel - Core filtering and color coding logic
 * Manages time-based filtering and gradient color calculation for tweets/news items
 */

class TimeFilterPanel {
    constructor() {
        // Time range (timestamps in milliseconds)
        this.timeFrom = null;
        this.timeTo = null;
        
        // Color gradient settings
        this.colorFrom = '#FFC0CB';  // Pink (old)
        this.colorTo = '#990000';    // Dark red (new)
        
        // State flags - filtering enabled by default
        this.filterEnabled = true;
        this.colorCodingEnabled = false;
        
        // Rainbow instance for color interpolation
        this.rainbow = new Rainbow();
        this.rainbow.setSpectrum(this.colorFrom, this.colorTo);
        
        // Callbacks
        this.onFilterChange = null;
        this.onColorChange = null;
        
        // Initialize with default range
        this._initDefaultRange();
    }
    
    /**
     * Initialize default time range: 7 days ago to current time
     */
    _initDefaultRange() {
        const now = Date.now();
        const sevenDaysAgo = now - (7 * 24 * 60 * 60 * 1000);

        // Use current time instead of next midnight
        this.timeFrom = sevenDaysAgo;
        this.timeTo = now;
    }
    
    /**
     * Set time range
     * @param {number} from - Timestamp in milliseconds
     * @param {number} to - Timestamp in milliseconds
     */
    setTimeRange(from, to) {
        if (from >= to) {
            console.warn('TimeFilterPanel: from time must be before to time');
            return;
        }
        
        this.timeFrom = from;
        this.timeTo = to;
        
        // Trigger callback
        if (this.onFilterChange) {
            this.onFilterChange();
        }
    }
    
    /**
     * Set color gradient
     * @param {string} colorFrom - Hex color for old items
     * @param {string} colorTo - Hex color for new items
     */
    setColorGradient(colorFrom, colorTo) {
        this.colorFrom = colorFrom;
        this.colorTo = colorTo;
        
        // Update rainbow spectrum
        this.rainbow.setSpectrum(colorFrom, colorTo);
        
        // Trigger callback
        if (this.onColorChange) {
            this.onColorChange();
        }
    }
    
    /**
     * Check if an item's published_at is within the time range
     * @param {object} item - Item with published_at property
     * @returns {boolean}
     */
    isItemInRange(item) {
        if (!this.filterEnabled) {
            return true;  // Filter disabled, show all items
        }

        if (!item || !item.published_at) {
            return false;  // No timestamp, exclude
        }

        const itemTime = new Date(item.published_at).getTime();
        return itemTime >= this.timeFrom && itemTime <= this.timeTo;
    }
    
    /**
     * Get the latest published_at timestamp from a location's items
     * @param {object} location - Location object with items array
     * @returns {number|null} - Timestamp in milliseconds, or null if no items
     */
    getLatestItemDate(location) {
        if (!location || !location.items || location.items.length === 0) {
            return null;
        }

        let latest = 0;
        for (const item of location.items) {
            if (item.published_at) {
                const itemTime = new Date(item.published_at).getTime();
                if (itemTime > latest) {
                    latest = itemTime;
                }
            }
        }

        return latest > 0 ? latest : null;
    }
    
    /**
     * Calculate color for an item based on its published_at within the time range
     * @param {object} item - Item with published_at property
     * @returns {string} - Hex color code
     */
    getColorForItem(item) {
        if (!this.colorCodingEnabled) {
            return '#990000';  // Default red when color coding disabled
        }

        if (!item || !item.published_at) {
            return this.colorFrom;  // Default to old color if no timestamp
        }

        const itemTime = new Date(item.published_at).getTime();
        const range = this.timeTo - this.timeFrom;

        if (range <= 0) {
            return this.colorFrom;
        }

        // Calculate percentage (0 = old, 1 = new)
        let percentage = (itemTime - this.timeFrom) / range;
        percentage = Math.max(0, Math.min(1, percentage));  // Clamp to 0-1

        // Get color from rainbow gradient
        const colorHex = '#' + this.rainbow.colorAt(percentage * 100);
        return colorHex;
    }
    
    /**
     * Calculate color for a location based on its latest item
     * @param {object} location - Location object with items array
     * @returns {string} - Hex color code
     */
    getColorForLocation(location) {
        if (!this.colorCodingEnabled) {
            return '#990000';  // Default red when color coding disabled
        }
        
        const latestDate = this.getLatestItemDate(location);
        if (!latestDate) {
            return this.colorFrom;  // Default to old color if no items
        }
        
        // Create a temporary item object for color calculation
        const tempItem = { published_at: new Date(latestDate).toISOString() };
        return this.getColorForItem(tempItem);
    }
    
    /**
     * Filter an array of items by time range
     * @param {Array} items - Array of items
     * @returns {Array} - Filtered array
     */
    filterItems(items) {
        if (!this.filterEnabled) {
            return items;  // Filter disabled, return all
        }
        
        return items.filter(item => this.isItemInRange(item));
    }
    
    /**
     * Get current time range
     * @returns {object} - {from, to} in milliseconds
     */
    getTimeRange() {
        return {
            from: this.timeFrom,
            to: this.timeTo
        };
    }
    
    /**
     * Get current color gradient
     * @returns {object} - {from, to} hex colors
     */
    getColorGradient() {
        return {
            from: this.colorFrom,
            to: this.colorTo
        };
    }
    
    /**
     * Enable/disable time filtering
     * @param {boolean} enabled
     */
    setFilterEnabled(enabled) {
        this.filterEnabled = enabled;
        if (this.onFilterChange) {
            this.onFilterChange();
        }
    }
    
    /**
     * Enable/disable color coding
     * @param {boolean} enabled
     */
    setColorCodingEnabled(enabled) {
        this.colorCodingEnabled = enabled;
        if (this.onColorChange) {
            this.onColorChange();
        }
    }
    
    /**
     * Check if filter is enabled
     * @returns {boolean}
     */
    isFilterEnabled() {
        return this.filterEnabled;
    }

    /**
     * Check if color coding is enabled
     * @returns {boolean}
     */
    isColorCodingEnabled() {
        return this.colorCodingEnabled;
    }
}

// Create global instance
window.timeFilterPanel = new TimeFilterPanel();
