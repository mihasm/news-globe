/**
 * Time Filter Manager - Centralized coordinator for all time filtering operations
 * Ensures consistent time filtering across map, sidebar, and UI components
 */

class TimeFilterManager {
    constructor() {
        this.timeFilterPanel = null;
        this.mapInterface = null;
        this.newsSidebar = null;
        this.isInitialized = false;
        this.pendingOperations = new Set();
        this.lastFilterChange = 0;

        // Debounce settings
        this.debounceDelay = 150; // ms
        this.debounceTimer = null;

        this._init();
    }

    /**
     * Initialize the manager and wait for dependencies
     */
    _init() {
        const checkInterval = setInterval(() => {
            if (window.timeFilterPanel && window.mapInterface !== undefined && window.newsSidebar !== undefined) {
                this.timeFilterPanel = window.timeFilterPanel;
                this.mapInterface = window.mapInterface;
                this.newsSidebar = window.newsSidebar;
                this._setupCallbacks();
                this.isInitialized = true;
                clearInterval(checkInterval);
            }
        }, 100);
    }

    /**
     * Setup unified callbacks for time filter changes
     */
    _setupCallbacks() {
        if (!this.timeFilterPanel) return;

        // Replace any existing callbacks with our unified handler
        this.timeFilterPanel.onFilterChange = () => this._handleFilterChange();
        this.timeFilterPanel.onColorChange = () => this._handleColorChange();
    }

    /**
     * Handle time filter changes
     */
    _handleFilterChange() {
        const now = Date.now();
        this.lastFilterChange = now;

        // Apply filter immediately - let components handle their own debouncing if needed
        this._applyTimeFilter();
    }

    /**
     * Handle color changes (immediate, no debouncing needed)
     */
    _handleColorChange() {
        this._applyColorChanges();
    }

    /**
     * Apply time filter to all components
     */
    _applyTimeFilter() {
        if (!this.isInitialized) return;

        const operationId = `filter-${Date.now()}`;
        this.pendingOperations.add(operationId);

        const timeRange = this.timeFilterPanel.getTimeRange();

        try {
            // 1. Update map markers/clusters (instant from cache if available)
            this._updateMap();

            // 2. Update sidebar (API call)
            this._updateSidebar();

            // 3. Update 3D visualization if active
            this._update3DVisualization();

            // 4. Ensure UI state is synchronized
            this.syncUIState();

        } finally {
            this.pendingOperations.delete(operationId);
        }
    }

    /**
     * Update map markers/clusters
     */
    _updateMap() {
        // When time filter changes, we need to refresh the entire map with filtered data
        // refresh_all_items() will fetch data from DataManager with current time filter applied
        if (typeof window.refresh_all_items === 'function') {
            window.refresh_all_items();
        }

        // Also refresh circle markers if in circle mode
        if (typeof window.refreshCircleMarkers === 'function') {
            window.refreshCircleMarkers();
        }
    }

    /**
     * Update sidebar content
     */
    _updateSidebar() {
        if (this.newsSidebar && this.newsSidebar.isVisible && this.newsSidebar.currentLocationKey) {
            // Use the sidebar's refresh method which makes an API call with time filter parameters
            if (typeof this.newsSidebar._refreshCurrentLocation === 'function') {
                this.newsSidebar._refreshCurrentLocation();
            }
        }
    }

    /**
     * Update 3D visualization
     */
    _update3DVisualization() {
        if (this.mapInterface && this.mapInterface.cesiumMap) {
            this.mapInterface.cesiumMap.refresh();
        }
    }

    /**
     * Apply color changes to all components
     */
    _applyColorChanges() {
        if (!this.isInitialized) return;

        // Update map markers that use color coding
        this._updateMap();

        // Update 3D visualization colors
        this._update3DVisualization();

        // Ensure UI state is synchronized
        this.syncUIState();
    }

    /**
     * Get current time filter parameters for API calls
     */
    getTimeFilterParams() {
        if (!this.timeFilterPanel || !this.timeFilterPanel.isFilterEnabled()) {
            return {};
        }

        const timeRange = this.timeFilterPanel.getTimeRange();
        const params = {};

        if (timeRange.from) {
            params.time_from = new Date(timeRange.from).toISOString();
        }
        if (timeRange.to) {
            params.time_to = new Date(timeRange.to).toISOString();
        }

        return params;
    }

    /**
     * Check if time filtering is currently active
     */
    isFilterActive() {
        return this.timeFilterPanel && this.timeFilterPanel.isFilterEnabled();
    }

    /**
     * Force immediate refresh of all components (bypass debouncing)
     */
    forceRefresh() {
        if (this.debounceTimer) {
            clearTimeout(this.debounceTimer);
            this.debounceTimer = null;
        }
        this._applyTimeFilter();
    }

    /**
     * Synchronize UI elements with current time filter state
     */
    syncUIState() {
        if (!this.timeFilterPanel) return;

        // Update datetime inputs
        const range = this.timeFilterPanel.getTimeRange();
        const fromInput = document.getElementById('time_filter_from');
        const toInput = document.getElementById('time_filter_to');


        if (fromInput) {
            fromInput.value = window.formatDateTimeLocal(range.from);
        }
        if (toInput) {
            toInput.value = window.formatDateTimeLocal(range.to);
        }

        // Update color pickers
        const colors = this.timeFilterPanel.getColorGradient();
        const colorFromInput = document.getElementById('time_filter_color_from');
        const colorToInput = document.getElementById('time_filter_color_to');

        if (colorFromInput) {
            colorFromInput.value = colors.from;
        }
        if (colorToInput) {
            colorToInput.value = colors.to;
        }

        // Update checkboxes
        const filterEnabledCheckbox = document.getElementById('time_filter_enabled');
        const colorCodingCheckbox = document.getElementById('time_filter_color_coding');

        if (filterEnabledCheckbox) {
            filterEnabledCheckbox.checked = this.timeFilterPanel.isFilterEnabled();
        }
        if (colorCodingCheckbox) {
            colorCodingCheckbox.checked = this.timeFilterPanel.isColorCodingEnabled();
        }
    }


    /**
     * Check if any operations are currently pending
     */
    hasPendingOperations() {
        return this.pendingOperations.size > 0;
    }

    /**
     * Get the last filter change timestamp
     */
    getLastFilterChange() {
        return this.lastFilterChange;
    }
}

// Create global instance
window.timeFilterManager = new TimeFilterManager();