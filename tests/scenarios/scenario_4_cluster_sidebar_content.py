"""
Scenario 4: Cluster sidebar content verification test

Load page, wait for interactive marker, click it, verify popup appears,
then click cluster-sidebar-button and verify sidebar appears with content,
and verify that the sidebar location matches the clicked marker's location.
"""

from playwright.async_api import Page


async def run(page: Page, log) -> bool:
    """Run the cluster sidebar content verification scenario. Returns True if successful."""
    try:
        # Wait for the page to be fully loaded
        await page.wait_for_load_state("networkidle")
        log("Page loaded successfully")

        # Wait for map container to be present
        await page.wait_for_selector("#map", timeout=10000)
        log("Map container found")

        # Wait for interactive markers to appear
        log("Waiting for interactive markers...")
        await page.wait_for_selector(".leaflet-marker-icon.leaflet-interactive", timeout=30000)
        log("Interactive markers found")

        # Wait a moment for popup event handlers to be attached
        log("Waiting for popup system to be ready...")
        await page.wait_for_timeout(2000)
        log("Popup system ready")

        # Find all leaflet marker icons
        marker_icons = page.locator(".leaflet-marker-icon.leaflet-interactive")

        # Find the first marker that is actually visible in the viewport
        visible_marker = None
        marker_count = await marker_icons.count()
        log(f"Found {marker_count} marker icons total")

        for i in range(marker_count):
            marker = marker_icons.nth(i)
            # Check if the marker is visible in the viewport
            if await marker.is_visible():
                bounding_box = await marker.bounding_box()
                if bounding_box and bounding_box['x'] >= 0 and bounding_box['y'] >= 0:
                    # Additional check: ensure marker is within viewport bounds
                    viewport = page.viewport_size
                    if (bounding_box['x'] + bounding_box['width'] <= viewport['width'] and
                        bounding_box['y'] + bounding_box['height'] <= viewport['height']):
                        visible_marker = marker
                        log(f"Found visible marker icon at index {i}")
                        break

        if not visible_marker:
            raise Exception("No marker found in viewport")

        # Click on the visible marker
        await visible_marker.click()
        log("Clicked on visible marker")

        # Click the cluster-sidebar-button (should appear in the popup)
        log("Clicking cluster-sidebar-button...")
        await page.click(".cluster-sidebar-button", timeout=5000)
        log("Clicked cluster-sidebar-button")

        # Wait for sidebar to appear
        log("Waiting for sidebar to appear...")
        await page.wait_for_selector("#news_sidebar", timeout=5000)
        log("Sidebar appeared")

        # Wait for sidebar content to load and verify it has items
        log("Waiting for sidebar content to load...")
        await page.wait_for_selector("#news_sidebar .sidebar-item", timeout=10000)
        log("Sidebar items loaded")

        # Verify sidebar has actual content (at least one sidebar-item)
        sidebar_items = page.locator("#news_sidebar .sidebar-item")
        item_count = await sidebar_items.count()
        if item_count == 0:
            raise Exception("Sidebar appeared but contains no items")
        log(f"Sidebar contains {item_count} items")

        # Get the location key that the sidebar was opened with
        sidebar_location_key = await page.evaluate("""
            () => {
                // Get the current location key from the sidebar object
                if (window.newsSidebar && window.newsSidebar.currentLocationKey) {
                    return window.newsSidebar.currentLocationKey;
                }
                return null;
            }
        """)

        if not sidebar_location_key:
            raise Exception("Could not get location key from sidebar")

        log(f"Sidebar location key: '{sidebar_location_key}'")

        # Verify the sidebar location matches what was expected
        sidebar_location_text = await page.locator("#news_sidebar .sidebar-location-text").text_content()
        log(f"Sidebar location text: '{sidebar_location_text}'")

        # Extract the location name from the sidebar header
        # The format is typically "Location Name (X items)" or just "Location Name"
        sidebar_location_name = sidebar_location_text.split(' (')[0] if ' (' in sidebar_location_text else sidebar_location_text

        # Get the expected location name from the locationStore for the sidebar's location key
        expected_location_name = await page.evaluate("""
            (locationKey) => {
                if (window.locationStore) {
                    const location = window.locationStore.getLocation(locationKey);
                    return location ? location.locationName : locationKey;
                }
                return locationKey;
            }
        """, sidebar_location_key)

        log(f"Expected location name: '{expected_location_name}', Sidebar location name: '{sidebar_location_name}'")

        # Verify the location matches (case-insensitive comparison)
        if sidebar_location_name.lower() != expected_location_name.lower():
            raise Exception(f"Sidebar location '{sidebar_location_name}' does not match expected location '{expected_location_name}'")

        log("Sidebar location matches clicked marker location - test successful!")
        return True

    except Exception as e:
        log(f"Test failed: {e}")
        return False