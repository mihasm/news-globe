"""
Scenario 2: Marker click test

Load page, wait for interactive marker, click it, verify popup appears.
"""

from playwright.async_api import Page


async def run(page: Page, log) -> bool:
    """Run the marker click scenario. Returns True if successful."""
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
        log(f"Found visible marker icon at index {i}")
        log(f"Visible marker icon: {visible_marker}")
        # Click on the visible marker
        await visible_marker.click()
        log("Clicked on visible marker")

        # Wait for popup to appear
        log("Waiting for popup to appear...")
        await page.wait_for_selector(".leaflet-popup", timeout=5000)
        log("Popup appeared - test successful!")

        return True

    except Exception as e:
        log(f"Test failed: {e}")
        return False