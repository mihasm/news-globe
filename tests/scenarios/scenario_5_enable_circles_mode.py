"""
Scenario 5: Enable circles mode test

Load page, open layers panel, select circles mode radio button,
and verify that circles mode is enabled.
"""

from playwright.async_api import Page


async def run(page: Page, log) -> bool:
    """Run the enable circles mode scenario. Returns True if successful."""
    try:
        # Wait for the page to be fully loaded
        await page.wait_for_load_state("networkidle")
        log("Page loaded successfully")

        # Wait for map container to be present
        await page.wait_for_selector("#map", timeout=10000)
        log("Map container found")

        # Open the layers panel by clicking the layers button label
        layers_button_label = page.locator("label[title='Layers']")
        await page.wait_for_selector("label[title='Layers']", timeout=5000)
        await layers_button_label.click()
        log("Layers panel opened")

        # Wait for the layers panel to be visible
        await page.wait_for_selector("#layers_panel", timeout=5000)
        log("Layers panel visible")

        # Get circle count before enabling circles mode
        circles_before = await page.evaluate("""
            () => {
                if (typeof window.getCircleCount === 'function') {
                    return window.getCircleCount();
                }
                return -1; // Function not available
            }
        """)

        if circles_before < 0:
            raise Exception("Circle counting function not available")

        log(f"Circles before enabling mode: {circles_before}")

        # Find and click the circles mode radio button
        circles_radio = page.locator("#marker_mode_circles")
        if not await circles_radio.is_visible():
            raise Exception("Circles mode radio button not found")
        await circles_radio.check()
        log("Selected circles mode radio button")

        # Wait a moment for the mode change to take effect
        await page.wait_for_timeout(1000)
        log("Waiting for circles mode to be applied")

        # Verify that circles mode is enabled
        is_circles_enabled = await page.evaluate("""
            () => {
                if (typeof window.isCircleModeEnabled === 'function') {
                    return window.isCircleModeEnabled();
                }
                return false;
            }
        """)

        if not is_circles_enabled:
            raise Exception("Circles mode was not enabled after selecting the radio button")

        # Get circle count after enabling circles mode
        circles_after = await page.evaluate("""
            () => {
                if (typeof window.getCircleCount === 'function') {
                    return window.getCircleCount();
                }
                return -1; // Function not available
            }
        """)

        log(f"Circles after enabling mode: {circles_after}")

        # Verify that enabling circles mode actually added circles to the map
        if circles_after <= circles_before:
            raise Exception(f"Enabling circles mode did not add circles to the map (before: {circles_before}, after: {circles_after})")

        log(f"Circles mode successfully added {circles_after - circles_before} circles to the map")

        log("Circles mode successfully enabled and circles are drawn on the map - test successful!")
        return True

    except Exception as e:
        log(f"Test failed: {e}")
        return False