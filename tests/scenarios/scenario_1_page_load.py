"""
Scenario 1: Basic page load test

Navigates to localhost and waits for the page to load.
"""

from playwright.async_api import Page


async def run(page: Page, log) -> bool:
    """Run the page load scenario. Returns True if successful."""
    try:
        # Wait for the page to be fully loaded
        await page.wait_for_load_state("networkidle")
        log("Page loaded successfully")

        # Verify we can get the page title
        title = await page.title()
        if not title:
            log("Page has no title")
            return False

        log(f"Page title: '{title}'")

        # Wait a moment to ensure everything is stable
        await page.wait_for_timeout(1000)

        return True

    except Exception as e:
        log(f"Page load test failed: {e}")
        return False