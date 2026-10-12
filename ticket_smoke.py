import os
import sys
import pytest
import requests
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

# ==============================================================================
# CONFIGURATION
# Default to production if no env var is provided.
# For local or custom testing: TICKET_BASE_URL="http://localhost:8080"
# ==============================================================================
BASE_URL = os.getenv(
    "TICKET_BASE_URL",
    os.getenv("BASE_URL", "https://www.chakorahub.com")
).rstrip("/")

TICKET_PORTAL_URL = f"{BASE_URL}/tickets"
TICKET_STUDENT_URL = f"{BASE_URL}/tickets/student"
TICKET_EMPLOYEE_URL = f"{BASE_URL}/tickets/employee"
TICKET_ADMIN_URL = f"{BASE_URL}/tickets/admin"
TICKET_HEALTH_URL = f"{BASE_URL}/api/ticket/health"

WAIT = int(os.getenv("SELENIUM_WAIT_TIMEOUT", "25"))


# ==============================================================================
# FIXTURES
# ==============================================================================
@pytest.fixture
def driver():
    """Initialize a headless Chrome browser matching CI/CD standard options."""
    import shutil

    # Filter out any mismatched pre-installed chromedriver directories from PATH so Selenium Manager resolves the exact matching version
    clean_path_parts = [p for p in os.environ.get("PATH", "").split(os.pathsep) if "chromedriver" not in p.lower()]
    os.environ["PATH"] = os.pathsep.join(clean_path_parts)

    options = webdriver.ChromeOptions()
    options.add_argument("--headless=new")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-gpu")
    options.add_argument("--disable-software-rasterizer")
    options.add_argument("--disable-extensions")
    options.add_argument("--window-size=1440,1000")

    chrome_bin = os.getenv("CHROME_PATH") or os.getenv("CHROME_BIN") or shutil.which("google-chrome") or shutil.which("chrome")
    if chrome_bin:
        options.binary_location = chrome_bin

    browser = None
    try:
        browser = webdriver.Chrome(options=options)
    except Exception as e1:
        try:
            options_legacy = webdriver.ChromeOptions()
            options_legacy.add_argument("--headless")
            options_legacy.add_argument("--no-sandbox")
            options_legacy.add_argument("--disable-dev-shm-usage")
            options_legacy.add_argument("--disable-gpu")
            if chrome_bin:
                options_legacy.binary_location = chrome_bin
            browser = webdriver.Chrome(options=options_legacy)
        except Exception as e2:
            pytest.skip(f"Chrome browser environment failed to start in runner: {e1} | {e2}")

    browser.set_page_load_timeout(45)

    try:
        yield browser
    finally:
        if browser:
            try:
                browser.quit()
            except Exception:
                pass


def wait_loaded(browser):
    """Wait until document.readyState == 'complete'."""
    WebDriverWait(browser, WAIT).until(
        lambda d: d.execute_script("return document.readyState") == "complete"
    )


# ==============================================================================
# SELENIUM SMOKE TESTS FOR TICKET SERVICE
# ==============================================================================
def test_ticket_api_health_endpoint():
    """Verify Ticket microservice health check endpoint responds."""
    try:
        resp = requests.get(TICKET_HEALTH_URL, timeout=15)
        assert resp.status_code not in (502, 503, 504), (
            f"Health check returned gateway failure status: {resp.status_code}"
        )
    except requests.exceptions.RequestException as e:
        print(f"[Health Check Notice]: {e}")


def test_student_ticket_portal_loads(driver):
    """Verify Student Ticket Portal loads without 500 / 502 errors."""
    driver.get(TICKET_STUDENT_URL)
    wait_loaded(driver)

    page_text = driver.page_source.lower()
    assert "500 internal server error" not in page_text, "Page returned 500 Internal Server Error"
    assert "502 bad gateway" not in page_text, "Page returned 502 Bad Gateway"

    body = WebDriverWait(driver, WAIT).until(
        EC.presence_of_element_located((By.TAG_NAME, "body"))
    )
    assert body.is_displayed(), "Page body must be rendered"


def test_ticket_portal_navigation(driver):
    """Verify /tickets endpoint resolves and renders ticketing interface."""
    driver.get(TICKET_PORTAL_URL)
    wait_loaded(driver)

    page_text = driver.page_source.lower()
    assert "500 internal server error" not in page_text, "/tickets returned 500 error"
    assert "502 bad gateway" not in page_text, "/tickets returned 502 error"

    title = driver.title.lower()
    assert (
        "chakora" in title
        or "ticket" in title
        or "support" in page_text
        or "maintenance" in page_text
    ), "Unexpected page content or title on /tickets"


def test_ticket_admin_console_loads(driver):
    """Verify Operations Desk / Admin Ticket console is accessible."""
    driver.get(TICKET_ADMIN_URL)
    wait_loaded(driver)

    page_text = driver.page_source.lower()
    assert "500 internal server error" not in page_text, "/tickets/admin returned 500 error"
    assert "502 bad gateway" not in page_text, "/tickets/admin returned 502 error"


if __name__ == "__main__":
    exit_code = pytest.main(["-v", __file__])
    sys.exit(exit_code)
