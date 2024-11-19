import pytest

from girder_wholetale.lib.license import WholeTaleLicense


@pytest.mark.plugin("wholetale")
def testGetLicenses(server):
    resp = server.request(path="/license", method="GET", type="application/json")

    # Make sure that we support CC0
    is_supported = all(x for x in resp.json if (x["spdx"] == "CC0-1.0"))
    assert is_supported
    # Make sure that we support CC-BY
    is_supported = all(x for x in resp.json if (x["spdx"] == "CC-BY-4.0"))
    assert is_supported


@pytest.mark.plugin("wholetale")
def testMinimumLicenses():
    # Test that we're supporting a non-zero number of licenses
    wholetale_license = WholeTaleLicense()
    assert len(wholetale_license.supported_licenses()) == 2
    assert wholetale_license.supported_spdxes() == {"CC-BY-4.0", "CC0-1.0"}
    assert wholetale_license.default_spdx() == "CC-BY-4.0"
    assert "name" in wholetale_license.license_from_spdx("CC-BY-4.0")
    assert "text" in wholetale_license.license_from_spdx("CC0-1.0")
    assert wholetale_license.license_from_spdx("CC-BY-4.0")["spdx"] == "CC-BY-4.0"
    assert (
        wholetale_license.license_from_spdx("CC-BY-4.0")["name"]
        == "Creative Commons Attribution 4.0 International"
    )
