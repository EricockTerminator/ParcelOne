import asyncio
import aiohttp
import pytest
from parcelone import wfs
import wfs

@pytest.mark.asyncio
async def test_fetch_gml_pages_async(monkeypatch):
    calls = []

    async def fake_fetch(session, url, **kwargs):
        calls.append(url)
        if "startIndex=0" in url:
            return (
                b'<wfs:FeatureCollection numberMatched="3" numberReturned="2">'
                b'<wfs:member/><wfs:member/></wfs:FeatureCollection>'
            )
        elif "startIndex=1000" in url or "startIndex=2" in url:
            return (
                b'<wfs:FeatureCollection numberMatched="3" numberReturned="1">'
                b'<wfs:member/></wfs:FeatureCollection>'
            )
        raise AssertionError(f"unexpected url {url}")

    monkeypatch.setattr(wfs, "_fetch", fake_fetch)
    res = await wfs.fetch_gml_pages_async("C", "123", "", None, page_size=2)
    assert res.ok
    assert len(res.pages) == 2
    assert "startIndex=0" in res.first_url
    assert any("startIndex=2" in c or "startIndex=1000" in c for c in calls)


@pytest.mark.asyncio
async def test_fetch_retries(monkeypatch):
    attempts = {"n": 0}

    class DummyResponse:
        async def read(self):
            return b"ok"

        def raise_for_status(self):
            pass

        async def __aenter__(self):
            attempts["n"] += 1
            if attempts["n"] < 3:
                raise aiohttp.ClientError("boom")
            return self

        async def __aexit__(self, exc_type, exc, tb):
            pass

    class DummySession:
        def get(self, url, timeout):
            return DummyResponse()

    monkeypatch.setattr(asyncio, "sleep", lambda x: asyncio.sleep(0))
    data = await wfs._fetch(DummySession(), "http://example.com")
    assert data == b"ok"
    assert attempts["n"] == 3
