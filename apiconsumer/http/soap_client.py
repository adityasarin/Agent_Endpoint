from __future__ import annotations

import time
import xml.etree.ElementTree as ET
from typing import Any

import httpx

from apiconsumer.http.auth import apply_auth
from apiconsumer.models.pipeline import AuthConfig, EndpointSpec


SOAP11_NS = "http://schemas.xmlsoap.org/soap/envelope/"
SOAP12_NS = "http://www.w3.org/2003/05/soap-envelope"


def build_envelope(body_xml: str, soap_version: str = "1.1") -> str:
    ns = SOAP11_NS if soap_version == "1.1" else SOAP12_NS
    return (
        f'<?xml version="1.0" encoding="utf-8"?>'
        f'<s:Envelope xmlns:s="{ns}">'
        f"<s:Header/>"
        f"<s:Body>{body_xml}</s:Body>"
        f"</s:Envelope>"
    )


def call_soap(
    endpoint: EndpointSpec,
    body_params: dict[str, Any],
    auth: AuthConfig | None = None,
    soap_version: str = "1.1",
) -> dict:
    """
    Render the soap_body_template with body_params, wrap in envelope, POST.
    Returns parsed response as a dict (parsed from XML).
    """
    if not endpoint.soap_body_template:
        raise ValueError("EndpointSpec.soap_body_template is required for SOAP calls")

    body_xml = endpoint.soap_body_template.format(**body_params)
    envelope = build_envelope(body_xml, soap_version)

    content_type = "text/xml; charset=utf-8" if soap_version == "1.1" else "application/soap+xml; charset=utf-8"
    headers: dict[str, str] = {
        "Content-Type": content_type,
    }
    if endpoint.soap_action:
        headers["SOAPAction"] = f'"{endpoint.soap_action}"'

    kwargs: dict[str, Any] = {"headers": headers, "content": envelope.encode("utf-8"), "timeout": 30.0}
    if auth:
        apply_auth(kwargs, auth)

    t0 = time.monotonic()
    with httpx.Client(follow_redirects=True) as client:
        resp = client.post(endpoint.full_url, **kwargs)
    elapsed_ms = (time.monotonic() - t0) * 1000

    resp.raise_for_status()
    return {
        "status_code": resp.status_code,
        "response_time_ms": elapsed_ms,
        "headers": dict(resp.headers),
        "body": resp.text,
        "parsed": _parse_soap_response(resp.text),
    }


def _parse_soap_response(xml_text: str) -> dict:
    """Flatten SOAP body content to a dict."""
    try:
        root = ET.fromstring(xml_text)
        # Strip envelope/body wrappers — return the first child of Body
        for child in root:
            local = child.tag.split("}")[-1] if "}" in child.tag else child.tag
            if local.lower() == "body":
                return _elem_to_dict(child)
        return _elem_to_dict(root)
    except ET.ParseError:
        return {"raw": xml_text}


def _elem_to_dict(elem: ET.Element) -> dict | str:
    children = list(elem)
    if not children:
        return elem.text or ""
    result: dict = {}
    for child in children:
        tag = child.tag.split("}")[-1] if "}" in child.tag else child.tag
        value = _elem_to_dict(child)
        if tag in result:
            if not isinstance(result[tag], list):
                result[tag] = [result[tag]]
            result[tag].append(value)
        else:
            result[tag] = value
    return result
