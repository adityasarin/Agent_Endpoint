from __future__ import annotations

import xml.etree.ElementTree as ET
from pathlib import Path
from typing import Optional
from urllib.parse import urlparse

import httpx

from apiconsumer.models.pipeline import EndpointSpec


_WSDL_NS = {
    "wsdl": "http://schemas.xmlsoap.org/wsdl/",
    "soap": "http://schemas.xmlsoap.org/wsdl/soap/",
    "soap12": "http://schemas.xmlsoap.org/wsdl/soap12/",
    "xsd": "http://www.w3.org/2001/XMLSchema",
}


def parse_wsdl(wsdl_source: str) -> list[dict]:
    """
    Parse a WSDL file or URL and return a list of operation descriptors:
    [{"name": str, "soap_action": str, "endpoint_url": str, "input_parts": list}, ...]
    """
    if wsdl_source.startswith("http://") or wsdl_source.startswith("https://"):
        resp = httpx.get(wsdl_source, timeout=15, follow_redirects=True)
        resp.raise_for_status()
        wsdl_text = resp.text
        base_url = wsdl_source
    else:
        path = Path(wsdl_source)
        if not path.exists():
            raise FileNotFoundError(f"WSDL file not found: {wsdl_source}")
        wsdl_text = path.read_text(encoding="utf-8")
        base_url = ""

    try:
        root = ET.fromstring(wsdl_text)
    except ET.ParseError as exc:
        raise ValueError(f"Invalid WSDL XML: {exc}") from exc

    # Strip namespace from tag for comparison
    def local(tag: str) -> str:
        return tag.split("}")[-1] if "}" in tag else tag

    # Find service endpoint URL
    endpoint_url = base_url
    for elem in root.iter():
        if local(elem.tag) == "address":
            loc = elem.get("location", "")
            if loc.startswith("http"):
                endpoint_url = loc
                break

    # Collect operation → SOAPAction mappings
    soap_actions: dict[str, str] = {}
    for elem in root.iter():
        if local(elem.tag) == "operation":
            op_name = elem.get("name", "")
            action = elem.get("soapAction", "")
            if op_name:
                soap_actions[op_name] = action

    # Collect portType operations and their input messages
    messages: dict[str, list[str]] = {}  # message_name → [part_names]
    for msg_elem in root.iter():
        if local(msg_elem.tag) == "message":
            msg_name = msg_elem.get("name", "")
            parts = [
                p.get("name", "") for p in msg_elem
                if local(p.tag) == "part"
            ]
            messages[msg_name] = parts

    input_messages: dict[str, str] = {}  # op_name → message_name
    for pt_elem in root.iter():
        if local(pt_elem.tag) == "portType":
            for op_elem in pt_elem:
                if local(op_elem.tag) == "operation":
                    op_name = op_elem.get("name", "")
                    for child in op_elem:
                        if local(child.tag) == "input":
                            msg = child.get("message", "").split(":")[-1]
                            input_messages[op_name] = msg

    operations = []
    for op_name, soap_action in soap_actions.items():
        msg_name = input_messages.get(op_name, "")
        input_parts = messages.get(msg_name, [])
        operations.append({
            "name": op_name,
            "soap_action": soap_action,
            "endpoint_url": endpoint_url,
            "input_parts": input_parts,
        })

    if not operations:
        # Fallback: just list all operation names found anywhere
        op_names = set()
        for elem in root.iter():
            if local(elem.tag) == "operation":
                name = elem.get("name", "")
                if name:
                    op_names.add(name)
        operations = [{"name": n, "soap_action": "", "endpoint_url": endpoint_url, "input_parts": []} for n in op_names]

    return operations


def build_soap_endpoint_spec(operation: dict) -> EndpointSpec:
    """Convert a parsed WSDL operation dict to an EndpointSpec."""
    parts = operation.get("input_parts", [])
    # Build a template with {part_name} placeholders
    body_inner = "\n".join(f"  <{p}>{{{p}}}</{p}>" for p in parts)
    op_name = operation["name"]
    body_template = f"<{op_name}>\n{body_inner}\n</{op_name}>"

    parsed = urlparse(operation["endpoint_url"])
    return EndpointSpec(
        api_type="soap",
        base_url=f"{parsed.scheme}://{parsed.netloc}",
        path=parsed.path or "",
        method="POST",
        soap_action=operation.get("soap_action", ""),
        soap_body_template=body_template,
    )
