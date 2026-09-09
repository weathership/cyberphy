"""DCAT/DPROD-shaped Turtle + lean SHACL-Core — kvasir-friendly, no JSON-LD.

Syntax goals
------------
* Turtle for catalog graphs (as real as JSON-LD for interchange; sdg-corpora style).
* SHACL-Core subset compatible with kvasir's lean shapes reader (no full RDF stack).
* Optional later: Manchester ABox export; not required for v0.
"""

from __future__ import annotations

from typing import Iterable, Optional
from urllib.parse import quote

from hdf5_iceberg.descriptor import DatasetDescriptor

# Prefixes — DCAT, DCTERMS, DPROD-ish extension under sci:
_PREFIXES = """\
@prefix dcat:  <http://www.w3.org/ns/dcat#> .
@prefix dct:   <http://purl.org/dc/terms/> .
@prefix dprod: <https://w3id.org/dprod#> .
@prefix sci:   <https://hdf5-iceberg.example/sci#> .
@prefix xsd:   <http://www.w3.org/2001/XMLSchema#> .
@prefix sh:    <http://www.w3.org/ns/shacl#> .
@prefix rdf:   <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .
@prefix rdfs:  <http://www.w3.org/2000/01/rdf-schema#> .

"""


def _iri_safe(s: str) -> str:
    return quote(s, safe=":/#-_")


def _lit(s: Optional[str]) -> str:
    if s is None:
        return '""'
    esc = str(s).replace("\\", "\\\\").replace('"', '\\"')
    return f'"{esc}"'


def emit_dcat_ttl(
    descriptors: Iterable[DatasetDescriptor],
    *,
    catalog_iri: str = "https://example.org/hdf5-iceberg/catalog",
) -> str:
    """Emit a DCAT Catalog with Dataset + Distribution + DataService per descriptor.

    * Distribution (dcat:Distribution) → Layer A HDF5 URI (downloadURL).
    * DataService (dcat:DataService) → Iceberg metadata access (endpointURL placeholder).
    * dprod:DataProduct-style typing via sci:AcquisitionProduct for scientific extensions.
    """
    lines = [_PREFIXES]
    cat = f"<{_iri_safe(catalog_iri)}>"
    lines.append(f"{cat} a dcat:Catalog ;")
    lines.append(f"  dct:title {_lit('HDF5 Iceberg metadata catalog')} ;")
    lines.append("  dcat:dataset")
    descs = list(descriptors)
    if not descs:
        lines.append("    <https://example.org/hdf5-iceberg/empty> .\n")
        return "\n".join(lines)

    ds_iris = []
    for i, d in enumerate(descs):
        fp = d.fingerprint or f"anon{i}"
        ds_iris.append(f"<https://example.org/hdf5-iceberg/dataset/{_iri_safe(fp)}>")
    lines.append(",\n    ".join(ds_iris) + " .\n")

    for i, d in enumerate(descs):
        fp = d.fingerprint or f"anon{i}"
        ds = f"<https://example.org/hdf5-iceberg/dataset/{_iri_safe(fp)}>"
        dist = f"<https://example.org/hdf5-iceberg/distribution/{_iri_safe(fp)}>"
        svc = f"<https://example.org/hdf5-iceberg/service/{_iri_safe(fp)}>"
        lines.append(f"{ds} a dcat:Dataset, sci:AcquisitionProduct ;")
        lines.append(f"  dct:identifier {_lit(d.dataset_uuid or fp)} ;")
        lines.append(f"  dct:title {_lit(d.key or d.uri)} ;")
        if d.t_min_ns is not None:
            lines.append(f"  sci:tMinNs {int(d.t_min_ns)} ;")
        if d.t_max_ns is not None:
            lines.append(f"  sci:tMaxNs {int(d.t_max_ns)} ;")
        if d.n_series is not None:
            lines.append(f"  sci:nSeries {int(d.n_series)} ;")
        if d.n_time is not None:
            lines.append(f"  sci:nTime {int(d.n_time)} ;")
        lines.append(f"  sci:layout {_lit(d.layout)} ;")
        lines.append(f"  dcat:distribution {dist} ;")
        lines.append(f"  dcat:accessService {svc} .")
        lines.append("")
        lines.append(f"{dist} a dcat:Distribution ;")
        lines.append(f"  dcat:downloadURL <{_iri_safe(d.uri)}> ;")
        lines.append(f"  dcat:mediaType {_lit('application/x-hdf5')} ;")
        lines.append(f"  dcat:byteSize {int(d.size_bytes)} ;")
        lines.append(f"  dct:format {_lit('HDF5')} .")
        lines.append("")
        lines.append(f"{svc} a dcat:DataService ;")
        lines.append(
            f"  dcat:endpointURL <https://example.org/hdf5-iceberg/iceberg/telemetry.hdf5_datasets> ;"
        )
        lines.append(f"  dct:conformsTo <https://iceberg.apache.org/> ;")
        lines.append(f"  dct:description {_lit('Iceberg pointer-table access (metadata plane)')} .")
        lines.append("")

    return "\n".join(lines)


def emit_shacl_shapes() -> str:
    """Lean SHACL-Core shapes for Dataset / Distribution (kvasir shapes subset)."""
    return _PREFIXES + """\
<https://example.org/hdf5-iceberg/shapes/DatasetShape>
  a sh:NodeShape ;
  sh:targetClass dcat:Dataset ;
  sh:property [
    sh:path dct:identifier ;
    sh:minCount 1 ;
    sh:maxCount 1 ;
    sh:datatype xsd:string ;
  ] ;
  sh:property [
    sh:path dcat:distribution ;
    sh:minCount 1 ;
    sh:class dcat:Distribution ;
  ] .

<https://example.org/hdf5-iceberg/shapes/DistributionShape>
  a sh:NodeShape ;
  sh:targetClass dcat:Distribution ;
  sh:property [
    sh:path dcat:downloadURL ;
    sh:minCount 1 ;
    sh:maxCount 1 ;
  ] ;
  sh:property [
    sh:path dcat:mediaType ;
    sh:minCount 0 ;
    sh:maxCount 1 ;
    sh:datatype xsd:string ;
  ] .

<https://example.org/hdf5-iceberg/shapes/AcquisitionProductShape>
  a sh:NodeShape ;
  sh:targetClass sci:AcquisitionProduct ;
  sh:property [
    sh:path sci:layout ;
    sh:in ( "contiguous" "chunked" ) ;
  ] .
"""
