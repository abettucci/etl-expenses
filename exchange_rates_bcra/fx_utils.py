"""Utilidades puras para interpretar la serie de cotización BCRA."""

from datetime import date
from decimal import Decimal, InvalidOperation


FX_SOURCE = "BCRA_USD_OFICIAL_VENDEDOR"


def parse_bcra_records(payload: dict) -> list[dict]:
    """Normaliza la respuesta BCRA y conserva precisión decimal."""
    rows = []
    for result in payload.get("results", []):
        for detail in result.get("detalle", []):
            try:
                quoted_on = date.fromisoformat(str(detail["fecha"])[:10])
                raw_rate = str(detail["valor"]).strip()
                if "," in raw_rate and "." in raw_rate:
                    raw_rate = raw_rate.replace(".", "").replace(",", ".")
                else:
                    raw_rate = raw_rate.replace(",", ".")
                rate = Decimal(raw_rate)
            except (KeyError, ValueError, InvalidOperation):
                continue
            if rate > 0:
                rows.append({
                    "fecha_cotizacion": quoted_on.isoformat(),
                    "tipo_cambio_ars": str(rate),
                    "fuente": FX_SOURCE,
                    "serie": "usd_oficial_minorista_vendedor",
                })
    return rows
