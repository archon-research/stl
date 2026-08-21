"""CoreModelResultsWriter port — appends computed CRR rows to the DB.

The cronjob writes through this; CoreModelRiskService reads the same table
at request time via CoreModelResultsReader.
"""

from typing import Protocol

from app.risk_engine.core_model.runner import CoreModelPipelineResult


class CoreModelResultsWriter(Protocol):
    async def insert(self, result: CoreModelPipelineResult) -> None:
        """Append one computed result row (the table is append-only)."""
        ...
