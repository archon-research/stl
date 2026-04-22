from dataclasses import dataclass, field
from pathlib import Path

import numpy as np
import pandas as pd

ROMAN_TO_ARABIC = {"I": "1", "II": "2", "III": "3", "IV": "4", "V": "5"}


def _weighted_avg(g: pd.DataFrame) -> float:
    """Weighted average of scored rows in a group, returning NaN if none scored."""
    valid = g.dropna(subset=["score"])
    if valid.empty:
        return np.nan
    return np.average(valid["score"], weights=valid["sub_weight"])


@dataclass
class SURAFResults:
    """End-to-end SURAF scoring: loads weights and assessor scores,
    computes weighted averages, and maps to a Capital Risk Requirement."""

    # Weight lookups
    sub_weights: pd.Series = field(init=False, repr=False, default=None)
    pillar_weights: pd.Series = field(init=False, repr=False, default=None)

    # Per-assessor results
    assessor_results: pd.DataFrame = field(init=False, repr=False, default=None)

    # Aggregates
    avg_score: float = field(init=False, default=None)
    total_score_1: int = field(init=False, default=None)
    total_scored: int = field(init=False, default=None)

    # CRR config (interpolation arrays)
    _crr_scores: np.ndarray = field(init=False, repr=False, default=None)
    _crr_values: np.ndarray = field(init=False, repr=False, default=None)
    _penalty_ns: np.ndarray = field(init=False, repr=False, default=None)
    _penalty_pps: np.ndarray = field(init=False, repr=False, default=None)

    # CRR outputs
    unadjusted_crr: float = field(init=False, default=None)
    penalty: float = field(init=False, default=None)
    adjusted_crr: float = field(init=False, default=None)

    def load_weights(self, weights_path: Path | str) -> None:
        """Load subsection and pillar weights from a weights CSV."""
        df = pd.read_csv(weights_path, keep_default_na=False)

        self.sub_weights = (
            df[df["subsection_weight"] != ""]
            .drop_duplicates(subset="subsection_ref")
            .set_index("subsection_ref")["subsection_weight"]
            .astype(int)
        )

        pillar_rows = df[df["pillar_weight"] != ""].copy()
        pillar_rows["pillar_num"] = (
            pillar_rows["title"].str.extract(r"PILLAR\s+([IVX]+)", expand=False).map(ROMAN_TO_ARABIC)
        )
        pillar_rows = pillar_rows.dropna(subset="pillar_num")
        self.pillar_weights = pillar_rows.set_index("pillar_num")["pillar_weight"].astype(int)

    def load_assessor_scores(self, assessor_paths: list[Path | str]) -> None:
        """Load assessor score CSVs and compute per-assessor results."""
        rows = []

        for path in assessor_paths:
            path = Path(path)
            df = pd.read_csv(path, keep_default_na=False)

            scoreable = df[df["pillar"].str.isnumeric()][["pillar", "subsection_ref"]].copy()
            scoreable["score"] = pd.to_numeric(df.loc[scoreable.index, "score"], errors="coerce")
            scoreable["sub_weight"] = scoreable["subsection_ref"].map(self.sub_weights)

            # Weighted average per pillar
            pillar_scores = scoreable.groupby("pillar").apply(_weighted_avg, include_groups=False)

            # Overall weighted average across pillars
            pw = self.pillar_weights.reindex(pillar_scores.index)
            valid = pillar_scores.dropna()
            overall = np.average(valid, weights=pw.reindex(valid.index))

            rows.append(
                {
                    "assessor": path.stem,
                    "overall_score": overall,
                    "n_score_1": int((scoreable["score"] == 1).sum()),
                    "n_scored": int(scoreable["score"].notna().sum()),
                }
            )

        self.assessor_results = pd.DataFrame(rows)

    def aggregate(self) -> None:
        """Compute cross-assessor aggregates."""
        self.avg_score = self.assessor_results["overall_score"].mean()
        self.total_score_1 = int(self.assessor_results["n_score_1"].sum())
        self.total_scored = int(self.assessor_results["n_scored"].sum())

    def load_crr_config(self, crr_path: Path | str) -> None:
        """Load the score-to-CRR mapping CSV."""
        df = pd.read_csv(crr_path)
        self._crr_scores = df["score"].values
        self._crr_values = df["crr"].values

    def load_penalty_config(self, penalty_path: Path | str) -> None:
        """Load the score-of-1 penalty mapping CSV."""
        df = pd.read_csv(penalty_path)
        self._penalty_ns = df["n_score_1"].values
        self._penalty_pps = df["penalty_pp"].values

    def map_crr(self) -> None:
        """Map the average score to an unadjusted CRR via interpolation."""
        self.unadjusted_crr = float(np.interp(self.avg_score, self._crr_scores, self._crr_values))

    def apply_penalty(self) -> None:
        """Apply the score-of-1 penalty and compute the adjusted CRR."""
        self.penalty = float(np.interp(self.total_score_1, self._penalty_ns, self._penalty_pps))
        self.adjusted_crr = min(self.unadjusted_crr + self.penalty, 100.0)

    def run(
        self,
        weights_path: Path | str,
        assessor_paths: list[Path | str],
        crr_path: Path | str,
        penalty_path: Path | str,
    ) -> None:
        """End-to-end: load inputs, score, aggregate, and compute CRR."""
        self.load_weights(weights_path)
        self.load_assessor_scores(assessor_paths)
        self.aggregate()
        self.load_crr_config(crr_path)
        self.load_penalty_config(penalty_path)
        self.map_crr()
        self.apply_penalty()

    def summary(self) -> str:
        """Return a formatted summary string."""
        lines = [
            f"Average score:         {self.avg_score:.3f}",
            f"Unadjusted CRR:        {self.unadjusted_crr:.1f}%",
            "",
            f"Subsections scored 1:  {self.total_score_1} (out of {self.total_scored} scored)",
            f"Score-1 penalty:       +{self.penalty:.1f}pp",
            "",
            f"Adjusted CRR:          {self.adjusted_crr:.1f}%",
            f"Implied max leverage:  {100 / self.adjusted_crr:.1f}x",
        ]
        return "\n".join(lines)
