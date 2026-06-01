import numpy as np
import pandas as pd
from scipy.stats import norm, t


class Aggregator:


    def __init__(
        self,
        residuals_df: pd.DataFrame,
        seed: int = 0
    ) -> None:
        """
        Input: prices_df --> matrix T x n, where T is the time variable and
        n is the number of different assets.
        Output: aggregate_prices_df --> matrix T x n where the asset prices are
        correlated through Kendall metric. 
        """
        
        # from prices_df, compute correlation matrix
        # metric used: Spearman --> monotonic relationships (not necessarily linear) | uses rank ordering → reduces effect of outliers
        
        rho_s = residuals_df.corr(method="spearman")
        p_corr_matrix = pd.DataFrame(
            2 * np.sin(np.pi * rho_s / 6),
            index=rho_s.index,
            columns=rho_s.columns
        )
        p_corr_matrix = p_corr_matrix.sort_index(axis=0).sort_index(axis=1)
        
        values = p_corr_matrix.values.copy()
        np.fill_diagonal(values, 1.0)
        p_corr_matrix = pd.DataFrame(values, index=p_corr_matrix.index, columns=p_corr_matrix.columns)

        # insert PSD check with regularization algo
        if not self.is_psd(p_corr_matrix):
            print("\nNon PSD matrix: applying Rebonato-Jackel...\n")
            p_corr_matrix = self.rebonato_jackel(p_corr_matrix)

        self.corr_matrix = p_corr_matrix
        self.residuals_df = residuals_df
        self.rng = np.random.default_rng(seed)


    @staticmethod
    def is_psd(
        matrix: pd.DataFrame,
        tol: float = 1e-10
    ) -> bool:
        eigvals = np.linalg.eigvalsh(matrix)
        return np.min(eigvals) >= -tol


    @staticmethod
    def rebonato_jackel(
        corr_matrix: pd.DataFrame,
        *,
        eps: float = 1e-8
    ) -> pd.DataFrame:
        """
        Nearest PSD correlation matrix via Rebonato-Jäckel eigenvalue flooring.
        """

        A = corr_matrix.values.astype(float)
        eigvals, eigvecs = np.linalg.eigh(A)
        eigvals_clipped = np.clip(eigvals, eps, None)
        A_psd = eigvecs @ np.diag(eigvals_clipped) @ eigvecs.T
        A_psd = 0.5 * (A_psd + A_psd.T)
        d = np.sqrt(np.diag(A_psd))
        A_corr = A_psd / np.outer(d, d)

        A_corr = 0.5 * (A_corr + A_corr.T)
        np.fill_diagonal(A_corr, 1.0)

        if not Aggregator.is_psd(A_corr):
            raise ValueError("Rebonato-Jäckel regularization failed: matrix still not PSD.")

        return pd.DataFrame(
            A_corr,
            index=corr_matrix.index,
            columns=corr_matrix.columns
        )


    def generate_gaussian_copula_samples(
        self,
        n_sims: int,
        forecasted_step: int
    ) -> dict:
        """
        Generates correlated samples using a Gaussian copula.

        Parameters:
            corr_matrix: np.ndarray of shape (n_assets, n_assets)
                Target cross-asset correlation matrix.
            n_sims: int
                Number of Monte-Carlo scenarios.
            forecasted_step: int
                Forecast horizon (number of steps per scenario).

        Returns:
            np.ndarray of shape (n_sims, forecasted_step, n_assets)
            Correlated uniform samples in (0,1) for each scenario, step, and asset.
        """
        n_assets = self.corr_matrix.shape[0]
        tokens = list(self.corr_matrix.columns)
        n_total = n_sims * forecasted_step
        
        L = np.linalg.cholesky(self.corr_matrix)
        Z = self.rng.standard_normal(size=(n_total, n_assets))

        L = np.asarray(L, dtype=np.float64)
        Z = np.asarray(Z, dtype=np.float64)

        L = np.ascontiguousarray(L)
        Z = np.ascontiguousarray(Z)

        assert np.all(np.isfinite(L))
        assert np.all(np.isfinite(Z))

        with np.errstate(divide='ignore', over='ignore', invalid='ignore'):
            correlated_Z = Z @ L.T

        U = norm.cdf(correlated_Z).reshape(n_sims, forecasted_step, n_assets)
        
        U_dict = {tokens[j]: pd.DataFrame(U[:, :, j],
                                          columns=[f"Step_{i+1}" for i in range(forecasted_step)])
                                          for j in range(n_assets)}
        return U_dict


    def generate_t_copula_samples(
        self,
        n_sims: int,
        forecasted_step: int
    ) -> dict:
        """
        Generates correlated samples using a t-copula.

        Parameters:
            corr_matrix: np.ndarray of shape (n_assets, n_assets)
                Target cross-asset correlation matrix.
            nu: float
                Degrees of freedom for the t-copula.
            n_sims: int
                Number of Monte-Carlo scenarios.
            forecasted_step: int
                Forecast horizon (number of steps per scenario).

        Returns:
            np.ndarray of shape (n_sims, forecasted_step, n_assets)
            Correlated uniform samples in (0,1) for each scenario, step, and asset.
        """
        n_assets = self.corr_matrix.shape[0]
        tokens = list(self.corr_matrix.columns)
        L = np.linalg.cholesky(self.corr_matrix)

        # def estimate_t_dof(
        #     residuals_df: pd.DataFrame, 
        #     corr_matrix: pd.DataFrame
        # ) -> float:
            
        #     Z = residuals_df.values
        #     Sigma = corr_matrix.values
        #     inv_S = np.linalg.inv(Sigma)
        #     det_S = np.linalg.det(Sigma)

        #     T, n = Z.shape

        #     def neg_log_lik(nu):
        #         if nu <= 2:
        #             return 1e10

        #         quad = np.sum((Z @ inv_S) * Z, axis=1)

        #         ll = (
        #             gammaln((nu + n) / 2)
        #             - gammaln(nu / 2)
        #             - (n / 2) * np.log(nu * np.pi)
        #             - 0.5 * np.log(det_S)
        #             - ((nu + n) / 2) * np.log(1 + quad / nu)
        #         )

        #         return -np.mean(ll)

        #     res = minimize(
        #         neg_log_lik,
        #         x0=np.array([8.0]),
        #         bounds=[(2.5, 80.0)],
        #         method="L-BFGS-B"
        #     )

        #     return float(res.x[0])

        # nu = estimate_t_dof(
        #     self.residuals_df,
        #     self.corr_matrix
        # )
        # nu = max(nu, 2.5)
        # TODO(bug#4): t-Copula nu is hardcoded to 3. MLE estimation code exists but is
        # disabled. nu=3 produces very fat tails and may be materially wrong for
        # stablecoins and less volatile collateral. Fix: enable the MLE estimation.
        nu = 3

        n_total = n_sims * forecasted_step
        Z = self.rng.standard_normal(size=(n_total, n_assets))
        chi2_samples = self.rng.chisquare(nu, size=(n_total, 1))
        scaling = np.sqrt(nu / chi2_samples)

        L = np.asarray(L, dtype=np.float64)
        Z = np.asarray(Z, dtype=np.float64)

        L = np.ascontiguousarray(L, dtype=np.float64)
        Z = np.ascontiguousarray(Z, dtype=np.float64)

        assert np.all(np.isfinite(L))
        assert np.all(np.isfinite(Z))

        with np.errstate(divide='ignore', over='ignore', invalid='ignore'):
            t_samples = (Z @ L.T) * scaling
            
        U = t.cdf(t_samples, df=nu).reshape(n_sims, forecasted_step, n_assets)
        
        U_dict = {tokens[j]: pd.DataFrame(U[:, :, j],
                                      columns=[f"Step_{i+1}" for i in range(forecasted_step)])
                                      for j in range(n_assets)}
        return U_dict


    def copula_aggregator(
        self,
        copula_type: str,
        n_sims: int,
        forecasted_step: int
    ) -> np.ndarray:
        
        copula_type = copula_type.upper()
        
        if "GAUSSIAN" in copula_type:
            return self.generate_gaussian_copula_samples(
                n_sims = n_sims,
                forecasted_step = forecasted_step
            )
        else:
            return self.generate_t_copula_samples(
                n_sims = n_sims,
                forecasted_step = forecasted_step
            )
