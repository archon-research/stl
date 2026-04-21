import { SidebarLayout } from '@archon-research/design-system';
import { useEffect, useMemo, useState } from 'react';

import { SelectedStarOverview } from './components/shared/SelectedStarOverview';
import { StarSidebar } from './components/shared/StarSidebar';
import { TopBar } from './components/shared/TopBar';
import { getStars } from './lib/api';
import { PARAMS, useUrlParam } from './lib/url-params';
import type { Star } from './types/allocation';

function isAbortError(error: unknown): boolean {
  return error instanceof DOMException && error.name === 'AbortError';
}

function toErrorMessage(error: unknown): string {
  return error instanceof Error ? error.message : 'Unknown request failure.';
}

function App() {
  const [stars, setStars] = useState<Star[]>([]);
  const [isLoading, setIsLoading] = useState(true);
  const [errorMessage, setErrorMessage] = useState<string | null>(null);
  const [reloadKey, setReloadKey] = useState(0);
  const [selectedStarId, setSelectedStarId] = useUrlParam(PARAMS.star);

  useEffect(() => {
    const controller = new AbortController();

    setIsLoading(true);
    setErrorMessage(null);

    void getStars(controller.signal)
      .then((response) => {
        setStars(response);
      })
      .catch((error: unknown) => {
        if (isAbortError(error)) {
          return;
        }

        setErrorMessage(toErrorMessage(error));
      })
      .finally(() => {
        if (!controller.signal.aborted) {
          setIsLoading(false);
        }
      });

    return () => controller.abort();
  }, [reloadKey]);

  useEffect(() => {
    if (isLoading) {
      return;
    }

    if (stars.length === 0) {
      if (selectedStarId !== null) {
        setSelectedStarId(null);
      }
      return;
    }

    if (!selectedStarId || !stars.some((star) => star.id === selectedStarId)) {
      setSelectedStarId(stars[0]?.id ?? null);
    }
  }, [isLoading, selectedStarId, setSelectedStarId, stars]);

  const selectedStar = useMemo(
    () => stars.find((star) => star.id === selectedStarId) ?? null,
    [selectedStarId, stars],
  );

  return (
    <SidebarLayout
      sidebar={
        <StarSidebar
          stars={stars}
          selectedStarId={selectedStarId}
          isLoading={isLoading}
          errorMessage={errorMessage}
          onRetry={() => setReloadKey((value) => value + 1)}
          onSelectStar={setSelectedStarId}
        />
      }
      topBar={<TopBar selectedStar={selectedStar} starCount={stars.length} />}
      main={
        <SelectedStarOverview
          selectedStar={selectedStar}
          isLoading={isLoading}
          errorMessage={errorMessage}
        />
      }
    />
  );
}

export default App;
