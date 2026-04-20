import type { paths } from '../generated/openapi-types'

type StarsResponse = NonNullable<
  paths['/v1/stars']['get']['responses']['200']['content']['application/json']
>

const API_BASE_URL = import.meta.env.VITE_API_BASE_URL ?? ''

export async function getStars(): Promise<StarsResponse> {
  const response = await fetch(`${API_BASE_URL}/v1/stars`)
  if (!response.ok) {
    const body = await response.text()
    throw new Error(`GET /v1/stars failed (${response.status}): ${body}`)
  }

  return (await response.json()) as StarsResponse
}