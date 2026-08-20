/**
 * The failure responses the mock has to be able to produce.
 *
 * No operation in the generated document declares anything but `200` and `422`,
 * while the real API 404s an unknown id, 400s an unsupported mode combination
 * and 503s a failed share-data lookup. A mock that can only succeed teaches the
 * app that those paths do not exist, so every one of them is reachable here —
 * through `response.untyped`, which records that the gap is in the document.
 *
 * Body shapes follow FastAPI: a hand-raised `HTTPException` carries a string
 * `detail`, a rejected `Query`/path parameter carries the validation array.
 */
export type Problem = {
  status: number;
  body: { detail: string | ValidationDetail[] };
};

type ValidationDetail = {
  loc: (string | number)[];
  msg: string;
  type: string;
};

export function problemResponse(problem: Problem): Response {
  return Response.json(problem.body, { status: problem.status });
}

export function notFound(detail: string): Problem {
  return { status: 404, body: { detail } };
}

export function badRequest(detail: string): Problem {
  return { status: 400, body: { detail } };
}

/** A domain rule the API checks by hand, so a plain string, not the array. */
export function unprocessable(detail: string): Problem {
  return { status: 422, body: { detail } };
}

/** A rejected query parameter, in the shape pydantic produces. */
export function invalidQueryParam(name: string, msg: string): Problem {
  return {
    status: 422,
    body: { detail: [{ loc: ['query', name], msg, type: 'value_error' }] },
  };
}

export function unavailable(detail: string): Problem {
  return { status: 503, body: { detail } };
}

export type Parsed<T> =
  | { ok: true; value: T }
  | { ok: false; problem: Problem };
