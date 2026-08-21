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
import type { operations } from './schema.ts';

export type Problem = {
  status: number;
  body: { detail: string | ValidationDetail[] };
};

/**
 * `response.untyped` is the one hole in the contract check: it accepts any body
 * at any status, so nothing above would notice the document gaining the statuses
 * it is standing in for. This asserts the premise instead of the bodies — it
 * stops compiling the day any operation declares a third status, which is the
 * day that endpoint's failure branch should move to `response(<status>)` and be
 * type-checked like the success one.
 */
type DeclaredStatus = {
  [Name in keyof operations]: keyof operations[Name]['responses'];
}[keyof operations];

type IsExactly<Actual, Expected> =
  (<T>() => T extends Actual ? 1 : 2) extends <T>() => T extends Expected
    ? 1
    : 2
    ? true
    : false;

type Assert<T extends true> = T;

export type DocumentDeclaresOnly200And422 = Assert<
  IsExactly<DeclaredStatus, 200 | 422>
>;

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
