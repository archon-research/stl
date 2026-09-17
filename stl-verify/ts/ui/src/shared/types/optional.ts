/**
 * Lets a type's optional properties also hold an explicit `undefined`.
 *
 * Homomorphic, so `?` and `readonly` survive — it widens what a property
 * accepts, it does not make anything newly optional. Wrap only the optional
 * members: applied to a whole type it widens the required ones too.
 */
export type Undefinable<T> = { [K in keyof T]: T[K] | undefined };
