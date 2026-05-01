declare module '@archon-research/design-system' {
  type AnyComponent = (props: any) => any;

  export const SidebarLayout: AnyComponent;
  export const SkeletonRows: (...args: any[]) => any;
  export const SkeletonStack: AnyComponent;
  export const ThemeProvider: AnyComponent;
  export const SearchInput: AnyComponent;
  export const LoadingIndicator: AnyComponent;
  export const ThemeToggle: AnyComponent;
  export const StyledSelect: AnyComponent;
  export const Toggle: AnyComponent;
  export const ToggleGroup: AnyComponent;
}
