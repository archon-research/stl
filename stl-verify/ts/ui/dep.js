import {
  Toggle,
  ToggleGroup,
  index_parts_exports25 as index_parts_exports,
  index_parts_exports26 as index_parts_exports2
} from "/node_modules/.vite/deps/chunk-BLAQC2JA.js?v=661cbe4f";
import "/node_modules/.vite/deps/chunk-JCMH2W7Z.js?v=661cbe4f";
import {
  require_jsx_runtime
} from "/node_modules/.vite/deps/chunk-Y3TRD2FL.js?v=661cbe4f";
import "/node_modules/.vite/deps/chunk-OSNXXM3O.js?v=661cbe4f";
import {
  __toESM,
  require_react
} from "/node_modules/.vite/deps/chunk-ANJGFBW7.js?v=661cbe4f";

// ../node_modules/@pandacss/shared/dist/index.mjs
var memo = (fn) => {
  const cache = /* @__PURE__ */ new Map();
  const get = (...args) => {
    const key = JSON.stringify(args);
    if (cache.has(key)) {
      return cache.get(key);
    }
    const result = fn(...args);
    cache.set(key, result);
    return result;
  };
  return get;
};
var regex = /-(\w|$)/g;
var callback = (_dashChar, char) => char.toUpperCase();
var camelCaseProperty = memo((property) => {
  if (property.startsWith("--")) return property;
  let str = property.toLowerCase();
  str = str.startsWith("-ms-") ? str.substring(1) : str;
  return str.replace(regex, callback);
});
var wordRegex = /([A-Z])/g;
var msRegex = /^ms-/;
var hypenateProperty = memo((property) => {
  if (property.startsWith("--")) return property;
  return property.replace(wordRegex, "-$1").replace(msRegex, "-ms-").toLowerCase();
});
var fns = ["min", "max", "clamp", "calc"];
var fnRegExp = new RegExp(`^(${fns.join("|")})\\(.*\\)`);
var lengthUnits = "cm,mm,Q,in,pc,pt,px,em,ex,ch,rem,lh,rlh,vw,vh,vmin,vmax,vb,vi,svw,svh,lvw,lvh,dvw,dvh,cqw,cqh,cqi,cqb,cqmin,cqmax,%";
var lengthUnitsPattern = `(?:${lengthUnits.split(",").join("|")})`;
var lengthRegExp = new RegExp(`^[+-]?[0-9]*.?[0-9]+(?:[eE][+-]?[0-9]+)?${lengthUnitsPattern}$`);
var longHandPhysical = /* @__PURE__ */ new Set();
var longHandLogical = /* @__PURE__ */ new Set();
var shorthandsOfLonghands = /* @__PURE__ */ new Set();
var shorthandsOfShorthands = /* @__PURE__ */ new Set();
longHandLogical.add("backgroundBlendMode");
longHandLogical.add("isolation");
longHandLogical.add("mixBlendMode");
shorthandsOfShorthands.add("animation");
longHandLogical.add("animationComposition");
longHandLogical.add("animationDelay");
longHandLogical.add("animationDirection");
longHandLogical.add("animationDuration");
longHandLogical.add("animationFillMode");
longHandLogical.add("animationIterationCount");
longHandLogical.add("animationName");
longHandLogical.add("animationPlayState");
shorthandsOfLonghands.add("animationRange");
longHandLogical.add("animationRangeEnd");
longHandLogical.add("animationRangeStart");
longHandLogical.add("animationTimingFunction");
longHandLogical.add("animationTimeline");
shorthandsOfLonghands.add("scrollTimeline");
longHandLogical.add("scrollTimelineAxis");
longHandLogical.add("scrollTimelineName");
longHandLogical.add("timelineScope");
shorthandsOfLonghands.add("viewTimeline");
longHandLogical.add("viewTimelineAxis");
longHandLogical.add("viewTimelineInset");
longHandLogical.add("viewTimelineName");
shorthandsOfShorthands.add("background");
longHandLogical.add("backgroundAttachment");
longHandLogical.add("backgroundClip");
longHandLogical.add("backgroundColor");
longHandLogical.add("backgroundImage");
longHandLogical.add("backgroundOrigin");
longHandLogical.add("backgroundRepeat");
longHandLogical.add("backgroundSize");
shorthandsOfLonghands.add("backgroundPosition");
longHandLogical.add("backgroundPositionX");
longHandLogical.add("backgroundPositionY");
shorthandsOfShorthands.add("border");
shorthandsOfLonghands.add("borderColor");
shorthandsOfLonghands.add("borderStyle");
shorthandsOfLonghands.add("borderWidth");
shorthandsOfShorthands.add("borderBlock");
longHandLogical.add("borderBlockColor");
longHandLogical.add("borderBlockStyle");
longHandLogical.add("borderBlockWidth");
shorthandsOfLonghands.add("borderBlockStart");
shorthandsOfLonghands.add("borderTop");
longHandLogical.add("borderBlockStartColor");
longHandPhysical.add("borderTopColor");
longHandLogical.add("borderBlockStartStyle");
longHandPhysical.add("borderTopStyle");
longHandLogical.add("borderBlockStartWidth");
longHandPhysical.add("borderTopWidth");
shorthandsOfLonghands.add("borderBlockEnd");
shorthandsOfLonghands.add("borderBottom");
longHandLogical.add("borderBlockEndColor");
longHandPhysical.add("borderBottomColor");
longHandLogical.add("borderBlockEndStyle");
longHandPhysical.add("borderBottomStyle");
longHandLogical.add("borderBlockEndWidth");
longHandPhysical.add("borderBottomWidth");
shorthandsOfShorthands.add("borderInline");
shorthandsOfLonghands.add("borderInlineColor");
shorthandsOfLonghands.add("borderInlineStyle");
shorthandsOfLonghands.add("borderInlineWidth");
shorthandsOfLonghands.add("borderInlineStart");
shorthandsOfLonghands.add("borderLeft");
longHandLogical.add("borderInlineStartColor");
longHandPhysical.add("borderLeftColor");
longHandLogical.add("borderInlineStartStyle");
longHandPhysical.add("borderLeftStyle");
longHandLogical.add("borderInlineStartWidth");
longHandPhysical.add("borderLeftWidth");
shorthandsOfLonghands.add("borderInlineEnd");
shorthandsOfLonghands.add("borderRight");
longHandLogical.add("borderInlineEndColor");
longHandPhysical.add("borderRightColor");
longHandLogical.add("borderInlineEndStyle");
longHandPhysical.add("borderRightStyle");
longHandLogical.add("borderInlineEndWidth");
longHandPhysical.add("borderRightWidth");
shorthandsOfLonghands.add("borderImage");
longHandLogical.add("borderImageOutset");
longHandLogical.add("borderImageRepeat");
longHandLogical.add("borderImageSlice");
longHandLogical.add("borderImageSource");
longHandLogical.add("borderImageWidth");
shorthandsOfLonghands.add("borderRadius");
longHandLogical.add("borderStartEndRadius");
longHandLogical.add("borderStartStartRadius");
longHandLogical.add("borderEndEndRadius");
longHandLogical.add("borderEndStartRadius");
longHandPhysical.add("borderTopLeftRadius");
longHandPhysical.add("borderTopRightRadius");
longHandPhysical.add("borderBottomLeftRadius");
longHandPhysical.add("borderBottomRightRadius");
longHandLogical.add("boxShadow");
longHandLogical.add("accentColor");
longHandLogical.add("appearance");
longHandLogical.add("aspectRatio");
shorthandsOfLonghands.add("caret");
longHandLogical.add("caretColor");
longHandLogical.add("caretShape");
longHandLogical.add("cursor");
longHandLogical.add("imeMode");
longHandLogical.add("inputSecurity");
shorthandsOfLonghands.add("outline");
longHandLogical.add("outlineColor");
longHandLogical.add("outlineOffset");
longHandLogical.add("outlineStyle");
longHandLogical.add("outlineWidth");
longHandLogical.add("pointerEvents");
longHandLogical.add("resize");
longHandLogical.add("textOverflow");
longHandLogical.add("userSelect");
shorthandsOfLonghands.add("gridGap");
shorthandsOfLonghands.add("gap");
longHandLogical.add("gridRowGap");
longHandLogical.add("rowGap");
longHandLogical.add("gridColumnGap");
longHandLogical.add("columnGap");
shorthandsOfLonghands.add("placeContent");
longHandLogical.add("alignContent");
longHandLogical.add("justifyContent");
shorthandsOfLonghands.add("placeItems");
longHandLogical.add("alignItems");
longHandLogical.add("justifyItems");
shorthandsOfLonghands.add("placeSelf");
longHandLogical.add("alignSelf");
longHandLogical.add("justifySelf");
longHandLogical.add("boxSizing");
longHandLogical.add("fieldSizing");
longHandLogical.add("blockSize");
longHandPhysical.add("height");
longHandLogical.add("inlineSize");
longHandPhysical.add("width");
longHandLogical.add("maxBlockSize");
longHandPhysical.add("maxHeight");
longHandLogical.add("maxInlineSize");
longHandPhysical.add("maxWidth");
longHandLogical.add("minBlockSize");
longHandPhysical.add("minHeight");
longHandLogical.add("minInlineSize");
longHandPhysical.add("minWidth");
shorthandsOfShorthands.add("margin");
shorthandsOfLonghands.add("marginBlock");
longHandLogical.add("marginBlockStart");
longHandPhysical.add("marginTop");
longHandLogical.add("marginBlockEnd");
longHandPhysical.add("marginBottom");
shorthandsOfLonghands.add("marginInline");
longHandLogical.add("marginInlineStart");
longHandPhysical.add("marginLeft");
longHandLogical.add("marginInlineEnd");
longHandPhysical.add("marginRight");
longHandLogical.add("marginTrim");
shorthandsOfLonghands.add("overscrollBehavior");
longHandLogical.add("overscrollBehaviorBlock");
longHandPhysical.add("overscrollBehaviorY");
longHandLogical.add("overscrollBehaviorInline");
longHandPhysical.add("overscrollBehaviorX");
shorthandsOfShorthands.add("padding");
shorthandsOfLonghands.add("paddingBlock");
longHandLogical.add("paddingBlockStart");
longHandPhysical.add("paddingTop");
longHandLogical.add("paddingBlockEnd");
longHandPhysical.add("paddingBottom");
shorthandsOfLonghands.add("paddingInline");
longHandLogical.add("paddingInlineStart");
longHandPhysical.add("paddingLeft");
longHandLogical.add("paddingInlineEnd");
longHandPhysical.add("paddingRight");
longHandLogical.add("visibility");
longHandLogical.add("color");
longHandLogical.add("colorScheme");
longHandLogical.add("forcedColorAdjust");
longHandLogical.add("opacity");
longHandLogical.add("printColorAdjust");
shorthandsOfLonghands.add("columns");
longHandLogical.add("columnCount");
longHandLogical.add("columnWidth");
longHandLogical.add("columnFill");
longHandLogical.add("columnSpan");
shorthandsOfLonghands.add("columnRule");
longHandLogical.add("columnRuleColor");
longHandLogical.add("columnRuleStyle");
longHandLogical.add("columnRuleWidth");
longHandLogical.add("contain");
shorthandsOfLonghands.add("containIntrinsicSize");
longHandLogical.add("containIntrinsicBlockSize");
longHandLogical.add("containIntrinsicWidth");
longHandLogical.add("containIntrinsicHeight");
longHandLogical.add("containIntrinsicInlineSize");
shorthandsOfLonghands.add("container");
longHandLogical.add("containerName");
longHandLogical.add("containerType");
longHandLogical.add("contentVisibility");
longHandLogical.add("counterIncrement");
longHandLogical.add("counterReset");
longHandLogical.add("counterSet");
longHandLogical.add("display");
shorthandsOfLonghands.add("flex");
longHandLogical.add("flexBasis");
longHandLogical.add("flexGrow");
longHandLogical.add("flexShrink");
shorthandsOfLonghands.add("flexFlow");
longHandLogical.add("flexDirection");
longHandLogical.add("flexWrap");
longHandLogical.add("order");
shorthandsOfShorthands.add("font");
longHandLogical.add("fontFamily");
longHandLogical.add("fontSize");
longHandLogical.add("fontStretch");
longHandLogical.add("fontStyle");
longHandLogical.add("fontWeight");
longHandLogical.add("lineHeight");
shorthandsOfLonghands.add("fontVariant");
longHandLogical.add("fontVariantAlternates");
longHandLogical.add("fontVariantCaps");
longHandLogical.add("fontVariantEastAsian");
longHandLogical.add("fontVariantEmoji");
longHandLogical.add("fontVariantLigatures");
longHandLogical.add("fontVariantNumeric");
longHandLogical.add("fontVariantPosition");
longHandLogical.add("fontFeatureSettings");
longHandLogical.add("fontKerning");
longHandLogical.add("fontLanguageOverride");
longHandLogical.add("fontOpticalSizing");
longHandLogical.add("fontPalette");
longHandLogical.add("fontVariationSettings");
longHandLogical.add("fontSizeAdjust");
longHandLogical.add("fontSmooth");
longHandLogical.add("fontSynthesisPosition");
longHandLogical.add("fontSynthesisSmallCaps");
longHandLogical.add("fontSynthesisStyle");
longHandLogical.add("fontSynthesisWeight");
shorthandsOfLonghands.add("fontSynthesis");
longHandLogical.add("lineHeightStep");
longHandLogical.add("boxDecorationBreak");
longHandLogical.add("breakAfter");
longHandLogical.add("breakBefore");
longHandLogical.add("breakInside");
longHandLogical.add("orphans");
longHandLogical.add("widows");
longHandLogical.add("content");
longHandLogical.add("quotes");
shorthandsOfShorthands.add("grid");
longHandLogical.add("gridAutoFlow");
longHandLogical.add("gridAutoRows");
longHandLogical.add("gridAutoColumns");
shorthandsOfShorthands.add("gridTemplate");
shorthandsOfLonghands.add("gridTemplateAreas");
longHandLogical.add("gridTemplateColumns");
longHandLogical.add("gridTemplateRows");
shorthandsOfShorthands.add("gridArea");
shorthandsOfLonghands.add("gridRow");
longHandLogical.add("gridRowStart");
longHandLogical.add("gridRowEnd");
shorthandsOfLonghands.add("gridColumn");
longHandLogical.add("gridColumnStart");
longHandLogical.add("gridColumnEnd");
longHandLogical.add("alignTracks");
longHandLogical.add("justifyTracks");
longHandLogical.add("masonryAutoFlow");
longHandLogical.add("imageOrientation");
longHandLogical.add("imageRendering");
longHandLogical.add("imageResolution");
longHandLogical.add("objectFit");
longHandLogical.add("objectPosition");
longHandLogical.add("initialLetter");
longHandLogical.add("initialLetterAlign");
shorthandsOfLonghands.add("listStyle");
longHandLogical.add("listStyleImage");
longHandLogical.add("listStylePosition");
longHandLogical.add("listStyleType");
longHandLogical.add("clip");
longHandLogical.add("clipPath");
shorthandsOfLonghands.add("mask");
longHandLogical.add("maskClip");
longHandLogical.add("maskComposite");
longHandLogical.add("maskImage");
longHandLogical.add("maskMode");
longHandLogical.add("maskOrigin");
longHandLogical.add("maskPosition");
longHandLogical.add("maskRepeat");
longHandLogical.add("maskSize");
longHandLogical.add("maskType");
shorthandsOfLonghands.add("maskBorder");
longHandLogical.add("maskBorderMode");
longHandLogical.add("maskBorderOutset");
longHandLogical.add("maskBorderRepeat");
longHandLogical.add("maskBorderSlice");
longHandLogical.add("maskBorderSource");
longHandLogical.add("maskBorderWidth");
shorthandsOfShorthands.add("all");
longHandLogical.add("textRendering");
longHandLogical.add("zoom");
shorthandsOfLonghands.add("offset");
longHandLogical.add("offsetAnchor");
longHandLogical.add("offsetDistance");
longHandLogical.add("offsetPath");
longHandLogical.add("offsetPosition");
longHandLogical.add("offsetRotate");
longHandLogical.add("WebkitBoxOrient");
longHandLogical.add("WebkitLineClamp");
longHandPhysical.add("lineClamp");
longHandPhysical.add("maxLines");
longHandLogical.add("blockOverflow");
shorthandsOfLonghands.add("overflow");
longHandLogical.add("overflowBlock");
longHandPhysical.add("overflowY");
longHandLogical.add("overflowInline");
longHandPhysical.add("overflowX");
longHandLogical.add("overflowClipMargin");
longHandLogical.add("scrollGutter");
longHandLogical.add("scrollBehavior");
longHandLogical.add("page");
longHandLogical.add("pageBreakAfter");
longHandLogical.add("pageBreakBefore");
longHandLogical.add("pageBreakInside");
shorthandsOfShorthands.add("inset");
shorthandsOfLonghands.add("insetBlock");
longHandLogical.add("insetBlockStart");
longHandPhysical.add("top");
longHandLogical.add("insetBlockEnd");
longHandPhysical.add("bottom");
shorthandsOfLonghands.add("insetInline");
longHandLogical.add("insetInlineStart");
longHandPhysical.add("left");
longHandLogical.add("insetInlineEnd");
longHandPhysical.add("right");
longHandLogical.add("clear");
longHandLogical.add("float");
longHandLogical.add("overlay");
longHandLogical.add("position");
longHandLogical.add("zIndex");
longHandLogical.add("rubyAlign");
longHandLogical.add("rubyMerge");
longHandLogical.add("rubyPosition");
longHandLogical.add("overflowAnchor");
shorthandsOfShorthands.add("scrollMargin");
shorthandsOfLonghands.add("scrollMarginBlock");
longHandLogical.add("scrollMarginBlockStart");
longHandPhysical.add("scrollMarginTop");
longHandLogical.add("scrollMarginBlockEnd");
longHandPhysical.add("scrollMarginBottom");
shorthandsOfLonghands.add("scrollMarginInline");
longHandLogical.add("scrollMarginInlineStart");
longHandPhysical.add("scrollMarginLeft");
longHandLogical.add("scrollMarginInlineEnd");
longHandPhysical.add("scrollMarginRight");
shorthandsOfShorthands.add("scrollPadding");
shorthandsOfLonghands.add("scrollPaddingBlock");
longHandLogical.add("scrollPaddingBlockStart");
longHandPhysical.add("scrollPaddingTop");
longHandLogical.add("scrollPaddingBlockEnd");
longHandPhysical.add("scrollPaddingBottom");
shorthandsOfLonghands.add("scrollPaddingInline");
longHandLogical.add("scrollPaddingInlineStart");
longHandPhysical.add("scrollPaddingLeft");
longHandLogical.add("scrollPaddingInlineEnd");
longHandPhysical.add("scrollPaddingRight");
longHandLogical.add("scrollSnapAlign");
longHandLogical.add("scrollSnapStop");
shorthandsOfLonghands.add("scrollSnapType");
longHandLogical.add("scrollbarColor");
longHandLogical.add("scrollbarWidth");
longHandLogical.add("shapeImageThreshold");
longHandLogical.add("shapeMargin");
longHandLogical.add("shapeOutside");
longHandLogical.add("azimuth");
longHandLogical.add("borderCollapse");
longHandLogical.add("borderSpacing");
longHandLogical.add("captionSide");
longHandLogical.add("emptyCells");
longHandLogical.add("tableLayout");
longHandLogical.add("verticalAlign");
shorthandsOfLonghands.add("textDecoration");
longHandLogical.add("textDecorationColor");
longHandLogical.add("textDecorationLine");
longHandLogical.add("textDecorationSkip");
longHandLogical.add("textDecorationSkipInk");
longHandLogical.add("textDecorationStyle");
longHandLogical.add("textDecorationThickness");
shorthandsOfLonghands.add("WebkitTextStroke");
longHandLogical.add("WebkitTextStrokeColor");
longHandLogical.add("WebkitTextStrokeWidth");
longHandLogical.add("WebkitTextFillColor");
shorthandsOfLonghands.add("textEmphasis");
longHandLogical.add("textEmphasisColor");
longHandLogical.add("textEmphasisPosition");
longHandLogical.add("textEmphasisStyle");
longHandLogical.add("textShadow");
longHandLogical.add("textUnderlineOffset");
longHandLogical.add("textUnderlinePosition");
longHandLogical.add("hangingPunctuation");
longHandLogical.add("hyphenateCharacter");
longHandLogical.add("hyphenateLimitChars");
longHandLogical.add("hyphens");
longHandLogical.add("letterSpacing");
longHandLogical.add("lineBreak");
longHandLogical.add("overflowWrap");
longHandLogical.add("paintOrder");
longHandLogical.add("tabSize");
longHandLogical.add("textAlign");
longHandLogical.add("textAlignLast");
longHandLogical.add("textIndent");
longHandLogical.add("textJustify");
longHandLogical.add("textSizeAdjust");
longHandLogical.add("textTransform");
shorthandsOfLonghands.add("textWrap");
longHandLogical.add("textWrapMode");
longHandLogical.add("textWrapStyle");
longHandLogical.add("whiteSpace");
longHandLogical.add("whiteSpaceCollapse");
longHandLogical.add("whiteSpaceTrim");
longHandLogical.add("wordBreak");
longHandLogical.add("wordSpacing");
longHandLogical.add("wordWrap");
longHandLogical.add("backfaceVisibility");
longHandLogical.add("perspective");
longHandLogical.add("perspectiveOrigin");
longHandLogical.add("rotate");
longHandLogical.add("scale");
longHandLogical.add("transform");
longHandLogical.add("transformBox");
longHandLogical.add("transformOrigin");
longHandLogical.add("transformStyle");
longHandLogical.add("translate");
shorthandsOfLonghands.add("transition");
longHandLogical.add("transitionBehavior");
longHandLogical.add("transitionDelay");
longHandLogical.add("transitionDuration");
longHandLogical.add("transitionProperty");
longHandLogical.add("transitionTimingFunction");
longHandLogical.add("viewTransitionName");
longHandLogical.add("willChange");
longHandLogical.add("direction");
longHandLogical.add("textCombineUpright");
longHandLogical.add("textOrientation");
longHandLogical.add("unicodeBidi");
longHandLogical.add("writingMode");
longHandLogical.add("backdropFilter");
longHandLogical.add("filter");
longHandLogical.add("mathDepth");
longHandLogical.add("mathShift");
longHandLogical.add("mathStyle");
longHandLogical.add("touchAction");
var UNIT_PX = "px";
var UNIT_EM = "em";
var UNIT_REM = "rem";
var DIGIT_REGEX = new RegExp(String.raw`-?\d+(?:\.\d+|\d*)`);
var UNIT_REGEX = new RegExp(`${UNIT_PX}|${UNIT_EM}|${UNIT_REM}`);
var VALUE_REGEX = new RegExp(`${DIGIT_REGEX.source}(${UNIT_REGEX.source})`);

// ../node_modules/@pandacss/dev/dist/index.mjs
function defineSlotRecipe(config) {
  return config;
}
function createProxy() {
  const identity = (v) => v;
  return new Proxy(identity, {
    get() {
      return identity;
    }
  });
}
var defineTokens = createProxy();
var defineSemanticTokens = createProxy();

// ../node_modules/@archon-research/design-system/src/recipes/switch.recipe.ts
var switchRecipe = defineSlotRecipe({
  className: "toggleSwitch",
  description: "Toggle switch built on Base UI Switch.",
  slots: ["root", "thumb"],
  base: {
    root: {
      position: "relative",
      display: "inline-flex",
      alignItems: "center",
      width: "9",
      height: "5",
      borderRadius: "full",
      borderWidth: "1px",
      borderStyle: "solid",
      borderColor: "border.default",
      bg: "surface.subtle",
      cursor: "pointer",
      flexShrink: "0",
      transitionDuration: "fast",
      transitionProperty: "background-color, border-color",
      "&[data-checked]": {
        bg: "gray.800",
        borderColor: "gray.700",
        _dark: {
          bg: "gray.600",
          borderColor: "gray.500"
        }
      },
      _focusVisible: {
        outline: "2px solid",
        outlineColor: "gray.500",
        outlineOffset: "2px"
      }
    },
    thumb: {
      display: "block",
      width: "3.5",
      height: "3.5",
      borderRadius: "full",
      bg: "gray.400",
      transitionDuration: "fast",
      transitionProperty: "transform, background-color",
      transform: "translateX(2px)",
      "[data-checked] &": {
        transform: "translateX(calc(2.25rem - 100% - 2px))",
        bg: "white",
        _dark: {
          bg: "gray.100"
        }
      }
    }
  }
});

// ../node_modules/@archon-research/design-system/src/layouts/SidebarLayout.tsx
var import_react = __toESM(require_react(), 1);
var import_jsx_runtime = __toESM(require_jsx_runtime(), 1);
var DEFAULT_SIDEBAR_WIDTH = 320;
var DEFAULT_MIN_SIDEBAR_WIDTH = 200;
var DEFAULT_MAX_SIDEBAR_WIDTH = 600;
var DEFAULT_BOTTOM_HEIGHT = 280;
var DEFAULT_MIN_BOTTOM_HEIGHT = 120;
var DEFAULT_MAX_BOTTOM_HEIGHT = 600;
var SIDEBAR_STORAGE_KEY = "sidebar-width";
var BOTTOM_STORAGE_KEY = "bottom-panel-height";
var rootStyle = {
  display: "flex",
  width: "100%",
  height: "100vh",
  minWidth: 0,
  overflow: "hidden"
};
var sidebarBaseStyle = {
  position: "relative",
  flexShrink: 0,
  height: "100vh",
  overflow: "auto",
  borderRight: "1px solid var(--colors-border-subtle, #d0d5dd)",
  background: "var(--colors-surface-default, #ffffff)"
};
var sidebarHandleStyle = {
  position: "absolute",
  top: 0,
  right: 0,
  width: 8,
  height: "100%",
  cursor: "col-resize",
  background: "transparent"
};
var mainStyle = {
  minWidth: 0,
  flex: 1,
  height: "100vh",
  overflow: "hidden",
  display: "flex",
  flexDirection: "column"
};
var topBarStyle = {
  padding: "12px 16px",
  borderBottom: "1px solid var(--colors-border-subtle, #d0d5dd)",
  background: "var(--colors-surface-default, #ffffff)",
  display: "flex",
  justifyContent: "flex-end",
  alignItems: "center",
  minHeight: 64
};
var contentStyle = {
  minWidth: 0,
  minHeight: 0,
  flex: 1,
  overflow: "auto"
};
var mainColumnStyle = {
  minWidth: 0,
  minHeight: 0,
  flex: 1,
  display: "flex",
  flexDirection: "column",
  overflow: "hidden"
};
var bottomPanelStyle = {
  background: "var(--colors-surface-default, #ffffff)",
  overflow: "auto",
  minHeight: 0
};
var bottomHandleStyle = {
  height: 8,
  cursor: "row-resize",
  marginTop: -4,
  marginBottom: -4,
  position: "relative",
  zIndex: 1,
  background: "linear-gradient(to bottom, transparent calc(50% - 0.5px), var(--colors-border-subtle, #d0d5dd) calc(50% - 0.5px), var(--colors-border-subtle, #d0d5dd) calc(50% + 0.5px), transparent calc(50% + 0.5px))"
};
function isBrowser() {
  return typeof window !== "undefined";
}
function readNumberFromStorage(key, fallback) {
  if (!isBrowser()) {
    return fallback;
  }
  const stored = window.localStorage.getItem(key);
  if (!stored) {
    return fallback;
  }
  const parsed = Number(stored);
  if (Number.isNaN(parsed)) {
    return fallback;
  }
  return parsed;
}
function clamp(value, min, max) {
  return Math.max(min, Math.min(max, value));
}
function SidebarLayout({
  sidebar,
  main,
  topBar,
  bottomPanel,
  defaultSidebarWidth = DEFAULT_SIDEBAR_WIDTH,
  minSidebarWidth = DEFAULT_MIN_SIDEBAR_WIDTH,
  maxSidebarWidth = DEFAULT_MAX_SIDEBAR_WIDTH,
  defaultBottomPanelHeight = DEFAULT_BOTTOM_HEIGHT,
  minBottomPanelHeight = DEFAULT_MIN_BOTTOM_HEIGHT,
  maxBottomPanelHeight = DEFAULT_MAX_BOTTOM_HEIGHT,
  sidebarStorageKey = SIDEBAR_STORAGE_KEY,
  bottomPanelStorageKey = BOTTOM_STORAGE_KEY
}) {
  const [sidebarWidth, setSidebarWidth] = (0, import_react.useState)(
    () => clamp(
      readNumberFromStorage(sidebarStorageKey, defaultSidebarWidth),
      minSidebarWidth,
      maxSidebarWidth
    )
  );
  const [bottomPanelHeight, setBottomPanelHeight] = (0, import_react.useState)(
    () => clamp(
      readNumberFromStorage(bottomPanelStorageKey, defaultBottomPanelHeight),
      minBottomPanelHeight,
      maxBottomPanelHeight
    )
  );
  const [sidebarDrag, setSidebarDrag] = (0, import_react.useState)(null);
  const [bottomDrag, setBottomDrag] = (0, import_react.useState)(null);
  (0, import_react.useEffect)(() => {
    if (!isBrowser()) {
      return;
    }
    if (sidebarDrag === null) {
      return;
    }
    const handleMouseMove = (event) => {
      const delta = event.clientX - sidebarDrag.startPosition;
      const next = clamp(
        sidebarDrag.startSize + delta,
        minSidebarWidth,
        maxSidebarWidth
      );
      setSidebarWidth(next);
    };
    const handleMouseUp = () => {
      window.localStorage.setItem(sidebarStorageKey, String(sidebarWidth));
      setSidebarDrag(null);
    };
    document.body.style.userSelect = "none";
    document.body.style.cursor = "col-resize";
    window.addEventListener("mousemove", handleMouseMove);
    window.addEventListener("mouseup", handleMouseUp, { once: true });
    return () => {
      document.body.style.userSelect = "";
      document.body.style.cursor = "";
      window.removeEventListener("mousemove", handleMouseMove);
      window.removeEventListener("mouseup", handleMouseUp);
    };
  }, [sidebarDrag, maxSidebarWidth, minSidebarWidth, sidebarStorageKey, sidebarWidth]);
  (0, import_react.useEffect)(() => {
    if (!isBrowser()) {
      return;
    }
    if (bottomDrag === null) {
      return;
    }
    const handleMouseMove = (event) => {
      const delta = bottomDrag.startPosition - event.clientY;
      const next = clamp(
        bottomDrag.startSize + delta,
        minBottomPanelHeight,
        maxBottomPanelHeight
      );
      setBottomPanelHeight(next);
    };
    const handleMouseUp = () => {
      window.localStorage.setItem(bottomPanelStorageKey, String(bottomPanelHeight));
      setBottomDrag(null);
    };
    document.body.style.userSelect = "none";
    document.body.style.cursor = "row-resize";
    window.addEventListener("mousemove", handleMouseMove);
    window.addEventListener("mouseup", handleMouseUp, { once: true });
    return () => {
      document.body.style.userSelect = "";
      document.body.style.cursor = "";
      window.removeEventListener("mousemove", handleMouseMove);
      window.removeEventListener("mouseup", handleMouseUp);
    };
  }, [
    bottomDrag,
    maxBottomPanelHeight,
    minBottomPanelHeight,
    bottomPanelStorageKey,
    bottomPanelHeight
  ]);
  const startSidebarDrag = (event) => {
    event.preventDefault();
    setSidebarDrag({
      startPosition: event.clientX,
      startSize: sidebarWidth
    });
  };
  const startBottomDrag = (event) => {
    event.preventDefault();
    setBottomDrag({
      startPosition: event.clientY,
      startSize: bottomPanelHeight
    });
  };
  return (0, import_jsx_runtime.jsxs)("div", { style: rootStyle, children: [
    (0, import_jsx_runtime.jsxs)("aside", { style: { ...sidebarBaseStyle, width: sidebarWidth }, children: [
      sidebar,
      (0, import_jsx_runtime.jsx)(
        "div",
        {
          role: "separator",
          "aria-orientation": "vertical",
          "aria-label": "Resize sidebar",
          style: sidebarHandleStyle,
          onMouseDown: startSidebarDrag
        }
      )
    ] }),
    (0, import_jsx_runtime.jsxs)("main", { style: mainStyle, children: [
      topBar ? (0, import_jsx_runtime.jsx)("header", { style: topBarStyle, children: topBar }) : null,
      (0, import_jsx_runtime.jsxs)("div", { style: mainColumnStyle, children: [
        (0, import_jsx_runtime.jsx)("div", { style: contentStyle, children: main }),
        bottomPanel ? (0, import_jsx_runtime.jsxs)(import_jsx_runtime.Fragment, { children: [
          (0, import_jsx_runtime.jsx)(
            "div",
            {
              role: "separator",
              "aria-orientation": "horizontal",
              "aria-label": "Resize bottom panel",
              style: bottomHandleStyle,
              onMouseDown: startBottomDrag
            }
          ),
          (0, import_jsx_runtime.jsx)("section", { style: { ...bottomPanelStyle, height: bottomPanelHeight }, children: bottomPanel })
        ] }) : null
      ] })
    ] })
  ] });
}

// ../node_modules/@archon-research/design-system/src/theme/ThemeProvider.tsx
var import_react3 = __toESM(require_react(), 1);

// ../node_modules/@archon-research/design-system/src/theme/useTheme.ts
var import_react2 = __toESM(require_react(), 1);
var ThemeContext = (0, import_react2.createContext)(
  void 0
);
function useTheme() {
  const context = (0, import_react2.useContext)(ThemeContext);
  if (!context) {
    throw new Error("useTheme must be used within ThemeProvider.");
  }
  return context;
}

// ../node_modules/@archon-research/design-system/src/theme/ThemeProvider.tsx
var import_jsx_runtime2 = __toESM(require_jsx_runtime(), 1);
var STORAGE_KEY = "theme";
var LEGACY_STORAGE_KEY = "archon-theme";
function isBrowser2() {
  return typeof window !== "undefined";
}
function readInitialThemeMode() {
  if (!isBrowser2()) {
    return "auto";
  }
  const stored = window.localStorage.getItem(STORAGE_KEY);
  const legacyStored = window.localStorage.getItem(LEGACY_STORAGE_KEY);
  const value = stored ?? legacyStored;
  if (value === "light" || value === "dark" || value === "auto") {
    return value;
  }
  return "auto";
}
function readSystemPrefersDark() {
  if (!isBrowser2()) {
    return false;
  }
  return window.matchMedia("(prefers-color-scheme: dark)").matches;
}
function ThemeProvider({ children }) {
  const [mode, setModeState] = (0, import_react3.useState)(readInitialThemeMode);
  const [systemPrefersDark, setSystemPrefersDark] = (0, import_react3.useState)(
    readSystemPrefersDark
  );
  (0, import_react3.useEffect)(() => {
    if (!isBrowser2()) {
      return;
    }
    const media = window.matchMedia("(prefers-color-scheme: dark)");
    const handleChange = (event) => {
      setSystemPrefersDark(event.matches);
    };
    setSystemPrefersDark(media.matches);
    if (typeof media.addEventListener === "function") {
      media.addEventListener("change", handleChange);
      return () => media.removeEventListener("change", handleChange);
    }
    media.addListener(handleChange);
    return () => media.removeListener(handleChange);
  }, []);
  const isDark = mode === "dark" || mode === "auto" && systemPrefersDark;
  (0, import_react3.useEffect)(() => {
    if (!isBrowser2()) {
      return;
    }
    window.localStorage.setItem(STORAGE_KEY, mode);
    window.localStorage.setItem(LEGACY_STORAGE_KEY, mode);
    document.documentElement.classList.toggle("dark", isDark);
    document.documentElement.dataset.theme = mode;
  }, [isDark, mode]);
  const value = (0, import_react3.useMemo)(
    () => ({
      mode,
      isDark,
      setMode: (nextMode) => setModeState(nextMode)
    }),
    [isDark, mode]
  );
  return (0, import_jsx_runtime2.jsx)(ThemeContext.Provider, { value, children });
}

// ../node_modules/@archon-research/design-system/src/components/ThemeToggle.tsx
var import_jsx_runtime3 = __toESM(require_jsx_runtime(), 1);
var containerStyle = {
  display: "inline-flex",
  alignItems: "center",
  gap: 6,
  padding: 4,
  borderRadius: 10,
  border: "1px solid var(--colors-border-subtle, #d0d5dd)",
  background: "var(--colors-surface-subtle, #f8f9fb)"
};
var buttonBaseStyle = {
  display: "inline-flex",
  alignItems: "center",
  gap: 6,
  border: 0,
  borderRadius: 8,
  padding: "6px 10px",
  fontSize: 12,
  lineHeight: 1.2,
  cursor: "pointer",
  background: "transparent",
  color: "var(--colors-text-muted, #667085)"
};
var activeButtonStyle = {
  background: "var(--colors-surface-default, #ffffff)",
  color: "var(--colors-text-default, #111827)",
  boxShadow: "0 1px 2px rgba(0, 0, 0, 0.08)"
};
var MODES = [
  { mode: "auto", label: "Auto" },
  { mode: "light", label: "Light" },
  { mode: "dark", label: "Dark" }
];
function ThemeIcon({ mode }) {
  const iconProps = {
    "aria-hidden": true,
    fill: "none",
    height: 14,
    stroke: "currentColor",
    strokeLinecap: "round",
    strokeLinejoin: "round",
    strokeWidth: 1.5,
    viewBox: "0 0 16 16",
    width: 14
  };
  switch (mode) {
    case "auto":
      return (0, import_jsx_runtime3.jsxs)("svg", { ...iconProps, children: [
        (0, import_jsx_runtime3.jsx)("rect", { x: "2.5", y: "3", width: "11", height: "7.5", rx: "1.5" }),
        (0, import_jsx_runtime3.jsx)("path", { d: "M6 13h4" }),
        (0, import_jsx_runtime3.jsx)("path", { d: "M8 10.5V13" })
      ] });
    case "light":
      return (0, import_jsx_runtime3.jsxs)("svg", { ...iconProps, children: [
        (0, import_jsx_runtime3.jsx)("circle", { cx: "8", cy: "8", r: "2.5" }),
        (0, import_jsx_runtime3.jsx)("path", { d: "M8 1.75v1.5" }),
        (0, import_jsx_runtime3.jsx)("path", { d: "M8 12.75v1.5" }),
        (0, import_jsx_runtime3.jsx)("path", { d: "M1.75 8h1.5" }),
        (0, import_jsx_runtime3.jsx)("path", { d: "M12.75 8h1.5" }),
        (0, import_jsx_runtime3.jsx)("path", { d: "M3.6 3.6l1.05 1.05" }),
        (0, import_jsx_runtime3.jsx)("path", { d: "M11.35 11.35l1.05 1.05" }),
        (0, import_jsx_runtime3.jsx)("path", { d: "M11.35 4.65l1.05-1.05" }),
        (0, import_jsx_runtime3.jsx)("path", { d: "M3.6 12.4l1.05-1.05" })
      ] });
    default:
      return (0, import_jsx_runtime3.jsx)("svg", { ...iconProps, children: (0, import_jsx_runtime3.jsx)("path", { d: "M10.75 1.75a5.75 5.75 0 1 0 3.5 10.3A6.5 6.5 0 0 1 10.75 1.75Z" }) });
  }
}
function ThemeToggle() {
  const { mode, setMode } = useTheme();
  return (0, import_jsx_runtime3.jsx)("div", { role: "radiogroup", "aria-label": "Theme mode", style: containerStyle, children: MODES.map((item) => {
    const active = mode === item.mode;
    return (0, import_jsx_runtime3.jsxs)(
      "button",
      {
        type: "button",
        role: "radio",
        "aria-checked": active,
        onClick: () => setMode(item.mode),
        style: active ? { ...buttonBaseStyle, ...activeButtonStyle } : buttonBaseStyle,
        children: [
          (0, import_jsx_runtime3.jsx)(ThemeIcon, { mode: item.mode }),
          item.label
        ]
      },
      item.mode
    );
  }) });
}

// ../node_modules/@archon-research/design-system/src/components/StyledSelect.tsx
var import_jsx_runtime4 = __toESM(require_jsx_runtime(), 1);
var wrapperStyle = {
  position: "relative",
  display: "inline-flex",
  alignItems: "center",
  width: "100%"
};
var selectStyle = {
  width: "100%",
  minWidth: 0,
  height: 36,
  borderWidth: 1,
  borderStyle: "solid",
  borderColor: "var(--colors-border-subtle, #d0d5dd)",
  borderRadius: 8,
  paddingLeft: 12,
  paddingRight: 40,
  background: "var(--colors-surface-default, #ffffff)",
  color: "var(--colors-text-default, #111827)",
  fontSize: 14,
  lineHeight: 1.4,
  fontFamily: "inherit",
  appearance: "none",
  WebkitAppearance: "none",
  MozAppearance: "none"
};
var disabledSelectStyle = {
  opacity: 0.65,
  cursor: "not-allowed"
};
var chevronStyle = {
  position: "absolute",
  top: "50%",
  right: 12,
  width: 16,
  height: 16,
  color: "var(--colors-text-muted, #667085)",
  pointerEvents: "none",
  transform: "translateY(-50%)"
};
function SelectChevron() {
  return (0, import_jsx_runtime4.jsx)("svg", { "aria-hidden": "true", viewBox: "0 0 16 16", style: chevronStyle, children: (0, import_jsx_runtime4.jsx)(
    "path",
    {
      d: "M4 6.5L8 10l4-3.5",
      fill: "none",
      stroke: "currentColor",
      strokeLinecap: "round",
      strokeLinejoin: "round",
      strokeWidth: "1.5"
    }
  ) });
}
function StyledSelect({
  children,
  className,
  ...props
}) {
  return (0, import_jsx_runtime4.jsxs)("div", { className, style: wrapperStyle, children: [
    (0, import_jsx_runtime4.jsx)(
      "select",
      {
        ...props,
        style: props.disabled ? { ...selectStyle, ...disabledSelectStyle } : selectStyle,
        children
      }
    ),
    (0, import_jsx_runtime4.jsx)(SelectChevron, {})
  ] });
}
export {
  SidebarLayout,
  StyledSelect,
  index_parts_exports as Switch,
  index_parts_exports2 as Tabs,
  ThemeProvider,
  ThemeToggle,
  Toggle,
  ToggleGroup,
  switchRecipe,
  useTheme
};
//# sourceMappingURL=@archon-research_design-system.js.map
