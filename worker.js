"use strict";
require("dotenv").config();
const axios = require("axios");
const crypto = require("crypto");

/* =========================
   CONFIG
========================= */
const IS_PROD = (process.env.NODE_ENV || "development") === "production";
const CONFIG = {
  maxProducts:          Number(process.env.MAX_PRODUCTS           || 500),
  maxRetries:           Number(process.env.MAX_RETRIES            || 3),
  retryDelay:           Number(process.env.RETRY_DELAY_MS         || 1500),
  affiliateCid:                process.env.AFFILIATE_CID          || "286505",
  supabaseBatchSize:    Number(process.env.SUPABASE_BATCH_SIZE     || 25),
  debug:                !IS_PROD,
  searchResultsPerTerm: Number(process.env.SEARCH_RESULTS_PER_TERM || 8),
  requestDelayMs:       Number(process.env.REQUEST_DELAY_MS        || 1200),
  axiosTimeoutMs:       Number(process.env.AXIOS_TIMEOUT_MS        || 25000),
  enableTermFiltering:
    String(process.env.ENABLE_TERM_FILTERING || "true").toLowerCase() === "true",
};
const OOS_SUFFIX = " - Out of Stock";

/* =========================
   STRUCTURED LOGGING
========================= */
function writeWorkerLog(level, message, meta = {}) {
  if (!CONFIG.debug && level === "debug") return;
  const line = JSON.stringify({
    timestamp: new Date().toISOString(),
    level,
    source: "worker",
    message,
    ...meta,
  });
  if (level === "error" || level === "warn") {
    process.stderr.write(line + "\n");
  } else {
    process.stdout.write(line + "\n");
  }
}
const workerLogger = {
  debug: (msg, meta = {}) => writeWorkerLog("debug", msg, meta),
  info:  (msg, meta = {}) => writeWorkerLog("info",  msg, meta),
  warn:  (msg, meta = {}) => writeWorkerLog("warn",  msg, meta),
  error: (msg, meta = {}) => writeWorkerLog("error", msg, meta),
};
function debugSupabaseError(context, error) {
  writeWorkerLog("error", `Supabase error: ${context}`, {
    supabaseMessage: error?.message,
    details:         error?.details,
    hint:            error?.hint,
    code:            error?.code,
  });
}

/* =========================
   HTTP CLIENT
========================= */
const httpClient = axios.create({
  timeout: CONFIG.axiosTimeoutMs,
  headers: {
    "User-Agent":
      "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 " +
      "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Accept":
      "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer":         "[google.com](https://www.google.com/)",
  },
  maxRedirects: 10,
});

/* =========================
   UTILITIES
========================= */
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

async function retry(fn, retries = CONFIG.maxRetries, delay = CONFIG.retryDelay) {
  for (let attempt = 1; attempt <= retries; attempt++) {
    try {
      return await fn();
    } catch (err) {
      workerLogger.warn("Retry attempt failed", {
        attempt,
        maxRetries: retries,
        error: err.message,
      });
      if (attempt === retries) throw err;
      await sleep(delay * attempt);
    }
  }
}

function normalizeUrl(url = "") {
  try {
    const u = new URL(url);
    u.hash = "";
    return u.toString();
  } catch {
    return url;
  }
}

function stripOosSuffix(title = "") {
  if (!title) return title;
  return title.replace(/\s*[-–—]\s*out\s*of\s*stock\s*$/i, "").trim();
}

function applyAvailabilityToTitle(title = "", isAvailable = true) {
  const base = stripOosSuffix(title);
  return isAvailable ? base : `${base}${OOS_SUFFIX}`;
}

function parsePriceNumber(p = "") {
  if (p === null || p === undefined) return null;
  if (typeof p === "number") return Math.round(p);
  const num = String(p).replace(/[^\d]/g, "");
  return num ? parseInt(num, 10) : null;
}

function slugify(text = "") {
  return text
    .toLowerCase()
    .replace(/[^\w\s-]/g, "")
    .trim()
    .replace(/\s+/g, "-")
    .replace(/-+/g, "-")
    .substring(0, 80);
}

function generateAffiliateUrl(productUrl = "", title = "") {
  if (!productUrl) return null;
  const slug = slugify(stripOosSuffix(title) || "product");
  return (
    `https://linksredirect.com/?cid=${CONFIG.affiliateCid}` +
    `&subid=esco-${slug}&source=linkkit&url=${encodeURIComponent(productUrl)}`
  );
}

function cleanStoreUrl(url = "") {
  try {
    const u = new URL(url);
    if (u.hostname.includes("redirect.buyhatke.com")) {
      const inner = u.searchParams.get("link");
      if (inner) return cleanStoreUrl(decodeURIComponent(inner));
    }
    if (u.hostname.includes("amazon")) {
      const m = u.pathname.match(/\/(dp|gp\/product)\/([A-Z0-9]{10})/i);
      if (m) return `https://${u.hostname}/dp/${m[2]}`;
    }
    if (u.hostname.includes("flipkart")) {
      const pid = u.searchParams.get("pid");
      if (pid) return `https://www.flipkart.com${u.pathname}?pid=${pid}`;
    }
    const junkParams = [
      "tag", "ref", "ref_", "psc", "th", "price-bh",
      "utm_source", "utm_medium", "utm_campaign", "utm_term", "utm_content",
      "affid", "affExtParam1", "affExtParam2", "cmpid", "gclid", "fbclid",
      "_branch_match_id", "lid", "marketplace", "spLa", "srno", "otracker",
      "fm", "iid", "ssid", "pageUID", "linkCode", "smid", "ascsubtag",
      "source", "ext1", "ext2", "pos",
    ];
    junkParams.forEach((p) => u.searchParams.delete(p));
    return u.toString();
  } catch {
    return url;
  }
}

const STORE_MAP = [
  { match: /amazon\./i,          key: "amazon",           name: "Amazon"           },
  { match: /flipkart\.com/i,     key: "flipkart",         name: "Flipkart"         },
  { match: /croma\.com/i,        key: "croma",            name: "Croma"            },
  { match: /vijaysales\.com/i,   key: "vijaysales",       name: "Vijay Sales"      },
  { match: /jiomart\.com/i,      key: "jiomart",          name: "JioMart"          },
  { match: /reliancedigital/i,   key: "reliance_digital", name: "Reliance Digital" },
  { match: /tatacliq\.com/i,     key: "tatacliq",         name: "Tata CLiQ"        },
  { match: /myntra\.com/i,       key: "myntra",           name: "Myntra"           },
  { match: /ajio\.com/i,         key: "ajio",             name: "Ajio"             },
  { match: /shopsy\.in/i,        key: "shopsy",           name: "Shopsy"           },
  { match: /paytmmall/i,         key: "paytm_mall",       name: "Paytm Mall"       },
  { match: /snapdeal\.com/i,     key: "snapdeal",         name: "Snapdeal"         },
  { match: /snapmint\.com/i,     key: "snapmint",         name: "Snapmint"         },
  { match: /vivo\.com/i,         key: "vivo",             name: "Vivo"             },
  { match: /samsung\.com/i,      key: "samsung",          name: "Samsung"          },
  { match: /mi\.com/i,           key: "mi",               name: "Mi"               },
  { match: /oneplus\.in/i,       key: "oneplus",          name: "OnePlus"          },
  { match: /realme\.com/i,       key: "realme",           name: "Realme"           },
  { match: /lenovo\.com/i,       key: "lenovo",           name: "Lenovo"           },
  { match: /paiinternational\.in/i, key: "paiinternational", name: "Pai International" },
  { match: /poorvika\.com/i,     key: "poorvika",         name: "Poorvika Mobile"  },
  { match: /apple\.com/i,        key: "apple",            name: "Apple"            },
  { match: /ubuy\./i,            key: "ubuy",             name: "Ubuy"             },
  { match: /desertcart/i,        key: "desertcart",       name: "Desertcart"       },
  { match: /bigbasket\.com/i,    key: "bigbasket",        name: "Bigbasket"        },
  { match: /zepto\.com|zeptonow\.com/i, key: "zepto",     name: "Zepto"            },
  { match: /blinkit\.com/i,      key: "blinkit",          name: "Blinkit"          },
  { match: /nykaa\.com/i,        key: "nykaa",            name: "Nykaa"            },
  { match: /purplle\.com/i,      key: "purplle",          name: "Purplle"          },
  { match: /meesho\.com/i,       key: "meesho",           name: "Meesho"           },
  { match: /smytten\.com/i,      key: "smytten",          name: "Smytten"          },
  { match: /getepik\.in/i,       key: "getepik",          name: "Getepik"          },
];

function detectStore(url = "", fallbackName = "") {
  if (!url && !fallbackName) return null;
  for (const s of STORE_MAP) {
    if (s.match.test(url || "")) return { key: s.key, name: s.name };
  }
  if (fallbackName) {
    const norm = String(fallbackName).trim().toLowerCase();
    const hit  = STORE_MAP.find(
      (s) => s.name.toLowerCase() === norm || s.key === norm
    );
    if (hit) return { key: hit.key, name: hit.name };
    const key = norm.replace(/\s+/g, "_").replace(/[^a-z0-9_]/g, "");
    return {
      key,
      name: fallbackName
        .replace(/\b\w/g, (c) => c.toUpperCase())
        .trim(),
    };
  }
  return null;
}

/* =========================
   CLASSIFICATION
========================= */
const CATEGORY_TREE = {
  Electronics: {
    Smartphones: [
      "smartphone", "mobile", "phone", "galaxy", "iphone", "oneplus",
      "redmi", "pixel", "vivo", "oppo", "realme",
    ],
    Laptops: [
      "laptop", "macbook", "notebook", "ultrabook", "thinkpad",
      "ideapad", "inspiron", "pavilion", "vivobook",
    ],
    Tablets: ["tablet", "ipad", "tab ", "galaxy tab", "matepad"],
    Smartwatches: [
      "smartwatch", "smart watch", "apple watch", "galaxy watch", "fitness band",
    ],
    Headphones: ["headphone", "headset", "over-ear", "on-ear"],
    Earbuds: [
      "earbuds", "earphone", "airpods", "tws", "true wireless", "buds",
    ],
    Cameras: [
      "camera", "dslr", "mirrorless", "canon eos", "nikon", "sony alpha",
      "gopro", "powershot", "camcorder", "lens",
    ],
    TVs: [
      "smart tv", "led tv", "oled tv", "qled", "android tv", "google tv",
    ],
    Monitors: ["monitor", "display", "ultrawide", "4k monitor"],
    "Gaming Consoles": [
      "playstation", "ps5", "ps4", "xbox", "nintendo switch", "console",
    ],
    Printers: [
      "printer", "ink tank", "laser printer", "deskjet", "ecotank",
      "pixma", "imageclass", "smart tank",
    ],
  },
};

const KNOWN_BRANDS = [
  "Samsung", "Apple", "OnePlus", "Xiaomi", "Redmi", "Realme", "Oppo", "Vivo",
  "Google", "Motorola", "Nothing", "Asus", "Acer", "Dell", "HP", "Lenovo",
  "MSI", "Sony", "LG", "Bose", "JBL", "Sennheiser", "Boat", "Noise",
  "Canon", "Nikon", "GoPro", "Honor", "Huawei", "TCL", "Hisense", "Panasonic",
  "Philips", "Whirlpool", "Bosch", "IFB", "Godrej", "Haier", "Voltas", "Daikin",
  "Nike", "Adidas", "Puma", "Reebok", "Titan", "Casio", "Fossil", "Fastrack",
  "Lakme", "Maybelline", "Nivea", "Dove", "Dyson", "Logitech", "Razer",
  "Corsair", "Seagate", "WD", "SanDisk", "Kingston", "Sigma", "Viltrox",
  "iQOO", "Brother", "Epson",
];

function detectBrand(title = "", brandHint = "") {
  if (brandHint && brandHint !== "Unknown") {
    const match = KNOWN_BRANDS.find(
      (b) => b.toLowerCase() === String(brandHint).toLowerCase()
    );
    return match || brandHint;
  }
  const text = title.toLowerCase();
  for (const brand of KNOWN_BRANDS) {
    const pattern = new RegExp(
      `\\b${brand.toLowerCase().replace(/[.*+?^${}()|[\]\\]/g, "\\$&")}\\b`,
      "i"
    );
    if (pattern.test(text)) return brand;
  }
  return "Unknown";
}

function classifyProduct(title = "", brandHint = "", categoryHint = "") {
  const text = `${categoryHint} ${title}`.toLowerCase();
  let best = {
    main_category: "Other",
    sub_category:  "Other",
    product_type:  "General",
    confidence:    30,
  };
  let maxScore = 0;
  for (const [mainCat, subCats] of Object.entries(CATEGORY_TREE)) {
    for (const [subCat, keywords] of Object.entries(subCats)) {
      for (const kw of keywords) {
        if (text.includes(kw)) {
          const score = kw.length;
          if (score > maxScore) {
            maxScore = score;
            best = {
              main_category: mainCat,
              sub_category:  subCat,
              product_type:  subCat,
              confidence:    Math.min(99, 70 + score * 2),
            };
          }
        }
      }
    }
  }
  const brand = detectBrand(title, brandHint);
  return {
    ...best,
    brand,
    confidence:
      brand !== "Unknown"
        ? Math.min(99, best.confidence + 5)
        : best.confidence,
  };
}

function generateSearchTags(title = "", c = {}) {
  const tags = new Set();
  const cleanTitle = stripOosSuffix(title);
  const text = cleanTitle.toLowerCase();
  if (c.brand && c.brand !== "Unknown") tags.add(c.brand);
  if (c.main_category) tags.add(c.main_category);
  if (c.sub_category)  tags.add(c.sub_category);
  if (c.product_type)  tags.add(c.product_type);
  cleanTitle
    .split(/[\s,()\-/:]+/)
    .filter((w) => w.length >= 2 && !/^\d+$/.test(w))
    .forEach((w) => tags.add(w));
  const ram = text.match(/(\d{1,3})\s*gb\s*ram/i);
  if (ram) tags.add(`${ram[1]}GB RAM`);
  const storage = text.match(/(\d{2,4})\s*(gb|tb)\s*(storage|rom|ssd|hdd)?/i);
  if (storage) tags.add(`${storage[1]}${storage[2].toUpperCase()} Storage`);
  ["5g", "4g", "pro", "ultra", "max", "mini", "plus", "4k", "8k", "wifi", "duplex"]
    .forEach((k) => { if (text.includes(k)) tags.add(k.toUpperCase()); });
  if (c.brand && c.sub_category) tags.add(`${c.brand} ${c.sub_category}`);
  return [...tags].filter(Boolean).slice(0, 25);
}

function generateSeoKeywords(title = "", c = {}) {
  const k = new Set();
  const cleanTitle = stripOosSuffix(title);
  const lower = cleanTitle.toLowerCase();
  const brand = (c.brand || "").toLowerCase();
  const sub   = (c.sub_category || "").toLowerCase();
  const subS  = sub.replace(/s$/, "");
  k.add(lower);
  if (brand && sub) {
    [
      `best ${brand} ${subS}`,
      `${brand} ${subS} price`,
      `buy ${brand} ${subS}`,
      `latest ${brand} ${subS}`,
      `${brand} ${subS} review`,
      `${brand} ${subS} online`,
    ].forEach((x) => k.add(x));
  }
  if (sub) {
    [`best ${sub} 2026`, `top ${sub}`, `premium ${subS}`].forEach((x) => k.add(x));
  }
  [
    `buy ${lower}`, `${lower} price`, `${lower} review`, `${lower} offers`,
    `${lower} lowest price`, `${lower} amazon`, `${lower} flipkart`,
    `${lower} specifications`, `${lower} features`,
  ].forEach((x) => k.add(x));
  return [...k].filter(Boolean).slice(0, 25);
}

function normalizeSpecs(title = "") {
  return stripOosSuffix(title)
    .toLowerCase()
    .replace(/(\d+)\s*(gb|tb|mb)/gi, "$1$2")
    .replace(/samsung electronics/gi, "samsung")
    .replace(/apple inc\.?/gi, "apple")
    .replace(/[^\w\s]/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}

function generateNormalizedId(title = "", brand = "") {
  const tokens = normalizeSpecs(title).split(" ").sort().join("-");
  const hash = crypto
    .createHash("md5")
    .update(`${String(brand).toLowerCase()}-${tokens}`)
    .digest("hex")
    .substring(0, 16);
  return `npid_${hash}`;
}

function computeAnalytics(offers = [], mainPrice = null) {
  const inStock = offers.filter((o) => o.is_available);
  const prices  = inStock
    .map((o) => parsePriceNumber(o.price))
    .filter((p) => p && p > 0);
  const main = parsePriceNumber(mainPrice);
  if (main && main > 0 && !prices.includes(main)) prices.push(main);
  if (!prices.length) {
    return {
      lowest_price:   0,
      highest_price:  0,
      average_price:  0,
      offer_count:    offers.length,
      in_stock_count: inStock.length,
    };
  }
  return {
    lowest_price:   Math.min(...prices),
    highest_price:  Math.max(...prices),
    average_price:  Math.round(prices.reduce((s, n) => s + n, 0) / prices.length),
    offer_count:    offers.length,
    in_stock_count: inStock.length,
  };
}

/* =========================
   JSON-LD EXTRACTION
========================= */
function extractJsonLdFromHtml(html = "") {
  const blocks = [];
  const regex =
    /<script[^>]+type=["']application\/ld\+json["'][^>]*>([\s\S]*?)<\/script>/gi;
  let match;
  while ((match = regex.exec(html)) !== null) {
    try {
      const raw  = match[1].trim();
      const data = JSON.parse(raw);
      if (Array.isArray(data)) {
        blocks.push(...data);
      } else if (data && typeof data === "object") {
        if (Array.isArray(data["@graph"])) blocks.push(...data["@graph"]);
        else blocks.push(data);
      }
    } catch (_) {
      // Malformed JSON-LD — skip
    }
  }
  return blocks;
}

/* =========================
   BUYHATKE SEARCH
   ─ Primary:  href regex on the search HTML
   ─ Fallback: JSON-LD ItemList
   ─ Fallback: direct URL construction from Svelte __data__ JSON embedded
     in the search page (BuyHatke often embeds product slugs in a JS object)
========================= */
function isBuyhatkeProductUrl(url = "") {
  try {
    const u = new URL(url);
    if (!u.hostname.includes("buyhatke.com")) return false;
    // matches both /iphone-16-price-in-india-12345 and /iphone-16-price-in-india-12345-67890
    return /-price-in-india-\d+/i.test(u.pathname);
  } catch {
    return false;
  }
}

function extractProductUrlsFromBuyhatkeSearchHtml(
  html = "",
  baseUrl = "https://buyhatke.com"
) {
  const urls = new Set();

  // Strategy 1: standard href attributes
  const hrefRx = /href=["']([^"']+)["']/gi;
  let match;
  while ((match = hrefRx.exec(html)) !== null) {
    const href = match[1];
    try {
      const full = href.startsWith("http")
        ? href
        : new URL(href, baseUrl).toString();
      if (isBuyhatkeProductUrl(full)) urls.add(normalizeUrl(full));
    } catch (_) {}
  }

  // Strategy 2: unquoted or JS-string hrefs like: url:"/samsung-galaxy..."
  // BuyHatke's Svelte bundles sometimes emit these in JS chunks
  const jsHrefRx = /['"\/]([\w-]+-price-in-india-\d+(?:-\d+)?)['"\/?\s]/gi;
  while ((match = jsHrefRx.exec(html)) !== null) {
    const path = match[1];
    if (!path.includes("http")) {
      try {
        const full = new URL("/" + path, baseUrl).toString();
        if (isBuyhatkeProductUrl(full)) urls.add(normalizeUrl(full));
      } catch (_) {}
    }
  }

  // Strategy 3: scan for bare slugs in JSON-like blobs (e.g. "slug":"iphone-16-price-in-india-12345")
  const slugRx = /"(?:slug|url|path|href)"\s*:\s*"(\/[^"]*price-in-india-\d+[^"]*)"/gi;
  while ((match = slugRx.exec(html)) !== null) {
    try {
      const full = match[1].startsWith("http")
        ? match[1]
        : new URL(match[1], baseUrl).toString();
      if (isBuyhatkeProductUrl(full)) urls.add(normalizeUrl(full));
    } catch (_) {}
  }

  return [...urls];
}

function normalizeForMatch(text = "") {
  return String(text)
    .toLowerCase()
    .replace(/[^\w\s]/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}

function termLooksRelevant(term = "", url = "") {
  const t = normalizeForMatch(term);
  const u = normalizeForMatch(url);
  if (!t || !u) return true;
  const mustTokens = t.split(" ").filter((x) => x.length >= 2);
  const forbiddenHints = [
    "case", "cover", "magsafe", "charger", "cable",
    "screen guard", "tempered", "back cover", "evocrystal",
  ];
  if (forbiddenHints.some((x) => u.includes(x))) return false;
  let hits = 0;
  for (const token of mustTokens) {
    if (u.includes(token)) hits++;
  }
  return hits >= Math.max(2, Math.floor(mustTokens.length / 2));
}

/* ── NEW: extract product list from embedded Svelte __data__ on the search page ── */
function extractUrlsFromSvelteSearchPayload(html = "", baseUrl = "https://buyhatke.com") {
  const urls = new Set();
  try {
    // BuyHatke search pages embed data in kit.start() just like product pages
    const root = extractPageDataObject(html);
    if (!root || !Array.isArray(root.data)) return [];

    for (const entry of root.data) {
      const payload = entry?.data;
      if (!payload || typeof payload !== "object") continue;

      // Search result payload usually has a "products" or "searchResults" array
      const candidates = [
        ...(payload.products         || []),
        ...(payload.searchResults    || []),
        ...(payload.productList      || []),
        ...(payload.items            || []),
        ...(payload.data             || []),
      ];

      for (const item of candidates) {
        // Each item may expose a direct BuyHatke URL or a slug
        const rawUrl =
          item?.bhUrl ||
          item?.buyhatke_url ||
          item?.url ||
          item?.link ||
          item?.slug ||
          null;

        if (!rawUrl) continue;

        const full = rawUrl.startsWith("http")
          ? rawUrl
          : new URL(String(rawUrl).startsWith("/") ? rawUrl : "/" + rawUrl, baseUrl).toString();

        if (isBuyhatkeProductUrl(full)) urls.add(normalizeUrl(full));
      }
    }
  } catch (err) {
    workerLogger.warn("extractUrlsFromSvelteSearchPayload() failed", { error: err.message });
  }
  return [...urls];
}

async function seedFromSearch(term) {
  const collected = new Set();
  const searchUrl = `https://buyhatke.com/search?product=${encodeURIComponent(term)}`;

  workerLogger.info("seedFromSearch() started", { term, searchUrl });

  let html = "";
  try {
    const response = await retry(() =>
      httpClient.get(searchUrl, { responseType: "text" })
    );
    html = response.data || "";
    workerLogger.info("Search page downloaded", {
      term,
      bytes: html.length,
      statusCode: response.status,
    });
  } catch (err) {
    workerLogger.error("seedFromSearch() HTTP failed", { term, error: err.message });
    return [];
  }

  // --- Strategy 1: href / JS / JSON slug scanning ---
  let urls = extractProductUrlsFromBuyhatkeSearchHtml(html);
  workerLogger.info("URLs from HTML scan", { term, count: urls.length });

  if (CONFIG.enableTermFiltering) {
    const before = urls.length;
    urls = urls.filter((u) => termLooksRelevant(term, u));
    workerLogger.info("URLs after term filter", { term, before, after: urls.length });
  }
  urls.slice(0, CONFIG.searchResultsPerTerm).forEach((u) => collected.add(u));

  // --- Strategy 2: JSON-LD ItemList ---
  const blocks = extractJsonLdFromHtml(html);
  workerLogger.info("JSON-LD blocks on search page", { term, count: blocks.length });
  for (const block of blocks) {
    if (block["@type"] === "ItemList" && Array.isArray(block.itemListElement)) {
      for (const item of block.itemListElement) {
        const itemUrl = item?.url || item?.item?.url || null;
        if (
          itemUrl &&
          isBuyhatkeProductUrl(itemUrl) &&
          (!CONFIG.enableTermFiltering || termLooksRelevant(term, itemUrl))
        ) {
          collected.add(normalizeUrl(itemUrl));
        }
      }
    }
  }
  workerLogger.info("After JSON-LD strategy", { term, collected: collected.size });

  // --- Strategy 3: Svelte embedded search payload ---
  const svelteUrls = extractUrlsFromSvelteSearchPayload(html);
  workerLogger.info("URLs from Svelte search payload", { term, count: svelteUrls.length });
  for (const u of svelteUrls) {
    if (!CONFIG.enableTermFiltering || termLooksRelevant(term, u)) {
      collected.add(u);
    }
  }

  // --- Strategy 4: If still empty, dump first 2000 chars of HTML to diagnose ---
  if (collected.size === 0) {
    workerLogger.warn("seedFromSearch() found ZERO URLs — dumping HTML sample for diagnosis", {
      term,
      htmlSample: html.substring(0, 3000),
    });
  }

  const finalUrls = [...collected];
  workerLogger.info("seedFromSearch() complete", { term, discovered: finalUrls.length });
  return finalUrls;
}

/* =========================
   SVELTE DATA PARSER
========================= */
function extractBalancedObject(text = "", startIndex = -1) {
  if (startIndex < 0 || startIndex >= text.length || text[startIndex] !== "{") {
    return null;
  }
  let depth = 0;
  let inString = false;
  let stringQuote = "";
  let escaped = false;
  for (let i = startIndex; i < text.length; i++) {
    const ch = text[i];
    if (inString) {
      if (escaped)             { escaped = false; continue; }
      if (ch === "\\")         { escaped = true;  continue; }
      if (ch === stringQuote)  { inString = false; stringQuote = ""; }
      continue;
    }
    if (ch === '"' || ch === "'" || ch === "`") {
      inString    = true;
      stringQuote = ch;
      continue;
    }
    if      (ch === "{") depth++;
    else if (ch === "}") {
      depth--;
      if (depth === 0) return text.slice(startIndex, i + 1);
    }
  }
  return null;
}

function extractPageDataObject(html = "") {
  // BuyHatke uses SvelteKit — the bootstrap call changed over time.
  // Try multiple known markers.
  const markers = [
    "kit.start(app, element,",
    "__sveltekit_",
    "sveltekit:data",
  ];

  for (const marker of markers) {
    const idx = html.indexOf(marker);
    if (idx === -1) continue;

    const after      = html.slice(idx + marker.length);
    const firstBrace = after.indexOf("{");
    if (firstBrace === -1) continue;

    const objText = extractBalancedObject(after, firstBrace);
    if (!objText) continue;

    try {
      // eslint-disable-next-line no-new-func
      const result = Function('"use strict"; return (' + objText + ");")();
      if (result && typeof result === "object") return result;
    } catch (_) {
      // try next marker
    }
  }

  // Last resort: look for __data__ pattern used in newer SvelteKit versions
  // e.g.  <script>window.__sveltekit_data = {...}</script>
  const dataAssignRx = /window\.__sveltekit_\w+\s*=\s*(\{[\s\S]*?\});\s*<\/script>/i;
  const dataMatch    = dataAssignRx.exec(html);
  if (dataMatch) {
    try {
      return JSON.parse(dataMatch[1]);
    } catch (_) {}
  }

  return null;
}

function findSvelteProductPayload(html = "") {
  const root = extractPageDataObject(html);
  if (!root) return null;

  // Standard SvelteKit shape: root.data is an array of route-segment data
  if (Array.isArray(root.data)) {
    for (const entry of root.data) {
      const payload = entry?.data;
      if (payload && typeof payload === "object" && (payload.productData || payload.dealsData)) {
        return payload;
      }
    }
  }

  // Some builds nest it differently
  if (root.data && typeof root.data === "object") {
    if (root.data.productData || root.data.dealsData) return root.data;
  }

  return null;
}

function parseOfferFromDealsListItem(item = {}, fallbackTitle = "") {
  const priceNum = parsePriceNumber(item.price);
  if (!priceNum || priceNum <= 0) return null;
  const rawUrl        = item.link || null;
  const cleanUrl      = rawUrl ? cleanStoreUrl(rawUrl) : null;
  const detectedStore = detectStore(cleanUrl || "", item.site_name || "");
  const isAvailable   =
    item.inStock === 1 ||
    item.inStock === true ||
    item.inStock === null ||
    item.inStock === undefined;
  const title = item.prod || item.prod1 || fallbackTitle || "";
  return {
    store:              detectedStore?.name || item.site_name || "Unknown",
    store_key:          detectedStore?.key  || "unknown",
    price:              `₹${priceNum.toLocaleString("en-IN")}`,
    url:                cleanUrl,
    stock:              isAvailable ? "IN_STOCK" : "OUT_OF_STOCK",
    condition:          item.refurbished_flag ? "REFURBISHED" : "NEW",
    is_available:       isAvailable,
    availability_label: isAvailable ? "In Stock" : "Out of Stock",
    affiliate_url:      cleanUrl ? generateAffiliateUrl(cleanUrl, title) : null,
    trusted:            item.trustedFlag !== false,
  };
}

function dedupeOffers(offers = []) {
  const seen = new Set();
  const out  = [];
  for (const o of offers) {
    const key = [o.store_key || "", o.url || "", o.price || "", o.stock || ""].join("|");
    if (seen.has(key)) continue;
    seen.add(key);
    out.push(o);
  }
  return out;
}

function parseBuyhatkeSveltePayload(html = "", sourceUrl = "") {
  const payload = findSvelteProductPayload(html);
  if (!payload) return null;

  const productData = payload.productData || {};
  const dealsList   = payload.dealsData?.dealsList || [];

  const rawTitle = String(productData.name || "").trim();
  if (!rawTitle) return null;

  const baseTitle = stripOosSuffix(rawTitle);

  let image = productData.image || null;
  if (
    Array.isArray(productData.thumbnailImages) &&
    productData.thumbnailImages.length
  ) {
    image = image || productData.thumbnailImages[0];
  }

  const rating  = productData.rating     ?? null;
  const reviews = productData.ratingCount ?? null;

  const parsedOffers = dedupeOffers(
    dealsList
      .map((item) => parseOfferFromDealsListItem(item, baseTitle))
      .filter(Boolean)
  );

  const bestInStock = parsedOffers
    .filter((o) => o.is_available)
    .sort(
      (a, b) =>
        (parsePriceNumber(a.price) || Infinity) -
        (parsePriceNumber(b.price) || Infinity)
    );

  const allSorted = [...parsedOffers].sort(
    (a, b) =>
      (parsePriceNumber(a.price) || Infinity) -
      (parsePriceNumber(b.price) || Infinity)
  );

  const bestOffer = bestInStock[0] || allSorted[0] || null;

  const mainUrl      = cleanStoreUrl(productData.link || bestOffer?.url || sourceUrl);
  const mainPriceNum =
    parsePriceNumber(productData.cur_price) || parsePriceNumber(bestOffer?.price);

  const productAvailable =
    Number(productData.inStock) === 1 ||
    productData.inStock === true ||
    parsedOffers.some((o) => o.is_available);

  const brandHint      = productData.brand || detectBrand(baseTitle);
  const classification = classifyProduct(baseTitle, brandHint, productData.category || "BuyHatke");
  const finalTitle     = applyAvailabilityToTitle(baseTitle, productAvailable);
  const search_tags    = generateSearchTags(baseTitle, classification);
  const seo_keywords   = generateSeoKeywords(baseTitle, classification);
  const affiliate_url  = generateAffiliateUrl(mainUrl, baseTitle);
  const normalized_product_id = generateNormalizedId(baseTitle, classification.brand);
  const analytics      = computeAnalytics(
    parsedOffers,
    mainPriceNum ? `₹${mainPriceNum.toLocaleString("en-IN")}` : null
  );

  return {
    source_platform:       "BuyHatke",
    buyhatke_product_id:   normalized_product_id,
    url:                   mainUrl,
    buyhatke_url:          normalizeUrl(sourceUrl),
    title:                 finalTitle,
    base_title:            baseTitle,
    price:                 mainPriceNum ? `₹${mainPriceNum.toLocaleString("en-IN")}` : null,
    image:                 image || null,
    rating:                rating  !== null ? String(rating)  : null,
    reviews:               reviews !== null ? String(reviews) : null,
    description:           null,
    offers:                parsedOffers,
    primary_store:
      bestOffer?.store || productData.site_name || payload.siteName || "Unknown",
    classification,
    search_tags,
    seo_keywords,
    affiliate_url,
    normalized_product_id,
    analytics,
    is_available:          productAvailable,
    availability_label:    productAvailable ? "In Stock" : "Out of Stock",
  };
}

/* =========================
   JSON-LD OFFER PARSER
========================= */
function parseBuyhatkeOffer(offerObj = {}, productTitle = "") {
  if (!offerObj || typeof offerObj !== "object") return null;
  const priceRaw = offerObj.price ?? offerObj.lowPrice ?? null;
  if (priceRaw === null || priceRaw === undefined) return null;
  const priceNum = parsePriceNumber(priceRaw);
  if (!priceNum || priceNum <= 0) return null;
  const sellerName =
    offerObj?.seller?.name ||
    offerObj?.offeredBy?.name ||
    offerObj?.name ||
    "Unknown";
  const availabilityRaw = String(offerObj.availability || "");
  const isAvailable =
    /instock/i.test(availabilityRaw) || /in_stock/i.test(availabilityRaw);
  const rawUrl        = offerObj.url || offerObj.offerURL || null;
  const cleanUrl      = rawUrl ? cleanStoreUrl(rawUrl) : null;
  const detectedStore = detectStore(cleanUrl || "", sellerName);
  return {
    store:              detectedStore?.name || sellerName || "Unknown",
    store_key:          detectedStore?.key  || "unknown",
    price:              `₹${priceNum.toLocaleString("en-IN")}`,
    url:                cleanUrl,
    stock:              isAvailable ? "IN_STOCK" : "OUT_OF_STOCK",
    condition:          "NEW",
    is_available:       isAvailable,
    availability_label: isAvailable ? "In Stock" : "Out of Stock",
    affiliate_url:      cleanUrl ? generateAffiliateUrl(cleanUrl, productTitle) : null,
  };
}

/* =========================
   SCRAPER
========================= */
function parseFromJsonLd(html = "", sourceUrl = "") {
  const blocks = extractJsonLdFromHtml(html);
  for (const block of blocks) {
    const schemaType = block?.["@type"];
    const isProduct  =
      schemaType === "Product" ||
      (Array.isArray(schemaType) && schemaType.includes("Product")) ||
      String(schemaType || "").includes("Product");
    if (!isProduct) continue;

    const title = String(block.name || "").trim();
    if (!title) continue;

    let image = block.image || null;
    if (Array.isArray(image))      image = image[0] || null;
    if (image && typeof image === "object") image = image.url || null;

    const brand = typeof block.brand === "object"
      ? (block.brand?.name || "")
      : (block.brand || "");
    const description = String(block.description || "").trim();

    let rawOffers = block.offers || [];
    if (rawOffers && !Array.isArray(rawOffers) && typeof rawOffers === "object") {
      const inner = rawOffers.offers || [];
      if      (Array.isArray(inner))      rawOffers = inner;
      else if (typeof inner === "object") rawOffers = [inner];
      else                                rawOffers = [rawOffers];
    }

    const baseTitle    = stripOosSuffix(title);
    const parsedOffers = rawOffers
      .map((o) => parseBuyhatkeOffer(o, baseTitle))
      .filter(Boolean);

    if (!parsedOffers.length) {
      return { skipped: "no_offers", buyhatke_url: sourceUrl, title: baseTitle };
    }

    const inStock   = parsedOffers.filter((o) => o.is_available && parsePriceNumber(o.price) > 0);
    const anyPrice  = parsedOffers.filter((o) => parsePriceNumber(o.price) > 0);
    const bestGroup = inStock.length ? inStock : anyPrice;

    let bestPrice       = null;
    let primaryStore    = "Unknown";
    let finalProductUrl = sourceUrl;

    if (bestGroup.length) {
      const sorted = [...bestGroup].sort(
        (a, b) =>
          (parsePriceNumber(a.price) || Infinity) -
          (parsePriceNumber(b.price) || Infinity)
      );
      bestPrice    = parsePriceNumber(sorted[0].price);
      primaryStore = sorted[0].store || "Unknown";
      const inStockWithUrl = inStock.filter((o) => o.url);
      if (inStockWithUrl.length) {
        const sortedWithUrl = [...inStockWithUrl].sort(
          (a, b) =>
            (parsePriceNumber(a.price) || Infinity) -
            (parsePriceNumber(b.price) || Infinity)
        );
        finalProductUrl = sortedWithUrl[0].url;
      } else {
        const anyWithUrl = anyPrice.find((o) => o.url);
        if (anyWithUrl?.url) finalProductUrl = anyWithUrl.url;
      }
    }

    const productAvailable      = parsedOffers.some((o) => o.is_available);
    const finalTitle            = applyAvailabilityToTitle(baseTitle, productAvailable);
    const classification        = classifyProduct(baseTitle, brand, "BuyHatke");
    const search_tags           = generateSearchTags(baseTitle, classification);
    const seo_keywords          = generateSeoKeywords(baseTitle, classification);
    const affiliate_url         = generateAffiliateUrl(finalProductUrl, baseTitle);
    const normalized_product_id = generateNormalizedId(baseTitle, classification.brand);
    const analytics             = computeAnalytics(
      parsedOffers,
      bestPrice ? `₹${bestPrice.toLocaleString("en-IN")}` : null
    );

    return {
      source_platform:       "BuyHatke",
      buyhatke_product_id:   normalized_product_id,
      url:                   finalProductUrl,
      buyhatke_url:          sourceUrl,
      title:                 finalTitle,
      base_title:            baseTitle,
      price:                 bestPrice ? `₹${bestPrice.toLocaleString("en-IN")}` : null,
      image:                 image || null,
      rating:                null,
      reviews:               null,
      description,
      offers:                parsedOffers,
      primary_store:         primaryStore,
      classification,
      search_tags,
      seo_keywords,
      affiliate_url,
      normalized_product_id,
      analytics,
      is_available:          productAvailable,
      availability_label:    productAvailable ? "In Stock" : "Out of Stock",
    };
  }
  return null;
}

async function scrapeBuyhatkeProduct(buyhatkeUrl) {
  const sourceUrl = normalizeUrl(buyhatkeUrl);
  workerLogger.info("Scraping product page", { url: sourceUrl });

  let html;
  try {
    const response = await retry(() =>
      httpClient.get(sourceUrl, { responseType: "text" })
    );
    html = response.data || "";
    workerLogger.debug("Product page downloaded", {
      url: sourceUrl,
      bytes: html.length,
      status: response.status,
    });
  } catch (err) {
    const msg = `HTTP fetch failed: ${err.message}`;
    workerLogger.error("HTTP fetch failed", { url: sourceUrl, error: err.message });
    return { __error: msg, buyhatke_url: sourceUrl };
  }

  // Attempt 1: JSON-LD
  let result = null;
  try {
    result = parseFromJsonLd(html, sourceUrl);
  } catch (err) {
    workerLogger.warn("JSON-LD parse error", { url: sourceUrl, error: err.message });
  }

  if (result && !result.skipped) {
    workerLogger.info("Product parsed via JSON-LD", { url: sourceUrl, title: result.title });
    return result;
  }

  if (result?.skipped) {
    workerLogger.info("JSON-LD skipped", { url: sourceUrl, reason: result.skipped });
  }

  // Attempt 2: Svelte embedded data
  let svelteResult = null;
  try {
    svelteResult = parseBuyhatkeSveltePayload(html, sourceUrl);
  } catch (err) {
    workerLogger.warn("Svelte parse error", { url: sourceUrl, error: err.message });
  }

  if (svelteResult) {
    if (!svelteResult.offers?.length) {
      workerLogger.info("Svelte parsed but no offers found", { url: sourceUrl });
      return {
        skipped:      "no_offers",
        buyhatke_url: sourceUrl,
        title:        svelteResult.base_title || svelteResult.title || "",
      };
    }
    workerLogger.info("Product parsed via Svelte payload", {
      url: sourceUrl,
      title: svelteResult.title,
      offerCount: svelteResult.offers.length,
    });
    return svelteResult;
  }

  // Dump a sample so you can see what the page actually looks like in prod
  workerLogger.warn("No product data found — dumping HTML sample", {
    url: sourceUrl,
    htmlSample: html.substring(0, 2000),
  });

  return { skipped: "no_embedded_product_data", buyhatke_url: sourceUrl };
}

/* =========================
   SUPABASE PERSISTENCE
========================= */
async function insertPriceHistory(supabase, productUrl, offers = [], sourcePlatform = "BuyHatke") {
  if (!supabase || !offers.length) return;
  const now  = new Date().toISOString();
  const rows = offers
    .filter((o) => o.price)
    .map((o) => ({
      product_url:     productUrl,
      source_platform: sourcePlatform,
      price:           parsePriceNumber(o.price) || 0,
      store:           o.store,
      store_key:       o.store_key,
      is_available:    !!o.is_available,
      stock:           o.stock || "UNKNOWN",
      captured_at:     now,
    }));
  if (!rows.length) return;
  for (let i = 0; i < rows.length; i += CONFIG.supabaseBatchSize) {
    const batch     = rows.slice(i, i + CONFIG.supabaseBatchSize);
    const { error } = await supabase.from("price_history").insert(batch);
    if (error) throw error;
  }
}

async function saveRawBuyhatkeUrl(supabase, p) {
  if (!supabase) return;
  const row = {
    buyhatke_url:          p.buyhatke_url,
    buyhatke_product_id:   p.buyhatke_product_id,
    product_url:           p.url,
    normalized_product_id: p.normalized_product_id,
    title:                 p.base_title,
    brand:                 p.classification.brand,
    main_category:         p.classification.main_category,
    sub_category:          p.classification.sub_category,
    last_scraped_at:       new Date().toISOString(),
  };
  const { error } = await supabase
    .from("raw_url_buyhatke")
    .upsert([row], { onConflict: "buyhatke_url" });
  if (error) throw error;
}

async function saveProductToSupabase(supabase, p, jobId = null) {
  if (!supabase) throw new Error("Supabase client is required");
  const row = {
    job_id:                jobId,
    url:                   p.url,
    buyhatke_url:          p.buyhatke_url,
    source_platform:       "BuyHatke",
    title:                 p.title,
    base_title:            p.base_title,
    price:                 p.price,
    image:                 p.image      || null,
    rating:                p.rating     ? String(p.rating)  : null,
    reviews:               p.reviews    ? String(p.reviews) : null,
    description:           p.description || null,
    offers:                JSON.parse(JSON.stringify(p.offers || [])),
    main_category:         p.classification.main_category,
    sub_category:          p.classification.sub_category,
    product_type:          p.classification.product_type,
    brand:                 p.classification.brand,
    search_tags:           p.search_tags,
    seo_keywords:          p.seo_keywords,
    affiliate_url:         p.affiliate_url,
    normalized_product_id: p.normalized_product_id,
    match_score:           100,
    lowest_price:          p.analytics.lowest_price,
    highest_price:         p.analytics.highest_price,
    average_price:         p.analytics.average_price,
    is_available:          p.is_available,
    availability_label:    p.availability_label,
    in_stock_count:        p.analytics.in_stock_count,
    offer_count:           p.analytics.offer_count,
    last_seen_at:          new Date().toISOString(),
  };
  const { error } = await supabase
    .from("products")
    .upsert([row], { onConflict: "url" });
  return { error };
}

/* =========================
   BACKGROUND PERSISTENCE
========================= */
function persistProductInBackground({ supabase, jobId, product, helpers, counters }) {
  return new Promise((resolve) => {
    setImmediate(async () => {
      const results = await Promise.allSettled([
        saveProductToSupabase(supabase, product, jobId),
        saveRawBuyhatkeUrl(supabase, product),
        insertPriceHistory(supabase, product.url, product.offers, "BuyHatke"),
        helpers.cacheProduct(product),
      ]);

      const [saveRes, rawRes, priceRes] = results;

      if (saveRes.status === "fulfilled" && !saveRes.value?.error) {
        counters.saved++;
        helpers.appendJobLog("info", `Saved: ${product.title}`);
      } else {
        const err =
          saveRes.status === "rejected"
            ? saveRes.reason
            : saveRes.value?.error;
        debugSupabaseError("saveProductToSupabase() [background]", err);
        helpers.appendJobLog(
          "error",
          `Failed to persist ${product.url}: ${err?.message || String(err)}`
        );
      }
      if (rawRes.status === "rejected") {
        debugSupabaseError("saveRawBuyhatkeUrl() [background]", rawRes.reason);
      }
      if (priceRes.status === "rejected") {
        debugSupabaseError("insertPriceHistory() [background]", priceRes.reason);
      }
      resolve();
    });
  });
}

/* =========================
   MAIN JOB PROCESSOR
========================= */
async function processBuyhatkeJob({ job, supabase, logger: jobLogger, config: jobConfig, helpers }) {
  const startedAt = Date.now();
  if (!job || !job.data) throw new Error("Invalid BullMQ job payload");

  const jobId = job.data.jobId || job.id;
  const query = String(job.data.query || "").trim();
  if (!query) throw new Error("Missing query in job data");

  const updateJobProgress = helpers?.updateJobProgress || (async () => {});
  const appendJobLog      = helpers?.appendJobLog      || (() => {});
  const emitProduct       = helpers?.pushProductLive   || helpers?.emitProduct || (() => {});
  const cacheProduct      = helpers?.cacheProduct      || (async () => {});

  const log = jobLogger || workerLogger;
  log.info("processBuyhatkeJob started", { jobId, query });

  await updateJobProgress({
    status:           "running",
    progress:         5,
    current_product:  query,
    products_scraped: 0,
    products_saved:   0,
    offers_found:     0,
    errors:           0,
  });

  appendJobLog("info", `Seeding BuyHatke URLs for query: ${query}`);

  const urls      = await seedFromSearch(query);
  const toProcess = [...new Set(urls)].slice(0, CONFIG.maxProducts);

  log.info("URLs ready for processing", { jobId, count: toProcess.length, urls: toProcess });

  if (!toProcess.length) {
    appendJobLog("warn", `No product URLs discovered for query: ${query}. Check worker logs for HTML sample.`);
    await updateJobProgress({
      status:                   "completed",
      progress:                 100,
      products_scraped:         0,
      products_saved:           0,
      offers_found:             0,
      errors:                   0,
      estimated_remaining_time: 0,
    });
    return {
      jobId,
      status:         "completed",
      query,
      discoveredUrls: 0,
      processedUrls:  0,
      productsSaved:  0,
      offersFound:    0,
      errors:         0,
      durationMs:     Date.now() - startedAt,
    };
  }

  const counters        = { saved: 0 };
  let scraped           = 0;
  let errors            = 0;
  let offersFound       = 0;
  const backgroundTasks = [];

  for (let i = 0; i < toProcess.length; i++) {
    const url      = toProcess[i];
    const progress = Math.min(99, Math.round(((i + 1) / toProcess.length) * 90) + 5);

    log.info(`Processing URL ${i + 1}/${toProcess.length}`, { url });

    let p;
    try {
      p = await scrapeBuyhatkeProduct(url);
    } catch (err) {
      errors++;
      log.error("scrapeBuyhatkeProduct() threw", { url, error: err.message });
      appendJobLog("error", `Scrape error for ${url}: ${err.message}`);
      await sleep(CONFIG.requestDelayMs);
      continue;
    }

    scraped++;

    if (!p) {
      errors++;
      appendJobLog("warn", `No product data returned for ${url}`);
      await sleep(CONFIG.requestDelayMs);
      continue;
    }
    if (p.__error) {
      errors++;
      appendJobLog("error", `Fetch error for ${url}: ${p.__error}`);
      await sleep(CONFIG.requestDelayMs);
      continue;
    }
    if (p.skipped) {
      appendJobLog("info", `Skipped ${url}: ${p.skipped}`);
      await sleep(CONFIG.requestDelayMs);
      continue;
    }

    offersFound += Array.isArray(p.offers) ? p.offers.length : 0;

    // Stream to frontend immediately — no await
    emitProduct(p);

    // Background persistence — does not block the loop
    backgroundTasks.push(
      persistProductInBackground({
        supabase,
        jobId,
        product: p,
        helpers: { appendJobLog, cacheProduct },
        counters,
      })
    );

    // Non-blocking progress update
    updateJobProgress({
      status:                   "running",
      progress,
      current_url:              url,
      current_product:          p.title,
      current_store:            p.primary_store,
      products_scraped:         scraped,
      products_saved:           counters.saved,
      offers_found:             offersFound,
      errors,
      estimated_remaining_time: (toProcess.length - i - 1) * CONFIG.requestDelayMs,
    }).catch(() => {});

    await sleep(CONFIG.requestDelayMs);
  }

  await updateJobProgress({
    status:                   "completed",
    progress:                 100,
    current_product:          query,
    current_url:              null,
    products_scraped:         scraped,
    products_saved:           counters.saved,
    offers_found:             offersFound,
    errors,
    estimated_remaining_time: 0,
  });

  appendJobLog(
    "info",
    `Scraping complete. scraped=${scraped} offers=${offersFound} errors=${errors}`
  );

  Promise.allSettled(backgroundTasks).then(() => {
    log.info("Background persistence settled", { jobId, saved: counters.saved, scraped });
    appendJobLog("info", `Background persistence complete. saved=${counters.saved}/${scraped}`);
  });

  const result = {
    jobId,
    status:         "completed",
    query,
    discoveredUrls: toProcess.length,
    processedUrls:  scraped,
    productsSaved:  counters.saved,
    offersFound,
    errors,
    durationMs:     Date.now() - startedAt,
  };

  log.info("processBuyhatkeJob finished", result);
  return result;
}

/* =========================
   EXPORTS
========================= */
module.exports = {
  processBuyhatkeJob,
  scrapeBuyhatkeProduct,
  seedFromSearch,
  parseFromJsonLd,
  parseBuyhatkeSveltePayload,
  extractJsonLdFromHtml,
  cleanStoreUrl,
  detectStore,
  classifyProduct,
  generateAffiliateUrl,
  generateNormalizedId,
};
