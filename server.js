import express from 'express';
import cors from 'cors';
import dotenv from 'dotenv';
import mysql from 'mysql2/promise';
import { fileURLToPath } from 'url';
import { dirname, join } from 'path';

dotenv.config();
const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);
const app = express();
const PORT = process.env.PORT || 3001;
const VER = 'v4.0-2026-03-03';
app.use(cors());
app.use(express.json({ limit: '10mb' }));

/* ═══════════════════════════════════════════════════════════
   DEFINITIONS — Single Source of Truth (matching Power BI)
   ─────────────────────────────────────────────────────────
   Primary table:     seller_board_sales  (= PBI's seller_board_sales_append)
   Product table:     seller_board_product (ASIN-level detail)
   FBA Stock source:  seller_board_stock   (PBI: SUM(seller_board_stock[FBAStock]))
   Inventory detail:  fba_iventory_planning (age, DOS, storage)
   Plan:              asin_plan
   Analytics:         analytics_search_catalog_performance (impressions, clicks)
   
   Revenue     = salesOrganic + salesPPC
   Units       = unitsOrganic + unitsPPC
   Ads Cost    = sponsoredProducts + sponsoredDisplay + sponsoredBrands + sponsoredBrandsVideo
   COGS        = seller_board_product.costOfGoods  (not in sales table)
   Real ACOS   = ABS(Ads Cost) / Revenue
   Margin      = Net Profit / Revenue
   CR          = Units / Sessions  (weighted)
   CTR         = Clicks / Impressions  (weighted)
   ═══════════════════════════════════════════════════════════ */

/* ═══════════ DATABASE POOL ═══════════ */
let pool = null;
try {
  pool = mysql.createPool({
    host: process.env.DB_HOST || 'localhost',
    port: process.env.DB_PORT || 3306,
    user: process.env.DB_USER || 'root',
    password: process.env.DB_PASSWORD || '',
    database: process.env.DB_NAME || 'expeditee',
    waitForConnections: true,
    connectionLimit: 10,
    queueLimit: 0,
    connectTimeout: 10000,
    ssl: process.env.DB_SSL === 'true' ? { rejectUnauthorized: false } : undefined,
  });
  console.log('✅ MySQL pool created');
} catch (e) {
  console.warn('⚠️ MySQL pool failed:', e.message);
}

async function q(sql, params = [], timeoutMs = 30000) {
  if (!pool) throw new Error('Database not connected');
  const timeout = new Promise((_, rej) =>
    setTimeout(() => rej(new Error(`Query timeout ${timeoutMs / 1000}s`)), timeoutMs)
  );
  return Promise.race([pool.execute(sql, params).then(([rows]) => rows), timeout]);
}

/* ═══════════ SQL FRAGMENTS ═══════════ */
// seller_board_sales (alias sc)
const SC_SALES = 'COALESCE(sc.salesOrganic,0)+COALESCE(sc.salesPPC,0)';
const SC_UNITS = 'COALESCE(sc.unitsOrganic,0)+COALESCE(sc.unitsPPC,0)';
const SC_ADS = 'COALESCE(sc.sponsoredProducts,0)+COALESCE(sc.sponsoredDisplay,0)+COALESCE(sc.sponsoredBrands,0)+COALESCE(sc.sponsoredBrandsVideo,0)';

// seller_board_product (alias p) — includes googleAds/facebookAds
const P_SALES = 'COALESCE(p.salesOrganic,0)+COALESCE(p.salesPPC,0)';
const P_UNITS = 'COALESCE(p.unitsOrganic,0)+COALESCE(p.unitsPPC,0)';
const P_ADS = 'COALESCE(p.sponsoredProducts,0)+COALESCE(p.sponsoredDisplay,0)+COALESCE(p.sponsoredBrands,0)+COALESCE(p.sponsoredBrandsVideo,0)';
const P_ADS_ALL = P_ADS + '+COALESCE(p.googleAds,0)+COALESCE(p.facebookAds,0)';

/* ═══════════ HELPERS ═══════════ */
async function getShopMap() {
  const rows = await q('SELECT id, shop FROM accounts WHERE deleted_at IS NULL');
  const m = {};
  rows.forEach(r => { m[r.id] = r.shop; });
  return m;
}

async function getShopReverseMap() {
  const rows = await q('SELECT id, shop FROM accounts WHERE deleted_at IS NULL');
  const m = {};
  rows.forEach(r => { m[r.shop] = r.id; });
  return m;
}

async function storeToAccId(storeName) {
  if (!storeName || storeName === 'All') return null;
  const rm = await getShopReverseMap();
  return rm[storeName] || null;
}

function defDates(start, end) {
  return {
    s: start || new Date(Date.now() - 30 * 86400000).toISOString().slice(0, 10),
    e: end || new Date().toISOString().slice(0, 10),
  };
}

// WHERE builders
function scWhere(sd, ed, accId) {
  let w = 'WHERE sc.date BETWEEN ? AND ?';
  const p = [sd, ed];
  if (accId) { w += ' AND sc.accountId = ?'; p.push(accId); }
  return { w, p };
}

function pWhere(sd, ed, accId, seller, brand, af) {
  let w = 'WHERE p.date BETWEEN ? AND ?';
  const p = [sd, ed];
  if (accId) { w += ' AND p.accountId = ?'; p.push(accId); }
  if (seller && seller !== 'All') { w += ' AND a.seller = ?'; p.push(seller); }
  if (brand && brand !== 'All') { w += ' AND a.store = ?'; p.push(brand); }
  if (af && af !== 'All') { w += ' AND p.asin = ?'; p.push(af); }
  return { w, p };
}

function useProduct(seller, brand, af) {
  return (seller && seller !== 'All') || (brand && brand !== 'All') || (af && af !== 'All');
}

/* ═══════════ HEALTH ═══════════ */
app.get('/api/health', async (req, res) => {
  try {
    if (pool) {
      await q('SELECT 1');
      res.json({ status: 'ok', database: 'connected', version: VER });
    } else {
      res.json({ status: 'ok', database: 'not configured', version: VER });
    }
  } catch (e) {
    res.json({ status: 'ok', database: 'error: ' + e.message, version: VER });
  }
});

/* ═══════════ DATE RANGE ═══════════ */
app.get('/api/date-range', async (req, res) => {
  try {
    const rows = await q('SELECT MIN(sc.date) as minDate, MAX(sc.date) as maxDate FROM seller_board_sales sc');
    const r = rows[0] || {};
    const maxDate = r.maxDate ? new Date(r.maxDate).toISOString().slice(0, 10) : null;
    const minDate = r.minDate ? new Date(r.minDate).toISOString().slice(0, 10) : null;
    const today = new Date().toISOString().slice(0, 10);
    let defaultStart = new Date(Date.now() - 29 * 86400000).toISOString().slice(0, 10);
    if (minDate && defaultStart < minDate) defaultStart = minDate;
    res.json({ minDate, maxDate, defaultStart, defaultEnd: today });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

/* ═══════════ FILTERS ═══════════ */
app.get('/api/filters', async (req, res) => {
  try {
    const shops = await q('SELECT id, shop as name FROM accounts WHERE deleted_at IS NULL ORDER BY shop');
    const sellers = await q('SELECT DISTINCT seller FROM asin WHERE seller IS NOT NULL AND LENGTH(seller) > 0 ORDER BY seller');
    const brands = await q('SELECT DISTINCT store FROM asin WHERE store IS NOT NULL AND LENGTH(store) > 0 ORDER BY store');
    const asinShops = await q("SELECT DISTINCT p.asin, p.accountId FROM seller_board_product p WHERE p.date >= DATE_SUB(CURDATE(), INTERVAL 365 DAY)");
    const sm = {};
    shops.forEach(s => { sm[s.id] = s.name; });
    const asm = {};
    asinShops.forEach(r => {
      if (!asm[r.asin]) asm[r.asin] = [];
      const sn = sm[r.accountId];
      if (sn && !asm[r.asin].includes(sn)) asm[r.asin].push(sn);
    });
    const asins = await q("SELECT DISTINCT a.asin, a.seller, a.store FROM asin a WHERE a.asin REGEXP '^(AU-)?B0[A-Za-z0-9]{8}$' ORDER BY a.store, a.asin");
    res.json({
      shops: shops.map(s => ({ id: s.id, name: s.name })),
      sellers: sellers.map(s => s.seller),
      brands: brands.map(b => b.store),
      asins: asins.map(a => ({ asin: a.asin, seller: a.seller, brand: a.store, shops: asm[a.asin] || [] })),
    });
  } catch (e) {
    console.error('FILTER ERROR:', e.message);
    res.status(500).json({ error: e.message });
  }
});

/* ═══════════ EXEC SUMMARY ═══════════
   PBI: seller_board_sales_append for all KPIs except COGS
   COGS from seller_board_product
   ═══════════════════════════════════ */
app.get('/api/exec/summary', async (req, res) => {
  try {
    const { start, end, store, seller, brand, asin: af } = req.query;
    const { s, e } = defDates(start, end);
    const accId = await storeToAccId(store);
    let rows, cogsVal = 0;

    if (useProduct(seller, brand, af)) {
      // ASIN-level filter → use seller_board_product
      const f = pWhere(s, e, accId, seller, brand, af);
      rows = await q(`SELECT
        SUM(${P_SALES}) as sales, SUM(${P_UNITS}) as units, 0 as orders,
        SUM(COALESCE(p.refunds,0)) as refunds, SUM(${P_ADS_ALL}) as advCost,
        0 as shippingCost, 0 as refundCost,
        SUM(COALESCE(p.amazonFees,0)) as amazonFees, SUM(COALESCE(p.costOfGoods,0)) as cogs,
        SUM(COALESCE(p.netProfit,0)) as netProfit, SUM(COALESCE(p.estimatedPayout,0)) as estPayout,
        SUM(COALESCE(p.sessions,0)) as sessions, SUM(COALESCE(p.grossProfit,0)) as grossProfit
        FROM seller_board_product p LEFT JOIN asin a ON p.asin=a.asin ${f.w}`, f.p, 45000);
    } else {
      // No entity filter → use seller_board_sales (fast, matches PBI)
      const f = scWhere(s, e, accId);
      rows = await q(`SELECT
        SUM(${SC_SALES}) as sales, SUM(${SC_UNITS}) as units, SUM(COALESCE(sc.orders,0)) as orders,
        SUM(COALESCE(sc.refunds,0)) as refunds, SUM(${SC_ADS}) as advCost,
        SUM(COALESCE(sc.shipping,0)) as shippingCost, SUM(COALESCE(sc.refundCost,0)) as refundCost,
        SUM(COALESCE(sc.amazonFees,0)) as amazonFees,
        SUM(COALESCE(sc.netProfit,0)) as netProfit, SUM(COALESCE(sc.estimatedPayout,0)) as estPayout,
        SUM(COALESCE(sc.sessions,0)) as sessions, SUM(COALESCE(sc.grossProfit,0)) as grossProfit
        FROM seller_board_sales sc ${f.w}`, f.p, 45000);
      // COGS from seller_board_product (PBI: SUM(seller_board_product[costOfGoods]))
      try {
        let cw = 'WHERE p.date BETWEEN ? AND ?';
        const cp = [s, e];
        if (accId) { cw += ' AND p.accountId = ?'; cp.push(accId); }
        const cr = await q(`SELECT SUM(COALESCE(p.costOfGoods,0)) as cogs FROM seller_board_product p ${cw}`, cp);
        cogsVal = parseFloat(cr[0]?.cogs) || 0;
      } catch (ce) { /* ok */ }
    }
    const r = rows[0] || {};
    const sales = parseFloat(r.sales) || 0, np = parseFloat(r.netProfit) || 0, cogs = parseFloat(r.cogs) || cogsVal;
    res.json({
      sales, units: parseInt(r.units) || 0, orders: parseInt(r.orders) || 0,
      refunds: parseInt(r.refunds) || 0, advCost: parseFloat(r.advCost) || 0,
      shippingCost: parseFloat(r.shippingCost) || 0, refundCost: parseFloat(r.refundCost) || 0,
      amazonFees: parseFloat(r.amazonFees) || 0, cogs, netProfit: np,
      estPayout: parseFloat(r.estPayout) || 0, grossProfit: parseFloat(r.grossProfit) || 0,
      sessions: parseFloat(r.sessions) || 0,
      realAcos: sales > 0 ? (Math.abs(parseFloat(r.advCost) || 0) / sales * 100) : 0,
      pctRefunds: (parseInt(r.orders) || 0) > 0 ? ((parseInt(r.refunds) || 0) / parseInt(r.orders) * 100) : 0,
      margin: sales > 0 ? (np / sales * 100) : 0,
    });
  } catch (e) {
    console.error('exec/summary:', e.message);
    res.status(500).json({ error: e.message });
  }
});

/* ═══════════ EXEC DAILY ═══════════ */
app.get('/api/exec/daily', async (req, res) => {
  try {
    const { start, end, store, seller, brand, asin: af } = req.query;
    const { s, e } = defDates(start, end);
    const accId = await storeToAccId(store);
    let rows;
    if (useProduct(seller, brand, af)) {
      const f = pWhere(s, e, accId, seller, brand, af);
      rows = await q(`SELECT p.date, SUM(${P_SALES}) as revenue, SUM(COALESCE(p.netProfit,0)) as netProfit, SUM(${P_UNITS}) as units
        FROM seller_board_product p LEFT JOIN asin a ON p.asin=a.asin ${f.w} GROUP BY p.date ORDER BY p.date`, f.p, 45000);
    } else {
      const f = scWhere(s, e, accId);
      rows = await q(`SELECT sc.date, SUM(${SC_SALES}) as revenue, SUM(COALESCE(sc.netProfit,0)) as netProfit, SUM(${SC_UNITS}) as units
        FROM seller_board_sales sc ${f.w} GROUP BY sc.date ORDER BY sc.date`, f.p, 45000);
    }
    res.json(rows || []);
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

/* ═══════════ PRODUCT ASINS ═══════════ */
app.get('/api/product/asins', async (req, res) => {
  try {
    const { start, end, store, seller, brand, asin: af } = req.query;
    const { s, e } = defDates(start, end);
    const accId = await storeToAccId(store);
    const f = pWhere(s, e, accId, seller, brand, af);
    const shopMap = await getShopMap();
    const rows = await q(`SELECT p.asin, p.accountId, a.seller, a.store as brand,
      SUM(${P_SALES}) as revenue, SUM(COALESCE(p.netProfit,0)) as netProfit,
      SUM(${P_UNITS}) as units, AVG(COALESCE(p.realACOS,0)) as acos,
      SUM(COALESCE(p.sessions,0)) as sessions, AVG(COALESCE(p.unitSessionPercentage,0)) as cr
      FROM seller_board_product p LEFT JOIN asin a ON p.asin=a.asin
      ${f.w} GROUP BY p.asin, p.accountId, a.seller, a.store ORDER BY revenue DESC LIMIT 500`, f.p, 45000);
    res.json(rows.map(r => {
      const rev = parseFloat(r.revenue) || 0, np = parseFloat(r.netProfit) || 0;
      const acos = Math.round((parseFloat(r.acos) || 0) * 100) / 100;
      const cr = Math.round((parseFloat(r.cr) || 0) * 100) / 100;
      return {
        asin: r.asin, shop: shopMap[r.accountId] || '', seller: r.seller || '', brand: r.brand || '',
        revenue: rev, netProfit: np, units: parseInt(r.units) || 0,
        margin: rev > 0 ? Math.round(np / rev * 1000) / 10 : 0,
        acos, roas: acos > 0 ? Math.round(100 / acos * 100) / 100 : 0, cr,
        sessions: parseFloat(r.sessions) || 0,
      };
    }));
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

/* ═══════════ SHOPS ═══════════ */
app.get('/api/shops', async (req, res) => {
  try {
    const { start, end, store, seller, brand, asin: af } = req.query;
    const shopMap = await getShopMap();
    const { s, e } = defDates(start, end);
    const accId = await storeToAccId(store);
    let rows;
    if (useProduct(seller, brand, af)) {
      const f = pWhere(s, e, accId, seller, brand, af);
      rows = await q(`SELECT p.accountId, SUM(${P_SALES}) as revenue, SUM(COALESCE(p.netProfit,0)) as netProfit,
        SUM(${P_UNITS}) as units, 0 as orders
        FROM seller_board_product p LEFT JOIN asin a ON p.asin=a.asin ${f.w} GROUP BY p.accountId ORDER BY revenue DESC`, f.p);
    } else {
      const f = scWhere(s, e, accId);
      rows = await q(`SELECT sc.accountId, SUM(${SC_SALES}) as revenue, SUM(COALESCE(sc.netProfit,0)) as netProfit,
        SUM(${SC_UNITS}) as units, SUM(COALESCE(sc.orders,0)) as orders
        FROM seller_board_sales sc ${f.w} GROUP BY sc.accountId ORDER BY revenue DESC`, f.p, 45000);
    }
    // FBA Stock from seller_board_stock (PBI source)
    let stockMap = {};
    try {
      (await q('SELECT accountId, SUM(FBAStock) as fba FROM seller_board_stock WHERE date=(SELECT MAX(date) FROM seller_board_stock) GROUP BY accountId'))
        .forEach(s => { stockMap[s.accountId] = s.fba; });
    } catch (e) {
      // Fallback to fba_iventory_planning
      try {
        (await q('SELECT accountId, SUM(CAST(available AS SIGNED)) as fba FROM fba_iventory_planning WHERE date=(SELECT MAX(date) FROM fba_iventory_planning) GROUP BY accountId'))
          .forEach(s => { stockMap[s.accountId] = s.fba; });
      } catch (e2) { /* ok */ }
    }
    res.json(rows.map(r => {
      const rev = parseFloat(r.revenue) || 0, np = parseFloat(r.netProfit) || 0;
      return {
        shop: shopMap[r.accountId] || `Account ${r.accountId}`,
        revenue: rev, netProfit: np, units: parseInt(r.units) || 0,
        orders: parseInt(r.orders) || 0, margin: rev > 0 ? (np / rev * 100) : 0,
        fbaStock: parseInt(stockMap[r.accountId]) || 0,
      };
    }));
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

/* ═══════════ TEAM ═══════════ */
app.get('/api/team', async (req, res) => {
  try {
    const { start, end, store, seller, brand, asin: af } = req.query;
    const { s, e } = defDates(start, end);
    const accId = await storeToAccId(store);

    // Strategy: 2 small queries (no heavy JOIN)
    const sellerMap = {};
    (await q('SELECT asin, COALESCE(NULLIF(seller,\'\'),\'Unassigned\') as seller, store FROM asin'))
      .forEach(r => { sellerMap[r.asin] = { seller: r.seller, store: r.store }; });

    let where = 'WHERE date BETWEEN ? AND ?';
    const params = [s, e];
    if (af && af !== 'All') { where += ' AND asin = ?'; params.push(af); }
    if (accId) { where += ' AND accountId = ?'; params.push(accId); }

    const rows = await q(`SELECT asin,
      SUM(COALESCE(salesOrganic,0)+COALESCE(salesPPC,0)) as revenue,
      SUM(COALESCE(netProfit,0)) as netProfit,
      SUM(COALESCE(unitsOrganic,0)+COALESCE(unitsPPC,0)) as units
      FROM seller_board_product ${where} GROUP BY asin`, params, 45000);

    const sellerAgg = {};
    rows.forEach(r => {
      const info = sellerMap[r.asin] || { seller: 'Unassigned', store: '' };
      if (seller && seller !== 'All' && info.seller !== seller) return;
      if (brand && brand !== 'All' && info.store !== brand) return;
      const sl = info.seller || 'Unassigned';
      if (!sellerAgg[sl]) sellerAgg[sl] = { revenue: 0, netProfit: 0, units: 0, asins: new Set() };
      sellerAgg[sl].revenue += parseFloat(r.revenue) || 0;
      sellerAgg[sl].netProfit += parseFloat(r.netProfit) || 0;
      sellerAgg[sl].units += parseInt(r.units) || 0;
      sellerAgg[sl].asins.add(r.asin);
    });

    const result = Object.entries(sellerAgg)
      .map(([sl, d]) => ({
        seller: sl, revenue: d.revenue, netProfit: d.netProfit, units: d.units,
        margin: d.revenue > 0 ? (d.netProfit / d.revenue * 100) : 0,
        asinCount: d.asins.size,
      }))
      .sort((a, b) => b.revenue - a.revenue)
      .slice(0, 100);
    res.json(result);
  } catch (e) {
    console.error('TEAM:', e.message);
    res.status(500).json({ error: e.message });
  }
});

/* ═══════════════════════════════════════════════════
   INVENTORY
   PBI formulas:
     FBA Stock         = SUM(seller_board_stock[FBAStock])
     Available         = SUM(fba_iventory_planning[available])
     Reserved          = SUM(fba_iventory_planning[totalReservedQuantity])
     Critical SKUs     = DISTINCTCOUNT(sku) WHERE daysOfSupply <= 7
     InvAge 0-90       = SUM(fba_iventory_planning[invAge0To90Days])   ← direct column!
     InvAge 91-180     = SUM(fba_iventory_planning[invAge91To180Days])
     etc.
   ═══════════════════════════════════════════════════ */
app.get('/api/inventory/snapshot', async (req, res) => {
  try {
    const accId = await storeToAccId(req.query.store);

    // 1) FBA Stock from seller_board_stock (PBI source)
    let fbaStock = 0;
    try {
      let sw = 'WHERE date=(SELECT MAX(date) FROM seller_board_stock)';
      const sp = [];
      if (accId) { sw += ' AND accountId = ?'; sp.push(accId); }
      const sr = await q(`SELECT SUM(FBAStock) as fba FROM seller_board_stock ${sw}`, sp);
      fbaStock = parseInt(sr[0]?.fba) || 0;
    } catch (e) {
      console.warn('seller_board_stock not available, falling back:', e.message);
    }

    // 2) Detail from fba_iventory_planning (latest date)
    let extra = '';
    const params = [];
    if (accId) { extra = ' AND accountId = ?'; params.push(accId); }
    const rows = await q(`SELECT
      SUM(CAST(available AS SIGNED)) as availableInv,
      SUM(COALESCE(totalReservedQuantity,0)) as reserved,
      SUM(COALESCE(inboundQuantity,0)) as inbound,
      COUNT(DISTINCT CASE WHEN daysOfSupply<=7 THEN sku END) as criticalSkus,
      COUNT(DISTINCT CASE WHEN daysOfSupply>7 AND daysOfSupply<=14 THEN sku END) as warningSkus,
      AVG(COALESCE(daysOfSupply,0)) as avgDaysOfSupply,
      SUM(COALESCE(invAge0To90Days,0)) as a0,
      SUM(COALESCE(invAge91To180Days,0)) as a91,
      SUM(COALESCE(invAge181To270Days,0)) as a181,
      SUM(COALESCE(invAge271To365Days,0)) as a271,
      SUM(COALESCE(invAge365PlusDays,0)) as a365,
      SUM(COALESCE(estimatedStorageCostNextMonth,0)) as storageFee,
      AVG(COALESCE(sellThrough,0)) as avgSellThrough
      FROM fba_iventory_planning WHERE date=(SELECT MAX(date) FROM fba_iventory_planning)${extra}`, params);
    const r = rows[0] || {};
    const avail = parseInt(r.availableInv) || 0;
    const reserved = parseInt(r.reserved) || 0;
    const inbound = parseInt(r.inbound) || 0;

    // If seller_board_stock was empty, fallback to available+reserved
    if (fbaStock === 0) fbaStock = avail + reserved;

    res.json({
      fbaStock,
      availableInv: avail,
      totalInventory: avail + reserved + inbound,
      reserved, inbound,
      criticalSkus: parseInt(r.criticalSkus) || 0,
      warningSkus: parseInt(r.warningSkus) || 0,
      avgDaysOfSupply: Math.round(parseFloat(r.avgDaysOfSupply) || 0),
      age0_90: parseInt(r.a0) || 0,   // Direct column, NOT computed by subtraction
      age91_180: parseInt(r.a91) || 0,
      age181_270: parseInt(r.a181) || 0,
      age271_365: parseInt(r.a271) || 0,
      age365plus: parseInt(r.a365) || 0,
      storageFee: parseFloat(r.storageFee) || 0,
      avgSellThrough: parseFloat(r.avgSellThrough) || 0,
    });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

app.get('/api/inventory/stock-trend', async (req, res) => {
  try {
    res.json(await q('SELECT date, SUM(FBAStock) as fbaStock FROM seller_board_stock_daily WHERE date>=DATE_SUB(CURDATE(), INTERVAL 60 DAY) GROUP BY date ORDER BY date'));
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

app.get('/api/inventory/by-shop', async (req, res) => {
  try {
    const shopMap = await getShopMap();

    // FBA Stock per shop from seller_board_stock (PBI source)
    let stockMap = {};
    try {
      (await q('SELECT accountId, SUM(FBAStock) as fba FROM seller_board_stock WHERE date=(SELECT MAX(date) FROM seller_board_stock) GROUP BY accountId'))
        .forEach(r => { stockMap[r.accountId] = parseInt(r.fba) || 0; });
    } catch (e) { /* ok */ }

    // Detail from fba_iventory_planning
    const inv = await q(`SELECT f.accountId,
      SUM(CAST(f.available AS SIGNED)) as avail,
      SUM(COALESCE(f.inboundQuantity,0)) as inb,
      SUM(COALESCE(f.totalReservedQuantity,0)) as res,
      COUNT(DISTINCT CASE WHEN f.daysOfSupply<=7 THEN f.sku END) as crit,
      AVG(COALESCE(f.sellThrough,0)) as st,
      AVG(COALESCE(f.daysOfSupply,0)) as dos
      FROM fba_iventory_planning f WHERE f.date=(SELECT MAX(date) FROM fba_iventory_planning) GROUP BY f.accountId`).catch(() => []);

    const combined = inv.map(r => {
      const accId = r.accountId;
      const fbaFromStock = stockMap[accId];
      const fbaFromPlanning = (parseInt(r.avail) || 0) + (parseInt(r.res) || 0);
      return {
        shop: shopMap[accId] || `Account ${accId}`,
        fbaStock: fbaFromStock != null ? fbaFromStock : fbaFromPlanning,
        inbound: parseInt(r.inb) || 0,
        reserved: parseInt(r.res) || 0,
        criticalSkus: parseInt(r.crit) || 0,
        sellThrough: parseFloat(r.st) || 0,
        daysOfSupply: parseFloat(r.dos) || 0,
      };
    });

    // Add shops only in seller_board_stock but not fba_iventory_planning
    const invAccIds = new Set(inv.map(r => r.accountId));
    Object.entries(stockMap).forEach(([accId, fba]) => {
      if (!invAccIds.has(parseInt(accId)) && !invAccIds.has(accId)) {
        combined.push({
          shop: shopMap[accId] || `Account ${accId}`,
          fbaStock: fba, inbound: 0, reserved: 0,
          criticalSkus: 0, sellThrough: 0, daysOfSupply: 0,
        });
      }
    });
    combined.sort((a, b) => b.fbaStock - a.fbaStock);
    res.json(combined);
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

/* ═══════════ OPS DAILY ═══════════ */
app.get('/api/ops/daily', async (req, res) => {
  try {
    const { start, end, store, seller, brand, asin: af } = req.query;
    const { s, e } = defDates(start, end);
    const accId = await storeToAccId(store);
    let rows;
    if (useProduct(seller, brand, af)) {
      const f = pWhere(s, e, accId, seller, brand, af);
      rows = await q(`SELECT p.date, SUM(${P_SALES}) as revenue, SUM(COALESCE(p.netProfit,0)) as netProfit,
        SUM(${P_UNITS}) as units, 0 as orders, SUM(${P_ADS}) as adSpend
        FROM seller_board_product p LEFT JOIN asin a ON p.asin=a.asin ${f.w} GROUP BY p.date ORDER BY p.date DESC LIMIT 60`, f.p);
    } else {
      const f = scWhere(s, e, accId);
      rows = await q(`SELECT sc.date, SUM(${SC_SALES}) as revenue, SUM(COALESCE(sc.netProfit,0)) as netProfit,
        SUM(${SC_UNITS}) as units, SUM(COALESCE(sc.orders,0)) as orders, SUM(${SC_ADS}) as adSpend
        FROM seller_board_sales sc ${f.w} GROUP BY sc.date ORDER BY sc.date DESC LIMIT 60`, f.p);
    }
    res.json(rows);
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

/* ═══════════════════════════════════════════════════
   ASIN PLAN
   ═══════════════════════════════════════════════════ */
const MS = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'];

function mapMetric(m) {
  if (!m) return null;
  const t = m.trim();
  const MAP = {
    'Rev': 'rv', 'Revenue': 'rv', 'revenue': 'rv', 'rev': 'rv',
    'Unit': 'un', 'Units': 'un', 'units': 'un', 'unit': 'un',
    'Ads': 'ad', 'Ad Spend': 'ad', 'ads': 'ad', 'ad': 'ad',
    'Gross Profit': 'gp', 'GP': 'gp', 'gp': 'gp', 'NP': 'gp', 'np': 'gp',
    'Session': 'se', 'Sessions': 'se', 'sessions': 'se', 'session': 'se',
    'Impression': 'im', 'Impressions': 'im', 'impressions': 'im', 'impression': 'im',
    'CR': 'cr', 'cr': 'cr', 'Conversion Rate': 'cr',
    'CTR': 'ct', 'ctr': 'ct', 'Click Through Rate': 'ct',
    'Price': 'pr', 'price': 'pr',
    'CPC': 'cpc', 'cpc': 'cpc', 'CPM': 'cpm', 'cpm': 'cpm',
    'Cogs': 'cg', 'cogs': 'cg', 'AMZ fee': 'af', 'amz fee': 'af',
  };
  if (MAP[t]) return MAP[t];
  const lm = t.toLowerCase();
  if (lm.includes('revenue') || lm.includes('sales')) return 'rv';
  if (lm.includes('gross profit') || lm.includes('net profit')) return 'gp';
  if (lm.includes('unit')) return 'un';
  if (lm.includes('session')) return 'se';
  if (lm.includes('impression')) return 'im';
  if (lm.includes('ad')) return 'ad';
  if (lm === 'cr' || lm.includes('conversion')) return 'cr';
  if (lm === 'ctr' || lm.includes('click')) return 'ct';
  return null;
}

/* ═══════════ PLAN DATA (plan targets) ═══════════ */
app.get('/api/plan/data', async (req, res) => {
  try {
    const { year, month, store, seller, brand, asin: af } = req.query;
    const yr = parseInt(year) || new Date().getFullYear();
    let cols;
    try { cols = (await q('SHOW COLUMNS FROM asin_plan')).map(c => c.Field); }
    catch { return res.json({ kpi: {}, monthlyPlan: {}, asinPlan: {} }); }

    const hasYear = cols.includes('year');
    let where = hasYear ? 'WHERE ap.`year` = ?' : 'WHERE 1=1';
    let params = hasYear ? [yr] : [];
    if (month && month !== 'All') {
      const mn = parseInt(month);
      if (mn >= 1 && mn <= 12) { where += ' AND ap.month_num = ?'; params.push(mn); }
    }
    // Brand filter: match brand_name in asin_plan OR via store filter
    if (brand && brand !== 'All') { where += ' AND (ap.brand_name = ? OR a.store = ?)'; params.push(brand, brand); }
    if (store && store !== 'All') {
      const accId2 = await storeToAccId(store);
      if (accId2) {
        const storeAsins = (await q(
          'SELECT DISTINCT asin FROM seller_board_product WHERE accountId = ? AND date >= DATE_SUB(CURDATE(), INTERVAL 365 DAY) LIMIT 2000',
          [accId2], 15000
        ).catch(() => [])).map(r => r.asin);
        if (storeAsins.length) {
          const ph = storeAsins.map(() => '?').join(',');
          where += ` AND (ap.brand_name = ? OR ap.asin IN (${ph}))`;
          params.push(store, ...storeAsins);
        } else {
          where += ' AND ap.brand_name = ?';
          params.push(store);
        }
      }
    }
    if (af && af !== 'All') { where += ' AND ap.asin = ?'; params.push(af); }
    if (seller && seller !== 'All') { where += ' AND a.seller = ?'; params.push(seller); }

    const rows = await q(`SELECT ap.asin, ap.brand_name, ap.month_num, ap.metrics,
      COALESCE(CAST(ap.value AS DECIMAL(20,4)),0) as value
      FROM asin_plan ap LEFT JOIN asin a ON ap.asin=a.asin ${where} ORDER BY ap.month_num`, params, 45000);

    const monthlyPlan = {}, asinPlan = {};
    rows.forEach(r => {
      const mk = mapMetric(r.metrics);
      if (!mk) return;
      const mn = r.month_num, val = parseFloat(r.value) || 0;
      if (!monthlyPlan[mn]) monthlyPlan[mn] = {};
      monthlyPlan[mn][mk] = (monthlyPlan[mn][mk] || 0) + val;
      const key = r.asin;
      if (!asinPlan[key]) asinPlan[key] = { brand: r.brand_name, months: {} };
      if (!asinPlan[key].months[mn]) asinPlan[key].months[mn] = {};
      asinPlan[key].months[mn][mk] = (asinPlan[key].months[mn][mk] || 0) + val;
    });

    /* ── KPI: CR/CTR weighted (matching PBI) ── */
    const kpi = { gp: { a: 0, p: 0 }, rv: { a: 0, p: 0 }, ad: { a: 0, p: 0 }, un: { a: 0, p: 0 }, se: { a: 0, p: 0 }, im: { a: 0, p: 0 }, cr: { a: 0, p: 0 }, ct: { a: 0, p: 0 } };
    const crDirect = [], ctDirect = [];
    Object.values(monthlyPlan).forEach(mp => {
      ['gp', 'rv', 'ad', 'un', 'se', 'im'].forEach(k => { kpi[k].p += mp[k] || 0; });
      if (mp.cr) crDirect.push(mp.cr);
      if (mp.ct) ctDirect.push(mp.ct);
    });
    // PBI: CR = DIVIDE(Units, Sessions) → weighted
    if (kpi.se.p > 0 && kpi.un.p > 0) kpi.cr.p = kpi.un.p / kpi.se.p;
    else if (crDirect.length) kpi.cr.p = (crDirect.reduce((s, v) => s + v, 0) / crDirect.length) / 100;
    // PBI: CTR = DIVIDE(Sessions, Impressions) → weighted
    if (kpi.im.p > 0 && kpi.se.p > 0) kpi.ct.p = kpi.se.p / kpi.im.p;
    else if (ctDirect.length) kpi.ct.p = (ctDirect.reduce((s, v) => s + v, 0) / ctDirect.length) / 100;

    // Per-month plan CR/CTR (weighted per month)
    for (const mn in monthlyPlan) {
      const mp = monthlyPlan[mn];
      if (mp.un && mp.se) mp.crW = mp.un / mp.se;
      else if (mp.cr) mp.crW = mp.cr / 100;
      else mp.crW = 0;
      if (mp.se && mp.im) mp.ctW = mp.se / mp.im;
      else if (mp.ct) mp.ctW = mp.ct / 100;
      else mp.ctW = 0;
    }

    res.json({ kpi, monthlyPlan, asinPlan });
  } catch (e) {
    console.error('plan/data ERROR:', e.message);
    res.status(500).json({ error: 'Plan data: ' + e.message });
  }
});

/* ═══════════ PLAN ACTUALS ═══════════
   PBI sources:
     Revenue, GP, Units, Sessions → seller_board_sales_append (= seller_board_sales here)
     Ads Spend → ABS(seller_board_product sponsored*)
     Impressions, Clicks → analytics_search_catalog_performance
   ═══════════════════════════════════ */
app.get('/api/plan/actuals', async (req, res) => {
  try {
    const { year, store, seller, brand, asin: af } = req.query;
    const yr = parseInt(year) || new Date().getFullYear();
    const accId = await storeToAccId(store);

    // ── Source 1: Revenue, GP, Units, Sessions from seller_board_sales (PBI match) ──
    let salesRows = [];
    try {
      const scF = scWhere(`${yr}-01-01`, `${yr}-12-31`, accId);
      salesRows = await q(`SELECT MONTH(sc.date) as mn,
        SUM(${SC_SALES}) as revenue, SUM(COALESCE(sc.grossProfit,0)) as gp,
        SUM(${SC_UNITS}) as units, SUM(COALESCE(sc.sessions,0)) as sessions
        FROM seller_board_sales sc ${scF.w} GROUP BY MONTH(sc.date)`, scF.p, 45000);
    } catch (e1) { console.warn('plan/actuals sales failed:', e1.message); }

    // ── Source 2: Ads from seller_board_product ──
    let adsRows = [];
    const pF = pWhere(`${yr}-01-01`, `${yr}-12-31`, accId, seller, brand, af);
    try {
      adsRows = await q(`SELECT MONTH(p.date) as mn, SUM(ABS(${P_ADS})) as ads
        FROM seller_board_product p LEFT JOIN asin a ON p.asin=a.asin ${pF.w} GROUP BY MONTH(p.date)`, pF.p, 45000);
    } catch (e2) { console.warn('plan/actuals ads failed:', e2.message); }

    // ── Source 3: Impressions + Clicks from analytics ──
    let impRows = [];
    try {
      let iw = 'WHERE YEAR(startDate)=?';
      const ip = [yr];
      if (accId) { iw += ' AND accountId=?'; ip.push(accId); }
      impRows = await q(`SELECT MONTH(startDate) as mn,
        SUM(COALESCE(impressionCount,0)) as imp, SUM(COALESCE(clickCount,0)) as clicks
        FROM analytics_search_catalog_performance ${iw} GROUP BY MONTH(startDate)`, ip, 45000);
    } catch (e3) { console.warn('analytics not available:', e3.message); }

    // ── Merge into monthly ──
    const monthly = {};
    for (let m = 1; m <= 12; m++) monthly[m] = { rv: 0, gp: 0, un: 0, se: 0, ad: 0, im: 0, clicks: 0 };
    salesRows.forEach(r => {
      const m = monthly[r.mn]; if (!m) return;
      m.rv = parseFloat(r.revenue) || 0; m.gp = parseFloat(r.gp) || 0;
      m.un = parseInt(r.units) || 0; m.se = parseFloat(r.sessions) || 0;
    });
    adsRows.forEach(r => { const m = monthly[r.mn]; if (m) m.ad = parseFloat(r.ads) || 0; });
    impRows.forEach(r => { const m = monthly[r.mn]; if (m) { m.im = parseFloat(r.imp) || 0; m.clicks = parseFloat(r.clicks) || 0; } });

    const monthlyArr = [];
    for (let m = 1; m <= 12; m++) {
      const d = monthly[m];
      const cr = d.se > 0 ? d.un / d.se : 0;
      const ctr = d.im > 0 ? d.clicks / d.im : 0;
      monthlyArr.push({
        m: MS[m - 1], mn: m, ra: d.rv, gpa: d.gp, aa: d.ad, ua: d.un, sa: d.se, ia: d.im,
        cra: Math.round(cr * 10000) / 10000,
        cta: Math.round(ctr * 10000) / 10000,
      });
    }

    // ── ASIN breakdown from seller_board_product ──
    let asinRows = [];
    try {
      asinRows = await q(`SELECT p.asin, ap2.brand_name as planBrand, a.seller, MONTH(p.date) as mn,
        SUM(${P_SALES}) as revenue, SUM(COALESCE(p.grossProfit,0)) as gp,
        SUM(${P_UNITS}) as units, SUM(COALESCE(p.sessions,0)) as sessions, SUM(ABS(${P_ADS})) as ads
        FROM seller_board_product p LEFT JOIN asin a ON p.asin=a.asin
        LEFT JOIN (SELECT DISTINCT asin, brand_name FROM asin_plan) ap2 ON p.asin=ap2.asin
        ${pF.w} GROUP BY p.asin, ap2.brand_name, a.seller, MONTH(p.date) ORDER BY gp DESC`, pF.p, 45000);
    } catch (e4) { console.warn('plan/actuals asin query failed:', e4.message); }

    const asinData = {};
    asinRows.forEach(r => {
      const key = r.asin, mn = r.mn;
      if (!asinData[key]) asinData[key] = { brand: r.planBrand || '', seller: r.seller || '', months: {} };
      if (!asinData[key].months[mn]) asinData[key].months[mn] = { rv: 0, gp: 0, ad: 0, un: 0, se: 0, im: 0, cr: 0, ct: 0 };
      const md = asinData[key].months[mn];
      md.rv += parseFloat(r.revenue) || 0; md.gp += parseFloat(r.gp) || 0;
      md.ad += parseFloat(r.ads) || 0; md.un += parseInt(r.units) || 0;
      md.se += parseFloat(r.sessions) || 0;
      md.cr = md.se > 0 ? md.un / md.se : 0;
    });

    const asinBreakdown = Object.entries(asinData).map(([asin, d]) => {
      const t = { rv: 0, gp: 0, ad: 0, un: 0, se: 0 };
      Object.values(d.months).forEach(m => { t.rv += m.rv; t.gp += m.gp; t.ad += m.ad; t.un += m.un; t.se += m.se; });
      const cr = t.se > 0 ? t.un / t.se : 0;
      return {
        a: asin, br: d.brand, sl: d.seller, ra: t.rv, ga: t.gp, aa: t.ad, ua: t.un, sa: t.se, ia: 0,
        cra: Math.round(cr * 10000) / 10000, cta: 0, months: d.months,
      };
    }).sort((a, b) => b.ga - a.ga);

    res.json({ monthly: monthlyArr, asinBreakdown });
  } catch (e) {
    console.error('plan/actuals:', e.message);
    res.status(500).json({ error: e.message });
  }
});

/* ═══════════ AI INSIGHT ═══════════ */
app.post('/api/ai/insight', async (req, res) => {
  try {
    const apiKey = process.env.ANTHROPIC_API_KEY;
    if (!apiKey) return res.status(400).json({ error: 'No API key' });
    const { context, question } = req.body;
    const r = await fetch('https://api.anthropic.com/v1/messages', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', 'x-api-key': apiKey, 'anthropic-version': '2023-06-01' },
      body: JSON.stringify({
        model: 'claude-sonnet-4-20250514', max_tokens: 1500,
        system: 'You are an expert Amazon FBA analyst for Expeditee LLC. Give actionable insights in 300-500 words.',
        messages: [{ role: 'user', content: `Data:\n${JSON.stringify(context, null, 2)}\n\n${question || 'Analyze.'}` }],
      }),
    });
    const data = await r.json();
    res.json({ insight: data.content?.[0]?.text || 'Unable to generate' });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

/* ═══════════ DEBUG ═══════════ */
app.get('/api/debug/filters', async (req, res) => {
  const R = { version: VER, steps: {} };
  try {
    R.steps.accounts = await q('SELECT id, shop FROM accounts WHERE deleted_at IS NULL LIMIT 3');
    R.steps.tables = (await q("SHOW TABLES")).map(r => Object.values(r)[0]);
    try {
      const dr = await q('SELECT MIN(sc.date) as mi, MAX(sc.date) as mx FROM seller_board_sales sc');
      R.steps.salesRange = { min: dr[0]?.mi, max: dr[0]?.mx };
    } catch (e) { R.steps.salesRange = e.message; }
    R.steps.planMetrics = (await q('SELECT DISTINCT metrics FROM asin_plan LIMIT 20').catch(() => [])).map(m => `${m.metrics}→${mapMetric(m.metrics)}`);
    try { R.steps.analyticsCols = (await q('SHOW COLUMNS FROM analytics_search_catalog_performance')).map(c => c.Field); } catch (e) { R.steps.analyticsCols = e.message; }
    try { R.steps.stockCols = (await q('SHOW COLUMNS FROM seller_board_stock')).map(c => c.Field); } catch (e) { R.steps.stockCols = e.message; }
  } catch (e) { R.globalError = e.message; }
  res.json(R);
});

/* ═══════════ SERVE FRONTEND ═══════════ */
const distPath = join(__dirname, 'dist');
app.use(express.static(distPath));
app.get('*', (req, res) => { res.sendFile(join(distPath, 'index.html')); });
app.listen(PORT, '0.0.0.0', () => {
  console.log(`\n🚀 Dashboard ${VER} on :${PORT} | DB: ${process.env.DB_HOST || 'none'}\n`);
});
