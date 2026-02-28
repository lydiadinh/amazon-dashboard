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
app.use(cors());
app.use(express.json({ limit: '10mb' }));

/* ═══════════ DATABASE POOL ═══════════ */
let pool = null;
try {
  const dbConfig = {
    host: process.env.DB_HOST || 'localhost',
    port: parseInt(process.env.DB_PORT) || 3306,
    user: process.env.DB_USER || 'root',
    password: process.env.DB_PASSWORD || '',
    database: process.env.DB_NAME || 'expeditee',
    waitForConnections: true,
    connectionLimit: 10,
    queueLimit: 0,
    connectTimeout: 15000,
    enableKeepAlive: true,
    keepAliveInitialDelay: 10000,
  };
  // Enable SSL for remote databases (common for cloud MySQL)
  if (process.env.DB_SSL === 'true' || (process.env.DB_HOST && process.env.DB_HOST !== 'localhost' && process.env.DB_HOST !== '127.0.0.1')) {
    dbConfig.ssl = { rejectUnauthorized: process.env.DB_SSL_REJECT !== 'false' };
    console.log('🔒 SSL enabled for database connection');
  }
  if (process.env.DB_HOST && process.env.DB_HOST !== 'your-db-host.com') {
    pool = mysql.createPool(dbConfig);
    console.log(`✅ MySQL pool created → ${process.env.DB_HOST}:${dbConfig.port}/${process.env.DB_NAME}`);
  } else {
    console.log('⚠️ No database configured — running in demo mode');
    console.log('   Set DB_HOST, DB_USER, DB_PASSWORD, DB_NAME in environment variables');
  }
} catch (e) {
  console.warn('⚠️ MySQL pool failed:', e.message);
}

async function q(sql, params = []) {
  if (!pool) throw new Error('Database not connected');
  const [rows] = await pool.execute(sql, params);
  return rows;
}

async function getShopMap() {
  const rows = await q('SELECT id, shop FROM accounts WHERE deleted_at IS NULL');
  const map = {};
  rows.forEach(r => { map[r.id] = r.shop; });
  return map;
}

/* ═══════════════════════════════════════════════════════════════
   DEDUP: seller_board_sales + seller_board_sales_old
   
   - seller_board_sales_old: data up to ~end May 2024
   - seller_board_sales: data from ~26 May 2024 onwards
   - Overlapping dates: seller_board_sales takes priority
   
   Column differences handled in the UNION:
   - Both tables have: amazonFees (pre-calculated), productCostSales, grossProfit, netProfit, margin, realACOS
   - seller_board_day/product have: costOfGoods (not productCostSales), googleAds, facebookAds
   ═══════════════════════════════════════════════════════════════ */

// Build deduped UNION subquery for a given date range
// Returns a virtual table with unified column names
function salesUnion(dateAlias = 'date') {
  return `(
    SELECT accountId, date,
      (COALESCE(salesOrganic,0) + COALESCE(salesPPC,0)) as sales,
      (COALESCE(unitsOrganic,0) + COALESCE(unitsPPC,0)) as units,
      COALESCE(orders,0) as orders,
      COALESCE(refunds,0) as refunds,
      (COALESCE(sponsoredProducts,0) + COALESCE(sponsoredDisplay,0) + COALESCE(sponsoredBrands,0) + COALESCE(sponsoredBrandsVideo,0)) as adSpend,
      COALESCE(shipping,0) as shipping,
      COALESCE(refundCost,0) as refundCost,
      COALESCE(amazonFees,0) as amazonFees,
      COALESCE(productCostSales,0) as cogs,
      COALESCE(estimatedPayout,0) as estimatedPayout,
      COALESCE(grossProfit,0) as grossProfit,
      COALESCE(netProfit,0) as netProfit,
      COALESCE(margin,0) as margin,
      COALESCE(realACOS,0) as realACOS,
      COALESCE(sessions,0) as sessions,
      COALESCE(unitSessionPercentage,0) as unitSessionPct,
      'new' as src
    FROM seller_board_sales

    UNION ALL

    SELECT accountId, date,
      (COALESCE(salesOrganic,0) + COALESCE(salesPPC,0)) as sales,
      (COALESCE(unitsOrganic,0) + COALESCE(unitsPPC,0)) as units,
      COALESCE(orders,0) as orders,
      COALESCE(refunds,0) as refunds,
      (COALESCE(sponsoredProducts,0) + COALESCE(sponsoredDisplay,0) + COALESCE(sponsoredBrands,0) + COALESCE(sponsoredBrandsVideo,0)) as adSpend,
      COALESCE(shipping,0) as shipping,
      COALESCE(refundCost,0) as refundCost,
      COALESCE(amazonFees,0) as amazonFees,
      COALESCE(productCostSales,0) as cogs,
      COALESCE(estimatedPayout,0) as estimatedPayout,
      COALESCE(grossProfit,0) as grossProfit,
      COALESCE(netProfit,0) as netProfit,
      COALESCE(margin,0) as margin,
      COALESCE(realACOS,0) as realACOS,
      COALESCE(sessions,0) as sessions,
      COALESCE(unitSessionPercentage,0) as unitSessionPct,
      'old' as src
    FROM seller_board_sales_old
    WHERE (accountId, date) NOT IN (
      SELECT accountId, date FROM seller_board_sales
    )
  )`;
}

/* ═══════════ HEALTH CHECK ═══════════ */
app.get('/api/health', async (req, res) => {
  try {
    if (pool) {
      await q('SELECT 1');
      const tables = await q("SHOW TABLES");
      const tableNames = tables.map(t => Object.values(t)[0]);
      const hasRequired = ['seller_board_sales', 'seller_board_day', 'seller_board_product'].every(t => tableNames.includes(t));
      res.json({ status: 'ok', database: 'connected', tables: tableNames.length, hasRequired });
    } else {
      res.json({ status: 'ok', database: 'not configured' });
    }
  } catch (e) {
    res.json({ status: 'ok', database: 'error: ' + e.message });
  }
});

/* ═══════════ DATE RANGE — auto-detect from DB ═══════════ */
app.get('/api/date-range', async (req, res) => {
  try {
    // Check date ranges across all critical tables
    const salesRange = await q(`
      SELECT MIN(d) as minDate, MAX(d) as maxDate FROM (
        SELECT MIN(date) as d FROM seller_board_sales
        UNION ALL SELECT MAX(date) as d FROM seller_board_sales
        UNION ALL SELECT MIN(date) as d FROM seller_board_sales_old
        UNION ALL SELECT MAX(date) as d FROM seller_board_sales_old
      ) t
    `);
    let dayMax = null, productMax = null;
    try {
      const dm = await q('SELECT MAX(date) as d FROM seller_board_day');
      dayMax = dm[0]?.d ? new Date(dm[0].d).toISOString().slice(0,10) : null;
    } catch(e) {}
    try {
      const pm = await q('SELECT MAX(date) as d FROM seller_board_product');
      productMax = pm[0]?.d ? new Date(pm[0].d).toISOString().slice(0,10) : null;
    } catch(e) {}

    const r = salesRange[0] || {};
    const minD = r.minDate ? new Date(r.minDate).toISOString().slice(0,10) : null;
    const maxD = r.maxDate ? new Date(r.maxDate).toISOString().slice(0,10) : null;
    
    // Smart default: use the min of all max dates so all tables have data
    const allMax = [maxD, dayMax, productMax].filter(Boolean);
    const smartMax = allMax.length > 0 ? allMax.sort()[0] : maxD; // earliest max = all tables have data
    
    let defaultStart = smartMax, defaultEnd = smartMax;
    if (smartMax) {
      const ms = new Date(smartMax);
      ms.setDate(ms.getDate() - 29);
      defaultStart = ms.toISOString().slice(0,10);
      if (defaultStart < minD) defaultStart = minD;
    }
    res.json({ minDate: minD, maxDate: maxD, defaultStart, defaultEnd,
      tableMaxDates: { sales: maxD, day: dayMax, product: productMax }
    });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

/* ═══════════ ADMIN: Check & clean overlap ═══════════ */
app.get('/api/admin/check-overlap', async (req, res) => {
  try {
    const oldMax = await q('SELECT MAX(date) as d FROM seller_board_sales_old');
    const newMin = await q('SELECT MIN(date) as d FROM seller_board_sales');
    const overlapCount = await q(`
      SELECT COUNT(*) as cnt FROM seller_board_sales_old o
      WHERE EXISTS (SELECT 1 FROM seller_board_sales s WHERE s.accountId = o.accountId AND s.date = o.date)
    `);
    res.json({
      oldTableMaxDate: oldMax[0]?.d,
      newTableMinDate: newMin[0]?.d,
      overlappingRows: overlapCount[0]?.cnt || 0,
      recommendation: overlapCount[0]?.cnt > 0
        ? `Found ${overlapCount[0].cnt} overlapping rows. POST /api/admin/clean-overlap to clean.`
        : 'No overlap found.'
    });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.post('/api/admin/clean-overlap', async (req, res) => {
  try {
    const result = await q(`
      DELETE o FROM seller_board_sales_old o
      INNER JOIN seller_board_sales s ON o.accountId = s.accountId AND o.date = s.date
    `);
    res.json({ deleted: result.affectedRows || 0 });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

/* ═══════════ FILTERS ═══════════ */
app.get('/api/filters', async (req, res) => {
  try {
    const shops = await q('SELECT id, shop FROM accounts WHERE deleted_at IS NULL ORDER BY shop');
    const sellers = await q('SELECT DISTINCT seller FROM asin WHERE seller IS NOT NULL AND seller != "" ORDER BY seller');
    const brands = await q('SELECT DISTINCT store FROM asin WHERE store IS NOT NULL AND store != "" ORDER BY store');
    // Get full mapping: asin → seller → brand → shop (via seller_board_product → accountId → accounts)
    const asinShopMap = await q(`
      SELECT DISTINCT a.asin, a.seller, a.store as brand, acc.shop
      FROM asin a
      LEFT JOIN (
        SELECT DISTINCT asin, accountId FROM seller_board_product
      ) p ON a.asin = p.asin
      LEFT JOIN accounts acc ON p.accountId = acc.id AND acc.deleted_at IS NULL
      ORDER BY acc.shop, a.store, a.asin
    `);
    res.json({
      shops: shops.map(s => ({ id: s.id, name: s.shop })),
      sellers: sellers.map(s => s.seller),
      brands: brands.map(b => b.store),
      asins: asinShopMap.map(a => ({ asin: a.asin, seller: a.seller, brand: a.brand, shop: a.shop })),
    });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

/* ═══════════════════════════════════════════════════
   EXECUTIVE OVERVIEW
   Source: seller_board_sales + seller_board_sales_old
   ═══════════════════════════════════════════════════ */
app.get('/api/exec/summary', async (req, res) => {
  try {
    const { start, end, store, seller, brand, asin: asinFilter } = req.query;
    const s = start || '2026-01-01', e = end || '2026-01-31';
    const hasSBA = (seller && seller !== 'All') || (brand && brand !== 'All') || (asinFilter && asinFilter !== 'All');

    let rows;
    if (hasSBA) {
      // Use seller_board_product (has per-ASIN data for seller/brand/asin filtering)
      // Note: seller_board_product does NOT have orders column
      let where = 'WHERE p.date BETWEEN ? AND ?';
      const params = [s, e];
      if (store && store !== 'All') {
        const shopMap = await getShopMap();
        const accId = Object.entries(shopMap).find(([k, v]) => v === store)?.[0];
        if (accId) { where += ' AND p.accountId = ?'; params.push(parseInt(accId)); }
      }
      if (seller !== 'All' && seller) { where += ' AND a.seller = ?'; params.push(seller); }
      if (brand !== 'All' && brand) { where += ' AND a.store = ?'; params.push(brand); }
      if (asinFilter !== 'All' && asinFilter) { where += ' AND p.asin = ?'; params.push(asinFilter); }
      rows = await q(`
        SELECT
          SUM(p.salesOrganic + p.salesPPC) as sales,
          SUM(p.unitsOrganic + p.unitsPPC) as units,
          0 as orders, 0 as refunds,
          SUM(COALESCE(p.sponsoredProducts,0)+COALESCE(p.sponsoredDisplay,0)+COALESCE(p.sponsoredBrands,0)+COALESCE(p.sponsoredBrandsVideo,0)+COALESCE(p.googleAds,0)+COALESCE(p.facebookAds,0)) as advCost,
          0 as shippingCost, 0 as refundCost,
          SUM(COALESCE(p.amazonFees,0)) as amazonFees,
          SUM(COALESCE(p.costOfGoods,0)) as cogs,
          0 as estPayout,
          SUM(COALESCE(p.grossProfit,0)) as grossProfit,
          SUM(COALESCE(p.netProfit,0)) as netProfit,
          SUM(COALESCE(p.sessions,0)) as sessions
        FROM seller_board_product p LEFT JOIN asin a ON p.asin = a.asin
        ${where}
      `, params);
    } else {
      // Use salesUnion (faster, account-level)
      let extraWhere = '';
      const params = [s, e];
      if (store && store !== 'All') {
        const shopMap = await getShopMap();
        const accId = Object.entries(shopMap).find(([k, v]) => v === store)?.[0];
        if (accId) { extraWhere = ' AND d.accountId = ?'; params.push(parseInt(accId)); }
      }
      rows = await q(`
        SELECT
          SUM(d.sales) as sales, SUM(d.units) as units,
          SUM(d.orders) as orders, SUM(d.refunds) as refunds,
          SUM(d.adSpend) as advCost, SUM(d.shipping) as shippingCost,
          SUM(d.refundCost) as refundCost, SUM(d.amazonFees) as amazonFees,
          SUM(d.cogs) as cogs, SUM(d.estimatedPayout) as estPayout,
          SUM(d.grossProfit) as grossProfit, SUM(d.netProfit) as netProfit,
          SUM(d.sessions) as sessions
        FROM ${salesUnion()} d
        WHERE d.date BETWEEN ? AND ? ${extraWhere}
      `, params);
    }
    const r = rows[0] || {};
    const sales = parseFloat(r.sales) || 0;
    const np = parseFloat(r.netProfit) || 0;
    res.json({
      sales, units: parseInt(r.units) || 0, orders: parseInt(r.orders) || 0,
      refunds: parseInt(r.refunds) || 0,
      advCost: parseFloat(r.advCost) || 0, shippingCost: parseFloat(r.shippingCost) || 0,
      refundCost: parseFloat(r.refundCost) || 0, amazonFees: parseFloat(r.amazonFees) || 0,
      cogs: parseFloat(r.cogs) || 0, netProfit: np,
      estPayout: parseFloat(r.estPayout) || 0, grossProfit: parseFloat(r.grossProfit) || 0,
      sessions: parseFloat(r.sessions) || 0,
      realAcos: sales > 0 ? (Math.abs(parseFloat(r.advCost) || 0) / sales * 100) : 0,
      pctRefunds: (parseInt(r.orders) || 0) > 0 ? ((parseInt(r.refunds) || 0) / parseInt(r.orders) * 100) : 0,
      margin: sales > 0 ? (np / sales * 100) : 0,
    });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

// Daily trend — from seller_board_day (matches PBI "DR" measures)
app.get('/api/exec/daily', async (req, res) => {
  try {
    const { start, end, store, seller, brand, asin: asinFilter } = req.query;
    const s = start || '2026-01-01', e = end || '2026-01-31';
    const hasSBA = (seller && seller !== 'All') || (brand && brand !== 'All') || (asinFilter && asinFilter !== 'All');

    let rows;
    if (hasSBA) {
      // Use seller_board_product for seller/brand/asin filtering
      let where = 'WHERE p.date BETWEEN ? AND ?';
      const params = [s, e];
      if (store && store !== 'All') {
        const shopMap = await getShopMap();
        const accId = Object.entries(shopMap).find(([k, v]) => v === store)?.[0];
        if (accId) { where += ' AND p.accountId = ?'; params.push(parseInt(accId)); }
      }
      if (seller !== 'All' && seller) { where += ' AND a.seller = ?'; params.push(seller); }
      if (brand !== 'All' && brand) { where += ' AND a.store = ?'; params.push(brand); }
      if (asinFilter !== 'All' && asinFilter) { where += ' AND p.asin = ?'; params.push(asinFilter); }
      rows = await q(`
        SELECT p.date,
          SUM(p.salesOrganic + p.salesPPC) as revenue,
          SUM(p.netProfit) as netProfit,
          SUM(p.unitsOrganic + p.unitsPPC) as units
        FROM seller_board_product p LEFT JOIN asin a ON p.asin = a.asin
        ${where} GROUP BY p.date ORDER BY p.date
      `, params);
    } else {
      // Try seller_board_day (has googleAds, facebookAds) with optional store filter
      let extraWhere = '';
      const params = [s, e];
      if (store && store !== 'All') {
        const shopMap = await getShopMap();
        const accId = Object.entries(shopMap).find(([k, v]) => v === store)?.[0];
        if (accId) { extraWhere = ' AND accountId = ?'; params.push(parseInt(accId)); }
      }
      rows = await q(`
        SELECT date,
          SUM(salesOrganic + salesPPC) as revenue,
          SUM(netProfit) as netProfit,
          SUM(unitsOrganic + unitsPPC) as units,
          SUM(orders) as orders, SUM(refunds) as refunds, SUM(sessions) as sessions,
          SUM(COALESCE(sponsoredProducts,0)+COALESCE(sponsoredDisplay,0)+COALESCE(sponsoredBrands,0)+COALESCE(sponsoredBrandsVideo,0)+COALESCE(googleAds,0)+COALESCE(facebookAds,0)) as adSpend
        FROM seller_board_day
        WHERE date BETWEEN ? AND ? ${extraWhere}
        GROUP BY date ORDER BY date
      `, params);
      // Fallback to salesUnion
      if (!rows || rows.length === 0) {
        const params2 = [s, e];
        let ew2 = '';
        if (store && store !== 'All') {
          const shopMap = await getShopMap();
          const accId = Object.entries(shopMap).find(([k, v]) => v === store)?.[0];
          if (accId) { ew2 = ' AND d.accountId = ?'; params2.push(parseInt(accId)); }
        }
        rows = await q(`
          SELECT d.date, SUM(d.sales) as revenue, SUM(d.netProfit) as netProfit,
            SUM(d.units) as units, SUM(d.orders) as orders, SUM(d.sessions) as sessions,
            SUM(d.adSpend) as adSpend
          FROM ${salesUnion()} d WHERE d.date BETWEEN ? AND ? ${ew2}
          GROUP BY d.date ORDER BY d.date
        `, params2);
      }
    }
    res.json(rows);
  } catch (e) { res.status(500).json({ error: e.message }); }
});

// Revenue by shop from seller_board_sales (+ old dedup)
app.get('/api/exec/by-shop', async (req, res) => {
  try {
    const { start, end } = req.query;
    const s = start || '2026-01-01', e = end || '2026-01-31';
    const shopMap = await getShopMap();
    const rows = await q(`
      SELECT d.accountId,
        SUM(d.sales) as revenue, SUM(d.netProfit) as netProfit, SUM(d.units) as units
      FROM ${salesUnion()} d
      WHERE d.date BETWEEN ? AND ?
      GROUP BY d.accountId ORDER BY revenue DESC
    `, [s, e]);
    res.json(rows.map(r => ({
      shop: shopMap[r.accountId] || `Account ${r.accountId}`,
      revenue: parseFloat(r.revenue) || 0,
      netProfit: parseFloat(r.netProfit) || 0,
      units: parseInt(r.units) || 0,
    })));
  } catch (e) { res.status(500).json({ error: e.message }); }
});

// Top ASINs — from seller_board_product (per-ASIN data)
app.get('/api/exec/top-asins', async (req, res) => {
  try {
    const { start, end, store, seller, limit: lim } = req.query;
    const s = start || '2026-01-01', e = end || '2026-01-31';
    let where = 'WHERE p.date BETWEEN ? AND ?';
    const params = [s, e];
    if (store && store !== 'All') { where += ' AND a.store = ?'; params.push(store); }
    if (seller && seller !== 'All') { where += ' AND a.seller = ?'; params.push(seller); }
    const rows = await q(`
      SELECT p.asin, a.store as brand, a.seller,
        SUM(p.salesOrganic + p.salesPPC) as revenue,
        SUM(p.netProfit) as netProfit,
        SUM(p.unitsOrganic + p.unitsPPC) as units,
        AVG(p.margin) as margin, AVG(p.realACOS) as acos,
        SUM(p.sessions) as sessions, AVG(p.unitSessionPercentage) as cr
      FROM seller_board_product p
      LEFT JOIN asin a ON p.asin = a.asin
      ${where}
      GROUP BY p.asin, a.store, a.seller
      ORDER BY netProfit DESC LIMIT ?
    `, [...params, parseInt(lim) || 200]);
    res.json(rows.map(r => ({
      asin: r.asin, brand: r.brand || '', seller: r.seller || '',
      revenue: parseFloat(r.revenue) || 0, netProfit: parseFloat(r.netProfit) || 0,
      units: parseInt(r.units) || 0, margin: parseFloat(r.margin) || 0,
      acos: parseFloat(r.acos) || 0, cr: parseFloat(r.cr) || 0,
      sessions: parseFloat(r.sessions) || 0,
    })));
  } catch (e) { res.status(500).json({ error: e.message }); }
});

/* ═══════════ INVENTORY ═══════════ */
app.get('/api/inventory/snapshot', async (req, res) => {
  try {
    // Inventory planning data (matches DAX: Available, Reserved, Critical SKUs, Age buckets)
    const inv = await q(`
      SELECT
        SUM(CAST(available AS SIGNED)) as availableInv,
        SUM(COALESCE(totalReservedQuantity,0)) as reserved,
        SUM(COALESCE(inboundQuantity,0)) as inbound,
        SUM(COALESCE(inventorySupplyAtFBA,0)) as totalInvAtFBA,
        SUM(CASE WHEN daysOfSupply <= 7 THEN 1 ELSE 0 END) as criticalSkus,
        SUM(COALESCE(invAge0To90Days,0)) as age0_90,
        SUM(COALESCE(invAge91To180Days,0)) as age91_180,
        SUM(COALESCE(invAge181To270Days,0)) as age181_270,
        SUM(COALESCE(invAge271To365Days,0)) as age271_365,
        SUM(COALESCE(invAge365PlusDays,0)) as age365plus,
        SUM(COALESCE(estimatedStorageCostNextMonth,0)) as storageFee,
        AVG(COALESCE(daysOfSupply,0)) as avgDaysOfSupply
      FROM fba_iventory_planning
      WHERE date = (SELECT MAX(date) FROM fba_iventory_planning)
    `);
    // FBA Stock from seller_board_stock (NO date column - it's a snapshot table)
    let stock = {};
    try {
      const s = await q(`
        SELECT SUM(FBAStock) as fbaStock, SUM(COALESCE(stockValue,0)) as stockValue
        FROM seller_board_stock
      `);
      stock = s[0] || {};
    } catch(e) {}
    const r = inv[0] || {};
    const units90 = (parseInt(r.age91_180)||0)+(parseInt(r.age181_270)||0)+(parseInt(r.age271_365)||0)+(parseInt(r.age365plus)||0);
    res.json({
      fbaStock: parseInt(stock.fbaStock) || 0,
      stockValue: parseFloat(stock.stockValue) || 0,
      availableInv: parseInt(r.availableInv) || 0,
      totalInvAtFBA: parseInt(r.totalInvAtFBA) || 0,
      reserved: parseInt(r.reserved) || 0,
      inbound: parseInt(r.inbound) || 0,
      criticalSkus: parseInt(r.criticalSkus) || 0,
      age0_90: parseInt(r.age0_90) || 0,
      age91_180: parseInt(r.age91_180) || 0,
      age181_270: parseInt(r.age181_270) || 0,
      age271_365: parseInt(r.age271_365) || 0,
      age365plus: parseInt(r.age365plus) || 0,
      units90plus: units90,
      agedPct: (parseInt(r.availableInv)||1) > 0 ? (units90 / parseInt(r.availableInv) * 100) : 0,
      storageFee: parseFloat(r.storageFee) || 0,
      avgDaysOfSupply: parseFloat(r.avgDaysOfSupply) || 0,
    });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.get('/api/inventory/stock-trend', async (req, res) => {
  try {
    const rows = await q(`
      SELECT date, SUM(FBAStock) as fbaStock, AVG(estimatedSalesVelocity) as velocity
      FROM seller_board_stock_daily
      WHERE date >= DATE_SUB(CURDATE(), INTERVAL 60 DAY)
      GROUP BY date ORDER BY date
    `);
    res.json(rows);
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.get('/api/inventory/by-shop', async (req, res) => {
  try {
    const shopMap = await getShopMap();
    const rows = await q(`
      SELECT f.accountId,
        SUM(CAST(f.available AS SIGNED)) as fbaStock,
        SUM(COALESCE(f.inboundQuantity,0)) as inbound,
        SUM(COALESCE(f.totalReservedQuantity,0)) as reserved,
        SUM(CASE WHEN f.daysOfSupply <= 7 AND CAST(f.available AS SIGNED) > 0 THEN 1 ELSE 0 END) as criticalSkus,
        AVG(COALESCE(f.sellThrough,0)) as sellThrough,
        AVG(COALESCE(f.daysOfSupply,0)) as daysOfSupply
      FROM fba_iventory_planning f
      WHERE f.date = (SELECT MAX(date) FROM fba_iventory_planning)
      GROUP BY f.accountId ORDER BY fbaStock DESC
    `);
    res.json(rows.map(r => ({
      shop: shopMap[r.accountId] || `Account ${r.accountId}`,
      fbaStock: parseInt(r.fbaStock) || 0, inbound: parseInt(r.inbound) || 0,
      reserved: parseInt(r.reserved) || 0, criticalSkus: parseInt(r.criticalSkus) || 0,
      sellThrough: parseFloat(r.sellThrough) || 0, daysOfSupply: parseFloat(r.daysOfSupply) || 0,
    })));
  } catch (e) { res.status(500).json({ error: e.message }); }
});

/* ═══════════ ASIN PLAN ═══════════ */
// Metric name mapping from asin_plan table → internal keys
const planMetricMap = {
  'grossProfit': 'gp', 'gross_profit': 'gp', 'gp': 'gp', 'GP': 'gp',
  'revenue': 'rv', 'Revenue': 'rv', 'rv': 'rv', 'sales': 'rv',
  'ads': 'ad', 'Ads': 'ad', 'ad': 'ad', 'adSpend': 'ad', 'ad_spend': 'ad',
  'units': 'un', 'Units': 'un', 'un': 'un',
  'sessions': 'se', 'Sessions': 'se', 'se': 'se',
  'impressions': 'im', 'Impressions': 'im', 'im': 'im',
  'cvr': 'cr', 'CVR': 'cr', 'cr': 'cr', 'conversionRate': 'cr', 'unitSessionPercentage': 'cr',
  'ctr': 'ct', 'CTR': 'ct', 'ct': 'ct', 'clickThroughRate': 'ct',
};
const MS_NAMES = ['Jan','Feb','Mar','Apr','May','Jun','Jul','Aug','Sep','Oct','Nov','Dec'];

// plan/data — returns aggregated KPI cards: {gp:{a,p}, rv:{a,p}, ...}
app.get('/api/plan/data', async (req, res) => {
  try {
    const { year, month, brand, seller, asin: asinFilter } = req.query;
    const yr = parseInt(year) || new Date().getFullYear();

    // 1. Get plan values from asin_plan (pivoted)
    let planWhere = 'WHERE 1=1';
    const planParams = [];
    if (month && month !== 'All') {
      let mi = parseInt(month);
      if (isNaN(mi)) mi = MS_NAMES.indexOf(month) + 1;
      if (mi > 0) { planWhere += ' AND ap.month_num = ?'; planParams.push(mi); }
    }
    if (brand && brand !== 'All') { planWhere += ' AND ap.brand_name = ?'; planParams.push(brand); }
    if (asinFilter && asinFilter !== 'All') { planWhere += ' AND ap.asin = ?'; planParams.push(asinFilter); }
    if (seller && seller !== 'All') { planWhere += ' AND a.seller = ?'; planParams.push(seller); }
    const planRows = await q(`
      SELECT ap.metrics, SUM(ap.value) as total
      FROM asin_plan ap LEFT JOIN asin a ON ap.asin = a.asin
      ${planWhere}
      GROUP BY ap.metrics
    `, planParams);

    const planTotals = {};
    planRows.forEach(r => {
      const key = planMetricMap[r.metrics] || planMetricMap[r.metrics?.toLowerCase()];
      if (key) planTotals[key] = (planTotals[key] || 0) + (parseFloat(r.total) || 0);
    });

    // 2. Get actuals from seller_board_product
    let startDate, endDate;
    if (month && month !== 'All') {
      let mi = parseInt(month);
      if (isNaN(mi)) mi = MS_NAMES.indexOf(month) + 1;
      startDate = `${yr}-${String(mi).padStart(2,'0')}-01`;
      endDate = `${yr}-${String(mi).padStart(2,'0')}-${new Date(yr, mi, 0).getDate()}`;
    } else { startDate = `${yr}-01-01`; endDate = `${yr}-12-31`; }

    let actWhere = 'WHERE p.date BETWEEN ? AND ?';
    const actParams = [startDate, endDate];
    if (brand && brand !== 'All') { actWhere += ' AND a.store = ?'; actParams.push(brand); }
    if (seller && seller !== 'All') { actWhere += ' AND a.seller = ?'; actParams.push(seller); }
    if (asinFilter && asinFilter !== 'All') { actWhere += ' AND p.asin = ?'; actParams.push(asinFilter); }

    const actRows = await q(`
      SELECT
        SUM(COALESCE(p.grossProfit,0)) as gp,
        SUM(p.salesOrganic + p.salesPPC) as rv,
        SUM(COALESCE(p.sponsoredProducts,0)+COALESCE(p.sponsoredDisplay,0)+COALESCE(p.sponsoredBrands,0)+COALESCE(p.sponsoredBrandsVideo,0)+COALESCE(p.googleAds,0)+COALESCE(p.facebookAds,0)) as ad,
        SUM(p.unitsOrganic + p.unitsPPC) as un,
        SUM(COALESCE(p.sessions,0)) as se,
        0 as im,
        AVG(NULLIF(p.unitSessionPercentage,0)) as cr,
        0 as ct
      FROM seller_board_product p LEFT JOIN asin a ON p.asin = a.asin
      ${actWhere}
    `, actParams);

    const act = actRows[0] || {};
    const result = {};
    ['gp','rv','ad','un','se','im','cr','ct'].forEach(k => {
      result[k] = { a: parseFloat(act[k]) || 0, p: planTotals[k] || 0 };
    });
    res.json(result);
  } catch (e) { res.status(500).json({ error: e.message }); }
});

// plan/actuals — returns {monthly:[...], asinBreakdown:[...]}
app.get('/api/plan/actuals', async (req, res) => {
  try {
    const { year, month, brand, seller, asin: asinFilter } = req.query;
    const yr = parseInt(year) || new Date().getFullYear();
    let startDate, endDate;
    if (month && month !== 'All') {
      let mi = parseInt(month);
      if (isNaN(mi)) mi = MS_NAMES.indexOf(month) + 1;
      startDate = `${yr}-${String(mi).padStart(2,'0')}-01`;
      endDate = `${yr}-${String(mi).padStart(2,'0')}-${new Date(yr, mi, 0).getDate()}`;
    } else { startDate = `${yr}-01-01`; endDate = `${yr}-12-31`; }

    // Actuals by month from seller_board_product
    let actWhere = 'WHERE p.date BETWEEN ? AND ?';
    const actParams = [startDate, endDate];
    if (brand && brand !== 'All') { actWhere += ' AND a.store = ?'; actParams.push(brand); }
    if (seller && seller !== 'All') { actWhere += ' AND a.seller = ?'; actParams.push(seller); }
    if (asinFilter && asinFilter !== 'All') { actWhere += ' AND p.asin = ?'; actParams.push(asinFilter); }

    const actByMonth = await q(`
      SELECT MONTH(p.date) as mn,
        SUM(COALESCE(p.grossProfit,0)) as gp,
        SUM(p.salesOrganic + p.salesPPC) as rv,
        SUM(COALESCE(p.sponsoredProducts,0)+COALESCE(p.sponsoredDisplay,0)+COALESCE(p.sponsoredBrands,0)+COALESCE(p.sponsoredBrandsVideo,0)+COALESCE(p.googleAds,0)+COALESCE(p.facebookAds,0)) as ad,
        SUM(p.unitsOrganic + p.unitsPPC) as un,
        SUM(COALESCE(p.sessions,0)) as se,
        0 as im,
        AVG(NULLIF(p.unitSessionPercentage,0)) as cr,
        0 as ct
      FROM seller_board_product p LEFT JOIN asin a ON p.asin = a.asin
      ${actWhere} GROUP BY MONTH(p.date) ORDER BY mn
    `, actParams);

    // Plan by month from asin_plan
    let planWhere = 'WHERE 1=1';
    const planParams = [];
    if (brand && brand !== 'All') { planWhere += ' AND ap.brand_name = ?'; planParams.push(brand); }
    if (seller && seller !== 'All') { planWhere += ' AND a.seller = ?'; planParams.push(seller); }
    if (asinFilter && asinFilter !== 'All') { planWhere += ' AND ap.asin = ?'; planParams.push(asinFilter); }
    const planByMonth = await q(`
      SELECT ap.month_num as mn, ap.metrics, SUM(ap.value) as val
      FROM asin_plan ap LEFT JOIN asin a ON ap.asin = a.asin
      ${planWhere} GROUP BY ap.month_num, ap.metrics ORDER BY mn
    `, planParams);

    // Pivot plan data by month
    const planMap = {};
    planByMonth.forEach(r => {
      const mn = r.mn;
      if (!planMap[mn]) planMap[mn] = {};
      const key = planMetricMap[r.metrics] || planMetricMap[r.metrics?.toLowerCase()];
      if (key) planMap[mn][key] = (planMap[mn][key] || 0) + (parseFloat(r.val) || 0);
    });

    // Merge actual + plan by month
    const allMonths = new Set([
      ...actByMonth.map(r => r.mn),
      ...Object.keys(planMap).map(Number)
    ]);
    const monthly = [...allMonths].sort((a,b) => a-b).map(mn => {
      const act = actByMonth.find(r => r.mn === mn) || {};
      const plan = planMap[mn] || {};
      return {
        m: MS_NAMES[mn - 1] || `M${mn}`,
        gpa: parseFloat(act.gp) || null, gpp: plan.gp || 0,
        ra: parseFloat(act.rv) || null, rp: plan.rv || 0,
        aa: parseFloat(act.ad) || null, ap: plan.ad || 0,
        ua: parseFloat(act.un) || null, up: plan.un || 0,
        sa: parseFloat(act.se) || null, sp: plan.se || 0,
        ia: parseFloat(act.im) || null, ip: plan.im || 0,
        cra: parseFloat(act.cr) || null, crp: plan.cr || 0,
        cta: parseFloat(act.ct) || null, ctp: plan.ct || 0,
      };
    });

    // ASIN Breakdown — actuals by asin + plan by asin
    const actByAsin = await q(`
      SELECT p.asin, a.store as brand,
        SUM(COALESCE(p.grossProfit,0)) as gp,
        SUM(p.salesOrganic + p.salesPPC) as rv,
        SUM(COALESCE(p.sponsoredProducts,0)+COALESCE(p.sponsoredDisplay,0)+COALESCE(p.sponsoredBrands,0)+COALESCE(p.sponsoredBrandsVideo,0)+COALESCE(p.googleAds,0)+COALESCE(p.facebookAds,0)) as ad,
        SUM(p.unitsOrganic + p.unitsPPC) as un,
        SUM(COALESCE(p.sessions,0)) as se,
        0 as im,
        AVG(NULLIF(p.unitSessionPercentage,0)) as cr,
        0 as ct
      FROM seller_board_product p LEFT JOIN asin a ON p.asin = a.asin
      ${actWhere} GROUP BY p.asin, a.store ORDER BY gp DESC LIMIT 200
    `, actParams);

    // Plan by asin (month filter if applicable)
    let planAsinWhere = planWhere;
    const planAsinParams = [...planParams];
    if (month && month !== 'All') {
      let mi = parseInt(month);
      if (isNaN(mi)) mi = MS_NAMES.indexOf(month) + 1;
      if (mi > 0) { planAsinWhere += ' AND ap.month_num = ?'; planAsinParams.push(mi); }
    }
    const planByAsin = await q(`
      SELECT ap.asin, ap.brand_name, ap.metrics, SUM(ap.value) as val
      FROM asin_plan ap LEFT JOIN asin a ON ap.asin = a.asin
      ${planAsinWhere} GROUP BY ap.asin, ap.brand_name, ap.metrics
    `, planAsinParams);

    const planAsinMap = {};
    planByAsin.forEach(r => {
      if (!planAsinMap[r.asin]) planAsinMap[r.asin] = { brand: r.brand_name };
      const key = planMetricMap[r.metrics] || planMetricMap[r.metrics?.toLowerCase()];
      if (key) planAsinMap[r.asin][key] = (planAsinMap[r.asin][key] || 0) + (parseFloat(r.val) || 0);
    });

    // Merge
    const allAsins = new Set([
      ...actByAsin.map(r => r.asin),
      ...Object.keys(planAsinMap)
    ]);
    const asinBreakdown = [...allAsins].map(asin => {
      const act = actByAsin.find(r => r.asin === asin) || {};
      const plan = planAsinMap[asin] || {};
      return {
        br: act.brand || plan.brand || '', a: asin,
        ga: parseFloat(act.gp) || null, gp: plan.gp || 0,
        ra: parseFloat(act.rv) || null, rp: plan.rv || 0,
        aa: parseFloat(act.ad) || null, ap: plan.ad || 0,
        ua: parseFloat(act.un) || null, up: plan.un || 0,
        sa: parseFloat(act.se) || null, sp: plan.se || 0,
        ia: parseFloat(act.im) || null, ip: plan.im || 0,
        cra: parseFloat(act.cr) || null, crp: plan.cr || 0,
        cta: parseFloat(act.ct) || null, ctp: plan.ct || 0,
      };
    }).sort((a,b) => (b.ga||0) - (a.ga||0)).slice(0, 200);

    res.json({ monthly, asinBreakdown });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

/* ═════════════════════════════════════════════════════
   PRODUCT PERFORMANCE — per ASIN from seller_board_product
   ═════════════════════════════════════════════════════ */
app.get('/api/product/asins', async (req, res) => {
  try {
    const { start, end, brand, seller, asin: asinFilter } = req.query;
    const s = start || '2026-01-01', e = end || '2026-01-31';
    let where = 'WHERE p.date BETWEEN ? AND ?';
    const params = [s, e];
    if (brand && brand !== 'All') { where += ' AND a.store = ?'; params.push(brand); }
    if (seller && seller !== 'All') { where += ' AND a.seller = ?'; params.push(seller); }
    if (asinFilter && asinFilter !== 'All') { where += ' AND p.asin = ?'; params.push(asinFilter); }
    const rows = await q(`
      SELECT p.asin, a.store as brand, a.seller,
        SUM(p.salesOrganic + p.salesPPC) as revenue,
        SUM(p.netProfit) as netProfit,
        SUM(p.unitsOrganic + p.unitsPPC) as units,
        AVG(p.margin) as margin, AVG(p.realACOS) as acos,
        SUM(p.sessions) as sessions, AVG(p.unitSessionPercentage) as cr
      FROM seller_board_product p LEFT JOIN asin a ON p.asin = a.asin
      ${where} GROUP BY p.asin, a.store, a.seller ORDER BY revenue DESC LIMIT 500
    `, params);
    res.json(rows.map(r => {
      const rev = parseFloat(r.revenue) || 0, np = parseFloat(r.netProfit) || 0;
      return {
        asin: r.asin, brand: r.brand || '', seller: r.seller || '',
        revenue: rev, netProfit: np, units: parseInt(r.units) || 0,
        margin: rev > 0 ? (np / rev * 100) : 0,
        acos: parseFloat(r.acos) || 0,
        roas: parseFloat(r.acos) > 0 ? (100 / parseFloat(r.acos)) : 0,
        cr: parseFloat(r.cr) || 0, sessions: parseFloat(r.sessions) || 0,
      };
    }));
  } catch (e) { res.status(500).json({ error: e.message }); }
});

/* ═══════════════════════════════════════════════
   SHOP PERFORMANCE — from seller_board_sales (dedup)
   ═══════════════════════════════════════════════ */
app.get('/api/shops', async (req, res) => {
  try {
    const { start, end, store, seller } = req.query;
    const s = start || '2026-01-01', e = end || '2026-01-31';
    const shopMap = await getShopMap();
    const hasSeller = seller && seller !== 'All';

    let rows;
    if (hasSeller) {
      // Use seller_board_product joined with asin to filter by seller, grouped by accountId
      let where = 'WHERE p.date BETWEEN ? AND ? AND a.seller = ?';
      const params = [s, e, seller];
      if (store && store !== 'All') {
        const accId = Object.entries(shopMap).find(([k, v]) => v === store)?.[0];
        if (accId) { where += ' AND p.accountId = ?'; params.push(parseInt(accId)); }
      }
      rows = await q(`
        SELECT p.accountId,
          SUM(p.salesOrganic + p.salesPPC) as revenue,
          SUM(p.netProfit) as netProfit,
          SUM(p.unitsOrganic + p.unitsPPC) as units,
          0 as orders
        FROM seller_board_product p LEFT JOIN asin a ON p.asin = a.asin
        ${where} GROUP BY p.accountId ORDER BY revenue DESC
      `, params);
    } else {
      let having = '';
      const params = [s, e];
      if (store && store !== 'All') {
        const accId = Object.entries(shopMap).find(([k, v]) => v === store)?.[0];
        if (accId) having = ` HAVING d.accountId = ${parseInt(accId)}`;
      }
      rows = await q(`
        SELECT d.accountId,
          SUM(d.sales) as revenue, SUM(d.netProfit) as netProfit,
          SUM(d.units) as units, SUM(d.orders) as orders
        FROM ${salesUnion()} d
        WHERE d.date BETWEEN ? AND ?
        GROUP BY d.accountId ${having} ORDER BY revenue DESC
      `, params);
    }

    // FBA stock from seller_board_stock (no date column - snapshot table)
    let stockMap = {};
    try {
      const stocks = await q(`
        SELECT accountId, SUM(FBAStock) as fbaStock, SUM(COALESCE(stockValue,0)) as stockValue
        FROM seller_board_stock GROUP BY accountId
      `);
      stocks.forEach(ss => { stockMap[ss.accountId] = { fba: ss.fbaStock, sv: ss.stockValue }; });
    } catch (ex) {}

    res.json(rows.map(r => {
      const rev = parseFloat(r.revenue) || 0, np = parseFloat(r.netProfit) || 0;
      return {
        shop: shopMap[r.accountId] || `Account ${r.accountId}`,
        revenue: rev, netProfit: np, units: parseInt(r.units) || 0,
        orders: parseInt(r.orders) || 0, margin: rev > 0 ? (np / rev * 100) : 0,
        fbaStock: parseInt(stockMap[r.accountId]?.fba) || 0,
        stockValue: parseFloat(stockMap[r.accountId]?.sv) || 0,
      };
    }));
  } catch (e) { res.status(500).json({ error: e.message }); }
});

/* ═══════════════════════════════════════════════
   TEAM PERFORMANCE — from seller_board_product (need seller from asin table)
   ═══════════════════════════════════════════════ */
app.get('/api/team', async (req, res) => {
  try {
    const { start, end, seller } = req.query;
    const s = start || '2026-01-01', e = end || '2026-01-31';
    let where = 'WHERE p.date BETWEEN ? AND ? AND a.seller IS NOT NULL AND a.seller != ""';
    const params = [s, e];
    if (seller && seller !== 'All') { where += ' AND a.seller = ?'; params.push(seller); }
    const rows = await q(`
      SELECT a.seller,
        SUM(p.salesOrganic + p.salesPPC) as revenue,
        SUM(p.netProfit) as netProfit,
        SUM(p.unitsOrganic + p.unitsPPC) as units,
        COUNT(DISTINCT p.asin) as asinCount
      FROM seller_board_product p LEFT JOIN asin a ON p.asin = a.asin
      ${where} GROUP BY a.seller ORDER BY revenue DESC
    `, params);
    res.json(rows.map(r => {
      const rev = parseFloat(r.revenue) || 0, np = parseFloat(r.netProfit) || 0;
      return { seller: r.seller, revenue: rev, netProfit: np, units: parseInt(r.units) || 0,
        margin: rev > 0 ? (np / rev * 100) : 0, asinCount: parseInt(r.asinCount) || 0 };
    }));
  } catch (e) { res.status(500).json({ error: e.message }); }
});

/* ═══════════════════════════════════════════════
   DAILY / OPS — from seller_board_day (matches PBI "DR" measures)
   ═══════════════════════════════════════════════ */
app.get('/api/ops/daily', async (req, res) => {
  try {
    const { start, end, store } = req.query;
    const s = start || '2026-01-01', e = end || '2026-01-31';
    let extraWhere = '';
    const params = [s, e];
    if (store && store !== 'All') {
      const shopMap = await getShopMap();
      const accId = Object.entries(shopMap).find(([k, v]) => v === store)?.[0];
      if (accId) { extraWhere = ' AND accountId = ?'; params.push(parseInt(accId)); }
    }
    let rows = await q(`
      SELECT date, SUM(salesOrganic + salesPPC) as revenue, SUM(netProfit) as netProfit,
        SUM(unitsOrganic + unitsPPC) as units, SUM(orders) as orders,
        SUM(COALESCE(sponsoredProducts,0)+COALESCE(sponsoredDisplay,0)+COALESCE(sponsoredBrands,0)+COALESCE(sponsoredBrandsVideo,0)+COALESCE(googleAds,0)+COALESCE(facebookAds,0)) as adSpend
      FROM seller_board_day WHERE date BETWEEN ? AND ? ${extraWhere}
      GROUP BY date ORDER BY date DESC LIMIT 30
    `, params);
    if (!rows || rows.length === 0) {
      const p2 = [s, e];
      let ew2 = '';
      if (store && store !== 'All') {
        const shopMap = await getShopMap();
        const accId = Object.entries(shopMap).find(([k, v]) => v === store)?.[0];
        if (accId) { ew2 = ' AND d.accountId = ?'; p2.push(parseInt(accId)); }
      }
      rows = await q(`SELECT d.date, SUM(d.sales) as revenue, SUM(d.netProfit) as netProfit,
        SUM(d.units) as units, SUM(d.orders) as orders, SUM(d.adSpend) as adSpend
        FROM ${salesUnion()} d WHERE d.date BETWEEN ? AND ? ${ew2}
        GROUP BY d.date ORDER BY d.date DESC LIMIT 30`, p2);
    }
    res.json(rows);
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.get('/api/ops/by-shop', async (req, res) => {
  try {
    const { start, end, store } = req.query;
    const s = start || '2026-01-01', e = end || '2026-01-31';
    const shopMap = await getShopMap();
    let having = '';
    if (store && store !== 'All') {
      const accId = Object.entries(shopMap).find(([k, v]) => v === store)?.[0];
      if (accId) having = ` HAVING d.accountId = ${parseInt(accId)}`;
    }
    const rows = await q(`
      SELECT d.accountId,
        SUM(d.sales) as revenue, SUM(d.netProfit) as netProfit,
        SUM(d.orders) as orders, SUM(d.adSpend) as adSpend
      FROM ${salesUnion()} d
      WHERE d.date BETWEEN ? AND ?
      GROUP BY d.accountId ${having} ORDER BY revenue DESC
    `, [s, e]);
    res.json(rows.map(r => ({
      shop: shopMap[r.accountId] || `Account ${r.accountId}`,
      revenue: parseFloat(r.revenue) || 0, netProfit: parseFloat(r.netProfit) || 0,
      orders: parseInt(r.orders) || 0, adSpend: parseFloat(r.adSpend) || 0,
    })));
  } catch (e) { res.status(500).json({ error: e.message }); }
});

/* ═══════════ AI INSIGHT ═══════════ */
app.post('/api/ai/insight', async (req, res) => {
  try {
    const apiKey = process.env.ANTHROPIC_API_KEY;
    if (!apiKey) return res.status(400).json({ error: 'ANTHROPIC_API_KEY not configured in .env' });
    const { context, question } = req.body;
    const systemPrompt = `You are an expert Amazon FBA business analyst for Expeditee LLC, an e-commerce holding company managing 32+ brands.
Analyze the provided data and give actionable business insights. Focus on:
1. Revenue & profit drivers and drags
2. ASIN-level performance issues (high ACoS, negative margin, poor CR)
3. Inventory health concerns
4. Recommendations with specific actions
Be specific with numbers. Keep to 300-500 words. Respond in English.`;
    const response = await fetch('https://api.anthropic.com/v1/messages', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', 'x-api-key': apiKey, 'anthropic-version': '2023-06-01' },
      body: JSON.stringify({
        model: 'claude-sonnet-4-20250514', max_tokens: 1500, system: systemPrompt,
        messages: [{ role: 'user', content: `Dashboard data:\n${JSON.stringify(context, null, 2)}\n\n${question || 'Analyze this data and provide key insights.'}` }],
      }),
    });
    const data = await response.json();
    res.json({ insight: data.content?.[0]?.text || 'Unable to generate insight' });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

/* ═══════════ SERVE FRONTEND ═══════════ */
const distPath = join(__dirname, 'dist');
app.use(express.static(distPath));
app.get('*', (req, res) => { res.sendFile(join(distPath, 'index.html')); });

app.listen(PORT, '0.0.0.0', () => {
  console.log(`\n🚀 Amazon Dashboard on http://localhost:${PORT}`);
  console.log(`   Database: ${process.env.DB_HOST || 'not configured'}`);
  console.log(`   AI Insight: ${process.env.ANTHROPIC_API_KEY ? 'enabled' : 'not configured'}`);
  console.log(`\n   📊 Data sources (matching PBI DAX):`);
  console.log(`      Shop/Total   → seller_board_sales + seller_board_sales_old (dedup)`);
  console.log(`      Daily (DR)   → seller_board_day`);
  console.log(`      ASIN-level   → seller_board_product`);
  console.log(`      FBA Stock    → seller_board_stock + seller_board_stock_daily`);
  console.log(`      Inventory    → fba_iventory_planning`);
  console.log(`      Plan         → asin_plan\n`);
});
