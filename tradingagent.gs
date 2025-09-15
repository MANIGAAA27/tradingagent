/** ============================================================================
 * AI Trading Agent — Full Pipeline (Staging → MarketCache → AI Scoring)
 * - Source list: https://www.nasdaqtrader.com/dynamic/symdir/nasdaqlisted.txt
 * - Background worker (chunked, incremental) with lock & status
 * - Command Center sidebar (buttons + live progress)
 * - Exports ONLY qualified rows to MarketCache (no dupes)
 * - AI Agent reads MarketCache, scores, and emits trade signals
 * ========================================================================== */

/** -------------------- CONFIG: Background/Staging/Export ------------------- */
const NASDAQ_URL = 'https://www.nasdaqtrader.com/dynamic/symdir/nasdaqlisted.txt';

const BG_CFG = {
  stagingName: 'Staging_Nasdaq',
  marketCacheName: 'MarketCache',
  chunkSize: 1200, // symbols per pass; adjust for speed vs. throttling
  // Staging columns (with formulas; do not reorder)
  stagingHeader: [
    'Ticker','Company','Price','Volume','$Volume','AvgVol(10D)','RVOL',
    'Prev Close','Change %','52W High','Dist to High %','Category','Qualified','Exported'
  ],
  // MarketCache columns the AI expects (keep first 10 exactly as below)
  marketCacheHeader: [
    'Ticker','Company','Price','Volume','$Volume','AvgVol(10D)','RVOL',
    'Prev Close','Change %','52W High','Dist to High %','Category','Exported At'
  ],
  // PropertiesService keys
  stateKey:   'BG_NASDAQ_STATE',
  symbolsKey: 'BG_NASDAQ_SYMBOLS',
};

const STATUS_KEY = 'BG_STATUS'; // sidebar status blob key

/** --------------------------- CONFIG: AI Agent ----------------------------- */
const AI_CONFIG = {
  targets: {
    minDailyMove: 1,
    idealDailyMove: 3,
    maxDailyMove: 5
  },
  risk: {
    maxRiskPerTrade: 2,
    stopLossPercent: 2,
    trailingStop: 1.5
  },
  // These should mirror your "Qualified" gate in staging/export
  criteria: {
    minPrice: 5,
    maxPrice: 100,
    minVolume: 2000000,
    minRVOL: 1.5,
    minATR: 1.5 // placeholder
  },
  weights: {
    momentum: 0.25,
    volume: 0.25,
    technical: 0.20,
    sentiment: 0.15,
    risk: 0.15
  }
};

/** =============================== MENUS/UI ================================ */
function onOpen() {
  const ui = SpreadsheetApp.getUi();
  ui.createMenu('🤖 AI Agent')
    .addItem('Command Center (Sidebar)', 'openCommandCenter')
    .addSeparator()
    .addItem('Start Background (5 min)', 'startBackgroundScanner')
    .addItem('Stop Background', 'stopBackgroundScanner')
    .addItem('Run Scan Once', 'runScanOnce')
    .addItem('Run Full Now (best effort)', 'runFullProcessNow')
    .addSeparator()
    .addItem('Run AI Scoring Now', 'runAITradingAgent')
    .addItem('Debug Status', 'debugBackgroundState')
    .addSeparator()
  .addItem('Reset Scanner (soft)', 'resetScannerStateSoft')
  .addItem('Hard Reset (rebuild)', 'hardResetScanner')
  .addItem('Reset + Run Full Now', 'resetAndRunFullNow')
    .addToUi();
}

function openCommandCenter() {
  const html = HtmlService.createHtmlOutput(`
    <div style="font:13px/1.4 Arial;padding:14px;width:320px">
      <h2 style="margin:0 0 8px">AI Agent — Command Center</h2>
      <div id="status" style="border:1px solid #ddd;padding:10px;border-radius:8px;background:#fafafa">
        <div><b>Step:</b> <span id="step">—</span></div>
        <div><b>Progress:</b> <span id="progress">0 / 0 (0%)</span></div>
        <div style="height:8px;background:#eee;border-radius:4px;margin:6px 0 10px">
          <div id="bar" style="height:8px;width:0%;background:#4caf50;border-radius:4px"></div>
        </div>
        <div><b>Staging</b> rows: <span id="staging">0</span></div>
        <div>Ready: <span id="ready">0</span> • Qualified: <span id="qualified">0</span> • Exported: <span id="exported">0</span></div>
        <div><b>MarketCache</b> rows: <span id="mc">0</span></div>
        <div>Trigger: <span id="trigger">—</span></div>
        <div>Last run: <span id="lastrun">—</span></div>
        <div style="color:#b00020" id="err"></div>
      </div>
      <div style="margin-top:12px;display:flex;gap:6px;flex-wrap:wrap">
        <button onclick="start()" style="padding:6px 10px">Start Background</button>
        <button onclick="stop()"  style="padding:6px 10px">Stop Background</button>
        <button onclick="once()"  style="padding:6px 10px">Run Scan Once</button>
        <button onclick="full()"  style="padding:6px 10px">Run Full Now</button>
        <button onclick="refresh()" style="padding:6px 10px">Refresh</button>
      </div>
      <p style="margin-top:10px;color:#666">Tip: “Run Full Now” pumps multiple chunks this session, then runs the AI.</p>
      <script>
        function render(s){
          const pct = s.symbolsTotal ? Math.min(100, Math.round(100*s.nextIndex/Math.max(1,s.symbolsTotal))) : 0;
          document.getElementById('step').textContent = s.step || '—';
          document.getElementById('progress').textContent = (s.nextIndex||0)+' / '+(s.symbolsTotal||0)+' ('+pct+'%)';
          document.getElementById('bar').style.width = pct+'%';
          document.getElementById('staging').textContent = s.stagingRows||0;
          document.getElementById('ready').textContent = s.ready||0;
          document.getElementById('qualified').textContent = s.qualified||0;
          document.getElementById('exported').textContent = s.exported||0;
          document.getElementById('mc').textContent = s.marketCacheRows||0;
          document.getElementById('trigger').textContent = s.hasTrigger ? 'ON' : 'OFF';
          document.getElementById('lastrun').textContent = s.lastRun ? new Date(s.lastRun).toLocaleString() : '—';
          document.getElementById('err').textContent = s.lastError || '';
        }
        function refresh(){ google.script.run.withSuccessHandler(render).getBackgroundStatus(); }
        function start(){ google.script.run.withSuccessHandler(refresh).startBackgroundScanner(); }
        function stop(){  google.script.run.withSuccessHandler(refresh).stopBackgroundScanner(); }
        function once(){  google.script.run.withSuccessHandler(refresh).runScanOnce(); }
        function full(){  google.script.run.withSuccessHandler(refresh).runFullProcessNow(); }
        refresh(); setInterval(refresh, 5000);
      </script>
    </div>
  `).setTitle('AI Agent — Command Center').setWidth(360);
  SpreadsheetApp.getUi().showSidebar(html);
}

/** ==================== BACKGROUND SCHEDULER HELPERS ====================== */
function startBackgroundScanner() {
  ScriptApp.getProjectTriggers().forEach(t => {
    if (t.getHandlerFunction() === 'scanNasdaqInBackground') ScriptApp.deleteTrigger(t);
  });
  ScriptApp.newTrigger('scanNasdaqInBackground').timeBased().everyMinutes(5).create();
  SpreadsheetApp.getActive().toast('Background scanner scheduled (every 5 minutes).');
}

function stopBackgroundScanner() {
  let n = 0;
  ScriptApp.getProjectTriggers().forEach(t => {
    if (t.getHandlerFunction() === 'scanNasdaqInBackground') { ScriptApp.deleteTrigger(t); n++; }
  });
  SpreadsheetApp.getActive().toast('Stopped '+n+' background trigger(s).');
}

function runScanOnce() { scanNasdaqInBackground(); }

/** Best-effort full pass within one execution window, then run AI. */
function runFullProcessNow() {
  const ss = SpreadsheetApp.getActive();
  const props = PropertiesService.getUserProperties();
  const start = Date.now();
  const budgetMs = 5*60*1000 - 15000; // ~5 min with buffer

  updateStatus_({ step:'FULL_INIT', lastRun: new Date().toISOString(), lastError:'' });
  buildNasdaqStaging_(ss, props);

  while (Date.now() - start < budgetMs) {
    updateStatus_({ step:'FULL_ADD_CHUNK' });
    addNextChunkToStaging_(ss, props);
    updateStatus_({ step:'FULL_EXPORT' });
    exportQualifiedToMarketCache_(ss);
    Utilities.sleep(600);
    const state   = JSON.parse(props.getProperty(BG_CFG.stateKey) || '{"nextIndex":0}');
    const symbols = JSON.parse(props.getProperty(BG_CFG.symbolsKey) || '[]');
    if ((state.nextIndex||0) >= symbols.length) break;
  }
  exportQualifiedToMarketCache_(ss);
  updateStatus_({ step:'RUN_AI' });
  runAITradingAgent();
  updateStatus_({ step:'FULL_DONE' });
  appendLog_('FULL_DONE');
}

/** ===================== CORE BACKGROUND WORKER =========================== */
function scanNasdaqInBackground() {
  const lock = LockService.getScriptLock();
  if (!lock.tryLock(5000)) { appendLog_('SKIP_LOCKED'); return; }
  try {
    updateStatus_({ step:'INIT', lastRun: new Date().toISOString(), lastError:'' });
    const ss = SpreadsheetApp.getActive();
    const props = PropertiesService.getUserProperties();

    updateStatus_({ step:'BUILD'  }); buildNasdaqStaging_(ss, props);
    updateStatus_({ step:'ADD'    }); addNextChunkToStaging_(ss, props);
    updateStatus_({ step:'EXPORT' }); exportQualifiedToMarketCache_(ss);

    updateStatus_({ step:'DONE' });
    appendLog_('OK');
  } catch (e) {
    updateStatus_({ step:'ERROR', lastError: String(e && e.message ? e.message : e) });
    appendLog_('ERROR: '+e);
    throw e;
  } finally {
    lock.releaseLock();
  }
}

/** ================= STAGING (init + populate + formulas) ================= */
function buildNasdaqStaging_(ss, props) {
  const sheet = ensureSheet_(ss, BG_CFG.stagingName);

  // Initialize header if missing
  const headerOk = sheet.getLastRow() >= 1 && sheet.getRange(1,1).getValue() === BG_CFG.stagingHeader[0];
  if (!headerOk) {
    sheet.clear();
    sheet.getRange(1,1,1,BG_CFG.stagingHeader.length).setValues([BG_CFG.stagingHeader]);
    sheet.setFrozenRows(1);
    sheet.getRange(1,1,1,BG_CFG.stagingHeader.length).createFilter();
    sheet.autoResizeColumns(1, BG_CFG.stagingHeader.length);
  }

  // Ensure we have symbols cached (first run or after reset)
  if (!props.getProperty(BG_CFG.symbolsKey)) {
    const symbols = fetchNasdaqList_(); // [{ticker,name}]
    props.setProperty(BG_CFG.symbolsKey, JSON.stringify(symbols));
    props.setProperty(BG_CFG.stateKey, JSON.stringify({ nextIndex: 0 }));
    // Pre-lay formulas for first 50 rows
    ensureFormulas_(sheet, 2, 51);
  }
}

/** Adds next chunk of tickers to staging, extends formulas. */
function addNextChunkToStaging_(ss, props) {
  const sheet = ss.getSheetByName(BG_CFG.stagingName);
  if (!sheet) return;

  const symbolsJson = props.getProperty(BG_CFG.symbolsKey);
  if (!symbolsJson) return;
  const symbols = JSON.parse(symbolsJson);

  const state = JSON.parse(props.getProperty(BG_CFG.stateKey) || '{"nextIndex":0}');
  const start = state.nextIndex || 0;
  if (start >= symbols.length) return; // complete

  const end = Math.min(start + BG_CFG.chunkSize, symbols.length);
  const slice = symbols.slice(start, end);

  // Append rows (A:B = Ticker, Company)
  const writeStartRow = sheet.getLastRow() + 1;
  const values = slice.map(s => [s.ticker, s.name]);
  sheet.getRange(writeStartRow, 1, values.length, 2).setValues(values);

  // Extend formulas across newly added rows
  ensureFormulas_(sheet, writeStartRow, writeStartRow + values.length - 1);

  // Advance pointer
  props.setProperty(BG_CFG.stateKey, JSON.stringify({ nextIndex: end }));
}

/** Set/extend R1C1 formulas over [rowStart, rowEnd]. */
function ensureFormulas_(sheet, rowStart, rowEnd) {
  if (rowEnd < rowStart) return;
  const n = rowEnd - rowStart + 1;

  // C: Price
  sheet.getRange(rowStart, 3, n, 1).setFormulaR1C1('=IFERROR(GOOGLEFINANCE(RC1,"price"),)');
  // D: Volume (today)
  sheet.getRange(rowStart, 4, n, 1).setFormulaR1C1('=IFERROR(GOOGLEFINANCE(RC1,"volume"),)');
  // E: $Volume
  sheet.getRange(rowStart, 5, n, 1).setFormulaR1C1('=IF(RC3*RC4="","",RC3*RC4)');
  // F: AvgVol(10D)
  sheet.getRange(rowStart, 6, n, 1).setFormulaR1C1(
    '=IFERROR(AVERAGE(QUERY(GOOGLEFINANCE(RC1,"volume",TODAY()-20,TODAY()),"select Col2 offset 1",0)),"")'
  );
  // G: RVOL
  sheet.getRange(rowStart, 7, n, 1).setFormulaR1C1('=IF(RC6="","",IFERROR(RC4/RC6,))');
 // H: Prev Close (robust: try closeyest; else last non-null close over past 20 workdays)
sheet.getRange(rowStart, 8, n, 1).setFormulaR1C1(
  '=IFERROR(' +
    'GOOGLEFINANCE(RC1,"closeyest"),' +                                  // quick path (may work for many tickers)
    'IFERROR(' +
      'INDEX(' +
        'SORT(QUERY(GOOGLEFINANCE(RC1,"close",WORKDAY(TODAY(),-20),TODAY()),' +
             '"select Col1,Col2 where Col2 is not null",0),1,TRUE),' +    // sort by date asc
        'ROWS(SORT(QUERY(GOOGLEFINANCE(RC1,"close",WORKDAY(TODAY(),-20),TODAY()),' +
             '"select Col1,Col2 where Col2 is not null",0),1,TRUE)) ,2),' + // last row, col 2
      '""' +
    ')' +
  ')'
);

// I: Change % (prefer built-in changepct; fallback to (Price-PrevClose)/PrevClose)
sheet.getRange(rowStart, 9, n, 1).setFormulaR1C1(
  '=IFERROR(' +
    'GOOGLEFINANCE(RC1,"changepct")/100,' +                               // returns e.g. 1.23 → 0.0123
    'IF(OR(RC3="",RC8=""),"",IFERROR((RC3-RC8)/RC8,))' +                  // fallback from current price & prev close
  ')'
);

  // J: 52W High
  sheet.getRange(rowStart,10, n, 1).setFormulaR1C1('=IFERROR(GOOGLEFINANCE(RC1,"high52"),)');
  // K: Dist to High %
  sheet.getRange(rowStart,11, n, 1).setFormulaR1C1('=IF(OR(RC10="",RC3=""),"",IFERROR((RC10-RC3)/RC10,))');

  // L: Category (five-bucket logic)
  sheet.getRange(rowStart,12, n, 1).setFormulaR1C1([
    '=IF(OR(RC3="",RC4="",RC6="",RC7="",RC8="",RC9="",RC10="",RC11=""),"PENDING",',
    ' IF(AND(RC7>=3, RC9>=0.03), "SQUEEZE",',
    '  IF(RC11<=0.02, "BREAKOUT",',
    '   IF(AND(RC7>=2, RC9>=0.01), "MOMENTUM",',
    '    IF(AND(RC9>0, RC7>=1.5), "ACCUMULATION","BUILDING")',
    '   )',
    '  )',
    ' )',
    ')'
  ].join(''));

  // M: Qualified (gate mirrors AI_CONFIG.criteria)
  sheet.getRange(rowStart,13, n, 1).setFormulaR1C1(
    '=IF(AND(RC3>=5, RC3<=100, RC4>=2000000, RC7>=1.5), "✓ QUALIFIED","FILTERED")'
  );

  // N: Exported (blank until exported)
  // number formats
  sheet.getRange(rowStart, 3, n, 1).setNumberFormat('$#,##0.00'); // Price
  sheet.getRange(rowStart, 4, n, 1).setNumberFormat('#,##0');     // Volume
  sheet.getRange(rowStart, 5, n, 1).setNumberFormat('$#,##0');    // $Volume
  sheet.getRange(rowStart, 6, n, 1).setNumberFormat('#,##0');     // AvgVol
  sheet.getRange(rowStart, 7, n, 1).setNumberFormat('0.00');      // RVOL
  sheet.getRange(rowStart, 8, n, 1).setNumberFormat('$#,##0.00'); // Prev Close
  sheet.getRange(rowStart, 9, n, 1).setNumberFormat('0.00%');     // Δ%
  sheet.getRange(rowStart,10, n, 1).setNumberFormat('$#,##0.00'); // 52W High
  sheet.getRange(rowStart,11, n, 1).setNumberFormat('0.00%');     // Dist to High %
}

/** ============== EXPORT: only qualified rows → MarketCache ================= */
function exportQualifiedToMarketCache_(ss) {
  const staging = ss.getSheetByName(BG_CFG.stagingName);
  if (!staging) return;

  const mc = ensureSheet_(ss, BG_CFG.marketCacheName);
  if (mc.getLastRow() < 1) {
    mc.getRange(1,1,1,BG_CFG.marketCacheHeader.length).setValues([BG_CFG.marketCacheHeader]);
    mc.setFrozenRows(1);
    mc.getRange(1,1,1,BG_CFG.marketCacheHeader.length).createFilter();
  }

  const lastRow = staging.getLastRow();
  if (lastRow < 2) return;

  // Existing tickers in MarketCache (for idempotency)
  const mcRows = mc.getLastRow() - 1;
  const existing = mcRows > 0
    ? new Set(mc.getRange(2,1,mcRows,1).getValues().flat().filter(Boolean))
    : new Set();

  // Read all staging rows
  const data = staging.getRange(2, 1, lastRow-1, 14).getValues(); // A:N
  const out = [];
  const toMark = [];

  for (let i = 0; i < data.length; i++) {
    const r = data[i];
    const [ticker, company, price, volume, dollarVol, avgVol, rvol,
           prevClose, chgPct, high52, distHigh, category, qualified, exported] = r;

    if (!ticker) continue;
    if (exported === 'YES') continue;
    if (qualified !== '✓ QUALIFIED') continue;
    if (!category || category === 'PENDING' || category === 'BUILDING') continue;
    if (existing.has(ticker)) { // already exported previously
      toMark.push(2 + i); // just mark exported to avoid future rechecks
      continue;
    }

    out.push([
      ticker, company, price, volume, dollarVol, avgVol, rvol,
      prevClose, chgPct, high52, distHigh, category, new Date()
    ]);
    toMark.push(2 + i);
    existing.add(ticker);
  }

  if (out.length > 0) {
    const destRow = mc.getLastRow() + 1;
    mc.getRange(destRow, 1, out.length, BG_CFG.marketCacheHeader.length).setValues(out);
    // Mark exported rows
    toMark.forEach(ridx => staging.getRange(ridx, 14).setValue('YES'));

    // Formats on MarketCache
    const lr = mc.getLastRow();
    if (lr >= 2) {
      mc.getRange(2,3, lr-1, 1).setNumberFormat('$#,##0.00'); // Price
      mc.getRange(2,4, lr-1, 1).setNumberFormat('#,##0');     // Volume
      mc.getRange(2,5, lr-1, 1).setNumberFormat('$#,##0');    // $Vol
      mc.getRange(2,6, lr-1, 1).setNumberFormat('#,##0');     // AvgVol
      mc.getRange(2,7, lr-1, 1).setNumberFormat('0.00');      // RVOL
      mc.getRange(2,8, lr-1, 1).setNumberFormat('$#,##0.00'); // PrevClose
      mc.getRange(2,9, lr-1, 1).setNumberFormat('0.00%');     // Δ%
      mc.getRange(2,10,lr-1, 1).setNumberFormat('$#,##0.00'); // 52W High
      mc.getRange(2,11,lr-1, 1).setNumberFormat('0.00%');     // Dist to High
    }
  }
}

/** =========================== FETCH + PARSE ============================== */
function fetchNasdaqList_() {
  const res = UrlFetchApp.fetch(NASDAQ_URL, {
    muteHttpExceptions: true,
    followRedirects: true,
    validateHttpsCertificates: true,
    timeout: 30000
  });
  if (res.getResponseCode() < 200 || res.getResponseCode() >= 300) {
    throw new Error('Failed to fetch NASDAQ list: HTTP ' + res.getResponseCode());
  }
  return parseNasdaqListed_(res.getContentText());
}

function parseNasdaqListed_(text) {
  const out = [];
  if (!text) return out;
  const lines = text.trim().split(/\r?\n/);
  for (let i = 1; i < lines.length - 1; i++) {
    const parts = lines[i].split('|');
    // Format: Symbol|Security Name|Market Category|Test Issue|Financial Status|ETF|NextShares
    const sym  = (parts[0] || '').trim();
    const name = (parts[1] || '').trim();
    const isTestIssue = (parts[3] || '').trim() === 'Y';
    const isETF       = (parts[5] || '').trim() === 'Y';
    if (!sym || isTestIssue || isETF) continue;
    if (!/^[A-Z.\-]+$/.test(sym)) continue;
    if (sym.endsWith('W') || sym.endsWith('WS') || sym.endsWith('U')) continue; // warrants/units
    out.push({ ticker: sym, name });
  }
  return out;
}

/** ===================== STATUS + LOGGING + UTILS ========================= */
function getBackgroundStatus() {
  const ss = SpreadsheetApp.getActive();
  const props = PropertiesService.getUserProperties();
  const status  = JSON.parse(props.getProperty(STATUS_KEY) || '{}');
  const state   = JSON.parse(props.getProperty(BG_CFG.stateKey)   || '{"nextIndex":0}');
  const symbols = JSON.parse(props.getProperty(BG_CFG.symbolsKey) || '[]');

  const staging = ss.getSheetByName(BG_CFG.stagingName);
  const mc = ss.getSheetByName(BG_CFG.marketCacheName);

  let stagingRows = 0, ready = 0, qualified = 0, exported = 0;
  if (staging && staging.getLastRow() > 1) {
    stagingRows = staging.getLastRow() - 1;
    const data = staging.getRange(2,1,stagingRows,14).getValues();
    for (const r of data) {
      const cat = r[11], qual = r[12], exp = r[13];
      if (cat && cat !== 'PENDING') ready++;
      if (qual === '✓ QUALIFIED') qualified++;
      if (exp === 'YES') exported++;
    }
  }
  const marketCacheRows = (mc && mc.getLastRow() > 1) ? mc.getLastRow() - 1 : 0;
  const hasTrigger = ScriptApp.getProjectTriggers().some(t => t.getHandlerFunction() === 'scanNasdaqInBackground');

  return {
    step: status.step || '—',
    lastRun: status.lastRun || null,
    lastError: status.lastError || '',
    nextIndex: state.nextIndex || 0,
    symbolsTotal: symbols.length || 0,
    stagingRows, ready, qualified, exported, marketCacheRows,
    hasTrigger
  };
}

function updateStatus_(patch) {
  const props = PropertiesService.getUserProperties();
  const curr = JSON.parse(props.getProperty(STATUS_KEY) || '{}');
  const next = Object.assign({}, curr, patch, { updatedAt: new Date().toISOString() });
  props.setProperty(STATUS_KEY, JSON.stringify(next));
}

function appendLog_(msg) {
  const ss = SpreadsheetApp.getActive();
  const log = ensureSheet_(ss, 'System_Log');
  log.appendRow([new Date(), msg]);
  log.autoResizeColumns(1, 2);
}

function debugBackgroundState() {
  const ss = SpreadsheetApp.getActive();
  const props = PropertiesService.getUserProperties();
  const staging = ss.getSheetByName(BG_CFG.stagingName);
  const mc = ss.getSheetByName(BG_CFG.marketCacheName);

  const state  = JSON.parse(props.getProperty(BG_CFG.stateKey)   || '{"nextIndex":null}');
  const symJ   = props.getProperty(BG_CFG.symbolsKey);
  const symLen = symJ ? JSON.parse(symJ).length : 0;

  let stagingRows = 0, ready = 0, qualified = 0, exported = 0;
  if (staging && staging.getLastRow() > 1) {
    stagingRows = staging.getLastRow() - 1;
    const data = staging.getRange(2,1,stagingRows,14).getValues();
    for (const r of data) {
      const cat = r[11], qual = r[12], exp = r[13];
      if (cat && cat !== 'PENDING') ready++;
      if (qual === '✓ QUALIFIED') qualified++;
      if (exp === 'YES') exported++;
    }
  }
  const mcRows = mc && mc.getLastRow() > 1 ? mc.getLastRow() - 1 : 0;

  const msg = [
    `Symbols cached: ${symLen}`,
    `Next index: ${state.nextIndex}`,
    `Staging rows: ${stagingRows}`,
    `Ready (Category computed): ${ready}`,
    `Qualified (✓): ${qualified}`,
    `Exported (YES): ${exported}`,
    `MarketCache rows: ${mcRows}`
  ].join('\n');
  SpreadsheetApp.getUi().alert('BG Scanner Status', msg, SpreadsheetApp.getUi().ButtonSet.OK);
}

/** Ensure (or create) a sheet by name. */
function ensureSheet_(ss, name) {
  const s = ss.getSheetByName(name);
  return s ? s : ss.insertSheet(name);
}

/** =========================== AI AGENT LAYER ============================= */
/** Main AI run: reads MarketCache → scores → writes AI_Agent & AI_Tracker */
function runAITradingAgent() {
  const ss = SpreadsheetApp.getActive();
  try {
    let agentSheet = ss.getSheetByName('AI_Agent');
    if (agentSheet) ss.deleteSheet(agentSheet);
    agentSheet = ss.insertSheet('AI_Agent');

    const headers = [
      'Rank','Ticker','Company','AI Score','Signal',
      'Current Price','Entry Price','Stop Loss','Target 1','Target 2',
      'Risk/Reward','Expected Move %','Volume Score','Pattern',
      'News Sentiment','Time','Status','Notes'
    ];
    agentSheet.getRange(1,1,1,headers.length).setValues([headers]);
    agentSheet.setFrozenRows(1);

    // Build candidates from MarketCache
    const candidates = scanForCandidates_();
    if (!candidates.length) {
      agentSheet.getRange(2,1).setValue('No qualifying signals found');
      SpreadsheetApp.getUi().alert('No Candidates Found',
        'MarketCache has no rows that meet AI criteria.\nTip: run the scanner first, or loosen gates.',
        SpreadsheetApp.getUi().ButtonSet.OK);
      return;
    }

    // Score, detect pattern, sentiment
    const scored = candidates.map(s => ({
      ...s,
      aiScore: calculateAIScore(s),
      pattern: detectPattern(s),
      sentiment: analyzeSentiment(s)
    })).sort((a,b)=> b.aiScore - a.aiScore);

    const top = scored.slice(0, Math.min(10, scored.length));
    const signals = top.map((stock, i) => generateTradingSignal(stock, i+1));

    if (signals.length) {
      const rows = signals.map(s => [
        s.rank, s.ticker, s.company, s.aiScore, s.signal,
        s.currentPrice, s.entryPrice, s.stopLoss, s.target1, s.target2,
        s.riskReward, s.expectedMove, s.volumeScore, s.pattern,
        s.sentiment, s.time, s.status, s.notes
      ]);
      agentSheet.getRange(2,1,rows.length,headers.length).setValues(rows);
      formatAIAgentSheet_(agentSheet);
      showTopPicks_(signals.slice(0,3));
      logDailyPicks_(signals);
    } else {
      agentSheet.getRange(2,1).setValue('No qualifying signals found');
    }
  } catch (err) {
    SpreadsheetApp.getUi().alert('Error', 'Error running AI Agent: ' + err, SpreadsheetApp.getUi().ButtonSet.OK);
  }
}

/** Build candidate list from MarketCache (defensive re-check) */
function scanForCandidates_() {
  const ss = SpreadsheetApp.getActive();
  const sheet = ss.getSheetByName(BG_CFG.marketCacheName);
  if (!sheet || sheet.getLastRow() < 2) return [];

  const n = sheet.getLastRow() - 1;
  const data = sheet.getRange(2,1,n, BG_CFG.marketCacheHeader.length).getValues();

  const out = [];
  for (const row of data) {
    const [ticker,name,price,volume,dollarVol,avgVol,rvol,prevClose,chgPct,high52,distHigh] = row;
    if (!ticker || !price) continue;

    if (price >= AI_CONFIG.criteria.minPrice &&
        price <= AI_CONFIG.criteria.maxPrice &&
        volume >= AI_CONFIG.criteria.minVolume &&
        rvol >= AI_CONFIG.criteria.minRVOL) {
      out.push({
        ticker: String(ticker),
        name: String(name || ''),
        price: Number(price) || 0,
        volume: Number(volume) || 0,
        avgVol: Number(avgVol) || 1,
        rvol: Number(rvol) || 0,
        closeYest: Number(prevClose) || Number(price) || 0,
        high52: Number(high52) || Number(price) || 0,
        priceChange: (Number(prevClose) > 0) ? ((Number(price)-Number(prevClose))/Number(prevClose))*100 : 0,
        distanceFrom52High: (Number(high52) > 0) ? ((Number(high52)-Number(price))/Number(high52))*100 : 0
      });
    }
  }
  return out;
}

/** ---------- AI scoring and helpers ---------- */
function calculateAIScore(stock) {
  let score = 0;
  const momentum  = calculateMomentumScore_(stock);
  const volume    = calculateVolumeScore_(stock);
  const technical = calculateTechnicalScore_(stock);
  const sentiment = 50; // placeholder
  const risk      = calculateRiskScore_(stock);

  score += momentum  * AI_CONFIG.weights.momentum;
  score += volume    * AI_CONFIG.weights.volume;
  score += technical * AI_CONFIG.weights.technical;
  score += sentiment * AI_CONFIG.weights.sentiment;
  score += risk      * AI_CONFIG.weights.risk;

  return Math.round(score);
}

function calculateMomentumScore_(s) {
  let x = 0;
  if (s.priceChange > 3) x += 40;
  else if (s.priceChange > 2) x += 30;
  else if (s.priceChange > 1) x += 20;
  else if (s.priceChange > 0) x += 10;

  if (s.distanceFrom52High < 5) x += 30;
  else if (s.distanceFrom52High < 10) x += 20;
  else if (s.distanceFrom52High < 15) x += 10;

  if (s.priceChange > 1 && s.rvol > 2) x += 30;

  return Math.min(100, x);
}

function calculateVolumeScore_(s) {
  if (s.rvol > 3) return 100;
  if (s.rvol > 2.5) return 80;
  if (s.rvol > 2) return 60;
  if (s.rvol > 1.5) return 40;
  return 20;
}

function calculateTechnicalScore_(s) {
  let x = 50;
  if (s.priceChange > 0) x += 25;             // above vwap proxy
  if (s.distanceFrom52High < 3) x += 25;      // breakout proximity
  return Math.min(100, x);
}

function calculateRiskScore_(s) {
  let x = 100;
  if (s.priceChange > 10) x -= 50;
  else if (s.priceChange > 7) x -= 30;
  else if (s.priceChange > 5) x -= 10;
  if (s.volume < 5000000) x -= 20;
  return Math.max(0, x);
}

function detectPattern(s) {
  if (s.rvol > 3 && s.priceChange > 3) return 'SQUEEZE';
  if (s.distanceFrom52High < 2)        return 'BREAKOUT';
  if (s.rvol > 2 && s.priceChange > 1) return 'MOMENTUM';
  if (s.priceChange > 0 && s.rvol > 1.5) return 'ACCUMULATION';
  return 'BUILDING';
}

function analyzeSentiment() {
  const sentiments = ['BULLISH','NEUTRAL','MIXED'];
  return sentiments[Math.floor(Math.random()*sentiments.length)];
}

function generateTradingSignal(stock, rank) {
  const p = stock.price || 0;
  let entry, stop, t1, t2;
  switch (stock.pattern) {
    case 'SQUEEZE':
      entry = p*1.001; stop=entry*0.97; t1=entry*1.05; t2=entry*1.08; break;
    case 'BREAKOUT':
      entry = p*1.002; stop=entry*0.98; t1=entry*1.03; t2=entry*1.05; break;
    case 'MOMENTUM':
      entry = p*1.001; stop=entry*0.98; t1=entry*1.03; t2=entry*1.05; break;
    default:
      entry = p*1.003; stop=entry*0.98; t1=entry*1.02; t2=entry*1.04;
  }
  const risk = entry - stop;
  const reward = t1 - entry;
  const rr = risk > 0 ? (reward/risk).toFixed(2) : '0';
  const exp = entry > 0 ? (((t1-entry)/entry)*100).toFixed(2) : '0';

  let signal;
  if (stock.aiScore >= 80 && stock.pattern === 'SQUEEZE') signal = 'STRONG BUY';
  else if (stock.aiScore >= 70) signal = 'BUY';
  else if (stock.aiScore >= 60) signal = 'WATCH CLOSELY';
  else signal = 'MONITOR';

  const notes = generateTradingNotes_(stock);

  return {
    rank,
    ticker: stock.ticker,
    company: stock.name,
    aiScore: stock.aiScore,
    signal,
    currentPrice: p.toFixed(2),
    entryPrice: entry.toFixed(2),
    stopLoss: stop.toFixed(2),
    target1: t1.toFixed(2),
    target2: t2.toFixed(2),
    riskReward: rr,
    expectedMove: exp + '%',
    volumeScore: (stock.rvol || 0).toFixed(2),
    pattern: stock.pattern,
    sentiment: stock.sentiment,
    time: new Date().toLocaleTimeString(),
    status: 'PENDING',
    notes
  };
}

function generateTradingNotes_(s) {
  const notes = [];
  if (s.rvol > 3) notes.push('High volume');
  if (s.distanceFrom52High < 5) notes.push('Near 52W high');
  if (s.priceChange > 3) notes.push('Strong momentum');
  if (s.pattern === 'SQUEEZE') notes.push('Squeeze setup');
  return notes.join(', ') || 'Standard setup';
}

function formatAIAgentSheet_(sheet) {
  if (sheet.getLastRow() < 2) return;
  sheet.getRange('D2:D').setNumberFormat('0');
  sheet.getRange('F2:J').setNumberFormat('$#,##0.00');
  sheet.getRange('K2:K').setNumberFormat('0.00');
  sheet.getRange('L2:L').setNumberFormat('0.0%');
  sheet.getRange('M2:M').setNumberFormat('0.00');

  const rows = sheet.getLastRow() - 1;
  if (rows > 0) {
    const scoreRange = sheet.getRange(2,4,rows,1);
    const signalRange= sheet.getRange(2,5,rows,1);
    const rules = [];
    rules.push(SpreadsheetApp.newConditionalFormatRule()
      .whenNumberGreaterThanOrEqualTo(80).setBackground('#00FF00').setFontColor('#000').setBold(true).setRanges([scoreRange]).build());
    rules.push(SpreadsheetApp.newConditionalFormatRule()
      .whenNumberBetween(70,79).setBackground('#90EE90').setRanges([scoreRange]).build());
    rules.push(SpreadsheetApp.newConditionalFormatRule()
      .whenNumberBetween(60,69).setBackground('#FFFF99').setRanges([scoreRange]).build());
    rules.push(SpreadsheetApp.newConditionalFormatRule()
      .whenTextEqualTo('STRONG BUY').setBackground('#00FF00').setFontColor('#000').setBold(true).setRanges([signalRange]).build());
    sheet.setConditionalFormatRules(rules);
  }
  sheet.autoResizeColumns(1, Math.min(18, sheet.getLastColumn()));
  if (sheet.getLastRow() > 1) {
    sheet.getRange(1,1, sheet.getLastRow(), sheet.getLastColumn()).createFilter();
  }
}

function logDailyPicks_(signals) {
  if (!signals || !signals.length) return;
  const ss = SpreadsheetApp.getActive();
  let tracker = ss.getSheetByName('AI_Tracker');
  if (!tracker) {
    tracker = ss.insertSheet('AI_Tracker');
    tracker.getRange(1,1,1,9).setValues([['Date','Ticker','Entry','Stop','T1','T2','Result','P&L %','Status']]);
    tracker.setFrozenRows(1);
  }
  const date = new Date();
  const rows = signals.slice(0, Math.min(5, signals.length)).map(s => [
    date, s.ticker, s.entryPrice, s.stopLoss, s.target1, s.target2, '', '', 'OPEN'
  ]);
  const lr = tracker.getLastRow();
  tracker.getRange(lr+1,1, rows.length, 9).setValues(rows);
}

function showTopPicks_(top) {
  if (!top || !top.length) {
    SpreadsheetApp.getUi().alert('No trading signals generated today.');
    return;
  }
  let msg = '🤖 AI TRADING SIGNALS\n';
  msg += `Generated: ${new Date().toLocaleString()}\n\n`;
  top.forEach(p => {
    msg += `${p.rank}. ${p.ticker} - ${p.signal}\n`;
    msg += `   AI Score: ${p.aiScore} | Pattern: ${p.pattern}\n`;
    msg += `   Entry: $${p.entryPrice} | Stop: $${p.stopLoss}\n`;
    msg += `   Target: $${p.target1} (${p.expectedMove})\n`;
    msg += `   Risk/Reward: 1:${p.riskReward}\n\n`;
  });
  msg += '⚠️ Risk Warning: All trades carry risk. Use stops!';
  SpreadsheetApp.getUi().alert('AI Trading Picks', msg, SpreadsheetApp.getUi().ButtonSet.OK);
}
/** ---- RESET HELPERS ---- **/

// Soft reset: keep sheets/data, clear progress + exported flags
function resetScannerStateSoft() {
  const ss = SpreadsheetApp.getActive();
  const staging = ss.getSheetByName(BG_CFG.stagingName);
  const props = PropertiesService.getUserProperties();

  // clear status/progress
  props.deleteProperty(STATUS_KEY);
  // keep symbols cache (faster), start from 0
  if (props.getProperty(BG_CFG.symbolsKey)) {
    props.setProperty(BG_CFG.stateKey, JSON.stringify({ nextIndex: 0 }));
  } else {
    props.deleteProperty(BG_CFG.stateKey);
  }

  // clear Exported flags in staging (col N)
  if (staging && staging.getLastRow() > 1) {
    staging.getRange(2, 14, staging.getLastRow() - 1, 1).clearContent();
  }

  SpreadsheetApp.getActive().toast('Soft reset complete: state cleared, Exported flags reset.');
}

// Hard reset: remove triggers, delete sheets, wipe all saved state
function hardResetScanner() {
  // stop background triggers
  ScriptApp.getProjectTriggers().forEach(t => {
    if (t.getHandlerFunction() === 'scanNasdaqInBackground') ScriptApp.deleteTrigger(t);
  });

  const ss = SpreadsheetApp.getActive();
  ['Staging_Nasdaq','MarketCache','System_Log','AI_Agent','AI_Tracker'].forEach(name => {
    const s = ss.getSheetByName(name);
    if (s) ss.deleteSheet(s);
  });

  const props = PropertiesService.getUserProperties();
  [STATUS_KEY, BG_CFG.stateKey, BG_CFG.symbolsKey].forEach(k => props.deleteProperty(k));

  SpreadsheetApp.getActive().toast('Hard reset complete: sheets removed, triggers stopped, state wiped.');
}

// One-click: hard reset then run full pipeline now
function resetAndRunFullNow() {
  hardResetScanner();
  runFullProcessNow(); // uses your existing function
}

/** ====================== Optional: Config modal ========================== */
function configureAI() {
  const html = `
    <div style="padding:20px;font-family:Arial">
      <h3>AI Trading Agent Configuration</h3>
      <h4>Risk Settings:</h4>
      <p>Max Risk per Trade: 2%</p>
      <p>Stop Loss: 2-3%</p>
      <p>Daily Target: 1-3%</p>
      <h4>Current Mode:</h4>
      <p>Conservative Momentum Trading</p>
      <h4>Active Strategies:</h4>
      <ul>
        <li>Short Squeeze Detection</li>
        <li>Momentum Breakouts</li>
        <li>Volume Surge Patterns</li>
      </ul>
      <p style="color:red;margin-top:20px"><b>Warning:</b> AI recommendations are not financial advice. Always do your own research and manage risk.</p>
    </div>`;
  const ui = HtmlService.createHtmlOutput(html).setWidth(420).setHeight(420);
  SpreadsheetApp.getUi().showModalDialog(ui, 'AI Configuration');
}