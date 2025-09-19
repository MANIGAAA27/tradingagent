/** ============================================================================
 * AI Short-Squeeze Agent — Single Source: SavedFilters Only
 * - SavedFilters sheet with IsDefault column for initial loading
 * - Command Center filter switching with live reprocessing
 * - Removed Config sheet entirely - formulas reference SavedFilters directly
 * - ActiveFilter dropdown in SavedFilters!Z2 controls everything
 * ========================================================================== */

const NASDAQ_URL = 'https://www.nasdaqtrader.com/dynamic/symdir/nasdaqlisted.txt';

const SHEETS = {
  staging: 'Staging_Nasdaq',
  market:  'MarketCache', 
  fund:    'Fundamentals_Import',
  saved:   'SavedFilters',
  log:     'System_Log',
  agent:   'AI_Agent',
  tracker: 'AI_Tracker',
};

const BG_CFG = {
  chunkSize: 1200,
  stateKey:   'BG_NASDAQ_STATE',
  symbolsKey: 'BG_NASDAQ_SYMBOLS',
  statusKey:  'BG_STATUS'
};

// Staging header with squeeze fields
const STAGING_HEADER = [
  'Ticker','Company','Price','Volume','$Volume','AvgVol(10D)','RVOL',
  'Prev Close','Change %','52W High','Dist to High %','Category','Qualified','Exported',
  'Float(M)','SI%Float','DTC','BorrowFee%','Catalyst','NewsScore'
];

// MarketCache header  
const MARKET_HEADER = [
  'Ticker','Company','Price','Volume','$Volume','AvgVol(10D)','RVOL',
  'Prev Close','Change %','52W High','Dist to High %','Category',
  'Float(M)','SI%Float','DTC','BorrowFee%','Catalyst','NewsScore','Exported At'
];

// SavedFilters schema with IsDefault column
const FILTERS_HEADER = [
  'Name','IsDefault',
  'PriceMin','PriceMax','MinAvgVol10D','MinRVOL_Base','MinRVOL_Act',
  'IgnitionRVOL','IgnitionDeltaPct','BreakoutDistPct','MaxFloatM',
  'MinSIpct','MinDTC','MinBorrowFee','HorizonText','ScalePlanText'
];

/** -------------------- SINGLE SOURCE: SavedFilters Management ------------- */

function getActiveFilter_() {
  const ss = SpreadsheetApp.getActive();
  const sheet = ss.getSheetByName(SHEETS.saved);
  if (!sheet) throw new Error('SavedFilters sheet not found. Run Setup first.');
  
  // Get active filter name from Z2
  const activeName = String(sheet.getRange('Z2').getValue() || '').trim();
  
  // If Z2 is empty, find the default filter
  if (!activeName) {
    const defaultFilter = findDefaultFilter_();
    if (defaultFilter) {
      sheet.getRange('Z2').setValue(defaultFilter.Name);
      return defaultFilter;
    }
    throw new Error('No active filter selected and no default filter found.');
  }
  
  // Get the specific filter
  const filter = getFilterByName_(activeName);
  if (!filter) throw new Error(`Active filter '${activeName}' not found in SavedFilters.`);
  
  return filter;
}

function getFilterByName_(name) {
  const ss = SpreadsheetApp.getActive();
  const sheet = ss.getSheetByName(SHEETS.saved);
  if (!sheet || sheet.getLastRow() < 2) return null;
  
  const data = sheet.getDataRange().getValues();
  const headers = data[0].map(h => String(h || '').trim());
  
  for (let r = 1; r < data.length; r++) {
    if (String(data[r][0]).trim() === String(name).trim()) {
      const obj = {};
      headers.forEach((h, i) => obj[h] = data[r][i]);
      normalizeFilterValues_(obj);
      return obj;
    }
  }
  return null;
}

function findDefaultFilter_() {
  const ss = SpreadsheetApp.getActive();
  const sheet = ss.getSheetByName(SHEETS.saved);
  if (!sheet || sheet.getLastRow() < 2) return null;
  
  const data = sheet.getDataRange().getValues();
  const headers = data[0].map(h => String(h || '').trim());
  const defaultIdx = headers.indexOf('IsDefault');
  
  if (defaultIdx === -1) return null;
  
  for (let r = 1; r < data.length; r++) {
    if (data[r][defaultIdx] === true || String(data[r][defaultIdx]).toLowerCase() === 'true') {
      const obj = {};
      headers.forEach((h, i) => obj[h] = data[r][i]);
      normalizeFilterValues_(obj);
      return obj;
    }
  }
  return null;
}

function normalizeFilterValues_(obj) {
  // Convert percentage strings, multipliers, etc. to numbers
  Object.keys(obj).forEach(key => {
    if (key === 'Name' || key === 'IsDefault' || key === 'HorizonText' || key === 'ScalePlanText') return;
    
    const raw = obj[key];
    if (typeof raw === 'number') return;
    if (!raw) { obj[key] = 0; return; }
    
    let s = String(raw).trim();
    
    // Percentage handling
    if (s.endsWith('%')) {
      const n = parseFloat(s.replace('%','').replace(/,/g,''));
      if (key === 'IgnitionRVOL') {
        obj[key] = n / 100.0; // 300% -> 3.0
      } else if (key === 'IgnitionDeltaPct' || key === 'BreakoutDistPct') {
        obj[key] = n / 100.0; // 3% -> 0.03
      } else {
        obj[key] = n; // Other percentages stay as numbers
      }
      return;
    }
    
    // Volume with K/M/B multipliers
    const unitMatch = s.match(/^([\d.,]+)\s*([KMB])?/i);
    if (unitMatch) {
      let n = parseFloat(unitMatch[1].replace(/,/g,''));
      const unit = (unitMatch[2] || '').toUpperCase();
      if (unit === 'K') n *= 1e3;
      if (unit === 'M') n *= 1e6;
      if (unit === 'B') n *= 1e9;
      obj[key] = n;
      return;
    }
    
    // Plain number
    const num = parseFloat(s.replace(/,/g,''));
    obj[key] = isFinite(num) ? num : s;
  });
}

/** -------------------- SETUP & SEEDING ------------------------- */

/** -------------------- RUN ALL FILTERS FEATURE --------------------------- */

function runAllFilters() {
  const ss = SpreadsheetApp.getActive();
  
  // Ensure we have staging data first
  if (!ss.getSheetByName(SHEETS.staging) || ss.getSheetByName(SHEETS.staging).getLastRow() < 100) {
    // Build some staging data first
    const props = PropertiesService.getUserProperties();
    updateStatus_({ step:'BUILD_FOR_ALL_FILTERS' });
    buildStaging_(ss, props);
    addNextChunk_(ss, props);
    addNextChunk_(ss, props); // Get at least 2 chunks for testing
  }
  
  const filters = getAllFilters();
  if (filters.length === 0) {
    throw new Error('No filters found. Run Setup SavedFilters first.');
  }
  
  // Create consolidated results sheet
  let resultsSheet = ss.getSheetByName('All_Filters_Results');
  if (resultsSheet) ss.deleteSheet(resultsSheet);
  resultsSheet = ss.insertSheet('All_Filters_Results');
  
  const headers = [
    'Filter Name', 'Ticker', 'Company', 'AI Score', 'Signal', 'Price', 'RVOL', 
    'Change %', 'Float(M)', 'SI%', 'DTC', 'BorrowFee%', 'Pattern', 'Qualified'
  ];
  resultsSheet.getRange(1, 1, 1, headers.length).setValues([headers]);
  resultsSheet.setFrozenRows(1);
  
  let allResults = [];
  let filterSummary = {};
  
  // Process each filter
  for (let i = 0; i < filters.length; i++) {
    const filter = filters[i];
    updateStatus_({ step: `PROCESSING_FILTER_${i+1}/${filters.length}`, lastFilter: filter.Name });
    
    try {
      // Temporarily set this as active filter
      setActiveFilterName(filter.Name);
      
      // Clear and rebuild MarketCache with this filter's criteria
      const mcSheet = ss.getSheetByName(SHEETS.market);
      if (mcSheet && mcSheet.getLastRow() > 1) {
        mcSheet.deleteRows(2, mcSheet.getLastRow() - 1);
      }
      
      // Clear export flags and re-export with new criteria
      const stagingSheet = ss.getSheetByName(SHEETS.staging);
      if (stagingSheet && stagingSheet.getLastRow() > 2) {
        stagingSheet.getRange(3, 14, stagingSheet.getLastRow() - 2, 1).clearContent();
      }
      
      // Re-export with current filter
      exportQualified_(ss);
      
      // Get candidates for this filter
      const candidates = scanCandidatesFromMarket_(filter);
      
      // Score and rank candidates
      const scored = candidates.map(s => ({
        ...s,
        aiScore: scoreSqueeze_(s, filter),
        pattern: detectPattern_(s, filter),
        filterName: filter.Name
      })).sort((a,b) => b.aiScore - a.aiScore);
      
      // Take top 5 from each filter to avoid overwhelming results
      const topCandidates = scored.slice(0, 5);
      
      filterSummary[filter.Name] = {
        totalQualified: candidates.length,
        withSignals: topCandidates.length,
        topScore: topCandidates.length > 0 ? topCandidates[0].aiScore : 0
      };
      
      // Add to consolidated results
      topCandidates.forEach(candidate => {
        const signal = getSignalFromScore(candidate.aiScore, candidate.pattern);
        allResults.push([
          filter.Name,
          candidate.ticker,
          candidate.name,
          candidate.aiScore,
          signal,
          candidate.price,
          candidate.rvol,
          (candidate.chgPct * 100).toFixed(1) + '%',
          candidate.floatM,
          candidate.siPct,
          candidate.dtc,
          candidate.borrowFee,
          candidate.pattern,
          'YES'
        ]);
      });
      
    } catch (error) {
      console.log(`Error processing filter ${filter.Name}: ${error}`);
      filterSummary[filter.Name] = { error: error.toString() };
    }
  }
  
  // Write all results to sheet
  if (allResults.length > 0) {
    resultsSheet.getRange(2, 1, allResults.length, headers.length).setValues(allResults);
    
    // Format the results
    resultsSheet.getRange(2, 6, allResults.length, 1).setNumberFormat('$#,##0.00'); // Price
    resultsSheet.getRange(2, 7, allResults.length, 1).setNumberFormat('0.00'); // RVOL
    resultsSheet.getRange(2, 9, allResults.length, 1).setNumberFormat('0.0'); // Float
    resultsSheet.getRange(2, 10, allResults.length, 1).setNumberFormat('0.0'); // SI%
    resultsSheet.getRange(2, 11, allResults.length, 1).setNumberFormat('0.0'); // DTC
    resultsSheet.getRange(2, 12, allResults.length, 1).setNumberFormat('0.0'); // BorrowFee
    
    // Add conditional formatting for AI Scores
    const scoreRange = resultsSheet.getRange(2, 4, allResults.length, 1);
    const rules = [];
    rules.push(SpreadsheetApp.newConditionalFormatRule()
      .whenNumberGreaterThanOrEqualTo(80)
      .setBackground('#00FF00').setFontColor('#000').setBold(true)
      .setRanges([scoreRange]).build());
    rules.push(SpreadsheetApp.newConditionalFormatRule()
      .whenNumberBetween(70, 79)
      .setBackground('#90EE90')
      .setRanges([scoreRange]).build());
    rules.push(SpreadsheetApp.newConditionalFormatRule()
      .whenNumberBetween(60, 69)
      .setBackground('#FFFF99')
      .setRanges([scoreRange]).build());
    resultsSheet.setConditionalFormatRules(rules);
    
    // Auto-resize and add filter
    resultsSheet.autoResizeColumns(1, headers.length);
    resultsSheet.getRange(1, 1, resultsSheet.getLastRow(), headers.length).createFilter();
  }
  
  // Create summary
  const summary = Object.keys(filterSummary).map(name => {
    const stats = filterSummary[name];
    if (stats.error) return `${name}: ERROR`;
    return `${name}: ${stats.totalQualified} qualified, ${stats.withSignals} signals`;
  }).join(' | ');
  
  updateStatus_({ step: 'ALL_FILTERS_COMPLETE' });
  
  // Show summary dialog
  const ui = SpreadsheetApp.getUi();
  ui.alert(
    'All Filters Complete',
    `Processed ${filters.length} filters.\n\nResults Summary:\n${summary}\n\nTotal consolidated results: ${allResults.length}\n\nSee 'All_Filters_Results' sheet for detailed comparison.`,
    ui.ButtonSet.OK
  );
  
  return summary;
}

function generateFilterComparisonReport() {
  const ss = SpreadsheetApp.getActive();
  const resultsSheet = ss.getSheetByName('All_Filters_Results');
  
  if (!resultsSheet || resultsSheet.getLastRow() < 2) {
    SpreadsheetApp.getUi().alert('No Results', 'Run "Run All Filters" first to generate comparison data.', SpreadsheetApp.getUi().ButtonSet.OK);
    return;
  }
  
  // Create comparison report sheet
  let reportSheet = ss.getSheetByName('Filter_Comparison_Report');
  if (reportSheet) ss.deleteSheet(reportSheet);
  reportSheet = ss.insertSheet('Filter_Comparison_Report');
  
  const data = resultsSheet.getDataRange().getValues();
  const headers = data[0];
  const rows = data.slice(1);
  
  // Analyze results by filter
  const filterStats = {};
  const tickersByFilter = {};
  
  rows.forEach(row => {
    const filterName = row[0];
    const ticker = row[1];
    const aiScore = row[3];
    const signal = row[4];
    
    if (!filterStats[filterName]) {
      filterStats[filterName] = {
        totalSignals: 0,
        strongBuy: 0,
        buy: 0,
        specBuy: 0,
        watch: 0,
        avgScore: 0,
        maxScore: 0,
        tickers: []
      };
      tickersByFilter[filterName] = new Set();
    }
    
    const stats = filterStats[filterName];
    stats.totalSignals++;
    stats.maxScore = Math.max(stats.maxScore, aiScore);
    stats.avgScore += aiScore;
    stats.tickers.push({ ticker, score: aiScore, signal });
    tickersByFilter[filterName].add(ticker);
    
    switch (signal) {
      case 'STRONG BUY': stats.strongBuy++; break;
      case 'BUY': stats.buy++; break;
      case 'SPEC BUY': stats.specBuy++; break;
      default: stats.watch++; break;
    }
  });
  
  // Calculate averages
  Object.keys(filterStats).forEach(filterName => {
    const stats = filterStats[filterName];
    stats.avgScore = stats.totalSignals > 0 ? (stats.avgScore / stats.totalSignals).toFixed(1) : 0;
  });
  
  // Write comparison report
  const reportHeaders = [
    'Filter Name', 'Total Signals', 'STRONG BUY', 'BUY', 'SPEC BUY', 'WATCH',
    'Avg Score', 'Max Score', 'Unique Tickers', 'Top 3 Tickers'
  ];
  
  reportSheet.getRange(1, 1, 1, reportHeaders.length).setValues([reportHeaders]);
  reportSheet.setFrozenRows(1);
  
  const reportRows = Object.keys(filterStats).map(filterName => {
    const stats = filterStats[filterName];
    const top3 = stats.tickers
      .sort((a, b) => b.score - a.score)
      .slice(0, 3)
      .map(t => `${t.ticker}(${t.score})`)
      .join(', ');
    
    return [
      filterName,
      stats.totalSignals,
      stats.strongBuy,
      stats.buy,
      stats.specBuy,
      stats.watch,
      stats.avgScore,
      stats.maxScore,
      tickersByFilter[filterName].size,
      top3
    ];
  });
  
  reportSheet.getRange(2, 1, reportRows.length, reportHeaders.length).setValues(reportRows);
  
  // Format the report
  reportSheet.autoResizeColumns(1, reportHeaders.length);
  reportSheet.getRange(1, 1, reportSheet.getLastRow(), reportHeaders.length).createFilter();
  
  // Add summary at the top
  reportSheet.insertRowsBefore(1, 3);
  reportSheet.getRange(1, 1).setValue('FILTER COMPARISON SUMMARY');
  reportSheet.getRange(1, 1).setFontWeight('bold').setFontSize(14);
  
  const totalResults = rows.length;
  const uniqueTickers = new Set(rows.map(r => r[1])).size;
  const bestFilter = Object.keys(filterStats).reduce((a, b) => 
    filterStats[a].maxScore > filterStats[b].maxScore ? a : b
  );
  
  reportSheet.getRange(2, 1).setValue(
    `Total Results: ${totalResults} | Unique Tickers: ${uniqueTickers} | Best Performing Filter: ${bestFilter}`
  );
  
  SpreadsheetApp.getActive().setActiveSheet(reportSheet);
  SpreadsheetApp.getUi().alert(
    'Comparison Report Generated',
    `Filter comparison report created with ${Object.keys(filterStats).length} filters analyzed.\n\nBest performing: ${bestFilter}\nTotal unique tickers found: ${uniqueTickers}`,
    SpreadsheetApp.getUi().ButtonSet.OK
  );
}

function getSignalFromScore(score, pattern) {
  if (score >= 85 && pattern === 'SQUEEZE') return 'STRONG BUY';
  if (score >= 75) return 'BUY';
  if (score >= 65) return 'SPEC BUY';
  return 'WATCH';
}

function onOpen() {
  const ui = SpreadsheetApp.getUi();

  ui.createMenu('🤖 AI Agent')
    .addItem('Command Center (Sidebar)', 'openCommandCenter')
    .addSeparator()
    .addItem('Setup SavedFilters', 'setupSavedFilters')
    .addItem('Switch Filter & Reprocess', 'switchFilterDialog')
    .addSeparator()
    .addItem('🔍 Run All Filters', 'runAllFilters')
    .addItem('📊 Filter Comparison Report', 'generateFilterComparisonReport')
    .addSeparator()
    .addItem('Start Background (5 min)', 'startBackgroundScanner')
    .addItem('Stop Background', 'stopBackgroundScanner')
    .addItem('Run Scan Once', 'runScanOnce')
    .addItem('Run Full Now', 'runFullProcessNow')
    .addSeparator()
    .addItem('Run AI Scoring Now', 'runAITradingAgent')
    .addItem('Debug Status', 'debugBackgroundState')
    .addToUi();
}

function setupSavedFilters() {
  ensureSavedFiltersSheet_();
  seedDefaultFilters_();
  ensureActiveFilterDropdown_();
  SpreadsheetApp.getActive().toast('SavedFilters setup complete with default filters.');
}

function ensureSavedFiltersSheet_() {
  const ss = SpreadsheetApp.getActive();
  let sheet = ss.getSheetByName(SHEETS.saved);
  
  if (!sheet) {
    sheet = ss.insertSheet(SHEETS.saved);
    sheet.getRange(1, 1, 1, FILTERS_HEADER.length).setValues([FILTERS_HEADER]);
    sheet.setFrozenRows(1);
    sheet.getRange(1, 1, 1, FILTERS_HEADER.length).setFontWeight('bold');
    return sheet;
  }
  
  // Ensure headers are correct
  const currentHeaders = sheet.getRange(1, 1, 1, sheet.getLastColumn()).getValues()[0];
  const missingHeaders = FILTERS_HEADER.filter(h => !currentHeaders.includes(h));
  
  if (missingHeaders.length > 0) {
    const newHeaders = [...currentHeaders.filter(h => h), ...missingHeaders];
    sheet.getRange(1, 1, 1, newHeaders.length).setValues([newHeaders]);
  }
  
  return sheet;
}

function seedDefaultFilters_() {
  const ss = SpreadsheetApp.getActive();
  const sheet = ss.getSheetByName(SHEETS.saved);
  
  const realisticFilters = [
    {
      Name: 'Discovery_Mode',
      IsDefault: true,
      PriceMin: 1, PriceMax: 50, MinAvgVol10D: 250000,
      MinRVOL_Base: 1.3, MinRVOL_Act: 1.6, IgnitionRVOL: 2.0,
      IgnitionDeltaPct: 0.015, BreakoutDistPct: 0.10, MaxFloatM: 200,
      MinSIpct: 5, MinDTC: 0.5, MinBorrowFee: 1,
      HorizonText: '1-3 days',
      ScalePlanText: 'Scale 40% at +8-12%; trail to +20-30%'
    },
    {
      Name: 'Realistic_Squeeze',
      IsDefault: false,
      PriceMin: 2, PriceMax: 100, MinAvgVol10D: 500000,
      MinRVOL_Base: 1.5, MinRVOL_Act: 1.8, IgnitionRVOL: 2.5,
      IgnitionDeltaPct: 0.02, BreakoutDistPct: 0.05, MaxFloatM: 100,
      MinSIpct: 10, MinDTC: 1, MinBorrowFee: 2,
      HorizonText: '1-3 days',
      ScalePlanText: 'Scale 50% at +10-15%; trail to +25-40%'
    },
    {
      Name: 'Penny_Momentum',
      IsDefault: false,
      PriceMin: 0.5, PriceMax: 5, MinAvgVol10D: 150000,
      MinRVOL_Base: 1.4, MinRVOL_Act: 1.7, IgnitionRVOL: 2.2,
      IgnitionDeltaPct: 0.02, BreakoutDistPct: 0.15, MaxFloatM: 50,
      MinSIpct: 8, MinDTC: 0.5, MinBorrowFee: 2,
      HorizonText: '1-2 days',
      ScalePlanText: 'Scale 60% at +15-25%; trail to +40-60%'
    },
    {
      Name: 'SmallCap_Breakout',
      IsDefault: false,
      PriceMin: 3, PriceMax: 25, MinAvgVol10D: 400000,
      MinRVOL_Base: 1.4, MinRVOL_Act: 1.7, IgnitionRVOL: 2.3,
      IgnitionDeltaPct: 0.025, BreakoutDistPct: 0.08, MaxFloatM: 75,
      MinSIpct: 12, MinDTC: 1, MinBorrowFee: 3,
      HorizonText: '2-4 days',
      ScalePlanText: 'Scale 40% at +12-18%; trail to +25-35%'
    },
    {
      Name: 'MidCap_Momentum',
      IsDefault: false,
      PriceMin: 10, PriceMax: 80, MinAvgVol10D: 800000,
      MinRVOL_Base: 1.3, MinRVOL_Act: 1.6, IgnitionRVOL: 2.0,
      IgnitionDeltaPct: 0.02, BreakoutDistPct: 0.06, MaxFloatM: 150,
      MinSIpct: 8, MinDTC: 1, MinBorrowFee: 2,
      HorizonText: '2-5 days',
      ScalePlanText: 'Scale 30% at +8-12%; trail to +18-25%'
    },
    {
      Name: 'Float_Rotation',
      IsDefault: false,
      PriceMin: 5, PriceMax: 40, MinAvgVol10D: 600000,
      MinRVOL_Base: 1.5, MinRVOL_Act: 1.8, IgnitionRVOL: 2.4,
      IgnitionDeltaPct: 0.025, BreakoutDistPct: 0.07, MaxFloatM: 40,
      MinSIpct: 15, MinDTC: 1.5, MinBorrowFee: 4,
      HorizonText: '1-3 days',
      ScalePlanText: 'Scale 50% at +12-18%; trail to +25-40%'
    },
    {
      Name: 'News_Catalyst',
      IsDefault: false,
      PriceMin: 2, PriceMax: 60, MinAvgVol10D: 300000,
      MinRVOL_Base: 1.6, MinRVOL_Act: 2.0, IgnitionRVOL: 2.8,
      IgnitionDeltaPct: 0.03, BreakoutDistPct: 0.12, MaxFloatM: 120,
      MinSIpct: 6, MinDTC: 0.5, MinBorrowFee: 1.5,
      HorizonText: '1-2 days',
      ScalePlanText: 'Scale 40% at +10-15%; trail to +25-35%'
    },
    {
      Name: 'Technical_Breakout',
      IsDefault: false,
      PriceMin: 5, PriceMax: 100, MinAvgVol10D: 700000,
      MinRVOL_Base: 1.2, MinRVOL_Act: 1.5, IgnitionRVOL: 1.8,
      IgnitionDeltaPct: 0.015, BreakoutDistPct: 0.03, MaxFloatM: 180,
      MinSIpct: 5, MinDTC: 0.5, MinBorrowFee: 1,
      HorizonText: '3-7 days',
      ScalePlanText: 'Scale 25% at +8-12%; trail to +15-25%'
    },
    {
      Name: 'High_Beta_Momentum',
      IsDefault: false,
      PriceMin: 3, PriceMax: 30, MinAvgVol10D: 350000,
      MinRVOL_Base: 1.7, MinRVOL_Act: 2.1, IgnitionRVOL: 3.0,
      IgnitionDeltaPct: 0.035, BreakoutDistPct: 0.09, MaxFloatM: 60,
      MinSIpct: 12, MinDTC: 1, MinBorrowFee: 3,
      HorizonText: '1-2 days',
      ScalePlanText: 'Scale 50% at +15-20%; trail to +30-45%'
    },
    {
      Name: 'Volume_Surge',
      IsDefault: false,
      PriceMin: 1, PriceMax: 40, MinAvgVol10D: 200000,
      MinRVOL_Base: 2.0, MinRVOL_Act: 2.5, IgnitionRVOL: 3.5,
      IgnitionDeltaPct: 0.025, BreakoutDistPct: 0.08, MaxFloatM: 80,
      MinSIpct: 8, MinDTC: 0.5, MinBorrowFee: 2,
      HorizonText: '1-2 days',
      ScalePlanText: 'Scale 60% at +12-18%; trail to +25-40%'
    },
    {
      Name: 'Swing_Setup',
      IsDefault: false,
      PriceMin: 8, PriceMax: 120, MinAvgVol10D: 1000000,
      MinRVOL_Base: 1.2, MinRVOL_Act: 1.4, IgnitionRVOL: 1.7,
      IgnitionDeltaPct: 0.01, BreakoutDistPct: 0.05, MaxFloatM: 250,
      MinSIpct: 4, MinDTC: 0.5, MinBorrowFee: 1,
      HorizonText: '5-10 days',
      ScalePlanText: 'Scale 20% at +6-10%; trail to +12-20%'
    },
    {
      Name: 'Biotech_Catalyst',
      IsDefault: false,
      PriceMin: 1, PriceMax: 20, MinAvgVol10D: 100000,
      MinRVOL_Base: 1.8, MinRVOL_Act: 2.2, IgnitionRVOL: 3.0,
      IgnitionDeltaPct: 0.04, BreakoutDistPct: 0.20, MaxFloatM: 30,
      MinSIpct: 12, MinDTC: 1, MinBorrowFee: 5,
      HorizonText: '1-3 days',
      ScalePlanText: 'Scale 50% at +20-30%; trail to +40-70%'
    },
    {
      Name: 'Meme_Revival',
      IsDefault: false,
      PriceMin: 0.5, PriceMax: 15, MinAvgVol10D: 80000,
      MinRVOL_Base: 2.2, MinRVOL_Act: 2.8, IgnitionRVOL: 4.0,
      IgnitionDeltaPct: 0.05, BreakoutDistPct: 0.30, MaxFloatM: 25,
      MinSIpct: 20, MinDTC: 2, MinBorrowFee: 8,
      HorizonText: '1-2 days',
      ScalePlanText: 'Scale 70% at +20-30%; trail to +50-80%'
    }
  ];
  
  // Only add filters that don't already exist
  realisticFilters.forEach(filter => {
    if (!getFilterByName_(filter.Name)) {
      appendFilterRow_(sheet, filter);
    }
  });
}

function appendFilterRow_(sheet, filterObj) {
  const headers = sheet.getRange(1, 1, 1, sheet.getLastColumn()).getValues()[0];
  const row = headers.map(h => filterObj[h] || '');
  sheet.appendRow(row);
}

/** -------------------- COMMAND CENTER WITH FILTER SWITCHING --------------- */

function openCommandCenter() {
  const html = HtmlService.createHtmlOutput(`
    <div style="font:13px/1.4 Arial;padding:14px;width:400px">
      <h2 style="margin:0 0 8px">AI Short-Squeeze Command Center</h2>

      <div style="border:1px solid #ddd;padding:12px;border-radius:8px;background:#f8f9fa;margin-bottom:12px">
        <div style="display:flex;gap:8px;align-items:center;margin-bottom:8px">
          <label for="activeFilter" style="font-weight:bold;min-width:80px">Active Filter:</label>
          <select id="activeFilter" style="flex:1;padding:4px"></select>
        </div>
        <div id="filterInfo" style="font-size:11px;color:#666;margin-bottom:8px;padding:6px;background:#fff;border-radius:4px"></div>
        <div style="display:flex;gap:6px;flex-wrap:wrap;margin-bottom:8px">
          <button id="switchBtn" style="background:#ff9800;color:white;border:none;padding:6px 12px;border-radius:4px">Switch & Reprocess</button>
          <button id="aiOnlyBtn" style="background:#2196f3;color:white;border:none;padding:6px 12px;border-radius:4px">AI Only</button>
          <button id="fullBtn" style="background:#4caf50;color:white;border:none;padding:6px 12px;border-radius:4px">Full Process</button>
        </div>
        <div style="display:flex;gap:6px;flex-wrap:wrap">
          <button id="runAllBtn" style="background:#9c27b0;color:white;border:none;padding:6px 12px;border-radius:4px;font-weight:bold">🔍 Run All Filters</button>
          <button id="compareBtn" style="background:#607d8b;color:white;border:none;padding:6px 12px;border-radius:4px">📊 Compare Results</button>
        </div>
      </div>

      <div style="border:1px solid #ddd;padding:10px;border-radius:8px;background:#fafafa;margin-bottom:12px">
        <div style="display:flex;gap:6px;flex-wrap:wrap;margin-bottom:8px">
          <button id="startBtn">Start BG (5m)</button>
          <button id="stopBtn">Stop BG</button>
          <button id="scanBtn">Scan Once</button>
          <button id="setupBtn">Setup Filters</button>
        </div>
        <div style="display:flex;gap:6px;flex-wrap:wrap">
          <button id="softBtn">Soft Reset</button>
          <button id="hardBtn" style="color:#b00020">Hard Reset</button>
          <button id="refreshBtn">Refresh</button>
        </div>
      </div>

      <div id="status" style="border:1px solid #ddd;padding:10px;border-radius:8px;background:#fafafa">
        <div><b>Step:</b> <span id="step">—</span></div>
        <div><b>Progress:</b> <span id="progress">0 / 0 (0%)</span></div>
        <div style="height:8px;background:#eee;border-radius:4px;margin:6px 0">
          <div id="bar" style="height:8px;width:0%;background:#4caf50;border-radius:4px"></div>
        </div>
        <div><b>Staging:</b> <span id="staging">0</span> • <b>Qualified:</b> <span id="qualified">0</span> • <b>Exported:</b> <span id="exported">0</span></div>
        <div><b>MarketCache:</b> <span id="mc">0</span> • <b>Trigger:</b> <span id="trigger">—</span></div>
        <div>Last run: <span id="lastrun">—</span></div>
      </div>

      <div id="err" style="color:#b00020;margin-top:8px;font-size:12px"></div>

      <script>
        function $(id){ return document.getElementById(id); }
        function setErr(msg){ $('err').textContent = msg || ''; }

        function renderStatus(s){
          const pct = s.symbolsTotal ? Math.min(100, Math.round(100*s.nextIndex/Math.max(1,s.symbolsTotal))) : 0;
          $('step').textContent = s.step || '—';
          $('progress').textContent = (s.nextIndex||0)+' / '+(s.symbolsTotal||0)+' ('+pct+'%)';
          $('bar').style.width = pct+'%';
          $('staging').textContent = s.stagingRows||0;
          $('qualified').textContent = s.qualified||0;
          $('exported').textContent = s.exported||0;
          $('mc').textContent = s.marketCacheRows||0;
          $('trigger').textContent = s.hasTrigger ? 'ON' : 'OFF';
          $('lastrun').textContent = s.lastRun ? new Date(s.lastRun).toLocaleString() : '—';
        }

        function loadFilters(){
          google.script.run.withSuccessHandler(function(filters){
            const sel = $('activeFilter');
            sel.innerHTML = '';
            filters.forEach(f => {
              const opt = document.createElement('option');
              opt.value = f.Name;
              opt.textContent = f.Name + (f.IsDefault ? ' (Default)' : '');
              sel.appendChild(opt);
            });
            
            google.script.run.withSuccessHandler(function(active){
              if (active && filters.find(f => f.Name === active)) {
                sel.value = active;
                updateFilterInfo(filters.find(f => f.Name === active));
              }
            }).getActiveFilterName();
          }).withFailureHandler(e=>setErr(e.message||e)).getAllFilters();
        }

        function updateFilterInfo(filter) {
          if (!filter) return;
          const info = 
            \`Price: $\${filter.PriceMin}-$\${filter.PriceMax} • \` +
            \`RVOL: \${filter.MinRVOL_Base}x/\${filter.MinRVOL_Act}x • \` +
            \`Float: <\${filter.MaxFloatM}M • \` +
            \`SI: \${filter.MinSIpct}%+ • \` +
            \`Ignition: \${(filter.IgnitionRVOL*100).toFixed(0)}%RVOL + \${(filter.IgnitionDeltaPct*100).toFixed(0)}%Δ\`;
          $('filterInfo').textContent = info;
        }

        function refreshStatus(){
          google.script.run.withSuccessHandler(renderStatus).withFailureHandler(e=>setErr(e.message||e)).getBackgroundStatus();
        }

        // Event handlers
        $('activeFilter').onchange = function(){
          const name = $('activeFilter').value;
          google.script.run.withSuccessHandler(function(filter){
            updateFilterInfo(filter);
          }).setActiveFilterName(name);
        };

        $('switchBtn').onclick = function(){
          setErr('Switching filter and reprocessing...');
          const name = $('activeFilter').value;
          google.script.run.withSuccessHandler(function(){
            setErr('Filter switched and data reprocessed.');
            refreshStatus();
          }).withFailureHandler(e=>setErr(e.message||e)).switchFilterAndReprocess(name);
        };

        $('aiOnlyBtn').onclick = function(){
          setErr('Running AI analysis...');
          google.script.run.withSuccessHandler(function(){
            setErr('AI analysis complete.');
            refreshStatus();
          }).withFailureHandler(e=>setErr(e.message||e)).runAITradingAgent();
        };

        $('fullBtn').onclick = function(){
          setErr('Running full process...');
          google.script.run.withSuccessHandler(function(){
            setErr('Full process complete.');
            refreshStatus();
          }).withFailureHandler(e=>setErr(e.message||e)).runFullProcessNow();
        };

        $('runAllBtn').onclick = function(){
          setErr('Running all filters - this may take several minutes...');
          google.script.run.withSuccessHandler(function(summary){
            setErr('All filters complete. Results: ' + summary);
            refreshStatus();
          }).withFailureHandler(e=>setErr(e.message||e)).runAllFilters();
        };

        $('compareBtn').onclick = function(){
          setErr('Opening filter comparison report...');
          google.script.run.withSuccessHandler(function(){
            setErr('Comparison report generated.');
          }).withFailureHandler(e=>setErr(e.message||e)).generateFilterComparisonReport();
        };

        $('startBtn').onclick = function(){
          setErr('Switching filter and reprocessing...');
          const name = $('activeFilter').value;
          google.script.run.withSuccessHandler(function(){
            setErr('Filter switched and data reprocessed.');
            refreshStatus();
          }).withFailureHandler(e=>setErr(e.message||e)).switchFilterAndReprocess(name);
        };

        $('aiOnlyBtn').onclick = function(){
          setErr('Running AI analysis...');
          google.script.run.withSuccessHandler(function(){
            setErr('AI analysis complete.');
            refreshStatus();
          }).withFailureHandler(e=>setErr(e.message||e)).runAITradingAgent();
        };

        $('fullBtn').onclick = function(){
          setErr('Running full process...');
          google.script.run.withSuccessHandler(function(){
            setErr('Full process complete.');
            refreshStatus();
          }).withFailureHandler(e=>setErr(e.message||e)).runFullProcessNow();
        };

        $('startBtn').onclick = function(){
          google.script.run.withSuccessHandler(_=>refreshStatus()).withFailureHandler(e=>setErr(e.message||e)).startBackgroundScanner();
        };

        $('stopBtn').onclick = function(){
          google.script.run.withSuccessHandler(_=>refreshStatus()).withFailureHandler(e=>setErr(e.message||e)).stopBackgroundScanner();
        };

        $('scanBtn').onclick = function(){
          google.script.run.withSuccessHandler(_=>refreshStatus()).withFailureHandler(e=>setErr(e.message||e)).runScanOnce();
        };

        $('setupBtn').onclick = function(){
          google.script.run.withSuccessHandler(function(){ loadFilters(); }).withFailureHandler(e=>setErr(e.message||e)).setupSavedFilters();
        };

        $('softBtn').onclick = function(){
          google.script.run.withSuccessHandler(_=>refreshStatus()).withFailureHandler(e=>setErr(e.message||e)).resetScannerStateSoft();
        };

        $('hardBtn').onclick = function(){
          if (!confirm('Hard reset will delete all working sheets. Continue?')) return;
          google.script.run.withSuccessHandler(_=>{ loadFilters(); refreshStatus(); }).withFailureHandler(e=>setErr(e.message||e)).hardResetScanner();
        };

        $('refreshBtn').onclick = function(){
          loadFilters();
          refreshStatus();
        };

        // Initial load
        loadFilters();
        refreshStatus();
        setInterval(refreshStatus, 5000);
      </script>
    </div>
  `).setTitle('AI Short-Squeeze Command Center').setWidth(420);
  
  SpreadsheetApp.getUi().showSidebar(html);
}

function switchFilterDialog() {
  const ui = SpreadsheetApp.getUi();
  const filters = getAllFilters();
  
  if (filters.length === 0) {
    ui.alert('No filters found. Run Setup SavedFilters first.');
    return;
  }
  
  const filterNames = filters.map(f => f.Name).join('\n');
  const response = ui.prompt(
    'Switch Filter & Reprocess',
    `Available filters:\n${filterNames}\n\nEnter filter name:`,
    ui.ButtonSet.OK_CANCEL
  );
  
  if (response.getSelectedButton() === ui.Button.OK) {
    const name = response.getResponseText().trim();
    switchFilterAndReprocess(name);
  }
}

function switchFilterAndReprocess(filterName) {
  setActiveFilterName(filterName);
  
  // Clear MarketCache to force reprocessing with new criteria
  const ss = SpreadsheetApp.getActive();
  const mcSheet = ss.getSheetByName(SHEETS.market);
  if (mcSheet && mcSheet.getLastRow() > 1) {
    mcSheet.deleteRows(2, mcSheet.getLastRow() - 1);
  }
  
  // Clear export flags in staging to reprocess everything
  const stagingSheet = ss.getSheetByName(SHEETS.staging);
  if (stagingSheet && stagingSheet.getLastRow() > 2) {
    stagingSheet.getRange(3, 14, stagingSheet.getLastRow() - 2, 1).clearContent();
  }
  
  // Rebuild policy banner with new criteria
  if (stagingSheet) {
    ensurePolicyBanner_(stagingSheet);
  }
  
  // Run export and AI
  exportQualified_(ss);
  runAITradingAgent();
  
  SpreadsheetApp.getActive().toast(`Switched to filter: ${filterName} and reprocessed data.`);
}

/** -------------------- SIDEBAR SERVER FUNCTIONS --------------------------- */

function getAllFilters() {
  const ss = SpreadsheetApp.getActive();
  const sheet = ss.getSheetByName(SHEETS.saved);
  if (!sheet || sheet.getLastRow() < 2) return [];
  
  const data = sheet.getDataRange().getValues();
  const headers = data[0];
  const filters = [];
  
  for (let r = 1; r < data.length; r++) {
    const obj = {};
    headers.forEach((h, i) => obj[h] = data[r][i]);
    normalizeFilterValues_(obj);
    filters.push(obj);
  }
  
  return filters;
}

function getActiveFilterName() {
  const ss = SpreadsheetApp.getActive();
  const sheet = ss.getSheetByName(SHEETS.saved);
  if (!sheet) return '';
  return String(sheet.getRange('Z2').getValue() || '').trim();
}

function setActiveFilterName(name) {
  const ss = SpreadsheetApp.getActive();
  const sheet = ss.getSheetByName(SHEETS.saved);
  if (!sheet) throw new Error('SavedFilters sheet not found.');
  
  const filter = getFilterByName_(name);
  if (!filter) throw new Error(`Filter '${name}' not found.`);
  
  sheet.getRange('Z2').setValue(name);
  ensureActiveFilterDropdown_();
  return filter;
}

function ensureActiveFilterDropdown_() {
  const ss = SpreadsheetApp.getActive();
  const sheet = ss.getSheetByName(SHEETS.saved);
  if (!sheet) return;
  
  sheet.getRange('Z1').setValue('ActiveFilter').setFontWeight('bold');
  
  const lastRow = Math.max(2, sheet.getLastRow());
  const namesRange = sheet.getRange(2, 1, lastRow - 1, 1);
  const rule = SpreadsheetApp.newDataValidation()
    .requireValueInRange(namesRange, true)
    .setAllowInvalid(false)
    .build();
  
  const cell = sheet.getRange('Z2');
  cell.setDataValidation(rule);
  
  // Set to default if empty
  if (!cell.getValue()) {
    const defaultFilter = findDefaultFilter_();
    if (defaultFilter) {
      cell.setValue(defaultFilter.Name);
    }
  }
}

/** -------------------- BACKGROUND PROCESSING --------------------------- */

function startBackgroundScanner() {
  ScriptApp.getProjectTriggers().forEach(t => {
    if (t.getHandlerFunction() === 'scanNasdaqInBackground') ScriptApp.deleteTrigger(t);
  });
  ScriptApp.newTrigger('scanNasdaqInBackground').timeBased().everyMinutes(5).create();
  SpreadsheetApp.getActive().toast('Background scanner started (every 5 minutes).');
}

function stopBackgroundScanner() {
  let count = 0;
  ScriptApp.getProjectTriggers().forEach(t => {
    if (t.getHandlerFunction() === 'scanNasdaqInBackground') {
      ScriptApp.deleteTrigger(t);
      count++;
    }
  });
  SpreadsheetApp.getActive().toast(`Stopped ${count} background trigger(s).`);
}

function runScanOnce() {
  scanNasdaqInBackground();
}

function runFullProcessNow() {
  const ss = SpreadsheetApp.getActive();
  const props = PropertiesService.getUserProperties();
  const start = Date.now();
  const budgetMs = 5*60*1000 - 15000;

  updateStatus_({ step:'FULL_INIT', lastRun: new Date().toISOString(), lastError:'' });
  buildStaging_(ss, props);

  while (Date.now() - start < budgetMs) {
    updateStatus_({ step:'FULL_ADD_CHUNK' });
    addNextChunk_(ss, props);
    updateStatus_({ step:'FULL_EXPORT' });
    exportQualified_(ss);
    Utilities.sleep(600);
    
    const state = JSON.parse(props.getProperty(BG_CFG.stateKey) || '{"nextIndex":0}');
    const symbols = JSON.parse(props.getProperty(BG_CFG.symbolsKey) || '[]');
    if ((state.nextIndex||0) >= symbols.length) break;
  }
  
  exportQualified_(ss);
  updateStatus_({ step:'RUN_AI' });
  runAITradingAgent();
  updateStatus_({ step:'FULL_DONE' });
  appendLog_('FULL_DONE');
}

function scanNasdaqInBackground() {
  const lock = LockService.getScriptLock();
  if (!lock.tryLock(5000)) {
    appendLog_('SKIP_LOCKED');
    return;
  }
  
  try {
    updateStatus_({ step:'INIT', lastRun: new Date().toISOString(), lastError:'' });
    const ss = SpreadsheetApp.getActive();
    const props = PropertiesService.getUserProperties();
    
    updateStatus_({ step:'BUILD' });
    buildStaging_(ss, props);
    
    updateStatus_({ step:'ADD' });
    addNextChunk_(ss, props);
    
    updateStatus_({ step:'EXPORT' });
    exportQualified_(ss);
    
    updateStatus_({ step:'DONE' });
    appendLog_('OK');
  } catch (e) {
    updateStatus_({ step:'ERROR', lastError: String(e?.message || e) });
    appendLog_('ERROR: ' + e);
    throw e;
  } finally {
    lock.releaseLock();
  }
}

/** -------------------- STAGING WITH DIRECT SAVEDFILTERS REFERENCES ------- */

function buildStaging_(ss, props) {
  const sheet = ensureSheet_(ss, SHEETS.staging);
  
  // Initialize header
  const headerOk = sheet.getLastRow() >= 1 && sheet.getRange(1,1).getValue() === STAGING_HEADER[0];
  if (!headerOk) {
    sheet.clear();
    sheet.getRange(1,1,1,STAGING_HEADER.length).setValues([STAGING_HEADER]);
    sheet.setFrozenRows(2);
    sheet.getRange(1,1,1,STAGING_HEADER.length).createFilter();
    sheet.autoResizeColumns(1, STAGING_HEADER.length);
  }
  
  // Policy banner with active filter info
  ensurePolicyBanner_(sheet);
  
  // Ensure dependencies
  ensureFundamentalsSheet();
  ensureSavedFiltersSheet_();
  
  // Cache symbols on first run
  if (!props.getProperty(BG_CFG.symbolsKey)) {
    const symbols = fetchNasdaqList_();
    props.setProperty(BG_CFG.symbolsKey, JSON.stringify(symbols));
    props.setProperty(BG_CFG.stateKey, JSON.stringify({ nextIndex: 0 }));
    ensureFormulas_(sheet, 3, 52);
  }
}

function addNextChunk_(ss, props) {
  const sheet = ss.getSheetByName(SHEETS.staging);
  if (!sheet) return;
  
  const symbolsJson = props.getProperty(BG_CFG.symbolsKey);
  if (!symbolsJson) return;
  const symbols = JSON.parse(symbolsJson);
  
  const state = JSON.parse(props.getProperty(BG_CFG.stateKey) || '{"nextIndex":0}');
  const start = state.nextIndex || 0;
  if (start >= symbols.length) return;

  const end = Math.min(start + BG_CFG.chunkSize, symbols.length);
  const slice = symbols.slice(start, end);

  const writeStartRow = sheet.getLastRow() + 1;
  const values = slice.map(s => [s.ticker, s.name]);
  sheet.getRange(writeStartRow, 1, values.length, 2).setValues(values);

  ensureFormulas_(sheet, writeStartRow, writeStartRow + values.length - 1);
  props.setProperty(BG_CFG.stateKey, JSON.stringify({ nextIndex: end }));
}

function ensureFormulas_(sheet, rowStart, rowEnd) {
  if (rowEnd < rowStart) return;
  const n = rowEnd - rowStart + 1;

  // Basic market data from Google Finance
  sheet.getRange(rowStart, 3, n, 1).setFormulaR1C1('=IFERROR(GOOGLEFINANCE(RC1,"price"),)');
  sheet.getRange(rowStart, 4, n, 1).setFormulaR1C1('=IFERROR(GOOGLEFINANCE(RC1,"volume"),)');
  sheet.getRange(rowStart, 5, n, 1).setFormulaR1C1('=IF(RC3*RC4="","",RC3*RC4)');
  sheet.getRange(rowStart, 6, n, 1).setFormulaR1C1('=IFERROR(AVERAGE(QUERY(GOOGLEFINANCE(RC1,"volume",TODAY()-20,TODAY()),"select Col2 offset 1",0)),"")');
  sheet.getRange(rowStart, 7, n, 1).setFormulaR1C1('=IF(RC6="","",IFERROR(RC4/RC6,))');
  sheet.getRange(rowStart, 8, n, 1).setFormulaR1C1(
    '=IFERROR(GOOGLEFINANCE(RC1,"closeyest"),IFERROR(INDEX(SORT(QUERY(GOOGLEFINANCE(RC1,"close",WORKDAY(TODAY(),-20),TODAY()),"select Col1,Col2 where Col2 is not null",0),1,TRUE),ROWS(SORT(QUERY(GOOGLEFINANCE(RC1,"close",WORKDAY(TODAY(),-20),TODAY()),"select Col1,Col2 where Col2 is not null",0),1,TRUE)),2),""))'
  );
  sheet.getRange(rowStart, 9, n, 1).setFormulaR1C1('=IFERROR(GOOGLEFINANCE(RC1,"changepct")/100,IF(OR(RC3="",RC8=""),"",IFERROR((RC3-RC8)/RC8,)))');
  sheet.getRange(rowStart,10, n, 1).setFormulaR1C1('=IFERROR(GOOGLEFINANCE(RC1,"high52"),)');
  sheet.getRange(rowStart,11, n, 1).setFormulaR1C1('=IF(OR(RC10="",RC3=""),"",IFERROR((RC10-RC3)/RC10,))');

  // Category detection using SavedFilters references (Z2 = active filter)
  const activeName = getActiveFilterName() || 'ShortSqueeze_Default';
  const filterRowRef = `MATCH("${activeName}",SavedFilters!A:A,0)`;
  
  for (let r = rowStart; r <= rowEnd; r++) {
    const catFormula = `
      =LET(
        filterRow, ${filterRowRef},
        ignitionRVOL, INDEX(SavedFilters!G:G, filterRow),
        ignitionDelta, INDEX(SavedFilters!H:H, filterRow),
        breakoutDist, INDEX(SavedFilters!I:I, filterRow),
        minRVOL, INDEX(SavedFilters!F:F, filterRow),
        IF(OR($C${r}="",$D${r}="",$F${r}="",$G${r}="",$H${r}="",$I${r}="",$J${r}="",$K${r}=""),"PENDING",
          IF(AND($G${r}>=ignitionRVOL, $I${r}>=ignitionDelta), "SQUEEZE",
            IF($K${r}<=breakoutDist, "BREAKOUT",
              IF(AND($G${r}>=minRVOL, $I${r}>=0.01), "MOMENTUM",
                IF(AND($I${r}>0, $G${r}>=1.5), "ACCUMULATION","BUILDING")))))
      )`;
    sheet.getRange(r, 12).setFormula(catFormula);
  }

  // Fundamentals from lookup sheet
  const fundSheet = SHEETS.fund;
  sheet.getRange(rowStart,15, n, 1).setFormula(`=IFERROR(VLOOKUP(A:A,INDIRECT("'${fundSheet}'!A:H"),2,FALSE),"")`); // Float
  sheet.getRange(rowStart,16, n, 1).setFormula(`=IFERROR(VLOOKUP(A:A,INDIRECT("'${fundSheet}'!A:H"),3,FALSE),"")`); // SI%
  sheet.getRange(rowStart,17, n, 1).setFormula(`=IFERROR(VLOOKUP(A:A,INDIRECT("'${fundSheet}'!A:H"),4,FALSE),"")`); // DTC
  sheet.getRange(rowStart,18, n, 1).setFormula(`=IFERROR(VLOOKUP(A:A,INDIRECT("'${fundSheet}'!A:H"),5,FALSE),"")`); // Fee
  sheet.getRange(rowStart,19, n, 1).setFormula(`=IFERROR(VLOOKUP(A:A,INDIRECT("'${fundSheet}'!A:H"),6,FALSE),IF(L${rowStart}="SQUEEZE","Price/Volume",""))`); // Catalyst
  sheet.getRange(rowStart,20, n, 1).setFormula(`=IFERROR(VLOOKUP(A:A,INDIRECT("'${fundSheet}'!A:H"),7,FALSE),"")`); // News

  // Qualified gate using SavedFilters references
  for (let r = rowStart; r <= rowEnd; r++) {
    const qualFormula = `
      =LET(
        filterRow, ${filterRowRef},
        priceMin, INDEX(SavedFilters!C:C, filterRow),
        priceMax, INDEX(SavedFilters!D:D, filterRow),
        minVol, INDEX(SavedFilters!E:E, filterRow),
        minRVOL, INDEX(SavedFilters!F:F, filterRow),
        maxFloat, INDEX(SavedFilters!J:J, filterRow),
        minSI, INDEX(SavedFilters!K:K, filterRow),
        minDTC, INDEX(SavedFilters!L:L, filterRow),
        minFee, INDEX(SavedFilters!M:M, filterRow),
        IF(AND(
          $C${r}>=priceMin, $C${r}<=priceMax,
          $F${r}>=minVol, $G${r}>=minRVOL,
          $O${r}>0, $O${r}<maxFloat,
          $P${r}>=minSI, $Q${r}>=minDTC, $R${r}>=minFee
        ),"✓ QUALIFIED","FILTERED")
      )`;
    sheet.getRange(r, 13).setFormula(qualFormula);
  }

  // Number formats
  sheet.getRange(rowStart, 3, n, 1).setNumberFormat('$#,##0.00');
  sheet.getRange(rowStart, 4, n, 1).setNumberFormat('#,##0');
  sheet.getRange(rowStart, 5, n, 1).setNumberFormat('$#,##0');
  sheet.getRange(rowStart, 6, n, 1).setNumberFormat('#,##0');
  sheet.getRange(rowStart, 7, n, 1).setNumberFormat('0.00');
  sheet.getRange(rowStart, 8, n, 1).setNumberFormat('$#,##0.00');
  sheet.getRange(rowStart, 9, n, 1).setNumberFormat('0.00%');
  sheet.getRange(rowStart,10, n, 1).setNumberFormat('$#,##0.00');
  sheet.getRange(rowStart,11, n, 1).setNumberFormat('0.00%');
  sheet.getRange(rowStart,15, n, 1).setNumberFormat('0.00');
  sheet.getRange(rowStart,16, n, 1).setNumberFormat('0.0');
  sheet.getRange(rowStart,17, n, 1).setNumberFormat('0.0');
  sheet.getRange(rowStart,18, n, 1).setNumberFormat('0.0');
}

function ensurePolicyBanner_(sheet) {
  const activeFilter = getActiveFilter_();
  const ignPct = (activeFilter.IgnitionDeltaPct * 100).toFixed(0) + '%';
  const ignRvol = (activeFilter.IgnitionRVOL * 100).toFixed(0) + '%';
  
  const text = `ACTIVE FILTER: ${activeFilter.Name} | ` +
               `Price ${activeFilter.PriceMin}-${activeFilter.PriceMax} • ` +
               `Vol≥${activeFilter.MinAvgVol10D.toLocaleString()} • ` +
               `RVOL≥${activeFilter.MinRVOL_Base}/${activeFilter.MinRVOL_Act} • ` +
               `Float<${activeFilter.MaxFloatM}M • ` +
               `SI≥${activeFilter.MinSIpct}% • ` +
               `DTC≥${activeFilter.MinDTC} • ` +
               `Fee≥${activeFilter.MinBorrowFee}% | ` +
               `IGNITION: RVOL≥${ignRvol} & Δ≥${ignPct}`;

  const row2Val = sheet.getLastRow() >= 2 ? (sheet.getRange(2,1).getValue() || '') : '';
  if (String(row2Val).startsWith('ACTIVE FILTER:')) {
    sheet.getRange(2,1).setValue(text);
  } else {
    sheet.insertRowsBefore(2, 1);
    sheet.getRange(2,1).setValue(text);
  }
  
  const lastCol = STAGING_HEADER.length;
  const rng = sheet.getRange(2,1,1,lastCol);
  rng.merge();
  rng.setBackground('#E8F5E8').setFontColor('#2E7D32').setWrap(true).setFontStyle('italic');
  rng.setHorizontalAlignment('center');
  sheet.setFrozenRows(2);
}

/** -------------------- EXPORT TO MARKETCACHE --------------------------- */

function exportQualified_(ss) {
  const staging = ss.getSheetByName(SHEETS.staging);
  if (!staging) return;

  const mc = ensureSheet_(ss, SHEETS.market);
  if (mc.getLastRow() < 1) {
    mc.getRange(1,1,1,MARKET_HEADER.length).setValues([MARKET_HEADER]);
    mc.setFrozenRows(1);
    mc.getRange(1,1,1,MARKET_HEADER.length).createFilter();
  }

  const lastRow = staging.getLastRow();
  if (lastRow < 3) return;

  const mcRows = mc.getLastRow() - 1;
  const existing = mcRows > 0
    ? new Set(mc.getRange(2,1,mcRows,1).getValues().flat().filter(Boolean))
    : new Set();

  const cols = STAGING_HEADER.length;
  const data = staging.getRange(3, 1, lastRow-2, cols).getValues();

  const out = [];
  const toMark = [];

  for (let i = 0; i < data.length; i++) {
    const r = data[i];
    const [ticker, company, price, volume, dollarVol, avgVol, rvol,
           prevClose, chgPct, high52, distHigh, category, qualified, exported,
           floatM, siPct, dtc, borrowFee, catalyst, newsScore] = r;

    if (!ticker) continue;
    if (exported === 'YES') continue;
    if (qualified !== '✓ QUALIFIED') continue;
    if (!category || category === 'PENDING' || category === 'BUILDING') continue;
    if (existing.has(ticker)) { toMark.push(3 + i); continue; }

    out.push([
      ticker, company, price, volume, dollarVol, avgVol, rvol,
      prevClose, chgPct, high52, distHigh, category,
      floatM, siPct, dtc, borrowFee, catalyst, newsScore, new Date()
    ]);
    toMark.push(3 + i);
    existing.add(ticker);
  }

  if (out.length > 0) {
    const destRow = mc.getLastRow() + 1;
    mc.getRange(destRow, 1, out.length, MARKET_HEADER.length).setValues(out);
    toMark.forEach(ridx => staging.getRange(ridx, 14).setValue('YES'));

    // Format MarketCache
    const lr = mc.getLastRow();
    if (lr >= 2) {
      mc.getRange(2,3, lr-1, 1).setNumberFormat('$#,##0.00');
      mc.getRange(2,4, lr-1, 1).setNumberFormat('#,##0');
      mc.getRange(2,5, lr-1, 1).setNumberFormat('$#,##0');
      mc.getRange(2,6, lr-1, 1).setNumberFormat('#,##0');
      mc.getRange(2,7, lr-1, 1).setNumberFormat('0.00');
      mc.getRange(2,8, lr-1, 1).setNumberFormat('$#,##0.00');
      mc.getRange(2,9, lr-1, 1).setNumberFormat('0.00%');
      mc.getRange(2,10,lr-1, 1).setNumberFormat('$#,##0.00');
      mc.getRange(2,11,lr-1, 1).setNumberFormat('0.00%');
      mc.getRange(2,13,lr-1, 1).setNumberFormat('0.00');
      mc.getRange(2,14,lr-1, 1).setNumberFormat('0.0');
      mc.getRange(2,15,lr-1, 1).setNumberFormat('0.0');
      mc.getRange(2,16,lr-1, 1).setNumberFormat('0.0');
    }
  }
}

/** -------------------- RUN ALL FILTERS FEATURE --------------------------- */

function runAllFilters() {
  const ss = SpreadsheetApp.getActive();
  
  // Ensure we have staging data first
  if (!ss.getSheetByName(SHEETS.staging) || ss.getSheetByName(SHEETS.staging).getLastRow() < 100) {
    // Build some staging data first
    const props = PropertiesService.getUserProperties();
    updateStatus_({ step:'BUILD_FOR_ALL_FILTERS' });
    buildStaging_(ss, props);
    addNextChunk_(ss, props);
    addNextChunk_(ss, props); // Get at least 2 chunks for testing
  }
  
  const filters = getAllFilters();
  if (filters.length === 0) {
    throw new Error('No filters found. Run Setup SavedFilters first.');
  }
  
  // Create consolidated results sheet
  let resultsSheet = ss.getSheetByName('All_Filters_Results');
  if (resultsSheet) ss.deleteSheet(resultsSheet);
  resultsSheet = ss.insertSheet('All_Filters_Results');
  
  const headers = [
    'Filter Name', 'Ticker', 'Company', 'AI Score', 'Signal', 'Price', 'RVOL', 
    'Change %', 'Float(M)', 'SI%', 'DTC', 'BorrowFee%', 'Pattern', 'Qualified'
  ];
  resultsSheet.getRange(1, 1, 1, headers.length).setValues([headers]);
  resultsSheet.setFrozenRows(1);
  
  let allResults = [];
  let filterSummary = {};
  
  // Process each filter
  for (let i = 0; i < filters.length; i++) {
    const filter = filters[i];
    updateStatus_({ step: `PROCESSING_FILTER_${i+1}/${filters.length}`, lastFilter: filter.Name });
    
    try {
      // Temporarily set this as active filter
      setActiveFilterName(filter.Name);
      
      // Clear and rebuild MarketCache with this filter's criteria
      const mcSheet = ss.getSheetByName(SHEETS.market);
      if (mcSheet && mcSheet.getLastRow() > 1) {
        mcSheet.deleteRows(2, mcSheet.getLastRow() - 1);
      }
      
      // Clear export flags and re-export with new criteria
      const stagingSheet = ss.getSheetByName(SHEETS.staging);
      if (stagingSheet && stagingSheet.getLastRow() > 2) {
        stagingSheet.getRange(3, 14, stagingSheet.getLastRow() - 2, 1).clearContent();
      }
      
      // Re-export with current filter
      exportQualified_(ss);
      
      // Get candidates for this filter
      const candidates = scanCandidatesFromMarket_(filter);
      
      // Score and rank candidates
      const scored = candidates.map(s => ({
        ...s,
        aiScore: scoreSqueeze_(s, filter),
        pattern: detectPattern_(s, filter),
        filterName: filter.Name
      })).sort((a,b) => b.aiScore - a.aiScore);
      
      // Take top 5 from each filter to avoid overwhelming results
      const topCandidates = scored.slice(0, 5);
      
      filterSummary[filter.Name] = {
        totalQualified: candidates.length,
        withSignals: topCandidates.length,
        topScore: topCandidates.length > 0 ? topCandidates[0].aiScore : 0
      };
      
      // Add to consolidated results
      topCandidates.forEach(candidate => {
        const signal = getSignalFromScore(candidate.aiScore, candidate.pattern);
        allResults.push([
          filter.Name,
          candidate.ticker,
          candidate.name,
          candidate.aiScore,
          signal,
          candidate.price,
          candidate.rvol,
          (candidate.chgPct * 100).toFixed(1) + '%',
          candidate.floatM,
          candidate.siPct,
          candidate.dtc,
          candidate.borrowFee,
          candidate.pattern,
          'YES'
        ]);
      });
      
    } catch (error) {
      console.log(`Error processing filter ${filter.Name}: ${error}`);
      filterSummary[filter.Name] = { error: error.toString() };
    }
  }
  
  // Write all results to sheet
  if (allResults.length > 0) {
    resultsSheet.getRange(2, 1, allResults.length, headers.length).setValues(allResults);
    
    // Format the results
    resultsSheet.getRange(2, 6, allResults.length, 1).setNumberFormat('$#,##0.00'); // Price
    resultsSheet.getRange(2, 7, allResults.length, 1).setNumberFormat('0.00'); // RVOL
    resultsSheet.getRange(2, 9, allResults.length, 1).setNumberFormat('0.0'); // Float
    resultsSheet.getRange(2, 10, allResults.length, 1).setNumberFormat('0.0'); // SI%
    resultsSheet.getRange(2, 11, allResults.length, 1).setNumberFormat('0.0'); // DTC
    resultsSheet.getRange(2, 12, allResults.length, 1).setNumberFormat('0.0'); // BorrowFee
    
    // Add conditional formatting for AI Scores
    const scoreRange = resultsSheet.getRange(2, 4, allResults.length, 1);
    const rules = [];
    rules.push(SpreadsheetApp.newConditionalFormatRule()
      .whenNumberGreaterThanOrEqualTo(80)
      .setBackground('#00FF00').setFontColor('#000').setBold(true)
      .setRanges([scoreRange]).build());
    rules.push(SpreadsheetApp.newConditionalFormatRule()
      .whenNumberBetween(70, 79)
      .setBackground('#90EE90')
      .setRanges([scoreRange]).build());
    rules.push(SpreadsheetApp.newConditionalFormatRule()
      .whenNumberBetween(60, 69)
      .setBackground('#FFFF99')
      .setRanges([scoreRange]).build());
    resultsSheet.setConditionalFormatRules(rules);
    
    // Auto-resize and add filter
    resultsSheet.autoResizeColumns(1, headers.length);
    resultsSheet.getRange(1, 1, resultsSheet.getLastRow(), headers.length).createFilter();
  }
  
  // Create summary
  const summary = Object.keys(filterSummary).map(name => {
    const stats = filterSummary[name];
    if (stats.error) return `${name}: ERROR`;
    return `${name}: ${stats.totalQualified} qualified, ${stats.withSignals} signals`;
  }).join(' | ');
  
  updateStatus_({ step: 'ALL_FILTERS_COMPLETE' });
  
  // Show summary dialog
  const ui = SpreadsheetApp.getUi();
  ui.alert(
    'All Filters Complete',
    `Processed ${filters.length} filters.\n\nResults Summary:\n${summary}\n\nTotal consolidated results: ${allResults.length}\n\nSee 'All_Filters_Results' sheet for detailed comparison.`,
    ui.ButtonSet.OK
  );
  
  return summary;
}

function generateFilterComparisonReport() {
  const ss = SpreadsheetApp.getActive();
  const resultsSheet = ss.getSheetByName('All_Filters_Results');
  
  if (!resultsSheet || resultsSheet.getLastRow() < 2) {
    SpreadsheetApp.getUi().alert('No Results', 'Run "Run All Filters" first to generate comparison data.', SpreadsheetApp.getUi().ButtonSet.OK);
    return;
  }
  
  // Create comparison report sheet
  let reportSheet = ss.getSheetByName('Filter_Comparison_Report');
  if (reportSheet) ss.deleteSheet(reportSheet);
  reportSheet = ss.insertSheet('Filter_Comparison_Report');
  
  const data = resultsSheet.getDataRange().getValues();
  const headers = data[0];
  const rows = data.slice(1);
  
  // Analyze results by filter
  const filterStats = {};
  const tickersByFilter = {};
  
  rows.forEach(row => {
    const filterName = row[0];
    const ticker = row[1];
    const aiScore = row[3];
    const signal = row[4];
    
    if (!filterStats[filterName]) {
      filterStats[filterName] = {
        totalSignals: 0,
        strongBuy: 0,
        buy: 0,
        specBuy: 0,
        watch: 0,
        avgScore: 0,
        maxScore: 0,
        tickers: []
      };
      tickersByFilter[filterName] = new Set();
    }
    
    const stats = filterStats[filterName];
    stats.totalSignals++;
    stats.maxScore = Math.max(stats.maxScore, aiScore);
    stats.avgScore += aiScore;
    stats.tickers.push({ ticker, score: aiScore, signal });
    tickersByFilter[filterName].add(ticker);
    
    switch (signal) {
      case 'STRONG BUY': stats.strongBuy++; break;
      case 'BUY': stats.buy++; break;
      case 'SPEC BUY': stats.specBuy++; break;
      default: stats.watch++; break;
    }
  });
  
  // Calculate averages
  Object.keys(filterStats).forEach(filterName => {
    const stats = filterStats[filterName];
    stats.avgScore = stats.totalSignals > 0 ? (stats.avgScore / stats.totalSignals).toFixed(1) : 0;
  });
  
  // Write comparison report
  const reportHeaders = [
    'Filter Name', 'Total Signals', 'STRONG BUY', 'BUY', 'SPEC BUY', 'WATCH',
    'Avg Score', 'Max Score', 'Unique Tickers', 'Top 3 Tickers'
  ];
  
  reportSheet.getRange(1, 1, 1, reportHeaders.length).setValues([reportHeaders]);
  reportSheet.setFrozenRows(1);
  
  const reportRows = Object.keys(filterStats).map(filterName => {
    const stats = filterStats[filterName];
    const top3 = stats.tickers
      .sort((a, b) => b.score - a.score)
      .slice(0, 3)
      .map(t => `${t.ticker}(${t.score})`)
      .join(', ');
    
    return [
      filterName,
      stats.totalSignals,
      stats.strongBuy,
      stats.buy,
      stats.specBuy,
      stats.watch,
      stats.avgScore,
      stats.maxScore,
      tickersByFilter[filterName].size,
      top3
    ];
  });
  
  reportSheet.getRange(2, 1, reportRows.length, reportHeaders.length).setValues(reportRows);
  
  // Format the report
  reportSheet.autoResizeColumns(1, reportHeaders.length);
  reportSheet.getRange(1, 1, reportSheet.getLastRow(), reportHeaders.length).createFilter();
  
  // Add summary at the top
  reportSheet.insertRowsBefore(1, 3);
  reportSheet.getRange(1, 1).setValue('FILTER COMPARISON SUMMARY');
  reportSheet.getRange(1, 1).setFontWeight('bold').setFontSize(14);
  
  const totalResults = rows.length;
  const uniqueTickers = new Set(rows.map(r => r[1])).size;
  const bestFilter = Object.keys(filterStats).reduce((a, b) => 
    filterStats[a].maxScore > filterStats[b].maxScore ? a : b
  );
  
  reportSheet.getRange(2, 1).setValue(
    `Total Results: ${totalResults} | Unique Tickers: ${uniqueTickers} | Best Performing Filter: ${bestFilter}`
  );
  
  SpreadsheetApp.getActive().setActiveSheet(reportSheet);
  SpreadsheetApp.getUi().alert(
    'Comparison Report Generated',
    `Filter comparison report created with ${Object.keys(filterStats).length} filters analyzed.\n\nBest performing: ${bestFilter}\nTotal unique tickers found: ${uniqueTickers}`,
    SpreadsheetApp.getUi().ButtonSet.OK
  );
}

function getSignalFromScore(score, pattern) {
  if (score >= 85 && pattern === 'SQUEEZE') return 'STRONG BUY';
  if (score >= 75) return 'BUY';
  if (score >= 65) return 'SPEC BUY';
  return 'WATCH';
}

/** -------------------- AI AGENT (USES ACTIVE FILTER) ---------------------- */

function runAITradingAgent() {
  const ss = SpreadsheetApp.getActive();
  const activeFilter = getActiveFilter_();
  
  try {
    let agent = ss.getSheetByName(SHEETS.agent);
    if (agent) ss.deleteSheet(agent);
    agent = ss.insertSheet(SHEETS.agent);

    const headers = [
      'Rank','Ticker','Company','AI Score','Signal',
      'Current Price','Entry','Stop','T1 (+10%)','T2 (+15%)',
      'Stretch1 (+25%)','Stretch2 (+40%)','R:R (to T1)','Expected to T1',
      'RVOL','Pattern','Catalyst','Filter Used','Notes','Status','Generated At'
    ];
    agent.getRange(1,1,1,headers.length).setValues([headers]);
    agent.setFrozenRows(1);

    const candidates = scanCandidatesFromMarket_(activeFilter);
    if (!candidates.length) {
      agent.getRange(2,1).setValue(`No qualifying candidates found using filter: ${activeFilter.Name}`);
      SpreadsheetApp.getUi().alert('No Candidates', `No squeeze candidates found using filter: ${activeFilter.Name}`, SpreadsheetApp.getUi().ButtonSet.OK);
      return;
    }

    const scored = candidates.map(s => ({
      ...s,
      aiScore: scoreSqueeze_(s, activeFilter),
      pattern: detectPattern_(s, activeFilter),
    })).sort((a,b) => b.aiScore - a.aiScore);

    const top = scored.slice(0, Math.min(12, scored.length));
    const signals = top.map((s, i) => buildSqueezePlan_(s, i+1, activeFilter));

    if (signals.length) {
      const rows = signals.map(s => [
        s.rank, s.ticker, s.company, s.aiScore, s.signal,
        s.currentPrice, s.entry, s.stop, s.t1, s.t2, s.stretch1, s.stretch2,
        s.rrToT1, s.expectedToT1, s.rvol, s.pattern, s.catalyst, s.filterUsed,
        s.notes, s.status, s.generatedAt
      ]);
      agent.getRange(2,1,rows.length,headers.length).setValues(rows);
      formatAgentSheet_(agent);
      logPicks_(signals);
      
      // Show results
      SpreadsheetApp.getUi().alert(
        `AI Analysis Complete - ${activeFilter.Name}`,
        `Generated ${signals.length} trading signals using filter: ${activeFilter.Name}\n\nTop 3:\n` +
        signals.slice(0,3).map(s => `${s.rank}. ${s.ticker} - ${s.signal} (Score: ${s.aiScore})`).join('\n'),
        SpreadsheetApp.getUi().ButtonSet.OK
      );
    } else {
      agent.getRange(2,1).setValue('No qualifying signals generated.');
    }
  } catch (err) {
    SpreadsheetApp.getUi().alert('Error', 'Error running AI Agent: ' + err, SpreadsheetApp.getUi().ButtonSet.OK);
  }
}

function scanCandidatesFromMarket_(filter) {
  const ss = SpreadsheetApp.getActive();
  const sheet = ss.getSheetByName(SHEETS.market);
  if (!sheet || sheet.getLastRow() < 2) return [];
  
  const n = sheet.getLastRow() - 1;
  const data = sheet.getRange(2,1,n, MARKET_HEADER.length).getValues();

  const out = [];
  for (const row of data) {
    const [ticker,name,price,volume,dollarVol,avgVol,rvol,prevClose,chgPct,high52,distHigh,category,
           floatM, siPct, dtc, borrowFee, catalyst, newsScore] = row;
    if (!ticker || !price) continue;

    // Additional filtering for AI candidates (stricter than base qualification)
    if (price >= filter.PriceMin && price <= filter.PriceMax &&
        Number(avgVol) >= filter.MinAvgVol10D &&
        Number(rvol) >= filter.MinRVOL_Act && // Use Act threshold for AI
        Number(floatM) > 0 && Number(floatM) < filter.MaxFloatM &&
        Number(siPct) >= filter.MinSIpct &&
        Number(dtc) >= filter.MinDTC &&
        Number(borrowFee) >= filter.MinBorrowFee) {

      out.push({
        ticker: String(ticker),
        name: String(name || ''),
        price: Number(price) || 0,
        rvol: Number(rvol) || 0,
        chgPct: Number(chgPct) || 0,
        distHigh: Number(distHigh) || 0,
        floatM: Number(floatM) || 0,
        siPct: Number(siPct) || 0,
        dtc: Number(dtc) || 0,
        borrowFee: Number(borrowFee) || 0,
        catalyst: String(catalyst || ''),
        newsScore: Number(newsScore) || 0,
        category: String(category || '')
      });
    }
  }
  return out;
}

function scoreSqueeze_(s, filter) {
  let momentum = 0;
  if (s.chgPct > 0.07) momentum += 45; // >7%
  else if (s.chgPct > 0.04) momentum += 35; // >4%
  else if (s.chgPct > 0.02) momentum += 25; // >2%
  else if (s.chgPct > 0) momentum += 10;
  
  if (s.distHigh < 0.05) momentum += 20; // <5% from high

  let volume = 0;
  if (s.rvol >= filter.IgnitionRVOL) volume = 100;
  else if (s.rvol >= (filter.MinRVOL_Act + 0.5)) volume = 80;
  else if (s.rvol >= filter.MinRVOL_Act) volume = 60;
  else if (s.rvol >= filter.MinRVOL_Base) volume = 40;

  let squeeze = 0;
  if (s.siPct >= 40) squeeze += 40;
  else if (s.siPct >= 25) squeeze += 30;
  else if (s.siPct >= filter.MinSIpct) squeeze += 20;
  
  if (s.dtc >= 7) squeeze += 30;
  else if (s.dtc >= 5) squeeze += 20;
  else if (s.dtc >= filter.MinDTC) squeeze += 10;
  
  if (s.floatM < 20) squeeze += 20;
  else if (s.floatM < 35) squeeze += 10;
  
  if (s.borrowFee >= 15) squeeze += 10;
  else if (s.borrowFee >= 8) squeeze += 5;

  let technical = 50;
  if (s.distHigh < 0.03) technical += 20; // <3% from high
  if (s.chgPct > 0.03) technical += 10; // >3% move
  technical = Math.min(100, technical);

  let risk = 90;
  if (s.chgPct > 0.15) risk -= 35; // >15% overextended
  else if (s.chgPct > 0.10) risk -= 20; // >10%
  else if (s.chgPct > 0.07) risk -= 10; // >7%

  const weights = { momentum:0.30, volume:0.25, technical:0.15, squeeze:0.20, risk:0.10 };
  return Math.round(
    momentum * weights.momentum + 
    volume * weights.volume + 
    technical * weights.technical + 
    squeeze * weights.squeeze + 
    risk * weights.risk
  );
}

function detectPattern_(s, filter) {
  if (s.rvol >= filter.IgnitionRVOL && s.chgPct >= filter.IgnitionDeltaPct) return 'SQUEEZE';
  if (s.distHigh <= filter.BreakoutDistPct) return 'BREAKOUT';
  if (s.rvol >= filter.MinRVOL_Act && s.chgPct >= 0.01) return 'MOMENTUM';
  if (s.chgPct > 0 && s.rvol >= 1.5) return 'ACCUMULATION';
  return 'BUILDING';
}

function buildSqueezePlan_(s, rank, filter) {
  const p = s.price || 0;
  const entry = p * 1.0025;
  const stopPct = s.rvol >= filter.IgnitionRVOL && s.chgPct >= filter.IgnitionDeltaPct ? 0.06 : 0.055;
  const stop = entry * (1 - stopPct);

  const t1 = entry * 1.10;
  const t2 = entry * 1.15;
  const st1 = entry * 1.25;
  const st2 = entry * 1.40;
  
  const rrToT1 = ((t1 - entry) / (entry - stop)).toFixed(2);
  const expectedToT1 = ((t1/entry - 1) * 100).toFixed(1) + '%';

  let signal = 'WATCH';
  const score = scoreSqueeze_(s, filter);
  if (score >= 85 && s.rvol >= filter.IgnitionRVOL) signal = 'STRONG BUY';
  else if (score >= 75) signal = 'BUY';
  else if (score >= 65) signal = 'SPEC BUY';

  const notes = [
    `Float ${s.floatM.toFixed(1)}M`,
    `SI% ${s.siPct.toFixed(1)}`,
    `DTC ${s.dtc.toFixed(1)}`,
    `Fee ${s.borrowFee.toFixed(1)}%`,
    `RVOL ${s.rvol.toFixed(2)}`,
    `Δ% ${(s.chgPct*100).toFixed(1)}`,
    `DistHi ${(s.distHigh*100).toFixed(1)}%`
  ].join(' • ');

  return {
    rank,
    ticker: s.ticker,
    company: s.name,
    aiScore: score,
    signal,
    currentPrice: p.toFixed(2),
    entry: entry.toFixed(2),
    stop: stop.toFixed(2),
    t1: t1.toFixed(2),
    t2: t2.toFixed(2),
    stretch1: st1.toFixed(2),
    stretch2: st2.toFixed(2),
    rrToT1,
    expectedToT1,
    rvol: s.rvol.toFixed(2),
    pattern: detectPattern_(s, filter),
    catalyst: s.catalyst || '',
    filterUsed: filter.Name,
    notes,
    status: 'PENDING',
    generatedAt: new Date().toLocaleString()
  };
}

function formatAgentSheet_(sheet) {
  if (sheet.getLastRow() < 2) return;
  sheet.getRange('F2:L').setNumberFormat('$#,##0.00');
  sheet.getRange('N2:N').setNumberFormat('0.0%');
  sheet.getRange('O2:O').setNumberFormat('0.00');
  sheet.autoResizeColumns(1, Math.min(21, sheet.getLastColumn()));
  
  if (sheet.getLastRow() > 1) {
    sheet.getRange(1,1, sheet.getLastRow(), sheet.getLastColumn()).createFilter();
  }
}

function logPicks_(signals) {
  if (!signals || !signals.length) return;
  const ss = SpreadsheetApp.getActive();
  let tracker = ss.getSheetByName(SHEETS.tracker);
  
  if (!tracker) {
    tracker = ss.insertSheet(SHEETS.tracker);
    tracker.getRange(1,1,1,13).setValues([[
      'Date','Ticker','Entry','Stop','T1','T2','Stretch1','Stretch2','R:R','Filter','Result','P&L %','Status'
    ]]);
    tracker.setFrozenRows(1);
  }
  
  const date = new Date();
  const rows = signals.slice(0, Math.min(10, signals.length)).map(s => [
    date, s.ticker, s.entry, s.stop, s.t1, s.t2, s.stretch1, s.stretch2, s.rrToT1, s.filterUsed, '', '', 'OPEN'
  ]);
  
  const lr = tracker.getLastRow();
  tracker.getRange(lr+1,1, rows.length, 13).setValues(rows);
}

/** -------------------- DATA FETCHING & UTILITIES ----------------------- */

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
    const sym = (parts[0] || '').trim();
    const name = (parts[1] || '').trim();
    const isTestIssue = (parts[3] || '').trim() === 'Y';
    const isETF = (parts[5] || '').trim() === 'Y';
    if (!sym || isTestIssue || isETF) continue;
    if (!/^[A-Z.\-]+$/.test(sym)) continue;
    if (sym.endsWith('W') || sym.endsWith('WS') || sym.endsWith('U')) continue;
    out.push({ ticker: sym, name });
  }
  return out;
}

function ensureFundamentalsSheet() {
  const ss = SpreadsheetApp.getActive();
  let sheet = ss.getSheetByName(SHEETS.fund);
  if (!sheet) sheet = ss.insertSheet(SHEETS.fund);
  
  const headers = ['Ticker','Float(M)','SI%Float','DTC','BorrowFee%','Catalyst','NewsScore','LastUpdated'];
  if (sheet.getLastRow() < 1) {
    sheet.getRange(1,1,1,headers.length).setValues([headers]);
    sheet.setFrozenRows(1);
  } else {
    sheet.getRange(1,1,1,headers.length).setValues([headers]);
  }
  sheet.autoResizeColumns(1, headers.length);
}

/** -------------------- STATUS & LOGGING --------------------------- */

function getBackgroundStatus() {
  const ss = SpreadsheetApp.getActive();
  const props = PropertiesService.getUserProperties();
  const status = JSON.parse(props.getProperty(BG_CFG.statusKey) || '{}');
  const state = JSON.parse(props.getProperty(BG_CFG.stateKey) || '{"nextIndex":0}');
  const symbols = JSON.parse(props.getProperty(BG_CFG.symbolsKey) || '[]');

  const staging = ss.getSheetByName(SHEETS.staging);
  const mc = ss.getSheetByName(SHEETS.market);

  let stagingRows = 0, ready = 0, qualified = 0, exported = 0;
  if (staging && staging.getLastRow() > 2) {
    stagingRows = staging.getLastRow() - 2;
    const data = staging.getRange(3,1,stagingRows,STAGING_HEADER.length).getValues();
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
  const curr = JSON.parse(props.getProperty(BG_CFG.statusKey) || '{}');
  const next = Object.assign({}, curr, patch, { updatedAt: new Date().toISOString() });
  props.setProperty(BG_CFG.statusKey, JSON.stringify(next));
}

function appendLog_(msg) {
  const ss = SpreadsheetApp.getActive();
  const log = ensureSheet_(ss, SHEETS.log);
  log.appendRow([new Date(), msg]);
  log.autoResizeColumns(1, 2);
}

function debugBackgroundState() {
  const ss = SpreadsheetApp.getActive();
  const props = PropertiesService.getUserProperties();
  const staging = ss.getSheetByName(SHEETS.staging);
  const mc = ss.getSheetByName(SHEETS.market);

  const state = JSON.parse(props.getProperty(BG_CFG.stateKey) || '{"nextIndex":null}');
  const symJ = props.getProperty(BG_CFG.symbolsKey);
  const symLen = symJ ? JSON.parse(symJ).length : 0;

  let stagingRows = 0, ready = 0, qualified = 0, exported = 0;
  if (staging && staging.getLastRow() > 2) {
    stagingRows = staging.getLastRow() - 2;
    const data = staging.getRange(3,1,stagingRows,STAGING_HEADER.length).getValues();
    for (const r of data) {
      const cat = r[11], qual = r[12], exp = r[13];
      if (cat && cat !== 'PENDING') ready++;
      if (qual === '✓ QUALIFIED') qualified++;
      if (exp === 'YES') exported++;
    }
  }
  const mcRows = mc && mc.getLastRow() > 1 ? mc.getLastRow() - 1 : 0;

  const activeFilter = getActiveFilterName() || 'None';
  const msg = [
    `Active Filter: ${activeFilter}`,
    `Symbols cached: ${symLen}`,
    `Next index: ${state.nextIndex}`,
    `Staging rows: ${stagingRows}`,
    `Ready (Category computed): ${ready}`,
    `Qualified (✓): ${qualified}`,
    `Exported (YES): ${exported}`,
    `MarketCache rows: ${mcRows}`
  ].join('\n');
  
  SpreadsheetApp.getUi().alert('Background Scanner Status', msg, SpreadsheetApp.getUi().ButtonSet.OK);
}

function ensureSheet_(ss, name) {
  const s = ss.getSheetByName(name);
  return s ? s : ss.insertSheet(name);
}

/** -------------------- RESET FUNCTIONS --------------------------- */

function resetScannerStateSoft() {
  const ss = SpreadsheetApp.getActive();
  const staging = ss.getSheetByName(SHEETS.staging);
  const props = PropertiesService.getUserProperties();
  
  props.deleteProperty(BG_CFG.statusKey);
  if (props.getProperty(BG_CFG.symbolsKey)) {
    props.setProperty(BG_CFG.stateKey, JSON.stringify({ nextIndex: 0 }));
  } else {
    props.deleteProperty(BG_CFG.stateKey);
  }
  
  if (staging && staging.getLastRow() > 2) {
    staging.getRange(3, 14, staging.getLastRow() - 2, 1).clearContent();
  }
  
  SpreadsheetApp.getActive().toast('Soft reset complete: state cleared, export flags reset.');
}

function hardResetScanner() {
  ScriptApp.getProjectTriggers().forEach(t => {
    if (t.getHandlerFunction() === 'scanNasdaqInBackground') ScriptApp.deleteTrigger(t);
  });
  
  const ss = SpreadsheetApp.getActive();
  [SHEETS.staging, SHEETS.market, SHEETS.log, SHEETS.agent, SHEETS.tracker, SHEETS.fund].forEach(name => {
    const s = ss.getSheetByName(name);
    if (s) ss.deleteSheet(s);
  });
  
  const props = PropertiesService.getUserProperties();
  [BG_CFG.statusKey, BG_CFG.stateKey, BG_CFG.symbolsKey].forEach(k => props.deleteProperty(k));
  
  SpreadsheetApp.getActive().toast('Hard reset complete: sheets removed, triggers stopped, state wiped.');
}

/** -------------------- RUN ALL FILTERS FEATURE --------------------------- */

function runAllFilters() {
  const ss = SpreadsheetApp.getActive();
  
  // Ensure we have staging data first
  if (!ss.getSheetByName(SHEETS.staging) || ss.getSheetByName(SHEETS.staging).getLastRow() < 100) {
    const props = PropertiesService.getUserProperties();
    updateStatus_({ step:'BUILD_FOR_ALL_FILTERS' });
    buildStaging_(ss, props);
    addNextChunk_(ss, props);
    addNextChunk_(ss, props);
  }
  
  const filters = getAllFilters();
  if (filters.length === 0) {
    throw new Error('No filters found. Run Setup SavedFilters first.');
  }
  
  // Create consolidated results sheet
  let resultsSheet = ss.getSheetByName('All_Filters_Results');
  if (resultsSheet) ss.deleteSheet(resultsSheet);
  resultsSheet = ss.insertSheet('All_Filters_Results');
  
  const headers = [
    'Filter Name', 'Ticker', 'Company', 'AI Score', 'Signal', 'Price', 'RVOL', 
    'Change %', 'Float(M)', 'SI%', 'DTC', 'BorrowFee%', 'Pattern', 'Qualified'
  ];
  resultsSheet.getRange(1, 1, 1, headers.length).setValues([headers]);
  resultsSheet.setFrozenRows(1);
  
  let allResults = [];
  let filterSummary = {};
  
  // Process each filter
  for (let i = 0; i < filters.length; i++) {
    const filter = filters[i];
    updateStatus_({ step: `PROCESSING_FILTER_${i+1}/${filters.length}`, lastFilter: filter.Name });
    
    try {
      setActiveFilterName(filter.Name);
      
      // Clear and rebuild MarketCache with this filter's criteria
      const mcSheet = ss.getSheetByName(SHEETS.market);
      if (mcSheet && mcSheet.getLastRow() > 1) {
        mcSheet.deleteRows(2, mcSheet.getLastRow() - 1);
      }
      
      // Clear export flags and re-export with new criteria
      const stagingSheet = ss.getSheetByName(SHEETS.staging);
      if (stagingSheet && stagingSheet.getLastRow() > 2) {
        stagingSheet.getRange(3, 14, stagingSheet.getLastRow() - 2, 1).clearContent();
      }
      
      exportQualified_(ss);
      const candidates = scanCandidatesFromMarket_(filter);
      
      const scored = candidates.map(s => ({
        ...s,
        aiScore: scoreSqueeze_(s, filter),
        pattern: detectPattern_(s, filter),
        filterName: filter.Name
      })).sort((a,b) => b.aiScore - a.aiScore);
      
      const topCandidates = scored.slice(0, 5);
      
      filterSummary[filter.Name] = {
        totalQualified: candidates.length,
        withSignals: topCandidates.length,
        topScore: topCandidates.length > 0 ? topCandidates[0].aiScore : 0
      };
      
      topCandidates.forEach(candidate => {
        const signal = getSignalFromScore(candidate.aiScore, candidate.pattern);
        allResults.push([
          filter.Name, candidate.ticker, candidate.name, candidate.aiScore, signal,
          candidate.price, candidate.rvol, (candidate.chgPct * 100).toFixed(1) + '%',
          candidate.floatM, candidate.siPct, candidate.dtc, candidate.borrowFee,
          candidate.pattern, 'YES'
        ]);
      });
      
    } catch (error) {
      console.log(`Error processing filter ${filter.Name}: ${error}`);
      filterSummary[filter.Name] = { error: error.toString() };
    }
  }
  
  if (allResults.length > 0) {
    resultsSheet.getRange(2, 1, allResults.length, headers.length).setValues(allResults);
    
    // Format the results
    resultsSheet.getRange(2, 6, allResults.length, 1).setNumberFormat('$#,##0.00'); // Price
    resultsSheet.getRange(2, 7, allResults.length, 1).setNumberFormat('0.00'); // RVOL
    resultsSheet.getRange(2, 9, allResults.length, 1).setNumberFormat('0.0'); // Float
    resultsSheet.getRange(2, 10, allResults.length, 1).setNumberFormat('0.0'); // SI%
    resultsSheet.getRange(2, 11, allResults.length, 1).setNumberFormat('0.0'); // DTC
    resultsSheet.getRange(2, 12, allResults.length, 1).setNumberFormat('0.0'); // BorrowFee
    
    // Add conditional formatting for AI Scores
    const scoreRange = resultsSheet.getRange(2, 4, allResults.length, 1);
    const rules = [];
    rules.push(SpreadsheetApp.newConditionalFormatRule()
      .whenNumberGreaterThanOrEqualTo(80)
      .setBackground('#00FF00').setFontColor('#000').setBold(true)
      .setRanges([scoreRange]).build());
    rules.push(SpreadsheetApp.newConditionalFormatRule()
      .whenNumberBetween(70, 79)
      .setBackground('#90EE90')
      .setRanges([scoreRange]).build());
    rules.push(SpreadsheetApp.newConditionalFormatRule()
      .whenNumberBetween(60, 69)
      .setBackground('#FFFF99')
      .setRanges([scoreRange]).build());
    resultsSheet.setConditionalFormatRules(rules);
    
    resultsSheet.autoResizeColumns(1, headers.length);
    resultsSheet.getRange(1, 1, resultsSheet.getLastRow(), headers.length).createFilter();
  }
  
  const summary = Object.keys(filterSummary).map(name => {
    const stats = filterSummary[name];
    if (stats.error) return `${name}: ERROR`;
    return `${name}: ${stats.totalQualified} qualified, ${stats.withSignals} signals`;
  }).join(' | ');
  
  updateStatus_({ step: 'ALL_FILTERS_COMPLETE' });
  
  SpreadsheetApp.getUi().alert(
    'All Filters Complete',
    `Processed ${filters.length} filters.\n\nResults Summary:\n${summary}\n\nTotal results: ${allResults.length}\n\nSee 'All_Filters_Results' sheet.`,
    SpreadsheetApp.getUi().ButtonSet.OK
  );
  
  return summary;
}

function getSignalFromScore(score, pattern) {
  if (score >= 85 && pattern === 'SQUEEZE') return 'STRONG BUY';
  if (score >= 75) return 'BUY';
  if (score >= 65) return 'SPEC BUY';
  return 'WATCH';
}

function onOpen() {
  const ui = SpreadsheetApp.getUi();

  ui.createMenu('🤖 AI Agent')
    .addItem('Command Center (Sidebar)', 'openCommandCenter')
    .addSeparator()
    .addItem('Setup SavedFilters', 'setupSavedFilters')
    .addItem('Switch Filter & Reprocess', 'switchFilterDialog')
    .addSeparator()
    .addItem('🔍 Run All Filters', 'runAllFilters')
    .addSeparator()
    .addItem('Start Background (5 min)', 'startBackgroundScanner')
    .addItem('Stop Background', 'stopBackgroundScanner')
    .addItem('Run Scan Once', 'runScanOnce')
    .addItem('Run Full Now', 'runFullProcessNow')
    .addSeparator()
    .addItem('Run AI Scoring Now', 'runAITradingAgent')
    .addItem('Debug Status', 'debugBackgroundState')
    .addToUi();
}

/** ===================== END OF REFACTORED CODE ===================== */