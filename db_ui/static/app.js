'use strict';
const $ = id => document.getElementById(id);
const state = {view:'journal',table:null,catalog:null,offset:0,limit:50,sort:null,direction:'desc',schema:false,version:0};
const number = value => Number(value).toLocaleString();
const missing = value => value === null || value === undefined;
const compact = value => value?.length > 24 ? `${value.slice(0,9)}…${value.slice(-7)}` : value;
const formatMs = value => missing(value) ? '—' : `${Number(value).toLocaleString(undefined,{maximumFractionDigits:2})} ms`;
const node = (tag,text,className) => {const element=document.createElement(tag);if(text!==undefined)element.textContent=text;if(className)element.className=className;return element;};
function error(message){$('error').textContent=message||'';$('error').hidden=!message;}
function toast(message){$('toast').textContent=message;$('toast').hidden=false;setTimeout(()=>$('toast').hidden=true,1800);}
async function api(path,options){const response=await fetch(path,options);const result=await response.json();if(!response.ok)throw new Error(result.error||'Could not load data');return result;}
function queryString(params){return new URLSearchParams(params).toString();}
function badge(value){const valid=['landed','failed','unknown','submitting','prepared','skipped','unsupported'];return node('span',value||'—',`badge ${valid.includes(value)?value:''}`);}
function parsed(value){try{return JSON.parse(value||'{}');}catch{return {};}}
async function loadCatalog(){
  state.catalog=await api('/api/catalog');
  $('database-name').textContent=state.catalog.filename;
  $('database-size').textContent=`${(state.catalog.size/1024/1024).toFixed(2)} MB on disk`;
  $('table-count').textContent=state.catalog.tables.length;
  $('table-list').replaceChildren();
  for(const table of state.catalog.tables){
    const button=node('button',undefined,'table-nav');button.dataset.table=table.name;
    button.append(node('span','▦','table-icon'),node('span',table.name),node('span',number(table.count),'count'));
    button.onclick=()=>selectView('table',table.name);$('table-list').append(button);
  }
  const totals=state.catalog.attempts;
  const total=totals.reduce((sum,row)=>sum+row.count,0);
  const count=status=>totals.filter(row=>row.status===status).reduce((sum,row)=>sum+row.count,0);
  const sources=state.catalog.tables.find(table=>table.name==='source_transactions')?.count||0;
  $('overview').replaceChildren();
  for(const [label,value,note,style] of [['Copy attempts',total,'Mainnet',''],['Landed',count('landed'),'Journal outcome','green'],['Failed',count('failed'),'Includes route and execution failures','rust'],['Source observations',sources,'Including skipped and recovered trades','']]){
    const stat=node('div',undefined,'stat');stat.append(node('div',label,'stat-label'),node('div',number(value),`stat-value ${style}`),node('div',note,'stat-note'));$('overview').append(stat);
  }
  markNavigation();
}
function markNavigation(){document.querySelectorAll('[data-view]').forEach(button=>button.classList.toggle('active',button.dataset.view===state.view));document.querySelectorAll('[data-table]').forEach(button=>button.classList.toggle('active',state.view==='table'&&button.dataset.table===state.table));}
function selectView(view,table=null){
  state.view=view;state.table=table;state.offset=0;state.sort=null;state.direction='desc';state.schema=false;state.version++;
  $('search').value='';error('');
  const title=view==='journal'?'Copy journal':view==='sql'?'SQL console':view==='flow'?'Execution flow':table;
  $('title').textContent=title;$('breadcrumb').textContent=title;document.title=`${title} · Database workbench`;
  $('subtitle').textContent=view==='journal'?'Follow each source trade through execution.':view==='sql'?'Ask questions of your journal. Your database stays unchanged.':view==='flow'?'See what overlaps, what waits, and what runs in the background.':'Browse records and inspect the table structure.';
  $('flow-panel').hidden=view!=='flow';$('records-panel').hidden=view==='flow';$('workspace-hint').hidden=view==='flow';
  $('overview').hidden=view!=='journal';$('sql-panel').hidden=view!=='sql';$('filters').hidden=view==='sql';$('target').hidden=view!=='journal';$('status').hidden=view!=='journal';$('schema-tab').hidden=view!=='table';$('schema-note').hidden=true;
  $('search').placeholder=view==='journal'?'Find a signature or pool…':'Search across table fields…';
  $('workspace-hint').textContent=view==='journal'?'Select a trade to inspect its source, copy, and timing details.':view==='sql'?'Queries are limited to 500 rows and 2 seconds. Select a result to inspect its values.':'Select a row to inspect its values. Click a column header to sort.';
  document.body.classList.toggle('sql-mode',view==='sql');markNavigation();setSchema(false);
  history.replaceState(null,'',view==='table'?`#table/${encodeURIComponent(table)}`:`#${view}`);
  if(view==='flow'){loadFlowStats();return;}
  if(view==='sql'){
    $('grid').replaceChildren(empty('Ready when you are','Write a query above, or choose an example.'));
    $('result-info').textContent='No query run yet';$('pagination').hidden=true;
  }else loadRows();
}
async function loadFlowStats(){
  const version=state.version;
  const content=$('flow-averages-content');
  content.textContent='Loading averages…';
  try{
    const result=await api('/api/flow-stats');
    if(version!==state.version||state.view!=='flow')return;
    const table=node('table');
    const header=node('tr');
    for(const label of ['Path','Trades','Receipt → send','Preparation','Mint RPC','Route','Build','Sender','Landing gap'])header.append(node('th',label));
    table.append(node('thead'));table.tHead.append(header);
    const body=node('tbody');
    const paths=[['pump_fun_source','Pump.fun · source metadata'],['pump_fun_rpc','Pump.fun · mint RPC'],['pump_swap_source','PumpSwap · source instruction']];
    const fields=['receipt_to_send_start_ms','preparation_ms','mint_read_ms','route_wall_ms','route_instruction_build_ms','sender_request_ms','slot_delta'];
    for(const [key,label] of paths){
      const group=result.groups[key]||{count:0,metrics:{}};
      const row=node('tr');row.append(node('td',label),node('td',number(group.count)));
      for(const field of fields){
        const sample=group.metrics[field];
        const cell=node('td',sample?(field==='slot_delta'?`${sample.average} slots`:formatMs(sample.average)):'—');
        if(sample&&sample.count!==group.count)cell.title=`${sample.count} trades have this timing`;
        row.append(cell);
      }
      body.append(row);
    }
    table.append(body);content.replaceChildren(table);
    $('refresh-time').textContent=`Updated ${new Date().toLocaleTimeString([], {hour:'2-digit',minute:'2-digit',second:'2-digit'})}`;
  }catch(cause){if(version===state.version&&state.view==='flow')content.textContent=`Averages unavailable: ${cause.message}`;}
}
function empty(title,description){const element=node('div',undefined,'empty');element.append(node('strong',title),node('span',description));return element;}
async function loadRows(){
  if(state.view==='flow')return;
  const version=++state.version;
  error('');$('result-info').textContent='Loading…';$('previous').disabled=true;$('next').disabled=true;
  const params={limit:state.limit,offset:state.offset,search:$('search').value.trim()};
  const path=state.view==='journal'?'/api/journal?'+queryString({...params,target:$('target').value,status:$('status').value}):'/api/table?'+queryString({...params,name:state.table,direction:state.direction,...(state.sort?{sort:state.sort}:{})});
  try{
    const result=await api(path);if(version!==state.version)return;
    if(result.sort)state.sort=result.sort;
    renderGrid(result,state.view==='journal');
    $('result-info').textContent=`${number(result.total)} ${state.view==='journal'?'observations':'records'}`;
    $('page-info').textContent=result.total?`${number(state.offset+1)}–${number(Math.min(state.offset+result.rows.length,result.total))} of ${number(result.total)}`:'0 records';
    $('previous').disabled=state.offset===0;$('next').disabled=state.offset+result.rows.length>=result.total;
    $('pagination').hidden=state.schema;
    $('schema-note').hidden=state.view!=='journal'||result.has_landed_slots;
    $('refresh-time').textContent=`Updated ${new Date().toLocaleTimeString([], {hour:'2-digit',minute:'2-digit',second:'2-digit'})}`;
  }catch(cause){if(version!==state.version)return;error(cause.message);$('result-info').textContent='Could not load records';$('grid').replaceChildren(empty('Unable to load records','Use Refresh to try again.'));}
}
function renderGrid(result,journal=false){
  if(!result.rows.length){$('grid').replaceChildren(empty('No matching records','Try another filter, search, or query.'));return;}
  const table=node('table');const head=node('thead');const header=node('tr');const body=node('tbody');
  const columns=journal?['Trade','Status','Target','DEX','Source slot','Copy slot','Slot gap','Route','Received → send','Created']:result.columns;
  for(const name of columns){const th=node('th');if(state.view==='table'&&!journal){const button=node('button',name+(state.sort===name?(state.direction==='desc'?' ↓':' ↑'):''));button.onclick=()=>{state.direction=state.sort===name&&state.direction==='desc'?'asc':'desc';state.sort=name;state.offset=0;loadRows();};th.append(button);}else th.textContent=name;header.append(th);}
  head.append(header);table.append(head);
  result.rows.forEach((row,index)=>{
    const tr=node('tr');tr.tabIndex=0;tr.setAttribute('aria-label',`Inspect row ${index+1}`);
    const data=Object.fromEntries(result.columns.map((column,i)=>[column,row[i]]));
    if(journal){
      const timing=parsed(data.timings_json);
      const cells=[compact(data.source_signature),data.status,data.target,data.dex||'—',data.source_slot,data.copy_slot,data.slot_delta,formatMs(data.route_latency_ms),formatMs(timing.receipt_to_send_start_ms),new Date(data.created_at*1000).toLocaleString([], {month:'short',day:'numeric',hour:'2-digit',minute:'2-digit'})];
      cells.forEach((value,i)=>{const td=node('td');if(i===1)td.append(badge(value));else if(i===2)td.append(node('span',value,'target'));else {td.textContent=missing(value)?'—':String(value);if([0,4,5,6].includes(i))td.className='mono'+(i===0?' signature':'');if(missing(value))td.classList.add('null');}if(i===0)td.title=data.source_signature;tr.append(td);});
      tr.onclick=()=>inspectTrade(data.source_signature);
    }else{
      row.forEach(value=>{const td=node('td',missing(value)?'NULL':String(value).slice(0,180),missing(value)?'null':'mono');td.title=missing(value)?'NULL':String(value).slice(0,500);tr.append(td);});
      tr.onclick=()=>inspectRow(result.columns,row);
    }
    tr.onkeydown=event=>{if(event.key==='Enter'||event.key===' '){event.preventDefault();tr.click();}};body.append(tr);
  });table.append(body);$('grid').replaceChildren(table);
}
function setSchema(show){state.schema=show;$('schema').hidden=!show;$('grid').hidden=show;$('filters').hidden=show||state.view==='sql';$('pagination').hidden=show||state.view==='sql';$('schema-tab').classList.toggle('active',show);$('data-tab').classList.toggle('active',!show);if(!show)return;
  const item=state.catalog.tables.find(table=>table.name===state.table);if(!item)return;
  const table=node('table'),head=node('thead'),tr=node('tr'),body=node('tbody');['Column','Type','Required','Primary key','Default'].forEach(label=>tr.append(node('th',label)));head.append(tr);table.append(head);
  for(const column of item.columns){const row=node('tr');[column.name,column.type,column.not_null?'Yes':'No',column.primary_key?'Yes':'—',column.default??'—'].forEach(value=>row.append(node('td',value,'mono')));body.append(row);}table.append(body);$('schema').replaceChildren(table,node('pre',item.sql));
}
function fields(entries){
  const section=node('div');
  for(const [name,value] of entries){
    const field=node('div',undefined,'record-field');field.append(node('div',name,'field-name'));const content=node('div',undefined,'field-value');
    if(missing(value)){content.append(node('span','NULL','null'));}
    else{
      let display=String(value);let isJson=false;try{const object=JSON.parse(display);if(object!==null&&typeof object==='object'){display=JSON.stringify(object,null,2);isJson=true;}}catch{}
      if(display.length>220||isJson){const details=node('details');details.append(node('summary',isJson?'Expand JSON':`Expand value (${number(display.length)} characters)`),node('pre',display));content.append(details);}else content.append(node('span',display));
      if((name.includes('signature')||name==='pool'||name==='wallet')&&display.length>20){const copy=node('button','Copy','button small');copy.onclick=async()=>{try{await navigator.clipboard.writeText(String(value));toast('Copied to clipboard');}catch{toast('Copy unavailable. Select the value to copy it.');}};content.append(node('br'),copy);}
    }field.append(content);section.append(field);
  }return section;
}
function section(title,content){const element=node('section',undefined,'detail-section');element.append(node('h3',title),content);return element;}
function openInspector(title,eyebrow){$('detail-title').textContent=title;$('detail-eyebrow').textContent=eyebrow;$('detail-body').replaceChildren();if(!$('detail').open)$('detail').showModal();}
function inspectRow(columns,row){openInspector(state.view==='table'?state.table:'Query result','Record inspector');$('detail-body').append(fields(columns.map((name,index)=>[name,row[index]])));}
async function inspectTrade(signature){
  openInspector('Trade details',compact(signature));$('detail-body').append(node('p','Loading…','muted'));
  try{
    const result=await api('/api/trade?'+queryString({signature}));$('detail-body').replaceChildren();
    const unsupported=parsed(result.source.skip_reason);
    if(result.source.status==='unsupported') $('detail-body').append(section('Unsupported',fields([['Reason',unsupported.message||result.source.skip_reason],['Code',unsupported.code||'unsupported']])));
    const timing=parsed(result.source.timings_json);
    if(Object.keys(timing).length){
      const list=node('div');for(const [key,value] of Object.entries(timing)){if(typeof value!=='number')continue;const row=node('div',undefined,'timing-row');row.append(node('span',key.replace(/_/g,' ')),node('strong',key==='mint_from_source'?(value?'Yes':'No'):key.endsWith('_us')?`${number(value)} µs`:formatMs(value)));list.append(row);}
      if(timing.database){for(const [operation,value] of Object.entries(timing.database)){const row=node('div',undefined,'timing-row');row.append(node('span',`DB · ${operation}`),node('strong',`${number(value.elapsed_us)} µs / ${value.calls} call${value.calls===1?'':'s'}`));list.append(row);}}
      $('detail-body').append(section('Execution timings',list));
    }
    if(result.copy)$('detail-body').append(section('Copy transaction',fields(Object.entries(result.copy))));
    $('detail-body').append(section('Source transaction',fields(Object.entries(result.source))));
  }catch(cause){$('detail-body').replaceChildren(node('p',cause.message,'error'));}
}
async function runQuery(){
  const version=++state.version;error('');$('run-query').disabled=true;$('run-query').textContent='Running…';$('result-info').textContent='Running query…';
  try{const result=await api('/api/query',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({sql:$('query').value})});if(version!==state.version)return;renderGrid(result);$('result-info').textContent=`${number(result.rows.length)} rows${result.truncated?' (500-row limit)':''} · ${result.elapsed_ms} ms`;$('refresh-time').textContent='Query completed';}
  catch(cause){if(version===state.version){error(cause.message);$('result-info').textContent='Query failed';$('grid').replaceChildren(empty('Query not completed','Review the message above, then edit and run your query again.'));}}
  finally{$('run-query').disabled=false;$('run-query').textContent='Run query ▸';}
}
const snippets={recent:'SELECT id, execution_target, status, route_latency_ms,\n       source_signature, local_signature\nFROM copy_attempts\nORDER BY id DESC\nLIMIT 50;',outcomes:'SELECT execution_target, status, COUNT(*) AS attempts\nFROM copy_attempts\nGROUP BY execution_target, status\nORDER BY execution_target, attempts DESC;',routing:'SELECT s.dex, a.execution_target, COUNT(*) AS timed_copies,\n       ROUND(AVG(a.route_latency_ms), 1) AS average_route_ms,\n       MIN(a.route_latency_ms) AS fastest_ms,\n       MAX(a.route_latency_ms) AS slowest_ms\nFROM copy_attempts a\nJOIN source_transactions s ON s.signature = a.source_signature\nWHERE a.route_latency_ms IS NOT NULL\nGROUP BY s.dex, a.execution_target;',timings:'SELECT signature, slot, timings_json\nFROM source_transactions\nWHERE timings_json IS NOT NULL\nORDER BY observed_at DESC\nLIMIT 50;'};
document.querySelectorAll('[data-view]').forEach(button=>button.onclick=()=>selectView(button.dataset.view));
document.querySelector('.brand').onclick=event=>{event.preventDefault();selectView('journal');};
$('data-tab').onclick=()=>setSchema(false);$('schema-tab').onclick=()=>setSchema(true);
$('close-detail').onclick=()=>$('detail').close();$('detail').onclick=event=>{if(event.target===$('detail')){const rect=$('detail').getBoundingClientRect();if(event.clientX<rect.left||event.clientX>rect.right||event.clientY<rect.top||event.clientY>rect.bottom)$('detail').close();}};
$('target').onchange=$('status').onchange=()=>{state.offset=0;loadRows();};
let searchTimer;$('search').oninput=()=>{clearTimeout(searchTimer);searchTimer=setTimeout(()=>{if(state.view==='sql')return;state.offset=0;loadRows();},250);};
$('page-size').onchange=()=>{state.limit=Number($('page-size').value);state.offset=0;loadRows();};
$('previous').onclick=()=>{state.offset=Math.max(0,state.offset-state.limit);loadRows();};$('next').onclick=()=>{state.offset+=state.limit;loadRows();};
$('run-query').onclick=runQuery;$('query').onkeydown=event=>{if(event.key==='Enter'&&(event.metaKey||event.ctrlKey)){event.preventDefault();if(!$('run-query').disabled)runQuery();}};
$('snippets').onchange=()=>{if(snippets[$('snippets').value]){$('query').value=snippets[$('snippets').value];$('query').focus();}};
$('refresh').onclick=async()=>{error('');$('refresh').disabled=true;try{await loadCatalog();if(state.view==='flow')await loadFlowStats();else if(state.view!=='sql'){await loadRows();if(state.schema)setSchema(true);}else toast('Database information refreshed');}catch(cause){error(cause.message);}finally{$('refresh').disabled=false;}};
(async()=>{try{await loadCatalog();const hash=location.hash.slice(1);if(hash.startsWith('table/')&&state.catalog.tables.some(table=>table.name===decodeURIComponent(hash.slice(6))))selectView('table',decodeURIComponent(hash.slice(6)));else selectView(['sql','flow'].includes(hash)?hash:'journal');}catch(cause){error(cause.message);$('database-name').textContent='Connection unavailable';$('grid').replaceChildren(empty('Database unavailable','Check the local server, then use Refresh.'));}})();

$('flow-journal-link').onclick=event=>{event.preventDefault();selectView('journal');};
document.querySelectorAll('[data-flow-filter]').forEach(button=>button.onclick=()=>{
  const selected=button.dataset.flowFilter;
  document.querySelectorAll('[data-flow-filter]').forEach(control=>control.setAttribute('aria-pressed',String(control===button)));
  document.querySelectorAll('[data-flow-kind]').forEach(task=>task.classList.toggle('flow-highlight',selected!=='all'&&task.dataset.flowKind.split(' ').includes(selected)));
});
