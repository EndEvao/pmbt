// Offline DOM smoke tests for the universe/comparison report templates.
import fs from 'node:fs';
import vm from 'node:vm';
import assert from 'node:assert/strict';

function check(kind, model, source) {
  const html = source ?? fs.readFileSync(new URL(`../crates/pmbt/assets/report_${kind}.html`, import.meta.url), 'utf8');
  const nodes = new Map();
  function node() {
    return {innerHTML:'', textContent:'', style:{}, attributes:{}, children:[], handlers:{},
      setAttribute(k,v){this.attributes[k]=v;},getAttribute(k){return this.attributes[k];},
      appendChild(child){this.children.push(child);return child;},
      addEventListener(name,handler){this.handlers[name]=handler;},
      getBoundingClientRect(){return {left:0,width:1080};}};
  }
  for (const [,id] of html.matchAll(/id="([^"]+)"/g)) {assert.ok(!nodes.has(id), `Duplicate ID ${id}`);nodes.set(id,node());}
  const get=id=>{assert.ok(nodes.has(id),`Unknown element ${id}`);return nodes.get(id);};
  get('report-data').textContent=JSON.stringify(model);
  const root=node();root.setAttribute('data-theme','light');
  let printed=false;
  const document={getElementById:get,createElementNS:()=>node(),documentElement:root};
  const scripts=[...html.matchAll(/<script([^>]*)>([\s\S]*?)<\/script>/g)];
  const code=scripts.find(([,attrs])=>!attrs.includes('application/json'))[2];
  vm.runInNewContext(code,{document,window:{print(){printed=true;}},innerWidth:1200},{timeout:5000});
  get('theme').handlers.click();assert.equal(root.getAttribute('data-theme'),'dark');
  get('theme').handlers.click();assert.equal(root.getAttribute('data-theme'),'light');
  get('print').handlers.click();assert.ok(printed);
  for (const [id,n] of nodes) assert.ok(!/NaN|Infinity|undefined/.test(n.innerHTML+n.textContent),`Invalid output in ${id}`);
  const table=get(kind==='batch'?'table':'config-table').innerHTML;
  assert.ok(!table.includes('<script>'));
  assert.ok(table.includes('Net marked P&L'));
  return get;
}
const row={id:'<script>bad</script>',short:'<script>bad</script>',category:'other',initial_capital:1000,marked_pnl:9,unrealized:10,return_on_capital:.009,total:-1,realized:0,settlement:0,fees:1,state:'unresolved',fills:1};
const batch={name:'test',agg:{markets:1,initial_capital:1000,marked_pnl:9,unrealized:10,ending_equity:1009,total_pnl:-1,realized_pnl:0,settlement_pnl:0,fees:1,fills:1,rejections:0,settled:0,unresolved:1,nothing_held:0},combined:[{t:1760000000000,p:0},{t:1760000010000,p:9}],markets:[row]};
const get=check('batch',batch);assert.ok(get('reconciliation').textContent.startsWith('Reconciled:'));
const missing=structuredClone(batch);missing.agg.marked_pnl=null;missing.agg.unrealized=null;missing.agg.ending_equity=null;missing.markets[0].marked_pnl=null;missing.markets[0].unrealized=null;missing.markets[0].return_on_capital=null;
assert.ok(check('batch',missing)('reconciliation').textContent.includes('unavailable'));
const empty=structuredClone(batch);Object.assign(empty.agg,{markets:0,initial_capital:0,marked_pnl:0,unrealized:0,ending_equity:0});empty.markets=[];empty.combined=[];check('batch',empty);
const run={label:'<script>bad</script>',detail:'"<script>bad</script>',marked_pnl:9,marked_return:.009,total_pnl:-1,total_return:-.001,sharpe:0,sharpe_raw:0,sharpe_defined:false,max_dd:0,fills:1,fees:1,rejections:0,expired:0,curve:batch.combined};
const compare={subtitle:'test',ladder:true,category_known:false,runs:[run,{...run,label:'second',marked_pnl:5}],waterfall:[{label:'base',delta:9,total:false},{label:'fees',delta:-4,total:false},{label:'net',delta:5,total:true}]};
assert.ok(check('compare',compare)('config-table').innerHTML.includes('Not defined'));
check('compare',{...compare,runs:[],waterfall:[]});
check('compare',{...compare,runs:[{...run,marked_pnl:null,marked_return:null,curve:[]}],waterfall:[]});
for(const file of process.argv.slice(2)) {
  const html=fs.readFileSync(file,'utf8');
  const text=[...html.matchAll(/<script([^>]*)>([\s\S]*?)<\/script>/g)].find(([,attrs])=>attrs.includes('application/json'))[2];
  const model=JSON.parse(text);check(model.runs?'compare':'batch',model,html);
  assert.ok(!html.includes('__REPORT_STYLE__'));
}
console.log('Universe/comparison report checks passed: accounting, unknown/empty states, escaping, themes, print, and script execution.');
